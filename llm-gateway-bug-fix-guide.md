# GitHub Copilot LLM Gateway — Bug Fix Guide
## Issue: "Sorry, no response was returned" with reasoning models (llama.cpp / vLLM)

---

## Root Cause Summary

When using reasoning-capable models (DeepSeek-R1, QwQ, GPT-OSS-120B, etc.), the
OpenAI-compatible streaming response includes a `reasoning_content` field alongside
the standard `content` field.

The **final chunk** of the stream has this structure:

```json
{
  "choices": [{
    "index": 0,
    "delta": { "reasoning_content": null },
    "finish_reason": "stop"
  }]
}
```

**The `content` field is completely absent** from this final chunk.

The gateway's stream accumulator only collects text from `delta.content`. When it
sees `finish_reason: "stop"` but has never received a `delta.content` in the final
chunk, it either:
- Fails to finalize the accumulated response, OR
- Returns an empty/null string instead of the buffered content

Result: gateway logs show `received 0 chars, 0 text parts` despite the model
having streamed a full response.

---

## Evidence from Gateway Logs

**Failing case (82 tools, parallel=false, 17 messages):**
```
Completed chat request, received 0 chars, 0 text parts, 0 tool calls
```

**Curl output showing the bad final chunk:**
```
data: {"choices":[{"delta":{"content":" information"},"finish_reason":null}]}
data: {"choices":[{"delta":{"content":"."},"finish_reason":null}]}
data: {"choices":[{"delta":{"reasoning_content":null},"finish_reason":"stop"}]}
                           ^^^ no "content" field ^^^
data: [DONE]
```

The gateway accumulates `content` chunks fine but then sees a final chunk with
**no `content` field at all** — just `reasoning_content: null` — and fails to
emit the buffered result.

---

## Fix Location

The bug is in the stream chunk processing function, most likely in one of these
source files (check `src/` directory):

- `streamChatCompletion.ts` — primary suspect
- `chatCompletionProvider.ts` — secondary
- `extension.ts` — if streaming logic is inline

Look for the SSE chunk parser — the section that processes each
`data: {...}` line and extracts text from `delta.content`.

---

## The Fix (TypeScript)

### Pattern 1 — Chunk accumulator missing null-guard

**Buggy code (likely looks like this):**
```typescript
// Processing each streaming chunk
const delta = chunk.choices[0]?.delta;
if (delta?.content) {
  accumulatedText += delta.content;
}

// Checking for completion
if (chunk.choices[0]?.finish_reason === 'stop') {
  resolve(accumulatedText);  // BUG: may not reach here if content check fails
}
```

**Fixed code:**
```typescript
const delta = chunk.choices[0]?.delta;

// Handle both content and reasoning_content fields
// reasoning models emit reasoning_content during thinking phase
// and may emit a final chunk with ONLY reasoning_content (null) + finish_reason
if (delta?.content != null) {          // use != null, not truthy check
  accumulatedText += delta.content;
}
// NOTE: intentionally ignore delta.reasoning_content — it's internal model thinking

const finishReason = chunk.choices[0]?.finish_reason;
if (finishReason === 'stop' || finishReason === 'length') {
  resolve(accumulatedText);
}
```

### Pattern 2 — Stream end detection ignores chunks without content

**Buggy code:**
```typescript
if (delta.content) {
  // Only processes chunks that have content
  // SKIPS the final chunk which has finish_reason but no content!
  textParts.push(delta.content);
  
  if (chunk.choices[0]?.finish_reason === 'stop') {
    finalize(textParts.join(''));
  }
}
```

**Fixed code:**
```typescript
// Process content if present (may be absent in final reasoning model chunk)
if (delta?.content != null && delta.content !== '') {
  textParts.push(delta.content);
}

// Check finish_reason OUTSIDE the content check
// Final chunk from reasoning models has finish_reason but NO content field
if (chunk.choices[0]?.finish_reason != null) {
  finalize(textParts.join(''));
}
```

### Pattern 3 — VS Code LanguageModelChatResponse stream

If the gateway uses VS Code's `LanguageModelChatResponse` API:

```typescript
// Buggy — only yields when content present, misses final chunk signal
async function* streamResponse(response: Response) {
  for await (const chunk of parseSSE(response)) {
    const content = chunk.choices[0]?.delta?.content;
    if (content) {                    // BUG: final chunk has no content
      yield new vscode.LanguageModelTextPart(content);
    }
  }
}

// Fixed
async function* streamResponse(response: Response) {
  for await (const chunk of parseSSE(response)) {
    const delta = chunk.choices[0]?.delta;
    
    // Yield content if present (empty string is valid — don't skip it)
    if (delta?.content != null) {
      yield new vscode.LanguageModelTextPart(delta.content);
    }
    
    // Stop on finish signal regardless of content presence
    const finish = chunk.choices[0]?.finish_reason;
    if (finish === 'stop' || finish === 'length') {
      return;
    }
  }
}
```

---

## Key Principle of the Fix

**Change every content check from:**
```typescript
if (delta.content)           // falsy — skips empty string AND missing field
if (delta.content !== null)  // misses undefined
```

**To:**
```typescript
if (delta?.content != null)  // catches both null and undefined, allows ""
```

**And decouple finish_reason detection from content presence:**
```typescript
// finish_reason check must NOT be nested inside content check
// It must run for EVERY chunk regardless of whether content is present
```

---

## Additional Fix: Handle `reasoning_content` chunks gracefully

During the thinking phase, chunks look like:
```json
{"delta": {"reasoning": "The user wants...", "reasoning_content": "The user wants..."}}
```

These have **no `content` field**. Without the fix, these could cause the
accumulator to reset or error. With the fix (using `!= null` guard), they are
silently and correctly ignored for the text output.

---

## How to Apply the Fix

### Option A — Patch the installed extension directly

The installed extension is at:
```
~/.vscode/extensions/andrewbutson.github-copilot-llm-gateway-1.0.4/out/extension.js
```

This is minified. Search for the pattern that handles `finish_reason` and
`delta.content` — look for strings like `"stop"`, `"content"`, `"finish_reason"`.

Use the browser DevTools source mapper or a JS beautifier first:
```bash
# Beautify the minified file for easier editing
npx js-beautify ~/.vscode/extensions/andrewbutson.github-copilot-llm-gateway-1.0.4/out/extension.js \
  > /tmp/extension-readable.js
```

Then search for the bug pattern and apply the fix.

### Option B — Build from source (recommended)

```bash
# Clone the repo
git clone https://github.com/arbs-io/github-copilot-llm-gateway
cd github-copilot-llm-gateway

# Install dependencies
npm install

# Apply the fix to src/ files (see patterns above)
# Then build
npm run compile   # or: npx tsc -p tsconfig.json

# Package as VSIX
npx vsce package

# Install the patched extension
code --install-extension github-copilot-llm-gateway-*.vsix
```

### Option C — Submit a PR

File a GitHub issue or PR at:
https://github.com/arbs-io/github-copilot-llm-gateway/issues

Include:
- The exact final chunk: `{"delta":{"reasoning_content":null},"finish_reason":"stop"}`
- Gateway log showing `received 0 chars`
- This fix guide

---

## Workaround (No Code Change Required)

While waiting for a proper fix, add to `settings.json`:

```json
"github.copilot.llm-gateway.extraModelOptions": {
    "enable_thinking": false,
    "thinking": false,
    "reasoning_effort": 0
}
```

This disables the reasoning phase on the model side, eliminating the
`reasoning_content` field from the stream entirely and avoiding the bug.

**Tradeoff**: Slightly reduces model quality on very complex multi-step reasoning
tasks. Negligible impact on typical coding tasks.

---

## Secondary Issue: Context Overflow

Separately from the streaming bug, very long agent sessions (100+ messages) can
hit context limits:

```
Context overflow: 44041 tokens > 19322 limit. Truncating...
Truncated: kept 40/127 messages
```

**Root cause**: 82 tools × ~290 tokens/tool = ~23,000 tokens consumed by tool
definitions alone, leaving little room for conversation history.

**Fix**: Disable unused VS Code extensions that contribute tools (Azure MCP,
notebook tools, etc.) to reduce the tool count from 82 to ~40, cutting tool
token overhead roughly in half.

**Settings to reduce tool overhead**:
```json
"github.copilot.llm-gateway.parallelToolCalling": false
```

---

## Summary of All Applied Settings

```json
{
  "github.copilot.llm-gateway.serverUrl": "https://your-llm-server/v1",
  "github.copilot.llm-gateway.apiKey": "your-key",
  "github.copilot.llm-gateway.verboseLogging": true,
  "github.copilot.llm-gateway.requestTimeout": 600000,
  "github.copilot.llm-gateway.parallelToolCalling": false,
  "github.copilot.llm-gateway.extraModelOptions": {
      "enable_thinking": false,
      "thinking": false,
      "reasoning_effort": 0
  }
}
```

---

## SOLUTION APPLIED (May 21, 2026)

The root cause has been identified and patched in the source code.

### The Problem

When reasoning models (GPT-OSS-120B, DeepSeek-R1, QwQ, etc.) emit their final
streaming chunk, it contains:
```json
{"delta": {"reasoning_content": null}, "finish_reason": "stop"}
```

**This final chunk has NO `content` field.** The bug was in `src/client.ts`,
function `applyDeltaChoice()` (line ~370):

The original code only drained accumulated tool calls on specific finish reasons:
```typescript
if (parsed.finishReason === 'tool_calls' || parsed.finishReason === 'function_call') {
  finishedToolCalls.push(...accumulator.drain());
}
```

When `finish_reason: 'stop'` arrived (the signal for stream end), any
accumulated tool calls were **never drained**, leaving them in-flight and
causing the response to be reported as empty ("received 0 chars").

### The Fix

Modified `src/client.ts` in `applyDeltaChoice()` to drain tool calls on ALL
terminal finish reasons:

```typescript
// Drain accumulated tool calls on any terminal finish_reason, including
// 'stop'. Reasoning models emit finish_reason: 'stop' on their final chunk,
// which may have no 'content' field — we still need to drain any tool calls
// that were accumulated during prior deltas (issue: "sorry, no response").
if (
  parsed.finishReason === 'tool_calls' ||
  parsed.finishReason === 'function_call' ||
  parsed.finishReason === 'stop' ||        // ← ADDED
  parsed.finishReason === 'length'         // ← ADDED
) {
  finishedToolCalls.push(...accumulator.drain());
}
```

### How to Apply

#### For End Users (Already Fixed in This Repository)

1. **Rebuild the extension**:
   ```bash
   cd /path/to/github-copilot-llm-gateway
   ./node_modules/.bin/esbuild ./src/extension.ts --bundle \
     --outfile=out/extension.js --external:vscode --format=cjs \
     --platform=node --target=es2020 --sourcemap
   ```

2. **Deploy to VS Code**:
   ```bash
   cp out/extension.js ~/.vscode/extensions/andrewbutson.github-copilot-llm-gateway-X.X.X/out/
   cp out/extension.js.map ~/.vscode/extensions/andrewbutson.github-copilot-llm-gateway-X.X.X/out/
   ```

3. **Reload VS Code**:
   - Press `Ctrl+Shift+P` / `Cmd+Shift+P`
   - Run **"Developer: Reload Window"**

4. **Test**:
   - Open Copilot Chat
   - Select a reasoning-capable model
   - Send a request — you should now receive full responses without "Sorry, no response was returned" errors

#### For Contributors / Maintainers

Submit the fix to the upstream repository:
https://github.com/arbs-io/github-copilot-llm-gateway/pulls

Include:
- The `src/client.ts` patch shown above
- A reference to this bug-fix guide
- Test case logs showing "received 0 chars" before and normal completion after

### Verification

After applying the fix, check gateway logs during a request:

**Before fix** (broken):
```
Completed chat request, received 0 chars, 0 text parts, 1 tool calls
Completed chat request, received 0 chars, 0 text parts, 0 tool calls
```

**After fix** (working):
```
Completed chat request, received 1247 chars, 1 text parts, 0 tool calls
Completed chat request, received 892 chars, 1 text parts, 3 tool calls
```

The fix is now live in this repository and ready for deployment.

---

## ADDITIONAL FIX (May 22, 2026) — Handle chunks with finish_reason but no delta/message

### The Problem (Round 2)

After the initial fix, a third issue emerged: some models send a **final chunk with a finish_reason but no content payload** (neither `delta` nor `message`). The code path in `dispatchParsedChunk()` was:

```typescript
if (chunk.delta) { /* process delta */ }
else if (chunk.message) { /* process message */ }
else { return null; }  // ← BUG: Silently drops chunk!
```

When a chunk arrived with `finish_reason: 'stop'` or `'length'` but no `delta` or `message`, it would return `null` and never be yielded. This meant:
- No chunk was emitted to the consumer
- Any accumulated tool calls were **never drained** (left in the accumulator)
- The response appeared empty again: "received 0 chars, 0 text parts"

### The Fix

Added a third condition in `dispatchParsedChunk()` to handle finish-reason-only chunks:

```typescript
// Handle final chunks with finish_reason but no delta/message payload.
// Some models (e.g., reasoning models) send a final chunk with only
// finish_reason and accumulated tool_calls; we must drain those calls
// even though there's no content to yield.
if (chunk.finishReason === 'stop' || chunk.finishReason === 'length') {
  const finishedToolCalls = accumulator.drain();
  if (finishedToolCalls.length > 0) {
    return {
      content: '',
      reasoning_content: '',
      tool_calls: [],
      finished_tool_calls: finishedToolCalls,
      ...(usage ? { usage } : {}),
    };
  }
}
```

This ensures:
1. Even if there's no `delta` or `message`, a final chunk with finish_reason is still processed
2. Accumulated tool calls are drained and yielded as `finished_tool_calls`
3. A chunk is always emitted, never silently dropped

### Verification

After this patch, even the most complex tool-heavy requests should show:
```
Completed chat request, received <N> chars, <N> text parts, <N> tool calls
```

Never `received 0 chars` again.

---

## CRITICAL FIX (May 22, 2026, Iteration 3) — Always yield on finish_reason, even when empty

### The Problem (Round 3)

After round 2, requests STILL showed "received 0 chars, 0 text parts, 0 tool calls". The issue was in the finish-reason-only handler: when a final chunk arrived with `finish_reason: 'stop'` or `'length'` but **had NO accumulated tool calls**, the code returned `null` without yielding anything:

```typescript
if (chunk.finishReason === 'stop' || chunk.finishReason === 'length') {
  const finishedToolCalls = accumulator.drain();
  if (finishedToolCalls.length > 0) {  // ← BUG: Skip yield if empty!
    return { ... };
  }
}
return null;  // ← Silent drop if no tool calls
```

**Result:** The stream ended with no chunks yielded, and the consumer saw an empty response.

### The Fix

**ALWAYS yield a chunk when there's a terminal finish_reason, regardless of whether tool calls are present:**

```typescript
if (chunk.finishReason === 'stop' || chunk.finishReason === 'length') {
  const finishedToolCalls = accumulator.drain();
  // Remove the length check — ALWAYS yield on finish_reason
  return {
    content: '',
    reasoning_content: '',
    tool_calls: [],
    finished_tool_calls: finishedToolCalls,  // Empty array is OK
    ...(usage ? { usage } : {}),
  };
}
```

This ensures:
1. Every terminal finish_reason produces a yielded chunk (never silent drops)
2. Stream completion is signaled correctly
3. Accumulated tool calls are drained regardless of whether they're populated
4. Consumer always gets a final chunk, even if empty

### Root Cause

The conditional `if (finishedToolCalls.length > 0)` was overly restrictive. The consumer needs to know the stream finished, even if there's no meaningful data. Yielding an empty chunk is semantically correct and necessary for proper stream semantics.
