/**
 * streamChatCompletion.ts — Patched version
 *
 * Fix: Handle streaming chunks from reasoning models (DeepSeek-R1, QwQ,
 * GPT-OSS-120B, etc.) that emit `reasoning_content` alongside or instead
 * of `content` in SSE delta chunks.
 *
 * Bug: The final streaming chunk from reasoning models has this structure:
 *   {"delta": {"reasoning_content": null}, "finish_reason": "stop"}
 * The `content` field is ABSENT. The original code's content-presence check
 * caused it to miss the finish signal and return 0 chars.
 *
 * Key changes marked with: // FIX:
 */

import * as vscode from "vscode";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ChatCompletionChunk {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: Array<{
    index: number;
    delta: {
      role?: string;
      content?: string | null;           // may be absent in reasoning chunks
      reasoning?: string | null;         // llama.cpp thinking field
      reasoning_content?: string | null; // vLLM / OpenAI-compat thinking field
      tool_calls?: ToolCallDelta[];
    };
    finish_reason: string | null;
    logprobs?: unknown;
  }>;
}

interface ToolCallDelta {
  index: number;
  id?: string;
  type?: string;
  function?: {
    name?: string;
    arguments?: string;
  };
}

interface StreamResult {
  text: string;
  toolCalls: ResolvedToolCall[];
}

interface ResolvedToolCall {
  id: string;
  name: string;
  arguments: string;
}

// ---------------------------------------------------------------------------
// Main streaming function
// ---------------------------------------------------------------------------

export async function streamChatCompletion(
  response: Response,
  stream: vscode.LanguageModelChatResponseStream,
  verboseLogging: boolean
): Promise<StreamResult> {
  let accumulatedText = "";
  let charCount = 0;
  let textPartCount = 0;
  let toolCallCount = 0;

  // Tool call accumulation (for parallel=false sequential tool calls)
  const toolCallMap = new Map<number, Partial<ResolvedToolCall> & { argumentsBuffer: string }>();

  try {
    for await (const chunk of parseSSEStream(response)) {
      if (!chunk.choices || chunk.choices.length === 0) {
        continue;
      }

      const choice = chunk.choices[0];
      const delta = choice.delta;
      const finishReason = choice.finish_reason;

      // ---------------------------------------------------------------
      // FIX: Handle content field safely.
      // Use `!= null` (not truthy) to correctly handle:
      //   - Normal chunks:  delta.content = "some text"  → accumulate
      //   - Empty chunks:   delta.content = ""           → accumulate (valid)
      //   - Absent field:   delta.content = undefined    → skip (reasoning chunk)
      //   - Null field:     delta.content = null         → skip
      // ---------------------------------------------------------------
      if (delta?.content != null) {
        accumulatedText += delta.content;
        charCount += delta.content.length;

        if (delta.content.length > 0) {
          textPartCount++;
          // Yield text to VS Code chat stream
          stream.text(delta.content);
        }
      }

      // ---------------------------------------------------------------
      // FIX: Intentionally ignore reasoning/thinking fields.
      // These are internal model chain-of-thought tokens. They appear in:
      //   delta.reasoning          (llama.cpp format)
      //   delta.reasoning_content  (vLLM / OpenAI-compat format)
      //
      // We do NOT yield these to the chat stream. They are model-internal.
      // The final chunk often has ONLY reasoning_content (null) + finish_reason.
      // The original bug was caused by gating finish_reason detection on
      // content presence, which caused this final chunk to be ignored.
      // ---------------------------------------------------------------

      // Handle tool call deltas (sequential, parallel=false)
      if (delta?.tool_calls) {
        for (const tcDelta of delta.tool_calls) {
          const idx = tcDelta.index;

          if (!toolCallMap.has(idx)) {
            toolCallMap.set(idx, {
              id: tcDelta.id ?? "",
              name: tcDelta.function?.name ?? "",
              argumentsBuffer: tcDelta.function?.arguments ?? "",
            });
          } else {
            const existing = toolCallMap.get(idx)!;
            if (tcDelta.id) existing.id = tcDelta.id;
            if (tcDelta.function?.name) existing.name = tcDelta.function.name;
            if (tcDelta.function?.arguments) {
              existing.argumentsBuffer += tcDelta.function.arguments;
            }
          }
        }
      }

      // ---------------------------------------------------------------
      // FIX: Check finish_reason OUTSIDE any content guard.
      // This is the critical fix — finish_reason must be detected even
      // when the final chunk has no `content` field (reasoning models).
      //
      // Original buggy pattern:
      //   if (delta.content) {
      //     ...
      //     if (finish_reason === 'stop') { resolve(); }  ← never reached!
      //   }
      //
      // Fixed pattern: finish_reason check is independent of content.
      // ---------------------------------------------------------------
      if (finishReason === "stop" || finishReason === "length") {
        if (verboseLogging) {
          console.log(
            `[LLM Gateway] Stream complete: finish_reason=${finishReason}, ` +
            `chars=${charCount}, textParts=${textPartCount}, ` +
            `toolCalls=${toolCallMap.size}`
          );
        }
        break;
      }
    }
  } catch (error) {
    // FIX: Distinguish between a true fetch error and a clean stream end.
    // If we have accumulated text, a stream read error after [DONE] is
    // not a real failure — return what we have.
    if (charCount > 0 || toolCallMap.size > 0) {
      console.warn(
        "[LLM Gateway] Stream read error after partial response — " +
        "returning accumulated content:",
        error
      );
    } else {
      throw error;
    }
  }

  // Resolve accumulated tool calls
  const toolCalls: ResolvedToolCall[] = [];
  for (const [, tc] of toolCallMap) {
    if (tc.id && tc.name) {
      toolCalls.push({
        id: tc.id,
        name: tc.name,
        arguments: tc.argumentsBuffer ?? "",
      });
      toolCallCount++;
    }
  }

  if (verboseLogging) {
    console.log(
      `Completed chat request, received ${charCount} chars, ` +
      `${textPartCount} text parts, ${toolCallCount} tool calls`
    );
  }

  return { text: accumulatedText, toolCalls };
}

// ---------------------------------------------------------------------------
// SSE parser
// ---------------------------------------------------------------------------

async function* parseSSEStream(
  response: Response
): AsyncGenerator<ChatCompletionChunk> {
  const reader = response.body?.getReader();
  if (!reader) {
    throw new Error("Response body is null — cannot read stream");
  }

  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        // Process any remaining buffer content
        if (buffer.trim()) {
          const chunk = tryParseSSELine(buffer.trim());
          if (chunk) yield chunk;
        }
        break;
      }

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");

      // Keep the last (potentially incomplete) line in the buffer
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        const trimmed = line.trim();

        // Skip empty lines and comments
        if (!trimmed || trimmed.startsWith(":")) continue;

        // [DONE] signals end of stream
        if (trimmed === "data: [DONE]") break;

        const chunk = tryParseSSELine(trimmed);
        if (chunk) yield chunk;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function tryParseSSELine(line: string): ChatCompletionChunk | null {
  if (!line.startsWith("data: ")) return null;

  const jsonStr = line.slice(6).trim();
  if (jsonStr === "[DONE]") return null;

  try {
    return JSON.parse(jsonStr) as ChatCompletionChunk;
  } catch {
    console.warn("[LLM Gateway] Failed to parse SSE chunk:", jsonStr);
    return null;
  }
}
