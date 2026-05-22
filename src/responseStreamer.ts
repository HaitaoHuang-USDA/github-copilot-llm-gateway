/**
 * Stream processor for OpenAI-compatible SSE chat completion chunks.
 *
 * Owns the ThinkingParser and the book-keeping around `reasoning_content`
 * fields, `<thinking>` tags, and force-closed thinking blocks. Reports
 * results through a {@link StreamReporter} interface rather than talking
 * to VS Code directly, so it can be exercised by unit tests with a fake
 * reporter.
 */

import { ThinkingParser, ThinkingChunk } from './thinking';
import { OpenAIUsage } from './types';

export interface StreamReporter {
  reportText(text: string): void;
  reportThinking(text: string): void;
  reportThinkingDone(): void;
  reportToolCall(id: string, name: string, args: Record<string, unknown>): void;
  /**
   * Report a usage frame from the inference server. Called at most once per
   * stream — the OpenAI convention is to emit a trailing chunk with totals
   * after the last delta. Wired to VS Code's chat context-window widget via
   * a `LanguageModelDataPart` (issue #24).
   */
  reportUsage(usage: OpenAIUsage): void;
}

export interface StreamChunk {
  content?: string;
  reasoning_content?: string;
  finished_tool_calls?: Array<{ id: string; name: string; arguments: string }>;
  usage?: OpenAIUsage;
}

export interface StreamStats {
  /** Number of content characters observed across all chunks. */
  totalContentLength: number;
  totalToolCalls: number;
  totalTextParts: number;
  hadThinking: boolean;
  thinkingForceClosed: boolean;
  /**
   * True once a usage frame has been dispatched to the reporter. Internal
   * book-keeping to dedupe re-emitted totals from chatty servers; optional
   * so callers constructing `StreamStats` for `isEmptyStreamResult` checks
   * don't need to pass it.
   */
  reportedUsage?: boolean;
}

export interface StreamResponseParams {
  chunks: AsyncIterable<StreamChunk>;
  reporter: StreamReporter;
  /** Called before reading each chunk; return true to stop early. */
  isCancelled: () => boolean;
  /**
   * Called with each finished tool call. The callback is responsible for
   * JSON-repairing the arguments and filling any missing required properties
   * from the tool's schema.
   */
  resolveToolCallArgs: (toolCall: { id: string; name: string; arguments: string }) => Record<string, unknown>;
}

const FORCE_CLOSED_THINKING_FALLBACK =
  '*(The model ran out of output tokens while thinking and could not produce a response. ' +
  'Try increasing the context length or max output tokens in LM Studio, ' +
  'or disable thinking for this model.)*';

/**
 * Find a safe place to split text, preferring natural boundaries like newlines/spaces.
 * Avoids splitting emoji or multi-codepoint sequences.
 */
function findSafeFlushPoint(text: string, maxLength: number): number {
  if (text.length <= maxLength) return text.length;
  
  // First, try to find a newline before maxLength (safest split)
  let lastNewline = text.lastIndexOf('\n', maxLength);
  if (lastNewline > 0 && lastNewline > maxLength - 100) {
    return lastNewline + 1; // Include the newline
  }
  
  // Second, try to find whitespace before maxLength
  let searchEnd = Math.min(maxLength, text.length);
  while (searchEnd > 0) {
    const char = text[searchEnd - 1];
    if (char === ' ' || char === '\t' || char === '\n') {
      return searchEnd;
    }
    searchEnd--;
    
    // If we've searched back 50 chars and found nothing, give up and use maxLength
    if (maxLength - searchEnd > 50) {
      break;
    }
  }
  
  // Last resort: split at maxLength but back up from emoji sequences
  let pos = maxLength;
  while (pos > Math.max(1, maxLength - 10)) {
    const code = text.charCodeAt(pos - 1);
    
    // Low surrogate: part of surrogate pair, back up
    if (code >= 0xDC00 && code <= 0xDFFF) {
      pos -= 2;
      continue;
    }
    
    // Variation selector or zero-width joiner: include with base char
    if (code === 0xFE0F || code === 0x200D) {
      pos -= 1;
      continue;
    }
    
    break;
  }
  
  return Math.max(1, pos);
}

/**
 * Dispatch a single ThinkingParser piece to the reporter, updating stats.
 *
 * `allowForceClose` is true only when flushing the parser at end-of-stream —
 * an 'E' piece mid-stream is just a normal end-of-thinking marker, while an
 * 'E' piece at flush time indicates the stream truncated mid-think block.
 */
function reportParserPiece(
  piece: ThinkingChunk,
  reporter: StreamReporter,
  stats: StreamStats,
  allowForceClose: boolean
): void {
  if (piece.t === 'T') {
    stats.hadThinking = true;
    reporter.reportThinking(piece.c);
    return;
  }
  if (piece.t === 'E') {
    if (allowForceClose) {
      stats.thinkingForceClosed = true;
    }
    reporter.reportThinkingDone();
    return;
  }
  if (piece.c) {
    stats.totalTextParts++;
    reporter.reportText(piece.c);
  }
}

/**
 * Process a single stream chunk, updating stats and dispatching events
 * through the reporter. Text content buffering is handled in streamResponse().
 * @returns updated inReasoningField flag.
 */
function processStreamChunk(
  chunk: StreamChunk,
  parser: ThinkingParser,
  reporter: StreamReporter,
  stats: StreamStats,
  inReasoningField: boolean,
  resolveToolCallArgs: StreamResponseParams['resolveToolCallArgs']
): boolean {
  if (chunk.reasoning_content) {
    stats.hadThinking = true;
    inReasoningField = true;
    reporter.reportThinking(chunk.reasoning_content);
  }

  // Content is buffered in streamResponse(), so we don't process it here
  if (chunk.content != null) {
    if (inReasoningField) {
      inReasoningField = false;
      reporter.reportThinkingDone();
    }
    stats.totalContentLength += chunk.content.length;
  }

  if (chunk.finished_tool_calls?.length) {
    for (const toolCall of chunk.finished_tool_calls) {
      stats.totalToolCalls++;
      const args = resolveToolCallArgs(toolCall);
      reporter.reportToolCall(toolCall.id, toolCall.name, args);
    }
  }

  if (chunk.usage && !stats.reportedUsage) {
    stats.reportedUsage = true;
    reporter.reportUsage(chunk.usage);
  }

  return inReasoningField;
}

/**
 * Consume an async stream of chat completion chunks, dispatching pieces to
 * the reporter as they arrive. Returns aggregate stats that the caller can
 * use to decide whether the response was empty and needs an error fallback.
 */
export async function streamResponse(params: StreamResponseParams): Promise<StreamStats> {
  const { chunks, reporter, isCancelled, resolveToolCallArgs } = params;

  const stats: StreamStats = {
    totalContentLength: 0,
    totalToolCalls: 0,
    totalTextParts: 0,
    hadThinking: false,
    thinkingForceClosed: false,
    reportedUsage: false,
  };

  const parser = new ThinkingParser();
  let inReasoningField = false;
  // Buffer to accumulate small text pieces before reporting.
  // Prevents word-per-line rendering when gateway sends token-by-token.
  // Use a conservative threshold (1024 chars) to ensure complete emoji sequences.
  let textBuffer = '';
  let emojiBuffer = ''; // Separate buffer for emoji chunks that need combining
  const BUFFER_THRESHOLD = 1024; // Conservative buffering for emoji safety

  for await (const chunk of chunks) {
    if (isCancelled()) {
      break;
    }
    
    // Accumulate content in buffer
    if (chunk.content != null && chunk.content.length > 0) {
      // Check if this chunk is pure emoji or combining marks
      // Matches:
      // 1. Surrogate pairs (outside BMP): \uD800-\uDBFF followed by \uDC00-\uDFFF
      // 2. BMP emoji ranges: \u2600-\u27BF (Miscellaneous Symbols, Dingbats), \u2300-\u23FF, \u2B50-\u2BFF
      // 3. Variation selectors and zero-width joiners: \uFE0F, \u200D
      const isEmojiOnlyChunk = /^(?:[\uD800-\uDBFF][\uDC00-\uDFFF]|[\u2300-\u23FF]|[\u2600-\u27BF]|[\u2B50-\u2BFF]|[\uFE0F\u200D])+$/.test(chunk.content);
      
      if (isEmojiOnlyChunk) {
        // Accumulate emoji in separate buffer to keep them together
        emojiBuffer += chunk.content;
      } else {
        // Non-emoji chunk: flush emoji buffer first, then handle text
        if (emojiBuffer.length > 0) {
          for (const piece of parser.process(emojiBuffer)) {
            reportParserPiece(piece, reporter, stats, false);
          }
          emojiBuffer = '';
        }
        
        // Add to text buffer
        textBuffer += chunk.content;
        
        // Flush when buffer reaches threshold
        if (textBuffer.length >= BUFFER_THRESHOLD) {
          const flushPoint = findSafeFlushPoint(textBuffer, BUFFER_THRESHOLD);
          const pointToFlush = flushPoint > 0 ? flushPoint : textBuffer.length;
          const toReport = textBuffer.slice(0, pointToFlush);
          textBuffer = textBuffer.slice(pointToFlush);
          
          for (const piece of parser.process(toReport)) {
            reportParserPiece(piece, reporter, stats, false);
          }
        }
      }
    }
    
    // Handle reasoning content (report immediately, don't buffer)
    if (chunk.reasoning_content) {
      // Flush any buffered text and emoji first
      if (emojiBuffer.length > 0) {
        for (const piece of parser.process(emojiBuffer)) {
          reportParserPiece(piece, reporter, stats, false);
        }
        emojiBuffer = '';
      }
      
      if (textBuffer.length > 0) {
        for (const piece of parser.process(textBuffer)) {
          reportParserPiece(piece, reporter, stats, false);
        }
        textBuffer = '';
      }
      
      stats.hadThinking = true;
      inReasoningField = true;
      reporter.reportThinking(chunk.reasoning_content);
    }
    
    // Handle tool calls and usage (report immediately, flush buffers first)
    if (chunk.finished_tool_calls?.length || chunk.usage) {
      if (emojiBuffer.length > 0) {
        for (const piece of parser.process(emojiBuffer)) {
          reportParserPiece(piece, reporter, stats, false);
        }
        emojiBuffer = '';
      }
      
      if (textBuffer.length > 0) {
        for (const piece of parser.process(textBuffer)) {
          reportParserPiece(piece, reporter, stats, false);
        }
        textBuffer = '';
      }
      
      if (inReasoningField) {
        inReasoningField = false;
        reporter.reportThinkingDone();
      }
    }
    
    if (chunk.finished_tool_calls?.length) {
      for (const toolCall of chunk.finished_tool_calls) {
        stats.totalToolCalls++;
        const args = resolveToolCallArgs(toolCall);
        reporter.reportToolCall(toolCall.id, toolCall.name, args);
      }
    }

    if (chunk.usage && !stats.reportedUsage) {
      stats.reportedUsage = true;
      reporter.reportUsage(chunk.usage);
    }
  }

  // Flush any remaining buffered content. 'E' pieces here signal that the
  // stream ended mid-think block.
  if (emojiBuffer.length > 0) {
    for (const piece of parser.process(emojiBuffer)) {
      reportParserPiece(piece, reporter, stats, false);
    }
  }
  
  if (textBuffer.length > 0) {
    for (const piece of parser.process(textBuffer)) {
      reportParserPiece(piece, reporter, stats, false);
    }
  }
  
  for (const piece of parser.flush()) {
    reportParserPiece(piece, reporter, stats, true);
  }

  if (inReasoningField) {
    reporter.reportThinkingDone();
  }

  // If the model spent all its output budget inside a thinking block and
  // produced no visible text or tool calls, emit a fallback message so the
  // Copilot Chat UI has something to render.
  if (stats.thinkingForceClosed && stats.totalTextParts === 0 && stats.totalToolCalls === 0) {
    reporter.reportText(FORCE_CLOSED_THINKING_FALLBACK);
  }

  return stats;
}

/**
 * Determine whether a completed stream should be treated as empty (and thus
 * needs an error fallback message). A stream with thinking content but no
 * visible output is still "empty" from the user's perspective only if the
 * thinking block was force-closed.
 */
export function isEmptyStreamResult(stats: StreamStats): boolean {
  return (
    stats.totalContentLength === 0 &&
    stats.totalToolCalls === 0 &&
    !stats.hadThinking &&
    !stats.thinkingForceClosed
  );
}
