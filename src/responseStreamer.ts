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
  // Use a larger threshold (512 chars) to ensure early chunks are buffered.
  let textBuffer = '';
  const BUFFER_THRESHOLD = 512; // More aggressive buffering

  for await (const chunk of chunks) {
    if (isCancelled()) {
      break;
    }
    
    // Accumulate content in buffer
    if (chunk.content != null && chunk.content.length > 0) {
      textBuffer += chunk.content;
      
      // Only report when buffer reaches threshold, unless we're at end or have other events
      if (textBuffer.length >= BUFFER_THRESHOLD) {
        // Process buffered content through parser and report
        for (const piece of parser.process(textBuffer)) {
          reportParserPiece(piece, reporter, stats, false);
        }
        textBuffer = '';
      }
    }
    
    // Handle reasoning content (report immediately, don't buffer)
    if (chunk.reasoning_content) {
      // Flush any buffered text first
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
    
    // Handle tool calls and usage (report immediately, flush buffer first)
    if (chunk.finished_tool_calls?.length || chunk.usage) {
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
