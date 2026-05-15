/**
 * TypeScript SSE client that consumes a streaming AI response.
 *
 * We use fetch + ReadableStream instead of EventSource because:
 *   1. EventSource only supports GET — our endpoint is POST.
 *   2. We want fine-grained control over parsing named events.
 *
 * Run with: npx tsx client.ts
 */

const API_URL = "http://localhost:8000/v1/stream";

interface ContentDelta {
  index: number;
  text: string;
}

interface MessageStart {
  id: string;
  model: string;
  prompt: string;
}

interface MessageStop {
  stop_reason: string;
  usage: {
    input_tokens: number;
    output_tokens: number;
  };
}

// ── SSE parser ──────────────────────────────────────────────────────
// Parses raw text/event-stream into discrete events.

interface SSEEvent {
  event: string;
  data: string;
}

async function* parseSSE(
  stream: ReadableStream<Uint8Array>
): AsyncGenerator<SSEEvent> {
  const decoder = new TextDecoder();
  let buffer = "";

  for await (const chunk of streamToAsyncIterable(stream)) {
    buffer += decoder.decode(chunk, { stream: true });

    // Split on double newline — each segment is one event block
    const parts = buffer.split("\n\n");

    // The last element is either empty or an incomplete block, keep it
    buffer = parts.pop() ?? "";

    for (const block of parts) {
      if (!block.trim()) continue;

      let event = "message";
      let data = "";

      for (const line of block.split("\n")) {
        if (line.startsWith("event: ")) {
          event = line.slice(7);
        } else if (line.startsWith("data: ")) {
          data += line.slice(6);
        } else if (line.startsWith(":")) {
          // Comment — ignored, just like EventSource would
        }
      }

      if (data) {
        yield { event, data };
      }
    }
  }
}

// Helper: adapt ReadableStream to async iterable (not all runtimes do this natively)
async function* streamToAsyncIterable(
  stream: ReadableStream<Uint8Array>
): AsyncGenerator<Uint8Array> {
  const reader = stream.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) yield value;
    }
  } finally {
    reader.releaseLock();
  }
}

// ── Main ────────────────────────────────────────────────────────────

async function main() {
  const prompt = "Explain general relativity in one sentence.";
  console.log(`Prompt: ${prompt}\n`);

  const response = await fetch(API_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ prompt }),
  });

  if (!response.ok || !response.body) {
    throw new Error(`Request failed: ${response.status}`);
  }

  let fullText = "";

  for await (const { event, data } of parseSSE(response.body)) {
    switch (event) {
      case "message_start": {
        const payload: MessageStart = JSON.parse(data);
        console.log(`Stream started — model: ${payload.model}, id: ${payload.id}`);
        break;
      }

      case "content_delta": {
        const payload: ContentDelta = JSON.parse(data);
        process.stdout.write(payload.text); // print token without newline
        fullText += payload.text;
        break;
      }

      case "message_stop": {
        const payload: MessageStop = JSON.parse(data);
        console.log(`\n\nStream complete — reason: ${payload.stop_reason}`);
        console.log(
          `Usage: ${payload.usage.input_tokens} input / ${payload.usage.output_tokens} output tokens`
        );
        break;
      }

      default:
        console.warn(`Unknown event type: ${event}`);
    }
  }

  console.log(`\nFull response: ${fullText}`);
}

main().catch(console.error);
