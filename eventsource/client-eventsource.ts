/**
 * TypeScript SSE client using the EventSource API.
 *
 * Node.js doesn't include EventSource natively, so we need a polyfill:
 *     npm install eventsource
 *     npm install -D @types/eventsource
 *
 * Run with: npx tsx client-eventsource.ts
 *
 * Key differences from the fetch-based client (client.ts):
 *   - No manual parsing — EventSource handles the text/event-stream format.
 *   - Named events need explicit addEventListener calls (onmessage won't catch them).
 *   - GET only — prompt must go in the URL as a query parameter.
 *   - Auto-reconnects on close — we must call .close() when the stream is done.
 */

import EventSource from "eventsource";

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

function main() {
  const prompt = "Explain general relativity in one sentence.";
  console.log(`Prompt: ${prompt}\n`);

  // EventSource only supports GET, so the prompt goes in the query string
  const url = `${API_URL}?prompt=${encodeURIComponent(prompt)}`;
  const source = new EventSource(url);

  let fullText = "";

  // ── Named event listeners ───────────────────────────────────────
  // Each "event: xxx" in the stream needs its own addEventListener.
  // The generic "message" event does NOT fire for named events.

  source.addEventListener("message_start", ((e: MessageEvent) => {
    const payload: MessageStart = JSON.parse(e.data);
    console.log(`Stream started — model: ${payload.model}, id: ${payload.id}`);
  }) as EventListener);

  source.addEventListener("content_delta", ((e: MessageEvent) => {
    const payload: ContentDelta = JSON.parse(e.data);
    process.stdout.write(payload.text);
    fullText += payload.text;
  }) as EventListener);

  source.addEventListener("message_stop", ((e: MessageEvent) => {
    const payload: MessageStop = JSON.parse(e.data);
    console.log(`\n\nStream complete — reason: ${payload.stop_reason}`);
    console.log(
      `Usage: ${payload.usage.input_tokens} input / ${payload.usage.output_tokens} output tokens`
    );
    console.log(`\nFull response: ${fullText}`);

    // Critical: close the connection or EventSource will reconnect
    source.close();
  }) as EventListener);

  // ── Lifecycle events ────────────────────────────────────────────

  source.onopen = () => {
    // Connection established
  };

  source.onerror = (e) => {
    if (source.readyState === EventSource.CLOSED) {
      console.error("Connection closed.");
    } else {
      console.error("Connection error — EventSource will retry automatically.");
    }
  };
}

main();
