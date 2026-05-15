# Server-Sent Events (SSE) Demo

Two implementations of an SSE client consuming a streaming AI-style response from a FastAPI server.

## Setup

### Python server (shared by both clients)

```bash
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn
```

### Node/TypeScript clients

```bash
npm install eventsource
npm install -D @types/eventsource tsx
```

## Running

### Manual (fetch + ReadableStream)

```bash
# Terminal 1 — start the server
source venv/bin/activate
cd manual
uvicorn server:app --reload --port 8000

# Terminal 2 — run the client
cd manual
npx tsx client.ts
```

### EventSource

```bash
# Terminal 1 — start the server
source venv/bin/activate
cd eventsource
uvicorn server:app --reload --port 8000

# Terminal 2 — run the client
cd eventsource
npx tsx client-eventsource.ts
```

## Why EventSource uses GET

The browser `EventSource` API only supports GET requests — there is no way to set a request method or body. This is a limitation of the spec (designed for simple subscription-style streams).

The manual client uses `fetch` with POST, sending the prompt in a JSON body. To support EventSource, the `eventsource/` server adds a second `GET /v1/stream?prompt=...` endpoint that accepts the prompt as a query parameter. The EventSource client encodes the prompt into the URL:

```ts
const url = `${API_URL}?prompt=${encodeURIComponent(prompt)}`;
const source = new EventSource(url);
```

This is the standard workaround: move request data from the body into query parameters. The trade-off is URL length limits and the prompt being visible in server logs/URLs, which is acceptable for learning but worth considering in production.
