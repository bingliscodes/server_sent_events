"""
SSE server that simulates streaming an AI response token by token.

Install dependencies:
    pip install fastapi uvicorn

Run:
    uvicorn server:app --reload --port 8000
"""

import asyncio
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simulated AI response broken into tokens
FAKE_RESPONSE_TOKENS = [
    "The",
    " theory",
    " of",
    " general",
    " relativity",
    ",",
    " published",
    " by",
    " Albert",
    " Einstein",
    " in",
    " 1915",
    ",",
    " describes",
    " gravity",
    " not",
    " as",
    " a",
    " force",
    ",",
    " but",
    " as",
    " a",
    " curvature",
    " of",
    " spacetime",
    " caused",
    " by",
    " mass",
    " and",
    " energy",
    ".",
]


async def generate_stream(prompt: str):
    """Yields SSE-formatted events that mimic an AI streaming response."""

    # 1. Send the start event
    yield format_sse(
        "message_start",
        {
            "id": "msg_001",
            "model": "fake-model-v1",
            "prompt": prompt,
        },
    )

    # 2. Stream tokens one by one
    for i, token in enumerate(FAKE_RESPONSE_TOKENS):
        await asyncio.sleep(0.05)  # simulate generation latency
        yield format_sse(
            "content_delta",
            {
                "index": i,
                "text": token,
            },
        )

    # 3. Send usage stats and close
    yield format_sse(
        "message_stop",
        {
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": len(prompt.split()),
                "output_tokens": len(FAKE_RESPONSE_TOKENS),
            },
        },
    )


def format_sse(event: str, data: dict) -> str:
    """Format a single SSE event with the given event type and JSON data."""
    payload = json.dumps(data)
    return f"event: {event}\ndata: {payload}\n\n"


@app.post("/v1/stream")
async def stream_response(body: dict):
    prompt = body.get("prompt", "")
    return StreamingResponse(
        generate_stream(prompt),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # disable nginx buffering if proxied
        },
    )


# GET endpoint for EventSource clients (prompt passed as query param)
@app.get("/v1/stream")
async def stream_response_get(
    prompt: str = "Explain general relativity in one sentence.",
):
    return StreamingResponse(
        generate_stream(prompt),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
