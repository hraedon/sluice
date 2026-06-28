"""Tests for the streaming reverse proxy.

Covers: incremental streaming, 503 on queue timeout, permit release on
completion and disconnect, auth passthrough, 429 reporting.
"""

from __future__ import annotations

import asyncio
import json

import httpx

from sluice.control import BreakerConfig, ControllerConfig, UsageReading
from sluice.gate import PermitGate
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.usage import CachedReading


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class FakeUsageClient:
    """Minimal usage client for proxy tests."""

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        return CachedReading(
            reading=UsageReading(concurrent_sessions=0, limit=4, hard_cap=8),
            fetched_at_monotonic=now_monotonic,
            ok=True,
        )

    async def close(self) -> None:
        pass


class _AsyncMockTransport(httpx.AsyncBaseTransport):
    """Mock transport that ensures responses support async streaming.

    httpx.Response(json=...) creates a ByteStream with is_stream_consumed=True,
    which breaks aiter_raw().  This transport re-wraps content as an async
    generator so the proxy can stream it properly.
    """

    def __init__(self, handler):
        self._handler = handler

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        # Drain the request body so the proxy's body_stream generator is consumed.
        async for _ in request.stream:
            pass

        response = self._handler(request)

        # Already a proper async stream — pass through.
        if isinstance(response.stream, httpx.AsyncByteStream) and not response.is_stream_consumed:
            return response

        # Re-wrap content as async generator.
        content = response.content if response.is_stream_consumed else b"".join(response.stream)

        async def gen():
            yield content

        headers = [
            (k, v)
            for k, v in response.headers.items()
            if k.lower() not in ("content-length", "transfer-encoding")
        ]
        return httpx.Response(response.status_code, headers=headers, content=gen())


def _resp(status: int = 200, *, json_data=None, headers=None) -> httpx.Response:
    """Create a Response with async-streamable content."""
    payload = json.dumps(json_data or {"ok": True}).encode()
    h = dict(headers or {})
    h.setdefault("content-type", "application/json")

    async def gen():
        yield payload

    return httpx.Response(status, content=gen(), headers=h)


def _streaming_resp(chunks: list[bytes], *, content_type: str = "text/event-stream") -> httpx.Response:
    """Create a streaming Response with multiple chunks."""

    async def gen():
        for chunk in chunks:
            yield chunk

    return httpx.Response(200, content=gen(), headers={"content-type": content_type})


def _make_app(
    *,
    gate_capacity: int = 3,
    queue_timeout: float = 30.0,
    upstream_handler=None,
) -> tuple[ProxyApp, PermitGate, ReconciliationLoop]:
    gate = PermitGate(initial_capacity=gate_capacity)
    usage = FakeUsageClient()
    upstream_client = httpx.AsyncClient(
        transport=_AsyncMockTransport(upstream_handler or _default_handler),
        timeout=None,
    )
    reconcile = ReconciliationLoop(
        usage_client=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        queue_timeout=queue_timeout,
        upstream_client=upstream_client,
    )
    return app, gate, reconcile


def _default_handler(request: httpx.Request) -> httpx.Response:
    return _resp(200, json_data={"ok": True})


def _asgi_client(app: ProxyApp) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------


async def test_streamed_response_incremental():
    """Response arrives in multiple chunks, not buffered into one.

    Uses raw ASGI (not ASGITransport) because ASGITransport buffers the body.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return _streaming_resp([b"data: chunk1\n\n", b"data: chunk2\n\n", b"data: chunk3\n\n"])

    app, gate, _ = _make_app(upstream_handler=handler)

    sent_events: list[dict] = []
    receive_queue: asyncio.Queue = asyncio.Queue()

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

    await receive_queue.put(
        {"type": "http.request", "body": b'{"prompt":"hi"}', "more_body": False}
    )

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    await app(scope, receive, send)

    body_events = [e for e in sent_events if e["type"] == "http.response.body" and e.get("body")]
    assert len(body_events) >= 2, "should send multiple body events"
    body = b"".join(e["body"] for e in body_events)
    assert b"chunk1" in body
    assert b"chunk3" in body


async def test_both_routes_proxy():
    """Both /v1/messages and /v1/chat/completions are proxied."""
    paths_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths_seen.append(request.url.path)
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        r1 = await client.post("/v1/messages", json={"prompt": "hi"})
        r2 = await client.post("/v1/chat/completions", json={"messages": []})

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert paths_seen == ["/v1/messages", "/v1/chat/completions"]


async def test_arbitrary_path_proxied():
    """Any path is transparently proxied, not just the two known routes."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(200, json_data={"path": request.url.path})

    app, _, _ = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        response = await client.get("/v1/models")

    assert response.status_code == 200
    assert response.json()["path"] == "/v1/models"


# ---------------------------------------------------------------------------
# Permit lifecycle
# ---------------------------------------------------------------------------


async def test_503_on_queue_timeout():
    """Returns 503 + Retry-After when the permit queue times out."""
    app, gate, _ = _make_app(gate_capacity=0, queue_timeout=0.1)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 503
    assert "retry-after" in {k.lower() for k in response.headers}


async def test_permit_released_on_completion():
    """Permit is released after a normal request completes."""
    app, gate, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 200
    assert gate.held == 0


async def test_permit_released_on_disconnect():
    """Permit is released when the downstream client disconnects mid-stream."""

    async def slow_gen():
        yield b"chunk1\n"
        await asyncio.sleep(0.2)  # yield control so disconnect can be processed
        yield b"chunk2\n"
        await asyncio.sleep(0.2)
        yield b"chunk3\n"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=slow_gen(),
            headers={"content-type": "text/event-stream"},
        )

    app, gate, _ = _make_app(upstream_handler=handler)

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []
    first_chunk = asyncio.Event()

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)
        if event["type"] == "http.response.body" and event.get("body"):
            first_chunk.set()

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    await receive_queue.put(
        {"type": "http.request", "body": b'{"prompt":"hi"}', "more_body": False}
    )

    proxy_task = asyncio.create_task(app(scope, receive, send))

    await asyncio.wait_for(first_chunk.wait(), timeout=5.0)
    await receive_queue.put({"type": "http.disconnect"})

    await asyncio.wait_for(proxy_task, timeout=5.0)

    assert gate.held == 0

    body_events = [e for e in sent_events if e["type"] == "http.response.body" and e.get("body")]
    assert len(body_events) < 3, "should have stopped streaming after disconnect"


# ---------------------------------------------------------------------------
# Auth passthrough
# ---------------------------------------------------------------------------


async def test_auth_header_passes_through():
    """The client's auth header reaches the upstream unchanged."""
    received: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received.update(dict(request.headers))
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post(
            "/v1/messages",
            json={"prompt": "hi"},
            headers={"Authorization": "Bearer secret-key", "x-api-key": "another-key"},
        )

    assert received.get("authorization") == "Bearer secret-key"
    assert received.get("x-api-key") == "another-key"


# ---------------------------------------------------------------------------
# 429 reporting
# ---------------------------------------------------------------------------


async def test_429_reported_to_reconcile():
    """A 429 from the upstream is reported to the reconciliation loop."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "rate_limit_exceeded"})

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 429
    assert reconcile.total_429s == 1


# ---------------------------------------------------------------------------
# Healthz / metrics
# ---------------------------------------------------------------------------


async def test_healthz():
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_readyz_503_before_first_poll():
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["status"] == "not ready"


async def test_readyz_200_after_successful_tick():
    app, _, reconcile = _make_app()

    await reconcile.tick()

    async with _asgi_client(app) as client:
        response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"


async def test_readyz_stays_503_after_failed_tick():
    """If the first poll fails (ok=False), /readyz stays 503."""
    app, _, reconcile = _make_app()
    reconcile._first_poll_ok = False

    # Simulate a failed tick by directly setting a non-ok cached reading.
    from sluice.usage import CachedReading
    from sluice.control import UsageReading

    reconcile._last_reading_cached = CachedReading(
        reading=UsageReading(concurrent_sessions=8, limit=4, hard_cap=8, priority_low=True),
        fetched_at_monotonic=0.0,
        ok=False,
    )

    async with _asgi_client(app) as client:
        response = await client.get("/readyz")

    assert response.status_code == 503


async def test_metrics():
    app, gate, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    data = response.json()
    assert "in_flight" in data
    assert "effective_permits" in data
    assert "band" in data
    assert "breaker" in data
    assert "ready" in data
    assert "phantom_estimate" in data
    assert "gate_closed_reason" in data


# ---------------------------------------------------------------------------
# Fast-fail when gate is closed (Plan 003 WI-004)
# ---------------------------------------------------------------------------


async def test_fast_fail_on_boxed():
    """When boxed, the proxy returns 503 immediately without waiting for queue_timeout."""
    from sluice.control import UsageReading
    from sluice.usage import CachedReading

    app, _, reconcile = _make_app(queue_timeout=30.0)

    # Simulate a boxed state.
    reconcile._last_reading_cached = CachedReading(
        reading=UsageReading(
            concurrent_sessions=0,
            limit=4,
            hard_cap=8,
            boxed_until_epoch=1e18,  # far future
            resets_at_epoch=1e18,
        ),
        fetched_at_monotonic=0.0,
        ok=True,
    )
    reconcile._last_permits = 0

    async with _asgi_client(app) as client:
        import time

        start = time.monotonic()
        response = await client.post("/v1/messages", json={"prompt": "hi"})
        elapsed = time.monotonic() - start

    assert response.status_code == 503
    assert elapsed < 2.0, "should fast-fail, not wait for queue_timeout"
    assert response.json()["reason"] == "boxed"
    assert response.headers.get("retry-after") is not None


async def test_fast_fail_on_breaker():
    """When breaker is open, the proxy returns 503 immediately."""
    from sluice.control import BreakerState
    from sluice.control import BreakerSnapshot

    app, _, reconcile = _make_app(queue_timeout=30.0)

    # Simulate breaker open.
    reconcile._breaker = BreakerSnapshot(state=BreakerState.OPEN, opened_at=0.0)
    reconcile._last_permits = 0

    async with _asgi_client(app) as client:
        import time

        start = time.monotonic()
        response = await client.post("/v1/messages", json={"prompt": "hi"})
        elapsed = time.monotonic() - start

    assert response.status_code == 503
    assert elapsed < 2.0, "should fast-fail, not wait for queue_timeout"
    assert response.json()["reason"] == "breaker"


async def test_saturated_503_has_reason():
    """When acquire times out (saturated), the 503 carries reason='saturated'."""
    app, _, _ = _make_app(gate_capacity=0, queue_timeout=0.1)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 503
    assert response.json()["reason"] == "saturated"
