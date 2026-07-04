"""Tests for the streaming reverse proxy.

Covers: incremental streaming, 503 on queue timeout, permit release on
completion and disconnect, auth passthrough, 429 reporting.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
from collections.abc import AsyncIterator

import httpx
import pytest

from sluice.control import BreakerConfig, ControllerConfig, UsageReading
from sluice.gate import PermitGate
from sluice.proxy import ProxyApp, _classify_429
from sluice.reconcile import ReconciliationLoop
from sluice.usage import CachedReading


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class FakeUsageClient:
    """Minimal usage client for proxy tests."""

    def __init__(self) -> None:
        self.fetch_count = 0

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        self.fetch_count += 1
        return CachedReading(
            reading=UsageReading(concurrent_sessions=0, limit=4, hard_cap=8),
            fetched_at_monotonic=now_monotonic,
            ok=True,
        )

    @property
    def last_cached(self) -> CachedReading | None:
        return None

    def record_response_headers(self, headers, status, *, now_monotonic) -> None:
        pass

    async def close(self) -> None:
        pass


class _StreamingMockTransport(httpx.AsyncBaseTransport):
    """Mock transport that streams the request body chunk-by-chunk (WI-1).

    Replaces _AsyncMockTransport which called ``await request.aread()``,
    buffering the entire body — so no test exercised ``body_stream()``'s
    backpressure.  This transport consumes ``request.stream`` incrementally,
    exercising the real streaming path.

    Supports configurable delays for race testing (WI-2):

    - ``header_delay``: sleep before calling the handler, simulating upstream
      latency so the ``asyncio.wait`` race (entry_task vs disconnect_task)
      actually races.
    - ``chunk_delay``: sleep before yielding each response chunk, so
      disconnect-during-response-streaming has a real window to fire in.
    """

    def __init__(
        self,
        handler,
        *,
        header_delay: float = 0.0,
        chunk_delay: float = 0.0,
    ):
        self._handler = handler
        self._header_delay = header_delay
        self._chunk_delay = chunk_delay

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        # WI-1: Consume the request body chunk-by-chunk from the stream,
        # NOT via aread() which buffers the entire body.  This exercises
        # body_stream()'s backpressure: if we are slow to consume a chunk,
        # the proxy does not call receive() for the next one.
        body_parts: list[bytes] = []
        async for chunk in request.stream:
            body_parts.append(chunk)
        request._content = b"".join(body_parts)

        if self._header_delay > 0:
            await asyncio.sleep(self._header_delay)

        response = self._handler(request)

        # Apply chunk_delay to response streaming if configured (WI-2).
        if self._chunk_delay > 0:
            return self._wrap_with_chunk_delay(response)

        # Already a proper async stream — pass through.
        if isinstance(response.stream, httpx.AsyncByteStream) and not response.is_stream_consumed:
            return response

        # Re-wrap content as async generator (for bytes content).
        content = response.content if response.is_stream_consumed else b"".join(response.stream)

        async def gen() -> AsyncIterator[bytes]:
            yield content

        headers = [
            (k, v)
            for k, v in response.headers.items()
            if k.lower() not in ("content-length", "transfer-encoding")
        ]
        return httpx.Response(response.status_code, headers=headers, content=gen())

    def _wrap_with_chunk_delay(self, response: httpx.Response) -> httpx.Response:
        original = response.stream
        delay = self._chunk_delay

        if isinstance(original, httpx.AsyncByteStream) and not response.is_stream_consumed:
            async def delayed_async() -> AsyncIterator[bytes]:
                async for chunk in original:
                    await asyncio.sleep(delay)
                    yield chunk
            content_gen: AsyncIterator[bytes] = delayed_async()
        else:
            content = response.content if response.is_stream_consumed else b"".join(original)

            async def delayed_sync() -> AsyncIterator[bytes]:
                await asyncio.sleep(delay)
                yield content
            content_gen = delayed_sync()

        headers = [
            (k, v)
            for k, v in response.headers.items()
            if k.lower() not in ("content-length", "transfer-encoding")
        ]
        return httpx.Response(response.status_code, headers=headers, content=content_gen)


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
    first_poll_ok: bool = True,
    header_delay: float = 0.0,
    chunk_delay: float = 0.0,
) -> tuple[ProxyApp, PermitGate, ReconciliationLoop]:
    gate = PermitGate(initial_capacity=gate_capacity)
    usage = FakeUsageClient()
    upstream_client = httpx.AsyncClient(
        transport=_StreamingMockTransport(
            upstream_handler or _default_handler,
            header_delay=header_delay,
            chunk_delay=chunk_delay,
        ),
        timeout=None,
    )
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = first_poll_ok
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


async def test_admin_basic_auth_stripped_from_proxy_request():
    """Browser-cached Basic auth (admin token) must not leak to upstream.

    After dashboard login, the browser sends Basic auth on all same-origin
    requests.  The proxy must strip the admin credentials before forwarding
    so the upstream never sees sluice's internal auth token (Rule 7).
    """
    received: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received.update(dict(request.headers))
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)
    app._admin_token = "admin-secret"

    basic = "Basic " + base64.b64encode(b"user:admin-secret").decode()

    async with _asgi_client(app) as client:
        await client.post(
            "/v1/messages",
            json={"prompt": "hi"},
            headers={"Authorization": basic},
        )

    assert "authorization" not in received, "admin Basic auth must not reach upstream"


async def test_provider_bearer_auth_preserved_when_admin_token_set():
    """The client's own upstream Bearer token is forwarded even when admin_token is set."""
    received: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received.update(dict(request.headers))
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)
    app._admin_token = "admin-secret"

    async with _asgi_client(app) as client:
        await client.post(
            "/v1/messages",
            json={"prompt": "hi"},
            headers={"Authorization": "Bearer sk-provider-key"},
        )

    assert received.get("authorization") == "Bearer sk-provider-key"


async def test_admin_bearer_auth_stripped_from_proxy_request():
    """Admin Bearer token is stripped from proxied requests (same as Basic)."""
    received: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received.update(dict(request.headers))
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)
    app._admin_token = "admin-secret"

    async with _asgi_client(app) as client:
        await client.post(
            "/v1/messages",
            json={"prompt": "hi"},
            headers={"Authorization": "Bearer admin-secret"},
        )

    assert "authorization" not in received


async def test_basic_auth_case_insensitive_scheme():
    """Basic auth with lowercase scheme is accepted (RFC 7235)."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.get(
            "/status.json",
            headers={"Authorization": "basic " + base64.b64encode(b"user:secret").decode()},
        )

    assert response.status_code == 200


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


async def test_429_with_retry_after_not_recorded_as_concurrency():
    """A 429 with a non-zero retry-after is classified as rate_limit, not concurrency.

    Rate-limit 429s are tracked in a separate counter (``rate_limit_429s``)
    but still feed the breaker — the retry-after heuristic is unreliable
    (capture 2026-07-03: umans sends retry_after=1 on concurrency 429s).
    A single rate-limit 429 must not trip the breaker (threshold=5), but
    it counts toward ``recent_429s``.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "rate_limit"}, headers={"retry-after": "60"})

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 429
    assert reconcile.total_429s == 0, "rate-limit 429 must not increment concurrency counter"
    assert reconcile.rate_limit_429s == 1, "rate-limit 429 must be tracked separately"
    assert reconcile.recent_429_count == 1, "rate-limit 429 must feed the breaker window"
    assert reconcile.breaker_state.value == "closed", "single rate-limit 429 must not trip breaker"


async def test_429_with_zero_retry_after_recorded_as_concurrency():
    """A 429 with retry-after:0 is classified as concurrency (retry immediately).

    The string "0" is truthy in Python, so a naive ``not header`` check would
    silently skip breaker recording (fail-open).  _classify_429 handles
    this edge case.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "concurrency"}, headers={"retry-after": "0"})

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 429
    assert reconcile.total_429s == 1, "retry-after=0 is a concurrency 429 (retry immediately)"


async def test_429_monitoring_hook_logs_all_429s(caplog):
    """Every 429 is logged with its retry-after value and concurrency classification.

    This is the monitoring hook that catches a regression: if umans starts
    sending non-zero retry-after on concurrency 429s, the log would show
    concurrency=False on what are actually concurrency rejections.
    """
    import logging as logging_mod

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "rate_limit"}, headers={"retry-after": "30"})

    app, _, _ = _make_app(upstream_handler=handler)

    with caplog.at_level(logging_mod.WARNING, logger="sluice.proxy"):
        async with _asgi_client(app) as client:
            await client.post("/v1/messages", json={"prompt": "hi"})

    # The log must mention retry_after and concurrency classification
    assert any("retry_after" in r.message and "classification" in r.message for r in caplog.records), (
        "every 429 must be logged with retry_after value and classification"
    )


# ---------------------------------------------------------------------------
# Gateway/CDN 429 classification (WI-024)
# ---------------------------------------------------------------------------


def test_classify_429_no_retry_after_no_cdn_headers_is_concurrency():
    """A 429 with no retry-after and no CDN headers is concurrency (fail-safe)."""
    assert _classify_429(None, {}) == "concurrency"
    assert _classify_429("0", {}) == "concurrency"
    assert _classify_429("", {}) == "concurrency"
    assert _classify_429("abc", {}) == "concurrency"


def test_classify_429_http_date_retry_after_is_rate_limit():
    """WI-031: HTTP-date retry-after is classified as rate_limit, not concurrency.

    RFC 7231 §7.1.3 allows retry-after to be either delta-seconds or an
    HTTP-date. An HTTP-date means "retry at this specific time" — a rate-limit
    window, not a concurrency rejection. Both classifications feed the breaker
    equally; the distinction is for telemetry accuracy only.
    """
    assert _classify_429("Wed, 21 Oct 2025 07:28:00 GMT", {}) == "rate_limit"
    assert _classify_429("Fri, 04 Jul 2026 12:00:00 GMT", {}) == "rate_limit"
    assert _classify_429("  Wed, 21 Oct 2025 07:28:00 GMT  ", {}) == "rate_limit"


def test_classify_429_garbage_non_date_non_integer_is_concurrency():
    """Truly unparseable values (not integer, not HTTP-date) default to concurrency (fail-safe)."""
    assert _classify_429("not-a-date", {}) == "concurrency"
    assert _classify_429("abc def ghi", {}) == "concurrency"
    assert _classify_429("123abc", {}) == "concurrency"


def test_classify_429_http_date_with_cdn_still_gateway():
    """HTTP-date retry-after with CDN headers is still gateway (CDN check runs first)."""
    assert _classify_429("Wed, 21 Oct 2025 07:28:00 GMT", {"cf-ray": "x"}) == "gateway"


def test_classify_429_retry_after_positive_is_rate_limit():
    """A 429 with a positive retry-after and no CDN headers is rate_limit."""
    assert _classify_429("60", {}) == "rate_limit"
    assert _classify_429("5", {}) == "rate_limit"


def test_classify_429_with_cdn_header_is_gateway():
    """A 429 with a known CDN header is classified as gateway regardless of retry-after."""
    assert _classify_429(None, {"cf-ray": "abc123"}) == "gateway"
    assert _classify_429("0", {"cf-ray": "abc123"}) == "gateway"
    assert _classify_429("60", {"cf-ray": "abc123"}) == "gateway"
    assert _classify_429(None, {"x-amz-cf-id": "xyz"}) == "gateway"
    assert _classify_429(None, {"x-served-by": "cache-lhr"}) == "gateway"


def test_classify_429_with_cdn_server_header_is_gateway():
    """A 429 with server: cloudflare is classified as gateway."""
    assert _classify_429(None, {"server": "cloudflare"}) == "gateway"
    assert _classify_429(None, {"server": "CloudFlare"}) == "gateway"
    assert _classify_429("60", {"server": "cloudflare"}) == "gateway"


def test_classify_429_non_cdn_server_is_not_gateway():
    """A non-CDN server header does not trigger gateway classification."""
    assert _classify_429(None, {"server": "uvicorn"}) == "concurrency"
    assert _classify_429(None, {"server": "gunicorn"}) == "concurrency"


async def test_gateway_429_not_fed_to_breaker():
    """A 429 with CDN headers does not trip the breaker (WI-024)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "cdn"}, headers={"cf-ray": "abc123"})

    app, _, reconcile = _make_app(upstream_handler=handler)
    reconcile._brk_cfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)

    async with _asgi_client(app) as client:
        for _ in range(5):  # well above threshold
            await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 0, "CDN 429s must not be recorded as concurrency"
    assert reconcile.gateway_429s == 5
    assert reconcile.breaker_state.value == "closed", "breaker must not trip on CDN 429s"


async def test_gateway_429_tracked_separately():
    """Gateway 429s are counted in a separate counter, visible in /status.json."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "cdn"}, headers={"server": "cloudflare"})

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})
        await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.gateway_429s == 2
    assert reconcile.total_429s == 0

    async with _asgi_client(app) as client:
        response = await client.get("/status.json")

    data = response.json()
    assert data["gateway_429s"] == 2
    assert data["total_429s"] == 0


async def test_gateway_429_in_prometheus():
    """Gateway 429s appear in /metrics as sluice_gateway_429s."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "cdn"}, headers={"cf-ray": "x"})

    app, _, _ = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})
        response = await client.get("/metrics")

    assert "sluice_gateway_429s" in response.text


async def test_concurrency_429_still_trips_breaker():
    """A 429 without CDN headers still feeds the breaker (regression test)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "concurrency"})

    app, _, reconcile = _make_app(upstream_handler=handler)
    reconcile._brk_cfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)

    async with _asgi_client(app) as client:
        for _ in range(3):
            await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 3
    assert reconcile.gateway_429s == 0


async def test_rate_limit_429_trips_breaker_when_sustained():
    """Sustained rate-limit 429s trip the breaker (capture 2026-07-03 fix).

    The retry-after heuristic is unreliable — umans sends retry_after=1 on
    concurrency 429s.  Rate-limit 429s must feed the breaker so sustained
    enforcement trips it, even if a single event doesn't.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "overloaded"}, headers={"retry-after": "1"})

    app, _, reconcile = _make_app(upstream_handler=handler)
    reconcile._brk_cfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)

    async with _asgi_client(app) as client:
        for _ in range(3):
            await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.rate_limit_429s == 3
    assert reconcile.total_429s == 0, "rate-limit 429s must not increment concurrency counter"
    assert reconcile.recent_429_count == 3
    assert reconcile.breaker_state.value == "open", "sustained rate-limit 429s must trip the breaker"


async def test_rate_limit_429_in_status_json():
    """Rate-limit 429s are visible in /status.json."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "rate_limit"}, headers={"retry-after": "30"})

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})
        response = await client.get("/status.json")

    data = response.json()
    assert data["rate_limit_429s"] == 1
    assert data["total_429s"] == 0


async def test_rate_limit_429_in_prometheus():
    """Rate-limit 429s appear in /metrics as sluice_rate_limit_429s."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "rate_limit"}, headers={"retry-after": "30"})

    app, _, _ = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})
        response = await client.get("/metrics")

    assert "sluice_rate_limit_429s" in response.text


async def test_mixed_concurrency_and_rate_limit_429s_trip_breaker():
    """Both concurrency and rate_limit 429s share the breaker window.

    2 concurrency 429s + 3 rate_limit 429s = 5 entries in recent_429s,
    which should trip the breaker (threshold=5).  The counters stay separate
    but the breaker window is shared.
    """
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        # First 2: no retry-after (concurrency), next 3: retry-after=1 (rate_limit)
        if call_count <= 2:
            return _resp(429, json_data={"error": "concurrency"})
        return _resp(429, json_data={"error": "overloaded"}, headers={"retry-after": "1"})

    app, _, reconcile = _make_app(upstream_handler=handler)
    reconcile._brk_cfg = BreakerConfig(threshold=5, window_seconds=300.0, cooldown_seconds=60.0)

    async with _asgi_client(app) as client:
        for _ in range(5):
            await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 2, "concurrency counter"
    assert reconcile.rate_limit_429s == 3, "rate_limit counter"
    assert reconcile.recent_429_count == 5, "shared breaker window"
    assert reconcile.breaker_state.value == "open", "mixed 429s must trip the breaker"


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
    app, _, reconcile = _make_app(first_poll_ok=False)
    assert reconcile._first_poll_ok is False

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
    app, _, reconcile = _make_app(first_poll_ok=False)

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
        response = await client.get("/status.json")

    assert response.status_code == 200
    data = response.json()
    assert "effective_permits" in data
    assert "band" in data
    assert "breaker" in data
    assert "ready" in data
    assert "phantom_estimate" in data
    assert "gate_closed_reason" in data
    assert "local_in_flight" in data
    assert "breaker_half_open_age_seconds" in data


async def test_status_json_reports_version_and_build(monkeypatch):
    """/status.json carries the package version and the image's git sha.

    The sha comes from SLUICE_BUILD_SHA (baked into the image via the release
    workflow's GIT_SHA build-arg); absent that env var, build is null rather
    than a fabricated value.
    """
    from sluice import __version__

    monkeypatch.setenv("SLUICE_BUILD_SHA", "abc1234")
    app, _, _ = _make_app()
    async with _asgi_client(app) as client:
        response = await client.get("/status.json")
    data = response.json()
    assert data["version"] == __version__
    assert data["build"] == "abc1234"

    monkeypatch.delenv("SLUICE_BUILD_SHA")
    app2, _, _ = _make_app()
    async with _asgi_client(app2) as client:
        response = await client.get("/status.json")
    assert response.json()["build"] is None


async def test_prometheus_metrics():
    """GET /metrics returns OpenMetrics text exposition."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    text = response.text
    assert "# HELP" in text
    assert "# TYPE" in text
    assert "sluice_in_flight" in text
    assert "sluice_effective_permits" in text
    assert "sluice_band" in text
    assert "sluice_breaker_half_open_age_seconds" in text


async def test_dashboard():
    """GET / returns the dashboard HTML page."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "sluice" in response.text.lower()
    assert "/static/css/tokens.css" in response.text
    assert "/static/theme.js" in response.text


async def test_dashboard_renders_half_open_age():
    """WI-021: dashboard JS includes logic to render breaker_half_open_age_seconds.

    Verifies both that /status.json carries a non-null age when the breaker is
    HALF_OPEN, and that the dashboard's inline JS has a conditional that checks
    for half_open state, references breaker_half_open_age_seconds within that
    conditional, and uses the age value in the banner text construction.
    """
    import time as time_mod
    from sluice.control import BreakerSnapshot, BreakerState

    app, _, reconcile = _make_app()
    reconcile._breaker = BreakerSnapshot(
        state=BreakerState.HALF_OPEN,
        opened_at=0.0,
        half_opened_at=time_mod.monotonic() - 5.0,
    )

    async with _asgi_client(app) as client:
        status_resp = await client.get("/status.json")

    status_data = status_resp.json()
    assert status_data["breaker"] == "half_open"
    assert status_data["breaker_half_open_age_seconds"] is not None
    assert isinstance(status_data["breaker_half_open_age_seconds"], (int, float))

    async with _asgi_client(app) as client:
        dash_resp = await client.get("/")

    html = dash_resp.text

    assert re.search(r"if\s*\([^)]*['\"]half_open['\"]", html), (
        "JS must have a conditional checking for half_open breaker state"
    )
    assert re.search(
        r"['\"]half_open['\"].*?breaker_half_open_age_seconds", html, re.DOTALL
    ), "half_open conditional must reference breaker_half_open_age_seconds"
    assert re.search(r"breaker_half_open_age_seconds\s*\+\s*['\"]s", html), (
        "banner text must include breaker_half_open_age_seconds value in construction"
    )


async def test_dashboard_sparkline_depth_elements():
    """Plan 009 + Fable additions: the sparkline card carries the queue spark,
    band ribbon, time-horizon toggle, effective-permits step line, limit/hard-cap
    guide lines, breaker/stale tick marks, and the hover crosshair tooltip.

    Static-content assertions against the inline dashboard (same approach as
    the half-open-age test): the HTML must contain the three range buttons
    wired to setRange, the ribbon and queue-spark SVGs, the long-range fetch
    limits (720 = 1h, 2880 = 4h at the 5s tick cadence), and the bucketing +
    event-tick code paths.
    """
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/")

    html = response.text
    for element_id in ('id="r-5m"', 'id="r-1h"', 'id="r-4h"', 'id="ribbon"', 'id="qspark"'):
        assert element_id in html
    assert "setRange" in html
    assert "'1h':{limit:720}" in html
    assert "'4h':{limit:2880}" in html
    assert "bucketize" in html
    assert "tick-429" in html and "tick-qt" in html
    # Live buffer must now carry the queue/band/counter fields the new
    # surfaces render from, plus the fields surfaced by the hover tooltip.
    assert "qd:d.queue_depth" in html
    assert "t429:d.total_429s" in html
    # Effective-permits line and guide lines.
    assert "stepPts('ep')" in html
    assert "spark-ep" in html
    assert "spark-lim" in html
    assert "spark-hc" in html
    # Breaker / stale ticks.
    assert "tick-brk-open" in html
    assert "tick-brk-half" in html
    assert "tick-stale" in html
    # Hover crosshair + tooltip container.
    assert "spark-tip" in html
    assert "onSparkHover" in html
    assert "crosshair" in html


async def test_dashboard_layout_and_help_titles():
    """Full-width sparkline layout + hover explanations.

    The sparkline card sits in its own row (its height is no longer coupled to
    the Reading table), Reading and Config share the row below it, and the
    legend / Reading / Config entries carry title-attribute explanations with
    the .help affordance.
    """
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/")

    html = response.text
    # Order: gauge, then spark card, then Reading, then Config.
    gauge_at = html.index("Concurrency Gauge")
    spark_at = html.index('id="spark-card"')
    reading_at = html.index("<h2>Reading</h2>")
    config_at = html.index("<h2>Config</h2>")
    assert gauge_at < spark_at < reading_at < config_at
    # Reading and Config share a row: no row boundary between them.
    assert '<div class="row">' not in html[reading_at:config_at]
    # The spark card's row closes before Reading opens (spark is alone in it).
    assert '<div class="row">' in html[spark_at:reading_at]
    # Taller main spark: 120-unit viewBox and matching crosshair extent.
    assert 'viewBox="0 0 200 120"' in html
    # Legend hover explanations.
    assert 'title="concurrent_sessions from the provider usage endpoint' in html
    assert 'title="effective_permits - the controller ceiling' in html
    # Reading/Config rows render title attributes via the shared kvRow helper.
    assert "kvRow" in html
    assert 'class="help" title=' in html
    assert "'Requests currently waiting for a permit'" in html
    # Config now surfaces provider/controller (present in the payload all along).
    assert "['provider',c.provider," in html
    assert "['controller',c.controller," in html


async def test_static_css_served():
    """GET /static/css/tokens.css serves the vendored patina tokens."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/static/css/tokens.css")

    assert response.status_code == 200
    assert "text/css" in response.headers.get("content-type", "")
    assert "--accent" in response.text


async def test_static_theme_js_served():
    """GET /static/theme.js serves the patina theme toggle."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/static/theme.js")

    assert response.status_code == 200
    assert "javascript" in response.headers.get("content-type", "")
    assert "data-theme" in response.text


async def test_static_path_traversal_blocked():
    """Path traversal attempts on /static/ are blocked (404)."""
    app, _, _ = _make_app()

    # Use a raw ASGI scope because httpx normalizes ../ in the URL path.
    sent_events: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.disconnect"}

    async def send(event: dict) -> None:
        sent_events.append(event)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/static/css/../../../../../etc/passwd",
        "query_string": b"",
        "headers": [],
    }

    await app(scope, receive, send)

    start = [e for e in sent_events if e["type"] == "http.response.start"]
    assert start[0]["status"] == 404


async def test_admin_token_unauthorized():
    """Admin routes return 401 when admin_token is set and not provided."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.get("/status.json")

    assert response.status_code == 401


async def test_admin_token_authorized():
    """Admin routes return 200 when admin_token is set and correct bearer is provided."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.get(
            "/status.json",
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 200


async def test_admin_token_not_required_for_healthz():
    """Health and readiness are not gated by admin_token."""
    app, _, reconcile = _make_app(first_poll_ok=False)
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        health = await client.get("/healthz")
        ready = await client.get("/readyz")

    assert health.status_code == 200
    assert ready.status_code == 503  # not ready before first poll


async def test_dashboard_requires_auth_when_token_set():
    """Unauthenticated GET / serves the login page (200, no challenge header)."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert response.headers.get("www-authenticate") is None
    assert "password" in response.text


async def test_dashboard_works_with_basic_auth():
    """The dashboard is served when Basic auth password matches admin_token."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.get(
            "/",
            headers={"Authorization": "Basic " + base64.b64encode(b"anyuser:secret").decode()},
        )

    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "sluice" in response.text.lower()


async def test_status_json_works_with_basic_auth():
    """Status endpoint accepts Basic auth as well as Bearer."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.get(
            "/status.json",
            headers={"Authorization": "Basic " + base64.b64encode(b"user:secret").decode()},
        )

    assert response.status_code == 200


async def test_basic_auth_rejects_wrong_password():
    """Basic auth with wrong password returns 401."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.get(
            "/status.json",
            headers={"Authorization": "Basic " + base64.b64encode(b"user:wrong").decode()},
        )

    assert response.status_code == 401


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


# ---------------------------------------------------------------------------
# Cache-transparency: wire-indistinguishable from a direct client (Plan 005 WI-000)
# ---------------------------------------------------------------------------


async def test_body_byte_identity():
    """The bytes forwarded upstream equal the bytes the client sent — exactly.

    Uses a body whose re-serialisation would differ (non-sorted keys, specific
    spacing) so the test actually bites: if sluice parsed and re-serialised the
    body, the upstream would receive different bytes and the cache key would change.
    """
    received_body = bytearray()

    def handler(request: httpx.Request) -> httpx.Response:
        # Capture the raw bytes the upstream received.
        received_body.extend(request.content)
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)

    # Deliberately non-sorted keys with specific spacing.
    raw_body = b'{"z": 1, "a": 2, "model": "claude-3"}'

    async with _asgi_client(app) as client:
        await client.post(
            "/v1/messages",
            content=raw_body,
            headers={"content-type": "application/json"},
        )

    assert bytes(received_body) == raw_body, (
        "sluice must forward body bytes exactly as received — "
        "re-serialisation changes the upstream's cache key"
    )


async def test_header_passthrough_unchanged():
    """anthropic-*, authorization, x-api-key, content-type, arbitrary headers reach upstream."""
    received: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received.update(dict(request.headers))
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post(
            "/v1/messages",
            json={"prompt": "hi"},
            headers={
                "Authorization": "Bearer secret-key",
                "x-api-key": "another-key",
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "prompt-caching-2024-07-31",
                "x-custom-header": "custom-value",
                "content-type": "application/json",
            },
        )

    assert received.get("authorization") == "Bearer secret-key"
    assert received.get("x-api-key") == "another-key"
    assert received.get("anthropic-version") == "2023-06-01"
    assert received.get("anthropic-beta") == "prompt-caching-2024-07-31"
    assert received.get("x-custom-header") == "custom-value"


async def test_sluice_control_headers_stripped():
    """sluice-internal control headers are stripped before forwarding upstream."""
    received: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received.update(dict(request.headers))
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post(
            "/v1/messages",
            json={"prompt": "hi"},
            headers={
                "x-sluice-client-label": "interactive",
                "x-sluice-qos": "high",
                "x-sluice-unknown-future-header": "value",
                "X-Sluice-Mixed-Case": "test",
                "Authorization": "Bearer secret-key",
            },
        )

    # ALL x-sluice-* headers must not reach upstream (prefix match, case-insensitive).
    assert "x-sluice-client-label" not in received
    assert "x-sluice-qos" not in received
    assert "x-sluice-unknown-future-header" not in received
    assert "x-sluice-mixed-case" not in received
    # But auth passes through.
    assert received.get("authorization") == "Bearer secret-key"


# ---------------------------------------------------------------------------
# Singleton guard integration (Plan 004)
# ---------------------------------------------------------------------------


class _FakeGuard:
    """Test guard with controllable is_held()."""

    def __init__(self, held: bool = True, acquire_result: bool = True) -> None:
        self._held = held
        self._acquire_result = acquire_result
        self.acquire_called = False
        self.release_called = False
        self.renewer_started = False

    async def acquire(self) -> bool:
        self.acquire_called = True
        self._held = self._acquire_result
        return self._acquire_result

    async def renew(self) -> bool:
        return self._held

    def is_held(self) -> bool:
        return self._held

    async def release(self) -> None:
        self.release_called = True
        self._held = False

    async def start_renewer(self) -> None:
        self.renewer_started = True

    async def stop_renewer(self) -> None:
        pass


def _make_app_with_guard(
    guard,
    *,
    gate_capacity: int = 3,
    upstream_handler=None,
    first_poll_ok: bool = True,
) -> tuple[ProxyApp, PermitGate, ReconciliationLoop]:
    gate = PermitGate(initial_capacity=gate_capacity)
    usage = FakeUsageClient()
    upstream_client = httpx.AsyncClient(
        transport=_StreamingMockTransport(upstream_handler or _default_handler),
        timeout=None,
    )
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
        guard=guard,  # type: ignore[arg-type]
    )
    reconcile._first_poll_ok = first_poll_ok
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        upstream_client=upstream_client,
        guard=guard,  # type: ignore[arg-type]
    )
    return app, gate, reconcile


async def test_non_leader_fast_fails_503():
    """A non-leader proxy returns 503 not_leader immediately."""
    guard = _FakeGuard(held=False, acquire_result=False)
    app, _, _ = _make_app_with_guard(guard)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 503
    assert response.json()["reason"] == "not_leader"


async def test_leader_serves_normally():
    """A leader proxy serves requests normally."""
    guard = _FakeGuard(held=True, acquire_result=True)
    app, _, _ = _make_app_with_guard(guard)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 200


async def test_readyz_requires_guard_held():
    """/readyz is 503 when guard is not held, even if reconcile is ready."""
    guard = _FakeGuard(held=True, acquire_result=True)
    app, _, reconcile = _make_app_with_guard(guard)

    # Make reconcile ready.
    await reconcile.tick()
    assert reconcile.ready is True

    # Guard held → ready.
    async with _asgi_client(app) as client:
        response = await client.get("/readyz")
    assert response.status_code == 200

    # Guard lost → not ready.
    guard._held = False
    async with _asgi_client(app) as client:
        response = await client.get("/readyz")
    assert response.status_code == 503


async def test_healthz_unaffected_by_guard():
    """/healthz returns 200 regardless of guard state."""
    guard = _FakeGuard(held=False, acquire_result=False)
    app, _, _ = _make_app_with_guard(guard)

    async with _asgi_client(app) as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_non_leader_does_not_poll_usage():
    """A non-leader reconcile loop does not poll /v1/usage."""
    guard = _FakeGuard(held=False, acquire_result=False)
    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
        guard=guard,  # type: ignore[arg-type]
    )

    await reconcile.tick()

    assert usage.fetch_count == 0
    assert gate.capacity == 0  # gate closed


# ---------------------------------------------------------------------------
# WI-004: Retry lease acquisition after failed startup
# ---------------------------------------------------------------------------


class _FakeGuardRetryable:
    """Guard that fails the first acquire, succeeds on subsequent calls.

    Set ``_start_renewer_fail_count`` to make ``start_renewer`` raise that many
    times before succeeding — used to test the zombie-leader prevention path.
    """

    def __init__(self) -> None:
        self._held = False
        self._acquire_count = 0
        self.renewer_started = False
        self._start_renewer_fail_count = 0
        self._release_count = 0

    async def acquire(self) -> bool:
        self._acquire_count += 1
        if self._acquire_count == 1:
            return False
        self._held = True
        return True

    async def renew(self) -> bool:
        return self._held

    def is_held(self) -> bool:
        return self._held

    async def release(self) -> None:
        self._held = False
        self._release_count += 1

    async def start_renewer(self) -> None:
        if self._start_renewer_fail_count > 0:
            self._start_renewer_fail_count -= 1
            raise RuntimeError("simulated start_renewer failure")
        self.renewer_started = True

    async def stop_renewer(self) -> None:
        pass


async def test_retry_acquire_after_failed_startup():
    """When initial acquire fails, the proxy retries and becomes leader."""
    guard = _FakeGuardRetryable()
    app, _, reconcile = _make_app_with_guard(guard)
    app._lifecycle._retry_interval = 0.01

    app._lifecycle._retry_task = asyncio.create_task(app._lifecycle._retry_acquire())

    await asyncio.sleep(0.2)

    assert app._lifecycle.acquired is True
    assert guard.renewer_started is True
    assert reconcile._task is not None

    await reconcile.stop()
    if app._lifecycle._retry_task and not app._lifecycle._retry_task.done():
        app._lifecycle._retry_task.cancel()
        try:
            await app._lifecycle._retry_task
        except asyncio.CancelledError:
            pass


async def test_retry_acquire_cancelled_on_shutdown():
    """The retry task is properly cancelled during shutdown."""
    guard = _FakeGuard(held=False, acquire_result=False)
    app, _, _ = _make_app_with_guard(guard)
    app._lifecycle._retry_interval = 0.01

    app._lifecycle._retry_task = asyncio.create_task(app._lifecycle._retry_acquire())

    await asyncio.sleep(0.05)

    # Simulate shutdown cancelling the task.
    assert app._lifecycle._retry_task is not None
    app._lifecycle._retry_task.cancel()
    try:
        await app._lifecycle._retry_task
    except asyncio.CancelledError:
        pass

    assert app._lifecycle._retry_task.cancelled()
    assert app._lifecycle.acquired is False


async def test_retry_acquire_releases_lease_if_start_fails():
    """If start_renewer raises after acquire succeeds, the lease is released and retry continues.

    Without this, the proxy would hold the lease but have no renewer or reconcile
    running — a zombie leader that serves traffic without usage polling.
    """
    guard = _FakeGuardRetryable()
    guard._start_renewer_fail_count = 1  # fail first start_renewer call
    app, _, reconcile = _make_app_with_guard(guard)
    app._lifecycle._retry_interval = 0.01

    app._lifecycle._retry_task = asyncio.create_task(app._lifecycle._retry_acquire())

    await asyncio.sleep(0.3)

    assert app._lifecycle.acquired is True
    assert guard.renewer_started is True
    assert reconcile._task is not None
    assert guard._release_count >= 1  # lease was released after the failed start

    await reconcile.stop()
    if app._lifecycle._retry_task and not app._lifecycle._retry_task.done():
        app._lifecycle._retry_task.cancel()
        try:
            await app._lifecycle._retry_task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# Multi-chunk request body (queueless direct-streaming)
# ---------------------------------------------------------------------------


async def test_large_multi_chunk_body_proxied():
    """A large multi-chunk request body is proxied correctly.

    body_stream() reads from receive() directly and yields to httpx, applying
    natural backpressure.  This test verifies that a body sent in multiple
    chunks is still proxied byte-for-byte.
    """
    chunks = [b"x" * 10000 for _ in range(10)]
    expected_body = b"".join(chunks)

    received_body = bytearray()

    def handler(request: httpx.Request) -> httpx.Response:
        received_body.extend(request.content)
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

    for chunk in chunks:
        await receive_queue.put({"type": "http.request", "body": chunk, "more_body": True})
    await receive_queue.put({"type": "http.request", "body": b"", "more_body": False})

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    await app(scope, receive, send)

    assert bytes(received_body) == expected_body
    status_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(status_events) == 1
    assert status_events[0]["status"] == 200


# ---------------------------------------------------------------------------
# WI-013: No double http.response.start when upstream drops mid-stream
# ---------------------------------------------------------------------------


async def test_no_double_response_start_on_mid_stream_drop():
    """If the upstream drops after sending headers, we must not send a second
    http.response.start (ASGI protocol violation).

    The proxy should send exactly one http.response.start, then stream what it
    can, and stop — no 502 after the response has already started.
    """

    async def dropping_gen():
        yield b"chunk1\n"
        raise httpx.ConnectError("upstream dropped mid-stream")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=dropping_gen(),
            headers={"content-type": "text/event-stream"},
        )

    app, _, _ = _make_app(upstream_handler=handler)

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

    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 1, "must not send a second http.response.start"


# ---------------------------------------------------------------------------
# WI-014: Upstream request cancelled on client disconnect during body upload
# ---------------------------------------------------------------------------


async def test_upstream_cancelled_on_disconnect_during_body_upload():
    """When the client disconnects during body upload, the upstream request
    should be cancelled (the stream context exited) rather than running to
    completion as a phantom.

    The key observable: the proxy does NOT send http.response.start to a
    client that has already disconnected.  Before the fix, the proxy would
    enter the stream context, send http.response.start, and only then notice
    the disconnect — leaving the response in a half-sent state.
    """
    upstream_responded = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        upstream_responded.set()
        return _resp(200)

    app, gate, _ = _make_app(upstream_handler=handler)

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    # Start sending body (more_body=True) then disconnect before body completes
    await receive_queue.put({"type": "http.request", "body": b"part1", "more_body": True})
    await receive_queue.put({"type": "http.disconnect"})

    proxy_task = asyncio.create_task(app(scope, receive, send))

    # Wait for the proxy to finish (should be quick — disconnect detected)
    await asyncio.wait_for(proxy_task, timeout=5.0)

    # The proxy should not have sent a response (disconnect before headers)
    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 0, "should not send response after disconnect"

    # Permit should be released
    assert gate.held == 0


async def test_upstream_cancelled_on_disconnect_while_waiting_for_headers():
    """When the client disconnects after body upload completes but while
    waiting for upstream response headers, the upstream request should be
    cancelled (not left running as a phantom).

    This is the core phantom-prevention scenario: body is complete,
    disconnect_watcher takes over receive(), detects disconnect, and
    the racing logic cancels the stream entry task.
    """
    handler_called = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        handler_called.set()
        # Return a slow-streaming response to simulate waiting for headers.
        async def slow_gen():
            await asyncio.sleep(10.0)
            yield b"done"

        return httpx.Response(
            200,
            content=slow_gen(),
            headers={"content-type": "text/event-stream"},
        )

    app, gate, _ = _make_app(upstream_handler=handler)

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    # Send complete body, then disconnect while waiting for upstream
    await receive_queue.put(
        {"type": "http.request", "body": b'{"prompt":"hi"}', "more_body": False}
    )
    # Wait a moment for body_stream to complete and disconnect_watcher to start
    await asyncio.sleep(0.05)
    await receive_queue.put({"type": "http.disconnect"})

    proxy_task = asyncio.create_task(app(scope, receive, send))

    # Should complete quickly — disconnect detected
    await asyncio.wait_for(proxy_task, timeout=5.0)

    # No response should have been sent to the disconnected client
    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 0, "should not send response after disconnect"

    # Permit should be released
    assert gate.held == 0


# ---------------------------------------------------------------------------
# WI-018: Proxy fast-fails 503 when not ready (before first usage poll)
# ---------------------------------------------------------------------------


async def test_fast_fail_when_not_ready():
    """Before the first successful usage poll, the proxy returns 503 not_ready."""
    app, _, _ = _make_app(first_poll_ok=False)

    async with _asgi_client(app) as client:
        import time

        start = time.monotonic()
        response = await client.post("/v1/messages", json={"prompt": "hi"})
        elapsed = time.monotonic() - start

    assert response.status_code == 503
    assert response.json()["reason"] == "not_ready"
    assert elapsed < 2.0, "should fast-fail, not wait for queue_timeout"


# ---------------------------------------------------------------------------
# WI-019: Only concurrency 429s (no retry-after) are recorded
# ---------------------------------------------------------------------------


# --- unit tests for the classifier ------------------------------------------


# --- integration tests through the proxy ------------------------------------


async def test_concurrency_429_without_retry_after_is_recorded():
    """A 429 without retry-after (concurrency rejection) is recorded."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "overloaded"})

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 1


async def test_rate_limit_429_with_retry_after_is_not_recorded():
    """A 429 with retry-after (rate-limit) is tracked separately but still feeds the breaker."""

    def handler(request: httpx.Request) -> httpx.Response:
        resp = _resp(429, json_data={"error": "rate_limit_exceeded"})
        resp.headers["retry-after"] = "60"
        return resp

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 0, "rate-limit 429s must not increment concurrency counter"
    assert reconcile.rate_limit_429s == 1, "rate-limit 429s must be tracked separately"
    assert reconcile.recent_429_count == 1, "rate-limit 429s must feed the breaker window"


@pytest.mark.parametrize("retry_after", ["0", "00", " 0 ", " 0", "0 "])
async def test_429_with_retry_after_zero_variants_is_recorded(retry_after):
    """A 429 with retry-after: 0 (any canonical form) is a concurrency signal.

    retry-after: 0 means "retry immediately," which is a concurrency rejection,
    not a rate-limit window.  The string "0" is truthy in Python, so a naive
    ``not header`` check would silently skip the breaker — fail-open.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        resp = _resp(429, json_data={"error": "overloaded"})
        resp.headers["retry-after"] = retry_after
        return resp

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 1, f"retry-after: {retry_after!r} must trip the breaker"


@pytest.mark.parametrize("retry_after", ["", "abc", "-1"])
async def test_429_with_unparseable_retry_after_is_recorded(retry_after):
    """A 429 with an unparseable retry-after is treated as concurrency (fail safe).

    Per AGENTS.md rule 1, any uncertainty must tighten the gate.  An unparseable
    retry-after cannot be classified as a rate-limit window, so it trips the breaker.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        resp = _resp(429, json_data={"error": "overloaded"})
        resp.headers["retry-after"] = retry_after
        return resp

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 1, f"unparseable retry-after: {retry_after!r} must trip the breaker (fail safe)"


async def test_429_retry_after_forwarded_downstream():
    """The retry-after header is forwarded to the client unchanged (cache-transparency)."""

    def handler(request: httpx.Request) -> httpx.Response:
        resp = _resp(429, json_data={"error": "overloaded"})
        resp.headers["retry-after"] = "0"
        return resp

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 429
    assert response.headers.get("retry-after") == "0"
    assert reconcile.total_429s == 1


async def test_429_with_retry_after_zero_on_chat_completions_is_recorded():
    """Both surfaces are identically gated (rule 5) — chat/completions trips too."""

    def handler(request: httpx.Request) -> httpx.Response:
        resp = _resp(429, json_data={"error": "overloaded"})
        resp.headers["retry-after"] = "0"
        return resp

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/chat/completions", json={"messages": []})

    assert reconcile.total_429s == 1, "chat/completions must trip the breaker identically"


# ---------------------------------------------------------------------------
# Plan 005 WI-002: Reserved floor (proxy-level)
# ---------------------------------------------------------------------------


def _make_app_with_reserve(
    *,
    gate_capacity: int = 3,
    reserve: int = 1,
    queue_timeout: float = 30.0,
    upstream_handler=None,
    first_poll_ok: bool = True,
) -> tuple[ProxyApp, PermitGate, ReconciliationLoop]:
    gate = PermitGate(initial_capacity=gate_capacity, reserve=reserve)
    usage = FakeUsageClient()
    upstream_client = httpx.AsyncClient(
        transport=_StreamingMockTransport(upstream_handler or _default_handler),
        timeout=None,
    )
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = first_poll_ok
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        queue_timeout=queue_timeout,
        upstream_client=upstream_client,
    )
    return app, gate, reconcile


async def test_reserved_label_admitted_when_shared_full():
    """A request with x-sluice-client-label: interactive is admitted via the
    reserved slot when the shared pool is full."""
    # capacity=2, reserve=1 → 1 shared, 1 reserved
    app, gate, _ = _make_app_with_reserve(gate_capacity=2, reserve=1, queue_timeout=0.1)

    # Use a slow upstream so the first request's permit stays held
    blocker = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        async def slow_gen():
            yield b"data: chunk1\n\n"
            await blocker.wait()  # hold the permit
            yield b"data: done\n\n"

        return httpx.Response(200, content=slow_gen(),
                               headers={"content-type": "text/event-stream"})

    app, gate, _ = _make_app_with_reserve(
        gate_capacity=2, reserve=1, queue_timeout=0.1, upstream_handler=handler
    )

    async with _asgi_client(app) as client:
        # Start the first (non-reserved) request — it holds the shared permit
        r1_task = asyncio.create_task(client.post("/v1/messages", json={"prompt": "hi"}))
        await asyncio.sleep(0.1)  # let it acquire

        # Non-reserved: shared full → 503 (queue_timeout=0.1)
        r2 = await client.post("/v1/messages", json={"prompt": "hi"})
        assert r2.status_code == 503

        # Reserved (interactive label): admitted via reserved slot
        r3_task = asyncio.create_task(
            client.post(
                "/v1/messages",
                json={"prompt": "hi"},
                headers={"x-sluice-client-label": "interactive"},
            )
        )
        await asyncio.sleep(0.1)
        assert gate.held == 2  # both permits held

        # Release both
        blocker.set()
        await r1_task
        await r3_task

    assert gate.held == 0  # all released after completion


async def test_reserved_label_stripped_from_upstream():
    """The x-sluice-client-label header is stripped before forwarding (WI-000)."""
    received: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received.update(dict(request.headers))
        return _resp(200)

    app, _, _ = _make_app_with_reserve(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post(
            "/v1/messages",
            json={"prompt": "hi"},
            headers={"x-sluice-client-label": "interactive"},
        )

    assert "x-sluice-client-label" not in received


async def test_no_reserve_configured_is_pure_fifo():
    """Without --reserve, all requests use the same pool regardless of label."""
    app, gate, _ = _make_app()  # default: no reserve

    async with _asgi_client(app) as client:
        # capacity=3, fill all
        for _ in range(3):
            r = await client.post(
                "/v1/messages",
                json={"prompt": "hi"},
                headers={"x-sluice-client-label": "interactive"},
            )
            assert r.status_code == 200

    assert gate.held == 0


# ---------------------------------------------------------------------------
# Plan 007 WI-1: Streaming mock transport — backpressure is real
# ---------------------------------------------------------------------------


async def test_backpressure_real():
    """body_stream() does not call receive() for chunk N+1 until the
    transport has consumed chunk N — backpressure is real, not buffered away.

    This test fails if body_stream() is replaced with a buffering implementation
    (e.g. await request.aread() in the transport), because aread() would
    consume all chunks immediately, causing receive() to be called for all
    chunks before the handler runs.
    """

    chunk_consumed = asyncio.Event()
    transport_waiting = asyncio.Event()
    receive_count = 0

    class _GatedTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            async for chunk in request.stream:
                transport_waiting.set()
                await chunk_consumed.wait()
                chunk_consumed.clear()
            request._content = b""
            return _resp(200)

    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    upstream_client = httpx.AsyncClient(transport=_GatedTransport(), timeout=None)
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        upstream_client=upstream_client,
    )

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        nonlocal receive_count
        receive_count += 1
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

    await receive_queue.put({"type": "http.request", "body": b"AAA", "more_body": True})
    await receive_queue.put({"type": "http.request", "body": b"BBB", "more_body": False})

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    proxy_task = asyncio.create_task(app(scope, receive, send))

    await asyncio.wait_for(transport_waiting.wait(), timeout=5.0)

    assert receive_count == 1, (
        f"backpressure not exercised: receive() called {receive_count} times, "
        "expected 1 (transport should not have consumed chunk 2 yet)"
    )

    chunk_consumed.set()

    transport_waiting.clear()
    await asyncio.wait_for(transport_waiting.wait(), timeout=5.0)

    assert receive_count == 2

    chunk_consumed.set()

    await asyncio.wait_for(proxy_task, timeout=5.0)

    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 1
    assert start_events[0]["status"] == 200


# ---------------------------------------------------------------------------
# Plan 007 WI-2: Latency-injecting transport — race the races
# ---------------------------------------------------------------------------


async def test_mid_body_upload_disconnect_with_latency():
    """Client disconnects mid-body-upload with real upstream latency.

    The header_delay ensures the asyncio.wait race (entry_task vs disconnect_task)
    actually races — the disconnect should win, cancelling the upstream request
    before it completes.  Permit must be released exactly once.
    """
    app, gate, _ = _make_app(header_delay=1.0)

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    await receive_queue.put({"type": "http.request", "body": b"part1", "more_body": True})
    await receive_queue.put({"type": "http.disconnect"})

    proxy_task = asyncio.create_task(app(scope, receive, send))
    await asyncio.wait_for(proxy_task, timeout=5.0)

    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 0, "should not send response after disconnect"

    assert gate.held == 0


async def test_disconnect_while_waiting_for_headers_with_latency():
    """Client disconnects while waiting for upstream headers with real latency.

    The header_delay ensures the asyncio.wait race (entry_task vs disconnect_task)
    actually races — both tasks are pending simultaneously, and the disconnect
    should win, cancelling the upstream request.

    This exercises the body_done/disconnect_watcher handoff: body_stream()
    completes, disconnect_watcher takes over receive(), and the disconnect
    is detected during the header_delay window.
    """
    app, gate, _ = _make_app(header_delay=1.0)

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

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
    await receive_queue.put({"type": "http.disconnect"})

    proxy_task = asyncio.create_task(app(scope, receive, send))
    await asyncio.wait_for(proxy_task, timeout=5.0)

    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 0, "should not send response after disconnect"

    assert gate.held == 0


async def test_upstream_raises_after_headers_streaming_transport():
    """Upstream raises after headers are sent — no double http.response.start.

    Regression guard for WI-013's response_started flag, now exercised under
    the streaming transport (which consumes request.stream chunk-by-chunk).
    """

    async def dropping_gen() -> AsyncIterator[bytes]:
        yield b"chunk1\n"
        raise httpx.ConnectError("upstream dropped mid-stream")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=dropping_gen(),
            headers={"content-type": "text/event-stream"},
        )

    app, _, _ = _make_app(upstream_handler=handler)

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

    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 1, "must not send a second http.response.start"


async def test_disconnect_during_response_streaming_with_chunk_delay():
    """Client disconnects during response streaming with transport-injected chunk_delay.

    Uses the _StreamingMockTransport's chunk_delay parameter (not a hand-rolled
    generator sleep) to create a deterministic race window for disconnect-during-
    response-streaming.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return _streaming_resp([b"chunk1\n", b"chunk2\n", b"chunk3\n"])

    app, gate, _ = _make_app(upstream_handler=handler, chunk_delay=0.3)

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
# Plan 007 WI-3: Startup-window honesty
# ---------------------------------------------------------------------------


async def test_startup_window_closes_on_first_poll():
    """Requests fail during the startup window, then succeed after the first poll.

    The startup fail-closed window (WI-018) is bypassed by almost every test
    via first_poll_ok=True.  This test exercises both sides explicitly.
    """
    app, _, reconcile = _make_app(first_poll_ok=False)

    async with _asgi_client(app) as client:
        r1 = await client.post("/v1/messages", json={"prompt": "hi"})
    assert r1.status_code == 503
    assert r1.json()["reason"] == "not_ready"

    await reconcile.tick()
    assert reconcile.ready is True

    async with _asgi_client(app) as client:
        r2 = await client.post("/v1/messages", json={"prompt": "hi"})
    assert r2.status_code == 200


async def test_startup_window_503_has_retry_after():
    """The startup fail-closed 503 includes a Retry-After header."""
    app, _, _ = _make_app(first_poll_ok=False)

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 503
    assert response.headers.get("retry-after") is not None
    assert response.json()["reason"] == "not_ready"


# ---------------------------------------------------------------------------
# Plan 007 WI-4: body_done/disconnect_watcher handoff pinning test
# ---------------------------------------------------------------------------


async def test_body_done_disconnect_watcher_handoff():
    """Pin the current behaviour of the body_done/disconnect_watcher handoff.

    There is a narrow window (proxy.py:disconnect_watcher) where a disconnect
    can arrive after body_done.set() but before disconnect_watcher calls
    receive().  This test documents that the current implementation catches
    the disconnect via the ASGI receive queue — the event is not lost, just
    delayed until the watcher arms.

    See docs/concurrency-model.md §8 for the full description of this window.
    If a future change alters this behaviour, this test should fail and force
    a deliberate update.
    """
    app, gate, _ = _make_app(header_delay=1.0)

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    # Send complete body — body_done is set, disconnect_watcher takes over.
    await receive_queue.put(
        {"type": "http.request", "body": b'{"prompt":"hi"}', "more_body": False}
    )
    # Immediately queue a disconnect — it arrives in the handoff window
    # (after body_done.set() but before the watcher's first receive()).
    await receive_queue.put({"type": "http.disconnect"})

    proxy_task = asyncio.create_task(app(scope, receive, send))
    await asyncio.wait_for(proxy_task, timeout=5.0)

    # The disconnect was caught — no response sent to the disconnected client.
    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 0, (
        "disconnect in the handoff window should be caught — no response sent"
    )

    assert gate.held == 0


# ---------------------------------------------------------------------------
# Bug fix: 429 recorded even when client disconnects after response headers
# ---------------------------------------------------------------------------


async def test_429_recorded_on_disconnect_after_headers():
    """A 429 from upstream must be recorded even when the client disconnects
    in the same event-loop turn.  Before the fix, the disconnect check ran
    before the 429 recording, silently dropping the signal and preventing
    the breaker from tripping.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "overloaded"})

    app, _, reconcile = _make_app(upstream_handler=handler)

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/messages",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json")],
    }

    await receive_queue.put({"type": "http.request", "body": b'{"prompt":"hi"}', "more_body": True})
    await receive_queue.put({"type": "http.disconnect"})

    await app(scope, receive, send)

    assert reconcile.total_429s == 1, "429 must be recorded even on disconnect"


# ---------------------------------------------------------------------------
# Bug fix: stream context closed on disconnect-during-entry
# ---------------------------------------------------------------------------


class _TrackableByteStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.aclose_called = False

    async def __aiter__(self):
        yield b"response"

    async def aclose(self) -> None:
        self.aclose_called = True


class _CancelSurvivingTransport(httpx.AsyncBaseTransport):
    """Transport that catches CancelledError and returns a response anyway.

    Simulates the edge case where __aenter__ completes (response obtained)
    despite a cancel request — the stream context is entered but __aexit__
    must still be called to close it.
    """

    def __init__(self) -> None:
        self.stream: _TrackableByteStream | None = None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        async for chunk in request.stream:
            pass
        try:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass
        self.stream = _TrackableByteStream()
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=self.stream,
        )


async def test_stream_closed_on_disconnect_during_entry():
    """When the client disconnects during __aenter__ but the entry task
    completes despite cancellation (e.g. the transport caught CancelledError),
    __aexit__ must still be called to close the stream context.  Without this,
    the upstream stream leaks.
    """
    transport = _CancelSurvivingTransport()

    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    upstream_client = httpx.AsyncClient(transport=transport, timeout=None)
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        upstream_client=upstream_client,
    )

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

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
    await receive_queue.put({"type": "http.disconnect"})

    await app(scope, receive, send)

    assert transport.stream is not None, "transport should have created a response"
    assert transport.stream.aclose_called, "stream must be closed via __aexit__"


class _CancelThenErrorTransport(httpx.AsyncBaseTransport):
    """Transport that catches CancelledError but raises a different exception.

    Simulates the edge case where __aenter__ is cancelled but the transport
    catches CancelledError and raises httpx.ConnectError instead — the stream
    context's __aexit__ must still be called.
    """

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        async for chunk in request.stream:
            pass
        try:
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            raise httpx.ConnectError("different error after cancellation")


class _StreamCMTracker:
    """Wraps a stream context manager to track __aexit__ calls."""

    def __init__(self, cm) -> None:
        self._cm = cm
        self.aexit_called = False

    async def __aenter__(self):
        return await self._cm.__aenter__()

    async def __aexit__(self, *args):
        self.aexit_called = True
        return await self._cm.__aexit__(*args)


async def test_stream_aexit_called_on_exception_after_cancellation():
    """When __aenter__ catches CancelledError but raises a different exception,
    __aexit__ must still be called to close the stream context.

    Regression guard for the else→finally fix in the disconnect-during-entry
    path: previously, if the entry task caught CancelledError but then raised
    httpx.RequestError, the except block swallowed it and the else branch
    (which calls __aexit__) was skipped, leaking the stream context.
    """
    transport = _CancelThenErrorTransport()

    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    upstream_client = httpx.AsyncClient(transport=transport, timeout=None)
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        upstream_client=upstream_client,
    )

    tracker_holder: list[_StreamCMTracker] = []
    original_stream = app._client.stream

    def tracking_stream(*args, **kwargs):
        cm = original_stream(*args, **kwargs)
        tracker = _StreamCMTracker(cm)
        tracker_holder.append(tracker)
        return tracker

    app._client.stream = tracking_stream  # type: ignore[assignment]

    receive_queue: asyncio.Queue = asyncio.Queue()
    sent_events: list[dict] = []

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)

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
    await receive_queue.put({"type": "http.disconnect"})

    await app(scope, receive, send)

    assert len(tracker_holder) == 1, "stream context manager should have been created"
    assert tracker_holder[0].aexit_called, (
        "__aexit__ must be called even when entry raises after cancellation"
    )
    assert gate.held == 0


# ---------------------------------------------------------------------------
# 502 error path: httpx.RequestError before response starts
# ---------------------------------------------------------------------------


async def test_502_on_upstream_request_error():
    """When httpx.RequestError is raised during stream.__aenter__, a 502 is sent."""

    class _ErrorTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            async for chunk in request.stream:
                pass
            raise httpx.ConnectError("connection refused")

    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    upstream_client = httpx.AsyncClient(transport=_ErrorTransport(), timeout=None)
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        upstream_client=upstream_client,
    )

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 502
    assert "error" in response.json()


# ---------------------------------------------------------------------------
# Query string forwarding
# ---------------------------------------------------------------------------


async def test_query_string_forwarded():
    """The query string from the incoming request reaches the upstream intact."""
    captured_url: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_url.append(str(request.url))
        return _resp(200)

    app, _, _ = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        response = await client.post(
            "/v1/messages",
            params={"stream": "true", "model": "claude-3"},
            json={"prompt": "hi"},
        )

    assert response.status_code == 200
    assert len(captured_url) == 1
    url = captured_url[0]
    assert "stream=true" in url
    assert "model=claude-3" in url


# ---------------------------------------------------------------------------
# Response hop-by-hop header stripping
# ---------------------------------------------------------------------------


async def test_response_hop_by_hop_headers_stripped():
    """Hop-by-hop headers are stripped from the response; normal headers pass through."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(
            200,
            json_data={"ok": True},
            headers={
                "connection": "keep-alive",
                "transfer-encoding": "chunked",
                "keep-alive": "timeout=120",
                "x-custom": "test-value",
            },
        )

    app, _, _ = _make_app(upstream_handler=handler)

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

    start_events = [e for e in sent_events if e["type"] == "http.response.start"]
    assert len(start_events) == 1
    header_keys = {k.decode("latin-1").lower() for k, _ in start_events[0]["headers"]}
    assert "connection" not in header_keys
    assert "transfer-encoding" not in header_keys
    assert "keep-alive" not in header_keys
    assert "content-type" in header_keys
    assert "x-custom" in header_keys


# ---------------------------------------------------------------------------
# ASGI lifespan: graceful startup and shutdown
# ---------------------------------------------------------------------------


async def test_lifespan_startup_and_shutdown():
    """ASGI lifespan handler starts/stops the reconcile loop and closes owned client."""
    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    assert app._owns_client is True

    sent_events: list[dict] = []
    receive_queue: asyncio.Queue = asyncio.Queue()
    startup_done = asyncio.Event()

    async def receive() -> dict:
        return await receive_queue.get()

    async def send(event: dict) -> None:
        sent_events.append(event)
        if event["type"] == "lifespan.startup.complete":
            startup_done.set()

    scope = {"type": "lifespan"}
    lifespan_task = asyncio.create_task(app(scope, receive, send))

    await receive_queue.put({"type": "lifespan.startup"})
    await asyncio.wait_for(startup_done.wait(), timeout=5.0)

    assert reconcile._task is not None

    await receive_queue.put({"type": "lifespan.shutdown"})
    await asyncio.wait_for(lifespan_task, timeout=5.0)

    startup_complete = [e for e in sent_events if e["type"] == "lifespan.startup.complete"]
    assert len(startup_complete) == 1
    shutdown_complete = [e for e in sent_events if e["type"] == "lifespan.shutdown.complete"]
    assert len(shutdown_complete) == 1
    assert reconcile._task is None
    assert app._client.is_closed


async def test_drain_waits_for_in_flight_requests():
    """Shutdown drain waits for in-flight requests before closing the client."""

    release_event = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        async def gen():
            yield b'{"partial": true}'
            await release_event.wait()
            yield b'{"done": true}'
        return httpx.Response(200, content=gen(), headers={"content-type": "application/json"})

    app, gate, reconcile = _make_app(upstream_handler=handler)
    reconcile._first_poll_ok = True

    # Start lifespan
    lifespan_receive: asyncio.Queue = asyncio.Queue()
    lifespan_sent: list[dict] = []
    startup_done = asyncio.Event()

    async def lifespan_receive_fn() -> dict:
        return await lifespan_receive.get()

    async def lifespan_send_fn(event: dict) -> None:
        lifespan_sent.append(event)
        if event["type"] == "lifespan.startup.complete":
            startup_done.set()

    lifespan_scope = {"type": "lifespan"}
    lifespan_task = asyncio.create_task(app(lifespan_scope, lifespan_receive_fn, lifespan_send_fn))
    await lifespan_receive.put({"type": "lifespan.startup"})
    await asyncio.wait_for(startup_done.wait(), timeout=5.0)

    # Start a request (will block on release_event)
    async with _asgi_client(app) as client:
        request_task = asyncio.create_task(client.post("/v1/messages", json={"prompt": "hi"}))

        # Wait for the request to acquire a permit
        await asyncio.sleep(0.2)
        assert gate.held == 1, "request should be in-flight"

        # Start shutdown — should block waiting for the in-flight request
        await lifespan_receive.put({"type": "lifespan.shutdown"})

        # Give shutdown time to reach the drain loop
        await asyncio.sleep(0.2)
        assert not lifespan_task.done(), "shutdown must wait for in-flight request"
        assert app._lifecycle.is_draining

        # Release the request — drain should complete
        release_event.set()
        await asyncio.wait_for(request_task, timeout=5.0)
        await asyncio.wait_for(lifespan_task, timeout=5.0)

    shutdown_complete = [e for e in lifespan_sent if e["type"] == "lifespan.shutdown.complete"]
    assert len(shutdown_complete) == 1
    assert app._lifecycle.is_draining is True  # draining flag stays set


async def test_drain_timeout_closes_with_in_flight():
    """Drain timeout expires and closes the client even if requests are still in-flight."""

    def handler(request: httpx.Request) -> httpx.Response:
        async def gen():
            await asyncio.sleep(10.0)  # won't complete within drain timeout
            yield b'{"ok": true}'
        return httpx.Response(200, content=gen(), headers={"content-type": "application/json"})

    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        drain_timeout=0.2,
    )
    # Inject custom transport into the owned client
    app._client = httpx.AsyncClient(
        transport=_StreamingMockTransport(handler),
        timeout=None,
    )
    app._lifecycle._client = app._client

    # Start lifespan
    lifespan_receive: asyncio.Queue = asyncio.Queue()
    lifespan_sent: list[dict] = []
    startup_done = asyncio.Event()

    async def lifespan_receive_fn() -> dict:
        return await lifespan_receive.get()

    async def lifespan_send_fn(event: dict) -> None:
        lifespan_sent.append(event)
        if event["type"] == "lifespan.startup.complete":
            startup_done.set()

    lifespan_scope = {"type": "lifespan"}
    lifespan_task = asyncio.create_task(app(lifespan_scope, lifespan_receive_fn, lifespan_send_fn))
    await lifespan_receive.put({"type": "lifespan.startup"})
    await asyncio.wait_for(startup_done.wait(), timeout=5.0)

    # Start a request (will block for 10s)
    async with _asgi_client(app) as client:
        request_task = asyncio.create_task(client.post("/v1/messages", json={"prompt": "hi"}))
        await asyncio.sleep(0.2)
        assert gate.held == 1

        # Start shutdown — drain timeout is 0.2s
        await lifespan_receive.put({"type": "lifespan.shutdown"})

        # Shutdown should complete after ~0.2s drain timeout, not 10s
        await asyncio.wait_for(lifespan_task, timeout=5.0)

    shutdown_complete = [e for e in lifespan_sent if e["type"] == "lifespan.shutdown.complete"]
    assert len(shutdown_complete) == 1
    assert app._client.is_closed, "owned client must be closed after shutdown"

    request_task.cancel()
    try:
        await request_task
    except (asyncio.CancelledError, Exception):
        pass


async def test_new_requests_503_during_drain():
    """New requests get 503 when the draining flag is set."""
    app, _, _ = _make_app()
    app._lifecycle._draining = True

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 503
    assert response.json()["reason"] == "draining"


async def test_request_blocked_on_acquire_during_drain():
    """A request blocked on gate.acquire() when drain starts gets 503 post-acquire.

    Race: request passes is_draining check (False), blocks on acquire (all
    permits held), drain starts and sees held==1, permit is released, the
    blocked request acquires it — but must check draining again, release
    the permit, and return 503 instead of forwarding.
    """
    gate = PermitGate(initial_capacity=1, release_cooldown=0.0)
    usage = FakeUsageClient()
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        queue_timeout=10.0,
    )

    # Hold the only permit so the next request blocks
    await gate.acquire(timeout=0.1)
    assert gate.held == 1

    # Start a request — it will block on acquire
    request_task = asyncio.create_task(_send_request(app, "/v1/messages"))
    await asyncio.sleep(0.1)
    assert gate.queue_depth == 1, "request should be waiting"

    # Set draining, then release the permit
    app._lifecycle._draining = True
    await gate.release()
    assert gate.held == 0

    # The blocked request acquires the permit, but the post-acquire drain
    # check should fire → release it and return 503
    result = await asyncio.wait_for(request_task, timeout=5.0)
    assert result.status_code == 503
    assert result.json()["reason"] == "draining"
    assert gate.held == 0, "permit must be released back by the post-acquire check"


async def _send_request(app: ProxyApp, path: str):
    async with _asgi_client(app) as client:
        return await client.post(path, json={"prompt": "hi"})


# ---------------------------------------------------------------------------
# Concurrent load: gate handles sustained contention
# ---------------------------------------------------------------------------


async def test_concurrent_load():
    """The gate correctly handles sustained contention with 10 requests over 3 permits."""
    def handler(request: httpx.Request) -> httpx.Response:
        async def gen():
            await asyncio.sleep(0.05)
            yield b'{"ok": true}'
        return httpx.Response(200, content=gen(), headers={"content-type": "application/json"})

    app, gate, _ = _make_app(gate_capacity=3, queue_timeout=10.0, upstream_handler=handler)

    max_queue_depth = 0

    async def track_depth():
        nonlocal max_queue_depth
        while True:
            max_queue_depth = max(max_queue_depth, gate.queue_depth)
            await asyncio.sleep(0.005)

    tracker = asyncio.create_task(track_depth())

    async with _asgi_client(app) as client:
        responses = await asyncio.gather(
            *[client.post("/v1/messages", json={"prompt": "hi"}) for _ in range(10)]
        )

    tracker.cancel()
    try:
        await tracker
    except asyncio.CancelledError:
        pass

    assert all(r.status_code == 200 for r in responses), "all requests should succeed"
    assert gate.held == 0, "all permits released after completion"
    assert gate.queue_timeouts == 0, "no queue timeouts expected"
    assert max_queue_depth > 0, "queue depth should have reached > 0 at some point"


# ---------------------------------------------------------------------------
# Plan 011: Runtime config override via dashboard
# ---------------------------------------------------------------------------


async def test_post_config_override_with_bearer():
    """POST /admin/config with valid bearer token applies a target override."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"target": 4},
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 200
    assert response.json()["target"] == 4
    assert response.json()["overridden"] is True
    assert reconcile.target == 4


async def test_post_config_override_with_warning():
    """POST /admin/config with target above limit returns a warning."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"target": 6},
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 200
    assert "warning" in response.json()
    assert "above limit" in response.json()["warning"]


async def test_post_config_override_resizes_gate_next_tick():
    """After applying an override, the next tick resizes the gate."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        await client.post(
            "/admin/config",
            json={"target": 4},
            headers={"Authorization": "Bearer secret"},
        )

    await reconcile.tick()
    assert reconcile.effective_permits_count == 4


async def test_post_config_no_token_returns_405():
    """POST /admin/config returns 405 when no admin token is configured."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.post("/admin/config", json={"target": 4})

    assert response.status_code == 405


async def test_post_config_wrong_auth_returns_403():
    """POST /admin/config returns 403 with invalid auth."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"target": 4},
            headers={"Authorization": "Bearer wrong"},
        )

    assert response.status_code == 403


async def test_post_config_invalid_value_returns_400():
    """POST /admin/config with target > hard_cap returns 400."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"target": 99},
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 400
    assert "hard_cap" in response.json()["error"]


async def test_post_config_unknown_field_returns_400():
    """POST /admin/config with a non-whitelisted field returns 400."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"min_floor": 2},
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 400


async def test_post_config_null_reverts_override():
    """POST /admin/config with {"target": null} reverts to boot value."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        await client.post(
            "/admin/config",
            json={"target": 5},
            headers={"Authorization": "Bearer secret"},
        )
        assert reconcile.target == 5

        response = await client.post(
            "/admin/config",
            json={"target": None},
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 200
    assert response.json()["overridden"] is False
    assert reconcile.target == 3  # boot value


async def test_delete_config_reverts_override():
    """DELETE /admin/config/target reverts to boot value."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        await client.post(
            "/admin/config",
            json={"target": 5},
            headers={"Authorization": "Bearer secret"},
        )
        assert reconcile.target == 5

        response = await client.delete(
            "/admin/config/target",
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 200
    assert response.json()["overridden"] is False
    assert reconcile.target == 3


async def test_delete_config_no_token_returns_405():
    """DELETE /admin/config/target returns 405 when no admin token is configured."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.delete("/admin/config/target")

    assert response.status_code == 405


async def test_override_visible_in_status_json():
    """The override is visible in /status.json after applying."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        await client.post(
            "/admin/config",
            json={"target": 5},
            headers={"Authorization": "Bearer secret"},
        )
        response = await client.get(
            "/status.json",
            headers={"Authorization": "Bearer secret"},
        )

    data = response.json()
    assert "overrides" in data
    assert data["overrides"]["target"]["boot"] == 3
    assert data["overrides"]["target"]["override"] == 5


async def test_no_override_shows_empty_overrides_in_status():
    """/status.json shows empty overrides when no override is active."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/status.json")

    data = response.json()
    assert data["overrides"] == {}


async def test_override_visible_in_metrics():
    """The override gauge appears in /metrics after applying."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        await client.post(
            "/admin/config",
            json={"target": 5},
            headers={"Authorization": "Bearer secret"},
        )
        response = await client.get(
            "/metrics",
            headers={"Authorization": "Bearer secret"},
        )

    assert 'sluice_config_overridden{field="target"} 1' in response.text
    assert "sluice_config_target 5" in response.text


async def test_no_override_shows_zero_in_metrics():
    """The override gauge is 0 in /metrics when no override is active."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/metrics")

    assert 'sluice_config_overridden{field="target"} 0' in response.text


async def test_post_config_with_basic_auth():
    """POST /admin/config works with Basic auth (same credentials as dashboard)."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    basic = "Basic " + base64.b64encode(b"user:secret").decode()

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"target": 4},
            headers={"Authorization": basic},
        )

    assert response.status_code == 200
    assert reconcile.target == 4


async def test_post_config_audit_log(caplog):
    """Every accepted override logs the change with audit info."""
    import logging as logging_mod

    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    with caplog.at_level(logging_mod.INFO, logger="sluice.admin"):
        async with _asgi_client(app) as client:
            await client.post(
                "/admin/config",
                json={"target": 4},
                headers={"Authorization": "Bearer secret"},
            )

    assert any("config override" in r.message for r in caplog.records), (
        "accepted override must be logged"
    )


async def test_dashboard_has_config_stepper_elements():
    """Dashboard HTML contains the target stepper, override badge, and revert link."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/")

    html = response.text
    assert "stepTarget" in html
    assert "revertTarget" in html
    assert "step-btn" in html
    assert "ov-badge" in html
    assert "ov-revert" in html
    assert "mutationsDisabled" in html


async def test_post_config_wrong_content_type_returns_415():
    """POST /admin/config with text/plain Content-Type is rejected (CSRF defence)."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            content=b'{"target": 4}',
            headers={
                "Authorization": "Bearer secret",
                "Content-Type": "text/plain",
            },
        )

    assert response.status_code == 415


async def test_post_config_override_before_tick_returns_400():
    """POST /admin/config returns 400 when no successful usage poll has occurred."""
    app, _, reconcile = _make_app(first_poll_ok=False)
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"target": 4},
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 400
    assert "poll" in response.json()["error"]


async def test_post_config_body_too_large_returns_413():
    """POST /admin/config rejects bodies exceeding the size cap (WI-025)."""
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    await reconcile.tick()

    big_body = b"x" * 10000

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            content=big_body,
            headers={
                "Authorization": "Bearer secret",
                "Content-Type": "application/json",
            },
        )

    assert response.status_code == 413


async def test_non_leader_config_post_returns_503():
    """POST /admin/config on a non-leader returns 503 not_leader."""
    guard = _FakeGuard(held=False, acquire_result=False)
    app, _, reconcile = _make_app_with_guard(guard)
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"target": 4},
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 503
    assert response.json()["reason"] == "not_leader"


async def test_non_leader_config_delete_returns_503():
    """DELETE /admin/config/target on a non-leader returns 503 not_leader."""
    guard = _FakeGuard(held=False, acquire_result=False)
    app, _, reconcile = _make_app_with_guard(guard)
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.delete(
            "/admin/config/target",
            headers={"Authorization": "Bearer secret"},
        )

    assert response.status_code == 503
    assert response.json()["reason"] == "not_leader"


async def test_config_post_auth_before_leader_check():
    """Auth is checked before leader status — unauthenticated request gets 403,
    not 503 not_leader (no topology leak)."""
    guard = _FakeGuard(held=False, acquire_result=False)
    app, _, reconcile = _make_app_with_guard(guard)
    app._admin_token = "secret"
    await reconcile.tick()

    async with _asgi_client(app) as client:
        response = await client.post(
            "/admin/config",
            json={"target": 4},
            headers={"Authorization": "Bearer wrong"},
        )

    assert response.status_code == 403


def test_upstream_client_has_bounded_connect_write_pool():
    """WI-027: the production client has finite connect/write/pool timeouts
    to protect against hung connections, but read=None (no response-duration
    cap) so slow/streaming completions are not truncated.

    A read timeout would kill any stream with a >300s inter-chunk gap,
    releasing the permit while the upstream may still count the session
    — self-inflicting the phantom the reconciler exists to absorb.
    """
    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    timeout = app._client.timeout
    assert timeout.connect is not None and timeout.connect > 0
    assert timeout.write is not None and timeout.write > 0
    assert timeout.pool is not None and timeout.pool > 0
    assert timeout.read is None, "read must be None — no response-duration cap (WI-027)"


# ---------------------------------------------------------------------------
# WI-029: drain_timeout=0 backward-compat path
# ---------------------------------------------------------------------------


async def test_drain_timeout_zero_closes_immediately():
    """WI-029: drain_timeout=0 skips the drain wait and closes immediately.

    The drain_timeout=0 path is the backward-compat default for users who
    don't want graceful drain. The upstream client must be closed right
    away, even if requests are in-flight. This path had zero coverage.
    """
    def handler(request: httpx.Request) -> httpx.Response:
        async def gen():
            await asyncio.sleep(10.0)  # won't complete — drain_timeout=0 skips
            yield b'{"ok": true}'
        return httpx.Response(200, content=gen(), headers={"content-type": "application/json"})

    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        drain_timeout=0.0,
    )
    app._client = httpx.AsyncClient(
        transport=_StreamingMockTransport(handler),
        timeout=None,
    )
    app._lifecycle._client = app._client

    # Start lifespan
    lifespan_receive: asyncio.Queue = asyncio.Queue()
    lifespan_sent: list[dict] = []
    startup_done = asyncio.Event()

    async def lifespan_receive_fn() -> dict:
        return await lifespan_receive.get()

    async def lifespan_send_fn(event: dict) -> None:
        lifespan_sent.append(event)
        if event["type"] == "lifespan.startup.complete":
            startup_done.set()

    lifespan_scope = {"type": "lifespan"}
    lifespan_task = asyncio.create_task(app(lifespan_scope, lifespan_receive_fn, lifespan_send_fn))
    await lifespan_receive.put({"type": "lifespan.startup"})
    await asyncio.wait_for(startup_done.wait(), timeout=5.0)

    # Start a request (will block for 10s)
    async with _asgi_client(app) as client:
        request_task = asyncio.create_task(client.post("/v1/messages", json={"prompt": "hi"}))
        await asyncio.sleep(0.2)
        assert gate.held == 1

        # Start shutdown — drain_timeout=0, should complete immediately
        start = asyncio.get_running_loop().time()
        await lifespan_receive.put({"type": "lifespan.shutdown"})
        await asyncio.wait_for(lifespan_task, timeout=5.0)
        elapsed = asyncio.get_running_loop().time() - start

    shutdown_complete = [e for e in lifespan_sent if e["type"] == "lifespan.shutdown.complete"]
    assert len(shutdown_complete) == 1
    assert app._client.is_closed, "owned client must be closed after shutdown"
    assert elapsed < 2.0, "shutdown with drain_timeout=0 must not wait for in-flight requests"

    request_task.cancel()
    try:
        await request_task
    except (asyncio.CancelledError, Exception):
        pass


async def test_drain_timeout_zero_with_no_in_flight():
    """WI-029: drain_timeout=0 with no in-flight requests completes normally."""
    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        drain_timeout=0.0,
    )

    lifespan_receive: asyncio.Queue = asyncio.Queue()
    lifespan_sent: list[dict] = []
    startup_done = asyncio.Event()

    async def lifespan_receive_fn() -> dict:
        return await lifespan_receive.get()

    async def lifespan_send_fn(event: dict) -> None:
        lifespan_sent.append(event)
        if event["type"] == "lifespan.startup.complete":
            startup_done.set()

    lifespan_scope = {"type": "lifespan"}
    lifespan_task = asyncio.create_task(app(lifespan_scope, lifespan_receive_fn, lifespan_send_fn))
    await lifespan_receive.put({"type": "lifespan.startup"})
    await asyncio.wait_for(startup_done.wait(), timeout=5.0)

    # No in-flight requests — shutdown should be instant
    await lifespan_receive.put({"type": "lifespan.shutdown"})
    await asyncio.wait_for(lifespan_task, timeout=5.0)

    shutdown_complete = [e for e in lifespan_sent if e["type"] == "lifespan.shutdown.complete"]
    assert len(shutdown_complete) == 1
    assert app._client.is_closed
