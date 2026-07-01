"""Tests for the streaming reverse proxy.

Covers: incremental streaming, 503 on queue timeout, permit release on
completion and disconnect, auth passthrough, 429 reporting.
"""

from __future__ import annotations

import asyncio
import base64
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

    def __init__(self) -> None:
        self.fetch_count = 0

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        self.fetch_count += 1
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
        # Read the request body so the proxy's body_stream generator is consumed
        # and the body is available for handler inspection via request.content.
        await request.aread()

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
    reconcile._first_poll_ok = True  # most tests assume ready state
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
    app, _, reconcile = _make_app()
    reconcile._first_poll_ok = False  # simulate pre-first-poll state

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


async def test_dashboard():
    """GET / returns the dashboard HTML page."""
    app, _, _ = _make_app()

    async with _asgi_client(app) as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "sluice" in response.text.lower()


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
    app, _, reconcile = _make_app()
    app._admin_token = "secret"
    reconcile._first_poll_ok = False  # simulate pre-first-poll state

    async with _asgi_client(app) as client:
        health = await client.get("/healthz")
        ready = await client.get("/readyz")

    assert health.status_code == 200
    assert ready.status_code == 503  # not ready before first poll


async def test_dashboard_requires_auth_when_token_set():
    """The dashboard at / requires auth when admin_token is set."""
    app, _, _ = _make_app()
    app._admin_token = "secret"

    async with _asgi_client(app) as client:
        response = await client.get("/")

    assert response.status_code == 401
    assert response.headers.get("www-authenticate") is not None
    assert "Basic" in response.headers.get("www-authenticate", "")


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
        guard=guard,  # type: ignore[arg-type]
    )
    reconcile._first_poll_ok = True  # most tests assume ready state
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
        usage_client=usage,  # type: ignore[arg-type]
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
    app._retry_interval = 0.01

    app._acquire_retry_task = asyncio.create_task(app._retry_acquire())

    await asyncio.sleep(0.2)

    assert app._guard_acquired is True
    assert guard.renewer_started is True
    assert reconcile._task is not None

    await reconcile.stop()
    if app._acquire_retry_task and not app._acquire_retry_task.done():
        app._acquire_retry_task.cancel()
        try:
            await app._acquire_retry_task
        except asyncio.CancelledError:
            pass


async def test_retry_acquire_cancelled_on_shutdown():
    """The retry task is properly cancelled during shutdown."""
    guard = _FakeGuard(held=False, acquire_result=False)
    app, _, _ = _make_app_with_guard(guard)
    app._retry_interval = 0.01

    app._acquire_retry_task = asyncio.create_task(app._retry_acquire())

    await asyncio.sleep(0.05)

    # Simulate shutdown cancelling the task.
    assert app._acquire_retry_task is not None
    app._acquire_retry_task.cancel()
    try:
        await app._acquire_retry_task
    except asyncio.CancelledError:
        pass

    assert app._acquire_retry_task.cancelled()
    assert app._guard_acquired is False


async def test_retry_acquire_releases_lease_if_start_fails():
    """If start_renewer raises after acquire succeeds, the lease is released and retry continues.

    Without this, the proxy would hold the lease but have no renewer or reconcile
    running — a zombie leader that serves traffic without usage polling.
    """
    guard = _FakeGuardRetryable()
    guard._start_renewer_fail_count = 1  # fail first start_renewer call
    app, _, reconcile = _make_app_with_guard(guard)
    app._retry_interval = 0.01

    app._acquire_retry_task = asyncio.create_task(app._retry_acquire())

    await asyncio.sleep(0.3)

    assert app._guard_acquired is True
    assert guard.renewer_started is True
    assert reconcile._task is not None
    assert guard._release_count >= 1  # lease was released after the failed start

    await reconcile.stop()
    if app._acquire_retry_task and not app._acquire_retry_task.done():
        app._acquire_retry_task.cancel()
        try:
            await app._acquire_retry_task
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
    app, _, reconcile = _make_app()
    reconcile._first_poll_ok = False  # simulate pre-first-poll state

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


async def test_concurrency_429_without_retry_after_is_recorded():
    """A 429 without retry-after (concurrency rejection) is recorded."""

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, json_data={"error": "overloaded"})

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 1


async def test_rate_limit_429_with_retry_after_is_not_recorded():
    """A 429 with retry-after (rate-limit) is NOT recorded as a concurrency 429."""

    def handler(request: httpx.Request) -> httpx.Response:
        resp = _resp(429, json_data={"error": "rate_limit_exceeded"})
        resp.headers["retry-after"] = "60"
        return resp

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 0, "rate-limit 429s must not trip the breaker"


async def test_429_with_retry_after_zero_is_not_recorded():
    """A 429 with retry-after: 0 is still a rate-limit signal — not recorded."""

    def handler(request: httpx.Request) -> httpx.Response:
        resp = _resp(429, json_data={"error": "rate_limit_exceeded"})
        resp.headers["retry-after"] = "0"
        return resp

    app, _, reconcile = _make_app(upstream_handler=handler)

    async with _asgi_client(app) as client:
        await client.post("/v1/messages", json={"prompt": "hi"})

    assert reconcile.total_429s == 0, "retry-after: 0 is still a rate-limit signal"
