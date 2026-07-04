"""Tests for the history ring buffer: unit tests, reconciliation integration, and endpoint."""

from __future__ import annotations

import asyncio
import json

import httpx

from sluice.control import (
    BreakerConfig,
    ControllerConfig,
    UsageReading,
)
from sluice.gate import PermitGate
from sluice.history import History, HistoryEntry
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.usage import CachedReading


# ---------------------------------------------------------------------------
# Unit tests: HistoryEntry
# ---------------------------------------------------------------------------


def test_history_entry_to_dict_keys():
    e = HistoryEntry(
        timestamp=1700000000.0,
        concurrent_sessions=3,
        local_in_flight=2,
        phantom_estimate=1,
        effective_permits=2,
        limit=4,
        hard_cap=8,
        band="normal",
        breaker="closed",
        priority_low=False,
        usage_age=1.5,
        stale=False,
        recent_429s=0,
        total_429s=0,
        queue_depth=0,
        queue_timeouts=0,
    )
    d = e.to_dict()
    assert d["ts"] == 1700000000.0
    assert d["obs"] == 3
    assert d["loc"] == 2
    assert d["ph"] == 1
    assert d["ep"] == 2
    assert d["lim"] == 4
    assert d["hc"] == 8
    assert d["band"] == "normal"
    assert d["brk"] == "closed"
    assert d["pl"] is False
    assert d["age"] == 1.5
    assert d["stl"] is False
    assert d["r429"] == 0
    assert d["t429"] == 0
    assert d["rl429"] == 0
    assert d["qd"] == 0
    assert d["qt"] == 0
    assert d["err"] is False
    assert d["rwin"] is None
    assert d["rlim"] is None
    assert d["rrem"] is None
    assert d["rlw"] is None
    assert d["rdelta"] is None


def test_history_entry_request_window_fields():
    e = HistoryEntry(
        timestamp=1700000000.0,
        concurrent_sessions=3,
        local_in_flight=2,
        phantom_estimate=1,
        effective_permits=2,
        limit=4,
        hard_cap=8,
        band="normal",
        breaker="closed",
        priority_low=False,
        usage_age=1.5,
        stale=False,
        recent_429s=0,
        total_429s=0,
        queue_depth=0,
        queue_timeouts=0,
        requests_in_window=48,
        requests_limit=200,
        requests_remaining=152,
        local_requests_in_window=40,
        request_window_delta=8,
    )
    d = e.to_dict()
    assert d["rwin"] == 48
    assert d["rlim"] == 200
    assert d["rrem"] == 152
    assert d["rlw"] == 40
    assert d["rdelta"] == 8


def test_history_entry_rate_limit_429s_field():
    e = HistoryEntry(
        timestamp=1700000000.0,
        concurrent_sessions=3,
        local_in_flight=2,
        phantom_estimate=1,
        effective_permits=2,
        limit=4,
        hard_cap=8,
        band="normal",
        breaker="closed",
        priority_low=False,
        usage_age=1.5,
        stale=False,
        recent_429s=5,
        total_429s=3,
        rate_limit_429s=7,
        queue_depth=0,
        queue_timeouts=0,
    )
    d = e.to_dict()
    assert d["rl429"] == 7
    assert d["r429"] == 5
    assert d["t429"] == 3


def test_history_entry_to_dict_roundtrip():
    e = HistoryEntry(
        timestamp=1700000000.5,
        concurrent_sessions=None,
        local_in_flight=0,
        phantom_estimate=0,
        effective_permits=0,
        limit=None,
        hard_cap=None,
        band="boxed",
        breaker="open",
        priority_low=True,
        usage_age=99.9,
        stale=True,
        recent_429s=5,
        total_429s=10,
        queue_depth=3,
        queue_timeouts=1,
        tick_failed=True,
    )
    d = e.to_dict()
    assert d["obs"] is None
    assert d["lim"] is None
    assert d["hc"] is None
    assert d["stl"] is True
    assert d["band"] == "boxed"
    assert d["brk"] == "open"
    assert d["pl"] is True
    assert d["r429"] == 5
    assert d["t429"] == 10
    assert d["err"] is True
    assert json.dumps(d)


# ---------------------------------------------------------------------------
# Unit tests: History ring buffer
# ---------------------------------------------------------------------------


def test_history_append_and_length():
    h = History(maxlen=10)
    assert h.length == 0
    for i in range(5):
        h.append(_entry(timestamp=1000.0 + i))
    assert h.length == 5


def test_history_evicts_oldest_when_full():
    h = History(maxlen=3)
    for i in range(5):
        h.append(_entry(timestamp=1000.0 + i))
    assert h.length == 3
    entries = h.entries()
    assert entries[0].timestamp == 1002.0
    assert entries[2].timestamp == 1004.0


def test_history_clear():
    h = History(maxlen=10)
    h.append(_entry())
    h.append(_entry())
    assert h.length == 2
    h.clear()
    assert h.length == 0


def test_history_maxlen_property():
    h = History(maxlen=42)
    assert h.maxlen == 42


def test_history_rejects_invalid_maxlen():
    try:
        History(maxlen=0)
        assert False, "should have raised"
    except ValueError:
        pass
    try:
        History(maxlen=-1)
        assert False, "should have raised"
    except ValueError:
        pass


def test_history_to_dict_list():
    h = History(maxlen=10)
    h.append(_entry(timestamp=1000.0, concurrent_sessions=1))
    h.append(_entry(timestamp=1001.0, concurrent_sessions=2))
    lst = h.to_dict_list()
    assert len(lst) == 2
    assert lst[0]["ts"] == 1000.0
    assert lst[0]["obs"] == 1
    assert lst[1]["ts"] == 1001.0
    assert lst[1]["obs"] == 2


def test_history_entries_returns_copy():
    h = History(maxlen=10)
    h.append(_entry(timestamp=1000.0))
    entries = h.entries()
    entries.clear()
    assert h.length == 1, "clearing the returned list must not affect the buffer"


# ---------------------------------------------------------------------------
# Integration: ReconciliationLoop records history on tick
# ---------------------------------------------------------------------------


CFG = ControllerConfig(target=3, min_floor=1, usage_fresh_ttl=15.0, stale_penalty=1, low_penalty=1, phantom_window=3)
BCFG = BreakerConfig(threshold=5, window_seconds=300.0, cooldown_seconds=60.0)


class FakeUsageClient:
    def __init__(self, reading: UsageReading) -> None:
        self._reading = reading
        self._fail = False
        self._last_ok_mono = 0.0

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        if self._fail:
            return CachedReading(
                reading=self._reading,
                fetched_at_monotonic=self._last_ok_mono,
                ok=False,
            )
        self._last_ok_mono = now_monotonic
        return CachedReading(
            reading=self._reading,
            fetched_at_monotonic=now_monotonic,
            ok=True,
        )

    def set_reading(self, reading: UsageReading) -> None:
        self._reading = reading

    def set_fail(self, fail: bool) -> None:
        self._fail = fail

    @property
    def last_cached(self) -> CachedReading | None:
        return None

    def record_response_headers(self, headers, status, *, now_monotonic) -> None:
        pass

    async def close(self) -> None:
        pass


def _reading(**kw) -> UsageReading:
    base: dict[str, object] = dict(concurrent_sessions=0, limit=4, hard_cap=8)
    base.update(kw)
    return UsageReading(**base)  # type: ignore[arg-type]


def _entry(**kw) -> HistoryEntry:
    defaults: dict[str, object] = dict(
        timestamp=1000.0,
        concurrent_sessions=0,
        local_in_flight=0,
        phantom_estimate=0,
        effective_permits=3,
        limit=4,
        hard_cap=8,
        band="normal",
        breaker="closed",
        priority_low=False,
        usage_age=0.0,
        stale=False,
        recent_429s=0,
        total_429s=0,
        queue_depth=0,
        queue_timeouts=0,
    )
    defaults.update(kw)
    return HistoryEntry(**defaults)  # type: ignore[arg-type]


def _make_loop_with_history(initial: UsageReading, *, maxlen: int = 100):
    m = [1000.0]
    w = [1_000_000.0]
    client = FakeUsageClient(initial)
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    history = History(maxlen=maxlen)
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=5.0,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        history=history,
    )
    return loop, client, gate, m, w, history


async def test_tick_records_history_entry():
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(concurrent_sessions=0)
    )
    await loop.tick()
    assert history.length == 1
    entry = history.entries()[0]
    assert entry.concurrent_sessions == 0
    assert entry.band == "normal"
    assert entry.effective_permits == 3
    assert entry.breaker == "closed"
    assert entry.stale is False


async def test_multiple_ticks_accumulate_history():
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(concurrent_sessions=2)
    )
    for i in range(5):
        m[0] += 5
        w[0] += 5
        await loop.tick()
    assert history.length == 5
    entries = history.entries()
    assert entries[0].timestamp < entries[-1].timestamp


async def test_history_records_stale_reading():
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(concurrent_sessions=0)
    )
    await loop.tick()
    assert history.entries()[0].stale is False

    client.set_fail(True)
    m[0] += 100
    w[0] += 100
    await loop.tick()
    entries = history.entries()
    assert entries[-1].stale is True


async def test_history_records_breaker_state():
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(concurrent_sessions=0)
    )
    loop._brk_cfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)

    for _ in range(3):
        loop.record_429()
    await loop.tick()

    entries = history.entries()
    assert entries[-1].breaker == "open"
    assert entries[-1].recent_429s == 3


async def test_history_records_phantom_estimate():
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(concurrent_sessions=5)
    )
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    await loop.tick()

    entries = history.entries()
    assert entries[-1].phantom_estimate >= 1
    assert entries[-1].concurrent_sessions == 5


async def test_history_eviction_at_maxlen():
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(concurrent_sessions=0), maxlen=3
    )
    for i in range(5):
        m[0] += 5
        w[0] += 5
        await loop.tick()
    assert history.length == 3


async def test_no_history_when_not_configured():
    """When history=None, tick() must work normally without recording."""
    m = [1000.0]
    w = [1_000_000.0]
    client = FakeUsageClient(_reading(concurrent_sessions=0))
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=5.0,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        history=None,
    )
    await loop.tick()
    assert loop.history is None
    assert gate.capacity == 3


async def test_history_property_returns_buffer():
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(concurrent_sessions=0)
    )
    assert loop.history is history
    await loop.tick()
    assert loop.history.length == 1


# ---------------------------------------------------------------------------
# Integration: /history.json endpoint
# ---------------------------------------------------------------------------


async def test_history_json_endpoint_returns_entries():
    """The /history.json endpoint returns recorded history after ticks."""
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=2, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )
    await reconcile.tick()
    await reconcile.tick()

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/history.json")

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert resp.headers["cache-control"] == "no-store"
    body = resp.json()
    assert body["count"] == 2
    assert len(body["entries"]) == 2
    assert body["entries"][0]["obs"] == 2
    assert body["entries"][1]["obs"] == 2


async def test_history_json_endpoint_empty_when_no_ticks():
    """The /history.json endpoint returns an empty array before any tick."""
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/history.json")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 0
    assert body["entries"] == []


async def test_history_json_endpoint_when_history_disabled():
    """The /history.json endpoint returns an empty array when history is not configured."""
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=None,
    )
    await reconcile.tick()

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/history.json")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 0
    assert body["entries"] == []


async def test_history_json_endpoint_auth_gated():
    """The /history.json endpoint requires admin auth when token is set."""
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        admin_token="secret-token",
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/history.json")
    assert resp.status_code == 401

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/history.json", headers={"Authorization": "Bearer secret-token"})
    assert resp.status_code == 200


async def test_history_does_not_leak_request_body():
    """History entries must not contain request or response body text."""
    secret = "THIS_IS_SECRET_REQUEST_CONTENT"

    class _AsyncMockTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            await request.aread()
            payload = json.dumps({"secret": secret}).encode()

            async def gen():
                yield payload

            return httpx.Response(200, content=gen(), headers={"content-type": "application/json"})

    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )
    upstream_client = httpx.AsyncClient(transport=_AsyncMockTransport(), timeout=None)
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        upstream_client=upstream_client,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        await client.post("/v1/messages", json={"prompt": secret})
        await reconcile.tick()

        resp = await client.get("/history.json")
    body_str = resp.text
    assert secret not in body_str, "history must not contain request body text"


# ---------------------------------------------------------------------------
# Adversarial-review fixes: ?limit=N, enabled field, tick failure, adaptive
# ---------------------------------------------------------------------------


async def test_history_json_limit_query_param():
    """The ?limit=N query parameter returns only the last N entries."""
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )
    for _ in range(10):
        await reconcile.tick()

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/history.json?limit=3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 3
    assert len(body["entries"]) == 3


async def test_history_json_enabled_field():
    """The response includes an 'enabled' field distinguishing configured vs disabled."""
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/history.json")
    assert resp.json()["enabled"] is True

    # Now test with history disabled
    reconcile_no_history = ReconciliationLoop(
        truth_source=FakeUsageClient(UsageReading(concurrent_sessions=0)),  # type: ignore[arg-type]
        gate=PermitGate(initial_capacity=3),
        controller_config=CFG,
        breaker_config=BCFG,
        history=None,
    )
    app2 = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=PermitGate(initial_capacity=3),
        reconcile=reconcile_no_history,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app2), base_url="http://test") as c:
        resp2 = await c.get("/history.json")
    assert resp2.json()["enabled"] is False


async def test_history_records_tick_failure():
    """When tick() raises, a fail-safe entry is recorded with tick_failed=True."""
    m = [1000.0]
    w = [1_000_000.0]
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    history = History(maxlen=100)

    class FailingTruthSource:
        @property
        def last_cached(self):
            return None

        async def fetch(self, *, now_monotonic: float):
            raise RuntimeError("simulated fetch failure")

        def record_response_headers(self, headers, status, *, now_monotonic):
            pass

        async def close(self):
            pass

    loop = ReconciliationLoop(
        truth_source=FailingTruthSource(),  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=0.01,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        history=history,
    )

    # Start run() — it will catch the tick exception and record a fail-safe entry
    task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert history.length >= 1
    entry = history.entries()[-1]
    assert entry.tick_failed is True
    assert entry.effective_permits == 0
    assert entry.stale is True


async def test_history_with_adaptive_controller():
    """History records correctly under the adaptive (AIMD) controller."""
    from sluice.control import AdaptiveConfig
    from sluice.providers import NullTruthSource

    m = [1000.0]
    w = [1_000_000.0]
    truth = NullTruthSource(provider="generic")
    gate = PermitGate(initial_capacity=0, release_cooldown=0.0, clock=lambda: m[0])
    history = History(maxlen=100)
    loop = ReconciliationLoop(
        truth_source=truth,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=5.0,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        controller="adaptive",
        adaptive_config=AdaptiveConfig(target=3),
        history=history,
    )
    await loop.tick()
    assert history.length == 1
    entry = history.entries()[0]
    assert entry.breaker == "closed"
    # Adaptive controller doesn't compute phantom estimates → always 0
    assert entry.phantom_estimate == 0


async def test_history_json_limit_zero():
    """?limit=0 returns an empty entries array but count is still 0."""
    gate = PermitGate(initial_capacity=3)
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    history = History(maxlen=100)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        history=history,
    )
    await reconcile.tick()

    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/history.json?limit=0")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 0
    assert body["entries"] == []
    assert body["enabled"] is True


async def test_history_records_limit_and_hard_cap():
    """History entries capture limit and hard_cap from the reading."""
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(concurrent_sessions=2, limit=4, hard_cap=8)
    )
    await loop.tick()
    entry = history.entries()[0]
    assert entry.limit == 4
    assert entry.hard_cap == 8


async def test_history_records_request_window_fields():
    """History entries capture request-window fields from the reading."""
    loop, client, gate, m, w, history = _make_loop_with_history(
        _reading(
            concurrent_sessions=2,
            limit=4,
            hard_cap=8,
            requests_limit=200,
            requests_remaining=152,
            requests_in_window=48,
            requests_hard_cap=400,
            requests_window_seconds=18000,
        )
    )
    await loop.tick()
    entry = history.entries()[0]
    assert entry.requests_in_window == 48
    assert entry.requests_limit == 200
    assert entry.requests_remaining == 152
    assert entry.local_requests_in_window == 0  # no requests forwarded yet
    assert entry.request_window_delta == 48  # provider 48 - local 0 = 48
