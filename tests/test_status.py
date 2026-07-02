"""Tests for the status projection: snapshot fields, Prometheus format, content-leak guarantee."""

from __future__ import annotations

import json

import httpx

from sluice.control import BreakerConfig, ControllerConfig, UsageReading
from sluice.gate import PermitGate
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.status import snapshot, to_prometheus
from sluice.usage import CachedReading


class FakeUsageClient:
    def __init__(self, reading: UsageReading | None = None) -> None:
        self._reading = reading or UsageReading(concurrent_sessions=0, limit=4, hard_cap=8)

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        return CachedReading(
            reading=self._reading,
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


def _make_reconcile(reading: UsageReading | None = None) -> ReconciliationLoop:
    gate = PermitGate(initial_capacity=3)
    return ReconciliationLoop(
        truth_source=FakeUsageClient(reading),  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(target=3, phantom_window=3),
        breaker_config=BreakerConfig(),
    )


async def test_snapshot_fields():
    loop = _make_reconcile(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    await loop.tick()

    snap = snapshot(loop)
    d = snap.to_dict()

    assert d["concurrent_sessions"] == 0
    assert d["limit"] == 4
    assert d["hard_cap"] == 8
    assert d["band"] == "normal"
    assert d["effective_permits"] == 3
    assert d["breaker"] == "closed"
    assert d["ready"] is True
    assert d["gate_closed_reason"] == "open"
    assert d["phantom_estimate"] == 0
    assert d["cooling_down"] == 0
    assert d["avg_wait_seconds"] == 0.0
    assert d["p95_wait_seconds"] == 0.0
    assert d["queue_timeouts"] == 0
    assert "config" in d
    assert d["config"]["target"] == 3
    assert d["config"]["breaker_threshold"] == 5
    assert d["config"]["poll_interval"] == 5.0


async def test_snapshot_queue_wait_reflects_gate():
    """avg/p95 queue wait and timeouts in the snapshot read the gate's counters."""
    gate = PermitGate(initial_capacity=0)  # no permits → acquire times out
    loop = ReconciliationLoop(
        truth_source=FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8)),  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(target=3, phantom_window=3),
        breaker_config=BreakerConfig(),
    )

    assert await gate.acquire(timeout=0.01) is False

    snap = snapshot(loop)
    assert snap.queue_timeouts == 1
    assert snap.to_dict()["queue_timeouts"] == 1


async def test_snapshot_cooling_down_reflects_gate():
    """cooling_down in the snapshot reads the gate's actual cooldown count."""
    gate = PermitGate(initial_capacity=3, release_cooldown=999.0)
    loop = ReconciliationLoop(
        truth_source=FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8)),  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(target=3, phantom_window=3),
        breaker_config=BreakerConfig(),
    )
    await loop.tick()

    await gate.acquire(timeout=1.0)
    await gate.release()

    snap = snapshot(loop)
    assert snap.cooling_down == 1


async def test_snapshot_before_first_tick():
    loop = _make_reconcile()
    snap = snapshot(loop)
    d = snap.to_dict()

    assert d["concurrent_sessions"] is None
    assert d["ready"] is False


async def test_snapshot_breaker_half_open_age_none_when_closed():
    """breaker_half_open_age_seconds is None when breaker is closed."""
    loop = _make_reconcile(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    await loop.tick()

    snap = snapshot(loop)
    d = snap.to_dict()

    assert d["breaker"] == "closed"
    assert d["breaker_half_open_age_seconds"] is None


async def test_snapshot_breaker_half_open_age_set_when_half_open():
    """breaker_half_open_age_seconds has a value when breaker is half-open."""
    from sluice.control import BreakerSnapshot, BreakerState

    loop = _make_reconcile(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    await loop.tick()

    loop._breaker = BreakerSnapshot(
        state=BreakerState.HALF_OPEN,
        opened_at=0.0,
        half_opened_at=loop._mono() - 5.0,
    )

    snap = snapshot(loop)
    d = snap.to_dict()

    assert d["breaker"] == "half_open"
    assert d["breaker_half_open_age_seconds"] is not None
    assert d["breaker_half_open_age_seconds"] >= 4.9


async def test_prometheus_format():
    loop = _make_reconcile(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    await loop.tick()

    snap = snapshot(loop)
    text = to_prometheus(snap)

    assert "# HELP sluice_in_flight" in text
    assert "# TYPE sluice_in_flight gauge" in text
    assert "sluice_in_flight 0" in text
    assert 'sluice_band{state="normal"} 1' in text
    assert 'sluice_band{state="boxed"} 0' in text
    assert 'sluice_breaker{state="closed"} 1' in text


async def test_no_request_body_in_status_payload():
    """The status payload must never contain request or response body text.

    This is the 'inert in-path' guarantee made visible (Plan 002 WI-001).
    """
    secret_body_text = "THIS_IS_SECRET_REQUEST_CONTENT"

    class _AsyncMockTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            await request.aread()
            payload = json.dumps({"secret": secret_body_text}).encode()

            async def gen():
                yield payload

            return httpx.Response(200, content=gen(), headers={"content-type": "application/json"})

    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    upstream_client = httpx.AsyncClient(
        transport=_AsyncMockTransport(),
        timeout=None,
    )
    reconcile = ReconciliationLoop(
        truth_source=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        upstream_client=upstream_client,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        await client.post("/v1/messages", json={"prompt": secret_body_text})

    snap = snapshot(reconcile)
    payload = str(snap.to_dict())

    assert secret_body_text not in payload, "status payload must not contain request body text"