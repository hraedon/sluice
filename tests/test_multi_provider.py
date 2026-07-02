"""Integration tests for Plan 006: proxy feeds headers to truth, CLI --provider.

These tests verify the end-to-end wiring:
- Proxy calls record_response_headers after each response (WI-004).
- A header-driven provider (anthropic) adaptively limits on ratelimit headers.
- The umans path is byte-identical to pre-abstraction (regression gate).
- The CLI --provider flag resolves the correct provider bundle.
"""

from __future__ import annotations

import json

import httpx
import pytest

from sluice.control import (
    AdaptiveConfig,
    AdaptiveSnapshot,
    BreakerConfig,
    BreakerState,
    ControllerConfig,
    ControllerState,
    LimitState,
    UsageReading,
    adaptive_effective_permits,
)
from sluice.gate import PermitGate
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.usage import CachedReading


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeHeaderTruthSource:
    """A HeaderTruthSource that records calls for testing."""

    def __init__(self) -> None:
        self.recorded_headers: list[tuple[dict[str, str], int]] = []
        self._cached: CachedReading | None = None
        self._provider = "anthropic"

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        if self._cached is None:
            return CachedReading(
                reading=LimitState(
                    requests_remaining=0,
                    tokens_remaining=0,
                    provider=self._provider,
                    age_seconds=99999.0,
                ),
                fetched_at_monotonic=now_monotonic - 99999.0,
                ok=False,
            )
        age = now_monotonic - self._cached.fetched_at_monotonic
        return CachedReading(
            reading=LimitState(provider=self._provider, age_seconds=age),
            fetched_at_monotonic=self._cached.fetched_at_monotonic,
            ok=True,
        )

    @property
    def last_cached(self) -> CachedReading | None:
        return self._cached

    def record_response_headers(
        self, headers: dict[str, str], status: int, *, now_monotonic: float
    ) -> None:
        self.recorded_headers.append((dict(headers), status))
        self._cached = CachedReading(
            reading=LimitState(
                requests_remaining=100,
                tokens_remaining=50000,
                provider=self._provider,
                age_seconds=0.0,
            ),
            fetched_at_monotonic=now_monotonic,
            ok=True,
        )

    async def close(self) -> None:
        pass


class FakeNullTruthSource:
    """A NullTruthSource that records calls for testing."""

    def __init__(self) -> None:
        self.recorded_headers: list[tuple[dict[str, str], int]] = []
        self._cached: CachedReading | None = None
        self._provider = "generic"

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        if self._cached is None:
            self._cached = CachedReading(
                reading=LimitState(provider=self._provider, age_seconds=0.0),
                fetched_at_monotonic=now_monotonic,
                ok=True,
            )
        return self._cached

    @property
    def last_cached(self) -> CachedReading | None:
        return self._cached

    def record_response_headers(
        self, headers: dict[str, str], status: int, *, now_monotonic: float
    ) -> None:
        self.recorded_headers.append((dict(headers), status))

    async def close(self) -> None:
        pass


def _resp(status: int = 200, *, json_data=None, headers=None) -> httpx.Response:
    payload = json.dumps(json_data or {"ok": True}).encode()
    h = dict(headers or {})
    h.setdefault("content-type", "application/json")

    async def gen():
        yield payload

    return httpx.Response(status, content=gen(), headers=h)


def _make_app_with_truth(
    truth_source,
    *,
    gate_capacity: int = 3,
    controller: str = "adaptive",
    upstream_handler=None,
) -> tuple[ProxyApp, PermitGate, ReconciliationLoop]:
    gate = PermitGate(initial_capacity=gate_capacity)
    upstream_client = httpx.AsyncClient(timeout=None)
    reconcile = ReconciliationLoop(
        truth_source=truth_source,
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
        controller=controller,
        adaptive_config=AdaptiveConfig(target=3) if controller == "adaptive" else None,
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
        upstream_client=upstream_client,
    )
    return app, gate, reconcile


def _default_handler(request: httpx.Request) -> httpx.Response:
    return _resp(200)


def _asgi_client(app: ProxyApp) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


# ---------------------------------------------------------------------------
# WI-004: Proxy feeds response headers to truth
# ---------------------------------------------------------------------------


async def test_proxy_records_response_headers():
    """The proxy calls record_response_headers after each response."""
    truth = FakeHeaderTruthSource()

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(200, headers={
            "anthropic-ratelimit-requests-remaining": "100",
            "anthropic-ratelimit-tokens-remaining": "50000",
        })

    app, _, _ = _make_app_with_truth(truth, upstream_handler=handler)

    # Use a mock transport instead
    app._client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: handler(req)),
        timeout=None,
    )

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 200
    assert len(truth.recorded_headers) == 1
    recorded, status = truth.recorded_headers[0]
    assert status == 200
    assert recorded.get("anthropic-ratelimit-requests-remaining") == "100"


async def test_proxy_records_headers_on_429():
    """The proxy records headers even on 429 responses."""
    truth = FakeHeaderTruthSource()

    def handler(request: httpx.Request) -> httpx.Response:
        return _resp(429, headers={"retry-after": "0"})

    app, _, reconcile = _make_app_with_truth(truth, upstream_handler=handler)
    app._client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: handler(req)),
        timeout=None,
    )

    async with _asgi_client(app) as client:
        response = await client.post("/v1/messages", json={"prompt": "hi"})

    assert response.status_code == 429
    assert reconcile.total_429s == 1  # concurrency 429 recorded
    assert len(truth.recorded_headers) == 1
    _, status = truth.recorded_headers[0]
    assert status == 429


async def test_polled_truth_source_ignores_response_headers():
    """For umans (polled truth), record_response_headers is a no-op."""
    from sluice.providers import PolledTruthSource
    import httpx

    ts = PolledTruthSource(
        base_url="https://api.example.com",
        api_key="test-key",
        transport=httpx.MockTransport(
            lambda req: httpx.Response(200, json={
                "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
                "usage": {"concurrent_sessions": 1, "priority": {"low": False}},
            })
        ),
    )
    ts.record_response_headers(
        {"anthropic-ratelimit-requests-remaining": "100"},
        200,
        now_monotonic=1000.0,
    )
    cached = await ts.fetch(now_monotonic=1000.0)
    # The poll is the truth — header data is not reflected.
    assert cached.reading.requests_remaining is None
    await ts.close()


# ---------------------------------------------------------------------------
# WI-005: CLI --provider flag
# ---------------------------------------------------------------------------


def test_provider_flag_default_is_umans():
    """The default provider is umans."""
    from sluice.cli import _DEFAULTS
    assert _DEFAULTS["provider"] == "umans"


def test_provider_flag_choices():
    """The --provider flag accepts the four known providers."""
    from sluice.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["serve", "--provider", "anthropic"])
    assert args.provider == "anthropic"


def test_provider_flag_rejects_unknown():
    """An unknown provider is rejected by argparse."""
    from sluice.cli import build_parser
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["serve", "--provider", "nonexistent"])


async def test_status_shows_provider_and_controller():
    """The status snapshot includes provider and controller in config."""
    from sluice.status import snapshot

    truth = FakeHeaderTruthSource()
    truth._cached = CachedReading(
        reading=LimitState(
            requests_remaining=100, tokens_remaining=50000,
            provider="anthropic", age_seconds=0.0,
        ),
        fetched_at_monotonic=1000.0,
        ok=True,
    )
    gate = PermitGate(initial_capacity=3)
    reconcile = ReconciliationLoop(
        truth_source=truth,
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
        controller="adaptive",
        adaptive_config=AdaptiveConfig(target=3),
        monotonic_clock=lambda: 1000.0,
        wall_clock=lambda: 1_000_000.0,
    )
    reconcile._first_poll_ok = True

    # Run a tick so the reading is populated
    await reconcile.tick()

    snap = snapshot(reconcile)
    d = snap.to_dict()
    assert d["config"]["provider"] == "anthropic"
    assert d["config"]["controller"] == "adaptive"


# ---------------------------------------------------------------------------
# Regression gate: umans controller trace unchanged
# ---------------------------------------------------------------------------


def test_umans_regression_effective_permits_unchanged():
    """The umans controller (effective_permits) produces the same traces
    as before the abstraction. This is the primary regression gate.

    Tests a representative trajectory: normal → phantom → low → stale → boxed.
    """
    from sluice.control import (
        ControllerConfig,
        ControllerState,
        effective_permits,
    )

    cfg = ControllerConfig(target=3, min_floor=1, usage_fresh_ttl=15.0,
                           stale_penalty=1, low_penalty=1, phantom_window=3)
    now = 1_000_000.0

    # Normal: observed=2, local=2 → target
    r = UsageReading(concurrent_sessions=2, limit=4, hard_cap=8)
    s = ControllerState(reading=r, local_in_flight=2)
    assert effective_permits(s, cfg, now=now) == 3

    # Phantom: observed=6, local=4 → 3-2=1
    r = UsageReading(concurrent_sessions=6, limit=4, hard_cap=8)
    s = ControllerState(reading=r, local_in_flight=4, phantom_estimate=2)
    assert effective_permits(s, cfg, now=now) == 1

    # Priority low: observed=4, priority_low=True → 3-1=2
    r = UsageReading(concurrent_sessions=4, limit=4, hard_cap=8, priority_low=True)
    s = ControllerState(reading=r, local_in_flight=4)
    assert effective_permits(s, cfg, now=now) == 2

    # Stale: age=60 → 3-1=2
    r = UsageReading(concurrent_sessions=0, limit=4, hard_cap=8, age_seconds=60.0)
    s = ControllerState(reading=r, local_in_flight=0)
    assert effective_permits(s, cfg, now=now) == 2

    # Boxed: 0
    r = UsageReading(concurrent_sessions=0, limit=4, hard_cap=8, boxed_until_epoch=now + 60)
    s = ControllerState(reading=r, local_in_flight=0)
    assert effective_permits(s, cfg, now=now) == 0

    # Breaker open: 0
    r = UsageReading(concurrent_sessions=0, limit=4, hard_cap=8)
    s = ControllerState(reading=r, local_in_flight=0, breaker=BreakerState.OPEN)
    assert effective_permits(s, cfg, now=now) == 0

    # Half-open: 1
    r = UsageReading(concurrent_sessions=0, limit=4, hard_cap=8)
    s = ControllerState(reading=r, local_in_flight=0, breaker=BreakerState.HALF_OPEN)
    assert effective_permits(s, cfg, now=now) == 1


# ---------------------------------------------------------------------------
# Adaptive controller integration through reconcile loop
# ---------------------------------------------------------------------------


async def test_adaptive_reconcile_loop_backs_off_on_429():
    """The reconcile loop uses AIMD and backs off when 429s are recorded."""
    truth = FakeHeaderTruthSource()
    truth._cached = CachedReading(
        reading=LimitState(
            requests_remaining=100, tokens_remaining=50000,
            provider="anthropic", age_seconds=0.0,
        ),
        fetched_at_monotonic=1000.0,
        ok=True,
    )

    gate = PermitGate(initial_capacity=3)
    reconcile = ReconciliationLoop(
        truth_source=truth,
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
        controller="adaptive",
        adaptive_config=AdaptiveConfig(target=3),
        monotonic_clock=lambda: 1000.0,
        wall_clock=lambda: 1_000_000.0,
    )

    # First tick: permits should increase (no bad signals)
    await reconcile.tick()
    assert gate.capacity >= 1

    # Record a 429
    reconcile.record_429()

    # Next tick: 429 → backoff
    await reconcile.tick()
    assert gate.capacity <= 2  # backed off from previous level


async def test_adaptive_reconcile_loop_increases_when_healthy():
    """The reconcile loop increases permits when healthy."""
    truth = FakeHeaderTruthSource()
    truth._cached = CachedReading(
        reading=LimitState(
            requests_remaining=100, tokens_remaining=50000,
            provider="anthropic", age_seconds=0.0,
        ),
        fetched_at_monotonic=1000.0,
        ok=True,
    )

    gate = PermitGate(initial_capacity=0)
    reconcile = ReconciliationLoop(
        truth_source=truth,
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
        controller="adaptive",
        adaptive_config=AdaptiveConfig(target=3),
        monotonic_clock=lambda: 1000.0,
        wall_clock=lambda: 1_000_000.0,
    )

    # Start at 2 (default AdaptiveSnapshot=1, first tick increases to 2)
    await reconcile.tick()
    assert gate.capacity == 2

    # Next tick: increase to 3 (target)
    await reconcile.tick()
    assert gate.capacity == 3

    # Next tick: stays at 3 (capped at target)
    await reconcile.tick()
    assert gate.capacity == 3


# ---------------------------------------------------------------------------
# Critical fix tests: C1 (readiness), C2 (null freshness), C3 (429 recovery)
# ---------------------------------------------------------------------------


async def test_header_truth_source_ready_on_first_tick():
    """C1 fix: HeaderTruthSource makes the reconcile loop ready on first tick.

    Before the fix, HeaderTruthSource returned ok=False when no headers were
    seen, creating a deadlock: ready stays False → proxy fast-fails →
    record_response_headers never called → headers never seen.
    """
    from sluice.providers import HeaderTruthSource

    truth = HeaderTruthSource(provider="anthropic")
    gate = PermitGate(initial_capacity=0)
    reconcile = ReconciliationLoop(
        truth_source=truth,
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
        controller="adaptive",
        adaptive_config=AdaptiveConfig(target=3),
        monotonic_clock=lambda: 1000.0,
        wall_clock=lambda: 1_000_000.0,
    )

    assert reconcile.ready is False

    await reconcile.tick()

    assert reconcile.ready is True  # C1 fix: ready after first tick
    # The gate should be at min_floor (conservative — requests_remaining=0)
    assert gate.capacity >= 1


async def test_null_truth_source_stays_fresh():
    """C2 fix: NullTruthSource returns a fresh reading each tick.

    Before the fix, the fetched_at_monotonic was frozen, causing age to
    grow unboundedly and the AIMD controller to permanently back off.
    """
    from sluice.providers import NullTruthSource

    truth = NullTruthSource(provider="generic")
    gate = PermitGate(initial_capacity=0)

    mono = [1000.0]
    reconcile = ReconciliationLoop(
        truth_source=truth,
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
        controller="adaptive",
        adaptive_config=AdaptiveConfig(target=3, fresh_ttl=15.0),
        monotonic_clock=lambda: mono[0],
        wall_clock=lambda: 1_000_000.0,
    )

    # First tick: permits should increase (no bad signals, fresh reading)
    await reconcile.tick()
    first_cap = gate.capacity
    assert first_cap >= 1

    # Advance time past fresh_ttl — without the fix, the reading would be
    # stale (age > fresh_ttl) and the controller would back off.
    mono[0] += 20.0
    await reconcile.tick()
    # With the fix, NullTruthSource returns a fresh reading (age=0),
    # so the controller should still be healthy and not back off.
    assert gate.capacity >= first_cap


async def test_adaptive_recovers_after_429_window_expires():
    """C3 fix: AIMD controller recovers after the 429 window expires.

    Before the fix, _recent_429s was not pruned during tick(), so a single
    429 kept the controller in permanent multiplicative-decrease.
    """
    truth = FakeHeaderTruthSource()
    truth._cached = CachedReading(
        reading=LimitState(
            requests_remaining=100, tokens_remaining=50000,
            provider="anthropic", age_seconds=0.0,
        ),
        fetched_at_monotonic=1000.0,
        ok=True,
    )

    gate = PermitGate(initial_capacity=0)
    mono = [1000.0]
    reconcile = ReconciliationLoop(
        truth_source=truth,
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(threshold=5, window_seconds=10.0, cooldown_seconds=60.0),
        controller="adaptive",
        adaptive_config=AdaptiveConfig(target=3, fresh_ttl=15.0),
        monotonic_clock=lambda: mono[0],
        wall_clock=lambda: 1_000_000.0,
    )

    # Tick to ramp up
    await reconcile.tick()
    await reconcile.tick()
    assert gate.capacity >= 2

    # Record a 429 → AIMD backs off
    reconcile.record_429()
    await reconcile.tick()
    assert gate.capacity <= 2  # backed off

    # Advance past the breaker window (10s) — the 429 should be pruned
    mono[0] += 15.0
    await reconcile.tick()
    # With the fix, the 429 is pruned and the controller increases again
    assert gate.capacity >= 2  # recovering


async def test_low_remaining_fraction_triggers_backoff():
    """M1 fix: low_remaining_fraction triggers backoff when remaining is low.

    With limit=1000 and remaining=50 (5% < 20% threshold), the controller
    should back off.
    """
    from sluice.control import AdaptiveConfig as _AC

    cfg = _AC(target=3, low_remaining_fraction=0.2)
    snap = AdaptiveSnapshot(current_permits=3)
    ls = LimitState(
        requests_limit=1000, requests_remaining=50,
        tokens_limit=10000, tokens_remaining=5000,
        provider="anthropic", age_seconds=0.0,
    )
    state = ControllerState(reading=ls, local_in_flight=0)
    permits, _ = adaptive_effective_permits(state, snap, cfg, now=1_000_000.0)
    assert permits < 3  # backed off due to low remaining fraction


async def test_record_response_headers_ignores_empty_responses():
    """M3 fix: record_response_headers doesn't overwrite with empty data.

    A 500 response with no ratelimit headers should not produce a fresh
    ok=True reading (which would cause the AIMD controller to increase).
    """
    from sluice.providers import HeaderTruthSource

    ts = HeaderTruthSource(provider="anthropic")
    # Record a healthy response first
    ts.record_response_headers(
        {"anthropic-ratelimit-requests-remaining": "100",
         "anthropic-ratelimit-requests-limit": "1000"},
        status=200,
        now_monotonic=1000.0,
    )
    cached1 = await ts.fetch(now_monotonic=1000.0)
    assert cached1.reading.requests_remaining == 100

    # Record an error response with no ratelimit headers
    ts.record_response_headers({}, status=500, now_monotonic=1005.0)
    cached2 = await ts.fetch(now_monotonic=1005.0)
    # The old data should be preserved (not overwritten with None)
    assert cached2.reading.requests_remaining == 100
