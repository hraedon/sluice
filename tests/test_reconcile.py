"""Tests for the reconciliation loop: phantom absorb/clear, box, staleness, breaker.

Uses a fake clock and a fake usage source — no network.
"""

from __future__ import annotations

import asyncio

import pytest

from sluice.control import (
    Band,
    BreakerConfig,
    ControllerConfig,
    UsageReading,
)
from sluice.gate import PermitGate
from sluice.reconcile import ReconciliationLoop
from sluice.usage import CachedReading

CFG = ControllerConfig(target=3, min_floor=1, usage_fresh_ttl=15.0, stale_penalty=1, low_penalty=1, phantom_window=3)
BCFG = BreakerConfig(threshold=5, window_seconds=300.0, cooldown_seconds=60.0)


class FakeUsageClient:
    """Controllable usage source: can serve readings, fail, or go stale."""

    def __init__(self, reading: UsageReading) -> None:
        self._reading = reading
        self._fail = False
        self._last_ok_mono = 0.0
        self.fetch_count = 0

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        self.fetch_count += 1
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
        return CachedReading(reading=self._reading, fetched_at_monotonic=self._last_ok_mono, ok=True) if self._last_ok_mono else None

    def record_response_headers(self, headers, status, *, now_monotonic) -> None:
        pass

    async def close(self) -> None:
        pass


def _reading(**kw) -> UsageReading:
    base: dict[str, object] = dict(concurrent_sessions=0, limit=4, hard_cap=8)
    base.update(kw)
    return UsageReading(**base)  # type: ignore[arg-type]


def _make_loop(initial: UsageReading, *, mono: float = 1000.0, wall: float = 1_000_000.0):
    m = [mono]
    w = [wall]
    client = FakeUsageClient(initial)
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=5.0,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
    )
    return loop, client, gate, m, w


# --- phantom absorb / clear -----------------------------------------------


async def test_phantom_appears_gate_shrinks():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=6))

    # Provider sees 6, sluice holds 0 → 6 phantoms → 3-6 = -3 → clamp to min_floor=1
    await loop.tick()
    assert gate.capacity == 1


async def test_phantom_clears_gate_reopens():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=6))

    await loop.tick()
    assert gate.capacity == 1

    # Phantom clears.
    client.set_reading(_reading(concurrent_sessions=0))
    await loop.tick()
    assert gate.capacity == 3  # back to target


async def test_normal_steady_state():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=2))

    # Simulate 2 permits held so observed == local → 0 phantoms.
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    await loop.tick()
    assert gate.capacity == 3  # target


# --- box -------------------------------------------------------------------


async def test_box_closes_gate():
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, boxed_until_epoch=1_000_100.0)
    )
    await loop.tick()
    assert gate.capacity == 0
    assert loop.band is Band.BOXED


async def test_box_elapsed_gate_reopens():
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, boxed_until_epoch=1_000_000.0)
    )
    await loop.tick()
    assert gate.capacity == 3  # not boxed anymore
    assert loop.band is Band.NORMAL


# --- staleness -------------------------------------------------------------


async def test_stale_reading_tightens():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))

    # Fresh → target.
    await loop.tick()
    assert gate.capacity == 3

    # Fail the fetch → LKG served with old timestamp → stale.
    client.set_fail(True)
    m[0] += 100  # 100s later, TTL is 15s → stale
    await loop.tick()
    assert gate.capacity == 2  # target - stale_penalty = 3 - 1


async def test_stale_reading_with_rate_limit_429s_tightens_to_min_floor():
    """Safety net: stale reading + recent rate_limit 429s → gate at min_floor.

    Without this, sluice would forward at target - stale_penalty (2) into an
    upstream that is actively 429ing — a fail-open window (AGENTS.md rule 1).
    The breaker can't help because rate_limit 429s don't feed it; this shell-
    level override is the backstop.
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))

    # Fresh → target.
    await loop.tick()
    assert gate.capacity == 3

    # Rate-limit 429s arrive (these do NOT feed the breaker).
    loop.record_rate_limit_429()
    loop.record_rate_limit_429()

    # Poll fails → stale reading.
    client.set_fail(True)
    m[0] += 100  # stale
    await loop.tick()
    assert gate.capacity == 1  # min_floor, not target - stale_penalty (2)


async def test_fresh_reading_with_rate_limit_429s_does_not_overtighten():
    """Fresh reading + rate_limit 429s → normal gate (poll sees the truth).

    The safety net only activates when the reading is stale.  When fresh,
    the poll's concurrent_sessions is trusted — if it says 0, the gate
    stays at target.
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))

    await loop.tick()
    assert gate.capacity == 3

    loop.record_rate_limit_429()
    loop.record_rate_limit_429()

    # Fresh reading (poll succeeds).
    await loop.tick()
    assert gate.capacity == 3  # no overtightening when fresh


async def test_rate_limit_429s_aging_out_releases_safety_net():
    """After rate_limit 429s age out of the breaker window, the safety net releases.

    Without this, a single burst of rate_limit 429s would pin the gate at
    min_floor forever (fail-closed stuck state).
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))

    # Fresh → target.
    await loop.tick()
    assert gate.capacity == 3

    # Rate-limit 429s arrive.
    loop.record_rate_limit_429()
    loop.record_rate_limit_429()

    # Poll fails → stale → safety net tightens to min_floor.
    client.set_fail(True)
    m[0] += 100  # stale
    await loop.tick()
    assert gate.capacity == 1  # min_floor

    # Advance past the breaker window (300s) → 429s age out.
    client.set_fail(False)
    m[0] += 310
    await loop.tick()
    # Fresh reading, no recent rate_limit 429s → safety net releases.
    assert gate.capacity == 3  # back to target


async def test_stale_reading_with_rate_limit_429s_tightens_adaptive_to_min_floor():
    """Adaptive controller safety net: stale + rate_limit 429s → min_floor.

    Same rationale as the concurrency reconciler: the AIMD stale-decrease is
    gated by min_decrease_interval (30s), so permits can be held steady into
    a rejecting upstream — a fail-open window (AGENTS.md rule 1).
    """
    from sluice.control import AdaptiveConfig

    initial = _reading(concurrent_sessions=0, provider="anthropic")
    m = [1000.0]
    w = [1_000_000.0]
    client = FakeUsageClient(initial)
    gate = PermitGate(initial_capacity=3, release_cooldown=0.0, clock=lambda: m[0])
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=5.0,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
        controller="adaptive",
        adaptive_config=AdaptiveConfig(target=3, min_floor=1),
    )

    # Fresh → AIMD additive increase (starts at 1, +1 per tick).
    await loop.tick()
    assert gate.capacity == 2  # 1 + additive_step
    await loop.tick()
    assert gate.capacity == 3  # 2 + additive_step, capped at target

    # Rate-limit 429s arrive.
    loop.record_rate_limit_429()

    # Poll fails → stale → safety net tightens to min_floor.
    client.set_fail(True)
    m[0] += 100  # stale
    await loop.tick()
    assert gate.capacity == 1  # adaptive min_floor


async def test_idle_not_set_when_rate_limit_429s_recent():
    """Idle detection must consider rate_limit 429s, not just concurrency 429s.

    Without this, the system declares itself idle and slows the poll cadence
    while rate_limit 429s are actively arriving — the poll is woken by each 429,
    but the cadence oscillates unnecessarily.
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))

    await loop.tick()
    assert loop.is_idle  # truly idle

    loop.record_rate_limit_429()

    # Fresh tick with rate_limit 429 → not idle.
    await loop.tick()
    assert not loop.is_idle


async def test_rate_limit_429_does_not_retrip_half_open_breaker():
    """HALF_OPEN breaker + rate_limit 429 → stays HALF_OPEN (not re-tripped).

    rate_limit 429s don't call breaker_on_429, so a half-open breaker
    stays half-open.  The probe timeout (30s) is the backstop that
    eventually re-trips to OPEN if no success arrives.
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    bcfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)
    loop._brk_cfg = bcfg

    # Trip with concurrency 429s → OPEN.
    for _ in range(3):
        loop.record_429()
    await loop.tick()
    assert gate.capacity == 0

    # Advance past cooldown → HALF_OPEN.
    m[0] += 100
    await loop.tick()
    assert gate.capacity == 1  # half-open probe

    # rate_limit 429 arrives — breaker stays HALF_OPEN (not re-tripped).
    loop.record_rate_limit_429()
    from sluice.control import BreakerState
    assert loop._breaker.state is BreakerState.HALF_OPEN


# --- breaker integration ---------------------------------------------------


async def test_429_trips_breaker_closes_gate():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    bcfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)
    loop._brk_cfg = bcfg  # lower threshold for test

    await loop.tick()
    assert gate.capacity == 3

    # Record 3 429s → breaker trips.
    for _ in range(3):
        loop.record_429()

    await loop.tick()
    assert gate.capacity == 0  # breaker open


async def test_breaker_half_open_admits_one():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    bcfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)
    loop._brk_cfg = bcfg

    # Trip the breaker.
    for _ in range(3):
        loop.record_429()
    await loop.tick()
    assert gate.capacity == 0

    # Advance past cooldown → half-open.
    m[0] += 100
    await loop.tick()
    assert gate.capacity == 1  # half-open probe


async def test_breaker_success_closes():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    bcfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)
    loop._brk_cfg = bcfg

    # Trip → half-open.
    for _ in range(3):
        loop.record_429()
    await loop.tick()
    m[0] += 100
    await loop.tick()
    assert gate.capacity == 1

    # Probe succeeds → closed.
    loop.record_success()
    await loop.tick()
    assert gate.capacity == 3  # back to target


# --- gate basics -----------------------------------------------------------


async def test_gate_acquire_release():
    gate = PermitGate(initial_capacity=2)
    assert await gate.acquire(timeout=0.1) is True
    assert await gate.acquire(timeout=0.1) is True
    assert gate.held == 2
    assert await gate.acquire(timeout=0.05) is False  # at capacity, timeout
    await gate.release()
    assert gate.held == 1
    assert await gate.acquire(timeout=0.1) is True  # slot reopened


async def test_gate_resize_does_not_revoke():
    gate = PermitGate(initial_capacity=3)
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    assert gate.held == 2

    await gate.resize(1)  # shrink below held
    assert gate.held == 2  # not revoked
    assert gate.available == 0  # no new grants

    await gate.release()
    assert gate.held == 1
    assert gate.available == 0  # still at capacity


async def test_gate_release_cooldown():
    gate = PermitGate(initial_capacity=1, release_cooldown=10.0)
    assert await gate.acquire(timeout=0.1) is True
    await gate.release()
    assert gate.held == 0
    assert gate.cooling_down == 1
    assert gate.available == 0  # in cooldown, not reusable yet


# --- windowed phantom estimation (Plan 003) --------------------------------


async def test_churn_does_not_shrink_gate():
    """Under churn at/under target, the gate does not dip below target.

    A request that completed moments ago has left local_in_flight but still sits
    in the lagged observed. The windowed min drops this transient spike.
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=3))

    # Tick 1: observed=3, local=0 → instant phantom=3, but window has 1 sample.
    # With K=3, a single sample's excess survives (min of 1 sample = that sample).
    # So gate shrinks. This is correct — we can't know yet if it's transient.
    await loop.tick()

    # Tick 2: observed drops to 0 (request gone from provider's view), local=0
    client.set_reading(_reading(concurrent_sessions=0))
    await loop.tick()
    # Now samples = [(3,0), (0,0)] → min(3, 0) = 0 → no phantom → gate at target
    assert gate.capacity == 3


async def test_sustained_phantom_shrinks_gate():
    """A phantom present in every sample survives the windowed min."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=5))

    # Fill the window with sustained excess: observed=5, local=3 → phantom=2
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    # held=3, observed=5 → phantom=2 → target(3) - 2 = 1
    await loop.tick()
    assert gate.capacity == 1

    # Next tick: still sustained
    await loop.tick()
    assert gate.capacity == 1


async def test_single_tick_spike_does_not_persist():
    """A one-tick spike in an otherwise-clean window doesn't keep the gate down."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=5))

    # Tick 1: spike (observed=5, local=3) → phantom=2
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    await loop.tick()
    assert gate.capacity == 1  # shrunk due to spike

    # Tick 2-3: phantom clears
    client.set_reading(_reading(concurrent_sessions=3))
    await loop.tick()
    await loop.tick()
    # Window now has clean samples → phantom=0 → gate back to target
    assert gate.capacity == 3


# --- gate_closed_reason + retry_after_seconds (Plan 003 WI-003) -------------


async def test_gate_closed_reason_open():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=2))
    await loop.tick()
    assert loop.gate_closed_reason() == "open"


async def test_gate_closed_reason_boxed():
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, boxed_until_epoch=1_000_100.0, resets_at_epoch=1_000_100.0)
    )
    await loop.tick()
    assert loop.gate_closed_reason() == "boxed"


async def test_gate_closed_reason_rate_limited_is_not_boxed():
    # Deprioritization rung: gate stays open at reduced permits; the proxy
    # must NOT fast-fail (docs/wi-024-429-capture-2026-07-03.md).
    loop, client, gate, m, w = _make_loop(
        _reading(
            concurrent_sessions=0,
            boxed_until_epoch=1_000_100.0,
            priority_low=True,
            priority_reason="rate_limited",
        )
    )
    await loop.tick()
    assert loop.gate_closed_reason() != "boxed"
    assert gate.capacity >= 1


async def test_gate_closed_reason_boxed_elapsed_is_open():
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, boxed_until_epoch=1_000_000.0)
    )
    await loop.tick()
    assert loop.gate_closed_reason() == "open"


async def test_gate_closed_reason_breaker():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    bcfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)
    loop._brk_cfg = bcfg

    await loop.tick()
    for _ in range(3):
        loop.record_429()
    await loop.tick()
    assert loop.gate_closed_reason() == "breaker"


async def test_retry_after_seconds_boxed_with_resets_at():
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, boxed_until_epoch=1_000_100.0, resets_at_epoch=1_000_100.0)
    )
    await loop.tick()
    # resets_at - now_wall = 1_000_100 - 1_000_000 = 100
    assert loop.retry_after_seconds() == 100


async def test_retry_after_seconds_boxed_without_resets_at():
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, boxed_until_epoch=1_000_100.0)
    )
    await loop.tick()
    # No resets_at → floor of 30
    assert loop.retry_after_seconds() == 30


async def test_retry_after_seconds_breaker():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    bcfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)
    loop._brk_cfg = bcfg

    await loop.tick()
    for _ in range(3):
        loop.record_429()
    await loop.tick()
    # Just tripped → cooldown is ~60s remaining
    ra = loop.retry_after_seconds()
    assert 55 <= ra <= 60


async def test_retry_after_seconds_open_is_default():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=2))
    await loop.tick()
    assert loop.retry_after_seconds() == 5


# --- WI-006: stale resets_at capped when cached.ok=False ---------------------


async def test_retry_after_seconds_stale_capped():
    """When the cached reading is stale (ok=False), retry_after is capped at 300."""
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, boxed_until_epoch=1_001_000.0, resets_at_epoch=1_001_000.0)
    )
    await loop.tick()

    client.set_fail(True)
    m[0] += 1
    await loop.tick()

    # resets_at = 1_001_000, now_wall = 1_000_000 → remaining = 1000
    # Stale (ok=False) → capped at 300
    assert loop.retry_after_seconds() == 300


async def test_retry_after_seconds_fresh_not_capped():
    """When the cached reading is fresh (ok=True), retry_after is not capped."""
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, boxed_until_epoch=1_001_000.0, resets_at_epoch=1_001_000.0)
    )
    await loop.tick()

    # resets_at = 1_001_000, now_wall = 1_000_000 → remaining = 1000
    # Fresh (ok=True) → not capped
    assert loop.retry_after_seconds() == 1000


# --- WI-017: phantom sample timing uses held_at_fetch, not current held ---------


async def test_phantom_sample_uses_held_at_fetch_not_current():
    """The phantom sample must pair observed with held-at-fetch-time.

    If permits are acquired during the fetch's network I/O, the current
    gate.held will be higher than it was when the provider counted sessions.
    Pairing the lagged observed with the post-fetch held would mask the
    phantom (excess = observed - held_at_fetch = 5 - 3 = 2, but
    observed - current_held = 5 - 5 = 0), causing a fail-open.
    """

    class SlowUsageClient:
        """Usage client with a delay to simulate network I/O during fetch."""

        def __init__(self, reading: UsageReading) -> None:
            self._reading = reading
            self.fetch_count = 0

        async def fetch(self, *, now_monotonic: float) -> CachedReading:
            self.fetch_count += 1
            await asyncio.sleep(0.05)
            return CachedReading(
                reading=self._reading, fetched_at_monotonic=now_monotonic, ok=True
            )

        @property
        def last_cached(self) -> CachedReading | None:
            return None

        def record_response_headers(self, headers, status, *, now_monotonic) -> None:
            pass

        async def close(self) -> None:
            pass

    import asyncio

    client = SlowUsageClient(_reading(concurrent_sessions=5))
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0)
    loop = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=5.0,
        monotonic_clock=lambda: 1000.0,
        wall_clock=lambda: 1_000_000.0,
    )

    # Acquire 3 permits → held=3 at fetch time
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)

    # Start the tick (fetch will sleep 50ms)
    tick_task = asyncio.create_task(loop.tick())

    # Let the fetch start, then acquire 2 more permits during the I/O
    await asyncio.sleep(0.01)
    await gate.acquire(timeout=0.1)
    await gate.acquire(timeout=0.1)  # held is now 5

    await tick_task

    # With the fix: held_at_fetch=3, sample (5,3), excess=2 → phantom=2, gate=1
    # Without the fix: held=current=5, sample (5,5), excess=0 → phantom=0, gate=3
    assert gate.capacity == 1, (
        f"phantom should be detected (held_at_fetch=3, observed=5 → excess=2), "
        f"got capacity={gate.capacity}"
    )


# --- WI-020: HALF_OPEN → OPEN on probe timeout (integration) -----------------


async def test_breaker_half_open_times_out_to_open():
    """Breaker transitions from HALF_OPEN to OPEN after probe timeout."""
    from sluice.control import BreakerState

    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    loop._brk_cfg = BreakerConfig(
        threshold=3, window_seconds=300.0, cooldown_seconds=60.0, probe_timeout_seconds=30.0
    )

    # Trip the breaker.
    for _ in range(3):
        loop.record_429()
    await loop.tick()
    assert gate.capacity == 0

    # Advance past cooldown → half-open.
    m[0] += 100
    await loop.tick()
    assert gate.capacity == 1  # half-open probe

    # Advance past probe timeout without any event → back to OPEN.
    m[0] += 40
    await loop.tick()
    assert gate.capacity == 0  # breaker open again
    assert loop.breaker_state is BreakerState.OPEN


# --- Concurrent record_429 during tick() -----------------------------------


async def test_concurrent_429_during_tick_does_not_lose_event():
    """A 429 arriving concurrently with tick() must not be lost.

    The fix (WI-016) reads self._breaker *after* the fetch so concurrent
    record_429 updates land between the fetch and the tick's breaker_on_tick
    call.  This test exercises actual concurrency: a 429 is recorded while
    the tick's fetch is in-flight.
    """
    import asyncio

    class SlowUsageClient:
        """Usage client with a delay to create a real concurrency window."""

        def __init__(self, reading: UsageReading) -> None:
            self._reading = reading
            self.fetch_count = 0

        async def fetch(self, *, now_monotonic: float) -> CachedReading:
            self.fetch_count += 1
            await asyncio.sleep(0.05)
            return CachedReading(
                reading=self._reading, fetched_at_monotonic=now_monotonic, ok=True
            )

        @property
        def last_cached(self) -> CachedReading | None:
            return None

        def record_response_headers(self, headers, status, *, now_monotonic) -> None:
            pass

        async def close(self) -> None:
            pass

    client = SlowUsageClient(_reading(concurrent_sessions=0))
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0)
    loop = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=CFG,
        breaker_config=BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0),
        poll_interval=5.0,
        monotonic_clock=lambda: 1000.0,
        wall_clock=lambda: 1_000_000.0,
    )

    # Pre-load 2 429s (one below threshold)
    loop.record_429()
    loop.record_429()
    assert loop.recent_429_count == 2

    # Start a tick (fetch will sleep 50ms)
    tick_task = asyncio.create_task(loop.tick())

    # Wait until the fetch is in-flight before recording the 429, so we
    # actually exercise the concurrency window the test claims to test.
    while client.fetch_count == 0:
        await asyncio.sleep(0.001)
    loop.record_429()

    # Wait for the tick to complete
    await tick_task

    # The breaker should be OPEN — either from breaker_on_429 (immediate trip)
    # or from breaker_on_tick (seeing 3 recent 429s).  The key assertion is
    # that the 429 was not lost: the breaker tripped.
    from sluice.control import BreakerState
    assert loop.breaker_state is BreakerState.OPEN, (
        f"breaker should be OPEN after concurrent 429, got {loop.breaker_state}"
    )
    assert loop.recent_429_count == 3
    assert gate.capacity == 0, (
        f"gate should be closed when breaker is OPEN, got capacity={gate.capacity}"
    )


# --- retry_after_seconds uses math.ceil (not int truncation) -------------------


async def test_retry_after_seconds_boxed_uses_ceil():
    """ceil(resets_at - now) must round up, not truncate.

    resets_at - now_wall = 30.9 → int() gives 30, math.ceil() gives 31.
    """
    loop, client, gate, m, w = _make_loop(
        _reading(
            concurrent_sessions=0,
            boxed_until_epoch=1_000_030.9,
            resets_at_epoch=1_000_030.9,
        )
    )
    await loop.tick()
    assert loop.retry_after_seconds() == 31


async def test_retry_after_seconds_breaker_uses_ceil():
    """Breaker cooldown_remaining is ceiled, not truncated."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    loop._brk_cfg = BreakerConfig(
        threshold=3, window_seconds=300.0, cooldown_seconds=60.0
    )

    await loop.tick()
    for _ in range(3):
        loop.record_429()
    await loop.tick()

    # Advance 29.5s → cooldown_remaining = 60 - 29.5 = 30.5 → ceil = 31
    m[0] += 29.5
    ra = loop.retry_after_seconds()
    assert ra == 31


# --- request-window tracking & reconciliation -------------------------------


async def test_record_request_forwarded_increments_counter():
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.total_requests_forwarded == 0

    loop.record_request_forwarded()
    loop.record_request_forwarded()
    assert loop.total_requests_forwarded == 2


async def test_request_window_reconciliation():
    """Local count is pruned to the provider's window; delta is computed."""
    loop, client, gate, m, w = _make_loop(
        _reading(
            concurrent_sessions=0,
            requests_limit=200,
            requests_in_window=50,
            requests_remaining=150,
            requests_window_seconds=100,
        )
    )

    # Forward 10 requests at t=1000.
    for _ in range(10):
        loop.record_request_forwarded()
    await loop.tick()

    assert loop.local_requests_in_window == 10
    assert loop.request_window_delta == 40  # provider 50 - local 10

    # Advance past the window — local timestamps should be pruned to 0.
    m[0] += 200  # window is 100s, so all are now outside
    client.set_reading(
        _reading(
            concurrent_sessions=0,
            requests_limit=200,
            requests_in_window=5,
            requests_remaining=195,
            requests_window_seconds=100,
        )
    )
    await loop.tick()

    assert loop.local_requests_in_window == 0
    assert loop.request_window_delta == 5  # provider 5 - local 0


async def test_request_window_no_window_seconds():
    """Without requests_window_seconds, local count and delta are None."""
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=0, requests_limit=200)
    )
    loop.record_request_forwarded()
    await loop.tick()

    assert loop.local_requests_in_window is None
    assert loop.request_window_delta is None


async def test_request_window_delta_stale_reading():
    """Delta is None when the reading is stale (ok=False)."""
    loop, client, gate, m, w = _make_loop(
        _reading(
            concurrent_sessions=0,
            requests_limit=200,
            requests_in_window=50,
            requests_window_seconds=100,
        )
    )
    await loop.tick()
    assert loop.request_window_delta == 50  # provider 50 - local 0

    # Make the next fetch fail — delta should be None (stale reading).
    client.set_fail(True)
    m[0] += 5
    await loop.tick()
    assert loop.request_window_delta is None


# --- Edge cases: extreme observed, zero local, gate-closing conditions -----------


async def test_extreme_observed_floors_gate_at_min_floor():
    """observed >> target * 2: gate shrinks to min_floor, not to 0.

    The phantom absorption eats all permits but _clamp floors at min_floor=1
    so sluice can still forward one request and observe the result.
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=20))
    await loop.tick()
    assert gate.capacity == 1  # min_floor, not 0


async def test_zero_local_during_burst_full_phantom_absorption():
    """local_in_flight=0 with high observed: full phantom absorption.

    All local requests completed but the provider still counts phantoms.
    The windowed estimate equals the full observed count (single sample).
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=7))
    await loop.tick()
    assert gate.capacity == 1  # 3 - 7 = -4 → clamp to min_floor=1


async def test_sustained_extreme_phantom_stays_at_min_floor():
    """Sustained extreme phantom across multiple ticks: gate stays at min_floor."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=15))
    for _ in range(5):
        await loop.tick()
    assert gate.capacity == 1  # never closes fully on phantoms alone


async def test_gate_closed_reason_saturated_when_permits_zero():
    """gate_closed_reason returns 'saturated' when effective_permits is 0 but
    not boxed or breaker."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.effective_permits_count == 3  # normal
    assert loop.gate_closed_reason() == "open"

    loop._last_permits = 0
    assert loop.gate_closed_reason() == "saturated"


async def test_phantom_clears_after_sustained_extreme():
    """After a sustained extreme phantom clears, the gate reopens to target.

    The windowed min drops the phantom once clean samples fill the window.
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=15))

    for _ in range(3):
        await loop.tick()
    assert gate.capacity == 1

    client.set_reading(_reading(concurrent_sessions=0))
    for _ in range(3):
        await loop.tick()
    assert gate.capacity == 3  # back to target


async def test_observed_equals_hard_cap_gives_low_band():
    """observed == hard_cap is LOW band (not REJECT — REJECT is > hard_cap)."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=8))
    await loop.tick()
    assert loop.band is Band.LOW
    assert gate.capacity == 1  # phantom absorption floors at min_floor


async def test_observed_above_hard_cap_gives_reject_band():
    """observed > hard_cap is REJECT band — but the gate still floors at min_floor."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=9))
    await loop.tick()
    assert loop.band is Band.REJECT
    assert gate.capacity == 1  # phantom absorption floors at min_floor


# --- Plan 011: Override store -----------------------------------------------


async def test_apply_override_changes_target():
    """apply_override updates the controller config for the next tick."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.target == 3  # boot value

    warning = loop.apply_override("target", 5)
    assert warning is not None  # 5 > limit=4 → warning
    assert loop.target == 5
    assert "target" in loop.overrides
    assert loop.overrides["target"]["boot"] == 3
    assert loop.overrides["target"]["override"] == 5


async def test_apply_override_resizes_gate_next_tick():
    """After applying an override, the next tick resizes the gate."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert gate.capacity == 3

    loop.apply_override("target", 4)  # at limit, no warning
    await loop.tick()
    assert gate.capacity == 4


async def test_clear_override_reverts_to_boot():
    """clear_override restores the boot config value."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    loop.apply_override("target", 5)
    assert loop.target == 5

    loop.clear_override("target")
    assert loop.target == 3
    assert loop.overrides == {}


async def test_clear_override_no_op_when_none():
    """clear_override on a field with no active override is a no-op."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    loop.clear_override("target")  # should not raise
    assert loop.target == 3


async def test_apply_override_rejects_unknown_field():
    """Non-whitelisted fields are rejected."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    with pytest.raises(ValueError, match="whitelist"):
        loop.apply_override("min_floor", 2)


async def test_apply_override_rejects_above_hard_cap():
    """A target above hard_cap is rejected by validate_target_override."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    with pytest.raises(ValueError, match="hard_cap"):
        loop.apply_override("target", 99)


async def test_override_visible_in_status_snapshot():
    """The override appears in the status snapshot's overrides dict."""
    from sluice.status import snapshot

    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()

    loop.apply_override("target", 5)
    snap = snapshot(loop)
    d = snap.to_dict()
    assert "overrides" in d
    assert d["overrides"]["target"]["boot"] == 3
    assert d["overrides"]["target"]["override"] == 5


async def test_no_override_shows_empty_overrides():
    """When no override is active, overrides is an empty dict."""
    from sluice.status import snapshot

    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()

    snap = snapshot(loop)
    d = snap.to_dict()
    assert d["overrides"] == {}


async def test_apply_override_before_first_tick_rejected():
    """Override is rejected before the first successful usage poll (fail-safe)."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    # No tick yet — _first_poll_ok is False
    with pytest.raises(ValueError, match="first successful usage poll"):
        loop.apply_override("target", 4)


async def test_apply_override_with_stale_reading_rejected():
    """Override is rejected when the latest reading is stale (fail-safe)."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()

    client.set_fail(True)
    await loop.tick()  # this tick gets a stale reading

    with pytest.raises(ValueError, match="stale"):
        loop.apply_override("target", 4)


async def test_clear_override_reverts_adaptive_config():
    """clear_override also reverts the adaptive config target."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    loop._controller = "adaptive"
    loop._adaptive_cfg = type(loop._adaptive_cfg)(target=3)
    await loop.tick()

    loop.apply_override("target", 5)
    assert loop._adaptive_cfg.target == 5

    loop.clear_override("target")
    assert loop._adaptive_cfg.target == 3


# ---------------------------------------------------------------------------
# WI-030: record_response_headers safe after stop
# ---------------------------------------------------------------------------


class _TrackingTruthSource:
    """Truth source that tracks record_response_headers calls and raises if closed."""

    def __init__(self) -> None:
        self.closed = False
        self.record_calls: list[tuple[dict, int, float]] = []

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        return CachedReading(
            reading=_reading(concurrent_sessions=0),
            fetched_at_monotonic=now_monotonic,
            ok=True,
        )

    @property
    def last_cached(self) -> CachedReading | None:
        return None

    def record_response_headers(self, headers, status, *, now_monotonic) -> None:
        if self.closed:
            raise RuntimeError("truth source is closed")
        self.record_calls.append((headers, status, now_monotonic))

    async def close(self) -> None:
        self.closed = True


async def test_record_response_headers_safe_after_stop():
    """WI-030: record_response_headers is a no-op after stop().

    The lifecycle manager calls reconcile.stop() (which closes the truth
    source) before the drain completes. In-flight proxy requests may still
    call record_response_headers. The method must return early instead of
    touching the closed truth source.
    """
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    # Replace with a tracking truth source that raises on use-after-close
    tracking = _TrackingTruthSource()
    loop._truth = tracking

    await loop.tick()
    assert not tracking.closed

    await loop.stop()
    assert tracking.closed, "stop() should close the truth source"

    # This must NOT raise — record_response_headers should be a no-op
    loop.record_response_headers({"x-ratelimit-limit": "100"}, 200)
    assert len(tracking.record_calls) == 0, "no calls should reach the closed truth source"


async def test_record_response_headers_works_before_stop():
    """Sanity check: record_response_headers works normally before stop()."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    tracking = _TrackingTruthSource()
    loop._truth = tracking

    await loop.tick()

    loop.record_response_headers({"x-ratelimit-limit": "100"}, 200)
    assert len(tracking.record_calls) == 1
    assert tracking.record_calls[0][0] == {"x-ratelimit-limit": "100"}


# ---------------------------------------------------------------------------
# Plan 013 WI-003: saturation_retry_after — shell-side jittered estimator
# ---------------------------------------------------------------------------


def _make_loop_with_hold(
    *,
    queue_depth: int = 0,
    capacity: int = 4,
    avg_hold: float = 0.0,
    permits: int = 3,
    poll_interval: float = 5.0,
    rng=None,
):
    """Build a loop with pre-populated gate hold samples and queue depth."""
    from collections import deque

    gate = PermitGate(initial_capacity=capacity)
    # Populate hold samples by direct injection.
    if avg_hold > 0:
        gate._hold_samples = deque([avg_hold] * 10, maxlen=64)
    # Simulate queue depth by setting _waiters.
    gate._waiters = queue_depth

    client = FakeUsageClient(_reading(concurrent_sessions=0))
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=poll_interval,
        monotonic_clock=lambda: 1000.0,
        wall_clock=lambda: 1_000_000.0,
        rng=rng or (lambda: 0.5),  # pinned mid-range by default
    )
    loop._first_poll_ok = True
    loop._last_permits = permits
    return loop, gate


def test_saturation_retry_after_deep_queue_larger_than_shallow():
    """A deeper queue yields a larger saturation Retry-After."""
    shallow, _ = _make_loop_with_hold(
        queue_depth=1, capacity=4, avg_hold=10.0, rng=lambda: 0.5
    )
    deep, _ = _make_loop_with_hold(
        queue_depth=10, capacity=4, avg_hold=10.0, rng=lambda: 0.5
    )
    assert deep.saturation_retry_after() > shallow.saturation_retry_after()


def test_saturation_retry_after_low_pressure_returns_floor():
    """Idle/low-pressure saturation returns the floor (regression-compatible)."""
    loop, _ = _make_loop_with_hold(
        queue_depth=0, capacity=4, avg_hold=1.0, rng=lambda: 0.5
    )
    # ceil(1×1/4)=1, jittered: 1*1.0=1.0 → ceil=1, floored at 5
    assert loop.saturation_retry_after() == 5


def test_saturation_retry_after_no_hold_samples_returns_floor():
    """No hold samples yet → floor (fail safe)."""
    loop, _ = _make_loop_with_hold(
        queue_depth=5, capacity=4, avg_hold=0.0, rng=lambda: 0.5
    )
    assert loop.saturation_retry_after() == 5


def test_saturation_retry_after_jitter_bounds_pinned_rng():
    """Jitter bounds hold with a pinned RNG."""
    # rng=0.0 → jitter factor = 0.85 (minimum)
    loop_min, _ = _make_loop_with_hold(
        queue_depth=3, capacity=2, avg_hold=10.0, rng=lambda: 0.0
    )
    # rng=1.0 → jitter factor = 1.15 (maximum)
    loop_max, _ = _make_loop_with_hold(
        queue_depth=3, capacity=2, avg_hold=10.0, rng=lambda: 1.0
    )
    # Pure estimate: ceil(4×10/2) = 20
    # Min jitter: ceil(20*0.85) = ceil(17.0) = 17
    # Max jitter: ceil(20*1.15) = ceil(23.0) = 23
    assert loop_min.saturation_retry_after() == 17
    assert loop_max.saturation_retry_after() == 23


def test_saturation_retry_after_capped_at_60():
    """Extreme pressure is capped at 60."""
    loop, _ = _make_loop_with_hold(
        queue_depth=100, capacity=1, avg_hold=30.0, rng=lambda: 1.0
    )
    assert loop.saturation_retry_after() == 60


def test_saturation_retry_after_floored_at_5():
    """Low estimate is floored at 5 even with minimum jitter."""
    loop, _ = _make_loop_with_hold(
        queue_depth=0, capacity=4, avg_hold=2.0, rng=lambda: 0.0
    )
    # Pure: ceil(1×2/4)=1, jittered: ceil(1*0.85)=1, floored at 5
    assert loop.saturation_retry_after() == 5


def test_saturation_hint_is_unjittered():
    """saturation_hint returns the pure estimator output (no jitter)."""
    loop, _ = _make_loop_with_hold(
        queue_depth=3, capacity=2, avg_hold=10.0, rng=lambda: 0.0
    )
    # Pure: ceil(4×10/2) = 20
    assert loop.saturation_hint == 20


def test_retry_after_seconds_saturated_never_below_poll_interval():
    """Flavour (b): structurally saturated never advertises below the poll interval."""
    loop, _ = _make_loop_with_hold(
        queue_depth=0, capacity=4, avg_hold=0.1, permits=0,
        poll_interval=10.0, rng=lambda: 0.0
    )
    # Pure estimate: ceil(1×0.1/4) = 1, floored at 5
    # max(5, ceil(10)) = 10 — poll_floor = max(5, 10) = 10
    # jittered: ceil(10 * 0.85) = ceil(8.5) = 9, but clamped to poll_floor=10 → 10
    assert loop.retry_after_seconds() >= 10  # poll interval is the floor before jitter


def test_retry_after_seconds_open_returns_floor():
    """When the gate is open (permits > 0), retry_after is the floor."""
    loop, _ = _make_loop_with_hold(
        queue_depth=0, capacity=4, avg_hold=0.0, permits=3
    )
    assert loop.retry_after_seconds() == 5


def test_saturation_retry_after_header_and_body_match():
    """The header and body carry the same post-jitter value."""
    # This is verified at the proxy level — here we verify the method returns
    # a single int that would be used for both.
    loop, _ = _make_loop_with_hold(
        queue_depth=3, capacity=2, avg_hold=10.0, rng=lambda: 0.5
    )
    result = loop.saturation_retry_after()
    assert isinstance(result, int)
    assert 5 <= result <= 60


# --- WI-022: idle poll backoff -----------------------------------------------


async def test_idle_detection_after_quiescent_tick():
    """The loop sets _idle=True when no traffic, no 429s, normal band, no phantoms."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.is_idle is True
    assert loop._idle is True


async def test_not_idle_when_traffic_in_flight():
    """Not idle when local_in_flight > 0."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.is_idle is True
    # Simulate a held permit
    await gate.acquire(timeout=0.01)
    await loop.tick()
    assert loop.is_idle is False
    await gate.release()


async def test_not_idle_when_recent_429():
    """Not idle when there are recent 429s."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.is_idle is True
    loop.record_429()
    await loop.tick()
    assert loop.is_idle is False


async def test_not_idle_when_phantom_estimate_nonzero():
    """Not idle when phantom estimate > 0."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=5))
    await loop.tick()
    # 5 observed, 0 local → phantom=5 → not idle
    assert loop.is_idle is False


async def test_not_idle_when_band_not_normal():
    """Not idle when band is not NORMAL (e.g. LOW)."""
    loop, client, gate, m, w = _make_loop(
        _reading(concurrent_sessions=6, priority_low=True)
    )
    await loop.tick()
    assert loop.is_idle is False


async def test_not_idle_when_breaker_open():
    """Not idle when breaker is OPEN."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    # Trip the breaker by recording enough 429s
    for _ in range(BCFG.threshold):
        loop.record_429()
    await loop.tick()
    assert loop.is_idle is False


def test_effective_poll_interval_fast_when_active():
    """When not idle, the effective interval is the fast poll_interval."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    loop._idle = False
    assert loop._effective_poll_interval() == 5.0


def test_effective_poll_interval_slow_when_idle():
    """When idle, the effective interval is poll_interval_idle."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    loop._poll_interval_idle_cfg = 30.0
    loop._idle = True
    # Capped at usage_fresh_ttl * 0.8 = 15 * 0.8 = 12
    assert loop._effective_poll_interval() == 12.0


def test_effective_poll_interval_disabled_when_none():
    """When poll_interval_idle is None, always uses fast interval."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    loop._poll_interval_idle_cfg = None
    loop._idle = True
    assert loop._effective_poll_interval() == 5.0


def test_effective_poll_interval_capped_at_fresh_ttl():
    """The idle interval is capped at usage_fresh_ttl * 0.8."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    loop._poll_interval_idle_cfg = 100.0  # very slow
    loop._idle = True
    # usage_fresh_ttl = 15 → cap = 12
    assert loop._effective_poll_interval() == 12.0


async def test_record_request_forwarded_wakes_poll():
    """record_request_forwarded sets the _poll_now event (WI-022)."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    loop._poll_now = asyncio.Event()
    assert not loop._poll_now.is_set()
    loop.record_request_forwarded()
    assert loop._poll_now.is_set()


async def test_record_429_wakes_poll():
    """record_429 sets the _poll_now event (WI-022)."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    loop._poll_now = asyncio.Event()
    assert not loop._poll_now.is_set()
    loop.record_429()
    assert loop._poll_now.is_set()


async def test_record_rate_limit_429_wakes_poll():
    """record_rate_limit_429 also sets the _poll_now event (H-1 fix)."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    loop._poll_now = asyncio.Event()
    assert not loop._poll_now.is_set()
    loop.record_rate_limit_429()
    assert loop._poll_now.is_set()


async def test_not_idle_when_reading_stale():
    """Not idle when the usage reading is stale — stay on fast cadence to
    detect recovery (M-1 fix)."""
    loop, client, gate, m, w = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.is_idle is True
    # Simulate a fetch failure
    client.set_fail(True)
    await loop.tick()
    assert loop.is_idle is False


async def test_wake_poll_noop_when_not_started():
    """_wake_poll is a no-op when _poll_now is None (before start())."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    loop._poll_now = None
    # Should not raise
    loop.record_request_forwarded()
    loop.record_429()


async def test_throughput_zero_on_first_tick():
    """Throughput is 0 on the first tick (no previous baseline)."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.last_throughput == 0


async def test_throughput_counts_forwarded_requests():
    """Throughput reflects requests forwarded since the last tick."""
    loop, _, _, _, _ = _make_loop(_reading(concurrent_sessions=0))
    await loop.tick()
    assert loop.last_throughput == 0
    # Forward 3 requests
    loop.record_request_forwarded()
    loop.record_request_forwarded()
    loop.record_request_forwarded()
    await loop.tick()
    assert loop.last_throughput == 3
    # Next tick with no new requests → throughput 0
    await loop.tick()
    assert loop.last_throughput == 0


# --- WI-022: integration test for run() loop with idle backoff (M-2) --------


async def test_run_loop_idle_backoff_wakes_on_activity():
    """Integration test: the run() loop uses the slow idle interval when idle,
    and wakes promptly when record_request_forwarded() is called.

    Uses real asyncio timing with very short intervals.  The idle interval
    (1.0s) is much longer than the active interval (0.01s), so we can
    distinguish them by counting ticks within a short wall-clock window.
    """
    import asyncio as _aio

    m = [1000.0]
    w = [1_000_000.0]
    client = FakeUsageClient(_reading(concurrent_sessions=0))
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=0.01,
        poll_interval_idle=1.0,  # long idle interval
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
    )

    task = _aio.create_task(loop.run())
    try:
        # Let the loop tick once and go idle.
        await _aio.sleep(0.05)
        assert loop._tick_count >= 1, "loop should have ticked at least once"
        assert loop.is_idle, "loop should be idle after first tick"

        # Wait a bit — the loop should be sleeping at the idle interval (1.0s),
        # so no additional ticks should happen.
        ticks_after_idle = loop._tick_count
        await _aio.sleep(0.05)
        assert loop._tick_count == ticks_after_idle, (
            "loop should not tick during idle sleep"
        )

        # Wake the loop by simulating a forwarded request.
        loop.record_request_forwarded()
        await _aio.sleep(0.05)
        assert loop._tick_count > ticks_after_idle, (
            "loop should have ticked after wake signal"
        )
        # The system goes idle again after the tick (no held permits),
        # but the wake signal caused an immediate tick — that's the
        # behaviour we're verifying.
    finally:
        task.cancel()
        try:
            await task
        except _aio.CancelledError:
            pass


async def test_run_loop_fast_interval_when_active():
    """When active (not idle), the loop ticks at the fast interval."""
    import asyncio as _aio

    m = [1000.0]
    w = [1_000_000.0]
    client = FakeUsageClient(_reading(concurrent_sessions=0))
    gate = PermitGate(initial_capacity=CFG.target, release_cooldown=0.0, clock=lambda: m[0])
    loop = ReconciliationLoop(
        truth_source=client,
        gate=gate,
        controller_config=CFG,
        breaker_config=BCFG,
        poll_interval=0.01,
        poll_interval_idle=1.0,
        monotonic_clock=lambda: m[0],
        wall_clock=lambda: w[0],
    )

    task = _aio.create_task(loop.run())
    try:
        # Keep the loop active by forwarding requests continuously.
        for _ in range(10):
            loop.record_request_forwarded()
            await _aio.sleep(0.02)

        # The loop should have ticked many times (fast interval = 0.01s).
        assert loop._tick_count >= 5, (
            f"loop should have ticked at least 5 times at fast interval, got {loop._tick_count}"
        )
    finally:
        task.cancel()
        try:
            await task
        except _aio.CancelledError:
            pass
