"""Tests for the reconciliation loop: phantom absorb/clear, box, staleness, breaker.

Uses a fake clock and a fake usage source — no network.
"""

from __future__ import annotations

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
