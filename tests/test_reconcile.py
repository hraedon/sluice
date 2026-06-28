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
        usage_client=client,
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
