"""Tests for the permit gate's queue-wait instrumentation.

Wait timing is sampled only for requests that actually *blocked* before being
granted, so the average reflects queue pressure rather than being diluted toward
zero by instant grants.  A controllable clock makes the durations deterministic.
"""

from __future__ import annotations

import asyncio

from sluice.gate import PermitGate


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


async def _grant_after_wait(gate: PermitGate, clk: FakeClock, *, elapsed: float) -> None:
    """Hold the single permit, make a second acquire block, then release after
    advancing the clock by ``elapsed`` so the grant records that wait."""
    assert await gate.acquire(timeout=10.0)  # instant grant of a free permit

    async def waiter() -> bool:
        return await gate.acquire(timeout=10.0)

    start = clk.t
    task = asyncio.create_task(waiter())
    while gate.queue_depth == 0:  # wait until the second acquire is parked
        await asyncio.sleep(0)
    clk.t = start + elapsed
    await gate.release()  # frees the permit, wakes the waiter
    assert await task
    await gate.release()  # drop the just-granted permit for the next round


async def test_instant_grant_is_not_sampled():
    clk = FakeClock()
    gate = PermitGate(initial_capacity=1, clock=clk)

    assert await gate.acquire(timeout=1.0)  # permit was free → no wait
    assert gate.avg_wait_seconds == 0.0
    assert gate.p95_wait_seconds == 0.0


async def test_blocked_grant_records_wait():
    clk = FakeClock()
    gate = PermitGate(initial_capacity=1, clock=clk)

    await _grant_after_wait(gate, clk, elapsed=2.5)

    assert gate.avg_wait_seconds == 2.5
    assert gate.p95_wait_seconds == 2.5


async def test_avg_and_p95_over_multiple_waits():
    clk = FakeClock()
    gate = PermitGate(initial_capacity=1, clock=clk)

    for elapsed in (1.0, 2.0, 9.0):
        await _grant_after_wait(gate, clk, elapsed=elapsed)

    assert gate.avg_wait_seconds == 4.0  # (1 + 2 + 9) / 3
    assert gate.p95_wait_seconds == 9.0  # tail sample


async def test_queue_timeout_is_counted_not_sampled():
    gate = PermitGate(initial_capacity=0)  # no permits → every acquire blocks out

    assert await gate.acquire(timeout=0.01) is False
    assert gate.queue_timeouts == 1
    assert gate.avg_wait_seconds == 0.0  # a timeout is not a grant sample


# ---------------------------------------------------------------------------
# p95_wait_seconds: nearest-rank method and small-sample behavior
#
# The formula is ceil(0.95 * n) - 1 (0-indexed).  For small n this always
# selects the maximum sample (overestimates), which is conservative for a
# monitoring metric.  At n=20 the index finally drops below n-1, and at the
# window cap (n=64) the estimate is close to a true p95.
#
# These tests directly populate _wait_samples to verify the formula at each
# sample count.  The end-to-end acquire/release path is exercised by the
# tests above (test_blocked_grant_records_wait, test_avg_and_p95_over_multiple_waits).
# ---------------------------------------------------------------------------


async def test_p95_nearest_rank_single_sample():
    """n=1: ceil(0.95*1)-1 = 0 → p95 is the only sample."""
    gate = PermitGate(initial_capacity=1)
    gate._wait_samples.append(5.0)
    assert gate.p95_wait_seconds == 5.0


async def test_p95_nearest_rank_two_samples_returns_max():
    """n=2: ceil(0.95*2)-1 = 1 → p95 is the max (overestimates).

    With only 2 samples the nearest-rank method selects the last element
    (index 1), which is the maximum.  A true p95 would interpolate between
    the two values, but nearest-rank cannot — this is the documented
    small-sample overestimation.
    """
    gate = PermitGate(initial_capacity=1)
    gate._wait_samples.extend([1.0, 9.0])
    assert gate.p95_wait_seconds == 9.0  # max, not a true p95


async def test_p95_nearest_rank_small_n_always_returns_max():
    """For n < 20, the nearest-rank index equals n-1 (the max).

    ceil(0.95 * n) - 1 = n - 1  when  0.95*n > n-1, i.e. n < 20.
    So for any sample count below 20, p95 is the maximum observed wait.
    This overestimates but is conservative for a monitoring metric.
    """
    gate = PermitGate(initial_capacity=1)
    gate._wait_samples.extend(float(i) for i in range(1, 11))  # 1.0..10.0
    assert gate.p95_wait_seconds == 10.0  # max (idx = ceil(9.5)-1 = 9)


async def test_p95_nearest_rank_twenty_not_max():
    """n=20: ceil(0.95*20)-1 = 18 → p95 is the 19th value, not the max.

    This is the first sample count where nearest-rank does NOT return the
    maximum.  The 95th percentile of 20 sorted values is the 19th (index 18),
    which is the second-highest — the maximum (index 19) is excluded.
    """
    gate = PermitGate(initial_capacity=1, wait_window=64)
    gate._wait_samples.extend(float(i) for i in range(1, 21))  # 1.0..20.0
    assert gate.p95_wait_seconds == 19.0  # ordered[18], not 20.0


async def test_p95_nearest_rank_at_window_cap():
    """n=64 (window cap): ceil(0.95*64)-1 = 60 → p95 is the 61st value.

    At the default window size of 64, the nearest-rank estimate selects
    index 60, which is close to a true p95 (which would fall between the
    60th and 61st values).  This is why the window is capped at 64: the
    estimate is advisory but close enough for monitoring.
    """
    gate = PermitGate(initial_capacity=1, wait_window=64)
    gate._wait_samples.extend(float(i) for i in range(1, 65))  # 1.0..64.0
    assert gate.p95_wait_seconds == 61.0  # ordered[60]


async def test_p95_sorts_samples_before_indexing():
    """p95 sorts before indexing — reverse-order data must give the correct rank.

    Without the sorted() call in gate.py, unsorted data at n=20 would
    return ordered[18] = the 19th insertion-order element, not the 19th-
    smallest.  This test uses reverse-sorted data to catch that regression.
    """
    gate = PermitGate(initial_capacity=1, wait_window=64)
    gate._wait_samples.extend(float(i) for i in range(20, 0, -1))  # 20.0..1.0
    assert gate.p95_wait_seconds == 19.0  # sorted[18], not unsorted[18]=2.0


async def test_p95_window_eviction_oldest_dropped():
    """When the deque is full, new samples evict the oldest.

    The deque's maxlen=wait_window evicts oldest entries.  After eviction,
    p95 reflects only the remaining (most recent) samples, not the full
    history.  This verifies the eviction → percentile interaction.
    """
    gate = PermitGate(initial_capacity=1, wait_window=4)
    # Fill with 1.0, 2.0, 3.0, 4.0
    gate._wait_samples.extend([1.0, 2.0, 3.0, 4.0])
    assert gate.p95_wait_seconds == 4.0  # max (n=4 → idx=3)
    # Add a 5th → deque evicts 1.0, leaving [2.0, 3.0, 4.0, 5.0]
    gate._wait_samples.append(5.0)
    assert len(gate._wait_samples) == 4
    assert gate.p95_wait_seconds == 5.0  # still max of remaining


async def test_p95_never_affects_gate_capacity():
    """p95_wait_seconds is advisory only — it does not affect gate sizing.

    The gate's capacity is set by the reconciliation loop via resize(),
    never by p95 or avg_wait.  Even with a high p95, the gate capacity
    is unchanged — p95 is a monitoring signal, not a control input.
    """
    gate = PermitGate(initial_capacity=3)
    gate._wait_samples.append(10.0)
    assert gate.p95_wait_seconds == 10.0
    assert gate.capacity == 3  # unchanged — p95 is not a control input


# ---------------------------------------------------------------------------
# Plan 005 WI-002: Reserved floor
# ---------------------------------------------------------------------------


async def test_reserve_no_reserve_behaves_as_fifo():
    """With reserve=0, reserved flag has no effect — pure FIFO."""
    gate = PermitGate(initial_capacity=2, reserve=0)

    assert await gate.acquire(timeout=0.1, reserved=False) is True
    assert await gate.acquire(timeout=0.1, reserved=True) is True
    assert await gate.acquire(timeout=0.01, reserved=False) is False
    assert await gate.acquire(timeout=0.01, reserved=True) is False
    assert gate.held == 2
    assert gate.held_reserved == 1


async def test_reserve_non_reserved_cannot_use_reserved_slot():
    """When the shared pool is full, a non-reserved request cannot acquire
    even though a reserved slot is free."""
    gate = PermitGate(initial_capacity=2, reserve=1)

    # Fill the shared pool (capacity - reserve = 1 shared slot)
    assert await gate.acquire(timeout=0.1, reserved=False) is True
    assert gate.held == 1
    assert gate.held_reserved == 0

    # Non-reserved: shared pool full, reserved slot free → must wait
    assert await gate.acquire(timeout=0.01, reserved=False) is False

    # Reserved: can use the reserved slot
    assert await gate.acquire(timeout=0.1, reserved=True) is True
    assert gate.held == 2
    assert gate.held_reserved == 1


async def test_reserve_reserved_can_use_shared_pool():
    """A reserved request can use shared slots when they are available."""
    gate = PermitGate(initial_capacity=3, reserve=1)

    # Reserved requests can use all 3 permits (2 shared + 1 reserved)
    assert await gate.acquire(timeout=0.1, reserved=True) is True
    assert await gate.acquire(timeout=0.1, reserved=True) is True
    assert await gate.acquire(timeout=0.1, reserved=True) is True
    # All 3 permits used by reserved-class requests
    assert gate.held == 3
    assert gate.held_reserved == 3
    assert await gate.acquire(timeout=0.01, reserved=True) is False


async def test_reserve_release_frees_correct_counter():
    """Releasing a reserved permit decrements the reserved counter."""
    gate = PermitGate(initial_capacity=2, reserve=1)

    await gate.acquire(timeout=0.1, reserved=False)
    await gate.acquire(timeout=0.1, reserved=True)
    assert gate.held_reserved == 1

    # Release the reserved permit — frees the reserved slot
    await gate.release(reserved=True)
    assert gate.held == 1
    assert gate.held_reserved == 0

    # Non-reserved still can't acquire (shared pool full: 1 held, cap=1)
    assert await gate.acquire(timeout=0.01, reserved=False) is False
    # Reserved can use the freed reserved slot
    assert await gate.acquire(timeout=0.1, reserved=True) is True

    # Release everything
    await gate.release(reserved=True)
    await gate.release(reserved=False)
    assert gate.held == 0

    # Now non-reserved can acquire (shared pool empty)
    assert await gate.acquire(timeout=0.1, reserved=False) is True
    await gate.release(reserved=False)


async def test_reserve_invisible_below_saturation():
    """Below saturation, the reserve doesn't restrict non-reserved requests."""
    gate = PermitGate(initial_capacity=4, reserve=1)

    # 3 non-reserved requests (capacity - reserve = 3 shared) — all succeed
    for _ in range(3):
        assert await gate.acquire(timeout=0.1, reserved=False) is True

    # 4th non-reserved: shared pool full, but reserved slot is free → blocked
    assert await gate.acquire(timeout=0.01, reserved=False) is False

    # Reserved request: can still use the reserved slot
    assert await gate.acquire(timeout=0.1, reserved=True) is True


async def test_reserve_resize_does_not_break_invariant():
    """After resize, the shared/reserved cap is recomputed from the new capacity."""
    gate = PermitGate(initial_capacity=4, reserve=1)

    # Fill all 4 permits with non-reserved (3 shared + ... wait, only 3 shared)
    for _ in range(3):
        await gate.acquire(timeout=0.1, reserved=False)
    assert gate.held == 3

    # Shrink to 2: held (3) > capacity (2), no new grants
    await gate.resize(2)
    assert await gate.acquire(timeout=0.01, reserved=False) is False
    assert await gate.acquire(timeout=0.01, reserved=True) is False

    # Release 2 → held=1, shared cap = 2-1=1, 1 non-reserved held → shared full
    await gate.release(reserved=False)
    await gate.release(reserved=False)
    assert gate.held == 1

    # Non-reserved: shared pool full (1 held, cap=1)
    assert await gate.acquire(timeout=0.01, reserved=False) is False
    # Reserved: can use the reserved slot
    assert await gate.acquire(timeout=0.1, reserved=True) is True


async def test_release_double_release_reserved_no_negative():
    """Double-release with reserved=True must not drive counters negative."""
    gate = PermitGate(initial_capacity=2, reserve=1)

    await gate.acquire(timeout=0.1, reserved=False)
    await gate.acquire(timeout=0.1, reserved=True)
    await gate.release(reserved=False)
    await gate.release(reserved=True)
    assert gate.held == 0
    assert gate.held_reserved == 0

    await gate.release(reserved=True)
    assert gate.held >= 0
    assert gate.held_reserved >= 0


async def test_acquire_timeout_zero_fast_path():
    """timeout <= 0 returns False immediately when no permit is available."""
    gate = PermitGate(initial_capacity=1)

    assert await gate.acquire(timeout=0.1) is True
    assert await gate.acquire(timeout=0) is False
    assert gate.held == 1


# ---------------------------------------------------------------------------
# Plan 013 WI-001: Hold-time sampling
# ---------------------------------------------------------------------------


async def test_hold_seconds_sampled_on_release():
    """When hold_seconds is passed to release(), it is sampled."""
    gate = PermitGate(initial_capacity=1)

    await gate.acquire(timeout=0.1)
    assert gate.avg_hold_seconds == 0.0  # cold — no samples yet

    await gate.release(hold_seconds=3.5)
    assert gate.avg_hold_seconds == 3.5


async def test_hold_seconds_none_not_sampled():
    """When hold_seconds is None (default), nothing is sampled."""
    gate = PermitGate(initial_capacity=1)

    await gate.acquire(timeout=0.1)
    await gate.release()  # hold_seconds defaults to None
    assert gate.avg_hold_seconds == 0.0  # still cold


async def test_hold_seconds_averaged():
    """Multiple hold samples are averaged."""
    gate = PermitGate(initial_capacity=1)

    for hold in (2.0, 4.0, 6.0):
        await gate.acquire(timeout=0.1)
        await gate.release(hold_seconds=hold)

    assert gate.avg_hold_seconds == 4.0  # (2+4+6)/3


async def test_hold_seconds_window_eviction():
    """Hold samples are capped at wait_window — oldest evicted."""
    gate = PermitGate(initial_capacity=1, wait_window=3)

    for hold in (1.0, 2.0, 3.0, 4.0):
        await gate.acquire(timeout=0.1)
        await gate.release(hold_seconds=hold)

    # Window=3 → [2.0, 3.0, 4.0] (1.0 evicted)
    assert gate.avg_hold_seconds == 3.0  # (2+3+4)/3


async def test_hold_seconds_mixed_with_unsampled():
    """Unsampled releases (hold_seconds=None) don't perturb the average."""
    gate = PermitGate(initial_capacity=1)

    await gate.acquire(timeout=0.1)
    await gate.release(hold_seconds=5.0)

    # An unsampled release — should not add a 0.0 sample
    await gate.acquire(timeout=0.1)
    await gate.release()  # hold_seconds=None

    assert gate.avg_hold_seconds == 5.0  # unchanged


async def test_hold_seconds_zero_when_empty():
    """avg_hold_seconds is 0.0 when no samples exist."""
    gate = PermitGate(initial_capacity=1)
    assert gate.avg_hold_seconds == 0.0
