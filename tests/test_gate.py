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
