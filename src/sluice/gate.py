"""Resizeable permit gate with release cooldown.

The gate is the fast inner loop (§4 of the concurrency model): a synchronous
semaphore sized to the latest ``effective_permits``.  Admission is always
through this gate; the reconciliation loop only tunes its width.

* ``acquire(timeout)`` blocks until a permit is available or the timeout elapses.
* ``release()`` returns a permit; it enters a cooldown before becoming reusable,
  blunting the lag race that turns a clean release into an apparent overshoot.
* ``resize(n)`` changes the capacity.  Shrinking below current holders does NOT
  revoke in-flight permits — it just prevents new grants until enough drain.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable

log = logging.getLogger("sluice.gate")


class PermitGate:
    """A resizeable semaphore with a release cooldown."""

    def __init__(
        self,
        initial_capacity: int,
        *,
        release_cooldown: float = 0.0,
        clock: Callable[[], float] = time.monotonic,
        wait_window: int = 64,
    ) -> None:
        self._capacity = initial_capacity
        self._release_cooldown = release_cooldown
        self._clock = clock
        self._held = 0
        self._cooldowns: deque[float] = deque()
        self._waiters = 0
        self._cond = asyncio.Condition()
        # Recent grant-wait durations, sampled only for requests that actually
        # blocked (queued) before being granted — "when queued, how long?".
        # Instant grants (a permit was free) are not sampled, so the average
        # reflects queue pressure rather than being diluted toward zero.
        self._wait_samples: deque[float] = deque(maxlen=wait_window)
        self._timeouts = 0

    def _prune_cooldowns(self) -> None:
        now = self._clock()
        while self._cooldowns and self._cooldowns[0] <= now:
            self._cooldowns.popleft()

    def _available(self) -> int:
        self._prune_cooldowns()
        return max(0, self._capacity - self._held - len(self._cooldowns))

    async def acquire(self, *, timeout: float) -> bool:
        """Try to acquire a permit.  Returns *True* on success, *False* on timeout."""
        if timeout <= 0:
            async with self._cond:
                if self._available() > 0:
                    self._held += 1
                    return True
                return False

        async with self._cond:
            self._waiters += 1
            start = self._clock()
            blocked = False
            try:
                deadline = start + timeout
                while self._available() <= 0:
                    blocked = True
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        self._timeouts += 1
                        return False
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                    except asyncio.TimeoutError:
                        self._timeouts += 1
                        return False
                self._held += 1
                if blocked:
                    self._wait_samples.append(self._clock() - start)
                return True
            finally:
                self._waiters -= 1

    async def release(self) -> None:
        """Release a permit.  It enters cooldown (if configured)."""
        async with self._cond:
            if self._held <= 0:
                log.warning("release called with no held permits (double-release?)")
                return
            self._held -= 1
            if self._release_cooldown > 0:
                self._cooldowns.append(self._clock() + self._release_cooldown)
            self._cond.notify_all()

    async def resize(self, new_capacity: int) -> None:
        """Change the capacity.  Never revokes in-flight permits."""
        async with self._cond:
            self._capacity = new_capacity
            self._cond.notify_all()

    @property
    def held(self) -> int:
        return self._held

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def queue_depth(self) -> int:
        return self._waiters

    @property
    def cooling_down(self) -> int:
        self._prune_cooldowns()
        return len(self._cooldowns)

    @property
    def available(self) -> int:
        return self._available()

    @property
    def avg_wait_seconds(self) -> float:
        """Mean queue wait over recent grants that actually blocked (0.0 if none)."""
        if not self._wait_samples:
            return 0.0
        return sum(self._wait_samples) / len(self._wait_samples)

    @property
    def p95_wait_seconds(self) -> float:
        """95th-percentile queue wait over recent blocked grants (0.0 if none)."""
        if not self._wait_samples:
            return 0.0
        ordered = sorted(self._wait_samples)
        # ceil(0.95 * n) - 1, clamped — for small n this lands on the tail sample.
        idx = max(0, -(-95 * len(ordered) // 100) - 1)
        return ordered[idx]

    @property
    def queue_timeouts(self) -> int:
        """Total requests that gave up waiting for a permit (queue-timeout 503s)."""
        return self._timeouts
