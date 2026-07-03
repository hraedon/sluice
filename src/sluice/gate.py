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
    """A resizeable semaphore with a release cooldown.

    When ``reserve`` > 0, that many permits are reserved for "reserved-class"
    requests (identified by ``reserved=True`` on acquire/release).  Non-reserved
    requests can use only the shared pool (``capacity - reserve``); reserved
    requests may use the shared pool *or* the reserved slots.  Below saturation
    the reserve is invisible — it only bites when the shared pool is exhausted.

    Release cooldown interacts with the reserve as follows: a cooling-down slot
    reduces the *total* available count (``capacity - held - cooling_down``)
    regardless of which pool it belonged to.  A just-released reserved slot is
    not acquirable until its cooldown expires — the reserve guarantees priority
    of admission, not instant availability.
    """

    def __init__(
        self,
        initial_capacity: int,
        *,
        release_cooldown: float = 0.0,
        reserve: int = 0,
        clock: Callable[[], float] = time.monotonic,
        wait_window: int = 64,
    ) -> None:
        self._capacity = initial_capacity
        self._release_cooldown = release_cooldown
        self._reserve = reserve
        self._clock = clock
        self._held = 0
        self._held_reserved = 0
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

    def _available_for(self, reserved: bool) -> int:
        """Permits available for a given class."""
        total = self._available()
        if self._reserve == 0 or reserved:
            return total
        held_non_reserved = self._held - self._held_reserved
        shared_cap = max(0, self._capacity - self._reserve)
        shared_avail = max(0, shared_cap - held_non_reserved)
        return min(total, shared_avail)

    async def acquire(self, *, timeout: float, reserved: bool = False) -> bool:
        """Try to acquire a permit.  Returns *True* on success, *False* on timeout.

        If ``reserved`` is *True* and the gate has a reserve configured, the
        request may use reserved slots.  Otherwise it is limited to the shared
        pool.
        """
        if timeout <= 0:
            async with self._cond:
                if self._available_for(reserved) > 0:
                    self._held += 1
                    if reserved:
                        self._held_reserved += 1
                    return True
                return False

        async with self._cond:
            self._waiters += 1
            start = self._clock()
            blocked = False
            try:
                deadline = start + timeout
                while self._available_for(reserved) <= 0:
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
                if reserved:
                    self._held_reserved += 1
                if blocked:
                    self._wait_samples.append(self._clock() - start)
                return True
            finally:
                self._waiters -= 1

    async def release(self, *, reserved: bool = False) -> None:
        """Release a permit.  It enters cooldown (if configured)."""
        async with self._cond:
            if self._held <= 0:
                log.warning("release called with no held permits (double-release?)")
                return
            self._held -= 1
            if reserved and self._held_reserved > 0:
                self._held_reserved -= 1
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
    def held_reserved(self) -> int:
        return self._held_reserved

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def reserve(self) -> int:
        return self._reserve

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
        """95th-percentile queue wait over recent blocked grants (0.0 if none).

        Uses the nearest-rank method: ``ceil(0.95 * n) - 1``.  For small
        samples this overestimates (n=2 returns the max, not a true p95);
        this is conservative for a monitoring metric and the samples are
        advisory (capped at ``wait_window`` = 64, where the estimate is
        close to the true p95).  Never a control input.
        """
        if not self._wait_samples:
            return 0.0
        ordered = sorted(self._wait_samples)
        idx = max(0, -(-95 * len(ordered) // 100) - 1)
        return ordered[idx]

    @property
    def queue_timeouts(self) -> int:
        """Total requests that gave up waiting for a permit (queue-timeout 503s)."""
        return self._timeouts
