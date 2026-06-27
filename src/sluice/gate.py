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
    ) -> None:
        self._capacity = initial_capacity
        self._release_cooldown = release_cooldown
        self._clock = clock
        self._held = 0
        self._cooldowns: deque[float] = deque()
        self._waiters = 0
        self._cond = asyncio.Condition()

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
            try:
                deadline = self._clock() + timeout
                while self._available() <= 0:
                    remaining = deadline - self._clock()
                    if remaining <= 0:
                        return False
                    try:
                        await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                    except asyncio.TimeoutError:
                        return False
                self._held += 1
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
