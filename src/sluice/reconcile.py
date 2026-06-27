"""Background reconciliation loop tying usage polling, the controller, and the gate.

Every ``poll_interval`` seconds:

1. Poll ``/v1/usage`` for the provider's ``concurrent_sessions`` (ground truth).
2. Compute ``effective_permits`` via the pure controller.
3. Resize the live :class:`~sluice.gate.PermitGate`.
4. Update breaker / box state.

The loop also receives event-driven callbacks from the proxy:
``record_429()`` (concurrency 429 received) and ``record_success()`` (request
completed normally).  These drive the breaker's event-based transitions
(HALF_OPEN→CLOSED on success, HALF_OPEN→OPEN on 429) immediately, without
waiting for the next tick.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from collections import deque
from collections.abc import Callable

from sluice.control import (
    Band,
    BreakerConfig,
    BreakerSnapshot,
    BreakerState,
    ControllerConfig,
    ControllerState,
    classify_band,
    breaker_on_429,
    breaker_on_success,
    breaker_on_tick,
    effective_permits,
)
from sluice.gate import PermitGate
from sluice.usage import CachedReading, UsageClient

log = logging.getLogger("sluice.reconcile")


class ReconciliationLoop:
    """Background task that reconciles the gate against upstream truth."""

    def __init__(
        self,
        *,
        usage_client: UsageClient,
        gate: PermitGate,
        controller_config: ControllerConfig,
        breaker_config: BreakerConfig,
        poll_interval: float = 5.0,
        monotonic_clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._usage = usage_client
        self._gate = gate
        self._ctrl_cfg = controller_config
        self._brk_cfg = breaker_config
        self._poll_interval = poll_interval
        self._mono = monotonic_clock
        self._wall = wall_clock

        self._breaker = BreakerSnapshot()
        self._recent_429s: deque[float] = deque()
        self._total_429s = 0

        self._last_permits = gate.capacity
        self._last_band: Band = Band.NORMAL
        self._last_reading_cached: CachedReading | None = None
        self._last_age: float = 0.0

        self._task: asyncio.Task[None] | None = None

    # -- event-driven callbacks (called by the proxy) ------------------------

    def record_429(self) -> None:
        """A concurrency 429 was received from the upstream."""
        now = self._mono()
        self._recent_429s.append(now)
        self._total_429s += 1
        self._prune_429s(now)
        self._breaker = breaker_on_429(
            self._breaker,
            self._recent_429s,
            now=now,
            config=self._brk_cfg,
        )

    def record_success(self) -> None:
        """An upstream request completed normally."""
        prev = self._breaker.state
        self._breaker = breaker_on_success(self._breaker)
        # A successful probe forgives the failures that tripped the breaker —
        # otherwise breaker_on_tick would immediately re-trip on the stale 429s.
        if prev is BreakerState.HALF_OPEN and self._breaker.state is BreakerState.CLOSED:
            self._recent_429s.clear()

    def _prune_429s(self, now: float) -> None:
        cutoff = now - self._brk_cfg.window_seconds
        while self._recent_429s and self._recent_429s[0] < cutoff:
            self._recent_429s.popleft()

    # -- the tick ------------------------------------------------------------

    async def tick(self) -> None:
        """One reconciliation cycle: fetch → compute → resize."""
        now_mono = self._mono()
        now_wall = self._wall()

        cached = await self._usage.fetch(now_monotonic=now_mono)
        age = now_mono - cached.fetched_at_monotonic
        reading = dataclasses.replace(cached.reading, age_seconds=age)

        # Time-based breaker transitions (OPEN→HALF_OPEN after cooldown, etc.).
        # Read self._breaker *after* the fetch so we don't lose concurrent
        # record_429 / record_success updates that may have landed during the await.
        breaker = self._breaker
        breaker = breaker_on_tick(
            breaker,
            self._recent_429s,
            now=now_mono,
            config=self._brk_cfg,
        )
        self._breaker = breaker

        state = ControllerState(
            reading=reading,
            local_in_flight=self._gate.held,
            breaker=breaker.state,
        )
        permits = effective_permits(state, self._ctrl_cfg, now=now_wall)

        await self._gate.resize(permits)

        # Cache for metrics / status.
        self._last_permits = permits
        self._last_band = classify_band(reading, now=now_wall)
        self._last_reading_cached = cached
        self._last_age = age

    async def run(self) -> None:
        """Run the reconciliation loop forever (until cancelled)."""
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconciliation tick failed")
            await asyncio.sleep(self._poll_interval)

    async def start(self) -> None:
        """Start the background loop as a task."""
        if self._task is None:
            self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        """Cancel the background loop."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # -- observability (read by /metrics, /status, etc.) ---------------------

    @property
    def effective_permits_count(self) -> int:
        return self._last_permits

    @property
    def band(self) -> Band:
        return self._last_band

    @property
    def breaker_state(self) -> BreakerState:
        return self._breaker.state

    @property
    def total_429s(self) -> int:
        return self._total_429s

    @property
    def recent_429_count(self) -> int:
        return len(self._recent_429s)

    @property
    def in_flight(self) -> int:
        return self._gate.held

    @property
    def queue_depth(self) -> int:
        return self._gate.queue_depth

    @property
    def last_age_seconds(self) -> float:
        return self._last_age

    @property
    def last_fetch_ok(self) -> bool:
        return self._last_reading_cached.ok if self._last_reading_cached else False

    @property
    def observed_concurrent_sessions(self) -> int | None:
        if self._last_reading_cached is None:
            return None
        return self._last_reading_cached.reading.concurrent_sessions
