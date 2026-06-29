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
    phantom_estimate,
)
from sluice.gate import PermitGate
from sluice.singleton import SingletonGuard
from sluice.usage import CachedReading, UsageClient

log = logging.getLogger("sluice.reconcile")

_RETRY_AFTER_STALE_CAP = 300


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
        guard: SingletonGuard | None = None,
    ) -> None:
        self._usage = usage_client
        self._gate = gate
        self._ctrl_cfg = controller_config
        self._brk_cfg = breaker_config
        self._poll_interval = poll_interval
        self._mono = monotonic_clock
        self._wall = wall_clock
        self._guard = guard

        self._breaker = BreakerSnapshot()
        self._recent_429s: deque[float] = deque()
        self._total_429s = 0

        self._phantom_samples: deque[tuple[int, int]] = deque(
            maxlen=controller_config.phantom_window
        )
        self._last_phantom_estimate = 0

        self._last_permits = gate.capacity
        self._last_band: Band = Band.NORMAL
        self._last_reading_cached: CachedReading | None = None
        self._last_age: float = 0.0

        self._task: asyncio.Task[None] | None = None
        self._first_poll_ok = False

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
        # Non-leader: don't poll, hold the gate closed (fail-safe).
        if self._guard is not None and not self._guard.is_held():
            await self._gate.resize(0)
            return

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

        # Record the (observed, local) pairing for windowed phantom estimation.
        # Only record real readings — synthetic fail-safe samples would poison
        # the window with fabricated concurrent_sessions values.
        if cached.ok:
            self._phantom_samples.append((reading.concurrent_sessions, self._gate.held))
        phantom_est = phantom_estimate(self._phantom_samples)

        state = ControllerState(
            reading=reading,
            local_in_flight=self._gate.held,
            breaker=breaker.state,
            phantom_estimate=phantom_est,
        )
        permits = effective_permits(state, self._ctrl_cfg, now=now_wall)

        await self._gate.resize(permits)

        # Cache for metrics / status.
        self._last_permits = permits
        self._last_phantom_estimate = phantom_est
        self._last_band = classify_band(reading, now=now_wall)
        self._last_reading_cached = cached
        self._last_age = age

        if cached.ok:
            self._first_poll_ok = True

    async def run(self) -> None:
        """Run the reconciliation loop forever (until cancelled)."""
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconciliation tick failed — closing gate (fail-safe)")
                try:
                    await self._gate.resize(0)
                except Exception:
                    log.critical("failed to close gate after tick exception")
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
    def cooling_down(self) -> int:
        return self._gate.cooling_down

    @property
    def avg_wait_seconds(self) -> float:
        return self._gate.avg_wait_seconds

    @property
    def p95_wait_seconds(self) -> float:
        return self._gate.p95_wait_seconds

    @property
    def queue_timeouts(self) -> int:
        return self._gate.queue_timeouts

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

    @property
    def ready(self) -> bool:
        """True once the first successful usage poll has completed."""
        return self._first_poll_ok

    @property
    def phantom_estimate_value(self) -> int:
        """Current windowed phantom estimate (sustained excess)."""
        return self._last_phantom_estimate

    def gate_closed_reason(self) -> str:
        """Why the gate is shut: 'open', 'boxed', 'breaker', or 'saturated'.

        The proxy consults this before ``acquire`` to fast-fail immediately
        when the gate cannot open (boxed / breaker), rather than burning the
        full queue timeout.
        """
        if self._last_reading_cached is not None:
            r = self._last_reading_cached.reading
            if r.boxed_until_epoch is not None:
                now_wall = self._wall()
                if now_wall < r.boxed_until_epoch:
                    return "boxed"
        if self._breaker.state is BreakerState.OPEN:
            return "breaker"
        if self._last_permits == 0:
            return "saturated"
        return "open"

    def retry_after_seconds(self) -> int:
        """Honest Retry-After based on the gate-closed reason.

        - **boxed:** ``ceil(resets_at - now)``, floored at 30 s.
          When the reading is stale (``ok=False``), capped at 300 s — a stale
          ``resets_at`` could be hours in the future and mislead clients.
        - **breaker:** remaining cooldown.
        - **saturated / open:** short default (5 s).
        """
        reason = self.gate_closed_reason()
        if reason == "boxed":
            if self._last_reading_cached is not None:
                r = self._last_reading_cached.reading
                if r.resets_at_epoch is not None:
                    remaining = int(r.resets_at_epoch - self._wall())
                    result = max(30, remaining)
                    if not self._last_reading_cached.ok:
                        return min(result, _RETRY_AFTER_STALE_CAP)
                    return result
            return 30  # floor when resets_at is unknown
        if reason == "breaker":
            if self._breaker.opened_at is not None:
                elapsed = self._mono() - self._breaker.opened_at
                cooldown_remaining = self._brk_cfg.cooldown_seconds - elapsed
                return max(1, int(cooldown_remaining))
            return 5
        return 5  # saturated or open
