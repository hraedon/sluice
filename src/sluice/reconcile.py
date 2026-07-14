"""Background reconciliation loop tying truth polling, the controller, and the gate.

Every ``poll_interval`` seconds:

1. Fetch the current :class:`LimitState` from the truth source (polled, header,
   or null depending on provider).
2. Compute permits via the selected controller strategy (concurrency-reconcile
   or AIMD adaptive).
3. Resize the live :class:`~sluice.gate.PermitGate`.
4. Update breaker / box state.

The loop also receives event-driven callbacks from the proxy:
``record_429()`` (concurrency 429 received), ``record_success()`` (request
completed normally), and ``record_response_headers()`` (in-band ratelimit
headers for header-driven providers).
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import math
import random
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from sluice.control import (
    Band,
    BreakerConfig,
    BreakerSnapshot,
    BreakerState,
    ControllerConfig,
    ControllerState,
    AdaptiveConfig,
    AdaptiveSnapshot,
    classify_band,
    adaptive_effective_permits,
    breaker_on_429,
    breaker_on_success,
    breaker_on_tick,
    effective_permits,
    is_hard_boxed,
    is_low_interactivity,
    phantom_estimate,
    saturation_retry_after,
    validate_target_override,
)
from sluice.gate import PermitGate
from sluice.history import History, HistoryEntry
from sluice.history_store import HistoryStore
from sluice.providers import TruthSource
from sluice.singleton import SingletonGuard
from sluice.usage import CachedReading

log = logging.getLogger("sluice.reconcile")

_RETRY_AFTER_STALE_CAP = 300
_RETRY_AFTER_SATURATION_FLOOR = 5
_RETRY_AFTER_SATURATION_CAP = 60
RETRY_AFTER_SHORT = 5  # unknowable-timing default (draining, not_leader)
_PRUNE_INTERVAL_TICKS = 60
_DEFAULT_HISTORY_TTL = 604800.0
_REQ_TS_MAXLEN = 10000  # safety cap for local request-timestamp deque

# Fields that may be overridden at runtime via the dashboard (Plan 011).
# Grows one deliberate field at a time; never by reflection over the config dataclass.
_OVERRIDE_WHITELIST = frozenset({"target"})


class ReconciliationLoop:
    """Background task that reconciles the gate against upstream truth."""

    def __init__(
        self,
        *,
        truth_source: TruthSource,
        gate: PermitGate,
        controller_config: ControllerConfig,
        breaker_config: BreakerConfig,
        poll_interval: float = 5.0,
        monotonic_clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        guard: SingletonGuard | None = None,
        controller: str = "concurrency_reconcile",
        adaptive_config: AdaptiveConfig | None = None,
        history: History | None = None,
        history_store: HistoryStore | None = None,
        history_ttl: float = _DEFAULT_HISTORY_TTL,
        rng: Callable[[], float] = random.random,
        poll_interval_idle: float | None = None,
    ) -> None:
        self._truth = truth_source
        self._gate = gate
        self._ctrl_cfg = controller_config
        self._boot_config = controller_config
        self._brk_cfg = breaker_config
        self._poll_interval = poll_interval
        self._mono = monotonic_clock
        self._wall = wall_clock
        self._guard = guard
        self._controller = controller
        self._adaptive_cfg = adaptive_config or AdaptiveConfig()
        self._history = history
        self._history_store = history_store
        self._history_ttl = history_ttl
        self._tick_count = 0
        self._rng = rng
        # Idle poll backoff (WI-022): when idle, sleep at poll_interval_idle
        # instead of poll_interval.  None disables the backoff (always fast).
        self._poll_interval_idle_cfg: float | None = poll_interval_idle
        self._idle = False
        self._poll_now: asyncio.Event | None = None  # created in start()

        self._breaker = BreakerSnapshot()
        self._recent_429s: deque[float] = deque()
        self._recent_rate_limit_429s: deque[float] = deque()
        self._total_429s = 0
        self._total_gateway_429s = 0
        self._total_rate_limit_429s = 0

        self._phantom_samples: deque[tuple[int, int]] = deque(
            maxlen=controller_config.phantom_window
        )
        self._last_phantom_estimate = 0

        # Local forwarded-request tracking for request-window reconciliation.
        # Stores monotonic timestamps of each forwarded request; pruned to
        # requests_window_seconds each tick so the count approximates the
        # provider's rolling window.  Bounded by _REQ_TS_MAXLEN as a safety
        # cap — at typical home-lab rates this is never hit.
        self._request_timestamps: deque[float] = deque(maxlen=_REQ_TS_MAXLEN)
        self._total_requests_forwarded = 0
        self._prev_total_requests_forwarded = 0  # for per-tick throughput (WI-023)
        self._last_local_requests_in_window: int | None = None
        self._last_request_window_delta: int | None = None
        self._last_throughput: int = 0  # requests forwarded since previous tick (WI-023)

        self._adaptive = AdaptiveSnapshot()

        self._last_permits = gate.capacity
        self._last_band: Band = Band.NORMAL
        self._last_reading_cached: CachedReading | None = None
        self._last_age: float = 0.0

        self._task: asyncio.Task[None] | None = None
        self._first_poll_ok = False
        self._stopped = False

        # Runtime overrides (Plan 011): ephemeral, leader-only, revert on restart.
        # {"target": {"value": 6, "since": <epoch>}} — empty when no override active.
        self._overrides: dict[str, dict[str, Any]] = {}

    # -- runtime overrides (Plan 011) -----------------------------------------

    def apply_override(self, field: str, value: int) -> str | None:
        """Apply a runtime config override for a whitelisted field.

        Rebuilds ``ControllerConfig`` via ``dataclasses.replace`` so the next
        tick resizes the gate — no new resize path.  Returns a warning string
        (accept-with-warning) or ``None`` (clean accept).  Raises
        :class:`ValueError` on rejection (caller returns 400 + reason).
        """
        if field not in _OVERRIDE_WHITELIST:
            raise ValueError(f"field '{field}' is not in the override whitelist")

        # Fail-safe: refuse overrides until we have a fresh provider reading.
        # Without a real reading, validation uses default hard_cap=8 which may
        # be higher than the actual account's limit (AGENTS.md rule 1).
        if not self._first_poll_ok:
            raise ValueError(
                "cannot apply override before first successful usage poll"
            )
        cached = self._last_reading_cached
        if cached is None or not cached.ok:
            raise ValueError(
                "usage reading stale; override unavailable until fresh reading"
            )

        warning = validate_target_override(value, cached.reading)

        self._ctrl_cfg = dataclasses.replace(self._ctrl_cfg, **{field: value})
        # The adaptive controller uses its own config; keep it in sync.
        if field == "target" and self._controller == "adaptive":
            self._adaptive_cfg = dataclasses.replace(self._adaptive_cfg, target=value)
        self._overrides[field] = {"value": value, "since": self._wall()}
        return warning

    def clear_override(self, field: str) -> None:
        """Revert a runtime override to its boot value."""
        if field not in _OVERRIDE_WHITELIST:
            raise ValueError(f"field '{field}' is not in the override whitelist")
        if field not in self._overrides:
            return

        boot_val = getattr(self._boot_config, field)
        self._ctrl_cfg = dataclasses.replace(self._ctrl_cfg, **{field: boot_val})
        if field == "target" and self._controller == "adaptive":
            self._adaptive_cfg = dataclasses.replace(self._adaptive_cfg, target=boot_val)
        del self._overrides[field]

    @property
    def overrides(self) -> dict[str, Any]:
        """Current runtime overrides for /status.json.

        ``{"target": {"boot": 4, "override": 6, "since": <epoch>}}``, empty when none.
        """
        result: dict[str, Any] = {}
        for field, info in self._overrides.items():
            result[field] = {
                "boot": getattr(self._boot_config, field),
                "override": info["value"],
                "since": info["since"],
            }
        return result

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
        self._wake_poll()

    def record_gateway_429(self) -> None:
        """A gateway/CDN 429 was received (not from the upstream's concurrency
        enforcement).  Tracked separately — does NOT feed the breaker (WI-024).
        """
        self._total_gateway_429s += 1

    def record_rate_limit_429(self) -> None:
        """A rate-limit 429 was received from the upstream (positive retry-after).

        Does NOT feed the breaker.  Capture 2026-07-07
        (samples/429-capture-2026-07-07.md, gitignored) proved that umans
        emits ``retry_after=2`` on transient concurrency rejections that
        never lead to a box — ``concurrent_sessions`` stayed 0 and
        ``boxed_until`` stayed null through 36 such 429s, while the breaker
        false-tripped 30 times in 24 h.  The earlier decision to feed these
        to the breaker was based on WI-024's initial (same-day-corrected)
        misdiagnosis that a 5-hour box had occurred; finding #5 of the
        capture doc proved it was a deprioritization window where the
        provider keeps serving 200s.

        The deprioritization rung is already handled correctly by
        ``is_deprioritized`` → LOW band → serve at the account limit
        (``effective_permits``).  Transient concurrency rejections are
        absorbed by the next ``/v1/usage`` poll adjusting the gate.  The
        breaker's job is to stop sluice from hammering into a *hard box*;
        rate-limit 429s don't cause one.

        Wakes the poll so fresh ``/v1/usage`` state is fetched ASAP — a 429
        means upstream state may have changed.  Tracked in a separate counter
        and a windowed deque (``_recent_rate_limit_429s``) for the stale-reading
        safety net in ``tick()`` (see below).  ``total_429s`` remains
        concurrency-only.
        """
        now = self._mono()
        self._total_rate_limit_429s += 1
        self._recent_rate_limit_429s.append(now)
        self._wake_poll()

    def record_success(self) -> None:
        """An upstream request completed normally."""
        prev = self._breaker.state
        self._breaker = breaker_on_success(self._breaker)
        # A successful probe forgives the failures that tripped the breaker —
        # otherwise breaker_on_tick would immediately re-trip on the stale 429s.
        if prev is BreakerState.HALF_OPEN and self._breaker.state is BreakerState.CLOSED:
            self._recent_429s.clear()

    def record_response_headers(
        self, headers: dict[str, str], status: int
    ) -> None:
        """Feed in-band response headers to the truth source (WI-004).

        For polled truth (umans) this is a no-op.  For header truth
        (Anthropic/OpenAI) it parses the allowlisted ratelimit headers into
        the :class:`HeaderTruthSource`.

        Safe to call after :meth:`stop` — returns early when the truth
        source has been closed (WI-030: in-flight requests during drain
        may call this after stop closes the truth source).
        """
        if self._stopped:
            return
        now = self._mono()
        self._truth.record_response_headers(
            headers, status, now_monotonic=now
        )

    def record_request_forwarded(self) -> None:
        """A request was forwarded upstream (permit acquired, not fast-failed).

        Called by the proxy after a successful gate acquire.  The timestamp
        is used for request-window reconciliation: comparing how many requests
        sluice forwarded within the provider's rolling window against the
        provider's reported ``requests_in_window``.
        """
        now = self._mono()
        self._request_timestamps.append(now)
        self._total_requests_forwarded += 1
        self._wake_poll()

    def _prune_429s(self, now: float) -> None:
        cutoff = now - self._brk_cfg.window_seconds
        while self._recent_429s and self._recent_429s[0] < cutoff:
            self._recent_429s.popleft()
        while self._recent_rate_limit_429s and self._recent_rate_limit_429s[0] < cutoff:
            self._recent_rate_limit_429s.popleft()

    def _wake_poll(self) -> None:
        """Signal the reconcile loop to wake from an idle sleep (WI-022).

        No-op when the loop hasn't started yet (``_poll_now`` is None) or
        when the loop is already using the fast interval (the event is
        cleared after each tick, so setting it is harmless if the loop
        is already awake).
        """
        if self._poll_now is not None:
            self._poll_now.set()

    # -- the tick ------------------------------------------------------------

    async def tick(self) -> None:
        """One reconciliation cycle: fetch → compute → resize."""
        # Non-leader: don't poll, hold the gate closed (fail-safe).
        if self._guard is not None and not self._guard.is_held():
            await self._gate.resize(0)
            return

        now_mono = self._mono()
        now_wall = self._wall()

        self._prune_429s(now_mono)

        # Capture local_in_flight *before* the fetch so the (observed, local)
        # pair in the phantom sample is aligned in time.  The provider's
        # concurrent_sessions reflects the moment the request was served, not
        # the moment we read self._gate.held after the fetch returns (WI-017).
        # Permits may be acquired or released during the fetch's network I/O,
        # corrupting the windowed estimate if we pair them mismatched.
        held_at_fetch = self._gate.held

        cached = await self._truth.fetch(now_monotonic=now_mono)
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

        if self._controller == "adaptive":
            # AIMD controller — no phantom absorption (no concurrency ground truth).
            state = ControllerState(
                reading=reading,
                local_in_flight=self._gate.held,
                breaker=breaker.state,
                recent_429_count=len(self._recent_429s),
            )
            permits, self._adaptive = adaptive_effective_permits(
                state, self._adaptive, self._adaptive_cfg, now=now_mono
            )

            # Stale-reading safety net (same rationale as the concurrency
            # reconciler below): when the usage reading is stale AND there are
            # recent rate_limit 429s, the AIMD controller's stale-decrease is
            # gated by min_decrease_interval (30s default), so permits can be
            # held steady into a rejecting upstream — a fail-open window.
            # Tighten to min_floor as a backstop (AGENTS.md rule 1).
            if not cached.ok and len(self._recent_rate_limit_429s) > 0:
                permits = min(permits, self._adaptive_cfg.min_floor)
        else:
            # Concurrency reconciler — the umans path (regression gate).
            # Record the (observed, local) pairing for windowed phantom estimation.
            # Only record real readings — synthetic fail-safe samples would poison
            # the window with fabricated concurrent_sessions values.
            # Use held_at_fetch (captured before the fetch) so the pairing reflects
            # the state at the time the provider counted the sessions (WI-017).
            if cached.ok:
                self._phantom_samples.append((reading.concurrent_sessions, held_at_fetch))
            phantom_est = phantom_estimate(self._phantom_samples)

            state = ControllerState(
                reading=reading,
                local_in_flight=self._gate.held,
                breaker=breaker.state,
                phantom_estimate=phantom_est,
            )
            permits = effective_permits(state, self._ctrl_cfg, now=now_wall)
            self._last_phantom_estimate = phantom_est

            # Stale-reading safety net (adversarial review 2026-07-07):
            # When the usage reading is stale AND there are recent rate_limit
            # 429s, the poll-driven gate can't see the upstream's state and
            # the breaker can't trip (rate_limit 429s don't feed it).  Without
            # this, sluice would forward at target - stale_penalty (e.g. 3)
            # into an upstream that is actively rejecting requests — a fail-open
            # window (AGENTS.md rule 1).  Tighten to min_floor as a backstop.
            # The breaker remains the backstop for concurrency-classified 429s;
            # this covers the gap for rate_limit-classified 429s when the poll
            # is the only available signal and it's unavailable.
            if not cached.ok and len(self._recent_rate_limit_429s) > 0:
                permits = min(permits, self._ctrl_cfg.min_floor)

        await self._gate.resize(permits)

        # Cache for metrics / status.
        self._last_permits = permits
        self._last_band = classify_band(reading, now=now_wall)
        self._last_reading_cached = cached
        self._last_age = age

        # Throughput: requests forwarded since the previous tick (WI-023).
        # Computed as the delta of the cumulative counter, so it reflects
        # actual traffic in this tick interval — zero means idle.
        self._last_throughput = self._total_requests_forwarded - self._prev_total_requests_forwarded
        self._prev_total_requests_forwarded = self._total_requests_forwarded

        # Request-window reconciliation: prune local timestamps to the
        # provider's window and compute the delta against requests_in_window.
        window_s = reading.requests_window_seconds
        if window_s is not None:
            cutoff = now_mono - window_s
            while self._request_timestamps and self._request_timestamps[0] < cutoff:
                self._request_timestamps.popleft()
            local_count = len(self._request_timestamps)
            self._last_local_requests_in_window = local_count
            if cached.ok and reading.requests_in_window is not None:
                self._last_request_window_delta = (
                    reading.requests_in_window - local_count
                )
            else:
                self._last_request_window_delta = None
        else:
            self._last_local_requests_in_window = None
            self._last_request_window_delta = None

        if cached.ok:
            self._first_poll_ok = True

        # Idle detection (WI-022): when the system is quiescent, the next
        # poll can happen at the slower idle interval.  Idle means no traffic,
        # no recent 429s, normal band, no phantoms, and a closed breaker.
        # Also requires a fresh reading — if the usage fetch is failing, stay
        # on the fast cadence so recovery is detected promptly (M-1).
        # The _poll_now event lets activity (record_request_forwarded /
        # record_429) wake the loop early from an idle sleep.
        self._idle = (
            cached.ok
            and self._gate.held == 0
            and len(self._recent_429s) == 0
            and len(self._recent_rate_limit_429s) == 0
            and self._last_band is Band.NORMAL
            and self._last_phantom_estimate == 0
            and self._breaker.state is BreakerState.CLOSED
        )

        # Record this tick's state for trend analysis.  The entry is frozen at
        # capture time so the history forms an immutable time series.  Recorded
        # when either the in-memory buffer or the persistent store is configured.
        if self._history is not None or self._history_store is not None:
            obs = reading.concurrent_sessions if cached.ok else None
            entry = HistoryEntry(
                timestamp=now_wall,
                concurrent_sessions=obs,
                local_in_flight=self._gate.held,
                phantom_estimate=self._last_phantom_estimate,
                effective_permits=permits,
                limit=reading.limit if cached.ok else None,
                hard_cap=reading.hard_cap if cached.ok else None,
                band=self._last_band.value,
                breaker=breaker.state.value,
                priority_low=reading.priority_low,
                usage_age=age,
                stale=not cached.ok,
                recent_429s=len(self._recent_429s),
                total_429s=self._total_429s,
                rate_limit_429s=self._total_rate_limit_429s,
                queue_depth=self._gate.queue_depth,
                queue_timeouts=self._gate.queue_timeouts,
                requests_in_window=reading.requests_in_window if cached.ok else None,
                requests_limit=reading.requests_limit if cached.ok else None,
                requests_remaining=reading.requests_remaining if cached.ok else None,
                local_requests_in_window=self._last_local_requests_in_window,
                request_window_delta=self._last_request_window_delta,
                throughput=self._last_throughput,
            )
            if self._history is not None:
                self._history.append(entry)
            if self._history_store is not None:
                self._history_store.append(entry)

    async def run(self) -> None:
        """Run the reconciliation loop forever (until cancelled).

        Uses a two-speed poll cadence (WI-022): the fast interval when
        active, the slow idle interval when quiescent.  An asyncio.Event
        lets activity (request forwarded, 429) wake the loop early from an
        idle sleep so the gate resizes promptly when traffic resumes.
        """
        if self._poll_now is None:
            self._poll_now = asyncio.Event()
        while True:
            try:
                await self.tick()
                self._tick_count += 1
                if (
                    self._history_store is not None
                    and self._tick_count % _PRUNE_INTERVAL_TICKS == 0
                ):
                    try:
                        self._history_store.prune(
                            ttl_seconds=self._history_ttl, now=self._wall()
                        )
                    except Exception:
                        log.warning("history store prune failed", exc_info=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconciliation tick failed — closing gate (fail-safe)")
                self._last_permits = 0
                try:
                    await self._gate.resize(0)
                except Exception:
                    log.critical("failed to close gate after tick exception")
                if self._history is not None or self._history_store is not None:
                    try:
                        self._record_failed_tick()
                    except Exception:
                        log.warning("_record_failed_tick failed", exc_info=True)
            # Dynamic sleep: slow when idle, fast when active (WI-022).
            interval = self._effective_poll_interval()
            self._poll_now.clear()
            try:
                await asyncio.wait_for(self._poll_now.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    def _record_failed_tick(self) -> None:
        """Record a fail-safe history entry when tick() raises.

        Uses the last-known state (which may be stale) and marks
        ``effective_permits=0``, ``stale=True``, ``tick_failed=True`` so the
        trend shows the gap rather than silently skipping it.
        """
        if self._history is None and self._history_store is None:
            return
        reading = None
        if self._last_reading_cached is not None:
            reading = self._last_reading_cached.reading
        entry = HistoryEntry(
            timestamp=self._wall(),
            concurrent_sessions=reading.concurrent_sessions if reading else None,
            local_in_flight=self._gate.held,
            phantom_estimate=self._last_phantom_estimate,
            effective_permits=0,
            limit=reading.limit if reading else None,
            hard_cap=reading.hard_cap if reading else None,
            band=self._last_band.value,
            breaker=self._breaker.state.value,
            priority_low=reading.priority_low if reading else False,
            usage_age=self._last_age,
            stale=True,
            recent_429s=len(self._recent_429s),
            total_429s=self._total_429s,
            rate_limit_429s=self._total_rate_limit_429s,
            queue_depth=self._gate.queue_depth,
            queue_timeouts=self._gate.queue_timeouts,
            requests_in_window=reading.requests_in_window if reading else None,
            requests_limit=reading.requests_limit if reading else None,
            requests_remaining=reading.requests_remaining if reading else None,
            local_requests_in_window=self._last_local_requests_in_window,
            request_window_delta=self._last_request_window_delta,
            throughput=0,
            tick_failed=True,
        )
        if self._history is not None:
            self._history.append(entry)
        if self._history_store is not None:
            self._history_store.append(entry)

    async def start(self) -> None:
        """Start the background loop as a task."""
        self._stopped = False
        if self._poll_now is None:
            self._poll_now = asyncio.Event()
        if self._task is None:
            self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        """Cancel the background loop and close the truth source + store.

        Sets ``_stopped`` first so in-flight proxy requests calling
        :meth:`record_response_headers` during the drain window return
        early instead of touching the closed truth source (WI-030).
        """
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._truth.close()
        if self._history_store is not None:
            self._history_store.close()

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
    def breaker_half_open_age_seconds(self) -> float | None:
        """Seconds since the breaker entered HALF_OPEN, or None if not half-open."""
        if self._breaker.state is not BreakerState.HALF_OPEN or self._breaker.half_opened_at is None:
            return None
        return self._mono() - self._breaker.half_opened_at

    @property
    def total_429s(self) -> int:
        return self._total_429s

    @property
    def gateway_429s(self) -> int:
        return self._total_gateway_429s

    @property
    def rate_limit_429s(self) -> int:
        return self._total_rate_limit_429s

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
    def avg_hold_seconds(self) -> float:
        """Mean hold duration (acquire→release) from the gate (Plan 013 WI-001)."""
        return self._gate.avg_hold_seconds

    @property
    def saturation_hint(self) -> int:
        """Un-jittered saturation Retry-After estimate (Plan 013 WI-002/WI-005).

        The current pure-estimator output before jitter is applied.
        Used for /status.json and /metrics so operators see the trend,
        not the per-response jitter.
        """
        return saturation_retry_after(
            queue_depth=self._gate.queue_depth,
            capacity=self._gate.capacity,
            avg_hold_seconds=self._gate.avg_hold_seconds,
            floor=_RETRY_AFTER_SATURATION_FLOOR,
            cap=_RETRY_AFTER_SATURATION_CAP,
        )

    def saturation_retry_after(self) -> int:
        """Pressure-derived, jittered Retry-After for saturated 503s (Plan 013 WI-003).

        Feeds the pure estimator (``control.saturation_retry_after``) from live
        gate pressure (``queue_depth`` / ``capacity`` / ``avg_hold_seconds``),
        then applies ±15 % jitter so rejected clients don't return in a
        synchronized wave.  Clamped to ``[5, 60]``.

        Called by the proxy's queue-timeout 503 path (flavour a).  This path
        can fire while ``gate_closed_reason()`` is ``"open"`` — do not dispatch
        on the reason here.
        """
        estimate = self.saturation_hint
        jittered = estimate * (0.85 + self._rng() * 0.30)
        return max(
            _RETRY_AFTER_SATURATION_FLOOR,
            min(_RETRY_AFTER_SATURATION_CAP, math.ceil(jittered)),
        )

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
        """True once the first successful truth fetch has completed."""
        return self._first_poll_ok

    @property
    def phantom_estimate_value(self) -> int:
        """Current windowed phantom estimate (sustained excess)."""
        return self._last_phantom_estimate

    @property
    def controller_name(self) -> str:
        """The active controller strategy name."""
        return self._controller

    @property
    def history(self) -> History | None:
        """The history ring buffer, if configured."""
        return self._history

    @property
    def history_store(self) -> HistoryStore | None:
        """The optional SQLite persistence store, if configured."""
        return self._history_store

    @property
    def provider_name(self) -> str:
        """The provider tag from the last reading (or 'unknown' before first tick)."""
        if self._last_reading_cached is not None:
            return self._last_reading_cached.reading.provider
        return "unknown"

    @property
    def last_reading(self) -> CachedReading | None:
        """The last cached truth-source reading, or None before the first tick."""
        return self._last_reading_cached

    @property
    def target(self) -> int:
        return self._ctrl_cfg.target

    @property
    def min_floor(self) -> int:
        return self._ctrl_cfg.min_floor

    @property
    def usage_fresh_ttl(self) -> float:
        return self._ctrl_cfg.usage_fresh_ttl

    @property
    def phantom_window(self) -> int:
        return self._ctrl_cfg.phantom_window

    @property
    def breaker_threshold(self) -> int:
        return self._brk_cfg.threshold

    @property
    def breaker_window_seconds(self) -> float:
        return self._brk_cfg.window_seconds

    @property
    def breaker_cooldown_seconds(self) -> float:
        return self._brk_cfg.cooldown_seconds

    @property
    def poll_interval(self) -> float:
        return self._poll_interval

    @property
    def poll_interval_idle(self) -> float | None:
        """The configured idle poll interval, or None if backoff is disabled (WI-022)."""
        return self._poll_interval_idle_cfg

    @property
    def is_idle(self) -> bool:
        """True when the system was idle at the last tick (WI-022)."""
        return self._idle

    def _effective_poll_interval(self) -> float:
        """The sleep interval for the next poll cycle (WI-022).

        When idle and idle backoff is enabled, returns ``poll_interval_idle``
        capped at ``usage_fresh_ttl * 0.8`` so the usage reading does not go
        stale before the next poll.  Otherwise returns ``poll_interval``.
        """
        if self._idle and self._poll_interval_idle_cfg is not None:
            cap = self._ctrl_cfg.usage_fresh_ttl * 0.8
            return min(self._poll_interval_idle_cfg, cap)
        return self._poll_interval

    @property
    def total_requests_forwarded(self) -> int:
        """Total requests forwarded upstream since startup."""
        return self._total_requests_forwarded

    @property
    def last_throughput(self) -> int:
        """Requests forwarded in the last tick interval (WI-023)."""
        return self._last_throughput

    @property
    def local_requests_in_window(self) -> int | None:
        """Requests sluice forwarded within the provider's rolling window.

        None when the provider reports no request window (e.g. Code Max
        'unlimited' plans).
        """
        return self._last_local_requests_in_window

    @property
    def request_window_delta(self) -> int | None:
        """Provider's requests_in_window minus sluice's local count.

        Positive = requests made outside sluice (leakage).  None when
        the provider reports no request window or the reading is stale.
        """
        return self._last_request_window_delta

    def is_low_interactivity(self) -> bool:
        """True while the account is in umans' low-interactivity service mode.

        Surfaced for observability and for switchboard's routing decision (Plan
        010 Feature 0).  Deliberately does NOT feed :meth:`gate_closed_reason`:
        low-interactivity still serves (degraded), so a *direct* sluice user
        keeps getting service rather than a blanket 503 for the whole penalty
        window.  switchboard — which has an alternate provider — is where this
        becomes a route-away decision.
        """
        if self._last_reading_cached is None:
            return False
        return is_low_interactivity(
            self._last_reading_cached.reading, now=self._wall()
        )

    def gate_closed_reason(self) -> str:
        """Why the gate is shut: 'open', 'boxed', 'breaker', or 'saturated'.

        The proxy consults this before ``acquire`` to fast-fail immediately
        when the gate cannot open (boxed / breaker), rather than burning the
        full queue timeout.
        """
        if self._last_reading_cached is not None:
            r = self._last_reading_cached.reading
            # Only a hard box closes the gate; the "rate_limited" rung keeps
            # serving at reduced permits (docs/wi-024-429-capture-2026-07-03.md).
            if is_hard_boxed(r, now=self._wall()):
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
          Unjittered — a deadline, not a load estimate.
        - **breaker:** remaining cooldown (``ceil(cooldown - elapsed)``), or
          ``RETRY_AFTER_SHORT`` (5 s) if ``opened_at`` is unknown.
          Unjittered — a deadline.
        - **saturated** (``_last_permits == 0``): ``max(estimator, ceil(poll_interval))``,
          then ±15 % jitter, clamped to ``[max(5, ceil(poll_interval)), 60]``.
          Nothing can change until the reconcile loop next resizes, so the poll
          cadence is the floor.  (The proxy's not_ready path also tracks the
          poll interval: ``max(2, ceil(poll_interval))``.)
        - **open:** ``_RETRY_AFTER_SATURATION_FLOOR`` (5 s).  The gate is open;
          this is a fallback, not a pressure estimate.

        ``draining`` / ``not_leader`` are not returned by :meth:`gate_closed_reason`
        and so are not handled here — the proxy emits ``RETRY_AFTER_SHORT`` (5 s)
        directly for them (the routing concern dominates the timing one; the
        honest value is genuinely unknowable).
        """
        reason = self.gate_closed_reason()
        if reason == "boxed":
            if self._last_reading_cached is not None:
                r = self._last_reading_cached.reading
                if r.resets_at_epoch is not None:
                    remaining = math.ceil(r.resets_at_epoch - self._wall())
                    result = max(30, remaining)
                    if not self._last_reading_cached.ok:
                        return min(result, _RETRY_AFTER_STALE_CAP)
                    return result
            return 30  # floor when resets_at is unknown
        if reason == "breaker":
            if self._breaker.opened_at is not None:
                elapsed = self._mono() - self._breaker.opened_at
                cooldown_remaining = self._brk_cfg.cooldown_seconds - elapsed
                return max(1, math.ceil(cooldown_remaining))
            return RETRY_AFTER_SHORT
        if reason == "saturated":
            # Flavour (b): structurally saturated — nothing can change until
            # the reconcile loop next resizes, so the poll cadence is the floor.
            # The cap always wins over the poll floor — a value above 60 s
            # would be discarded by the SDK (noise, not honesty).
            poll_floor = min(
                _RETRY_AFTER_SATURATION_CAP,
                max(_RETRY_AFTER_SATURATION_FLOOR, math.ceil(self._poll_interval)),
            )
            estimate = max(self.saturation_hint, poll_floor)
            jittered = estimate * (0.85 + self._rng() * 0.30)
            return max(poll_floor, min(_RETRY_AFTER_SATURATION_CAP, math.ceil(jittered)))
        return _RETRY_AFTER_SATURATION_FLOOR  # open
