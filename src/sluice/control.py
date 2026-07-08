"""Pure, deterministic concurrency controller — the truth path.

This module is the design spine (docs/concurrency-model.md) made executable. It imports
**nothing outside the standard library**, does **no I/O**, and reads **no clock**: the
current time and every observation are passed in as arguments so decisions are fully
reproducible and unit-testable without a network or a model.

Enforced by tests/test_import_boundary.py.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum

# ---------------------------------------------------------------------------
# Observations (inputs)
# ---------------------------------------------------------------------------


class Band(str, Enum):
    """Where observed concurrency sits on the provider's enforcement ladder."""

    NORMAL = "normal"  # <= limit
    LOW = "low"  # limit < observed <= hard_cap (priority.low territory)
    REJECT = "reject"  # observed > hard_cap (429s)
    BOXED = "boxed"  # account paused (boxed_until set and not yet elapsed)


class BreakerState(str, Enum):
    CLOSED = "closed"  # normal
    OPEN = "open"  # backing off; gate closed
    HALF_OPEN = "half_open"  # probing recovery


@dataclass(frozen=True)
class LimitState:
    """A normalized snapshot of the provider's limit state.

    Different providers expose different signals.  umans polls a live concurrency
    count; Anthropic/OpenAI surface token/request buckets via response headers;
    a generic compatible endpoint may expose neither.  This dataclass unions all
    fields so any controller strategy can consume a single type.

    *Concurrency fields* (umans ``/v1/usage``):
        ``concurrent_sessions``, ``limit``, ``hard_cap``, ``priority_low``,
        ``boxed_until_epoch``, ``resets_at_epoch``.

    *Token-bucket fields* (Anthropic/OpenAI response headers):
        ``requests_remaining``, ``tokens_remaining``, ``bucket_reset_epoch``.

    Only the fields the provider supplies are populated; the rest keep their
    defaults.  Controller strategies read only the fields relevant to them.
    """

    # Concurrency fields (umans: polled /v1/usage)
    concurrent_sessions: int = 0
    limit: int = 4
    hard_cap: int = 8
    priority_low: bool = False
    boxed_until_epoch: float | None = None  # seconds since epoch, or None
    resets_at_epoch: float | None = None  # when the box lifts (epoch seconds), or None
    priority_reason: str | None = None  # umans priority.reason ("rate_limited" = deprioritized rung)

    # Token-bucket fields (Anthropic/OpenAI: in-band response headers;
    # umans: polled from /v1/usage limits.requests + usage.requests_in_window)
    requests_limit: int | None = None
    requests_remaining: int | None = None
    requests_in_window: int | None = None  # umans: usage.requests_in_window
    requests_hard_cap: int | None = None  # umans: limits.requests.hard_cap
    requests_window_seconds: int | None = None  # umans: limits.requests.window_seconds
    tokens_limit: int | None = None
    tokens_remaining: int | None = None
    bucket_reset_epoch: float | None = None  # when the bucket resets (epoch seconds)

    # Metadata
    age_seconds: float = 0.0  # how stale this reading is, at decision time
    provider: str = "umans"


# Backward-compatible alias so Plans 001/003 code and tests don't churn.
UsageReading = LimitState


@dataclass(frozen=True)
class ControllerConfig:
    """Operating parameters. Defaults bias toward staying out of the box."""

    target: int = 3  # aim to keep observed concurrency at/below this (one below Max's 4)
    min_floor: int = 1  # never throttle fully closed on uncertainty alone
    usage_fresh_ttl: float = 15.0  # readings older than this are "stale"
    stale_penalty: int = 1  # how much to tighten when the reading is stale
    low_penalty: int = 1  # tighten by this when already in the 'low' band
    phantom_window: int = 3  # samples for sustained phantom detection (Plan 003)

    def __post_init__(self) -> None:
        if self.min_floor < 1:
            raise ValueError(f"min_floor must be >= 1, got {self.min_floor}")


@dataclass(frozen=True)
class ControllerState:
    """Everything the decision needs, assembled by the shell each tick.

    ``recent_429_count`` is populated only for the adaptive controller path
    (``adaptive_effective_permits``); the concurrency reconciler leaves it
    at 0 because the breaker state alone drives its decisions.  This is
    intentional — the two controllers consume different signal sets — but
    means ``effective_permits()`` never sees a non-zero 429 count on the
    umans path.  Tests that exercise the adaptive path should set it
    explicitly; tests of ``effective_permits`` can leave the default.
    """

    reading: LimitState
    local_in_flight: int
    breaker: BreakerState = BreakerState.CLOSED
    phantom_estimate: int = 0  # pre-computed windowed estimate (Plan 003)
    recent_429_count: int = 0  # windowed 429 count — adaptive path only (Plan 006)


# ---------------------------------------------------------------------------
# Derived quantities
# ---------------------------------------------------------------------------


def in_penalty_window(reading: UsageReading, *, now: float) -> bool:
    """True while the provider's ``boxed_until`` timestamp lies in the future."""
    return reading.boxed_until_epoch is not None and now < reading.boxed_until_epoch


def is_deprioritized(reading: UsageReading, *, now: float) -> bool:
    """Penalty window with ``reason == "rate_limited"`` — the deprioritization rung.

    Observed live 2026-07-03 (docs/wi-024-429-capture-2026-07-03.md): umans sets
    ``boxed_until`` for a single limit hit with ``priority.reason = "rate_limited"``,
    but keeps serving normally at low priority.  Only this exact reason is soft;
    a missing or unrecognized reason keeps the fail-safe full stop.
    """
    return in_penalty_window(reading, now=now) and reading.priority_reason == "rate_limited"


def is_hard_boxed(reading: UsageReading, *, now: float) -> bool:
    """Penalty window that is NOT the known-soft deprioritization rung.

    Fail safe (AGENTS.md rule 1): any ``boxed_until`` whose reason we cannot
    positively identify as ``rate_limited`` closes the gate, exactly as before
    the reason field was parsed.
    """
    return in_penalty_window(reading, now=now) and reading.priority_reason != "rate_limited"


def classify_band(reading: UsageReading, *, now: float) -> Band:
    """Map an observation onto the provider's enforcement ladder."""
    if is_hard_boxed(reading, now=now):
        return Band.BOXED
    obs = reading.concurrent_sessions
    if obs > reading.hard_cap:
        return Band.REJECT
    if obs > reading.limit or reading.priority_low or is_deprioritized(reading, now=now):
        return Band.LOW
    return Band.NORMAL


def phantom_estimate_instant(reading: UsageReading, local_in_flight: int) -> int:
    """Instantaneous excess of observed over local — a single sample.

    The provider's count is authority; any excess over what sluice is holding is treated
    as phantom load.  Used as a building block for the windowed estimate; callers should
    prefer :func:`phantom_estimate` (windowed) to avoid over-throttling on transient lag.
    """
    return max(0, reading.concurrent_sessions - local_in_flight)


def phantom_estimate(samples: Sequence[tuple[int, int]]) -> int:
    """Windowed phantom estimate: sustained excess over K samples.

    Each sample is ``(observed_i, local_in_flight_i)`` captured *at the time reading i
    was taken*.  Returns ``max(0, min_i(observed_i − local_in_flight_i))``.

    A transient spike (a just-completed request still in the lagged ``observed``) appears
    in only one sample, so the windowed ``min`` drops it.  A genuine phantom present in
    *every* sample survives the ``min`` — its excess is the floor the ``min`` selects.

    Fail-safe bound: a brand-new real phantom is only ignored for at most ``K−1`` polls,
    strictly shorter than the breaker window and far shorter than the day-scale box
    accumulation.  The slow backstops still catch sustained overload.
    """
    if not samples:
        return 0
    return max(0, min(obs - local for obs, local in samples))


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


# ---------------------------------------------------------------------------
# The decision
# ---------------------------------------------------------------------------


def effective_permits(state: ControllerState, config: ControllerConfig, *, now: float) -> int:
    """How many permits the gate should currently allow.

    Guarantees (see docs/concurrency-model.md §3):

    * Fail safe — every uncertain input (box, breaker, priority.low, staleness, phantoms)
      can only *lower* the result. Bad information never widens the gate.
    * Phantom-absorbing — provider-observed excess shrinks the gate so phantoms age out
      before sluice adds more load.  The estimate is windowed (Plan 003) so transient
      release-lag does not cause self-throttling.
    * Pure — ``now`` and the reading's ``age_seconds`` are supplied; no clock is read here.
    """
    reading = state.reading

    # Hard stops first.  A penalty window whose reason is known-soft
    # ("rate_limited") is NOT a stop — it is handled as a cap below.
    if is_hard_boxed(reading, now=now):
        return 0
    if state.breaker is BreakerState.OPEN:
        return 0

    base = config.target

    # Already deprioritised → drain back under target.
    if classify_band(reading, now=now) is Band.LOW:
        base -= config.low_penalty

    # Deprioritization window: serve at the account limit — one below it when
    # target already consumes the full limit — never fully closed.  The
    # provider keeps serving on this rung (proven live, see capture doc);
    # closing the gate here would turn a soft penalty into a full outage.
    if is_deprioritized(reading, now=now):
        cap = reading.limit if config.target < reading.limit else max(1, reading.limit - 1)
        base = min(base, cap)

    # Absorb phantoms only when the reading is fresh enough to trust the number;
    # otherwise apply a flat staleness penalty rather than assuming zero phantoms.
    if reading.age_seconds <= config.usage_fresh_ttl:
        base -= state.phantom_estimate
    else:
        base = min(base, config.target - config.stale_penalty)

    # A half-open breaker admits at most one probe — but never *raises* the
    # gate above what the phantom/penalty math already computed.  If phantoms
    # or penalties drove base to 0, HALF_OPEN must not widen it to 1 (fail-safe).
    if state.breaker is BreakerState.HALF_OPEN:
        return max(0, min(base, 1))

    # Clamp target against the provider's hard_cap — a runtime override (or a
    # plan downgrade) can leave target > hard_cap, and the provider will punish
    # requests above hard_cap (AGENTS.md rule 1).  The LKG hard_cap is the
    # last-known upper bound and must be respected even when stale: ignoring it
    # would be fail-open (a downgrade during a poll outage would forward above
    # the real limit).  The stale_penalty already tightened ``base``; the clamp
    # further restricts the ceiling to the last-known safe bound.
    return _clamp(base, config.min_floor, min(config.target, reading.hard_cap))


# ---------------------------------------------------------------------------
# Breaker state machine (pure — time and events passed as arguments)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BreakerConfig:
    """Circuit-breaker thresholds. Trips on sustained 429s before the box hits."""

    threshold: int = 5  # this many 429s within window → open
    window_seconds: float = 300.0  # 5-minute rolling window
    cooldown_seconds: float = 60.0  # OPEN → HALF_OPEN after this long
    probe_timeout_seconds: float = 30.0  # HALF_OPEN → OPEN if no probe result


@dataclass(frozen=True)
class BreakerSnapshot:
    """Breaker state carried across calls (the shell holds one)."""

    state: BreakerState = BreakerState.CLOSED
    opened_at: float | None = None  # monotonic timestamp of last OPEN transition
    half_opened_at: float | None = None  # monotonic timestamp of HALF_OPEN transition


def _count_recent(
    timestamps: Sequence[float], *, now: float, window: float
) -> int:
    return sum(1 for t in timestamps if t >= now - window)


def breaker_on_tick(
    snap: BreakerSnapshot,
    recent_429s: Sequence[float],
    *,
    now: float,
    config: BreakerConfig,
) -> BreakerSnapshot:
    """Time-driven transitions, called each reconciliation tick.

    * CLOSED → OPEN when ``threshold`` 429s accumulated in the window.
    * OPEN → HALF_OPEN after ``cooldown_seconds``.
    * HALF_OPEN → OPEN after ``probe_timeout_seconds`` with no event (success/429).
      Without this, a breaker that enters half-open but never receives a probe
      result (e.g. all requests are fast-failed by the proxy for other reasons)
      would stay half-open forever (WI-020).
    """
    if snap.state is BreakerState.CLOSED:
        if _count_recent(recent_429s, now=now, window=config.window_seconds) >= config.threshold:
            return BreakerSnapshot(state=BreakerState.OPEN, opened_at=now)
        return snap

    if snap.state is BreakerState.OPEN:
        if snap.opened_at is not None and now - snap.opened_at >= config.cooldown_seconds:
            return BreakerSnapshot(
                state=BreakerState.HALF_OPEN, opened_at=snap.opened_at, half_opened_at=now
            )
        return snap

    # HALF_OPEN: check for probe timeout.
    if snap.half_opened_at is not None and now - snap.half_opened_at >= config.probe_timeout_seconds:
        return BreakerSnapshot(state=BreakerState.OPEN, opened_at=now, half_opened_at=None)

    return snap


def breaker_on_429(
    snap: BreakerSnapshot,
    recent_429s: Sequence[float],
    *,
    now: float,
    config: BreakerConfig,
) -> BreakerSnapshot:
    """Event: a concurrency 429 was received from the upstream.

    Trips immediately if the threshold is met (don't wait for the next tick).
    If half-open (probing), the probe failed → back to OPEN.

    Note: only ``concurrency``-classified 429s (no/zero retry-after) reach
    this function.  ``rate_limit``-classified 429s (positive retry-after) are
    tracked separately and do NOT call this — see ``record_rate_limit_429``
    in ``reconcile.py``.
    """
    if snap.state is BreakerState.HALF_OPEN:
        return BreakerSnapshot(
            state=BreakerState.OPEN, opened_at=now, half_opened_at=None
        )

    if snap.state is BreakerState.OPEN:
        return snap

    if snap.state is BreakerState.CLOSED:
        if _count_recent(recent_429s, now=now, window=config.window_seconds) >= config.threshold:
            return BreakerSnapshot(state=BreakerState.OPEN, opened_at=now)

    return snap


def breaker_on_success(snap: BreakerSnapshot) -> BreakerSnapshot:
    """Event: an upstream request succeeded (probe succeeded if half-open)."""
    if snap.state is BreakerState.HALF_OPEN:
        return BreakerSnapshot(state=BreakerState.CLOSED, opened_at=snap.opened_at, half_opened_at=None)
    return snap


# ---------------------------------------------------------------------------
# AdaptiveRateController — AIMD for header/429-driven providers (Plan 006)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AdaptiveConfig:
    """AIMD operating parameters for the adaptive rate controller.

    Defaults bias toward conservative throughput: small additive step,
    aggressive backoff factor.
    """

    target: int = 3  # aim to keep permits at/below this
    min_floor: int = 1  # never throttle fully closed on uncertainty alone
    additive_step: int = 1  # increase by this each tick when healthy
    backoff_factor: float = 0.5  # multiply permits by this on a bad signal
    fresh_ttl: float = 15.0  # headers older than this are "stale"
    low_remaining_fraction: float = 0.2  # below this → backoff
    min_decrease_interval: float = 30.0  # minimum seconds between multiplicative decreases

    def __post_init__(self) -> None:
        if self.min_floor < 1:
            raise ValueError(f"min_floor must be >= 1, got {self.min_floor}")


@dataclass(frozen=True)
class AdaptiveSnapshot:
    """AIMD state carried across calls (the shell holds one).

    Like :class:`BreakerSnapshot`, this is pure data — the shell threads it
    through the controller and back.
    """

    current_permits: int = 1  # start conservative
    last_decrease_at: float | None = None
    last_decrease_monotonic: float = 0.0


def adaptive_effective_permits(
    state: ControllerState,
    adaptive: AdaptiveSnapshot,
    config: AdaptiveConfig,
    *,
    now: float,
) -> tuple[int, AdaptiveSnapshot]:
    """AIMD controller for header/429-driven providers (Plan 006 WI-003).

    Additive-increase toward ``target`` while budget headers are healthy and
    no recent 429s; multiplicative-decrease on a 429, on
    ``tokens_remaining``/``requests_remaining`` falling below a fraction of the
    bucket, or on stale headers.

    Guarantees (same contract as :func:`effective_permits`):

    * Fail safe — every uncertain input (stale headers, low remaining, 429s,
      breaker) can only *lower* the result.
    * Pure — ``now`` and the reading's ``age_seconds`` are supplied; no clock
      is read here.
    * Monotone-safe — a bad signal never raises permits.

    Returns ``(permits, new_adaptive_snapshot)`` so the shell can thread the
    AIMD level across ticks.
    """
    reading = state.reading

    # Hard stops (shared breaker — same as the concurrency reconciler).  The
    # known-soft "rate_limited" window does not stop the adaptive controller
    # either; its AIMD decrease path handles degraded signals.
    if is_hard_boxed(reading, now=now):
        return 0, adaptive
    if state.breaker is BreakerState.OPEN:
        return 0, adaptive

    # A half-open breaker admits at most one probe — but never *raises* the
    # AIMD level above 1 (fail-safe, same logic as the concurrency reconciler).
    if state.breaker is BreakerState.HALF_OPEN:
        return max(0, min(adaptive.current_permits, 1)), adaptive

    # Determine whether a bad signal calls for multiplicative decrease.
    decrease = False

    # Recent 429s → back off (even below the breaker threshold).
    if state.recent_429_count > 0:
        decrease = True

    # Low remaining requests → back off.
    if (
        reading.requests_remaining is not None
        and reading.requests_remaining <= 0
    ):
        decrease = True
    elif (
        reading.requests_remaining is not None
        and reading.requests_limit is not None
        and reading.requests_limit > 0
        and reading.requests_remaining
        <= reading.requests_limit * config.low_remaining_fraction
    ):
        decrease = True

    # Low remaining tokens → back off.
    if (
        reading.tokens_remaining is not None
        and reading.tokens_remaining <= 0
    ):
        decrease = True
    elif (
        reading.tokens_remaining is not None
        and reading.tokens_limit is not None
        and reading.tokens_limit > 0
        and reading.tokens_remaining
        <= reading.tokens_limit * config.low_remaining_fraction
    ):
        decrease = True

    # Stale headers → tighten (don't trust the numbers).
    if reading.age_seconds > config.fresh_ttl:
        decrease = True

    if decrease:
        if now - adaptive.last_decrease_monotonic >= config.min_decrease_interval:
            new_permits = min(
                config.target,
                max(
                    config.min_floor,
                    int(adaptive.current_permits * config.backoff_factor),
                ),
            )
            return new_permits, AdaptiveSnapshot(
                current_permits=new_permits,
                last_decrease_at=now,
                last_decrease_monotonic=now,
            )
        return adaptive.current_permits, adaptive

    # Healthy → additive increase toward target.
    new_permits = min(config.target, adaptive.current_permits + config.additive_step)
    return new_permits, AdaptiveSnapshot(
        current_permits=new_permits,
        last_decrease_at=adaptive.last_decrease_at,
        last_decrease_monotonic=adaptive.last_decrease_monotonic,
    )


# ---------------------------------------------------------------------------
# Runtime override validation (Plan 011 — pure, provider-aware bounds)
# ---------------------------------------------------------------------------


def validate_target_override(
    value: int, reading: UsageReading
) -> str | None:
    """Validate a target override against provider limits.

    Returns ``None`` on clean accept, a warning string on accept-with-warning.
    Raises :class:`ValueError` on rejection (caller returns 400 + reason).

    Bounds (from Plan 011 §4):

    * ``value < 1`` → reject (a zero-permit target is a deploy decision, not runtime).
    * ``value > hard_cap`` → reject (the provider will punish requests above this;
      never let an operator configure a value the provider won't serve).
    * ``value > limit`` → accept-with-warning (this is the WI-024 experiment shape —
      requests above ``limit`` run at low priority, sometimes wanted).
    * Otherwise → clean accept.
    """
    if value < 1:
        raise ValueError(f"target must be >= 1, got {value}")
    if value > reading.hard_cap:
        raise ValueError(
            f"target {value} exceeds hard_cap {reading.hard_cap} — "
            "the provider will reject requests above this"
        )
    if value > reading.limit:
        return (
            f"target {value} is above limit {reading.limit} — "
            "requests above the limit run at low priority"
        )
    return None


# ---------------------------------------------------------------------------
# Saturation Retry-After estimator (Plan 013 WI-002)
# ---------------------------------------------------------------------------


def saturation_retry_after(
    *,
    queue_depth: int,
    capacity: int,
    avg_hold_seconds: float,
    floor: int = 5,
    cap: int = 60,
) -> int:
    """Pressure-derived Retry-After hint for saturated 503s.

    Estimates how long a retrying client should wait before rejoining the queue:

    .. code-block:: text

        expected_wait ≈ (queue_depth + 1) × avg_hold_seconds / capacity

    The ``+1`` is the retrying client itself rejoining the back of the scramble.
    The result is clamped to ``[floor, cap]``.

    * **Advisory only** — shapes the ``Retry-After`` header, never the permit
      math.  ``effective_permits`` / band logic are untouched.
    * **Fail safe** — floored at ``floor`` (5 s): the estimator can never
      promise a *faster* retry than today's default.  When there is no data
      (no hold samples yet → ``avg_hold_seconds <= 0``), falls back to ``floor``.
    * **Pure** — no clock, no randomness.  Jitter is applied by the shell
      (:mod:`sluice.reconcile`) so tests stay deterministic.

    Degenerate inputs:

    * ``avg_hold_seconds <= 0`` (no samples yet) → ``floor``.
    * ``capacity <= 0`` (zero-width gate) → ``cap`` — the caller layers in
      the poll cadence (Plan 013 WI-003); the pure core does not know the
      interval.
    """
    if avg_hold_seconds <= 0:
        return floor
    if capacity <= 0:
        return cap
    raw = (queue_depth + 1) * avg_hold_seconds / capacity
    return max(floor, min(cap, math.ceil(raw)))
