"""Pure, deterministic concurrency controller — the truth path.

This module is the design spine (docs/concurrency-model.md) made executable. It imports
**nothing outside the standard library**, does **no I/O**, and reads **no clock**: the
current time and every observation are passed in as arguments so decisions are fully
reproducible and unit-testable without a network or a model.

Enforced by tests/test_import_boundary.py.
"""

from __future__ import annotations

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
class UsageReading:
    """A parsed snapshot of the provider's usage endpoint."""

    concurrent_sessions: int
    limit: int
    hard_cap: int
    priority_low: bool = False
    boxed_until_epoch: float | None = None  # seconds since epoch, or None
    resets_at_epoch: float | None = None  # when the box lifts (epoch seconds), or None
    age_seconds: float = 0.0  # how stale this reading is, at decision time


@dataclass(frozen=True)
class ControllerConfig:
    """Operating parameters. Defaults bias toward staying out of the box."""

    target: int = 3  # aim to keep observed concurrency at/below this (one below Max's 4)
    min_floor: int = 1  # never throttle fully closed on uncertainty alone
    usage_fresh_ttl: float = 15.0  # readings older than this are "stale"
    stale_penalty: int = 1  # how much to tighten when the reading is stale
    low_penalty: int = 1  # tighten by this when already in the 'low' band
    phantom_window: int = 3  # samples for sustained phantom detection (Plan 003)


@dataclass(frozen=True)
class ControllerState:
    """Everything the decision needs, assembled by the shell each tick."""

    reading: UsageReading
    local_in_flight: int
    breaker: BreakerState = BreakerState.CLOSED
    phantom_estimate: int = 0  # pre-computed windowed estimate (Plan 003)


# ---------------------------------------------------------------------------
# Derived quantities
# ---------------------------------------------------------------------------


def classify_band(reading: UsageReading, *, now: float) -> Band:
    """Map an observation onto the provider's enforcement ladder."""
    if reading.boxed_until_epoch is not None and now < reading.boxed_until_epoch:
        return Band.BOXED
    obs = reading.concurrent_sessions
    if obs > reading.hard_cap:
        return Band.REJECT
    if obs > reading.limit or reading.priority_low:
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

    # Hard stops first.
    if reading.boxed_until_epoch is not None and now < reading.boxed_until_epoch:
        return 0
    if state.breaker is BreakerState.OPEN:
        return 0

    base = config.target

    # Already deprioritised → drain back under target.
    if classify_band(reading, now=now) is Band.LOW:
        base -= config.low_penalty

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

    return _clamp(base, config.min_floor, config.target)


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
    """Mutable breaker state carried across calls (the shell holds one)."""

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
    """
    if snap.state is BreakerState.HALF_OPEN:
        return BreakerSnapshot(
            state=BreakerState.OPEN, opened_at=now, half_opened_at=None
        )

    if snap.state is BreakerState.OPEN:
        # A 429 while OPEN resets the cooldown so tick() doesn't prematurely
        # transition to HALF_OPEN (WI-016).  Without this, a 429 arriving
        # during the fetch would be invisible to breaker_on_tick, which would
        # see the old opened_at and transition OPEN→HALF_OPEN.
        return BreakerSnapshot(state=BreakerState.OPEN, opened_at=now, half_opened_at=None)

    if snap.state is BreakerState.CLOSED:
        if _count_recent(recent_429s, now=now, window=config.window_seconds) >= config.threshold:
            return BreakerSnapshot(state=BreakerState.OPEN, opened_at=now)

    return snap


def breaker_on_success(snap: BreakerSnapshot) -> BreakerSnapshot:
    """Event: an upstream request succeeded (probe succeeded if half-open)."""
    if snap.state is BreakerState.HALF_OPEN:
        return BreakerSnapshot(state=BreakerState.CLOSED, opened_at=snap.opened_at, half_opened_at=None)
    return snap
