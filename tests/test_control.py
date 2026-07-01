"""Unit tests for the pure controller. No network, no clock — `now` is supplied."""

from __future__ import annotations

import pytest

from sluice.control import (
    Band,
    BreakerConfig,
    BreakerSnapshot,
    BreakerState,
    ControllerConfig,
    ControllerState,
    UsageReading,
    classify_band,
    effective_permits,
    phantom_estimate,
    phantom_estimate_instant,
    breaker_on_429,
    breaker_on_success,
    breaker_on_tick,
)

NOW = 1_000_000.0
CFG = ControllerConfig(target=3, min_floor=1, usage_fresh_ttl=15.0, stale_penalty=1, low_penalty=1)
BCFG = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)


def reading(**kw) -> UsageReading:
    base = dict(concurrent_sessions=0, limit=4, hard_cap=8)
    base.update(kw)
    return UsageReading(**base)


def state(r: UsageReading, in_flight: int = 0, breaker=BreakerState.CLOSED, phantom: int | None = None) -> ControllerState:
    if phantom is None:
        phantom = phantom_estimate_instant(r, in_flight)
    return ControllerState(reading=r, local_in_flight=in_flight, breaker=breaker, phantom_estimate=phantom)


# --- band classification ----------------------------------------------------


@pytest.mark.parametrize(
    "sessions,low,boxed,expected",
    [
        (0, False, None, Band.NORMAL),
        (4, False, None, Band.NORMAL),  # == limit is still normal
        (5, False, None, Band.LOW),
        (4, True, None, Band.LOW),  # priority.low overrides count
        (9, False, None, Band.REJECT),
        (1, False, NOW + 100, Band.BOXED),  # box wins over everything
    ],
)
def test_classify_band(sessions, low, boxed, expected):
    r = reading(concurrent_sessions=sessions, priority_low=low, boxed_until_epoch=boxed)
    assert classify_band(r, now=NOW) == expected


def test_box_elapsed_is_not_boxed():
    r = reading(boxed_until_epoch=NOW - 1)
    assert classify_band(r, now=NOW) != Band.BOXED


# --- phantom estimate (instant) -------------------------------------------


def test_phantom_estimate_instant_only_counts_excess():
    assert phantom_estimate_instant(reading(concurrent_sessions=6), local_in_flight=4) == 2
    assert phantom_estimate_instant(reading(concurrent_sessions=2), local_in_flight=4) == 0


# --- phantom estimate (windowed — Plan 003) --------------------------------


def test_phantom_estimate_windowed_empty_returns_zero():
    assert phantom_estimate([]) == 0


def test_phantom_estimate_windowed_sustained_excess():
    # observed runs 2 over local for the whole window → estimate is 2
    samples = [(6, 4), (6, 4), (6, 4)]
    assert phantom_estimate(samples) == 2


def test_phantom_estimate_windowed_single_tick_spike_dropped():
    # one tick of excess (transient release-lag), rest clean → min is 0
    samples = [(6, 4), (4, 4), (4, 4)]
    assert phantom_estimate(samples) == 0


def test_phantom_estimate_windowed_churn_trace():
    # admit/release rapidly while observed lags one tick high → estimate is 0
    samples = [(5, 3), (3, 3), (3, 3)]
    assert phantom_estimate(samples) == 0


def test_phantom_estimate_windowed_monotonicity():
    # higher sustained observed never raises permits (lower estimate is worse)
    low = phantom_estimate([(5, 3), (5, 3), (5, 3)])
    high = phantom_estimate([(7, 3), (7, 3), (7, 3)])
    assert high >= low


# --- effective_permits hard stops -------------------------------------------


def test_boxed_closes_gate():
    r = reading(boxed_until_epoch=NOW + 60)
    assert effective_permits(state(r), CFG, now=NOW) == 0


def test_open_breaker_closes_gate():
    assert effective_permits(state(reading(), breaker=BreakerState.OPEN), CFG, now=NOW) == 0


def test_half_open_breaker_admits_one_probe():
    r = reading(concurrent_sessions=0)
    assert effective_permits(state(r, breaker=BreakerState.HALF_OPEN), CFG, now=NOW) == 1


# --- effective_permits steady state -----------------------------------------


def test_normal_gives_target():
    assert effective_permits(state(reading(concurrent_sessions=2), in_flight=2), CFG, now=NOW) == 3


def test_phantom_shrinks_gate():
    # provider sees 6, sluice holds 4 → 2 phantoms → target(3) - 2 = 1
    r = reading(concurrent_sessions=6)
    assert effective_permits(state(r, in_flight=4), CFG, now=NOW) == 1


def test_priority_low_tightens():
    r = reading(concurrent_sessions=4, priority_low=True)
    # low_penalty 1 → base 2; in_flight 4 means no extra phantom (4 vs 4)
    assert effective_permits(state(r, in_flight=4), CFG, now=NOW) == 2


def test_stale_reading_tightens_and_ignores_zero_phantom():
    # stale reading: don't trust concurrent_sessions=0; apply stale_penalty instead
    r = reading(concurrent_sessions=0, age_seconds=60.0)
    assert effective_permits(state(r, in_flight=0), CFG, now=NOW) == CFG.target - CFG.stale_penalty


# --- the invariant: uncertainty never widens the gate -----------------------


def test_monotonicity_no_uncertain_input_raises_result():
    baseline = effective_permits(state(reading(concurrent_sessions=2), in_flight=2), CFG, now=NOW)
    worse_cases = [
        state(reading(concurrent_sessions=6), in_flight=2),  # phantoms
        state(reading(concurrent_sessions=2, priority_low=True), in_flight=2),  # low
        state(reading(concurrent_sessions=2, age_seconds=99), in_flight=2),  # stale
        state(reading(concurrent_sessions=2), in_flight=2, breaker=BreakerState.HALF_OPEN),
        state(reading(concurrent_sessions=2, boxed_until_epoch=NOW + 9), in_flight=2),  # boxed
    ]
    for s in worse_cases:
        assert effective_permits(s, CFG, now=NOW) <= baseline


# --- breaker state machine -------------------------------------------------


def _snap(state=BreakerState.CLOSED, opened_at=None) -> BreakerSnapshot:
    return BreakerSnapshot(state=state, opened_at=opened_at)


def test_breaker_closed_stays_closed_with_no_429s():
    snap = breaker_on_tick(_snap(), [], now=NOW, config=BCFG)
    assert snap.state is BreakerState.CLOSED


def test_breaker_closed_trips_on_threshold():
    four29s = [NOW - 10, NOW - 5, NOW - 1]  # 3 within window
    snap = breaker_on_tick(_snap(), four29s, now=NOW, config=BCFG)
    assert snap.state is BreakerState.OPEN
    assert snap.opened_at == NOW


def test_breaker_does_not_trip_below_threshold():
    four29s = [NOW - 10, NOW - 5]  # only 2
    snap = breaker_on_tick(_snap(), four29s, now=NOW, config=BCFG)
    assert snap.state is BreakerState.CLOSED


def test_breaker_open_to_half_open_after_cooldown():
    snap = _snap(state=BreakerState.OPEN, opened_at=NOW - 100)
    result = breaker_on_tick(snap, [], now=NOW, config=BCFG)
    assert result.state is BreakerState.HALF_OPEN


def test_breaker_open_stays_open_before_cooldown():
    snap = _snap(state=BreakerState.OPEN, opened_at=NOW - 10)
    result = breaker_on_tick(snap, [], now=NOW, config=BCFG)
    assert result.state is BreakerState.OPEN


def test_breaker_on_429_trips_immediately_at_threshold():
    # 2 previous 429s + this one = 3 = threshold
    four29s = [NOW - 10, NOW - 5, NOW]
    snap = breaker_on_429(_snap(), four29s, now=NOW, config=BCFG)
    assert snap.state is BreakerState.OPEN
    assert snap.opened_at == NOW


def test_breaker_on_429_half_open_back_to_open():
    snap = _snap(state=BreakerState.HALF_OPEN, opened_at=NOW - 100)
    result = breaker_on_429(snap, [NOW], now=NOW, config=BCFG)
    assert result.state is BreakerState.OPEN
    assert result.opened_at == NOW


def test_breaker_on_success_half_open_to_closed():
    snap = _snap(state=BreakerState.HALF_OPEN, opened_at=NOW - 100)
    result = breaker_on_success(snap)
    assert result.state is BreakerState.CLOSED


def test_breaker_on_success_closed_stays_closed():
    snap = _snap(state=BreakerState.CLOSED)
    assert breaker_on_success(snap).state is BreakerState.CLOSED


def test_breaker_old_429s_outside_window_not_counted():
    # 3 429s but one is outside the window
    four29s = [NOW - 400, NOW - 5, NOW - 1]  # first is >300s old
    snap = breaker_on_tick(_snap(), four29s, now=NOW, config=BCFG)
    assert snap.state is BreakerState.CLOSED  # only 2 within window


# --- WI-015: half-open breaker must not raise permits above base ---------------


def test_half_open_does_not_raise_above_base_when_phantoms_absorb_all():
    """When phantoms drive base to 0, HALF_OPEN must not widen to 1.

    The old code used _clamp(base, 1, target) which raised 0→1.
    The fix returns max(0, min(base, 1)) — permits stay at 0.
    """
    r = reading(concurrent_sessions=10)
    # phantom_estimate = 10 - 0 = 10, base = 3 - 10 = -7 → clamp to 0
    s = state(r, in_flight=0, breaker=BreakerState.HALF_OPEN, phantom=10)
    assert effective_permits(s, CFG, now=NOW) == 0


def test_half_open_still_admits_one_when_no_phantoms():
    """When base is positive, HALF_OPEN caps at 1 (one probe)."""
    r = reading(concurrent_sessions=0)
    s = state(r, in_flight=0, breaker=BreakerState.HALF_OPEN, phantom=0)
    assert effective_permits(s, CFG, now=NOW) == 1


def test_half_open_caps_at_one_even_when_base_higher():
    """HALF_OPEN never admits more than one probe."""
    r = reading(concurrent_sessions=0)
    s = state(r, in_flight=0, breaker=BreakerState.HALF_OPEN, phantom=0)
    assert effective_permits(s, CFG, now=NOW) == 1


# --- WI-016: breaker_on_429 resets opened_at when OPEN ------------------------


def test_breaker_on_429_open_resets_opened_at():
    """A 429 while OPEN resets opened_at so tick() doesn't prematurely go HALF_OPEN."""
    snap = _snap(state=BreakerState.OPEN, opened_at=NOW - 100)
    result = breaker_on_429(snap, [NOW], now=NOW, config=BCFG)
    assert result.state is BreakerState.OPEN
    assert result.opened_at == NOW  # reset, not the old value


def test_breaker_on_429_open_prevents_premature_half_open():
    """A 429 during OPEN prevents the next tick from transitioning to HALF_OPEN.

    Without the fix, breaker_on_429 returned snap unchanged when OPEN, so
    breaker_on_tick would see the old opened_at and transition to HALF_OPEN.
    """
    snap = _snap(state=BreakerState.OPEN, opened_at=NOW - 100)
    # Simulate a 429 arriving during the fetch
    snap = breaker_on_429(snap, [NOW], now=NOW, config=BCFG)
    assert snap.opened_at == NOW
    # Now tick — cooldown is 60s, only 0s elapsed → stays OPEN
    result = breaker_on_tick(snap, [NOW], now=NOW, config=BCFG)
    assert result.state is BreakerState.OPEN


# --- WI-020: HALF_OPEN → OPEN on probe timeout --------------------------------


def test_breaker_half_open_to_open_on_probe_timeout():
    """HALF_OPEN transitions to OPEN after probe_timeout_seconds."""
    cfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0, probe_timeout_seconds=30.0)
    snap = BreakerSnapshot(state=BreakerState.HALF_OPEN, opened_at=NOW - 100, half_opened_at=NOW - 40)
    result = breaker_on_tick(snap, [], now=NOW, config=cfg)
    assert result.state is BreakerState.OPEN
    assert result.opened_at == NOW
    assert result.half_opened_at is None


def test_breaker_half_open_stays_half_open_before_probe_timeout():
    """HALF_OPEN stays HALF_OPEN before probe_timeout_seconds elapses."""
    cfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0, probe_timeout_seconds=30.0)
    snap = BreakerSnapshot(state=BreakerState.HALF_OPEN, opened_at=NOW - 100, half_opened_at=NOW - 10)
    result = breaker_on_tick(snap, [], now=NOW, config=cfg)
    assert result.state is BreakerState.HALF_OPEN


def test_breaker_half_open_no_half_opened_at_stays_half_open():
    """If half_opened_at is None (legacy), don't time out — wait for event."""
    snap = BreakerSnapshot(state=BreakerState.HALF_OPEN, opened_at=NOW - 100, half_opened_at=None)
    result = breaker_on_tick(snap, [], now=NOW, config=BCFG)
    assert result.state is BreakerState.HALF_OPEN
