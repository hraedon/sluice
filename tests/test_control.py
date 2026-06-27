"""Unit tests for the pure controller. No network, no clock — `now` is supplied."""

from __future__ import annotations

import pytest

from sluice.control import (
    Band,
    BreakerState,
    ControllerConfig,
    ControllerState,
    UsageReading,
    classify_band,
    effective_permits,
    phantom_estimate,
)

NOW = 1_000_000.0
CFG = ControllerConfig(target=3, min_floor=1, usage_fresh_ttl=15.0, stale_penalty=1, low_penalty=1)


def reading(**kw) -> UsageReading:
    base = dict(concurrent_sessions=0, limit=4, hard_cap=8)
    base.update(kw)
    return UsageReading(**base)


def state(r: UsageReading, in_flight: int = 0, breaker=BreakerState.CLOSED) -> ControllerState:
    return ControllerState(reading=r, local_in_flight=in_flight, breaker=breaker)


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


# --- phantom estimate -------------------------------------------------------


def test_phantom_estimate_only_counts_excess():
    assert phantom_estimate(reading(concurrent_sessions=6), local_in_flight=4) == 2
    assert phantom_estimate(reading(concurrent_sessions=2), local_in_flight=4) == 0


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
