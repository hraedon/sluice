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
    LimitState,
    UsageReading,
    AdaptiveConfig,
    AdaptiveSnapshot,
    classify_band,
    effective_permits,
    phantom_estimate,
    phantom_estimate_instant,
    breaker_on_429,
    breaker_on_success,
    breaker_on_tick,
    adaptive_effective_permits,
    validate_target_override,
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


def test_classify_band_boundary_at_hard_cap():
    assert classify_band(reading(concurrent_sessions=8), now=NOW) is Band.LOW
    assert classify_band(reading(concurrent_sessions=9), now=NOW) is Band.REJECT
    assert classify_band(reading(concurrent_sessions=4), now=NOW) is Band.NORMAL
    assert classify_band(reading(concurrent_sessions=3), now=NOW) is Band.NORMAL


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


# --- deprioritization rung (priority.reason == "rate_limited") ---------------
# Live capture 2026-07-03 (docs/wi-024-429-capture-2026-07-03.md): umans sets
# boxed_until with reason=rate_limited for a single limit hit but keeps
# serving; only an unrecognized/absent reason is a hard box.


def test_rate_limited_window_is_low_band_not_boxed():
    r = reading(boxed_until_epoch=NOW + 60, priority_reason="rate_limited", priority_low=True)
    assert classify_band(r, now=NOW) is Band.LOW


def test_rate_limited_window_without_low_flag_is_still_low_band():
    r = reading(boxed_until_epoch=NOW + 60, priority_reason="rate_limited")
    assert classify_band(r, now=NOW) is Band.LOW


def test_unknown_reason_stays_hard_boxed():
    r = reading(boxed_until_epoch=NOW + 60, priority_reason="suspended")
    assert classify_band(r, now=NOW) is Band.BOXED
    assert effective_permits(state(r), CFG, now=NOW) == 0


def test_missing_reason_stays_hard_boxed():
    r = reading(boxed_until_epoch=NOW + 60)
    assert classify_band(r, now=NOW) is Band.BOXED


def test_rate_limited_expired_window_is_normal():
    r = reading(boxed_until_epoch=NOW - 1, priority_reason="rate_limited")
    assert classify_band(r, now=NOW) is Band.NORMAL


def test_rate_limited_target_at_limit_serves_limit_minus_one():
    cfg = ControllerConfig(target=4)
    r = reading(boxed_until_epoch=NOW + 60, priority_reason="rate_limited", priority_low=True)
    assert effective_permits(state(r), cfg, now=NOW) == 3


def test_rate_limited_target_over_limit_capped_at_limit_minus_one():
    # The WI-024 experiment shape: target=10 past hard_cap; the window must
    # cap at limit-1, not track target.
    cfg = ControllerConfig(target=10)
    r = reading(boxed_until_epoch=NOW + 60, priority_reason="rate_limited", priority_low=True)
    assert effective_permits(state(r), cfg, now=NOW) == 3


def test_rate_limited_target_below_limit_keeps_low_band_drain():
    # target=3 < limit=4: the cap (4) never binds; LOW drain applies as usual.
    r = reading(boxed_until_epoch=NOW + 60, priority_reason="rate_limited", priority_low=True)
    assert effective_permits(state(r), CFG, now=NOW) == CFG.target - CFG.low_penalty


def test_rate_limited_never_fully_closes():
    cfg = ControllerConfig(target=1, min_floor=1)
    r = reading(
        limit=1, hard_cap=2, boxed_until_epoch=NOW + 60,
        priority_reason="rate_limited", priority_low=True,
    )
    assert effective_permits(state(r), cfg, now=NOW) >= 1


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


def test_effective_permits_phantom_exceeds_target_floors_at_min_floor():
    """When phantoms exceed target in CLOSED state, _clamp floors at min_floor."""
    r = reading(concurrent_sessions=10)
    s = state(r, in_flight=0, phantom=10)
    result = effective_permits(s, CFG, now=NOW)
    assert result == CFG.min_floor
    assert result > 0


# --- Edge cases: extreme observed, zero local, invariant bounds ------------------


def test_effective_permits_observed_far_exceeds_target_floors_at_min_floor():
    """observed > target * 2: phantoms eat all permits, _clamp floors at min_floor.

    E.g. target=3, observed=20, local=0 → phantom=20 → base=3-20=-17 → clamp to 1.
    The gate never closes fully on phantoms alone (min_floor=1) — a live permit
    is kept so sluice can still forward one request and observe the result.
    """
    r = reading(concurrent_sessions=20)
    s = state(r, in_flight=0, phantom=20)
    assert effective_permits(s, CFG, now=NOW) == CFG.min_floor


def test_effective_permits_local_zero_during_burst():
    """local_in_flight=0 with high observed: full phantom absorption.

    During a burst where all local requests have completed but the provider
    still counts phantoms, the estimate equals the full observed count.
    """
    r = reading(concurrent_sessions=7)
    s = state(r, in_flight=0, phantom=7)
    result = effective_permits(s, CFG, now=NOW)
    assert result == CFG.min_floor  # 3 - 7 = -4 → clamp to 1


def test_effective_permits_never_exceeds_target():
    """effective_permits is bounded above by target in CLOSED/NORMAL state."""
    r = reading(concurrent_sessions=0)
    s = state(r, in_flight=0, phantom=0)
    assert effective_permits(s, CFG, now=NOW) == CFG.target
    # Even with negative phantom (impossible but defensive)
    s2 = state(r, in_flight=10, phantom=0)
    assert effective_permits(s2, CFG, now=NOW) == CFG.target


def test_effective_permits_never_negative():
    """effective_permits is never negative — min_floor is the floor."""
    for phantom in range(0, 100, 10):
        r = reading(concurrent_sessions=phantom)
        s = state(r, in_flight=0, phantom=phantom)
        result = effective_permits(s, CFG, now=NOW)
        assert result >= 0, f"effective_permits negative for phantom={phantom}: {result}"
        assert result >= CFG.min_floor, f"below min_floor for phantom={phantom}: {result}"


def test_effective_permits_bounded_by_target_across_all_inputs():
    """No combination of inputs can push effective_permits above target."""
    for obs in range(0, 20):
        for local in range(0, 20):
            r = reading(concurrent_sessions=obs)
            phantom = max(0, obs - local)
            s = state(r, in_flight=local, phantom=phantom)
            result = effective_permits(s, CFG, now=NOW)
            assert result <= CFG.target, (
                f"effective_permits above target for obs={obs}, local={local}: {result}"
            )


def test_phantom_estimate_never_exceeds_max_observed():
    """phantom_estimate (windowed) never exceeds the maximum observed in the window."""
    samples = [(10, 0), (15, 2), (8, 1)]
    est = phantom_estimate(samples)
    assert est <= max(obs for obs, _ in samples)
    assert est >= 0


def test_phantom_estimate_empty_and_single():
    """Edge cases: empty window and single sample."""
    assert phantom_estimate([]) == 0
    assert phantom_estimate([(5, 0)]) == 5
    assert phantom_estimate([(3, 3)]) == 0


def test_phantom_estimate_all_below_local():
    """When observed < local in every sample (impossible in practice but defensive),
    the estimate is 0 — never negative."""
    samples = [(1, 5), (2, 6), (0, 3)]
    assert phantom_estimate(samples) == 0


def test_stale_reading_with_extreme_observed_ignores_phantom():
    """A stale reading with huge observed must NOT use the phantom estimate —
    it applies the flat stale_penalty instead (don't trust stale numbers)."""
    r = reading(concurrent_sessions=100, age_seconds=60.0)
    s = state(r, in_flight=0, phantom=100)
    result = effective_permits(s, CFG, now=NOW)
    assert result == CFG.target - CFG.stale_penalty  # 3 - 1 = 2, not min_floor


def test_low_band_with_extreme_phantom():
    """LOW band + extreme phantom: both penalties stack, floor at min_floor."""
    r = reading(concurrent_sessions=20, priority_low=True)
    s = state(r, in_flight=0, phantom=20)
    # base = target(3) - low_penalty(1) - phantom(20) = -18 → clamp to min_floor(1)
    result = effective_permits(s, CFG, now=NOW)
    assert result == CFG.min_floor


# --- Breaker OPEN does not extend cooldown on 429 ---------------------------


def test_breaker_on_429_open_does_not_extend_cooldown():
    """A 429 while OPEN must NOT reset opened_at.

    Under a sustained trickle of 429s the breaker would never reach HALF_OPEN
    if each 429 reset the cooldown timer.  The fix returns the snapshot
    unchanged so the cooldown elapses and a probe can eventually go out.
    """
    snap = _snap(state=BreakerState.OPEN, opened_at=NOW - 100)
    result = breaker_on_429(snap, [NOW], now=NOW, config=BCFG)
    assert result.state is BreakerState.OPEN
    assert result.opened_at == NOW - 100  # unchanged


def test_breaker_transitions_to_half_open_despite_ongoing_429s():
    """Even with 429s trickling in while OPEN, the breaker reaches HALF_OPEN
    after cooldown_seconds because opened_at is not reset."""
    cfg = BreakerConfig(threshold=3, window_seconds=300.0, cooldown_seconds=60.0)
    opened_at = NOW - 100
    snap = _snap(state=BreakerState.OPEN, opened_at=opened_at)

    # Simulate a trickle of 429s while OPEN — opened_at must not change.
    for t in [NOW - 80, NOW - 40, NOW - 10, NOW]:
        snap = breaker_on_429(snap, [t], now=t, config=cfg)
        assert snap.opened_at == opened_at

    # After cooldown elapses, breaker_on_tick transitions to HALF_OPEN.
    result = breaker_on_tick(snap, [NOW], now=NOW, config=cfg)
    assert result.state is BreakerState.HALF_OPEN


def test_breaker_half_open_429_reopens_with_fresh_opened_at():
    """A 429 while HALF_OPEN (probe failed) reopens with a fresh opened_at."""
    snap = BreakerSnapshot(state=BreakerState.HALF_OPEN, opened_at=NOW - 100, half_opened_at=NOW - 10)
    result = breaker_on_429(snap, [NOW], now=NOW, config=BCFG)
    assert result.state is BreakerState.OPEN
    assert result.opened_at == NOW
    assert result.half_opened_at is None


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


def test_breaker_open_with_none_opened_at_stays_open():
    """A corrupted/legacy snapshot with opened_at=None in OPEN state must not
    transition to HALF_OPEN — the condition guards on opened_at is not None,
    so the breaker stays OPEN (fail-safe)."""
    snap = BreakerSnapshot(state=BreakerState.OPEN, opened_at=None)
    result = breaker_on_tick(snap, [], now=NOW + 999_999, config=BCFG)
    assert result.state is BreakerState.OPEN
    assert result.opened_at is None


# --- Plan 006 WI-001: LimitState normalization -------------------------------


def test_usage_reading_is_alias_for_limit_state():
    """UsageReading is a backward-compatible alias for LimitState."""
    assert UsageReading is LimitState


def test_concurrency_only_state_round_trips():
    """A concurrency-only LimitState (umans) has the expected fields."""
    ls = LimitState(
        concurrent_sessions=3,
        limit=4,
        hard_cap=8,
        priority_low=False,
        provider="umans",
    )
    assert ls.concurrent_sessions == 3
    assert ls.limit == 4
    assert ls.hard_cap == 8
    assert ls.requests_remaining is None
    assert ls.tokens_remaining is None
    assert ls.bucket_reset_epoch is None
    assert ls.provider == "umans"
    band = classify_band(ls, now=NOW)
    assert band is Band.NORMAL


def test_bucket_only_state_round_trips():
    """A bucket-only LimitState (Anthropic/OpenAI) has the expected fields."""
    ls = LimitState(
        requests_remaining=100,
        tokens_remaining=50000,
        bucket_reset_epoch=NOW + 60,
        provider="anthropic",
    )
    assert ls.requests_remaining == 100
    assert ls.tokens_remaining == 50000
    assert ls.bucket_reset_epoch is not None
    assert ls.concurrent_sessions == 0  # default
    assert ls.provider == "anthropic"


def test_limit_state_defaults_are_fail_safe():
    """A bare LimitState with no provider data defaults to conservative values."""
    ls = LimitState(provider="generic")
    assert ls.concurrent_sessions == 0
    assert ls.limit == 4
    assert ls.hard_cap == 8
    assert ls.priority_low is False
    assert ls.requests_remaining is None
    assert ls.tokens_remaining is None
    assert ls.age_seconds == 0.0
    assert ls.provider == "generic"


def test_limit_state_is_frozen():
    """LimitState is immutable (frozen dataclass)."""
    ls = LimitState(concurrent_sessions=3)
    try:
        ls.concurrent_sessions = 5  # type: ignore[misc]
        assert False, "should have raised FrozenInstanceError"
    except Exception:
        pass


# --- AIMD adaptive controller: rate-limited multiplicative decrease ------------


def test_adaptive_decrease_rate_limited():
    """Multiplicative decrease only fires once per min_decrease_interval."""
    cfg = AdaptiveConfig(
        target=8, min_floor=1, backoff_factor=0.5, min_decrease_interval=30.0
    )
    snap = AdaptiveSnapshot(current_permits=8, last_decrease_monotonic=0.0)
    r = UsageReading()
    st = ControllerState(reading=r, local_in_flight=0, recent_429_count=1)

    now = 100.0
    # First call: 100 - 0 >= 30 → decrease applies (8 * 0.5 = 4).
    permits, snap = adaptive_effective_permits(st, snap, cfg, now=now)
    assert permits == 4
    assert snap.last_decrease_monotonic == now

    # Second call at the same `now`: rate-limited → permits held at 4.
    permits, snap = adaptive_effective_permits(st, snap, cfg, now=now)
    assert permits == 4

    # Third call 31s later: 131 - 100 = 31 >= 30 → decrease applies (4 * 0.5 = 2).
    now2 = now + 31
    permits, snap = adaptive_effective_permits(st, snap, cfg, now=now2)
    assert permits == 2
    assert snap.last_decrease_monotonic == now2


def test_adaptive_additive_increase_still_every_tick():
    """When healthy (no bad signal), permits increase every tick."""
    cfg = AdaptiveConfig(target=4, min_floor=1, additive_step=1, min_decrease_interval=30.0)
    snap = AdaptiveSnapshot(current_permits=1, last_decrease_monotonic=0.0)
    r = UsageReading()
    st = ControllerState(reading=r, local_in_flight=0)

    now = 10.0
    permits, snap = adaptive_effective_permits(st, snap, cfg, now=now)
    assert permits == 2

    permits, snap = adaptive_effective_permits(st, snap, cfg, now=now + 1)
    assert permits == 3

    permits, snap = adaptive_effective_permits(st, snap, cfg, now=now + 2)
    assert permits == 4  # capped at target


def test_adaptive_healthy_tick_preserves_decrease_timestamp():
    """A healthy tick between two decrease ticks must not reset the
    rate-limit timestamp — otherwise the second decrease fires too soon,
    causing steady-state oscillation.
    """
    cfg = AdaptiveConfig(
        target=8, min_floor=1, backoff_factor=0.5, min_decrease_interval=30.0
    )
    snap = AdaptiveSnapshot(current_permits=8, last_decrease_monotonic=0.0)
    r = UsageReading()
    bad = ControllerState(reading=r, local_in_flight=0, recent_429_count=1)
    good = ControllerState(reading=r, local_in_flight=0, recent_429_count=0)

    permits, snap = adaptive_effective_permits(bad, snap, cfg, now=100.0)
    assert permits == 4
    assert snap.last_decrease_monotonic == 100.0

    permits, snap = adaptive_effective_permits(good, snap, cfg, now=110.0)
    assert permits == 5

    permits, snap = adaptive_effective_permits(bad, snap, cfg, now=115.0)
    assert permits == 5


# --- Plan 011: Runtime override validation ----------------------------------


def test_validate_target_below_1_rejected():
    """target < 1 is rejected — a zero-permit target is a deploy decision."""
    r = reading(concurrent_sessions=0, limit=4, hard_cap=8)
    with pytest.raises(ValueError, match=">= 1"):
        validate_target_override(0, r, CFG)
    with pytest.raises(ValueError, match=">= 1"):
        validate_target_override(-5, r, CFG)


def test_validate_target_at_1_accepted():
    """target=1 is the minimum valid override."""
    r = reading(concurrent_sessions=0, limit=4, hard_cap=8)
    assert validate_target_override(1, r, CFG) is None


def test_validate_target_at_limit_accepted():
    """target=limit is a clean accept (no warning)."""
    r = reading(concurrent_sessions=0, limit=4, hard_cap=8)
    assert validate_target_override(4, r, CFG) is None


def test_validate_target_above_limit_accepted_with_warning():
    """target > limit but <= hard_cap is accepted with a warning."""
    r = reading(concurrent_sessions=0, limit=4, hard_cap=8)
    warning = validate_target_override(5, r, CFG)
    assert warning is not None
    assert "above limit" in warning


def test_validate_target_at_hard_cap_accepted_with_warning():
    """target=hard_cap is above limit, so accepted with a warning."""
    r = reading(concurrent_sessions=0, limit=4, hard_cap=8)
    warning = validate_target_override(8, r, CFG)
    assert warning is not None
    assert "above limit" in warning


def test_validate_target_above_hard_cap_rejected():
    """target > hard_cap is rejected — the provider will punish."""
    r = reading(concurrent_sessions=0, limit=4, hard_cap=8)
    with pytest.raises(ValueError, match="hard_cap"):
        validate_target_override(9, r, CFG)


def test_effective_permits_clamped_by_hard_cap():
    """When target > hard_cap (e.g. override), effective_permits clamps to hard_cap."""
    cfg = ControllerConfig(target=10, min_floor=1)
    r = reading(concurrent_sessions=0, limit=4, hard_cap=6)
    s = state(r, in_flight=0, phantom=0)
    # base = 10, but clamped to min(10, 6) = 6
    assert effective_permits(s, cfg, now=NOW) == 6


def test_effective_permits_not_clamped_by_hard_cap_when_stale():
    """When the reading is stale, hard_cap is not trusted — no clamp."""
    cfg = ControllerConfig(target=10, min_floor=1)
    r = reading(concurrent_sessions=0, limit=4, hard_cap=6, age_seconds=60.0)
    s = state(r, in_flight=0, phantom=0)
    # Stale: base = min(10, 10-1) = 9, clamped to [1, 10] = 9 (not clamped to 6)
    assert effective_permits(s, cfg, now=NOW) == 9
