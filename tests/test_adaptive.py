"""Tests for the AdaptiveRateController (AIMD) — Plan 006 WI-003 / WI-007.

Pure unit tests: no network, no clock — ``now`` is supplied.
"""

from __future__ import annotations


from sluice.control import (
    AdaptiveConfig,
    AdaptiveSnapshot,
    BreakerState,
    ControllerState,
    LimitState,
    adaptive_effective_permits,
)

NOW = 1_000_000.0
ACFG = AdaptiveConfig(target=3, min_floor=1, additive_step=1, backoff_factor=0.5,
                       fresh_ttl=15.0, low_remaining_fraction=0.2)


def _ls(**kw) -> LimitState:
    base = dict(provider="anthropic", age_seconds=0.0)
    base.update(kw)
    return LimitState(**base)


def _state(ls: LimitState, *, breaker=BreakerState.CLOSED, recent_429=0) -> ControllerState:
    return ControllerState(
        reading=ls,
        local_in_flight=0,
        breaker=breaker,
        recent_429_count=recent_429,
    )


# --- AIMD increase ------------------------------------------------------------


def test_aimd_increases_under_healthy_budget():
    """With healthy remaining budget, permits increase by additive_step."""
    snap = AdaptiveSnapshot(current_permits=1)
    ls = _ls(requests_remaining=100, tokens_remaining=50000)
    permits, new_snap = adaptive_effective_permits(_state(ls), snap, ACFG, now=NOW)
    assert permits == 2
    assert new_snap.current_permits == 2


def test_aimd_capped_at_target():
    """Permits never exceed target."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls(requests_remaining=100, tokens_remaining=50000)
    permits, new_snap = adaptive_effective_permits(_state(ls), snap, ACFG, now=NOW)
    assert permits == 3
    assert new_snap.current_permits == 3


def test_aimd_increases_step_by_step():
    """AIMD increases one step per tick, not jumps to target."""
    snap = AdaptiveSnapshot(current_permits=1)
    ls = _ls(requests_remaining=100)
    for expected in (2, 3, 3):
        permits, snap = adaptive_effective_permits(_state(ls), snap, ACFG, now=NOW)
        assert permits == expected


# --- AIMD decrease ------------------------------------------------------------


def test_aimd_backs_off_on_429():
    """A recent 429 triggers multiplicative decrease."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls(requests_remaining=100, tokens_remaining=50000)
    permits, new_snap = adaptive_effective_permits(
        _state(ls, recent_429=1), snap, ACFG, now=NOW
    )
    assert permits == 1  # 3 * 0.5 = 1.5 → int = 1
    assert new_snap.last_decrease_at == NOW


def test_aimd_backs_off_on_low_requests_remaining():
    """requests_remaining <= 0 triggers multiplicative decrease."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls(requests_remaining=0, tokens_remaining=50000)
    permits, new_snap = adaptive_effective_permits(_state(ls), snap, ACFG, now=NOW)
    assert permits == 1


def test_aimd_backs_off_on_low_tokens_remaining():
    """tokens_remaining <= 0 triggers multiplicative decrease."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls(requests_remaining=100, tokens_remaining=0)
    permits, new_snap = adaptive_effective_permits(_state(ls), snap, ACFG, now=NOW)
    assert permits == 1


def test_aimd_backs_off_on_stale_headers():
    """Stale headers trigger multiplicative decrease."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls(requests_remaining=100, tokens_remaining=50000, age_seconds=60.0)
    permits, new_snap = adaptive_effective_permits(_state(ls), snap, ACFG, now=NOW)
    assert permits == 1
    assert new_snap.last_decrease_at == NOW


def test_aimd_never_below_min_floor():
    """Multiplicative decrease never goes below min_floor."""
    snap = AdaptiveSnapshot(current_permits=1)
    ls = _ls(requests_remaining=0, tokens_remaining=0, age_seconds=99.0)
    permits, _ = adaptive_effective_permits(
        _state(ls, recent_429=5), snap, ACFG, now=NOW
    )
    assert permits == 1  # min_floor


# --- AIMD hard stops (shared breaker) -----------------------------------------


def test_aimd_boxed_closes_gate():
    """A boxed provider gets 0 permits."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls(boxed_until_epoch=NOW + 60)
    permits, new_snap = adaptive_effective_permits(_state(ls), snap, ACFG, now=NOW)
    assert permits == 0
    assert new_snap.current_permits == 3  # unchanged


def test_aimd_open_breaker_closes_gate():
    """An open breaker gives 0 permits."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls(requests_remaining=100)
    permits, new_snap = adaptive_effective_permits(
        _state(ls, breaker=BreakerState.OPEN), snap, ACFG, now=NOW
    )
    assert permits == 0


def test_aimd_half_open_admits_one_probe():
    """A half-open breaker admits at most one probe."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls(requests_remaining=100)
    permits, new_snap = adaptive_effective_permits(
        _state(ls, breaker=BreakerState.HALF_OPEN), snap, ACFG, now=NOW
    )
    assert permits == 1


def test_aimd_half_open_does_not_raise_above_one():
    """HALF_OPEN caps at 1 even if current_permits is higher."""
    snap = AdaptiveSnapshot(current_permits=5)
    ls = _ls(requests_remaining=100)
    permits, _ = adaptive_effective_permits(
        _state(ls, breaker=BreakerState.HALF_OPEN), snap, ACFG, now=NOW
    )
    assert permits == 1


# --- AIMD monotonicity (fail-safe) --------------------------------------------


def test_aimd_monotonicity_no_uncertain_input_raises_result():
    """Every uncertain input can only lower permits."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls_healthy = _ls(requests_remaining=100, tokens_remaining=50000)

    baseline, _ = adaptive_effective_permits(_state(ls_healthy), snap, ACFG, now=NOW)

    worse_cases = [
        _state(_ls(requests_remaining=0, tokens_remaining=50000)),  # low requests
        _state(_ls(requests_remaining=100, tokens_remaining=0)),  # low tokens
        _state(_ls(requests_remaining=100, age_seconds=99.0)),  # stale
        _state(ls_healthy, recent_429=1),  # recent 429
        _state(ls_healthy, breaker=BreakerState.HALF_OPEN),  # half-open
    ]
    for s in worse_cases:
        permits, _ = adaptive_effective_permits(s, snap, ACFG, now=NOW)
        assert permits <= baseline


# --- AIMD trajectory ----------------------------------------------------------


def test_aimd_trajectory_back_off_and_recover():
    """Simulate a full AIMD trajectory: increase → 429 → backoff → increase."""
    snap = AdaptiveSnapshot(current_permits=1)
    ls_healthy = _ls(requests_remaining=100, tokens_remaining=50000)

    # Tick 1: increase 1→2
    permits, snap = adaptive_effective_permits(_state(ls_healthy), snap, ACFG, now=NOW)
    assert permits == 2

    # Tick 2: increase 2→3
    permits, snap = adaptive_effective_permits(_state(ls_healthy), snap, ACFG, now=NOW)
    assert permits == 3

    # Tick 3: 429 → backoff 3→1
    permits, snap = adaptive_effective_permits(
        _state(ls_healthy, recent_429=1), snap, ACFG, now=NOW
    )
    assert permits == 1

    # Tick 4: healthy again → increase 1→2
    permits, snap = adaptive_effective_permits(_state(ls_healthy), snap, ACFG, now=NOW)
    assert permits == 2

    # Tick 5: increase 2→3
    permits, snap = adaptive_effective_permits(_state(ls_healthy), snap, ACFG, now=NOW)
    assert permits == 3


def test_aimd_default_config():
    """The default AdaptiveConfig has sensible values."""
    cfg = AdaptiveConfig()
    assert cfg.target == 3
    assert cfg.min_floor == 1
    assert cfg.additive_step == 1
    assert cfg.backoff_factor == 0.5
    assert cfg.fresh_ttl == 15.0


def test_aimd_snapshot_defaults():
    """AdaptiveSnapshot starts conservative (1 permit)."""
    snap = AdaptiveSnapshot()
    assert snap.current_permits == 1
    assert snap.last_decrease_at is None


def test_aimd_no_remaining_data_does_not_decrease():
    """When remaining fields are None (no headers), don't backoff on them."""
    snap = AdaptiveSnapshot(current_permits=3)
    ls = _ls()  # no remaining fields
    permits, _ = adaptive_effective_permits(_state(ls), snap, ACFG, now=NOW)
    # No bad signals → additive increase (capped at target)
    assert permits == 3
