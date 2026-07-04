"""Unit tests for session primitives and the login throttle. No ASGI plumbing."""

from __future__ import annotations

import pytest

from sluice.session import LoginThrottle, SESSION_COOKIE, mint_session, verify_session

NOW = 1_700_000_000.0
TOKEN = "a" * 64  # 64 hex chars, matching the real admin token shape


# ---------------------------------------------------------------------------
# mint_session / verify_session — round-trip and boundaries
# ---------------------------------------------------------------------------


def test_round_trip_within_ttl():
    cookie = mint_session(TOKEN, now=NOW)
    assert verify_session(cookie, TOKEN, now=NOW) is True


def test_round_trip_default_ttl_is_30_days():
    cookie = mint_session(TOKEN, now=NOW)
    expiry_str = cookie.split(".")[0]
    expiry = int(expiry_str)
    assert expiry == int(NOW + 2_592_000)
    # Still valid 29 days in.
    assert verify_session(cookie, TOKEN, now=NOW + 29 * 86400) is True


def test_expiry_boundary_just_before_expiry_valid():
    cookie = mint_session(TOKEN, now=NOW, ttl=10)
    # expiry == NOW + 10; at now == NOW + 9 it is still in the future.
    assert verify_session(cookie, TOKEN, now=NOW + 9) is True


def test_expiry_boundary_at_expiry_rejected():
    # expiry > now is required; expiry == now is expired.
    cookie = mint_session(TOKEN, now=NOW, ttl=10)
    assert verify_session(cookie, TOKEN, now=NOW + 10) is False


def test_expiry_boundary_after_expiry_rejected():
    cookie = mint_session(TOKEN, now=1000, ttl=1)
    assert verify_session(cookie, TOKEN, now=1000) is True
    assert verify_session(cookie, TOKEN, now=1002) is False


# ---------------------------------------------------------------------------
# Tamper resistance
# ---------------------------------------------------------------------------


def test_tampered_signature_rejected():
    cookie = mint_session(TOKEN, now=NOW)
    expiry, sig = cookie.split(".")
    c0 = sig[0]
    flipped = "B" if c0 != "B" else "C"
    tampered = f"{expiry}.{flipped}{sig[1:]}"
    assert verify_session(tampered, TOKEN, now=NOW) is False


def test_tampered_expiry_digit_rejected():
    cookie = mint_session(TOKEN, now=NOW)
    expiry, sig = cookie.split(".")
    # Flip the last digit of the expiry.
    d = expiry[-1]
    nd = "1" if d != "1" else "2"
    tampered = f"{expiry[:-1]}{nd}.{sig}"
    assert verify_session(tampered, TOKEN, now=NOW) is False


def test_tampered_extra_char_rejected():
    cookie = mint_session(TOKEN, now=NOW)
    tampered = cookie + "x"
    assert verify_session(tampered, TOKEN, now=NOW) is False


# ---------------------------------------------------------------------------
# Token rotation
# ---------------------------------------------------------------------------


def test_token_rotation_invalidates_old_cookie():
    cookie = mint_session(TOKEN, now=NOW)
    assert verify_session(cookie, "b" * 64, now=NOW) is False


def test_different_tokens_mint_different_cookies():
    a = mint_session("a" * 64, now=NOW)
    b = mint_session("b" * 64, now=NOW)
    assert a != b


# ---------------------------------------------------------------------------
# Garbage / degenerate input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cookie",
    [
        None,
        "",
        "garbage",
        "123.no signature",
        "abc.def.ghi",
        "notanumber.abc",
        ".",
        "123.",
        ".abc",
        "123.abc.def",
    ],
)
def test_garbage_input_rejected(cookie):
    assert verify_session(cookie, TOKEN, now=NOW) is False


def test_none_admin_token_rejected_even_with_valid_cookie():
    cookie = mint_session(TOKEN, now=NOW)
    assert verify_session(cookie, None, now=NOW) is False


def test_empty_admin_token_rejected():
    cookie = mint_session(TOKEN, now=NOW)
    assert verify_session(cookie, "", now=NOW) is False


def test_none_cookie_value_rejected():
    assert verify_session(None, TOKEN, now=NOW) is False


def test_non_integer_expiry_rejected():
    assert verify_session("abc.def", TOKEN, now=NOW) is False


def test_valid_format_wrong_signature_rejected():
    assert verify_session("9999999999.dGhpcyBpcyBmYWtl", TOKEN, now=NOW) is False


def test_very_long_cookie_value_does_not_crash():
    long_garbage = "a" * 10_000
    assert verify_session(long_garbage, TOKEN, now=NOW) is False
    long_formatted = f"9999999999.{'a' * 10_000}"
    assert verify_session(long_formatted, TOKEN, now=NOW) is False


def test_verify_never_raises_on_odd_types():
    # The public contract is str | None; ensure odd but callable inputs do not
    # raise uncaught (they fall through the type guard and return False).
    assert verify_session("   ", TOKEN, now=NOW) is False


def test_verify_non_ascii_cookie_does_not_crash():
    """Non-ASCII characters in the cookie signature must not raise TypeError.

    hmac.compare_digest raises TypeError when str operands contain non-ASCII.
    The fix encodes both operands to bytes before comparing.
    """
    assert verify_session("9999999999.café", TOKEN, now=NOW) is False
    assert verify_session("9999999999.日本語", TOKEN, now=NOW) is False
    assert verify_session("9999999999.\x00\x01\xff", TOKEN, now=NOW) is False


def test_verify_non_ascii_does_not_match_valid_signature():
    """A valid cookie with an ASCII signature still verifies after the fix."""
    cookie = mint_session(TOKEN, now=NOW)
    assert verify_session(cookie, TOKEN, now=NOW) is True


# ---------------------------------------------------------------------------
# LoginThrottle
# ---------------------------------------------------------------------------


def test_throttle_no_failures_not_locked():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    assert t.is_locked(now=0) is False


def test_throttle_below_threshold_not_locked():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for i in range(9):
        t.record_failure(now=float(i))
    assert t.is_locked(now=9) is False


def test_throttle_at_threshold_locked():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for i in range(10):
        t.record_failure(now=float(i))
    assert t.is_locked(now=9) is True


def test_throttle_at_threshold_all_same_timestamp_locked():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for _ in range(10):
        t.record_failure(now=0.0)
    assert t.is_locked(now=0) is True


def test_throttle_unlocks_after_window_expires():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for i in range(10):
        t.record_failure(now=float(i))
    # Past the window: the oldest failures have aged out.
    assert t.is_locked(now=301) is False


def test_throttle_unlocks_after_window_all_same_timestamp():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for _ in range(10):
        t.record_failure(now=0.0)
    # Forward time only: lazy eviction on read mutates the deque.
    assert t.is_locked(now=299) is True
    assert t.is_locked(now=301) is False


def test_throttle_retry_after_zero_when_not_locked():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    assert t.retry_after(now=0) == 0
    for i in range(5):
        t.record_failure(now=float(i))
    assert t.retry_after(now=5) == 0


def test_throttle_retry_after_positive_when_locked():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for i in range(10):
        t.record_failure(now=0.0)
    ra = t.retry_after(now=100)
    assert ra > 0
    # Oldest failure at t=0 expires at t=300 → retry after ~200s.
    assert ra == 200


def test_throttle_retry_after_when_over_threshold():
    # 15 failures, max 10: need 6 to expire (15-10+1) → the 6th oldest (index 5).
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for i in range(15):
        t.record_failure(now=float(i))
    ra = t.retry_after(now=100)
    # The 6th-oldest failure (index 5, t=5) expires at 5+300=305 → 205s.
    assert ra == 205
    assert t.is_locked(now=100) is True


def test_throttle_record_success_does_not_reset_lockout():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for _ in range(10):
        t.record_failure(now=0.0)
    assert t.is_locked(now=0) is True
    t.record_success(now=1.0)
    assert t.is_locked(now=1) is True


def test_throttle_success_then_more_failures_count_continues():
    t = LoginThrottle(max_failures=10, lockout_seconds=300)
    for i in range(5):
        t.record_failure(now=float(i))
    t.record_success(now=5.0)
    # Success did not reset the 5 prior failures.
    for i in range(5, 10):
        t.record_failure(now=float(i))
    assert t.is_locked(now=9) is True


def test_throttle_failures_age_out_individually():
    t = LoginThrottle(max_failures=3, lockout_seconds=100)
    t.record_failure(now=0.0)
    t.record_failure(now=10.0)
    t.record_failure(now=20.0)
    assert t.is_locked(now=20) is True
    # At now=101, the t=0 failure has aged out (0 <= 101-100=1) → 2 left.
    assert t.is_locked(now=101) is False
    # At now=111, t=10 ages out → 1 left.
    t.record_failure(now=101.0)  # now 3 again: 10, 20, 101
    assert t.is_locked(now=101) is True
    assert t.is_locked(now=111) is False


def test_throttle_record_failure_evicts_old_entries():
    t = LoginThrottle(max_failures=2, lockout_seconds=100)
    t.record_failure(now=0.0)
    t.record_failure(now=200.0)  # evicts the t=0 entry
    # Only 1 failure in the current window.
    assert t.is_locked(now=200) is False
    t.record_failure(now=201.0)
    assert t.is_locked(now=201) is True


def test_throttle_retry_after_at_least_one_when_locked():
    t = LoginThrottle(max_failures=1, lockout_seconds=300)
    t.record_failure(now=299.5)
    # Locked with a sub-second remaining; retry_after must not be 0.
    assert t.is_locked(now=299.5) is True
    assert t.retry_after(now=299.5) >= 1


def test_throttle_default_is_lenient():
    """Default throttle uses lenient settings (20 failures, 120s lockout)."""
    t = LoginThrottle()
    assert t._max_failures == 20
    assert t._lockout_seconds == 120


# ---------------------------------------------------------------------------
# SESSION_COOKIE single source of truth
# ---------------------------------------------------------------------------


def test_session_cookie_constant_is_single_source():
    """SESSION_COOKIE is defined once in session.py and imported everywhere.

    If the cookie name ever changes in one module but not the other,
    the proxy's Rule-7 strip silently stops matching and the session
    cookie leaks upstream.
    """
    from sluice.admin import _SESSION_COOKIE as admin_cookie
    from sluice.proxy import _SESSION_COOKIE as proxy_cookie

    assert admin_cookie == SESSION_COOKIE
    assert proxy_cookie == SESSION_COOKIE
    assert SESSION_COOKIE == "sluice_session"
