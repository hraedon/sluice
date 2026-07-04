"""Session primitives for the dashboard login page (Plan 012).

Stdlib-only session mint/verify pair and a global login throttle.  The session
cookie is signed with an HMAC keyed off a hash of the admin token — no new
secret to provision, and rotating the admin token revokes every outstanding
session without server-side state.  Verification is constant-time and fails
closed on every degenerate input (None, empty, malformed, tampered, expired,
wrong token); it never raises.

This is a shell module (not part of the pure core): it imports stdlib only and
is listed in ``SHELL_MODULES`` in the import-boundary test.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from collections import deque

_SESSION_CONTEXT = b"sluice-session-v1"
_DEFAULT_TTL = 2_592_000  # 30 days

SESSION_COOKIE = "sluice_session"


def mint_session(admin_token: str, now: float, ttl: int = _DEFAULT_TTL) -> str:
    """Mint a signed session cookie value: ``expiry.hmac_sha256(session_key, expiry)``.

    The session key is derived from the admin token so that rotating the token
    invalidates every outstanding session without requiring server-side state.
    """
    session_key = hashlib.sha256(_SESSION_CONTEXT + admin_token.encode()).digest()
    expiry = int(now + ttl)
    sig = hmac.new(session_key, str(expiry).encode(), hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")
    return f"{expiry}.{sig_b64}"


def verify_session(
    cookie_value: str | None, admin_token: str | None, now: float
) -> bool:
    """Verify a session cookie value.

    Returns ``True`` only when the cookie is well-formed, the signature matches
    under :func:`hmac.compare_digest`, and ``expiry > now``.  Every degenerate
    input (None, empty, malformed, tampered, expired, wrong token) returns
    ``False``; this function never raises.
    """
    if not cookie_value or not admin_token:
        return False
    parts = cookie_value.split(".")
    if len(parts) != 2:
        return False
    expiry_str, sig_str = parts
    try:
        expiry = int(expiry_str)
    except ValueError:
        return False
    if expiry <= now:
        return False
    session_key = hashlib.sha256(_SESSION_CONTEXT + admin_token.encode()).digest()
    expected = hmac.new(session_key, str(expiry).encode(), hashlib.sha256).digest()
    expected_b64 = base64.urlsafe_b64encode(expected).rstrip(b"=")
    # Constant-time compare so a timing oracle cannot leak signature bytes.
    # Compare bytes, not str: hmac.compare_digest raises TypeError when
    # passed str operands containing non-ASCII characters (attacker-
    # controlled cookie values can contain arbitrary Unicode).  Encoding
    # to UTF-8 is total and compare_digest on bytes never raises.
    return hmac.compare_digest(expected_b64, sig_str.encode("utf-8"))


class LoginThrottle:
    """Global in-memory login throttle (per Plan 012 §7).

    The ingress masks client IPs, so per-IP limits are theater.  This is a
    global limiter: ``max_failures`` within the lockout window locks the form
    until the window slides enough failures out to drop the count below the
    threshold.  Success resets nothing retroactive — a lockout, once tripped,
    cannot be bypassed by a later success.

    The admin token is a 64-hex-char secret that is not brute-forceable, so
    the throttle is a nuisance guard, not a security control.  The defaults
    are deliberately lenient (high threshold, short lockout) to avoid a
    trivial denial-of-login where any LAN peer can lock the form with a
    handful of bad attempts.
    """

    def __init__(
        self,
        max_failures: int = 20,
        lockout_seconds: int = 120,
    ) -> None:
        self._max_failures = max_failures
        self._lockout_seconds = lockout_seconds
        self._failures: deque[float] = deque()

    def _evict(self, now: float) -> None:
        cutoff = now - self._lockout_seconds
        while self._failures and self._failures[0] <= cutoff:
            self._failures.popleft()

    def is_locked(self, now: float) -> bool:
        self._evict(now)
        return len(self._failures) >= self._max_failures

    def record_failure(self, now: float) -> None:
        self._failures.append(now)
        self._evict(now)

    def record_success(self, now: float) -> None:
        """No-op: success does not reset a lockout (no retroactive bypass)."""

    def retry_after(self, now: float) -> int:
        """Seconds until the lockout would expire (0 when not locked).

        When locked, returns the time until enough failures age out of the
        window to drop the count below the threshold.  At least 1 when locked
        so the ``Retry-After`` header never advertises an immediate retry
        while the form is still locked.
        """
        self._evict(now)
        if self._max_failures <= 0:
            return 0
        if len(self._failures) < self._max_failures:
            return 0
        # The failure whose expiry drops the count below the threshold.
        idx = len(self._failures) - self._max_failures
        target = self._failures[idx]
        return max(1, int(target + self._lockout_seconds - now))
