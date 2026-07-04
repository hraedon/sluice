"""Tests for Plan 012 — dashboard login page.

Covers: session cookie auth, /login + /logout routes, challenge removal
(no WWW-Authenticate), CSRF fetch-metadata check, rule-7 cookie stripping,
login throttle, and tokenless-mode 404s.
"""

from __future__ import annotations

import base64
import time

import httpx
import pytest

from sluice.admin import (
    _build_set_cookie,
    _get_session_cookie,
    _should_set_secure,
    check_admin_auth,
    check_csrf,
)
from sluice.proxy import _SESSION_COOKIE
from sluice.session import LoginThrottle, mint_session
from test_proxy import _asgi_client, _make_app


# ---------------------------------------------------------------------------
# Session cookie auth in check_admin_auth
# ---------------------------------------------------------------------------


def _scope_with_cookie(cookie_val: str) -> dict:
    return {
        "type": "http",
        "headers": [(b"cookie", cookie_val.encode())],
    }


def _scope_with_auth(auth_val: str) -> dict:
    return {
        "type": "http",
        "headers": [(b"authorization", auth_val.encode())],
    }


def _scope_no_auth() -> dict:
    return {"type": "http", "headers": []}


class TestCheckAdminAuthSession:
    def test_valid_session_cookie_authenticates(self):
        now = 1_000_000.0
        token = "secret-token"
        cookie = mint_session(token, now, 3600)
        scope = _scope_with_cookie(f"other=val; { _SESSION_COOKIE }={cookie}")
        assert check_admin_auth(scope, token, now=now) is True

    def test_expired_session_cookie_rejected(self):
        now = 1_000_000.0
        token = "secret-token"
        cookie = mint_session(token, now - 7200, 3600)
        scope = _scope_with_cookie(f"{_SESSION_COOKIE}={cookie}")
        assert check_admin_auth(scope, token, now=now) is False

    def test_tampered_session_cookie_rejected(self):
        now = 1_000_000.0
        token = "secret-token"
        cookie = mint_session(token, now, 3600)
        tampered = cookie[:-1] + ("a" if cookie[-1] != "a" else "b")
        scope = _scope_with_cookie(f"{_SESSION_COOKIE}={tampered}")
        assert check_admin_auth(scope, token, now=now) is False

    def test_wrong_token_session_cookie_rejected(self):
        now = 1_000_000.0
        cookie = mint_session("token-a", now, 3600)
        scope = _scope_with_cookie(f"{_SESSION_COOKIE}={cookie}")
        assert check_admin_auth(scope, "token-b", now=now) is False

    def test_no_cookie_no_auth_rejected(self):
        assert check_admin_auth(_scope_no_auth(), "secret", now=1000.0) is False

    def test_no_token_always_authed(self):
        assert check_admin_auth(_scope_no_auth(), None, now=1000.0) is True

    def test_bearer_still_works(self):
        scope = _scope_with_auth("Bearer secret")
        assert check_admin_auth(scope, "secret", now=1000.0) is True

    def test_basic_still_works(self):
        basic = "Basic " + base64.b64encode(b"user:secret").decode()
        scope = _scope_with_auth(basic)
        assert check_admin_auth(scope, "secret", now=1000.0) is True


# ---------------------------------------------------------------------------
# Cookie helpers
# ---------------------------------------------------------------------------


class TestCookieHelpers:
    def test_get_session_cookie_extracts_value(self):
        scope = _scope_with_cookie(f"a=1; {_SESSION_COOKIE}=abc123; b=2")
        assert _get_session_cookie(scope) == "abc123"

    def test_get_session_cookie_none_when_absent(self):
        scope = _scope_with_cookie("a=1; b=2")
        assert _get_session_cookie(scope) is None

    def test_get_session_cookie_none_when_no_cookie_header(self):
        assert _get_session_cookie(_scope_no_auth()) is None

    def test_should_set_secure_https(self):
        scope = {"type": "http", "scheme": "https", "headers": []}
        assert _should_set_secure(scope) is True

    def test_should_set_secure_forwarded_proto(self):
        scope = {
            "type": "http",
            "scheme": "http",
            "headers": [(b"x-forwarded-proto", b"https")],
        }
        assert _should_set_secure(scope) is True

    def test_should_set_secure_localhost(self):
        scope = {"type": "http", "scheme": "http", "headers": [], "server": ("127.0.0.1", 8800)}
        assert _should_set_secure(scope) is True

    def test_should_not_set_secure_plain_http_lan(self):
        scope = {"type": "http", "scheme": "http", "headers": [], "server": ("192.168.1.5", 8800)}
        assert _should_set_secure(scope) is False

    def test_build_set_cookie_has_attributes(self):
        scope = {"type": "http", "scheme": "https", "headers": []}
        cookie = _build_set_cookie("testval", 3600, scope)
        decoded = cookie.decode("latin-1")
        assert "sluice_session=testval" in decoded
        assert "HttpOnly" in decoded
        assert "SameSite=Strict" in decoded
        assert "Path=/" in decoded
        assert "Max-Age=3600" in decoded
        assert "Secure" in decoded

    def test_build_set_cookie_no_secure_on_http(self):
        scope = {"type": "http", "scheme": "http", "headers": [], "server": ("10.0.0.1", 8800)}
        cookie = _build_set_cookie("testval", 3600, scope)
        assert b"Secure" not in cookie


# ---------------------------------------------------------------------------
# CSRF check
# ---------------------------------------------------------------------------


class TestCheckCSRF:
    def test_bearer_exempt(self):
        scope = _scope_with_auth("Bearer secret")
        assert check_csrf(scope, "secret") is True

    def test_basic_exempt(self):
        basic = "Basic " + base64.b64encode(b"user:secret").decode()
        scope = _scope_with_auth(basic)
        assert check_csrf(scope, "secret") is True

    def test_cookie_same_origin_allowed(self):
        scope = {
            "type": "http",
            "headers": [(b"sec-fetch-site", b"same-origin")],
        }
        assert check_csrf(scope, "secret") is True

    def test_cookie_cross_site_blocked(self):
        scope = {
            "type": "http",
            "headers": [(b"sec-fetch-site", b"cross-site")],
        }
        assert check_csrf(scope, "secret") is False

    def test_cookie_none_blocked(self):
        scope = {
            "type": "http",
            "headers": [(b"sec-fetch-site", b"none")],
        }
        assert check_csrf(scope, "secret") is False

    def test_no_sec_fetch_site_allowed(self):
        scope = {"type": "http", "headers": []}
        assert check_csrf(scope, "secret") is True


# ---------------------------------------------------------------------------
# /login + /logout route tests
# ---------------------------------------------------------------------------


class TestLoginLogoutRoutes:
    @pytest.mark.asyncio
    async def test_get_login_serves_form(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.get("/login")
        assert r.status_code == 200
        assert "password" in r.text
        assert "admin token" in r.text

    @pytest.mark.asyncio
    async def test_get_login_404_when_no_token(self):
        app, _, _ = _make_app()
        app._admin_token = None
        async with _asgi_client(app) as client:
            r = await client.get("/login")
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_post_login_correct_token_sets_cookie_redirects(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post(
                "/login",
                data={"token": "secret"},
                follow_redirects=False,
            )
        assert r.status_code == 303
        assert r.headers["location"] == "/"
        set_cookie = r.headers.get("set-cookie", "")
        assert "sluice_session=" in set_cookie
        assert "HttpOnly" in set_cookie
        assert "SameSite=Strict" in set_cookie
        assert "Max-Age=2592000" in set_cookie

    @pytest.mark.asyncio
    async def test_post_login_wrong_token_no_cookie_redirect_error(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post(
                "/login",
                data={"token": "wrong"},
                follow_redirects=False,
            )
        assert r.status_code == 303
        assert r.headers["location"] == "/login?error=1"
        assert "sluice_session=" not in r.headers.get("set-cookie", "")

    @pytest.mark.asyncio
    async def test_post_login_404_when_no_token(self):
        app, _, _ = _make_app()
        app._admin_token = None
        async with _asgi_client(app) as client:
            r = await client.post("/login", data={"token": "anything"})
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_post_logout_clears_cookie_redirects(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post("/logout", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == "/login"
        set_cookie = r.headers.get("set-cookie", "")
        assert "sluice_session=" in set_cookie
        assert "Max-Age=0" in set_cookie

    @pytest.mark.asyncio
    async def test_post_logout_redirects_when_no_token(self):
        app, _, _ = _make_app()
        app._admin_token = None
        async with _asgi_client(app) as client:
            r = await client.post("/logout", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == "/"

    @pytest.mark.asyncio
    async def test_get_logout_returns_405(self):
        """GET /logout should not fall through to the proxy."""
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.get("/logout")
        assert r.status_code == 405

    @pytest.mark.asyncio
    async def test_login_redirect_with_secure_cookie_on_https(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post(
                "/login",
                data={"token": "secret"},
                follow_redirects=False,
                headers={"x-forwarded-proto": "https"},
            )
        assert r.status_code == 303
        assert "Secure" in r.headers.get("set-cookie", "")

    @pytest.mark.asyncio
    async def test_login_no_secure_on_plain_http(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post(
                "/login",
                data={"token": "secret"},
                follow_redirects=False,
            )
        assert r.status_code == 303
        assert "Secure" not in r.headers.get("set-cookie", "")


# ---------------------------------------------------------------------------
# Challenge removal — no WWW-Authenticate anywhere
# ---------------------------------------------------------------------------


class TestChallengeRemoval:
    @pytest.mark.asyncio
    async def test_unauthed_root_serves_login_page_no_challenge(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.get("/")
        assert r.status_code == 200
        assert r.headers.get("www-authenticate") is None

    @pytest.mark.asyncio
    async def test_unauthed_status_json_401_no_challenge(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.get("/status.json")
        assert r.status_code == 401
        assert r.headers.get("www-authenticate") is None

    @pytest.mark.asyncio
    async def test_unauthed_metrics_401_no_challenge(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.get("/metrics")
        assert r.status_code == 401
        assert r.headers.get("www-authenticate") is None

    @pytest.mark.asyncio
    async def test_unauthed_history_json_401_no_challenge(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.get("/history.json")
        assert r.status_code == 401
        assert r.headers.get("www-authenticate") is None

    @pytest.mark.asyncio
    async def test_bearer_still_passes(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.get(
                "/status.json",
                headers={"Authorization": "Bearer secret"},
            )
        assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_basic_still_passes(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.get(
                "/status.json",
                headers={"Authorization": "Basic " + base64.b64encode(b"u:secret").decode()},
            )
        assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_session_cookie_authed_root_serves_dashboard(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        now = time.time()
        cookie_val = mint_session("secret", now, 3600)
        async with _asgi_client(app) as client:
            r = await client.get(
                "/",
                cookies={"sluice_session": cookie_val},
            )
        assert r.status_code == 200
        assert "Concurrency Gauge" in r.text

    @pytest.mark.asyncio
    async def test_tokenless_root_serves_dashboard_directly(self):
        app, _, _ = _make_app()
        app._admin_token = None
        async with _asgi_client(app) as client:
            r = await client.get("/")
        assert r.status_code == 200
        assert "Concurrency Gauge" in r.text


# ---------------------------------------------------------------------------
# CSRF on mutation endpoints
# ---------------------------------------------------------------------------


class TestCSRFOnMutations:
    @pytest.mark.asyncio
    async def test_cookie_auth_cross_site_post_blocked(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        now = time.time()
        cookie_val = mint_session("secret", now, 3600)
        async with _asgi_client(app) as client:
            r = await client.post(
                "/admin/config",
                json={"target": 6},
                cookies={"sluice_session": cookie_val},
                headers={"sec-fetch-site": "cross-site"},
            )
        assert r.status_code == 403

    @pytest.mark.asyncio
    async def test_cookie_auth_same_origin_post_allowed(self):
        app, _, reconcile = _make_app()
        app._admin_token = "secret"
        await reconcile.tick()
        now = time.time()
        cookie_val = mint_session("secret", now, 3600)
        async with _asgi_client(app) as client:
            r = await client.post(
                "/admin/config",
                json={"target": 4},
                cookies={"sluice_session": cookie_val},
                headers={"sec-fetch-site": "same-origin"},
            )
        assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_bearer_auth_cross_site_post_allowed(self):
        app, _, reconcile = _make_app()
        app._admin_token = "secret"
        await reconcile.tick()
        async with _asgi_client(app) as client:
            r = await client.post(
                "/admin/config",
                json={"target": 4},
                headers={
                    "Authorization": "Bearer secret",
                    "sec-fetch-site": "cross-site",
                },
            )
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# Rule 7: session cookie stripped from proxied requests
# ---------------------------------------------------------------------------


class TestCookieStripping:
    @pytest.mark.asyncio
    async def test_sluice_session_cookie_stripped_from_proxied_request(self):
        received_cookies: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            received_cookies.append(request.headers.get("cookie", ""))
            from test_proxy import _resp
            return _resp(200)

        app, _, _ = _make_app(upstream_handler=handler)
        async with _asgi_client(app) as client:
            await client.post(
                "/v1/messages",
                json={"msg": "test"},
                cookies={"sluice_session": "abc.def", "other": "val"},
            )
        sent = received_cookies[0] if received_cookies else ""
        assert "sluice_session" not in sent
        assert "other=val" in sent

    @pytest.mark.asyncio
    async def test_no_sluice_cookie_passes_through_untouched(self):
        received_cookies: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            received_cookies.append(request.headers.get("cookie", ""))
            from test_proxy import _resp
            return _resp(200)

        app, _, _ = _make_app(upstream_handler=handler)
        async with _asgi_client(app) as client:
            await client.post(
                "/v1/messages",
                json={"msg": "test"},
                cookies={"a": "1", "b": "2"},
            )
        sent = received_cookies[0] if received_cookies else ""
        assert "a=1" in sent
        assert "b=2" in sent
        assert "sluice_session" not in sent

    @pytest.mark.asyncio
    async def test_only_sluice_cookie_drops_header_entirely(self):
        received_cookies: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            received_cookies.append(request.headers.get("cookie", ""))
            from test_proxy import _resp
            return _resp(200)

        app, _, _ = _make_app(upstream_handler=handler)
        async with _asgi_client(app) as client:
            await client.post(
                "/v1/messages",
                json={"msg": "test"},
                cookies={"sluice_session": "abc.def"},
            )
        sent = received_cookies[0] if received_cookies else ""
        assert "sluice_session" not in sent

    @pytest.mark.asyncio
    async def test_sluice_cookie_stripped_semicolon_no_space(self):
        """Cookie stripping works even with non-standard separators (Rule 7)."""
        received: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            received.append(request.headers.get("cookie", ""))
            from test_proxy import _resp
            return _resp(200)

        app, _, _ = _make_app(upstream_handler=handler)
        async with _asgi_client(app) as client:
            await client.post(
                "/v1/messages",
                content=b'{"msg":"test"}',
                headers={
                    "content-type": "application/json",
                    "cookie": "a=1;sluice_session=abc.def;b=2",
                },
            )
        sent = received[0] if received else ""
        assert "sluice_session" not in sent
        assert "a=1" in sent
        assert "b=2" in sent

    @pytest.mark.asyncio
    async def test_sluice_cookie_not_stripped_when_prefix_match(self):
        """sluice_session_extra should NOT be stripped (exact name match)."""
        received: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            received.append(request.headers.get("cookie", ""))
            from test_proxy import _resp
            return _resp(200)

        app, _, _ = _make_app(upstream_handler=handler)
        async with _asgi_client(app) as client:
            await client.post(
                "/v1/messages",
                content=b'{"msg":"test"}',
                headers={
                    "content-type": "application/json",
                    "cookie": "sluice_session_extra=val; a=1",
                },
            )
        sent = received[0] if received else ""
        assert "sluice_session_extra=val" in sent


# ---------------------------------------------------------------------------
# Login throttle integration
# ---------------------------------------------------------------------------


class TestLoginThrottleIntegration:
    @pytest.mark.asyncio
    async def test_throttle_locks_after_max_failures(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        app._login_throttle = LoginThrottle(max_failures=3, lockout_seconds=300)

        async with _asgi_client(app) as client:
            for _ in range(3):
                await client.post("/login", data={"token": "wrong"}, follow_redirects=False)
            r = await client.post("/login", data={"token": "wrong"}, follow_redirects=False)

        assert r.status_code == 429
        assert "retry_after" in r.json()

    @pytest.mark.asyncio
    async def test_throttle_does_not_block_correct_token_within_window(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        app._login_throttle = LoginThrottle(max_failures=5, lockout_seconds=300)

        async with _asgi_client(app) as client:
            for _ in range(3):
                await client.post("/login", data={"token": "wrong"}, follow_redirects=False)
            r = await client.post(
                "/login",
                data={"token": "secret"},
                follow_redirects=False,
            )

        assert r.status_code == 303
        assert "sluice_session=" in r.headers.get("set-cookie", "")


# ---------------------------------------------------------------------------
# Regression: non-ASCII in hmac.compare_digest
# ---------------------------------------------------------------------------


class TestNonAsciiRegression:
    @pytest.mark.asyncio
    async def test_non_ascii_login_token_does_not_500(self):
        """A non-ASCII token in the login form must not raise TypeError.

        Before the fix, hmac.compare_digest(token, admin_token) raised
        TypeError when the token contained non-ASCII characters, causing
        a 500 and bypassing the login throttle (record_failure was never
        reached).
        """
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post(
                "/login",
                data={"token": "café"},
                follow_redirects=False,
            )
        assert r.status_code == 303
        assert r.headers["location"] == "/login?error=1"

    @pytest.mark.asyncio
    async def test_non_ascii_cookie_does_not_500_on_status(self):
        """A non-ASCII session cookie must not cause a 500 on admin routes.

        Uses raw ASGI because httpx encodes header values as ASCII, but
        the real attack vector is a raw HTTP request with non-ASCII bytes
        in the Cookie header (e.g. curl -H 'Cookie: ...').
        """
        app, _, _ = _make_app()
        app._admin_token = "secret"
        sent_events: list[dict] = []
        async def receive():
            return {"type": "http.disconnect"}
        async def send(event):
            sent_events.append(event)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/status.json",
            "query_string": b"",
            "headers": [(b"cookie", "sluice_session=9999999999.café".encode("utf-8"))],
        }
        await app(scope, receive, send)
        start = [e for e in sent_events if e["type"] == "http.response.start"]
        assert start[0]["status"] == 401

    @pytest.mark.asyncio
    async def test_non_ascii_cookie_does_not_500_on_root(self):
        """A non-ASCII session cookie on / must serve login page, not 500."""
        app, _, _ = _make_app()
        app._admin_token = "secret"
        sent_events: list[dict] = []
        body_chunks: list[bytes] = []
        async def receive():
            return {"type": "http.disconnect"}
        async def send(event):
            sent_events.append(event)
            if event["type"] == "http.response.body" and event.get("body"):
                body_chunks.append(event["body"])
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [(b"cookie", "sluice_session=9999999999.日本語".encode("utf-8"))],
        }
        await app(scope, receive, send)
        start = [e for e in sent_events if e["type"] == "http.response.start"]
        assert start[0]["status"] == 200
        body = b"".join(body_chunks).decode("utf-8")
        assert "password" in body

    @pytest.mark.asyncio
    async def test_non_ascii_token_still_records_failure(self):
        """A non-ASCII login attempt must still trip the throttle."""
        app, _, _ = _make_app()
        app._admin_token = "secret"
        app._login_throttle = LoginThrottle(max_failures=3, lockout_seconds=300)
        async with _asgi_client(app) as client:
            for _ in range(3):
                await client.post(
                    "/login", data={"token": "café"}, follow_redirects=False
                )
            r = await client.post(
                "/login", data={"token": "café"}, follow_redirects=False
            )
        assert r.status_code == 429


# ---------------------------------------------------------------------------
# Regression: logout CSRF guard
# ---------------------------------------------------------------------------


class TestLogoutCSRF:
    @pytest.mark.asyncio
    async def test_logout_cross_site_blocked(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post(
                "/logout",
                follow_redirects=False,
                headers={"sec-fetch-site": "cross-site"},
            )
        assert r.status_code == 403

    @pytest.mark.asyncio
    async def test_logout_same_origin_allowed(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post(
                "/logout",
                follow_redirects=False,
                headers={"sec-fetch-site": "same-origin"},
            )
        assert r.status_code == 303
        assert r.headers["location"] == "/login"

    @pytest.mark.asyncio
    async def test_logout_no_sec_fetch_site_allowed(self):
        """curl and old browsers (no sec-fetch-site) are allowed."""
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post("/logout", follow_redirects=False)
        assert r.status_code == 303

    @pytest.mark.asyncio
    async def test_logout_bearer_exempt(self):
        app, _, _ = _make_app()
        app._admin_token = "secret"
        async with _asgi_client(app) as client:
            r = await client.post(
                "/logout",
                follow_redirects=False,
                headers={
                    "Authorization": "Bearer secret",
                    "sec-fetch-site": "cross-site",
                },
            )
        assert r.status_code == 303

    @pytest.mark.asyncio
    async def test_logout_tokenless_redirects_to_root(self):
        """Tokenless mode: /logout 303-redirects to / instead of 404."""
        app, _, _ = _make_app()
        app._admin_token = None
        async with _asgi_client(app) as client:
            r = await client.post("/logout", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == "/"


# ---------------------------------------------------------------------------
# Regression: dashboard 403 does not latch mutationsDisabled
# ---------------------------------------------------------------------------


class TestDashboard403Behavior:
    @pytest.mark.asyncio
    async def test_dashboard_html_distinguishes_403_from_405(self):
        """The dashboard JS must not set mutationsDisabled on 403.

        403 means CSRF/expired-session (should redirect to /), not
        'mutations disabled' (405).  The old code latched
        mutationsDisabled on both.
        """
        app, _, _ = _make_app()
        async with _asgi_client(app) as client:
            r = await client.get("/")
        html = r.text
        # The stepTarget handler must check 405 and 403 separately.
        assert "r.status===405" in html
        assert "r.status===403" in html
        # The old combined check must be gone.
        assert "r.status===405||r.status===403" not in html

    @pytest.mark.asyncio
    async def test_dashboard_html_has_inithistory_401_redirect(self):
        """initHistory must redirect to / on 401, like doPoll and fetchLong."""
        app, _, _ = _make_app()
        async with _asgi_client(app) as client:
            r = await client.get("/")
        html = r.text
        # Find initHistory and check it has the 401 redirect.
        init_start = html.index("async function initHistory")
        init_block = html[init_start:init_start + 500]
        assert "401" in init_block, "initHistory must handle 401 → redirect"
