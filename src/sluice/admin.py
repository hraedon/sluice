"""Admin route handlers — health, readiness, status, metrics, history, dashboard, static.

Extracted from proxy.py for navigability.  These are the non-proxy HTTP endpoints
served by the ASGI app.  The proxy delegates to these functions for admin routes
and falls through to the proxy path for everything else.

All functions are stateless — they receive ``reconcile``, ``guard``, and
``admin_token`` as arguments from the caller (ProxyApp).  The only module-level
state is the dashboard and login HTML, loaded once from ``static/`` at import
time (same lifecycle as the previous inline string).
"""

from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TYPE_CHECKING
from urllib.parse import parse_qs

from sluice import __version__
from sluice.reconcile import RETRY_AFTER_SHORT
from sluice.session import LoginThrottle, SESSION_COOKIE, mint_session, verify_session
from sluice.status import snapshot as status_snapshot
from sluice.status import to_prometheus

if TYPE_CHECKING:
    from sluice.reconcile import ReconciliationLoop
    from sluice.singleton import SingletonGuard

log = logging.getLogger("sluice.admin")

# ASGI callable types (mirrored from proxy.py to avoid a circular import).
Scope = dict[str, Any]
Send = Callable[[dict[str, Any]], Awaitable[None]]
Receive = Callable[[], Awaitable[dict[str, Any]]]

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_STATIC_CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".woff2": "font/woff2",
    ".txt": "text/plain; charset=utf-8",
    ".html": "text/html; charset=utf-8",
    ".png": "image/png",
    ".webmanifest": "application/manifest+json; charset=utf-8",
}

_DASHBOARD_HTML = (_STATIC_DIR / "dashboard.html").read_text(encoding="utf-8")
_LOGIN_HTML = (_STATIC_DIR / "login.html").read_text(encoding="utf-8")

_SESSION_COOKIE = SESSION_COOKIE
_SESSION_TTL = 2_592_000  # 30 days


# ---------------------------------------------------------------------------
# Shared ASGI response helpers (used by both admin routes and the proxy)
# ---------------------------------------------------------------------------


async def send_json(
    send: Send,
    status: int,
    body: dict[str, Any],
    *,
    retry_after: int | None = None,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    payload = json.dumps(body).encode()
    headers: list[tuple[bytes, bytes]] = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(payload)).encode()),
    ]
    if retry_after is not None:
        headers.append((b"retry-after", str(retry_after).encode()))
    if extra_headers:
        headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": payload, "more_body": False})


async def send_text(
    send: Send,
    status: int,
    body: str,
    *,
    content_type: str = "text/plain",
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    payload = body.encode()
    headers: list[tuple[bytes, bytes]] = [
        (b"content-type", content_type.encode()),
        (b"content-length", str(len(payload)).encode()),
    ]
    if extra_headers:
        headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": payload, "more_body": False})


# ---------------------------------------------------------------------------
# Admin auth
# ---------------------------------------------------------------------------


def is_admin_auth_value(value: bytes, admin_token: str | None) -> bool:
    """Check if an Authorization header value matches sluice admin credentials.

    Used both to gate admin routes and to strip sluice-internal auth headers
    from proxied requests (browsers cache Basic auth origin-wide, so the
    dashboard login would otherwise leak to the upstream — Rule 7).
    """
    if not admin_token:
        return False
    bearer_expected = f"Bearer {admin_token}".encode()
    if hmac.compare_digest(value, bearer_expected):
        return True
    if value.lower().startswith(b"basic "):
        try:
            decoded = base64.b64decode(value[6:]).decode("utf-8")
            _, _, password = decoded.partition(":")
            if hmac.compare_digest(
                password.encode("utf-8"),
                admin_token.encode("utf-8"),
            ):
                return True
        except Exception:
            pass
    return False


def _get_session_cookie(scope: Scope) -> str | None:
    """Extract the sluice_session cookie value from the Cookie header."""
    for k, v in scope.get("headers", []):
        if k == b"cookie":
            for part in v.decode("latin-1").split(";"):
                part = part.strip()
                if part.startswith(f"{_SESSION_COOKIE}="):
                    val: str = part[len(_SESSION_COOKIE) + 1:]
                    return val
    return None


def _should_set_secure(scope: Scope) -> bool:
    """Determine whether the Secure cookie attribute should be set.

    Secure is set when the scheme is https (directly or via X-Forwarded-Proto,
    which is the k8s truth behind Traefik's TLS termination) or the host is
    localhost/127.0.0.1 (secure contexts per spec).  A plain-HTTP LAN origin
    gets a non-Secure cookie (browsers silently drop Secure cookies on http).

    .. warning::

       Trusting X-Forwarded-Proto is not strictly correct: if a misconfigured
       intermediate hop injects ``X-Forwarded-Proto: https`` on an actually
       plain-HTTP origin, the Secure attribute is set and the browser drops
       the cookie → silent login loop.  This is low-likelihood in the Traefik
       deployment (Traefik sets XFP correctly and ingress is internal-only)
       but the risk is real, not "harmless" as an earlier draft claimed.
       Uvicorn's ``--forwarded-allow-ips`` should be configured to restrict
       which clients can set XFP headers.
    """
    if scope.get("scheme") == "https":
        return True
    for k, v in scope.get("headers", []):
        if k == b"x-forwarded-proto":
            first = v.decode("latin-1").split(",")[0].strip().lower()
            if first == "https":
                return True
    server = scope.get("server")
    if server and server[0] in ("127.0.0.1", "localhost", "::1"):
        return True
    return False


def _build_set_cookie(value: str, max_age: int, scope: Scope) -> bytes:
    """Build a Set-Cookie header value for the session cookie."""
    parts = [
        f"{_SESSION_COOKIE}={value}",
        "HttpOnly",
        "SameSite=Strict",
        "Path=/",
        f"Max-Age={max_age}",
    ]
    if _should_set_secure(scope):
        parts.append("Secure")
    return "; ".join(parts).encode("latin-1")


def check_admin_auth(scope: Scope, admin_token: str | None, *, now: float | None = None) -> bool:
    """Return True if the request is authorized for admin routes.

    Accepts a Bearer token (API clients), HTTP Basic auth (curl -u), or a
    signed session cookie (browser login page — Plan 012).
    """
    if not admin_token:
        return True
    if now is None:
        now = time.time()
    for k, v in scope.get("headers", []):
        if k == b"authorization" and is_admin_auth_value(v, admin_token):
            return True
    cookie_value = _get_session_cookie(scope)
    if cookie_value is not None:
        return verify_session(cookie_value, admin_token, now)
    return False


# ---------------------------------------------------------------------------
# Admin route handlers
# ---------------------------------------------------------------------------


async def handle_healthz(send: Send) -> None:
    await send_json(send, 200, {"status": "ok"})


async def handle_readyz(
    send: Send, reconcile: ReconciliationLoop, guard: SingletonGuard | None
) -> None:
    ready = reconcile.ready
    if guard is not None:
        ready = ready and guard.is_held()
    if ready:
        await send_json(send, 200, {"status": "ready"})
    else:
        await send_json(send, 503, {"status": "not ready"})


async def send_status_json(
    send: Send,
    reconcile: ReconciliationLoop,
    guard: SingletonGuard | None,
    build_sha: str | None,
) -> None:
    snap = status_snapshot(reconcile, guard)
    payload = snap.to_dict()
    payload["version"] = __version__
    payload["build"] = build_sha
    await send_json(send, 200, payload)


async def send_prometheus(
    send: Send, reconcile: ReconciliationLoop, guard: SingletonGuard | None
) -> None:
    snap = status_snapshot(reconcile, guard)
    text = to_prometheus(snap)
    await send_text(send, 200, text, content_type="text/plain; version=0.0.4; charset=utf-8")


async def send_history_json(send: Send, scope: Scope, reconcile: ReconciliationLoop) -> None:
    history = reconcile.history
    if history is not None:
        qs = scope.get("query_string", b"").decode("latin-1")
        limit: int | None = None
        if qs:
            for pair in qs.split("&"):
                k, _, v = pair.partition("=")
                if k == "limit":
                    try:
                        limit = max(0, int(v))
                    except ValueError:
                        pass
        entries = history.to_dict_list(limit=limit)
    else:
        entries = []
    body = {"entries": entries, "count": len(entries), "enabled": history is not None}
    await send_json(send, 200, body, extra_headers=[(b"cache-control", b"no-store")])


async def send_dashboard(send: Send) -> None:
    await send_text(send, 200, _DASHBOARD_HTML, content_type="text/html; charset=utf-8")


async def send_login_page(send: Send) -> None:
    await send_text(send, 200, _LOGIN_HTML, content_type="text/html; charset=utf-8")


async def handle_login_get(send: Send, admin_token: str | None) -> None:
    """GET /login — serve the login form, or 404 if no token configured."""
    if not admin_token:
        await send_text(send, 404, "Not found")
        return
    await send_login_page(send)


async def handle_login_post(
    send: Send,
    receive: Receive,
    admin_token: str | None,
    scope: Scope,
    throttle: LoginThrottle,
) -> None:
    """POST /login — verify token, set session cookie, redirect to /."""
    if not admin_token:
        await send_text(send, 404, "Not found")
        return

    now = time.time()

    if throttle.is_locked(now):
        retry = throttle.retry_after(now)
        log.warning("login throttled — retry_after=%d", retry)
        await send_json(
            send,
            429,
            {"error": "too many attempts", "retry_after": retry},
            retry_after=retry,
        )
        return

    try:
        body = await _read_body(receive)
    except ValueError:
        await send_text(send, 413, "request body too large")
        return
    except ConnectionError:
        return

    params = parse_qs(body.decode("utf-8", errors="replace"))
    token = params.get("token", [""])[0]

    if not token or not hmac.compare_digest(token.encode("utf-8"), admin_token.encode("utf-8")):
        throttle.record_failure(now)
        log.warning(
            "login failed — remote=%s",
            _extract_remote(scope),
        )
        await asyncio.sleep(0.2)
        await send_text(
            send,
            303,
            "",
            extra_headers=[(b"location", b"/login?error=1")],
        )
        return

    throttle.record_success(now)
    cookie_value = mint_session(admin_token, now, _SESSION_TTL)
    set_cookie = _build_set_cookie(cookie_value, _SESSION_TTL, scope)
    if not _should_set_secure(scope):
        log.warning(
            "session cookie set without Secure — plain-HTTP origin (remote=%s)",
            _extract_remote(scope),
        )
    await send_text(
        send,
        303,
        "",
        extra_headers=[(b"location", b"/"), (b"set-cookie", set_cookie)],
    )


async def handle_logout(
    send: Send,
    admin_token: str | None,
    scope: Scope,
) -> None:
    """POST /logout — clear session cookie and redirect to /login."""
    if not admin_token:
        await send_text(send, 303, "", extra_headers=[(b"location", b"/")])
        return
    if not check_csrf(scope, admin_token):
        await send_text(send, 403, "cross-site request blocked")
        return
    set_cookie = _build_set_cookie("", 0, scope)
    await send_text(
        send,
        303,
        "",
        extra_headers=[(b"location", b"/login"), (b"set-cookie", set_cookie)],
    )


def check_csrf(scope: Scope, admin_token: str | None) -> bool:
    """CSRF check for cookie-authenticated mutation requests.

    Bearer/Basic-authenticated mutations are exempt (Authorization headers
    cannot be set cross-site by an attacker page).  For cookie-authenticated
    requests, the Sec-Fetch-Site header must be absent (older browsers, curl)
    or equal to ``same-origin``.
    """
    for k, v in scope.get("headers", []):
        if k == b"authorization" and is_admin_auth_value(v, admin_token):
            return True
    for k, v in scope.get("headers", []):
        if k == b"sec-fetch-site":
            site: str = v.decode("latin-1").strip().lower()
            return site == "same-origin"
    return True


async def serve_static(path: str, send: Send) -> None:
    """Serve a file from the vendored static directory (patina assets)."""
    rel = path[len("/static/"):]
    try:
        file_path = (_STATIC_DIR / rel).resolve()
        file_path.relative_to(_STATIC_DIR)
    except (ValueError, OSError):
        await send_text(send, 404, "Not found")
        return
    if not file_path.is_file():
        await send_text(send, 404, "Not found")
        return
    ext = file_path.suffix.lower()
    content_type = _STATIC_CONTENT_TYPES.get(ext, "application/octet-stream")
    data = file_path.read_bytes()
    headers: list[tuple[bytes, bytes]] = [
        (b"content-type", content_type.encode()),
        (b"content-length", str(len(data)).encode()),
        (b"cache-control", b"public, max-age=3600"),
    ]
    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": data, "more_body": False})


# ---------------------------------------------------------------------------
# Config mutation endpoints (Plan 011 — runtime settings from the dashboard)
# ---------------------------------------------------------------------------


_MAX_CONFIG_BODY = 8192


async def _read_body(receive: Receive) -> bytes:
    """Read the request body from ASGI receive(), capped at _MAX_CONFIG_BODY."""
    body: bytes = b""
    while True:
        event = await receive()
        if event["type"] == "http.request":
            body += event.get("body", b"")
            if len(body) > _MAX_CONFIG_BODY:
                raise ValueError("request body too large")
            if not event.get("more_body", False):
                return body
        elif event["type"] == "http.disconnect":
            raise ConnectionError("client disconnected during body upload")


def _extract_audit_user(scope: Scope) -> str:
    """Extract the authenticated user from the request for audit logging."""
    for k, v in scope.get("headers", []):
        if k == b"authorization":
            if v.lower().startswith(b"basic "):
                try:
                    decoded = base64.b64decode(v[6:]).decode("utf-8")
                    user, _, _ = decoded.partition(":")
                    return user or "unknown"
                except Exception:
                    pass
            elif v.lower().startswith(b"bearer "):
                return "bearer"
    if _get_session_cookie(scope) is not None:
        return "session"
    return "unknown"


def _extract_remote(scope: Scope) -> str:
    """Extract the client IP from the ASGI scope."""
    client = scope.get("client")
    return client[0] if client else "unknown"


async def handle_config_post(
    send: Send,
    receive: Receive,
    reconcile: ReconciliationLoop,
    admin_token: str | None,
    scope: Scope,
    guard: SingletonGuard | None = None,
) -> None:
    """POST /admin/config — apply a runtime config override.

    Body: ``{"target": 6}`` or ``{"target": null}`` to revert.
    Requires a valid admin token; disabled (405) when no token is configured.
    Leader-only (Plan 011 §5): non-leaders return 503.
    """
    if not admin_token:
        await send_json(send, 405, {"error": "mutations disabled — set SLUICE_ADMIN_TOKEN to enable"})
        return

    if not check_admin_auth(scope, admin_token):
        await send_json(send, 403, {"error": "unauthorized"})
        return

    if not check_csrf(scope, admin_token):
        await send_json(send, 403, {"error": "cross-site request blocked"})
        return

    if guard is not None and not guard.is_held():
        await send_json(send, 503, {"error": "not_leader", "reason": "not_leader", "retry_after": RETRY_AFTER_SHORT}, retry_after=RETRY_AFTER_SHORT)
        return

    # CSRF defence: require application/json Content-Type so cross-origin
    # HTML forms (which send text/plain or application/x-www-form-urlencoded
    # and are "simple requests" that skip CORS preflight) cannot forge a POST
    # using the browser's cached Basic auth credentials.
    ct = next(
        (v.decode("latin-1") for k, v in scope.get("headers", []) if k == b"content-type"),
        "",
    )
    if not ct.lower().startswith("application/json"):
        await send_json(send, 415, {"error": "Content-Type must be application/json"})
        return

    try:
        body = await _read_body(receive)
    except ValueError:
        await send_json(send, 413, {"error": "request body too large"})
        return
    except ConnectionError:
        return
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        await send_json(send, 400, {"error": "invalid JSON body"})
        return

    if not isinstance(data, dict) or "target" not in data:
        await send_json(send, 400, {"error": "missing required field 'target'"})
        return

    value = data["target"]

    if value is None:
        previous = reconcile.target
        reconcile.clear_override("target")
        user = _extract_audit_user(scope)
        remote = _extract_remote(scope)
        log.info("config override: target %d -> reverted (user=%s, remote=%s)", previous, user, remote)
        await send_json(send, 200, {"target": reconcile.target, "overridden": False})
        return

    if not isinstance(value, int) or isinstance(value, bool):
        await send_json(send, 400, {"error": "target must be an integer"})
        return

    previous = reconcile.target
    try:
        warning = reconcile.apply_override("target", value)
    except ValueError as exc:
        await send_json(send, 400, {"error": str(exc)})
        return

    user = _extract_audit_user(scope)
    remote = _extract_remote(scope)
    log.info(
        "config override: target %d -> %d (user=%s, remote=%s)%s",
        previous,
        value,
        user,
        remote,
        f" WARNING: {warning}" if warning else "",
    )

    response: dict[str, Any] = {"target": value, "overridden": True}
    if warning:
        response["warning"] = warning
    await send_json(send, 200, response)


async def handle_config_delete(
    send: Send,
    reconcile: ReconciliationLoop,
    admin_token: str | None,
    scope: Scope,
    guard: SingletonGuard | None = None,
) -> None:
    """DELETE /admin/config/target — revert a runtime override."""
    if not admin_token:
        await send_json(send, 405, {"error": "mutations disabled — set SLUICE_ADMIN_TOKEN to enable"})
        return

    if not check_admin_auth(scope, admin_token):
        await send_json(send, 403, {"error": "unauthorized"})
        return

    if not check_csrf(scope, admin_token):
        await send_json(send, 403, {"error": "cross-site request blocked"})
        return

    if guard is not None and not guard.is_held():
        await send_json(send, 503, {"error": "not_leader", "reason": "not_leader", "retry_after": RETRY_AFTER_SHORT}, retry_after=RETRY_AFTER_SHORT)
        return

    previous = reconcile.target
    reconcile.clear_override("target")
    user = _extract_audit_user(scope)
    remote = _extract_remote(scope)
    log.info("config override: target %d -> reverted (user=%s, remote=%s)", previous, user, remote)
    await send_json(send, 200, {"target": reconcile.target, "overridden": False})
