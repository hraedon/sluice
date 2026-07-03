"""Admin route handlers — health, readiness, status, metrics, history, dashboard, static.

Extracted from proxy.py for navigability.  These are the non-proxy HTTP endpoints
served by the ASGI app.  The proxy delegates to these functions for admin routes
and falls through to the proxy path for everything else.

All functions are stateless — they receive ``reconcile``, ``guard``, and
``admin_token`` as arguments from the caller (ProxyApp).  The only module-level
state is the dashboard HTML, loaded once from ``static/dashboard.html`` at import
time (same lifecycle as the previous inline string).
"""

from __future__ import annotations

import base64
import hmac
import json
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TYPE_CHECKING

from sluice import __version__
from sluice.status import snapshot as status_snapshot
from sluice.status import to_prometheus

if TYPE_CHECKING:
    from sluice.reconcile import ReconciliationLoop
    from sluice.singleton import SingletonGuard

log = logging.getLogger("sluice.admin")

# ASGI callable types (mirrored from proxy.py to avoid a circular import).
Scope = dict[str, Any]
Send = Callable[[dict[str, Any]], Awaitable[None]]

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_STATIC_CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".woff2": "font/woff2",
    ".txt": "text/plain; charset=utf-8",
    ".html": "text/html; charset=utf-8",
}

_DASHBOARD_HTML = (_STATIC_DIR / "dashboard.html").read_text(encoding="utf-8")


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


def check_admin_auth(scope: Scope, admin_token: str | None) -> bool:
    """Return True if the request is authorized for admin routes.

    Accepts either a Bearer token (for API clients like ``sluice status``)
    or HTTP Basic auth (for browser access to the dashboard, where the
    password is the admin token — username is ignored).
    """
    if not admin_token:
        return True
    for k, v in scope.get("headers", []):
        if k == b"authorization" and is_admin_auth_value(v, admin_token):
            return True
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
