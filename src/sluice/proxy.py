"""Async reverse-proxy shell — streaming passthrough with concurrency gating.

Forwards both ``/v1/messages`` and ``/v1/chat/completions`` (and any other path,
transparently) to the configured upstream.  Acquires a permit before forwarding;
releases on completion **or** downstream disconnect.  On disconnect, exits the
upstream streaming context promptly so the upstream sees a terminated request,
not an abandoned one — phantom prevention.

True streaming: request and response bytes are forwarded as they arrive, never
buffered into a full body.  Auth headers pass through unchanged; sluice holds no
key of its own beyond the usage poller's.
"""

from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
import os
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx

from sluice import __version__
from sluice.gate import PermitGate
from sluice.reconcile import ReconciliationLoop
from sluice.singleton import SingletonGuard
from sluice.status import snapshot as status_snapshot
from sluice.status import to_prometheus

log = logging.getLogger("sluice.proxy")

# ASGI callable types.
Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]

# RFC 7230 hop-by-hop headers — never forwarded in either direction.
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)

# Sluice-internal control headers — consumed and stripped before forwarding.
# These are QoS / routing metadata sluice uses; they must never reach the upstream
# so the request hashes identically to a direct client (cache-transparency, AGENTS.md #7).
_CONTROL_HEADERS = frozenset(
    {
        "x-sluice-client-label",  # QoS client label (Plan 005)
        "x-sluice-qos",           # future QoS class
    }
)

# All headers stripped from the request before forwarding upstream.
_STRIP_REQUEST = _HOP_BY_HOP | _CONTROL_HEADERS | frozenset({"host"})

_QUEUE_TIMEOUT_DEFAULT = 30.0
_RETRY_AFTER_DEFAULT = 5
_RETRY_ACQUIRE_INTERVAL = 10.0
_RESERVED_LABEL = "interactive"

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_STATIC_CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".woff2": "font/woff2",
    ".txt": "text/plain; charset=utf-8",
}


def _is_concurrency_429(retry_after: str | None) -> bool:
    """Classify a 429 as concurrency (trip breaker) or rate-limit (skip).

    Returns True when the 429 should be recorded in the breaker.  Per
    AGENTS.md rule 1 (fail safe), any unparseable or ambiguous value is
    treated as a concurrency 429 — tightening the gate rather than
    assuming a rate-limit window.

    - ``None`` (no header): concurrency rejection → True
    - ``"0"`` / ``"00"`` / ``" 0 "``: "retry immediately" → True
    - ``"60"``: rate-limit window → False
    - ``""`` / ``"abc"`` / ``"-1"``: unparseable → True (fail safe)
    """
    if retry_after is None:
        return True
    try:
        return int(retry_after.strip()) <= 0
    except (ValueError, TypeError):
        return True


class ProxyApp:
    """ASGI reverse proxy with concurrency gating."""

    def __init__(
        self,
        *,
        upstream_base_url: str,
        gate: PermitGate,
        reconcile: ReconciliationLoop,
        queue_timeout: float = _QUEUE_TIMEOUT_DEFAULT,
        upstream_client: httpx.AsyncClient | None = None,
        guard: SingletonGuard | None = None,
        admin_token: str | None = None,
        retry_interval: float = _RETRY_ACQUIRE_INTERVAL,
        reserved_labels: set[str] | None = None,
    ) -> None:
        self._upstream = upstream_base_url.rstrip("/")
        self._gate = gate
        self._reconcile = reconcile
        self._queue_timeout = queue_timeout
        self._client = upstream_client or httpx.AsyncClient(timeout=None)
        self._owns_client = upstream_client is None
        self._guard = guard
        self._admin_token = admin_token
        self._guard_acquired = False
        self._retry_interval = retry_interval
        self._build_sha = os.environ.get("SLUICE_BUILD_SHA") or None
        self._acquire_retry_task: asyncio.Task[None] | None = None
        self._reserved_labels = reserved_labels or ({_RESERVED_LABEL} if gate.reserve > 0 else set())

    def _is_admin_auth_value(self, value: bytes) -> bool:
        """Check if an Authorization header value matches sluice admin credentials.

        Used both to gate admin routes and to strip sluice-internal auth headers
        from proxied requests (browsers cache Basic auth origin-wide, so the
        dashboard login would otherwise leak to the upstream — Rule 7).
        """
        if not self._admin_token:
            return False
        # Bearer token (API clients like ``sluice status``).
        bearer_expected = f"Bearer {self._admin_token}".encode()
        if hmac.compare_digest(value, bearer_expected):
            return True
        # Basic auth (browser — password = admin token, username ignored).
        # Scheme is case-insensitive per RFC 7235.
        if value.lower().startswith(b"basic "):
            try:
                decoded = base64.b64decode(value[6:]).decode("utf-8")
                _, _, password = decoded.partition(":")
                if hmac.compare_digest(
                    password.encode("utf-8"),
                    self._admin_token.encode("utf-8"),
                ):
                    return True
            except Exception:
                pass
        return False

    def _check_admin_auth(self, scope: Scope) -> bool:
        """Return True if the request is authorized for admin routes.

        Accepts either a Bearer token (for API clients like ``sluice status``)
        or HTTP Basic auth (for browser access to the dashboard, where the
        password is the admin token — username is ignored).  Basic auth lets
        the browser cache credentials so the dashboard's JS fetch to
        /status.json succeeds after the initial 401 prompt.
        """
        if not self._admin_token:
            return True
        for k, v in scope.get("headers", []):
            if k == b"authorization" and self._is_admin_auth_value(v):
                return True
        return False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self._handle_lifespan(receive, send)
            return
        if scope["type"] != "http":
            return

        path = scope["path"]
        if path == "/healthz":
            await self._send_json(send, 200, {"status": "ok"})
            return
        if path == "/readyz":
            ready = self._reconcile.ready
            if self._guard is not None:
                ready = ready and self._guard.is_held()
            if ready:
                await self._send_json(send, 200, {"status": "ready"})
            else:
                await self._send_json(send, 503, {"status": "not ready"})
            return

        # Static assets (patina design system: tokens.css, theme.js, fonts).
        # These are served unauthenticated — they contain no secrets.
        if path.startswith("/static/"):
            await self._serve_static(path, send)
            return

        # Admin routes — token-gated when admin_token is set.  The dashboard
        # (/) is included so the browser prompts for Basic auth on first
        # visit; the cached credentials then authorize the JS fetch to
        # /status.json.  Without gating /, the dashboard's fetch would 401.
        if path in ("/", "/status.json", "/metrics", "/history.json"):
            if not self._check_admin_auth(scope):
                await self._send_text(
                    send,
                    401,
                    "Unauthorized",
                    content_type="text/plain",
                    extra_headers=[(b"www-authenticate", b'Basic realm="sluice"')],
                )
                return
            if path == "/":
                await self._send_dashboard(send)
                return
            if path == "/status.json":
                await self._send_status_json(send)
                return
            if path == "/metrics":
                await self._send_prometheus(send)
                return
            if path == "/history.json":
                await self._send_history_json(send, scope)
                return

        await self._proxy_request(scope, receive, send)

    async def _handle_lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                if self._guard is not None:
                    acquired = await self._guard.acquire()
                    if not acquired:
                        log.warning("singleton guard acquire failed — starting as non-leader, will retry")
                        self._acquire_retry_task = asyncio.create_task(self._retry_acquire())
                    else:
                        try:
                            await self._guard.start_renewer()
                            await self._reconcile.start()
                        except Exception:
                            log.warning(
                                "singleton guard start failed after acquire — releasing lease, will retry",
                                exc_info=True,
                            )
                            await self._guard.stop_renewer()
                            await self._guard.release()
                            self._acquire_retry_task = asyncio.create_task(self._retry_acquire())
                        else:
                            self._guard_acquired = True
                else:
                    await self._reconcile.start()
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                if self._acquire_retry_task is not None:
                    self._acquire_retry_task.cancel()
                    try:
                        await self._acquire_retry_task
                    except asyncio.CancelledError:
                        pass
                    self._acquire_retry_task = None
                await self._reconcile.stop()
                if self._guard is not None:
                    await self._guard.stop_renewer()
                    if self._guard_acquired:
                        await self._guard.release()
                if self._owns_client:
                    await self._client.aclose()
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _retry_acquire(self) -> None:
        """Periodically retry lease acquisition when the initial acquire failed."""
        guard = self._guard
        if guard is None:
            return
        while not self._guard_acquired:
            # Jitter (±50%) so multiple non-leader pods retrying after a leader
            # crash don't stampede the apiserver in lockstep.
            await asyncio.sleep(self._retry_interval * (0.5 + random.random()))
            try:
                acquired = await guard.acquire()
                if acquired:
                    try:
                        await guard.start_renewer()
                        await self._reconcile.start()
                    except Exception:
                        log.warning(
                            "singleton guard start failed after acquire — releasing lease, will retry",
                            exc_info=True,
                        )
                        await guard.stop_renewer()
                        await guard.release()
                        continue
                    self._guard_acquired = True
                    log.info("singleton guard acquired on retry — becoming leader")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("singleton guard retry acquire failed", exc_info=True)

    async def _proxy_request(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Non-leader fast-fail: if the singleton guard is not held, refuse admission.
        if self._guard is not None and not self._guard.is_held():
            log.info("not leader — fast-failing 503")
            await self._send_json(
                send,
                503,
                {"error": "not_leader", "reason": "not_leader", "retry_after": _RETRY_AFTER_DEFAULT},
                retry_after=_RETRY_AFTER_DEFAULT,
            )
            return

        # Not-ready fast-fail: before the first successful usage poll, the gate
        # has no truth to size against.  Refuse admission rather than allowing
        # traffic at the initial (target) capacity — fail-safe (WI-018).
        if not self._reconcile.ready:
            log.info("not ready (first poll pending) — fast-failing 503")
            await self._send_json(
                send,
                503,
                {"error": "not_ready", "reason": "not_ready", "retry_after": _RETRY_AFTER_DEFAULT},
                retry_after=_RETRY_AFTER_DEFAULT,
            )
            return

        # Fast-fail if the gate is closed for a structural reason (boxed / breaker).
        # Don't burn the queue timeout against a gate that cannot open.
        reason = self._reconcile.gate_closed_reason()
        if reason in ("boxed", "breaker"):
            retry_after = self._reconcile.retry_after_seconds()
            log.info("gate closed (%s) — fast-failing 503", reason)
            await self._send_json(
                send,
                503,
                {"error": reason, "reason": reason, "retry_after": retry_after},
                retry_after=retry_after,
            )
            return

        # Read QoS class from the sluice control header (stripped before
        # forwarding by _filter_request_headers).  If the label matches a
        # reserved class and the gate has a reserve, the request may use
        # reserved slots (Plan 005 WI-002).
        reserved = False
        if self._reserved_labels:
            for k, v in scope.get("headers", []):
                if k == b"x-sluice-client-label":
                    label = v.decode("latin-1")
                    if label in self._reserved_labels:
                        reserved = True
                    break

        acquired = await self._gate.acquire(timeout=self._queue_timeout, reserved=reserved)
        if not acquired:
            log.info("permit queue timeout — returning 503")
            await self._send_json(
                send,
                503,
                {"error": "concurrency limit reached", "reason": "saturated", "retry_after": _RETRY_AFTER_DEFAULT},
                retry_after=_RETRY_AFTER_DEFAULT,
            )
            return

        try:
            await self._forward(scope, receive, send)
        except Exception:
            log.exception("proxy forward failed")
        finally:
            await self._gate.release(reserved=reserved)

    async def _forward(self, scope: Scope, receive: Receive, send: Send) -> None:
        url = self._build_url(scope)
        headers = self._filter_request_headers(scope["headers"])
        method = scope["method"]

        disconnect = asyncio.Event()
        body_done = asyncio.Event()

        async def body_stream() -> AsyncIterator[bytes]:
            """Consume ASGI receive() directly — no intermediate queue.

            By reading from receive() inline, backpressure is applied at the
            ASGI level: if the upstream is slow to accept a chunk, we don't
            call receive() again, so the ASGI server's TCP buffer fills and
            the client slows down.  This avoids both the memory risk of an
            unbounded queue and the disconnect-detection delay of a bounded
            one (the pump could block on body_queue.put() and miss
            http.disconnect events).
            """
            while True:
                event = await receive()
                etype = event["type"]
                if etype == "http.disconnect":
                    disconnect.set()
                    body_done.set()  # WI-014: unblock disconnect_watcher
                    return
                if etype == "http.request":
                    data = event.get("body", b"")
                    if data:
                        yield data
                    if not event.get("more_body", False):
                        body_done.set()
                        return

        async def disconnect_watcher() -> None:
            """Listen for client disconnect during the response phase.

            body_stream() owns receive() while the request body is being
            uploaded; this task takes over once the body is complete so
            disconnects during response streaming are detected promptly.
            """
            await body_done.wait()
            if disconnect.is_set():  # WI-014: body_stream already saw disconnect
                return
            while True:
                event = await receive()
                if event["type"] == "http.disconnect":
                    disconnect.set()
                    return

        watcher_task = asyncio.create_task(disconnect_watcher())
        response_started = False  # WI-013: guard against double http.response.start

        try:
            stream_cm = self._client.stream(
                method, url, headers=headers, content=body_stream()
            )

            # WI-014: Race stream entry against client disconnect.  If the
            # client disconnects while we're waiting for response headers or
            # during body upload, cancel the upstream request instead of
            # letting it run to completion as a phantom.
            entry_task = asyncio.ensure_future(stream_cm.__aenter__())
            disconnect_task = asyncio.ensure_future(disconnect.wait())
            await asyncio.wait(
                [entry_task, disconnect_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if disconnect_task.done() and not entry_task.done():
                # Client disconnected — cancel entry to abort the upstream
                # request.  __aenter__ cancellation closes the connection.
                entry_task.cancel()
                try:
                    await entry_task
                except (asyncio.CancelledError, Exception):
                    pass
                finally:
                    try:
                        await stream_cm.__aexit__(None, None, None)
                    except Exception:
                        pass
                return

            # Entry completed (or raised) — cancel the disconnect wait.
            if not disconnect_task.done():
                disconnect_task.cancel()
                try:
                    await disconnect_task
                except (asyncio.CancelledError, Exception):
                    pass

            # __aenter__ may have raised httpx.RequestError — let it
            # propagate to the handler below.
            response = entry_task.result()

            try:
                # 429 and rate-limit headers must be recorded before the
                # disconnect check — the upstream signal is the same whether
                # or not the client is still connected, and dropping it would
                # prevent the breaker from tripping (WI-019 fail-open).
                #
                # Only concurrency 429s should trip the breaker (WI-019).
                # Heuristic: concurrency 429s (the kind that accumulate toward
                # the box) do not include a retry-after header; rate-limit 429s
                # (request-count window exhausted) do.  We inspect headers only
                # (never the body) to classify.
                #
                # Edge case: retry-after: 0 (or "00", " 0 ", etc.) means
                # "retry immediately" — a transient concurrency signal, not a
                # rate-limit window.  The string "0" is truthy in Python, so a
                # naive ``not header`` check would silently skip breaker
                # recording (fail-open).  _is_concurrency_429 handles this and
                # all other parse edge cases fail-safe.
                #
                # Assumption (unverified against umans API): if umans sends
                # a non-zero retry-after on concurrency 429s, the breaker will
                # silently stop tripping — a fail-open.  The reconciliation
                # loop's phantom absorption provides a backstop (sustained
                # overload shrinks the gate regardless), but if this heuristic
                # is wrong the breaker's fast trip is defeated.  Revisit when
                # a real concurrency 429 is observed live.
                if response.status_code == 429 and _is_concurrency_429(
                    response.headers.get("retry-after")
                ):
                    self._reconcile.record_429()

                # WI-004: Feed response headers to the truth source.
                # For polled truth (umans) this is a no-op; for header truth
                # (Anthropic/OpenAI) it parses the allowlisted ratelimit headers.
                # Headers only — the body is never read (inert in-path, rule 7).
                self._reconcile.record_response_headers(
                    dict(response.headers), response.status_code
                )

                # WI-014: disconnect may have occurred during body upload
                # (body_stream returned early after setting disconnect).
                if disconnect.is_set():
                    return

                try:
                    await send(
                        {
                            "type": "http.response.start",
                            "status": response.status_code,
                            "headers": self._encode_response_headers(response),
                        }
                    )
                    response_started = True
                except Exception:
                    disconnect.set()
                    return

                async for chunk in response.aiter_raw():
                    if disconnect.is_set():
                        break
                    try:
                        await send(
                            {
                                "type": "http.response.body",
                                "body": chunk,
                                "more_body": True,
                            }
                        )
                    except Exception:
                        disconnect.set()
                        break

                if not disconnect.is_set():
                    await send(
                        {"type": "http.response.body", "body": b"", "more_body": False}
                    )
                    # Success only after the full stream completed without
                    # a client disconnect — a half-open breaker probe that
                    # disconnects mid-stream must not count as success.
                    if 200 <= response.status_code < 400:
                        self._reconcile.record_success()
            finally:
                # Close the stream context to cancel the upstream request
                # (WI-014: phantom prevention).
                try:
                    await stream_cm.__aexit__(None, None, None)
                except Exception:
                    pass

        except httpx.RequestError as exc:
            # WI-013: only send 502 if we haven't started the response yet.
            # If the upstream dropped mid-stream (after http.response.start),
            # sending another http.response.start is an ASGI protocol violation.
            if not disconnect.is_set() and not response_started:
                log.warning("upstream error: %s: %s", type(exc).__name__, exc)
                try:
                    await self._send_json(send, 502, {"error": "upstream error"})
                except Exception:
                    pass
        finally:
            if not watcher_task.done():
                watcher_task.cancel()
                try:
                    await watcher_task
                except asyncio.CancelledError:
                    pass

    def _build_url(self, scope: Scope) -> str:
        path: str = scope["path"]
        qs: bytes = scope.get("query_string", b"")
        if qs:
            path += "?" + qs.decode("latin-1")
        return self._upstream + path

    def _filter_request_headers(self, scope_headers: list[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
        result: list[tuple[str, str]] = []
        for k, v in scope_headers:
            name = k.decode("latin-1").lower()
            if name in _STRIP_REQUEST:
                continue
            if name.startswith("x-sluice-"):
                continue
            # Strip sluice admin auth credentials that the browser caches
            # origin-wide after dashboard login (Basic auth realm is not
            # path-scoped).  The client's own upstream Authorization header
            # (e.g. ``Bearer sk-...``) is not affected — only values that
            # match sluice's admin token are stripped.  (Rule 7.)
            if name == "authorization" and self._is_admin_auth_value(v):
                continue
            result.append((k.decode("latin-1"), v.decode("latin-1")))
        return result

    @staticmethod
    def _encode_response_headers(response: httpx.Response) -> list[tuple[bytes, bytes]]:
        return [
            (k.encode("latin-1"), v.encode("latin-1"))
            for k, v in response.headers.items()
            if k.lower() not in _HOP_BY_HOP
        ]

    async def _send_json(
        self,
        send: Send,
        status: int,
        body: dict[str, Any],
        *,
        retry_after: int | None = None,
    ) -> None:
        payload = json.dumps(body).encode()
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(payload)).encode()),
        ]
        if retry_after is not None:
            headers.append((b"retry-after", str(retry_after).encode()))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": payload, "more_body": False})

    async def _send_status_json(self, send: Send) -> None:
        snap = status_snapshot(self._reconcile, self._guard)
        payload = snap.to_dict()
        payload["version"] = __version__
        payload["build"] = self._build_sha
        await self._send_json(send, 200, payload)

    async def _send_prometheus(self, send: Send) -> None:
        snap = status_snapshot(self._reconcile, self._guard)
        text = to_prometheus(snap)
        await self._send_text(send, 200, text, content_type="text/plain; version=0.0.4; charset=utf-8")

    async def _send_history_json(self, send: Send, scope: Scope) -> None:
        history = self._reconcile.history
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
        body = json.dumps({"entries": entries, "count": len(entries), "enabled": history is not None})
        payload = body.encode()
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(payload)).encode()),
            (b"cache-control", b"no-store"),
        ]
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": payload, "more_body": False})

    async def _send_dashboard(self, send: Send) -> None:
        await self._send_text(send, 200, _DASHBOARD_HTML, content_type="text/html; charset=utf-8")

    async def _serve_static(self, path: str, send: Send) -> None:
        """Serve a file from the vendored static directory (patina assets)."""
        rel = path[len("/static/"):]
        # Prevent path traversal: resolve and verify it's inside _STATIC_DIR
        try:
            file_path = (_STATIC_DIR / rel).resolve()
            file_path.relative_to(_STATIC_DIR)
        except (ValueError, OSError):
            await self._send_text(send, 404, "Not found")
            return
        if not file_path.is_file():
            await self._send_text(send, 404, "Not found")
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

    async def _send_text(
        self,
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


_DASHBOARD_HTML = """\
<!doctype html>
<html lang="en" data-theme-key="sluice-theme">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>sluice</title>
<script src="/static/theme.js"></script>
<link rel="stylesheet" href="/static/css/tokens.css">
<style>
body{font-family:var(--font-mono);background:var(--bg);color:var(--text);margin:0;padding:var(--space-4);font-size:var(--fs-sm);line-height:1.5}
.header{display:flex;justify-content:space-between;align-items:center;margin:0 0 var(--space-4)}
h1{font-size:var(--fs-base);font-weight:500;margin:0;color:var(--accent)}
.controls{display:flex;gap:var(--space-2);align-items:center}
.controls button{background:var(--panel-2);border:1px solid var(--border-2);color:var(--text-2);
padding:var(--space-1) var(--space-3);border-radius:var(--radius-sm);cursor:pointer;
font-family:var(--font-mono);font-size:var(--fs-xs)}
.controls button:hover{border-color:var(--accent);color:var(--accent)}
#theme-toggle{background:none;border:none;color:var(--text-3);cursor:pointer;padding:var(--space-1);font-size:var(--fs-md)}
#theme-toggle:hover{color:var(--accent)}
#theme-icon-dark{display:none}
:root[data-theme="dark"] #theme-icon-dark{display:none}
:root[data-theme="dark"] #theme-icon-light{display:block}
:root[data-theme="light"] #theme-icon-dark{display:block}
:root[data-theme="light"] #theme-icon-light{display:none}
.row{display:flex;gap:var(--space-4);flex-wrap:wrap;margin-bottom:var(--space-4)}
.card{background:var(--panel);border:1px solid var(--border);border-radius:var(--radius);padding:var(--space-4);flex:1;min-width:260px}
.card h2{font-size:var(--fs-xs);font-weight:400;color:var(--text-3);margin:0 0 var(--space-3);text-transform:uppercase;letter-spacing:.05em}
table{width:100%;border-collapse:collapse;font-size:var(--fs-xs)}
td,th{padding:var(--space-1) var(--space-2);text-align:left;border-bottom:1px solid var(--border)}
th{color:var(--text-3);font-weight:400}
td{color:var(--text);font-variant-numeric:tabular-nums}
.banner{padding:var(--space-3) var(--space-4);border-radius:var(--radius-sm);margin-bottom:var(--space-4);font-size:var(--fs-sm);display:none}
.banner.boxed{display:block;background:var(--crit-soft);color:var(--crit);border:1px solid var(--crit)}
.banner.breaker{display:block;background:var(--warn-soft);color:var(--warn);border:1px solid var(--warn)}
#countdown{font-weight:600}
.gauge{position:relative;height:24px;background:var(--inset);border-radius:var(--radius-sm);overflow:hidden;border:1px solid var(--border)}
.gz{position:absolute;top:0;height:100%}
.gz-n{background:var(--info-soft)}.gz-l{background:var(--warn-soft)}
.gm{position:absolute;top:0;height:100%;width:2px;transition:left .3s}
.gm-o{background:var(--accent)}.gm-l{background:var(--warn)}
.gt{position:absolute;top:-1px;height:calc(100% + 2px);border-left:1px dashed var(--accent)}
.gl{display:flex;justify-content:space-between;font-size:var(--fs-xs);color:var(--text-3);margin-top:var(--space-1)}
.gl .gl-limit{color:var(--accent)}
.gl .gl-hardcap{color:var(--crit)}
.spark{width:100%;height:60px;display:block}
.sleg{display:flex;gap:var(--space-4);font-size:var(--fs-xs);color:var(--text-3);margin-top:var(--space-1)}
.sleg span{display:flex;align-items:center;gap:var(--space-1)}
.dot{width:8px;height:8px;border-radius:50%;display:inline-block}
.spark-obs{stroke:var(--accent)}.spark-loc{stroke:var(--warn)}.spark-ph{stroke:var(--text-3)}
.spark-grid{stroke:var(--border);stroke-dasharray:2,2}
.ranges{float:right;display:inline-flex;gap:var(--space-1)}
.rbtn{background:none;border:1px solid var(--border-2);color:var(--text-3);
font-family:var(--font-mono);font-size:var(--fs-xs);padding:0 var(--space-2);
border-radius:var(--radius-sm);cursor:pointer;text-transform:none;letter-spacing:0}
.rbtn:hover,.rbtn.active{border-color:var(--accent);color:var(--accent)}
.ribbon{width:100%;height:4px;display:block;margin-top:2px}
.qspark{width:100%;height:28px;display:block;margin-top:var(--space-2)}
.spark-qd{stroke:var(--info,var(--accent))}
.qfill{fill:var(--info-soft,none)}
.tick-qt{stroke:var(--warn)}
.tick-429{stroke:var(--crit)}
</style>
</head>
<body>
<div class="header">
  <h1>sluice <span id="build" style="color:var(--text-3);font-size:.55em;font-weight:normal"></span></h1>
  <div class="controls">
    <button id="btn-pause" onclick="togglePause()">pause</button>
    <button onclick="refresh()">refresh</button>
    <button id="theme-toggle" title="toggle theme">
      <span id="theme-icon-dark">\u2600</span>
      <span id="theme-icon-light">\u263d</span>
    </button>
  </div>
</div>
<div id="banner-boxed" class="banner boxed">ACCOUNT BOXED \u2014 retry after <span id="countdown">?</span>s</div>
<div id="banner-breaker" class="banner breaker">CIRCUIT BREAKER OPEN \u2014 backing off</div>
<div class="row">
  <div class="card">
    <h2>Concurrency Gauge</h2>
    <div class="gauge" id="gauge"></div>
    <div class="gl" id="gauge-labels"></div>
  </div>
</div>
<div class="row">
  <div class="card">
    <h2>Sparkline <span id="spark-info" style="color:var(--text-3)"></span>
      <span class="ranges">
        <button class="rbtn active" id="r-5m" onclick="setRange('5m')">5m</button>
        <button class="rbtn" id="r-1h" onclick="setRange('1h')">1h</button>
        <button class="rbtn" id="r-4h" onclick="setRange('4h')">4h</button>
      </span></h2>
    <svg class="spark" id="spark" viewBox="0 0 200 60" preserveAspectRatio="none"></svg>
    <svg class="ribbon" id="ribbon" viewBox="0 0 200 4" preserveAspectRatio="none"></svg>
    <svg class="qspark" id="qspark" viewBox="0 0 200 28" preserveAspectRatio="none"></svg>
    <div class="sleg">
      <span><span class="dot" style="background:var(--accent)"></span> observed</span>
      <span><span class="dot" style="background:var(--warn)"></span> local</span>
      <span><span class="dot" style="background:var(--text-3)"></span> phantom</span>
      <span><span class="dot" style="background:var(--info,var(--accent))"></span> queue</span>
      <span><span class="dot" style="background:var(--warn)"></span> timeout</span>
      <span><span class="dot" style="background:var(--crit)"></span> 429</span>
    </div>
  </div>
  <div class="card">
    <h2>Reading</h2>
    <table id="stats"></table>
  </div>
</div>
<div class="row">
  <div class="card">
    <h2>Config</h2>
    <table id="config-table"></table>
  </div>
</div>
<script>
let paused=false,isActive=false,timer=null,fetching=false;
const HIST_MAX=60;
// Long ranges pull from /history.json (5s tick cadence): 720 ticks = 1h, 2880 = 4h.
const RANGES={'5m':{limit:HIST_MAX},'1h':{limit:720},'4h':{limit:2880}};
const BUCKETS=120,LONG_REFRESH_MS=15000;
let hist=[],viewRange='5m',longHist=[],lastLongFetch=0,lastD=null;

function esc(v){var e=document.createElement('span');e.textContent=String(v);return e.innerHTML;}

function fromHistEntry(e){
  return {obs:e.obs,loc:e.loc,ph:e.ph,qd:e.qd,band:e.band,qt:e.qt,t429:e.t429};
}

async function initHistory(){
  try{
    const r=await fetch('/history.json?limit='+HIST_MAX,{credentials:'include'});
    if(!r.ok) return;
    const d=await r.json();
    if(!d.entries||!d.entries.length) return;
    hist=d.entries.map(fromHistEntry);
  }catch(e){}
}

function setRange(rg){
  viewRange=rg;
  ['5m','1h','4h'].forEach(function(k){
    document.getElementById('r-'+k).className='rbtn'+(k===rg?' active':'');
  });
  if(rg==='5m'){renderSparks();}
  else{fetchLong(true);}
}

async function fetchLong(force){
  if(!force&&Date.now()-lastLongFetch<LONG_REFRESH_MS) return;
  lastLongFetch=Date.now();
  try{
    const r=await fetch('/history.json?limit='+RANGES[viewRange].limit,{credentials:'include'});
    if(!r.ok) return;
    const d=await r.json();
    longHist=(d.entries||[]).map(fromHistEntry);
    renderSparks();
  }catch(e){}
}

async function doPoll(){
  if(fetching) return;
  fetching=true;
  try{
    const r=await fetch('/status.json',{credentials:'include'});
    if(!r.ok){
      document.getElementById('stats').innerHTML='<tr><th>error</th><td>status '+r.status+'</td></tr>';
      return;
    }
    const d=await r.json();
    lastD=d;
    document.getElementById('build').textContent='v'+d.version+(d.build?' @ '+d.build:'');
    render(d);
    hist.push({obs:d.concurrent_sessions,loc:d.local_in_flight,ph:d.phantom_estimate,
               qd:d.queue_depth,band:d.band,qt:d.queue_timeouts,t429:d.total_429s});
    if(hist.length>HIST_MAX) hist.shift();
    if(viewRange==='5m'){renderSparks();}
    else{fetchLong(false);}
  }catch(e){
    document.getElementById('stats').innerHTML='<tr><th>error</th><td>'+esc(e.message)+'</td></tr>';
  }finally{
    fetching=false;
  }
}

function schedule(delay){
  if(timer) clearTimeout(timer);
  timer=setTimeout(poll,delay);
}

async function poll(){
  if(!paused) await doPoll();
  schedule(paused?5000:(isActive?1000:5000));
}

function refresh(){
  if(timer) clearTimeout(timer);
  doPoll();
  if(viewRange!=='5m') fetchLong(true);
  schedule(paused?5000:(isActive?1000:5000));
}

function togglePause(){
  paused=!paused;
  document.getElementById('btn-pause').textContent=paused?'resume':'pause';
  if(!paused) schedule(0);
}

function render(d){
  const limit=d.limit??4,hc=d.hard_cap??8,tgt=d.target??3;
  const obs=d.concurrent_sessions,loc=d.local_in_flight;
  // Gauge
  const g=document.getElementById('gauge');
  if(obs==null||hc==null||limit==null){
    g.innerHTML='<div style="text-align:center;color:var(--text-3);font-size:var(--fs-xs);line-height:24px">waiting for data...</div>';
    document.getElementById('gauge-labels').innerHTML='';
  }else{
    var nW=Math.min(100,Math.max(0,(limit/hc)*100));
    var lW=Math.max(0,100-nW);
    var h='<div class="gz gz-n" style="left:0;width:'+nW+'%"></div>';
    h+='<div class="gz gz-l" style="left:'+nW+'%;width:'+lW+'%"></div>';
    var tP=Math.min(100,(tgt/hc)*100);
    h+='<div class="gt" style="left:'+tP+'%" title="target='+tgt+'"></div>';
    var oP=Math.min(100,(obs/hc)*100);
    h+='<div class="gm gm-o" style="left:'+oP+'%" title="observed='+obs+'"></div>';
    if(loc!=null){
      var lP=Math.min(100,(loc/hc)*100);
      h+='<div class="gm gm-l" style="left:'+lP+'%" title="local='+loc+'"></div>';
    }
    g.innerHTML=h;
    document.getElementById('gauge-labels').innerHTML=
      '<span>0</span><span class="gl-limit">limit='+limit+'</span><span class="gl-hardcap">hard_cap='+hc+'</span>';
  }
  // Stats table
  var rows=[
    ['band',d.band],['effective_permits',d.effective_permits],
    ['concurrent_sessions',obs],['local_in_flight',loc],
    ['phantom_estimate',d.phantom_estimate],
    ['breaker',d.breaker],
    ['breaker_half_open_age',
      d.breaker_half_open_age_seconds!=null
        ? d.breaker_half_open_age_seconds+'s'
        : null],
    ['recent_429s',d.recent_429s],
    ['total_429s',d.total_429s],['queue_depth',d.queue_depth],
    ['queue_wait',d.avg_wait_seconds+'s avg / '+d.p95_wait_seconds+'s p95'],
    ['queue_timeouts',d.queue_timeouts],
    ['gate_closed',d.gate_closed_reason],['ready',d.ready],
    ['usage_age',d.usage_age+'s'+(d.stale?' (stale)':'')],
  ];
  document.getElementById('stats').innerHTML=rows
    .filter(function(r){return r[1]!=null;})
    .map(function(r){return '<tr><th>'+esc(r[0])+'</th><td>'+esc(r[1])+'</td></tr>';}).join('');
  // Config table
  var c=d.config||{};
  var crows=[
    ['target',c.target],['min_floor',c.min_floor],
    ['poll_interval',c.poll_interval!=null?c.poll_interval+'s':null],
    ['usage_fresh_ttl',c.usage_fresh_ttl!=null?c.usage_fresh_ttl+'s':null],
    ['phantom_window',c.phantom_window],
    ['breaker_threshold',c.breaker_threshold],
    ['breaker_window',c.breaker_window_seconds!=null?c.breaker_window_seconds+'s':null],
    ['breaker_cooldown',c.breaker_cooldown_seconds!=null?c.breaker_cooldown_seconds+'s':null],
  ].filter(function(r){return r[1]!=null;});
  document.getElementById('config-table').innerHTML=crows.map(function(r){return '<tr><th>'+esc(r[0])+'</th><td>'+esc(r[1])+'</td></tr>';}).join('');
  // Banners
  var bb=document.getElementById('banner-boxed');
  if(d.band==='boxed'){
    bb.style.display='block';
    var ra=d.resets_at?Math.max(0,Math.round(d.resets_at-Date.now()/1000)):'?';
    document.getElementById('countdown').textContent=ra;
  }else bb.style.display='none';
  document.getElementById('banner-breaker').style.display=
    (d.breaker==='open'||d.breaker==='half_open')?'block':'none';
  if(d.breaker==='half_open'&&d.breaker_half_open_age_seconds!=null){
    document.getElementById('banner-breaker').textContent=
      'CIRCUIT BREAKER HALF_OPEN — probing ('+d.breaker_half_open_age_seconds+'s)';
  }else{
    document.getElementById('banner-breaker').textContent=
      'CIRCUIT BREAKER OPEN — backing off';
  }
  // Active = anything moving
  isActive=(loc>0)||(d.queue_depth>0)||(d.band!=='normal')||(d.breaker!=='closed');
}

const SEV={normal:0,low:1,reject:2,boxed:3};
const SEV_COLOR={1:'var(--warn)',2:'var(--crit)',3:'var(--crit)'};

// Mark samples where the cumulative counters advanced vs the previous sample.
function withIncs(samples){
  return samples.map(function(s,i){
    var p=i>0?samples[i-1]:null;
    return {obs:s.obs,loc:s.loc,ph:s.ph,qd:s.qd||0,band:s.band||'normal',
            qtInc:!!(p&&s.qt>p.qt),t429Inc:!!(p&&s.t429>p.t429)};
  });
}

// Downsample to <=n buckets. Max for numeric series (mean would erase the
// spikes worth seeing), worst for band, any for event ticks.
function bucketize(samples,n){
  if(samples.length<=n) return samples;
  var k=Math.ceil(samples.length/n),out=[];
  for(var i=0;i<samples.length;i+=k){
    var b={obs:null,loc:0,ph:0,qd:0,band:'normal',qtInc:false,t429Inc:false};
    for(var j=i;j<Math.min(i+k,samples.length);j++){
      var s=samples[j];
      if(s.obs!=null&&(b.obs==null||s.obs>b.obs)) b.obs=s.obs;
      if(s.loc>b.loc) b.loc=s.loc;
      if(s.ph>b.ph) b.ph=s.ph;
      if(s.qd>b.qd) b.qd=s.qd;
      if((SEV[s.band]||0)>(SEV[b.band]||0)) b.band=s.band;
      b.qtInc=b.qtInc||s.qtInc;
      b.t429Inc=b.t429Inc||s.t429Inc;
    }
    out.push(b);
  }
  return out;
}

function renderSparks(){
  var d=lastD||{};
  var hc=d.hard_cap??8,limit=d.limit??4;
  var raw=viewRange==='5m'?hist:longHist;
  var samples=bucketize(withIncs(raw),BUCKETS);
  var denom=viewRange==='5m'?HIST_MAX:samples.length;
  var svg=document.getElementById('spark');
  var info=document.getElementById('spark-info');
  var valid=samples.filter(function(h){return h.obs!=null;});
  var infoBase=viewRange==='5m'
    ? valid.length+'/'+denom+' samples'
    : viewRange+' · '+raw.length+' ticks';
  info.textContent=infoBase;
  if(valid.length<2){
    svg.innerHTML='<text x="100" y="32" text-anchor="middle" fill="var(--text-3)" font-size="7" font-family="var(--font-mono)">waiting for data...</text>';
    document.getElementById('ribbon').innerHTML='';
    document.getElementById('qspark').innerHTML='';
    return;
  }
  var W=200,pad=3,span=Math.max(denom-1,valid.length-1,1);
  function xAt(i){return pad+(i/span)*(W-2*pad);}
  // -- main spark: observed / local / phantom against the limit line
  var H=60;
  var maxV=hc;
  for(var i=0;i<valid.length;i++){
    if(valid[i].obs>maxV) maxV=valid[i].obs;
    if(valid[i].loc>maxV) maxV=valid[i].loc;
  }
  if(maxV<1) maxV=1;
  function pts(key){
    return valid.map(function(h,i){
      var v=h[key]||0;
      var y=H-pad-(v/maxV)*(H-2*pad);
      return xAt(i).toFixed(1)+','+y.toFixed(1);
    }).join(' ');
  }
  var limitY=(H-pad-(limit/maxV)*(H-2*pad)).toFixed(1);
  var s='';
  s+='<line x1="'+pad+'" y1="'+limitY+'" x2="'+(W-pad)+'" y2="'+limitY+'" class="spark-grid" stroke-width="0.5"/>';
  s+='<polyline points="'+pts('obs')+'" fill="none" class="spark-obs" stroke-width="1"/>';
  s+='<polyline points="'+pts('loc')+'" fill="none" class="spark-loc" stroke-width="1"/>';
  var hasPh=false;
  for(var j=0;j<valid.length;j++){if(valid[j].ph>0){hasPh=true;break;}}
  if(hasPh) s+='<polyline points="'+pts('ph')+'" fill="none" class="spark-ph" stroke-width="0.8" stroke-dasharray="1.5,1.5"/>';
  svg.innerHTML=s;
  // -- band ribbon: one segment per non-normal sample
  var rb='';
  var segW=(W-2*pad)/Math.max(valid.length,1);
  for(var m=0;m<valid.length;m++){
    var sev=SEV[valid[m].band]||0;
    if(sev>0) rb+='<rect x="'+xAt(m).toFixed(1)+'" y="0" width="'+Math.max(segW,1).toFixed(1)+'" height="4" fill="'+SEV_COLOR[sev]+'"/>';
  }
  document.getElementById('ribbon').innerHTML=rb;
  // -- queue spark: depth area + event ticks, own scale
  var QH=28;
  var maxQ=1;
  for(var q=0;q<valid.length;q++){if(valid[q].qd>maxQ) maxQ=valid[q].qd;}
  function qy(v){return QH-pad-(v/maxQ)*(QH-2*pad);}
  var qpts=valid.map(function(h,i){return xAt(i).toFixed(1)+','+qy(h.qd||0).toFixed(1);}).join(' ');
  var first=xAt(0).toFixed(1),last=xAt(valid.length-1).toFixed(1),base=(QH-pad).toFixed(1);
  var qs='<polygon points="'+first+','+base+' '+qpts+' '+last+','+base+'" class="qfill" stroke="none"/>';
  qs+='<polyline points="'+qpts+'" fill="none" class="spark-qd" stroke-width="1"/>';
  for(var t=0;t<valid.length;t++){
    var x=xAt(t).toFixed(1);
    if(valid[t].t429Inc) qs+='<line x1="'+x+'" y1="'+pad+'" x2="'+x+'" y2="'+(QH-pad)+'" class="tick-429" stroke-width="1"/>';
    else if(valid[t].qtInc) qs+='<line x1="'+x+'" y1="'+(QH/2).toFixed(1)+'" x2="'+x+'" y2="'+(QH-pad)+'" class="tick-qt" stroke-width="1"/>';
  }
  document.getElementById('qspark').innerHTML=qs;
  info.textContent=infoBase+' · y max '+maxV+' · queue max '+maxQ;
}

initHistory().then(poll);
</script>
</body>
</html>
"""
