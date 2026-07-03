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
import logging
import os
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Any

import httpx

from sluice.admin import (
    Send,
    Scope,
    check_admin_auth,
    handle_healthz,
    handle_readyz,
    is_admin_auth_value,
    send_dashboard,
    send_history_json,
    send_json,
    send_prometheus,
    send_status_json,
    send_text,
    serve_static,
)
from sluice.gate import PermitGate
from sluice.reconcile import ReconciliationLoop
from sluice.singleton import SingletonGuard

log = logging.getLogger("sluice.proxy")

# ASGI receive type (Send/Scope are re-exported from admin for callers that need them).
Receive = Callable[[], Awaitable[dict[str, Any]]]

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

# Headers that indicate a CDN/gateway layer, not the upstream application.
# If present on a 429, the 427 is classified as "gateway" — tracked but not
# fed to the breaker (WI-024: CDN 429s don't represent concurrency pressure).
_CDN_HEADERS = frozenset(
    {
        "cf-ray",               # Cloudflare
        "x-amz-cf-id",          # CloudFront
        "x-served-by",          # Fastly / Varnish
        "x-fastly-request-id",  # Fastly
        "x-vercel-id",          # Vercel
        "fly-request-id",       # Fly.io
    }
)
_CDN_SERVERS = frozenset({"cloudflare"})


def _classify_429(
    retry_after: str | None, headers: Mapping[str, str]
) -> str:
    """Classify a 429 as 'concurrency', 'rate_limit', or 'gateway'.

    Classification order:

    1. **gateway** — CDN/gateway headers are present (``cf-ray``, ``server:
       cloudflare``, etc.).  The 429 was rejected at the edge, not by the
       upstream's concurrency enforcement.  Tracked separately, not fed to
       the breaker (WI-024).

    2. **concurrency** — no retry-after or retry-after <= 0.  Fed to the
       breaker (fail-safe: ambiguous values default to concurrency).

    3. **rate_limit** — retry-after > 0.  Not fed to the breaker.

    Per AGENTS.md rule 1 (fail safe), any unparseable or ambiguous value
    is treated as concurrency — tightening the gate rather than assuming
    a rate-limit window.
    """
    # 1. CDN/gateway detection (conservative: only known CDN headers).
    for cdn_header in _CDN_HEADERS:
        if headers.get(cdn_header) is not None:
            return "gateway"
    server = (headers.get("server") or "").lower()
    for cdn_server in _CDN_SERVERS:
        if cdn_server in server:
            return "gateway"

    # 2. Retry-after heuristic (unchanged from _is_concurrency_429).
    if retry_after is None:
        return "concurrency"
    try:
        return "concurrency" if int(retry_after.strip()) <= 0 else "rate_limit"
    except (ValueError, TypeError):
        return "concurrency"


def _is_concurrency_429(retry_after: str | None) -> bool:
    """Backward-compatible wrapper — prefer _classify_429 for three-way."""
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

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self._handle_lifespan(receive, send)
            return
        if scope["type"] != "http":
            return

        path = scope["path"]
        if path == "/healthz":
            await handle_healthz(send)
            return
        if path == "/readyz":
            await handle_readyz(send, self._reconcile, self._guard)
            return

        # Static assets (patina design system: tokens.css, theme.js, fonts).
        # These are served unauthenticated — they contain no secrets.
        if path.startswith("/static/"):
            await serve_static(path, send)
            return

        # Admin routes — token-gated when admin_token is set.  The dashboard
        # (/) is included so the browser prompts for Basic auth on first
        # visit; the cached credentials then authorize the JS fetch to
        # /status.json.  Without gating /, the dashboard's fetch would 401.
        if path in ("/", "/status.json", "/metrics", "/history.json"):
            if not check_admin_auth(scope, self._admin_token):
                await send_text(
                    send,
                    401,
                    "Unauthorized",
                    content_type="text/plain",
                    extra_headers=[(b"www-authenticate", b'Basic realm="sluice"')],
                )
                return
            if path == "/":
                await send_dashboard(send)
                return
            if path == "/status.json":
                await send_status_json(send, self._reconcile, self._guard, self._build_sha)
                return
            if path == "/metrics":
                await send_prometheus(send, self._reconcile, self._guard)
                return
            if path == "/history.json":
                await send_history_json(send, scope, self._reconcile)
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
            await send_json(
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
            await send_json(
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
            await send_json(
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
            await send_json(
                send,
                503,
                {"error": "concurrency limit reached", "reason": "saturated", "retry_after": _RETRY_AFTER_DEFAULT},
                retry_after=_RETRY_AFTER_DEFAULT,
            )
            return

        self._reconcile.record_request_forwarded()

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
                #
                # Monitoring hook (WI-024): classify every 429 as
                # 'concurrency' (feed breaker), 'rate_limit' (skip), or
                # 'gateway' (CDN/gateway rejection — tracked separately, not
                # fed to the breaker).  Logs the classification and server
                # header so CDN-originated 429s are visible.
                if response.status_code == 429:
                    retry_after_raw = response.headers.get("retry-after")
                    classification = _classify_429(retry_after_raw, response.headers)
                    log.warning(
                        "upstream 429: retry_after=%r classification=%s server=%s",
                        retry_after_raw,
                        classification,
                        response.headers.get("server"),
                    )
                    if classification == "concurrency":
                        self._reconcile.record_429()
                    elif classification == "gateway":
                        self._reconcile.record_gateway_429()

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
                    await send_json(send, 502, {"error": "upstream error"})
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
            if name == "authorization" and is_admin_auth_value(v, self._admin_token):
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
