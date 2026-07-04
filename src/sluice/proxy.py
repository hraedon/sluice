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
import math
import os
import time
from collections.abc import AsyncIterator, Mapping
from email.utils import parsedate_to_datetime

import httpx

from sluice.admin import (
    Receive,
    Send,
    Scope,
    check_admin_auth,
    handle_config_delete,
    handle_config_post,
    handle_healthz,
    handle_login_get,
    handle_login_post,
    handle_logout,
    handle_readyz,
    is_admin_auth_value,
    send_dashboard,
    send_history_json,
    send_json,
    send_login_page,
    send_prometheus,
    send_status_json,
    send_text,
    serve_static,
)
from sluice.gate import PermitGate
from sluice.lifecycle import LifecycleManager
from sluice.reconcile import ReconciliationLoop, RETRY_AFTER_SHORT
from sluice.session import LoginThrottle
from sluice.session import SESSION_COOKIE as _SESSION_COOKIE
from sluice.singleton import SingletonGuard

log = logging.getLogger("sluice.proxy")

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

# Session cookie name — imported from sluice.session (single source of truth).
# Stripped from forwarded Cookie headers (Rule 7).

_QUEUE_TIMEOUT_DEFAULT = 30.0
_RETRY_ACQUIRE_INTERVAL = 10.0
_DRAIN_TIMEOUT_DEFAULT = 25.0
_RESERVED_LABEL = "interactive"
# read=None: no response-duration cap. A finite read timeout (e.g. 300s)
# would kill any stream with a >300s inter-chunk gap, releasing the permit
# while the upstream may still count the session — self-inflicting the
# phantom the reconciler exists to absorb.  The tradeoff: a hung upstream
# (silent TCP death) holds the permit until the client disconnects or the
# drain timeout fires.  This is acceptable for a single-operator in-path
# proxy where client-side timeouts provide a fallback.  If hung upstreams
# become a problem, add an application-level liveness watchdog around the
# streaming loop rather than reintroducing a read timeout that truncates
# legitimate slow streams.
_UPSTREAM_TIMEOUT = httpx.Timeout(connect=10.0, read=None, write=30.0, pool=10.0)

# Headers that indicate a CDN/gateway layer, not the upstream application.
# If present on a 429, the 429 is classified as "gateway" — tracked but not
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
       upstream's concurrency enforcement.  Tracked separately, not fed
       to the breaker (WI-024).

    2. **concurrency** — no retry-after or retry-after <= 0.

    3. **rate_limit** — retry-after > 0.

    .. warning::

       The retry-after heuristic is **unreliable** for distinguishing
       concurrency from rate-limit.  Capture 2026-07-03
       (docs/wi-024-429-capture) proved umans sends ``retry_after=1`` on
       genuine concurrency 429s — they are classified as ``rate_limit``
       here.  Both ``concurrency`` and ``rate_limit`` are fed to the
       breaker in the proxy (the distinction is for telemetry only).
       Per AGENTS.md rule 1 (fail safe), any value that is neither a
       positive integer nor a valid HTTP-date is treated as concurrency.
    """
    # 1. CDN/gateway detection (conservative: only known CDN headers).
    for cdn_header in _CDN_HEADERS:
        if headers.get(cdn_header) is not None:
            return "gateway"
    server = (headers.get("server") or "").lower()
    for cdn_server in _CDN_SERVERS:
        if cdn_server in server:
            return "gateway"

    # 2. Retry-after heuristic: no retry-after or retry-after <= 0 means
    #    concurrency rejection (fail-safe: ambiguous values default here).
    if retry_after is None:
        return "concurrency"
    try:
        return "concurrency" if int(retry_after.strip()) <= 0 else "rate_limit"
    except (ValueError, TypeError):
        # Not a delta-seconds integer — check if it's an HTTP-date
        # (RFC 7231 §7.1.3: "Wed, 21 Oct 2015 07:28:00 GMT").  An
        # HTTP-date retry-after means "retry at this specific time" — a
        # rate-limit window, not a concurrency rejection.  Both
        # classifications feed the breaker equally; the distinction is
        # for telemetry accuracy only (WI-031).
        try:
            parsedate_to_datetime(retry_after.strip())
            return "rate_limit"
        except (ValueError, TypeError):
            log.debug("unrecognized retry-after format: %r", retry_after)
            return "concurrency"


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
        drain_timeout: float = _DRAIN_TIMEOUT_DEFAULT,
    ) -> None:
        self._upstream = upstream_base_url.rstrip("/")
        self._gate = gate
        self._reconcile = reconcile
        self._queue_timeout = queue_timeout
        self._client = upstream_client or httpx.AsyncClient(timeout=_UPSTREAM_TIMEOUT)
        self._owns_client = upstream_client is None
        self._guard = guard
        self._admin_token = admin_token
        self._retry_interval = retry_interval
        self._build_sha = os.environ.get("SLUICE_BUILD_SHA") or None
        self._login_throttle = LoginThrottle()
        self._reserved_labels = reserved_labels or ({_RESERVED_LABEL} if gate.reserve > 0 else set())
        self._lifecycle = LifecycleManager(
            guard=guard,
            reconcile=reconcile,
            client=self._client,
            owns_client=self._owns_client,
            retry_interval=retry_interval,
            gate=gate,
            drain_timeout=drain_timeout,
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self._lifecycle.handle_lifespan(receive, send)
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

        # Login / logout routes (Plan 012) — 404 when no token configured.
        if path == "/login":
            if scope["method"] == "GET":
                await handle_login_get(send, self._admin_token)
            elif scope["method"] == "POST":
                await handle_login_post(
                    send, receive, self._admin_token, scope, self._login_throttle
                )
            else:
                await send_text(send, 405, "Method not allowed")
            return
        if path == "/logout":
            if scope["method"] == "POST":
                await handle_logout(send, self._admin_token, scope)
            else:
                await send_text(send, 405, "Method not allowed")
            return

        # Config mutation endpoints (Plan 011) — own auth/disabled checks.
        if path == "/admin/config" and scope["method"] == "POST":
            await handle_config_post(
                send, receive, self._reconcile, self._admin_token, scope, self._guard
            )
            return
        if path == "/admin/config/target" and scope["method"] == "DELETE":
            await handle_config_delete(
                send, self._reconcile, self._admin_token, scope, self._guard
            )
            return

        # Admin routes — token-gated when admin_token is set.
        # GET / without auth serves the login page (200, no challenge);
        # JSON/metrics routes without auth return 401 JSON without
        # WWW-Authenticate (no Basic popup — Plan 012).
        if path in ("/", "/status.json", "/metrics", "/history.json"):
            authed = check_admin_auth(scope, self._admin_token)
            if not authed and path != "/":
                await send_json(send, 401, {"error": "unauthorized"})
                return
            if path == "/":
                if authed or not self._admin_token:
                    await send_dashboard(send)
                else:
                    await send_login_page(send)
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

    async def _proxy_request(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Draining fast-fail: during shutdown, refuse new requests so the
        # drain loop can wait for in-flight to finish and close the upstream
        # client cleanly.  uvicorn stops accepting new connections at the same
        # time, but in-flight requests that already entered the ASGI app are
        # still dispatched here.
        if self._lifecycle.is_draining:
            log.info("draining — fast-failing 503")
            await send_json(
                send,
                503,
                {"error": "draining", "reason": "draining", "retry_after": RETRY_AFTER_SHORT},
                retry_after=RETRY_AFTER_SHORT,
            )
            return

        # Non-leader fast-fail: if the singleton guard is not held, refuse admission.
        if self._guard is not None and not self._guard.is_held():
            log.info("not leader — fast-failing 503")
            await send_json(
                send,
                503,
                {"error": "not_leader", "reason": "not_leader", "retry_after": RETRY_AFTER_SHORT},
                retry_after=RETRY_AFTER_SHORT,
            )
            return

        # Not-ready fast-fail: before the first successful usage poll, the gate
        # has no truth to size against.  Refuse admission rather than allowing
        # traffic at the initial (target) capacity — fail-safe (WI-018).
        if not self._reconcile.ready:
            log.info("not ready (first poll pending) — fast-failing 503")
            # The honest value is "until the first successful poll" — track the
            # configured poll interval.  Floor at 2 s so a sub-second poll
            # interval doesn't promise an impossibly fast retry.
            not_ready_ra = max(2, math.ceil(self._reconcile.poll_interval))
            await send_json(
                send,
                503,
                {"error": "not_ready", "reason": "not_ready", "retry_after": not_ready_ra},
                retry_after=not_ready_ra,
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
            retry_after = self._reconcile.saturation_retry_after()
            await send_json(
                send,
                503,
                {"error": "concurrency limit reached", "reason": "saturated", "retry_after": retry_after},
                retry_after=retry_after,
            )
            return

        # Post-acquire drain check: the draining flag may have been set
        # while this request was blocked on gate.acquire().  Release the
        # permit immediately and fast-fail so the drain loop can proceed.
        if self._lifecycle.is_draining:
            await self._gate.release(reserved=reserved)
            log.info("draining — fast-failing 503 (post-acquire)")
            await send_json(
                send,
                503,
                {"error": "draining", "reason": "draining", "retry_after": RETRY_AFTER_SHORT},
                retry_after=RETRY_AFTER_SHORT,
            )
            return

        self._reconcile.record_request_forwarded()

        acquire_mono = time.monotonic()
        forward_failed = False
        try:
            await self._forward(scope, receive, send)
        except Exception:
            forward_failed = True
            log.exception("proxy forward failed")
        finally:
            hold_seconds = time.monotonic() - acquire_mono
            # Don't sample failed forwards — a quick connection error produces
            # a short hold that doesn't represent typical forward duration and
            # would skew avg_hold_seconds low (fail-safe: shorter waits = worse).
            await self._gate.release(
                reserved=reserved,
                hold_seconds=None if forward_failed else hold_seconds,
            )

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
                # All non-CDN 429s feed the breaker.  The retry-after heuristic
                # (_classify_429) distinguishes 'concurrency' (no/zero retry-after)
                # from 'rate_limit' (positive retry-after) for logging and separate
                # telemetry counters, but BOTH classifications feed the breaker.
                #
                # This is fail-safe: capture 2026-07-03 (docs/wi-024-429-capture)
                # proved umans sends retry_after=1 on genuine concurrency 429s.
                # The old heuristic classified these as rate_limit and skipped
                # the breaker — the breaker stayed CLOSED right into a 5-hour
                # box.  Feeding all non-CDN 429s ensures the breaker trips on
                # sustained enforcement regardless of the retry-after value.
                # The breaker threshold (5 in 5 minutes) prevents a single
                # rate-limit event from tripping; sustained rate-limiting should
                # trip it.
                #
                # Edge case: retry-after: 0 (or "00", " 0 ", etc.) means
                # "retry immediately" — a transient concurrency signal, not a
                # rate-limit window.  The string "0" is truthy in Python, so a
                # naive ``not header`` check would silently skip breaker
                # recording (fail-open).  _classify_429 handles this and
                # all other parse edge cases fail-safe.
                #
                # Monitoring hook (WI-024): classify every 429 as
                # 'concurrency' (feed breaker), 'rate_limit' (also feed breaker,
                # tracked separately), or 'gateway' (CDN/gateway rejection —
                # tracked separately, NOT fed to the breaker).  Logs the
                # classification and server header so CDN-originated 429s are
                # visible.
                if response.status_code == 429:
                    retry_after_raw = response.headers.get("retry-after")
                    classification = _classify_429(retry_after_raw, response.headers)
                    log.warning(
                        "upstream 429: retry_after=%r classification=%s server=%s",
                        retry_after_raw,
                        classification,
                        response.headers.get("server"),
                        extra={
                            "retry_after": retry_after_raw,
                            "classification": classification,
                            "upstream_server": response.headers.get("server"),
                        },
                    )
                    if classification == "concurrency":
                        self._reconcile.record_429()
                    elif classification == "rate_limit":
                        self._reconcile.record_rate_limit_429()
                    elif classification == "gateway":
                        self._reconcile.record_gateway_429()
                    else:
                        log.warning("unknown 429 classification: %s — feeding breaker", classification)
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
            if name == "cookie":
                cookie_str = v.decode("latin-1")
                parts = [p.strip() for p in cookie_str.split(";")]
                filtered = [p for p in parts if p and not p.startswith(f"{_SESSION_COOKIE}=")]
                if len(filtered) < len([p for p in parts if p]):
                    if filtered:
                        result.append((k.decode("latin-1"), "; ".join(filtered)))
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
