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
import contextlib
import ipaddress
import logging
import math
import os
import time
from collections.abc import AsyncIterator, Mapping
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from sluice.admin import (
    Receive,
    Send,
    Scope,
    cors_extra_headers,
    check_admin_auth,
    handle_config_delete,
    handle_config_post,
    handle_healthz,
    handle_login_get,
    handle_login_post,
    handle_logout,
    handle_readyz,
    handle_reload,
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
from sluice.metrics import ClientMetrics
from sluice.reconcile import ReconciliationLoop, RETRY_AFTER_SHORT
from sluice.session import LoginThrottle
from sluice.session import SESSION_COOKIE as _SESSION_COOKIE
from sluice.singleton import SingletonGuard
from sluice.trust import peer_is_trusted

log = logging.getLogger("sluice.proxy")


async def _cancel_task(task: "asyncio.Future[Any]") -> None:
    """Cancel a racing task and await it, swallowing the fallout.

    Used to tear down the read/disconnect tasks in the streaming loop
    without leaking pending tasks or surfacing ``CancelledError``.  A task
    that is already done has its result/exception retrieved so asyncio does
    not log it as never-retrieved.
    """
    if task.done():
        with contextlib.suppress(Exception, asyncio.CancelledError):
            task.result()
        return
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

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


def _parse_connection_headers(headers: list[tuple[bytes, bytes]]) -> set[str]:
    """Extract header names listed in Connection headers (RFC 7230 §6.1).

    These are hop-by-hop and must not be forwarded.  Returns a set of
    lowercased header names (without the ``connection`` header itself).
    """
    extra: set[str] = set()
    for k, v in headers:
        if k.lower() == b"connection":
            for name in v.decode("latin-1").split(","):
                name = name.strip().lower()
                if name:
                    extra.add(name)
    return extra

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
       here.  Only ``concurrency`` 429s feed the breaker; ``rate_limit``
       429s are tracked in a separate counter and wake the poll, but do
       NOT trip the breaker (capture 2026-07-07 proved they false-trip:
       36 rate_limit 429s with ``concurrent_sessions=0`` and no box caused
       30 self-inflicted outages in 24 h).  The deprioritization rung they
       signal is already handled by ``is_deprioritized`` → LOW band, and
       a shell-level safety net in ``tick()`` tightens to ``min_floor``
       when the reading is stale and rate_limit 429s are recent.
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
        trusted_proxies: frozenset[ipaddress.IPv4Network | ipaddress.IPv6Network] | None = None,
        max_request_body_bytes: int | None = None,
        upstream_idle_timeout: float | None = None,
        cors_allow_origin: str | None = None,
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
        self._trusted_proxies = trusted_proxies or frozenset()
        self._max_request_body_bytes = max_request_body_bytes
        self._upstream_idle_timeout = upstream_idle_timeout
        self._cors_allow_origin = cors_allow_origin
        self._config_path: str | None = None  # set by CLI for SIGHUP reload
        self._client_metrics = ClientMetrics()
        self._lifecycle = LifecycleManager(
            guard=guard,
            reconcile=reconcile,
            client=self._client,
            owns_client=self._owns_client,
            retry_interval=retry_interval,
            gate=gate,
            drain_timeout=drain_timeout,
        )
        self._lifecycle._app_ref = self

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

        # CORS preflight (WI-028 finding 10): answer OPTIONS on admin routes
        # with the configured allow-origin so a cross-origin dashboard (e.g.
        # embedded in a Grafana iframe) can read the response.  Only emitted
        # when --cors-allow-origin is set; otherwise OPTIONS falls through.
        if scope["method"] == "OPTIONS" and self._cors_allow_origin and path in (
            "/", "/status.json", "/metrics", "/history.json",
            "/admin/config", "/admin/config/target",
            "/admin/reload",
            "/login", "/logout",
        ):
            await send_text(
                send, 204, "",
                extra_headers=cors_extra_headers(self._cors_allow_origin, None),
            )
            return

        # Static assets (patina design system: tokens.css, theme.js, fonts).
        # These are served unauthenticated — they contain no secrets.
        if path.startswith("/static/"):
            await serve_static(path, send)
            return

        # Login / logout routes (Plan 012) — 404 when no token configured.
        if path == "/login":
            if scope["method"] == "GET":
                await handle_login_get(send, self._admin_token, self._cors_allow_origin)
            elif scope["method"] == "POST":
                await handle_login_post(
                    send, receive, self._admin_token, scope, self._login_throttle,
                    self._trusted_proxies,
                )
            else:
                await send_text(send, 405, "Method not allowed")
            return
        if path == "/logout":
            if scope["method"] == "POST":
                await handle_logout(
                    send, self._admin_token, scope, self._trusted_proxies,
                )
            else:
                await send_text(send, 405, "Method not allowed")
            return

        # Config mutation endpoints (Plan 011) — own auth/disabled checks.
        if path == "/admin/config" and scope["method"] == "POST":
            await handle_config_post(
                send, receive, self._reconcile, self._admin_token, scope, self._guard,
                self._cors_allow_origin,
            )
            return
        if path == "/admin/config/target" and scope["method"] == "DELETE":
            await handle_config_delete(
                send, self._reconcile, self._admin_token, scope, self._guard,
                self._cors_allow_origin,
            )
            return
        if path == "/admin/reload" and scope["method"] == "POST":
            await handle_reload(
                send, self, self._admin_token, scope, self._guard,
                self._cors_allow_origin,
            )
            return

        # Admin routes — token-gated when admin_token is set.
        # GET / without auth serves the login page (200, no challenge);
        # JSON/metrics routes without auth return 401 JSON without
        # WWW-Authenticate (no Basic popup — Plan 012).
        if path in ("/", "/status.json", "/metrics", "/history.json"):
            authed = check_admin_auth(scope, self._admin_token)
            if not authed and path != "/":
                await send_json(
                    send, 401, {"error": "unauthorized"},
                    extra_headers=cors_extra_headers(self._cors_allow_origin, None),
                )
                return
            if path == "/":
                if authed or not self._admin_token:
                    await send_dashboard(send, self._cors_allow_origin)
                else:
                    await send_login_page(send, self._cors_allow_origin)
                return
            if path == "/status.json":
                await send_status_json(
                    send, self._reconcile, self._guard, self._build_sha,
                    self._cors_allow_origin,
                    client_metrics=self._client_metrics.to_dict(),
                )
                return
            if path == "/metrics":
                await send_prometheus(
                    send, self._reconcile, self._guard, self._cors_allow_origin,
                    client_metrics=self._client_metrics.to_dict(),
                )
                return
            if path == "/history.json":
                await send_history_json(
                    send, scope, self._reconcile, self._cors_allow_origin,
                )
                return

        await self._proxy_request(scope, receive, send)

    @property
    def client_metrics(self) -> ClientMetrics:
        """Per-client metrics for /status.json and /metrics (WI-023)."""
        return self._client_metrics

    def reload_config(self, **kwargs: Any) -> dict[str, str]:
        """Apply runtime-safe config changes without restart.

        Only fields that can be safely changed at runtime are applied.
        Fields that require recreating resources (upstream URL, provider,
        admin token, gate internals) are silently ignored — the operator
        must restart for those.

        Returns a dict of ``{field: "old -> new"}`` for each changed field,
        suitable for logging.
        """
        changes: dict[str, str] = {}

        def _apply(field: str, attr: str, value: Any) -> None:
            old = getattr(self, attr)
            if value != old:
                changes[field] = f"{old} -> {value}"
                setattr(self, attr, value)

        if "queue_timeout" in kwargs:
            v = kwargs["queue_timeout"]
            if v is not None and v <= 0:
                log.warning("reload: ignoring invalid queue_timeout=%r (must be > 0)", v)
            else:
                _apply("queue_timeout", "_queue_timeout", v)
        if "trusted_proxies" in kwargs:
            _apply("trusted_proxies", "_trusted_proxies", kwargs["trusted_proxies"])
        if "max_request_body_bytes" in kwargs:
            v = kwargs["max_request_body_bytes"]
            if v is not None and v <= 0:
                v = None
            _apply("max_request_body_bytes", "_max_request_body_bytes", v)
        if "upstream_idle_timeout" in kwargs:
            _apply("upstream_idle_timeout", "_upstream_idle_timeout", kwargs["upstream_idle_timeout"])
        if "cors_allow_origin" in kwargs:
            _apply("cors_allow_origin", "_cors_allow_origin", kwargs["cors_allow_origin"])

        # Reconcile-level fields
        r = self._reconcile
        if "poll_interval" in kwargs:
            v = kwargs["poll_interval"]
            if v is not None and v <= 0:
                log.warning("reload: ignoring invalid poll_interval=%r (must be > 0)", v)
            else:
                old_pi = r._poll_interval
                if v != old_pi:
                    changes["poll_interval"] = f"{old_pi} -> {v}"
                    r._poll_interval = v
        if "poll_interval_idle" in kwargs:
            v = kwargs["poll_interval_idle"]
            if v is not None and v <= 0:
                v = None
            old_idle = r._poll_interval_idle_cfg
            if v != old_idle:
                changes["poll_interval_idle"] = f"{old_idle} -> {v}"
                r._poll_interval_idle_cfg = v

        return changes

    def _reload_from_config(self) -> dict[str, str]:
        """Re-read the config file and apply safe changes.

        Called by the SIGHUP handler and the ``POST /admin/reload`` endpoint.
        Returns a dict of ``{field: "old -> new"}`` for each changed field.
        Raises if no config file was specified at startup.
        """
        if not self._config_path:
            raise ValueError("no config file specified at startup")
        import tomllib
        from pathlib import Path

        p = Path(self._config_path)
        if not p.exists():
            raise FileNotFoundError(f"config file not found: {self._config_path}")
        with p.open("rb") as f:
            data = tomllib.load(f)
        serve_section = data.get("serve", data)
        if not isinstance(serve_section, dict):
            serve_section = {}

        # Build kwargs from the config file for safe-to-reload fields
        kwargs: dict[str, Any] = {}
        if "poll_interval" in serve_section:
            kwargs["poll_interval"] = float(serve_section["poll_interval"])
        if "poll_interval_idle" in serve_section:
            v = serve_section["poll_interval_idle"]
            kwargs["poll_interval_idle"] = float(v) if v else None
        if "queue_timeout" in serve_section:
            kwargs["queue_timeout"] = float(serve_section["queue_timeout"])
        if "trusted_proxies" in serve_section:
            from sluice.trust import parse_trusted_proxies
            kwargs["trusted_proxies"] = parse_trusted_proxies(serve_section["trusted_proxies"])
        if "max_request_body_bytes" in serve_section:
            v = serve_section["max_request_body_bytes"]
            kwargs["max_request_body_bytes"] = int(v) if v else None
        if "upstream_idle_timeout" in serve_section:
            v = serve_section["upstream_idle_timeout"]
            kwargs["upstream_idle_timeout"] = float(v) if v else None
        if "cors_allow_origin" in serve_section:
            kwargs["cors_allow_origin"] = serve_section["cors_allow_origin"]

        return self.reload_config(**kwargs)

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

        # Request body size cap (WI-028 finding 3).  Config mutations are
        # already bounded by _MAX_CONFIG_BODY in admin.py; the proxy path
        # streams bytes and so has no natural bound.  An explicit cap prevents
        # a single oversized upload from pinning a permit for the duration of
        # a slow body transfer.  Default off (None) preserves streaming for
        # deployments that genuinely send large bodies; set via
        # --max-request-body-bytes to enforce.
        if self._max_request_body_bytes is not None:
            declared = self._declared_content_length(scope)
            if declared is not None and declared > self._max_request_body_bytes:
                log.info(
                    "request body too large (declared %d > limit %d) — returning 413",
                    declared, self._max_request_body_bytes,
                )
                await send_json(send, 413, {"error": "request body too large"})
                return

        # Read QoS class from the sluice control header (stripped before
        # forwarding by _filter_request_headers).  If the label matches a
        # reserved class and the gate has a reserve, the request may use
        # reserved slots (Plan 005 WI-002).
        #
        # WI-028: the label is honoured only when the immediate peer is a
        # trusted proxy (or loopback, when no trusted-proxies are configured).
        # Without this check any client could spoof ``x-sluice-client-label:
        # interactive`` and consume the reserved slots.  A spoofed label from
        # a non-trusted peer is still stripped before forwarding (cache-
        # transparency) but does not grant reserve access.
        #
        # The label is always read for per-client metrics (WI-023 feature #4)
        # regardless of trust — it's observability metadata, not access control.
        client_label: str | None = None
        for k, v in scope.get("headers", []):
            if k == b"x-sluice-client-label":
                client_label = v.decode("latin-1")
                break

        reserved = False
        if self._reserved_labels and client_label and peer_is_trusted(scope, self._trusted_proxies):
            if client_label in self._reserved_labels:
                reserved = True

        acquired = await self._gate.acquire(timeout=self._queue_timeout, reserved=reserved)
        if not acquired:
            log.info("permit queue timeout — returning 503")
            self._client_metrics.record_queue_timeout(client_label)
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
        self._client_metrics.record_forwarded(client_label)

        acquire_mono = time.monotonic()
        forward_failed = False
        try:
            await self._forward(scope, receive, send, client_label=client_label)
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

    async def _forward(self, scope: Scope, receive: Receive, send: Send, *, client_label: str | None = None) -> None:
        url = self._build_url(scope)
        headers = self._filter_request_headers(scope["headers"])
        method = scope["method"]

        disconnect = asyncio.Event()
        body_done = asyncio.Event()
        body_overflow = asyncio.Event()

        async def body_stream() -> AsyncIterator[bytes]:
            """Consume ASGI receive() directly — no intermediate queue.

            By reading from receive() inline, backpressure is applied at the
            ASGI level: if the upstream is slow to accept a chunk, we don't
            call receive() again, so the ASGI server's TCP buffer fills and
            the client slows down.  This avoids both the memory risk of an
            unbounded queue and the disconnect-detection delay of a bounded
            one (the pump could block on body_queue.put() and miss
            http.disconnect events).

            WI-028 finding 3: when ``max_request_body_bytes`` is set, a
            running byte counter enforces the cap on chunked-encoding requests
            that lack a Content-Length (the pre-acquire check only sees the
            declared length).  On overflow, we stop consuming the request
            body and signal via ``body_overflow`` so the caller can send a
            413 response to the (still-connected) client.
            """
            seen = 0
            limit = self._max_request_body_bytes
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
                        if limit is not None:
                            seen += len(data)
                            if seen > limit:
                                log.info(
                                    "request body exceeded limit (%d > %d) — aborting",
                                    seen, limit,
                                )
                                body_overflow.set()
                                body_done.set()
                                return
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

            # WI-028 finding 3: if the body-size counter tripped during upload,
            # cancel the upstream request and send a 413 to the client.  The
            # entry race above may have completed (upstream accepted the partial
            # body) — close the stream context and respond.
            if body_overflow.is_set() and not response_started:
                if not entry_task.done():
                    entry_task.cancel()
                    try:
                        await entry_task
                    except (asyncio.CancelledError, Exception):
                        pass
                try:
                    await stream_cm.__aexit__(None, None, None)
                except Exception:
                    pass
                await send_json(send, 413, {"error": "request body too large"})
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
                # All non-CDN 429s are classified, but only `concurrency`
                # 429s feed the breaker.  `rate_limit` 429s (positive
                # retry-after) are tracked separately and wake the poll, but
                # do NOT trip the breaker — capture 2026-07-07 proved they
                # false-trip (36 rate_limit 429s, concurrent_sessions=0,
                # no box, 30 self-inflicted outages in 24 h).  The
                # deprioritization rung they signal is handled by
                # `is_deprioritized` → LOW band → serve at the account limit.
                #
                # This is not fully fail-safe: genuine concurrency 429s
                # that carry a small positive retry-after (capture
                # 2026-07-03: retry_after=1) are misclassified as `rate_limit`
                # here and thus don't feed the breaker.  The next `/v1/usage`
                # poll (woken immediately) *should* observe elevated
                # `concurrent_sessions` and the gate will tighten — but
                # capture 2026-07-07 showed `concurrent_sessions=0` during
                # transient bursts, so the poll may not see the pressure.
                # A shell-level safety net in `tick()` tightens to `min_floor`
                # when the reading is stale AND there are recent rate_limit
                # 429s, covering the poll-failure gap.  The breaker remains
                # the backstop for concurrency-classified 429s (no/zero
                # retry-after).
                #
                # Edge case: retry-after: 0 (or "00", " 0 ", etc.) means
                # "retry immediately" — a transient concurrency signal, not
                # a rate-limit window.  The string "0" is truthy in Python,
                # so a naive ``not header`` check would silently skip breaker
                # recording (fail-open).  _classify_429 handles this and
                # all other parse edge cases fail-safe.
                #
                # Monitoring hook (WI-024): classify every 429 as
                # 'concurrency' (feed breaker), 'rate_limit' (tracked
                # separately, wakes poll, does NOT feed breaker), or
                # 'gateway' (CDN/gateway rejection — tracked separately,
                # NOT fed to the breaker).  Logs the classification and
                # server header so CDN-originated 429s are visible.
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
                        self._client_metrics.record_concurrency_429(client_label)
                    elif classification == "rate_limit":
                        self._reconcile.record_rate_limit_429()
                        self._client_metrics.record_rate_limit_429(client_label)
                    elif classification == "gateway":
                        self._reconcile.record_gateway_429()
                        self._client_metrics.record_gateway_429(client_label)
                    else:
                        log.warning("unknown 429 classification: %s — feeding breaker", classification)
                        self._reconcile.record_429()
                        self._client_metrics.record_concurrency_429(client_label)

                # Track upstream 503s (e.g. during low-interactivity mode).
                # Does NOT feed the breaker — a 503 is an overload symptom,
                # not a concurrency rejection.  The provider keeps serving at
                # lower priority; the gate stays open.  Recorded for
                # observability and to prevent idle-detection from
                # slow-polling through a penalty window.
                if response.status_code == 503:
                    log.info(
                        "upstream 503: server=%s low_interactivity=%s",
                        response.headers.get("server"),
                        self._reconcile.is_low_interactivity(),
                    )
                    self._reconcile.record_503()
                    self._client_metrics.record_503(client_label)

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

                # Encode response headers (strips hop-by-hop).  For upstream
                # 503s during low-interactivity mode, add a Retry-After header
                # if the upstream didn't provide one — the low-interactivity
                # deadline is a honest signal that helps clients back off
                # instead of retrying immediately into more 503s.  This only
                # adds a header; the response body is never modified (inert
                # in-path, AGENTS.md rule 3).
                resp_headers = self._encode_response_headers(response)
                if (
                    response.status_code == 503
                    and not any(k.lower() == b"retry-after" for k, _ in resp_headers)
                ):
                    li_ra = self._reconcile.low_interactivity_retry_after()
                    if li_ra is not None:
                        resp_headers.append((b"retry-after", str(li_ra).encode("latin-1")))

                try:
                    await send(
                        {
                            "type": "http.response.start",
                            "status": response.status_code,
                            "headers": resp_headers,
                        }
                    )
                    response_started = True
                except Exception:
                    disconnect.set()
                    return

                idle = self._upstream_idle_timeout
                chunk_iter = response.aiter_raw()
                upstream_idle = False
                # Race each upstream read against the client-disconnect event.
                #
                # A bare ``disconnect.is_set()`` check only fires *between*
                # chunks, so a client that vanishes mid-stream while the
                # upstream has gone silent leaves this loop blocked forever in
                # ``__anext__`` — the permit is never released and becomes a
                # "local phantom" (``local_in_flight`` stuck above the
                # provider's ``concurrent_sessions`` with nothing to reconcile
                # it away).  An ungraceful client death (a rebooted host that
                # never sends FIN/RST) is surfaced as ``http.disconnect`` only
                # once TCP keepalive resets the socket (see cli._bind_listen_socket);
                # racing the read against ``disconnect`` lets the loop act on
                # that signal the instant it arrives instead of waiting for the
                # next upstream chunk that may never come.
                disc_wait = asyncio.ensure_future(disconnect.wait())
                try:
                    while True:
                        if disconnect.is_set():
                            break
                        read_task = asyncio.ensure_future(chunk_iter.__anext__())
                        done, _pending = await asyncio.wait(
                            {read_task, disc_wait},
                            timeout=idle,  # None → block until read or disconnect
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if not done:
                            # Idle timeout: no chunk *and* no disconnect within
                            # `idle` seconds — the upstream went silent.
                            await _cancel_task(read_task)
                            log.warning(
                                "upstream idle timeout (%.1fs) — aborting stream", idle,
                            )
                            upstream_idle = True
                            break
                        if read_task not in done:
                            # Client disconnected while awaiting the next chunk.
                            # Cancel the pending read; the stream context is
                            # closed in the finally below, terminating the
                            # upstream request (phantom prevention).
                            await _cancel_task(read_task)
                            break
                        try:
                            chunk = read_task.result()
                        except StopAsyncIteration:
                            break
                        if disconnect.is_set():
                            # Disconnect landed alongside the final chunk —
                            # don't bother sending to a client that is gone.
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
                finally:
                    await _cancel_task(disc_wait)

                # Close the response body.  When the upstream went idle (not a
                # client disconnect) the client is still connected and needs a
                # final ``more_body=False`` event so the ASGI transport doesn't
                # hang.  When the client disconnected, sending is futile.
                if not disconnect.is_set():
                    await send(
                        {"type": "http.response.body", "body": b"", "more_body": False}
                    )
                    # Success only after the full stream completed without
                    # a client disconnect or an upstream idle timeout — an
                    # idle-aborted stream is a degraded result, not a clean
                    # success (the breaker should not see it as healthy).
                    if 200 <= response.status_code < 400 and not upstream_idle:
                        self._reconcile.record_success()
                        self._client_metrics.record_success(client_label)
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

    @staticmethod
    def _declared_content_length(scope: Scope) -> int | None:
        """Return the Content-Length declared by the client, or None if absent/invalid.

        Used for the pre-acquire body-size fast-fail.  Chunked-encoding requests
        (no Content-Length) are caught by the running counter in body_stream().
        """
        for k, v in scope.get("headers", []):
            if k == b"content-length":
                try:
                    return int(v.decode("latin-1").strip())
                except (ValueError, UnicodeDecodeError):
                    return None
        return None

    def _filter_request_headers(self, scope_headers: list[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
        # Parse Connection header for additional hop-by-hop headers (RFC 7230 §6.1).
        connection_hop_by_hop = _parse_connection_headers(scope_headers)
        strip_set = _STRIP_REQUEST | connection_hop_by_hop

        result: list[tuple[str, str]] = []
        for k, v in scope_headers:
            name = k.decode("latin-1").lower()
            if name in strip_set:
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
        # Parse upstream Connection header for additional hop-by-hop headers.
        connection_hop_by_hop = _parse_connection_headers(
            [(k.encode("latin-1"), v.encode("latin-1")) for k, v in response.headers.items()]
        )
        strip_set = _HOP_BY_HOP | connection_hop_by_hop
        return [
            (k.encode("latin-1"), v.encode("latin-1"))
            for k, v in response.headers.items()
            if k.lower() not in strip_set
            # Strip upstream Set-Cookie values that set the sluice session cookie
            # to prevent session fixation (the upstream should never set this,
            # but if it does, it must not reach the client's browser).
            and not (
                k.lower() == "set-cookie"
                and v.strip().lower().startswith(f"{_SESSION_COOKIE}=")
            )
        ]
