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
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import httpx

from sluice.gate import PermitGate
from sluice.reconcile import ReconciliationLoop

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

_QUEUE_TIMEOUT_DEFAULT = 30.0
_RETRY_AFTER_DEFAULT = 5


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
    ) -> None:
        self._upstream = upstream_base_url.rstrip("/")
        self._gate = gate
        self._reconcile = reconcile
        self._queue_timeout = queue_timeout
        self._client = upstream_client or httpx.AsyncClient(timeout=None)
        self._owns_client = upstream_client is None

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
        if path == "/metrics":
            await self._send_metrics(send)
            return

        await self._proxy_request(scope, receive, send)

    async def _handle_lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                await self._reconcile.start()
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                await self._reconcile.stop()
                if self._owns_client:
                    await self._client.aclose()
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _proxy_request(self, scope: Scope, receive: Receive, send: Send) -> None:
        acquired = await self._gate.acquire(timeout=self._queue_timeout)
        if not acquired:
            log.info("permit queue timeout — returning 503")
            await self._send_json(
                send,
                503,
                {"error": "concurrency limit reached", "retry_after": _RETRY_AFTER_DEFAULT},
                retry_after=_RETRY_AFTER_DEFAULT,
            )
            return

        try:
            await self._forward(scope, receive, send)
        except Exception:
            log.exception("proxy forward failed")
        finally:
            await self._gate.release()

    async def _forward(self, scope: Scope, receive: Receive, send: Send) -> None:
        url = self._build_url(scope)
        headers = self._filter_request_headers(scope["headers"])
        method = scope["method"]

        disconnect = asyncio.Event()
        body_queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def pump_receive() -> None:
            """Single consumer of the ASGI receive callable."""
            while True:
                event = await receive()
                etype = event["type"]
                if etype == "http.disconnect":
                    disconnect.set()
                    await body_queue.put(None)  # unblock body_stream
                    return
                if etype == "http.request":
                    data = event.get("body", b"")
                    if data:
                        await body_queue.put(data)
                    if not event.get("more_body", False):
                        await body_queue.put(None)
                        # Keep listening for disconnect during response phase.

        async def body_stream() -> AsyncIterator[bytes]:
            while True:
                chunk = await body_queue.get()
                if chunk is None:
                    return
                yield chunk

        pump_task = asyncio.create_task(pump_receive())

        try:
            async with self._client.stream(
                method, url, headers=headers, content=body_stream()
            ) as response:
                # Report status to the reconciliation loop.
                if response.status_code == 429:
                    self._reconcile.record_429()
                elif 200 <= response.status_code < 400:
                    self._reconcile.record_success()

                await send(
                    {
                        "type": "http.response.start",
                        "status": response.status_code,
                        "headers": self._encode_response_headers(response),
                    }
                )

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
                        # send failed — client gone
                        disconnect.set()
                        break

                if not disconnect.is_set():
                    await send(
                        {"type": "http.response.body", "body": b"", "more_body": False}
                    )
        except httpx.RequestError as exc:
            if not disconnect.is_set():
                log.warning("upstream error: %s: %s", type(exc).__name__, exc)
                try:
                    await self._send_json(send, 502, {"error": "upstream error"})
                except Exception:
                    pass
        finally:
            if not pump_task.done():
                pump_task.cancel()
                try:
                    await pump_task
                except asyncio.CancelledError:
                    pass

    def _build_url(self, scope: Scope) -> str:
        path: str = scope["path"]
        qs: bytes = scope.get("query_string", b"")
        if qs:
            path += "?" + qs.decode("latin-1")
        return self._upstream + path

    @staticmethod
    def _filter_request_headers(scope_headers: list[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
        result: list[tuple[str, str]] = []
        for k, v in scope_headers:
            name = k.decode("latin-1").lower()
            if name in _HOP_BY_HOP or name == "host":
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

    async def _send_metrics(self, send: Send) -> None:
        body: dict[str, Any] = {
            "in_flight": self._gate.held,
            "effective_permits": self._reconcile.effective_permits_count,
            "observed_concurrent_sessions": self._reconcile.observed_concurrent_sessions,
            "band": self._reconcile.band.value,
            "breaker": self._reconcile.breaker_state.value,
            "total_429s": self._reconcile.total_429s,
            "recent_429_count": self._reconcile.recent_429_count,
            "queue_depth": self._gate.queue_depth,
            "cooling_down": self._gate.cooling_down,
        }
        await self._send_json(send, 200, body)
