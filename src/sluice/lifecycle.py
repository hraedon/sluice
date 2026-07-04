"""ASGI lifespan and singleton lease management — extracted from proxy.py.

Handles the ASGI lifespan protocol (startup/shutdown) and the singleton
guard retry loop that re-acquires a lease when the initial acquire fails.
This is the *when to start/stop* concern; the proxy module handles the
*what to do with each request* concern.

On startup:
  * If a singleton guard is configured, acquire the lease.  If acquisition
    fails, start a background retry task with jittered backoff.
  * Start the reconciliation loop (leader only).

On shutdown (graceful drain):
  * Cancel any pending retry task.
  * Set a draining flag so the proxy fast-fails new requests with 503.
  * Stop the reconciliation loop (no more polling or gate resizing).
  * Wait for in-flight requests to complete (``gate.held → 0``), bounded
    by ``drain_timeout`` (default 25 s — fits within uvicorn's
    ``timeout_graceful_shutdown=30`` and k8s' ``terminationGracePeriodSeconds=120``).
  * Close the upstream httpx client — now safe because no streaming
    connections remain.
  * Release the lease (if held) and stop the renewer.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING, Any

from collections.abc import Awaitable, Callable

if TYPE_CHECKING:
    import httpx
    from sluice.gate import PermitGate
    from sluice.reconcile import ReconciliationLoop
    from sluice.singleton import SingletonGuard

log = logging.getLogger("sluice.lifecycle")

# ASGI callable types (mirrored from admin.py to avoid a circular import).
Scope = dict[str, Any]
Send = Callable[[dict[str, Any]], Awaitable[None]]
Receive = Callable[[], Awaitable[dict[str, Any]]]

_DRAIN_POLL_INTERVAL = 0.1


class LifecycleManager:
    """Owns the ASGI lifespan protocol and singleton lease lifecycle.

    Created by :class:`~sluice.proxy.ProxyApp` and delegated to when the ASGI
    server sends lifespan events.  The proxy retains its own references to
    ``guard`` and ``reconcile`` for request-path checks (``is_held()``,
    ``record_429()``, etc.); this class manages only startup/shutdown.
    """

    def __init__(
        self,
        *,
        guard: SingletonGuard | None,
        reconcile: ReconciliationLoop,
        client: httpx.AsyncClient,
        owns_client: bool,
        retry_interval: float,
        gate: PermitGate,
        drain_timeout: float = 25.0,
    ) -> None:
        self._guard = guard
        self._reconcile = reconcile
        self._client = client
        self._owns_client = owns_client
        self._retry_interval = retry_interval
        self._gate = gate
        self._drain_timeout = drain_timeout
        self._acquired = False
        self._retry_task: asyncio.Task[None] | None = None
        self._draining = False

    @property
    def acquired(self) -> bool:
        """True if this instance successfully acquired the singleton lease."""
        return self._acquired

    @property
    def is_draining(self) -> bool:
        """True during shutdown drain — new requests should be fast-failed."""
        return self._draining

    async def handle_lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                if self._guard is not None:
                    acquired = await self._guard.acquire()
                    if not acquired:
                        log.warning("singleton guard acquire failed — starting as non-leader, will retry")
                        self._retry_task = asyncio.create_task(self._retry_acquire())
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
                            self._retry_task = asyncio.create_task(self._retry_acquire())
                        else:
                            self._acquired = True
                else:
                    await self._reconcile.start()
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                try:
                    if self._retry_task is not None:
                        self._retry_task.cancel()
                        try:
                            await self._retry_task
                        except asyncio.CancelledError:
                            pass
                        self._retry_task = None

                    # Graceful drain: stop accepting new requests, wait for
                    # in-flight to finish, then close the upstream client.
                    # This prevents severing active streaming connections
                    # mid-response.
                    self._draining = True
                    await self._reconcile.stop()

                    in_flight = self._gate.held
                    if in_flight > 0 and self._drain_timeout > 0:
                        log.info("shutdown: draining — waiting for %d in-flight request(s)", in_flight)
                        deadline = asyncio.get_running_loop().time() + self._drain_timeout
                        drained = False
                        while self._gate.held > 0:
                            remaining = deadline - asyncio.get_running_loop().time()
                            if remaining <= 0:
                                log.warning(
                                    "shutdown: drain timeout (%.1fs) — closing with %d request(s) still in-flight",
                                    self._drain_timeout,
                                    self._gate.held,
                                )
                                break
                            await asyncio.sleep(min(_DRAIN_POLL_INTERVAL, remaining))
                        else:
                            drained = True
                        if drained:
                            log.info("shutdown: drain complete — all requests finished")
                finally:
                    # Guarantee cleanup even if the drain is interrupted
                    # (e.g. CancelledError from uvicorn shutdown timeout).
                    if self._guard is not None:
                        await self._guard.stop_renewer()
                        if self._acquired:
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
        while not self._acquired:
            # Jitter (±50%) so multiple non-leader pods retrying after a leader
            # crash don't stampede the apiserver in lockstep.
            await asyncio.sleep(self._retry_interval * (0.5 + random.random()))
            try:
                acquired = await guard.acquire()
                if acquired:
                    try:
                        await guard.start_renewer()
                        await self._reconcile.start()
                    except asyncio.CancelledError:
                        # Shutdown during start: release the lease we just
                        # acquired before letting cancellation propagate.
                        await guard.stop_renewer()
                        await guard.release()
                        raise
                    except Exception:
                        log.warning(
                            "singleton guard start failed after acquire — releasing lease, will retry",
                            exc_info=True,
                        )
                        await guard.stop_renewer()
                        await guard.release()
                        continue
                    self._acquired = True
                    log.info("singleton guard acquired on retry — becoming leader")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("singleton guard retry acquire failed", exc_info=True)
