"""Integration test for WI-014: upstream cancellation on client disconnect.

Uses a real uvicorn server + httpx client (not mock transport) to validate
that the proxy's racing logic (asyncio.wait on stream entry vs disconnect)
works with real network I/O and connection latency.

Run only when uvicorn is available; skipped otherwise.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable

import httpx
import pytest

from sluice.control import BreakerConfig, ControllerConfig, UsageReading
from sluice.gate import PermitGate
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.usage import CachedReading

try:
    import uvicorn
except ImportError:
    pytest.skip("uvicorn not installed", allow_module=True)


class FakeUsageClient:
    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        return CachedReading(
            reading=UsageReading(concurrent_sessions=0, limit=4, hard_cap=8),
            fetched_at_monotonic=now_monotonic,
            ok=True,
        )

    async def close(self) -> None:
        pass


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _start_proxy(
    upstream_url: str, port: int
) -> tuple[uvicorn.Server, asyncio.Task[None], PermitGate]:
    gate = PermitGate(initial_capacity=3)
    usage = FakeUsageClient()
    reconcile = ReconciliationLoop(
        usage_client=usage,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(),
        breaker_config=BreakerConfig(),
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url=upstream_url,
        gate=gate,
        reconcile=reconcile,
    )
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.2)
    return server, server_task, gate


async def _wait_for(condition: Callable[[], bool], *, timeout: float = 5.0) -> bool:
    """Poll a condition until it returns True or the timeout elapses."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(0.05)
    return condition()


async def _shutdown_servers(
    *servers: tuple[uvicorn.Server, asyncio.Task[None]],
) -> None:
    """Signal shutdown and await all server tasks, swallowing cleanup errors."""
    for server, task in servers:
        server.should_exit = True
        if not task.done():
            task.cancel()
    for _, task in servers:
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


async def test_disconnect_cancels_upstream_with_real_connections():
    """When a client disconnects mid-stream, the upstream request is cancelled.

    Uses a real upstream server that streams slowly. The client connects
    through sluice, receives the first chunk, then disconnects. The permit
    must be released (phantom prevention).
    """
    upstream_port = _free_port()
    proxy_port = _free_port()

    upstream_received_request = asyncio.Event()
    upstream_terminated = asyncio.Event()
    upstream_completed = asyncio.Event()

    async def upstream_app(scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                event = await receive()
                if event["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif event["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        upstream_received_request.set()
        await send({"type": "http.response.start", "status": 200,
                     "headers": [(b"content-type", b"text/event-stream")]})
        try:
            for i in range(100):
                await send({"type": "http.response.body",
                            "body": f"data: chunk{i}\n\n".encode(),
                            "more_body": True})
                # Periodically call receive() to check for client disconnect.
                # In ASGI, http.disconnect is delivered via receive(), but
                # only when the app calls it.  Alternating send/receive
                # gives the event loop a chance to detect the proxy's
                # connection close and deliver the disconnect event.
                try:
                    event = await asyncio.wait_for(receive(), timeout=0.05)
                    if event["type"] == "http.disconnect":
                        upstream_terminated.set()
                        return
                except asyncio.TimeoutError:
                    pass
        except (Exception, asyncio.CancelledError):
            upstream_terminated.set()
            raise
        else:
            upstream_completed.set()

    upstream_config = uvicorn.Config(upstream_app, host="127.0.0.1",
                                      port=upstream_port, log_level="error")
    upstream_server = uvicorn.Server(upstream_config)
    upstream_task = asyncio.create_task(upstream_server.serve())
    await asyncio.sleep(0.2)

    try:
        server, server_task, gate = await _start_proxy(
            f"http://127.0.0.1:{upstream_port}", proxy_port
        )

        # Connect through sluice and read the first chunk
        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                f"http://127.0.0.1:{proxy_port}/v1/messages",
                json={"prompt": "hi"},
                timeout=30.0,
            ) as response:
                assert response.status_code == 200
                # Read one chunk to confirm streaming works
                async for chunk in response.aiter_bytes():
                    assert b"chunk0" in chunk
                    break  # got first chunk, now disconnect
                # Closing the context manager disconnects the client

        # Poll for the permit to be released (phantom prevention)
        released = await _wait_for(lambda: gate.held == 0, timeout=5.0)
        assert released, f"permit not released after disconnect (held={gate.held})"

        # Poll for the upstream to stop — either terminated (cancelled,
        # send-error, etc.) or completed (all chunks sent).  The WI-014
        # guarantee is that the upstream sees a terminated request, not an
        # abandoned one running to completion as a phantom.
        stopped = await _wait_for(
            lambda: upstream_terminated.is_set() or upstream_completed.is_set(),
            timeout=15.0,
        )
        assert stopped, "upstream did not stop within timeout"
        assert upstream_terminated.is_set(), (
            "upstream should have been terminated (not completed) on client disconnect"
        )

    finally:
        await _shutdown_servers(
            (upstream_server, upstream_task),
            (server, server_task),
        )


async def test_normal_request_completes_with_real_connections():
    """A normal request through the real proxy completes and releases the permit."""
    upstream_port = _free_port()
    proxy_port = _free_port()

    async def upstream_app(scope, receive, send):
        if scope["type"] == "lifespan":
            while True:
                event = await receive()
                if event["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif event["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        await send({"type": "http.response.start", "status": 200,
                     "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body",
                     "body": b'{"ok": true}', "more_body": False})

    upstream_config = uvicorn.Config(upstream_app, host="127.0.0.1",
                                      port=upstream_port, log_level="error")
    upstream_server = uvicorn.Server(upstream_config)
    upstream_task = asyncio.create_task(upstream_server.serve())
    await asyncio.sleep(0.2)

    try:
        server, server_task, gate = await _start_proxy(
            f"http://127.0.0.1:{upstream_port}", proxy_port
        )

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://127.0.0.1:{proxy_port}/v1/messages",
                json={"prompt": "hi"},
                timeout=10.0,
            )
            assert response.status_code == 200
            assert response.json() == {"ok": True}

        # Poll for the permit to be released (the proxy's finally block
        # may not have run yet when client.post() returns).
        released = await _wait_for(lambda: gate.held == 0, timeout=5.0)
        assert released, f"permit not released after completion (held={gate.held})"

    finally:
        await _shutdown_servers(
            (upstream_server, upstream_task),
            (server, server_task),
        )
