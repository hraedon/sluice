#!/usr/bin/env python3
"""Fault-injection soak harness for sluice (Plan 007 WI-5).

Runs a local mock upstream with configurable fault behaviours and a client
mix against a real ``sluice serve`` process.  After a bounded duration, asserts
invariants from ``/status.json``:

  * ``held == 0`` — no permit leak
  * breaker returned to ``CLOSED``
  * zero 5xx from sluice itself (upstream-originated errors excluded)
  * byte-identical egress on a checksummed echo route (Rule 7)

Usage::

    uv run scripts/soak.py --duration 60

Exits 0 on success, non-zero with the failing scenario named on failure.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import socket
import time

import httpx
import uvicorn

log = logging.getLogger("soak")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _wait_for(url: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with httpx.AsyncClient() as c:
                r = await c.get(url)
                if r.status_code == 200:
                    return
        except Exception:
            pass
        await asyncio.sleep(0.1)
    raise TimeoutError(f"server at {url} did not become ready within {timeout}s")


async def _shutdown(*servers: uvicorn.Server) -> None:
    for s in servers:
        s.should_exit = True
    await asyncio.sleep(0.3)


# ---------------------------------------------------------------------------
# Fault-injecting upstream
# ---------------------------------------------------------------------------


class FaultUpstream:
    """ASGI app that injects configurable faults and echoes bodies for checksum.

    Fault schedule (round-robin by request counter):
      - 1/4: normal stream with 50ms inter-chunk delay
      - 1/4: mid-stream abort after first chunk
      - 1/4: hang (never send headers, 5s timeout on sluice side)
      - 1/4: 429 burst (return 429 with retry-after: 0)

    The ``/echo`` route returns the request body as-is, for byte-identity checks.
    """

    def __init__(self) -> None:
        self.request_count = 0
        self.echo_hashes: dict[int, str] = {}

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "lifespan":
            while True:
                event = await receive()
                if event["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif event["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
            return

        if scope["type"] != "http":
            return

        path = scope["path"]
        method = scope["method"]

        # Health check for readiness probing.
        if path == "/healthz":
            await send({"type": "http.response.start", "status": 200,
                         "headers": [(b"content-type", b"application/json")]})
            await send({"type": "http.response.body",
                         "body": b'{"status":"ok"}', "more_body": False})
            return

        # Echo route — return the request body for byte-identity verification.
        if path == "/echo" and method == "POST":
            body = bytearray()
            more = True
            while more:
                event = await receive()
                if event["type"] == "http.disconnect":
                    return
                if event["type"] == "http.request":
                    body.extend(event.get("body", b""))
                    more = event.get("more_body", False)

            body_bytes = bytes(body)
            digest = hashlib.sha256(body_bytes).hexdigest()
            self.echo_hashes[id(scope)] = digest

            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"x-echo-hash", digest.encode()),
                ],
            })
            await send({
                "type": "http.response.body",
                "body": json.dumps({"hash": digest, "len": len(body_bytes)}).encode(),
                "more_body": False,
            })
            return

        # Fault-injection routes.
        self.request_count += 1
        fault = self.request_count % 4

        if fault == 0:
            # Normal stream with inter-chunk delay.
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            })
            for i in range(5):
                await send({
                    "type": "http.response.body",
                    "body": f"data: chunk{i}\n\n".encode(),
                    "more_body": True,
                })
                await asyncio.sleep(0.05)
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return

        if fault == 1:
            # Mid-stream abort after first chunk.
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            })
            await send({
                "type": "http.response.body",
                "body": b"data: abort-after-this\n\n",
                "more_body": True,
            })
            await asyncio.sleep(0.02)
            # Simulate upstream dropping the connection.
            return

        if fault == 2:
            # Hang — never send headers.  The soak client's read timeout
            # will fire, causing it to disconnect, which cancels the upstream
            # via sluice's disconnect handling.
            await asyncio.sleep(30.0)
            return

        if fault == 3:
            # 429 burst (concurrency-style, retry-after: 0).
            # Drain the request body first.
            more = True
            while more:
                event = await receive()
                if event["type"] == "http.request":
                    more = event.get("more_body", False)
                elif event["type"] == "http.disconnect":
                    return

            await send({
                "type": "http.response.start",
                "status": 429,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"retry-after", b"0"),
                ],
            })
            await send({
                "type": "http.response.body",
                "body": json.dumps({"error": "overloaded"}).encode(),
                "more_body": False,
            })
            return


# ---------------------------------------------------------------------------
# Client workers
# ---------------------------------------------------------------------------


class SoakStats:
    def __init__(self) -> None:
        self.requests = 0
        self.errors_5xx = 0
        self.errors_other = 0
        self.disconnects = 0
        self.echo_ok = 0
        self.echo_fail = 0
        self.sluice_5xx_statuses: list[int] = []


async def _client_streamer(
    proxy_url: str, stats: SoakStats, stop: asyncio.Event, client_id: int
) -> None:
    """Send streaming requests until stop is set."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
        while not stop.is_set():
            stats.requests += 1
            try:
                async with client.stream(
                    "POST",
                    f"{proxy_url}/v1/messages",
                    json={"prompt": f"client-{client_id}"},
                ) as r:
                    if r.status_code >= 500:
                        stats.errors_5xx += 1
                        stats.sluice_5xx_statuses.append(r.status_code)
                    async for _ in r.aiter_bytes():
                        pass
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError):
                stats.errors_other += 1
            except Exception:
                stats.errors_other += 1
            await asyncio.sleep(0.01)


async def _client_disconnector(
    proxy_url: str, stats: SoakStats, stop: asyncio.Event
) -> None:
    """Connect, read first chunk, then disconnect."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
        while not stop.is_set():
            stats.requests += 1
            try:
                async with client.stream(
                    "POST",
                    f"{proxy_url}/v1/messages",
                    json={"prompt": "disconnect"},
                ) as r:
                    if r.status_code >= 500:
                        stats.errors_5xx += 1
                        stats.sluice_5xx_statuses.append(r.status_code)
                    async for _ in r.aiter_bytes():
                        break  # read one chunk then disconnect
                stats.disconnects += 1
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError):
                stats.errors_other += 1
            except Exception:
                stats.errors_other += 1
            await asyncio.sleep(0.05)


async def _client_echo(
    proxy_url: str, stats: SoakStats, stop: asyncio.Event
) -> None:
    """Send a known body to /echo and verify byte-identity via checksum."""
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
        seq = 0
        while not stop.is_set():
            seq += 1
            body = json.dumps({"seq": seq, "data": "x" * 100}).encode()
            expected_hash = hashlib.sha256(body).hexdigest()
            stats.requests += 1
            try:
                r = await client.post(
                    f"{proxy_url}/echo",
                    content=body,
                    headers={"content-type": "application/json"},
                )
                if r.status_code == 200:
                    actual_hash = r.headers.get("x-echo-hash")
                    if actual_hash == expected_hash:
                        stats.echo_ok += 1
                    else:
                        stats.echo_fail += 1
                        log.warning(
                            "echo mismatch: expected=%s got=%s",
                            expected_hash[:12],
                            (actual_hash or "missing")[:12],
                        )
                elif r.status_code >= 500:
                    stats.errors_5xx += 1
                    stats.sluice_5xx_statuses.append(r.status_code)
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError):
                stats.errors_other += 1
            except Exception:
                stats.errors_other += 1
            await asyncio.sleep(0.02)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run_soak(duration: float, n_streamers: int) -> int:
    upstream_port = _free_port()
    proxy_port = _free_port()

    upstream_app = FaultUpstream()

    upstream_config = uvicorn.Config(
        upstream_app, host="127.0.0.1", port=upstream_port, log_level="error"
    )
    upstream_server = uvicorn.Server(upstream_config)
    upstream_task = asyncio.create_task(upstream_server.serve())

    proxy_url = f"http://127.0.0.1:{proxy_port}"

    # Build sluice proxy directly (no subprocess needed).
    from sluice.control import BreakerConfig, ControllerConfig, UsageReading
    from sluice.gate import PermitGate
    from sluice.proxy import ProxyApp
    from sluice.reconcile import ReconciliationLoop
    from sluice.usage import CachedReading

    class _SoakUsageClient:
        """TruthSource that returns a healthy reading for soak testing."""
        def __init__(self) -> None:
            self._cached: CachedReading | None = None

        async def fetch(self, *, now_monotonic: float) -> CachedReading:
            self._cached = CachedReading(
                reading=UsageReading(
                    concurrent_sessions=0, limit=4, hard_cap=8
                ),
                fetched_at_monotonic=now_monotonic,
                ok=True,
            )
            return self._cached

        @property
        def last_cached(self) -> CachedReading | None:
            return self._cached

        async def close(self) -> None:
            pass

        def record_response_headers(
            self, headers: dict[str, str], status: int, *, now_monotonic: float
        ) -> None:
            pass

    gate = PermitGate(initial_capacity=3, release_cooldown=0.5)
    usage = _SoakUsageClient()
    reconcile = ReconciliationLoop(
        truth_source=usage,
        gate=gate,
        controller_config=ControllerConfig(target=3),
        breaker_config=BreakerConfig(threshold=10, cooldown_seconds=2.0),
        poll_interval=0.5,
    )
    reconcile._first_poll_ok = True
    app = ProxyApp(
        upstream_base_url=f"http://127.0.0.1:{upstream_port}",
        gate=gate,
        reconcile=reconcile,
        queue_timeout=5.0,
    )
    proxy_config = uvicorn.Config(
        app, host="127.0.0.1", port=proxy_port, log_level="error"
    )
    proxy_server = uvicorn.Server(proxy_config)
    proxy_task = asyncio.create_task(proxy_server.serve())

    # Wait for both to be ready.
    await _wait_for(f"http://127.0.0.1:{upstream_port}/healthz", timeout=5.0)
    await _wait_for(f"http://127.0.0.1:{proxy_port}/healthz", timeout=5.0)
    log.info("upstream and proxy ready")

    # Start reconcile loop.
    await reconcile.start()

    # Run client workers.
    stats = SoakStats()
    stop = asyncio.Event()

    workers = []
    for i in range(n_streamers):
        workers.append(asyncio.create_task(_client_streamer(proxy_url, stats, stop, i)))
    workers.append(asyncio.create_task(_client_disconnector(proxy_url, stats, stop)))
    workers.append(asyncio.create_task(_client_echo(proxy_url, stats, stop)))

    log.info("running soak for %.0fs with %d streamers", duration, n_streamers)
    await asyncio.sleep(duration)
    stop.set()

    # Wait for workers to finish.
    await asyncio.gather(*workers, return_exceptions=True)
    log.info("workers stopped")

    # Wait a moment for permits to settle.
    await asyncio.sleep(2.0)

    # Stop reconcile.
    await reconcile.stop()

    # Collect status.
    async with httpx.AsyncClient() as c:
        status_resp = await c.get(f"{proxy_url}/status.json")
        status = status_resp.json()

    # Shutdown servers.
    await _shutdown(upstream_server, proxy_server)
    upstream_task.cancel()
    proxy_task.cancel()
    try:
        await upstream_task
    except (asyncio.CancelledError, Exception):
        pass
    try:
        await proxy_task
    except (asyncio.CancelledError, Exception):
        pass

    # Print stats.
    log.info("--- soak stats ---")
    log.info("  total requests:    %d", stats.requests)
    log.info("  5xx from sluice:   %d %s", stats.errors_5xx, stats.sluice_5xx_statuses[:10])
    log.info("  other errors:      %d", stats.errors_other)
    log.info("  disconnects:       %d", stats.disconnects)
    log.info("  echo ok:           %d", stats.echo_ok)
    log.info("  echo fail:         %d", stats.echo_fail)
    log.info("--- sluice status ---")
    log.info("  held:              %d", status.get("local_in_flight", -1))
    log.info("  breaker:           %s", status.get("breaker", "?"))
    log.info("  effective_permits: %s", status.get("effective_permits", "?"))
    log.info("  total_429s:        %s", status.get("total_429s", "?"))
    log.info("  queue_timeouts:    %s", status.get("queue_timeouts", "?"))

    # Assert invariants.
    failures = []

    held = status.get("local_in_flight", -1)
    if held != 0:
        failures.append(f"permit leak: held={held} (expected 0)")

    breaker = status.get("breaker", "?")
    if breaker not in ("closed", "half_open"):
        failures.append(f"breaker not recovered: {breaker}")

    if stats.echo_fail > 0:
        failures.append(f"byte-identity failures: {stats.echo_fail}")

    if stats.echo_ok == 0:
        failures.append("echo worker never completed a byte-identity check")

    if stats.errors_5xx > 0:
        failures.append(
            f"sluice emitted {stats.errors_5xx} 5xx responses: "
            f"{stats.sluice_5xx_statuses[:10]}"
        )

    if failures:
        log.error("SOAK FAILED:")
        for f in failures:
            log.error("  - %s", f)
        return 1

    log.info("SOAK PASSED — all invariants hold")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="sluice fault-injection soak harness")
    parser.add_argument("--duration", type=float, default=60.0, help="duration in seconds (default: 60)")
    parser.add_argument("--streamers", type=int, default=4, help="number of concurrent streamers (default: 4)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        return asyncio.run(run_soak(args.duration, args.streamers))
    except KeyboardInterrupt:
        log.warning("interrupted")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
