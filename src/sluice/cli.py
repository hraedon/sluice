"""``sluice`` command-line entry point.

``sluice serve`` runs the concurrency-metering reverse proxy.  ``sluice status``
queries a running instance's ``/metrics`` endpoint and prints a human-readable
summary.

Config precedence: flags → environment variables → built-in defaults.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import httpx

from sluice import __version__
from sluice.control import BreakerConfig, ControllerConfig
from sluice.gate import PermitGate
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.usage import UsageClient

log = logging.getLogger("sluice.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sluice",
        description="Concurrency-metering reverse proxy for LLM APIs.",
    )
    parser.add_argument("--version", action="version", version=f"sluice {__version__}")
    sub = parser.add_subparsers(dest="command")

    # -- serve ---------------------------------------------------------------
    serve = sub.add_parser("serve", help="run the concurrency-metering reverse proxy")
    serve.add_argument("--upstream", required=True, help="upstream base URL, e.g. https://api.code.umans.ai")
    serve.add_argument("--listen", default="127.0.0.1:8800", help="host:port to listen on (default: 127.0.0.1:8800)")
    serve.add_argument("--target", type=int, default=3, help="target max observed concurrency (default: 3)")
    serve.add_argument("--poll-interval", type=float, default=5.0, help="seconds between /v1/usage polls (default: 5)")
    serve.add_argument("--release-cooldown", type=float, default=2.0, help="seconds a freed permit rests (default: 2)")
    serve.add_argument("--queue-timeout", type=float, default=30.0, help="max seconds to wait for a permit (default: 30)")
    serve.add_argument("--usage-key-env", default="SLUICE_USAGE_KEY", help="env var holding the usage API key (default: SLUICE_USAGE_KEY)")
    serve.add_argument(
        "--usage-auth-header",
        default="authorization",
        choices=["authorization", "x-api-key"],
        help="auth header for /v1/usage (default: authorization)",
    )
    serve.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="logging level (default: INFO)")

    # -- status --------------------------------------------------------------
    status = sub.add_parser("status", help="print current reading, computed permits, and band")
    status.add_argument("--host", default="127.0.0.1:8800", help="sluice listen address to query (default: 127.0.0.1:8800)")

    return parser


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


def _cmd_serve(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    usage_key = os.environ.get(args.usage_key_env)
    if not usage_key:
        print(f"sluice: error: environment variable {args.usage_key_env} is not set", file=sys.stderr)
        print("       set it to the API key used for /v1/usage polling", file=sys.stderr)
        return 2

    host, _, port_str = args.listen.rpartition(":")
    if not host or not port_str:
        print(f"sluice: error: --listen must be host:port, got '{args.listen}'", file=sys.stderr)
        return 2
    port = int(port_str)

    usage_client = UsageClient(
        base_url=args.upstream,
        api_key=usage_key,
        auth_header=args.usage_auth_header,
    )
    gate = PermitGate(
        initial_capacity=args.target,
        release_cooldown=args.release_cooldown,
    )
    reconcile = ReconciliationLoop(
        usage_client=usage_client,
        gate=gate,
        controller_config=ControllerConfig(target=args.target),
        breaker_config=BreakerConfig(),
        poll_interval=args.poll_interval,
    )
    app = ProxyApp(
        upstream_base_url=args.upstream,
        gate=gate,
        reconcile=reconcile,
        queue_timeout=args.queue_timeout,
    )

    log.info("sluice %s starting", __version__)
    log.info("  upstream:          %s", args.upstream)
    log.info("  listen:            %s:%d", host, port)
    log.info("  target:            %d", args.target)
    log.info("  poll_interval:     %.1fs", args.poll_interval)
    log.info("  release_cooldown:  %.1fs", args.release_cooldown)
    log.info("  queue_timeout:     %.1fs", args.queue_timeout)
    log.info("  usage_key_env:     %s", args.usage_key_env)
    log.info("  usage_auth_header: %s", args.usage_auth_header)

    import uvicorn

    uvicorn.run(app, host=host, port=port, log_level=args.log_level.lower())
    return 0


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def _cmd_status(args: argparse.Namespace) -> int:
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(f"http://{args.host}/metrics")
    except httpx.ConnectError:
        print(f"sluice: cannot connect to {args.host} — is sluice running?", file=sys.stderr)
        return 1

    if response.status_code != 200:
        print(f"sluice: error {response.status_code} from {args.host}", file=sys.stderr)
        return 1

    d = response.json()
    print(f"band:               {d['band']}")
    print(f"breaker:            {d['breaker']}")
    print(f"effective_permits:  {d['effective_permits']}")
    print(f"in_flight:          {d['in_flight']}")
    print(f"observed_sessions:  {d['observed_concurrent_sessions']}")
    print(f"total_429s:         {d['total_429s']}")
    print(f"queue_depth:        {d['queue_depth']}")
    print(f"cooling_down:       {d['cooling_down']}")
    return 0


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "serve":
        return _cmd_serve(args)
    if args.command == "status":
        return _cmd_status(args)
    build_parser().print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
