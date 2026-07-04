"""``sluice`` command-line entry point.

``sluice serve`` runs the concurrency-metering reverse proxy.  ``sluice status``
queries a running instance's ``/metrics`` endpoint and prints a human-readable
summary.

Config precedence: flags → environment variables → config file → built-in defaults.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

import httpx

from sluice import __version__
from sluice.control import AdaptiveConfig, BreakerConfig, ControllerConfig
from sluice.gate import PermitGate
from sluice.history import History
from sluice.history_store import SQLiteHistoryStore
from sluice.providers import get_provider, make_truth_source
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.singleton import KubeLeaseGuard, NoopGuard, SingletonGuard

log = logging.getLogger("sluice.cli")

_ENV_PREFIX = "SLUICE_"

_DEFAULTS: dict[str, Any] = {
    "upstream": None,
    "listen": "127.0.0.1:8800",
    "target": 3,
    "poll_interval": 5.0,
    "release_cooldown": 2.0,
    "queue_timeout": 30.0,
    "retry_interval": 10.0,
    "usage_key_env": "SLUICE_USAGE_KEY",
    "usage_auth_header": "authorization",
    "log_level": "INFO",
    "config": None,
    "admin_token": None,
    "reserve": None,
    "provider": "umans",
    "history_size": 2880,
    "history_store": None,
    "history_ttl": 604800.0,
    "singleton_guard": "noop",
    "drain_timeout": 25.0,
}


def _resolve(key: str, args: argparse.Namespace) -> Any:
    """Resolve a config value: flag → env var → config file → built-in default."""
    flag_val = getattr(args, key, None)
    if flag_val is not None:
        return flag_val
    env_val = os.environ.get(_ENV_PREFIX + key.upper())
    if env_val is not None:
        return _coerce(env_val, key)
    config = getattr(args, "_config_data", None)
    if config and key in config:
        return config[key]
    return _DEFAULTS.get(key)


def _coerce(env_val: str, key: str) -> Any:
    if key in ("target", "history_size"):
        return int(env_val)
    if key in ("poll_interval", "release_cooldown", "queue_timeout", "retry_interval", "history_ttl", "drain_timeout"):
        return float(env_val)
    return env_val


def _load_config_file(path: str) -> dict[str, Any]:
    """Load a TOML config file's [serve] section."""
    import tomllib

    p = Path(path)
    if not p.exists():
        print(f"sluice: error: config file not found: {path}", file=sys.stderr)
        raise SystemExit(2)
    with p.open("rb") as f:
        data: dict[str, Any] = tomllib.load(f)
    serve_section = data.get("serve", data)
    if isinstance(serve_section, dict):
        return serve_section
    return data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sluice",
        description="Concurrency-metering reverse proxy for LLM APIs.",
    )
    parser.add_argument("--version", action="version", version=f"sluice {__version__}")
    sub = parser.add_subparsers(dest="command")

    # -- serve ---------------------------------------------------------------
    serve = sub.add_parser("serve", help="run the concurrency-metering reverse proxy")
    serve.add_argument("--upstream", default=None, help="upstream base URL, e.g. https://api.code.umans.ai")
    serve.add_argument("--provider", default=None, choices=["umans", "anthropic", "openai", "generic"], help="upstream provider type (default: umans)")
    serve.add_argument("--listen", default=None, help="host:port to listen on (default: 127.0.0.1:8800)")
    serve.add_argument("--target", type=int, default=None, help="target max observed concurrency (default: 3)")
    serve.add_argument("--poll-interval", type=float, default=None, help="seconds between /v1/usage polls (default: 5)")
    serve.add_argument("--release-cooldown", type=float, default=None, help="seconds a freed permit rests (default: 2)")
    serve.add_argument("--queue-timeout", type=float, default=None, help="max seconds to wait for a permit (default: 30)")
    serve.add_argument("--retry-interval", type=float, default=None, help="seconds between singleton-lease re-acquire attempts when not leader (default: 10)")
    serve.add_argument("--usage-key-env", default=None, help="env var holding the usage API key (default: SLUICE_USAGE_KEY)")
    serve.add_argument(
        "--usage-auth-header",
        default=None,
        choices=["authorization", "x-api-key"],
        help="auth header for /v1/usage (default: authorization)",
    )
    serve.add_argument("--log-level", default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="logging level (default: INFO)")
    serve.add_argument("--config", default=None, help="path to TOML config file with a [serve] section")
    serve.add_argument("--admin-token", default=None, help="token for admin routes (/, /status.json, /metrics) — sent as Bearer header or Basic auth password")
    serve.add_argument("--reserve", default=None, help="reserve permits for a QoS class, e.g. 'interactive=1' (default: none → pure FIFO)")
    serve.add_argument("--history-size", type=int, default=None, help="number of tick snapshots to retain for trend analysis (default: 2880, ~4h at 5s poll; 0 disables)")
    serve.add_argument("--history-store", default=None, help="path to SQLite file for history persistence (default: none — in-memory only). Survives restarts; enables crash forensics.")
    serve.add_argument("--history-ttl", type=float, default=None, help="seconds to retain entries in the SQLite store before pruning (default: 604800 = 7 days)")
    serve.add_argument("--singleton-guard", default=None, choices=["noop", "kube-lease"], help="singleton guard mode (default: noop; env: SLUICE_SINGLETON_GUARD)")
    serve.add_argument("--drain-timeout", type=float, default=None, help="seconds to wait for in-flight requests on shutdown before closing upstream (default: 25)")

    # -- status --------------------------------------------------------------
    status = sub.add_parser("status", help="print current reading, computed permits, and band")
    status.add_argument("--host", default="127.0.0.1:8800", help="sluice listen address to query (default: 127.0.0.1:8800)")
    status.add_argument("--admin-token", default=None, help="bearer token for admin routes")

    return parser


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


def _cmd_serve(args: argparse.Namespace) -> int:
    config_data: dict[str, Any] = {}
    config_path = args.config or os.environ.get(_ENV_PREFIX + "CONFIG")
    if config_path:
        config_data = _load_config_file(config_path)
    args._config_data = config_data

    provider_name = _resolve("provider", args)
    try:
        provider = get_provider(provider_name)
    except ValueError as exc:
        print(f"sluice: error: {exc}", file=sys.stderr)
        return 2

    upstream = _resolve("upstream", args) or provider.default_base_url
    if not upstream:
        print("sluice: error: --upstream is required (flag, SLUICE_UPSTREAM env, or [serve] in config file)", file=sys.stderr)
        return 2

    listen = _resolve("listen", args)
    target = _resolve("target", args)
    poll_interval = _resolve("poll_interval", args)
    release_cooldown = _resolve("release_cooldown", args)
    queue_timeout = _resolve("queue_timeout", args)
    retry_interval = _resolve("retry_interval", args)
    drain_timeout = _resolve("drain_timeout", args)
    usage_key_env = _resolve("usage_key_env", args)
    usage_auth_header = _resolve("usage_auth_header", args)
    log_level = _resolve("log_level", args)
    resolved_level = getattr(logging, log_level.upper(), None) if isinstance(log_level, str) else None
    if not isinstance(resolved_level, int):
        log.warning("invalid log level %r — falling back to INFO", log_level)
        log_level = "INFO"
    else:
        log_level = log_level.upper()
    admin_token = _resolve("admin_token", args)
    reserve_raw = _resolve("reserve", args)
    history_size = _resolve("history_size", args)
    history_store_path = _resolve("history_store", args)
    history_ttl = _resolve("history_ttl", args)

    if history_store_path and history_ttl is not None and history_ttl <= 0:
        print("sluice: error: --history-ttl must be positive", file=sys.stderr)
        return 2

    if drain_timeout is not None and drain_timeout < 0:
        print("sluice: error: --drain-timeout must be >= 0", file=sys.stderr)
        return 2

    reserve_count = 0
    reserved_labels: set[str] = set()
    if reserve_raw:
        # Format: "label=count" (e.g. "interactive=1")
        if "=" not in reserve_raw:
            print(f"sluice: error: --reserve must be 'label=count', got '{reserve_raw}'", file=sys.stderr)
            return 2
        label, _, count_str = reserve_raw.rpartition("=")
        try:
            reserve_count = int(count_str)
        except ValueError:
            print(f"sluice: error: --reserve count must be an integer, got '{count_str}'", file=sys.stderr)
            return 2
        reserved_labels = {label}

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    usage_key = os.environ.get(usage_key_env) or ""

    if listen.startswith("["):
        host, _, port_str = listen.rpartition("]")
        host = host.lstrip("[")
        port_str = port_str.lstrip(":")
    else:
        host, _, port_str = listen.rpartition(":")
        host = host.strip("[]")
    if not host or not port_str:
        print(f"sluice: error: --listen must be host:port, got '{listen}'", file=sys.stderr)
        return 2
    port = int(port_str)

    guard: SingletonGuard
    guard_mode = _resolve("singleton_guard", args) or "noop"
    if guard_mode == "kube-lease":
        pod_name = os.environ.get("POD_NAME", "sluice")
        pod_ns = os.environ.get("POD_NAMESPACE", "default")
        guard = KubeLeaseGuard(
            lease_name="sluice",
            namespace=pod_ns,
            identity=pod_name,
        )
        log.info("  singleton_guard:   kube-lease (identity=%s, ns=%s)", pod_name, pod_ns)
    else:
        guard = NoopGuard()
        log.info("  singleton_guard:   noop")

    if provider.needs_usage_key and not usage_key:
        print(f"sluice: error: environment variable {usage_key_env} is not set", file=sys.stderr)
        print("       set it to the API key used for /v1/usage polling", file=sys.stderr)
        return 2

    adaptive_config = AdaptiveConfig(target=target) if provider.controller == "adaptive" else None

    truth_source = make_truth_source(
        provider,
        base_url=upstream,
        api_key=usage_key,
        auth_header=usage_auth_header,
        fresh_ttl=adaptive_config.fresh_ttl if adaptive_config is not None else AdaptiveConfig.fresh_ttl,
    )

    gate = PermitGate(
        initial_capacity=0,
        release_cooldown=release_cooldown,
        reserve=reserve_count,
    )
    history: History | None = None
    if history_size and history_size > 0:
        history = History(maxlen=history_size)
    history_store: SQLiteHistoryStore | None = None
    if history_store_path:
        history_store = SQLiteHistoryStore(history_store_path)
        if not history_store.is_available:
            log.warning("history store at %s failed to open — persistence disabled, in-memory buffer only", history_store_path)
        elif history is not None:
            warmed = history_store.load_recent(history_size)
            for entry in warmed:
                history.append(entry)
            if warmed:
                log.info("  history warmed:    %d entries from store", len(warmed))
        elif history is None:
            log.warning("  --history-store given but --history-size is 0 — store will persist but /history.json is disabled")
    reconcile = ReconciliationLoop(
        truth_source=truth_source,
        gate=gate,
        controller_config=ControllerConfig(target=target),
        breaker_config=BreakerConfig(),
        poll_interval=poll_interval,
        guard=guard,
        controller=provider.controller,
        adaptive_config=adaptive_config,
        history=history,
        history_store=history_store,
        history_ttl=history_ttl,
    )
    app = ProxyApp(
        upstream_base_url=upstream,
        gate=gate,
        reconcile=reconcile,
        queue_timeout=queue_timeout,
        guard=guard,
        admin_token=admin_token,
        retry_interval=retry_interval,
        reserved_labels=reserved_labels,
        drain_timeout=drain_timeout,
    )

    log.info("sluice %s starting", __version__)
    log.info("  upstream:          %s", upstream)
    log.info("  provider:          %s", provider.name)
    log.info("  controller:        %s", provider.controller)
    log.info("  listen:            %s:%d", host, port)
    log.info("  target:            %d", target)
    log.info("  poll_interval:     %.1fs", poll_interval)
    log.info("  release_cooldown:  %.1fs", release_cooldown)
    log.info("  queue_timeout:     %.1fs", queue_timeout)
    log.info("  retry_interval:    %.1fs", retry_interval)
    log.info("  drain_timeout:     %.1fs", drain_timeout)
    if provider.needs_usage_key:
        log.info("  usage_key_env:     %s", usage_key_env)
        log.info("  usage_auth_header: %s", usage_auth_header)
    if config_path:
        log.info("  config:            %s", config_path)
    if reserve_count > 0:
        log.info("  reserve:           %s=%d", next(iter(reserved_labels)), reserve_count)
    log.info("  history_size:      %s", history_size if history_size and history_size > 0 else "disabled")
    if history_store_path:
        log.info("  history_store:     %s", history_store_path)
        log.info("  history_ttl:       %.0fs", history_ttl)

    import uvicorn

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=log_level.lower(),
        timeout_graceful_shutdown=30,
    )
    return 0


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def _cmd_status(args: argparse.Namespace) -> int:
    headers: dict[str, str] = {}
    if args.admin_token:
        headers["Authorization"] = f"Bearer {args.admin_token}"
    try:
        with httpx.Client(timeout=10.0, headers=headers) as client:
            response = client.get(f"http://{args.host}/status.json")
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
    print(f"in_flight:          {d['local_in_flight']}")
    print(f"observed_sessions:  {d['concurrent_sessions']}")
    print(f"phantom_estimate:   {d.get('phantom_estimate', '?')}")
    print(f"cooling_down:       {d.get('cooling_down', '?')}")
    print(f"gate_closed_reason: {d.get('gate_closed_reason', '?')}")
    print(f"total_429s:         {d['total_429s']}")
    print(f"queue_depth:        {d['queue_depth']}")
    print(f"queue_wait:         {d.get('avg_wait_seconds', '?')}s avg / {d.get('p95_wait_seconds', '?')}s p95")
    print(f"queue_timeouts:     {d.get('queue_timeouts', '?')}")
    print(f"ready:              {d.get('ready', '?')}")
    config = d.get("config", {})
    if "target" in config:
        print(f"target:             {config['target']}")
    if "min_floor" in config:
        print(f"min_floor:          {config['min_floor']}")
    if "poll_interval" in config:
        print(f"poll_interval:      {config['poll_interval']}s")
    if "provider" in config:
        print(f"provider:           {config['provider']}")
    if "controller" in config:
        print(f"controller:         {config['controller']}")
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
