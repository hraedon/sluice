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
import socket
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
from sluice.trust import parse_trusted_proxies

log = logging.getLogger("sluice.cli")

_ENV_PREFIX = "SLUICE_"

# TCP keepalive probe cadence for client connections.  With the default
# idle of 60s, a silently-dead peer (e.g. an ungracefully rebooted host that
# never sent FIN/RST) is detected after roughly idle + intvl*cnt ≈ 120s, at
# which point the kernel resets the connection, uvicorn delivers
# ``http.disconnect``, and the proxy releases the orphaned permit.  Without
# keepalive the OS default is ~2h — long enough to pin the concurrency slot
# indefinitely for a single-operator proxy.
_TCP_KEEPALIVE_INTVL = 15  # seconds between probes once idle threshold is crossed
_TCP_KEEPALIVE_CNT = 4     # unacked probes before the connection is dropped


def _apply_keepalive(sock: socket.socket, *, idle: int) -> None:
    """Enable TCP keepalive on ``sock`` with an aggressive-ish cadence.

    Best-effort and portable: ``SO_KEEPALIVE`` is standard, but the timing
    knobs (``TCP_KEEPIDLE``/``TCP_KEEPINTVL``/``TCP_KEEPCNT``) are Linux
    names.  Platforms that lack a given option are skipped rather than
    failing the bind (macOS uses ``TCP_KEEPALIVE`` for the idle time;
    Windows tunes keepalive via a different ioctl not reached here).
    Options set on the listening socket are inherited by accepted
    connections on Linux, which is the deployment target.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    for name, value in (
        ("TCP_KEEPIDLE", idle),
        ("TCP_KEEPINTVL", _TCP_KEEPALIVE_INTVL),
        ("TCP_KEEPCNT", _TCP_KEEPALIVE_CNT),
    ):
        opt = getattr(socket, name, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, opt, value)
        except OSError as exc:  # pragma: no cover - platform-dependent
            log.warning("keepalive: could not set %s=%s: %s", name, value, exc)
    # macOS fallback: no TCP_KEEPIDLE, but TCP_KEEPALIVE carries the idle time.
    if not hasattr(socket, "TCP_KEEPIDLE") and hasattr(socket, "TCP_KEEPALIVE"):
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, idle)
        except OSError as exc:  # pragma: no cover - platform-dependent
            log.warning("keepalive: could not set TCP_KEEPALIVE=%s: %s", idle, exc)


def _bind_listen_socket(host: str, port: int, *, keepalive_idle: int) -> socket.socket:
    """Create and bind a listening socket with TCP keepalive enabled.

    Mirrors uvicorn's own ``Config.bind_socket`` (bind + ``SO_REUSEADDR`` +
    ``set_inheritable``; asyncio calls ``listen()`` when it serves the
    socket) and adds keepalive so the proxy notices dead clients.  The
    returned socket is handed to ``uvicorn.Server.run(sockets=[...])``.
    """
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    family, socktype, proto, _canon, sockaddr = infos[0]
    sock = socket.socket(family, socktype, proto)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    _apply_keepalive(sock, idle=keepalive_idle)
    sock.bind(sockaddr)
    sock.set_inheritable(True)
    return sock


class _JSONFormatter(logging.Formatter):
    """Minimal stdlib-only JSON log formatter (WI-028 finding 7).

    Emits one JSON object per log record with ``ts``, ``level``, ``logger``,
    ``msg``, and (when present) ``exc_info``.  No third-party deps — the
    pure-core / stdlib-only convention extends to the logging surface.  The
    ``extra`` dict on the log record (used by the 429 classifier) is merged
    into the top level so structured-log consumers can query fields like
    ``classification`` directly.
    """

    _RESERVED = {"name", "msg", "args", "levelname", "levelno", "pathname",
                  "filename", "module", "exc_info", "exc_text", "stack_info",
                  "lineno", "funcName", "created", "msecs", "relativeCreated",
                  "thread", "threadName", "processName", "process", "message",
                  "asctime", "taskName"}

    def format(self, record: logging.LogRecord) -> str:
        import json as _json
        import time as _time
        out: dict[str, object] = {
            "ts": _time.strftime("%Y-%m-%dT%H:%M:%S", _time.gmtime(record.created))
                  + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k not in self._RESERVED and not k.startswith("_"):
                out[k] = v
        if record.exc_info:
            out["exc_info"] = self.formatException(record.exc_info)
        return _json.dumps(out, default=str)

_DEFAULTS: dict[str, Any] = {
    "upstream": None,
    "listen": "127.0.0.1:8800",
    "target": 3,
    "poll_interval": 5.0,
    "poll_interval_idle": 30.0,
    "release_cooldown": 2.0,
    "queue_timeout": 30.0,
    "retry_interval": 10.0,
    "usage_key_env": "SLUICE_USAGE_KEY",
    "usage_auth_header": "authorization",
    "log_level": "INFO",
    "log_format": "text",
    "config": None,
    "admin_token": None,
    "reserve": None,
    "provider": "umans",
    "history_size": 2880,
    "history_store": None,
    "history_ttl": 604800.0,
    "singleton_guard": "noop",
    "drain_timeout": 25.0,
    "trusted_proxies": None,
    "max_request_body_bytes": None,
    "upstream_idle_timeout": None,
    "cors_allow_origin": None,
    "tcp_keepalive": True,
    "tcp_keepalive_idle": 60,
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
    if key in ("target", "history_size", "max_request_body_bytes", "tcp_keepalive_idle"):
        return int(env_val)
    if key in ("poll_interval", "release_cooldown", "queue_timeout", "retry_interval", "history_ttl", "drain_timeout", "upstream_idle_timeout", "poll_interval_idle"):
        return float(env_val)
    if key == "tcp_keepalive":
        return env_val.strip().lower() in ("1", "true", "yes", "on")
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
    serve.add_argument("--poll-interval-idle", type=float, default=None, help="seconds between polls when idle — no traffic, no 429s, normal band (default: 30, capped at usage_fresh_ttl*0.8; WI-022)")
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
    serve.add_argument("--log-format", default=None, choices=["text", "json"], help="logging format: text (human-readable, default) or json (structured, for log aggregators)")
    serve.add_argument("--config", default=None, help="path to TOML config file with a [serve] section")
    serve.add_argument("--admin-token", default=None, help="token gating admin routes (/, /status.json, /metrics, /history.json) — accepts a browser login-page session cookie, a Bearer header, or a Basic auth password")
    serve.add_argument("--reserve", default=None, help="reserve permits for a QoS class, e.g. 'interactive=1' (default: none → pure FIFO)")
    serve.add_argument("--history-size", type=int, default=None, help="number of tick snapshots to retain for trend analysis (default: 2880, ~4h at 5s poll; 0 disables)")
    serve.add_argument("--history-store", default=None, help="path to SQLite file for history persistence (default: none — in-memory only). Survives restarts; enables crash forensics.")
    serve.add_argument("--history-ttl", type=float, default=None, help="seconds to retain entries in the SQLite store before pruning (default: 604800 = 7 days)")
    serve.add_argument("--singleton-guard", default=None, choices=["noop", "kube-lease"], help="singleton guard mode (default: noop; env: SLUICE_SINGLETON_GUARD)")
    serve.add_argument("--drain-timeout", type=float, default=None, help="seconds to wait for in-flight requests on shutdown before closing upstream (default: 25)")
    serve.add_argument("--trusted-proxies", default=None, help="comma-separated CIDR/IP allowlist of peers trusted to set x-sluice-client-label and X-Forwarded-Proto (default: empty → loopback only). Set to the ingress CIDR in deployments. (WI-028)")
    serve.add_argument("--max-request-body-bytes", type=int, default=None, help="reject proxied requests whose body exceeds this many bytes (default: no limit — streaming). -1 disables the cap.")
    serve.add_argument("--upstream-idle-timeout", type=float, default=None, help="abort a streaming upstream response if no chunk arrives within this many seconds (default: no idle timeout — matches _UPSTREAM_TIMEOUT read=None). Resets on each chunk, so slow-but-steady streams are unaffected; only a silent upstream trips it.")
    serve.add_argument("--cors-allow-origin", default=None, help="emit Access-Control-Allow-Origin on admin routes with this value (e.g. '*' or 'https://grafana.example.com'). Default: none (same-origin only). (WI-028 finding 10)")
    serve.add_argument("--tcp-keepalive", action=argparse.BooleanOptionalAction, default=None, help="enable TCP keepalive on client connections so a client that dies ungracefully (e.g. a rebooted host) is detected and its permit released, instead of orphaning the concurrency slot (default: on)")
    serve.add_argument("--tcp-keepalive-idle", type=int, default=None, help="seconds a client connection may be idle before the first keepalive probe (default: 60; dead-peer detection ≈ idle + 60s)")

    # -- status --------------------------------------------------------------
    status = sub.add_parser("status", help="print current reading, computed permits, and band")
    status.add_argument("--host", default="127.0.0.1:8800", help="sluice listen address to query (default: 127.0.0.1:8800)")
    status.add_argument("--admin-token", default=None, help="bearer token for admin routes")

    return parser


# ---------------------------------------------------------------------------
# serve
# ---------------------------------------------------------------------------


class _ConfigError(Exception):
    """A serve-config resolution error; its message is the stderr body."""


def _build_serve_app(args: argparse.Namespace) -> tuple[ProxyApp, str, int, str]:
    """Resolve config and build the ASGI app plus bind params.

    Shared by the CLI ``serve`` command and the Windows service so both run an
    identical proxy. Raises :class:`_ConfigError` on invalid config; returns
    ``(app, host, port, log_level)`` with ``log_level`` already lower-cased.
    """
    config_data: dict[str, Any] = {}
    config_path = args.config or os.environ.get(_ENV_PREFIX + "CONFIG")
    if config_path:
        config_data = _load_config_file(config_path)
    args._config_data = config_data

    provider_name = _resolve("provider", args)
    try:
        provider = get_provider(provider_name)
    except ValueError as exc:
        raise _ConfigError(str(exc)) from exc

    upstream = _resolve("upstream", args) or provider.default_base_url
    if not upstream:
        raise _ConfigError(
            "--upstream is required (flag, SLUICE_UPSTREAM env, or [serve] in config file)"
        )

    listen = _resolve("listen", args)
    target = _resolve("target", args)
    poll_interval = _resolve("poll_interval", args)
    poll_interval_idle = _resolve("poll_interval_idle", args)
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
    trusted_proxies_raw = _resolve("trusted_proxies", args)
    max_request_body_bytes = _resolve("max_request_body_bytes", args)
    upstream_idle_timeout = _resolve("upstream_idle_timeout", args)
    cors_allow_origin = _resolve("cors_allow_origin", args)

    if max_request_body_bytes is not None and max_request_body_bytes == -1:
        max_request_body_bytes = None

    # 0 or -1 disables idle poll backoff (WI-022)
    if poll_interval_idle is not None and poll_interval_idle <= 0:
        poll_interval_idle = None

    try:
        trusted_proxies = parse_trusted_proxies(trusted_proxies_raw)
    except ValueError as exc:
        raise _ConfigError(f"--trusted-proxies: {exc}") from exc

    if history_store_path and history_ttl is not None and history_ttl <= 0:
        raise _ConfigError("--history-ttl must be positive")

    if drain_timeout is not None and drain_timeout < 0:
        raise _ConfigError("--drain-timeout must be >= 0")

    reserve_count = 0
    reserved_labels: set[str] = set()
    if reserve_raw:
        # Format: "label=count" (e.g. "interactive=1")
        if "=" not in reserve_raw:
            raise _ConfigError(f"--reserve must be 'label=count', got '{reserve_raw}'")
        label, _, count_str = reserve_raw.rpartition("=")
        try:
            reserve_count = int(count_str)
        except ValueError:
            raise _ConfigError(f"--reserve count must be an integer, got '{count_str}'")
        reserved_labels = {label}

    log_format = _resolve("log_format", args)
    if log_format not in ("text", "json"):
        log.warning("invalid log format %r — falling back to text", log_format)
        log_format = "text"

    if log_format == "json":
        handler = logging.StreamHandler()
        handler.setFormatter(_JSONFormatter())
        logging.basicConfig(level=getattr(logging, log_level), handlers=[handler])
    else:
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
        raise _ConfigError(f"--listen must be host:port, got '{listen}'")
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
        raise _ConfigError(
            f"environment variable {usage_key_env} is not set\n"
            "       set it to the API key used for /v1/usage polling"
        )

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
        poll_interval_idle=poll_interval_idle,
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
        trusted_proxies=trusted_proxies,
        max_request_body_bytes=max_request_body_bytes,
        upstream_idle_timeout=upstream_idle_timeout,
        cors_allow_origin=cors_allow_origin,
        usage_api_key=usage_key,
        usage_auth_header=usage_auth_header or "authorization",
    )
    app._config_path = config_path

    # SIGHUP handler: re-read config file and apply safe changes (WI-022 feature #3)
    # Registered in the ASGI lifespan startup via loop.add_signal_handler()
    # to avoid blocking the event loop with file I/O (H-1).
    if config_path:
        log.info("  config_reload:     SIGHUP (path=%s)", config_path)
    else:
        log.info("  config_reload:     disabled (no --config)")

    log.info("sluice %s starting", __version__)
    log.info("  upstream:          %s", upstream)
    log.info("  provider:          %s", provider.name)
    log.info("  controller:        %s", provider.controller)
    log.info("  listen:            %s:%d", host, port)
    log.info("  target:            %d", target)
    log.info("  poll_interval:     %.1fs", poll_interval)
    if poll_interval_idle is not None:
        log.info("  poll_interval_idle: %.1fs (WI-022)", poll_interval_idle)
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
    if trusted_proxies:
        log.info("  trusted_proxies:   %s", ", ".join(str(n) for n in trusted_proxies))
    else:
        log.info("  trusted_proxies:   loopback-only (QoS labels / XFP untrusted from non-loopback)")
    if max_request_body_bytes is not None:
        log.info("  max_request_body:  %d bytes", max_request_body_bytes)
    if upstream_idle_timeout is not None:
        log.info("  upstream_idle:     %.1fs", upstream_idle_timeout)
    if cors_allow_origin:
        log.info("  cors_allow_origin: %s", cors_allow_origin)
    log.info("  log_format:        %s", log_format)

    return app, host, port, log_level.lower()


def _cmd_serve(args: argparse.Namespace) -> int:
    try:
        app, host, port, log_level = _build_serve_app(args)
    except _ConfigError as exc:
        print(f"sluice: error: {exc}", file=sys.stderr)
        return 2

    import uvicorn

    # TCP keepalive on client connections: a client that dies ungracefully
    # (a rebooted host that never sends FIN/RST) is otherwise invisible, so
    # its in-flight streaming request pins the concurrency permit forever
    # (orphaned "local phantom").  Binding the listening socket ourselves lets
    # us set keepalive; uvicorn serves it via Server.run(sockets=...).
    keepalive = _resolve("tcp_keepalive", args)
    keepalive_idle = _resolve("tcp_keepalive_idle", args)
    sock: socket.socket | None = None
    if keepalive:
        try:
            sock = _bind_listen_socket(host, port, keepalive_idle=keepalive_idle)
            log.info("  tcp_keepalive:     on (idle=%ds, intvl=%ds, cnt=%d)",
                     keepalive_idle, _TCP_KEEPALIVE_INTVL, _TCP_KEEPALIVE_CNT)
        except OSError as exc:
            log.warning("keepalive socket bind failed (%s) — falling back to uvicorn's default bind", exc)
            sock = None
    else:
        log.info("  tcp_keepalive:     off")

    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level=log_level,
        timeout_graceful_shutdown=30,
    )
    server = uvicorn.Server(config)
    server.run(sockets=[sock] if sock is not None else None)
    return 0


def build_service_app() -> tuple[ProxyApp, str, int, str]:
    """Build the ASGI app + bind params for the in-process Windows service.

    Uses the same env/config resolution as ``sluice serve`` with no CLI flags:
    config comes from the ``SLUICE_CONFIG`` file and ``SLUICE_*`` env vars.
    """
    args = build_parser().parse_args(["serve"])
    return _build_serve_app(args)


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
    print(f"throughput:         {d.get('throughput', '?')} req/tick")
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
