"""Tests for CLI env var config precedence: flags → env → config file → defaults."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from sluice.cli import _resolve, _DEFAULTS


def _make_args(**overrides) -> object:
    """Create a Namespace-like object with None defaults for all serve args."""
    import argparse

    ns = argparse.Namespace()
    for key in _DEFAULTS:
        setattr(ns, key, None)
    ns._config_data = {}  # type: ignore[attr-defined]
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


def test_flag_overrides_env(monkeypatch):
    """A flag value takes precedence over env var."""
    monkeypatch.setenv("SLUICE_TARGET", "5")
    args = _make_args(target=7)
    assert _resolve("target", args) == 7


def test_env_overrides_default(monkeypatch):
    """An env var provides a value when the flag is not set."""
    monkeypatch.setenv("SLUICE_TARGET", "5")
    args = _make_args()
    assert _resolve("target", args) == 5


def test_env_coerced_to_int(monkeypatch):
    """Env var strings are coerced to the right type."""
    monkeypatch.setenv("SLUICE_TARGET", "6")
    args = _make_args()
    val = _resolve("target", args)
    assert val == 6
    assert isinstance(val, int)


def test_env_coerced_to_float(monkeypatch):
    monkeypatch.setenv("SLUICE_POLL_INTERVAL", "10.5")
    args = _make_args()
    val = _resolve("poll_interval", args)
    assert val == 10.5
    assert isinstance(val, float)


def test_env_string_passthrough(monkeypatch):
    monkeypatch.setenv("SLUICE_UPSTREAM", "https://api.example.com")
    args = _make_args()
    assert _resolve("upstream", args) == "https://api.example.com"


def test_built_in_default_when_nothing_set(monkeypatch):
    """Built-in default is used when neither flag nor env is set."""
    monkeypatch.delenv("SLUICE_LISTEN", raising=False)
    args = _make_args()
    assert _resolve("listen", args) == "127.0.0.1:8800"


def test_config_file_provides_default(monkeypatch, tmp_path):
    """Config file provides a value when neither flag nor env is set."""
    config_path = tmp_path / "sluice.toml"
    config_path.write_text('[serve]\ntarget = 2\n')

    monkeypatch.delenv("SLUICE_TARGET", raising=False)
    args = _make_args()
    args._config_data = _load_toml_for_test(str(config_path))  # type: ignore[attr-defined]
    assert _resolve("target", args) == 2


def test_env_overrides_config_file(monkeypatch, tmp_path):
    """Env var overrides config file value."""
    config_path = tmp_path / "sluice.toml"
    config_path.write_text('[serve]\ntarget = 2\n')

    monkeypatch.setenv("SLUICE_TARGET", "5")
    args = _make_args()
    args._config_data = _load_toml_for_test(str(config_path))  # type: ignore[attr-defined]
    assert _resolve("target", args) == 5


def test_flag_overrides_config_file(monkeypatch, tmp_path):
    """Flag overrides config file value."""
    config_path = tmp_path / "sluice.toml"
    config_path.write_text('[serve]\ntarget = 2\n')

    monkeypatch.delenv("SLUICE_TARGET", raising=False)
    args = _make_args(target=7)
    args._config_data = _load_toml_for_test(str(config_path))  # type: ignore[attr-defined]
    assert _resolve("target", args) == 7


def _load_toml_for_test(path: str) -> dict:
    from sluice.cli import _load_config_file

    return _load_config_file(path)


def test_serve_requires_upstream(monkeypatch, capsys):
    """Without upstream from any source, serve exits with code 2."""
    monkeypatch.delenv("SLUICE_UPSTREAM", raising=False)
    monkeypatch.delenv("SLUICE_CONFIG", raising=False)
    monkeypatch.delenv("SLUICE_USAGE_KEY", raising=False)

    from sluice.cli import main

    rc = main(["serve"])
    assert rc == 2


def test_serve_upstream_from_env(monkeypatch):
    """Upstream can be provided via env var."""
    monkeypatch.setenv("SLUICE_UPSTREAM", "https://api.example.com")
    monkeypatch.setenv("SLUICE_USAGE_KEY", "test-key")
    monkeypatch.delenv("SLUICE_CONFIG", raising=False)
    # Disable keepalive so the runner doesn't bind a real socket in the test.
    monkeypatch.setenv("SLUICE_TCP_KEEPALIVE", "false")

    # We can't actually run uvicorn, but we can verify the config resolution
    # doesn't error on upstream being missing from flags.
    from sluice.cli import main

    with patch("uvicorn.Server.run", side_effect=KeyboardInterrupt):
        try:
            main(["serve"])
        except (KeyboardInterrupt, SystemExit):
            pass


# ---------------------------------------------------------------------------
# SLUICE_LOG_LEVEL validation
# ---------------------------------------------------------------------------


def test_invalid_log_level_env_falls_back(monkeypatch):
    """An invalid SLUICE_LOG_LEVEL falls back to INFO instead of crashing."""
    monkeypatch.setenv("SLUICE_UPSTREAM", "https://api.example.com")
    monkeypatch.setenv("SLUICE_USAGE_KEY", "test-key")
    monkeypatch.setenv("SLUICE_LOG_LEVEL", "BOGUS")
    monkeypatch.delenv("SLUICE_CONFIG", raising=False)
    monkeypatch.setenv("SLUICE_TCP_KEEPALIVE", "false")

    from sluice.cli import main

    with patch("uvicorn.Server.run", side_effect=KeyboardInterrupt):
        try:
            main(["serve"])
        except (KeyboardInterrupt, SystemExit):
            pass


# ---------------------------------------------------------------------------
# --singleton-guard flag precedence
# ---------------------------------------------------------------------------


def test_singleton_guard_flag_overrides_env(monkeypatch):
    """The --singleton-guard flag takes precedence over the env var."""
    monkeypatch.setenv("SLUICE_SINGLETON_GUARD", "noop")

    args = _make_args(singleton_guard="kube-lease")
    assert _resolve("singleton_guard", args) == "kube-lease"


def test_singleton_guard_env_when_no_flag(monkeypatch):
    """The env var is used when the flag is not set."""
    monkeypatch.setenv("SLUICE_SINGLETON_GUARD", "kube-lease")

    args = _make_args()
    assert _resolve("singleton_guard", args) == "kube-lease"


def test_singleton_guard_default_when_nothing_set(monkeypatch):
    """The default is noop when neither flag nor env is set."""
    monkeypatch.delenv("SLUICE_SINGLETON_GUARD", raising=False)

    args = _make_args()
    assert _resolve("singleton_guard", args) == "noop"


# ---------------------------------------------------------------------------
# IPv6 listen address parsing
# ---------------------------------------------------------------------------


def test_parse_listen_ipv6_bracketed():
    """[::1]:8800 parses to host=::1, port=8800."""
    from sluice.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(["serve", "--listen", "[::1]:8800"])
    assert args.listen == "[::1]:8800"


def test_parse_listen_ipv4():
    """0.0.0.0:8800 still works."""
    from sluice.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(["serve", "--listen", "0.0.0.0:8800"])
    assert args.listen == "0.0.0.0:8800"


def test_listen_ipv6_bracketed_parses_host_port(monkeypatch):
    """_build_serve_app extracts host=::1 port=8800 from [::1]:8800."""
    monkeypatch.setenv("SLUICE_UPSTREAM", "https://api.example.com")
    monkeypatch.setenv("SLUICE_USAGE_KEY", "test-key")
    monkeypatch.delenv("SLUICE_CONFIG", raising=False)

    from sluice.cli import _build_serve_app, build_parser

    args = build_parser().parse_args(["serve", "--listen", "[::1]:8800"])
    _app, host, port, _log = _build_serve_app(args)

    assert host == "::1"
    assert port == 8800


def test_listen_ipv4_parses_host_port(monkeypatch):
    """_build_serve_app extracts host=0.0.0.0 port=8800 from 0.0.0.0:8800."""
    monkeypatch.setenv("SLUICE_UPSTREAM", "https://api.example.com")
    monkeypatch.setenv("SLUICE_USAGE_KEY", "test-key")
    monkeypatch.delenv("SLUICE_CONFIG", raising=False)

    from sluice.cli import _build_serve_app, build_parser

    args = build_parser().parse_args(["serve", "--listen", "0.0.0.0:8800"])
    _app, host, port, _log = _build_serve_app(args)

    assert host == "0.0.0.0"
    assert port == 8800


# ---------------------------------------------------------------------------
# New security-hardening flags (WI-028)
# ---------------------------------------------------------------------------


def test_trusted_proxies_flag_parsed():
    """--trusted-proxies is accepted by the parser."""
    from sluice.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["serve", "--trusted-proxies", "10.0.0.0/8,127.0.0.1"])
    assert args.trusted_proxies == "10.0.0.0/8,127.0.0.1"


def test_trusted_proxies_env(monkeypatch):
    """SLUICE_TRUSTED_PROXIES env var is resolved when no flag is set."""
    monkeypatch.setenv("SLUICE_TRUSTED_PROXIES", "10.0.0.0/8")
    args = _make_args()
    assert _resolve("trusted_proxies", args) == "10.0.0.0/8"


def test_trusted_proxies_default_is_none(monkeypatch):
    """The default is None (→ empty allowlist → loopback-only)."""
    monkeypatch.delenv("SLUICE_TRUSTED_PROXIES", raising=False)
    args = _make_args()
    assert _resolve("trusted_proxies", args) is None


def test_max_request_body_bytes_flag_parsed():
    from sluice.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["serve", "--max-request-body-bytes", "65536"])
    assert args.max_request_body_bytes == 65536


def test_upstream_idle_timeout_flag_parsed():
    from sluice.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["serve", "--upstream-idle-timeout", "60"])
    assert args.upstream_idle_timeout == 60.0


def test_cors_allow_origin_flag_parsed():
    from sluice.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["serve", "--cors-allow-origin", "*"])
    assert args.cors_allow_origin == "*"


def test_log_format_flag_parsed():
    from sluice.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["serve", "--log-format", "json"])
    assert args.log_format == "json"


def test_log_format_default_is_text(monkeypatch):
    monkeypatch.delenv("SLUICE_LOG_FORMAT", raising=False)
    args = _make_args()
    assert _resolve("log_format", args) == "text"


def test_serve_with_trusted_proxies_bad_cidr_exits_2(monkeypatch, capsys):
    """An invalid CIDR in --trusted-proxies causes serve to exit with code 2."""
    monkeypatch.setenv("SLUICE_UPSTREAM", "https://api.example.com")
    monkeypatch.setenv("SLUICE_USAGE_KEY", "test-key")
    monkeypatch.delenv("SLUICE_CONFIG", raising=False)

    from sluice.cli import main

    rc = main(["serve", "--trusted-proxies", "not-a-cidr"])
    assert rc == 2
    assert "trusted-proxies" in capsys.readouterr().err.lower()


def test_max_request_body_bytes_env_coerced_to_int(monkeypatch):
    """SLUICE_MAX_REQUEST_BODY_BYTES env var is coerced to int."""
    monkeypatch.setenv("SLUICE_MAX_REQUEST_BODY_BYTES", "65536")
    args = _make_args()
    val = _resolve("max_request_body_bytes", args)
    assert val == 65536
    assert isinstance(val, int)


def test_upstream_idle_timeout_env_coerced_to_float(monkeypatch):
    """SLUICE_UPSTREAM_IDLE_TIMEOUT env var is coerced to float."""
    monkeypatch.setenv("SLUICE_UPSTREAM_IDLE_TIMEOUT", "60")
    args = _make_args()
    val = _resolve("upstream_idle_timeout", args)
    assert val == 60.0
    assert isinstance(val, float)


def test_cors_allow_origin_env_passthrough(monkeypatch):
    """SLUICE_CORS_ALLOW_ORIGIN env var is resolved as a string (no coercion)."""
    monkeypatch.setenv("SLUICE_CORS_ALLOW_ORIGIN", "https://grafana.example.com")
    args = _make_args()
    assert _resolve("cors_allow_origin", args) == "https://grafana.example.com"


# ---------------------------------------------------------------------------
# TCP keepalive (orphaned-permit fix): dead clients must be detectable
# ---------------------------------------------------------------------------


def test_tcp_keepalive_defaults_on():
    """Keepalive is on by default so ungracefully-dropped clients are noticed."""
    args = _make_args()
    assert _resolve("tcp_keepalive", args) is True
    assert _resolve("tcp_keepalive_idle", args) == 60


def test_tcp_keepalive_env_disables(monkeypatch):
    """SLUICE_TCP_KEEPALIVE=false turns keepalive off."""
    monkeypatch.setenv("SLUICE_TCP_KEEPALIVE", "false")
    args = _make_args()
    assert _resolve("tcp_keepalive", args) is False


def test_tcp_keepalive_idle_env_coerced_to_int(monkeypatch):
    """SLUICE_TCP_KEEPALIVE_IDLE is coerced to int seconds."""
    monkeypatch.setenv("SLUICE_TCP_KEEPALIVE_IDLE", "30")
    args = _make_args()
    val = _resolve("tcp_keepalive_idle", args)
    assert val == 30
    assert isinstance(val, int)


def test_no_tcp_keepalive_flag_overrides_default():
    """The --no-tcp-keepalive flag resolves to False (BooleanOptionalAction)."""
    from sluice.cli import build_parser

    args = build_parser().parse_args(["serve", "--no-tcp-keepalive"])
    assert _resolve("tcp_keepalive", args) is False


def test_bind_listen_socket_enables_keepalive():
    """_bind_listen_socket returns a bound socket with SO_KEEPALIVE set.

    Binds to an ephemeral port (0) to avoid collisions.  On Linux the
    TCP_KEEPIDLE knob is also applied and readable back.
    """
    import socket as _socket

    from sluice.cli import _bind_listen_socket

    sock = _bind_listen_socket("127.0.0.1", 0, keepalive_idle=42)
    try:
        assert sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_KEEPALIVE) == 1
        if hasattr(_socket, "TCP_KEEPIDLE"):
            assert sock.getsockopt(_socket.IPPROTO_TCP, _socket.TCP_KEEPIDLE) == 42
        # It is bound (has a concrete port) but listen() is left to asyncio.
        assert sock.getsockname()[1] != 0
    finally:
        sock.close()


# ---------------------------------------------------------------------------
# Version consistency: __version__ must match pyproject.toml when installed
# ---------------------------------------------------------------------------


def test_version_matches_pyproject():
    """__version__ resolved from package metadata matches pyproject.toml.

    Guards against the drift that motivated PR #19 (the hardcoded string
    in __init__.py lagged behind pyproject.toml by a full minor version).
    """
    import tomllib

    from sluice import __version__

    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with open(pyproject, "rb") as f:
        data = tomllib.load(f)
    assert __version__ == data["project"]["version"]
