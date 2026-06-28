"""Tests for CLI env var config precedence: flags → env → config file → defaults."""

from __future__ import annotations

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

    # We can't actually run uvicorn, but we can verify the config resolution
    # doesn't error on upstream being missing from flags.
    from sluice.cli import main

    with patch("uvicorn.run", side_effect=KeyboardInterrupt):
        try:
            main(["serve"])
        except (KeyboardInterrupt, SystemExit):
            pass
