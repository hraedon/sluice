"""Tests for graceful config reload (WI-022 feature #3)."""

from __future__ import annotations

import os
import tempfile

import httpx
import pytest

from sluice.control import BreakerConfig, ControllerConfig, UsageReading
from sluice.gate import PermitGate
from sluice.proxy import ProxyApp
from sluice.reconcile import ReconciliationLoop
from sluice.trust import parse_trusted_proxies
from sluice.usage import CachedReading


class FakeUsageClient:
    def __init__(self, reading: UsageReading) -> None:
        self._reading = reading

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        return CachedReading(reading=self._reading, fetched_at_monotonic=now_monotonic, ok=True)

    @property
    def last_cached(self) -> CachedReading | None:
        return None

    def record_response_headers(self, headers, status, *, now_monotonic) -> None:
        pass

    async def close(self) -> None:
        pass


def _make_app(**kwargs):
    client = FakeUsageClient(UsageReading(concurrent_sessions=0, limit=4, hard_cap=8))
    gate = PermitGate(initial_capacity=3)
    reconcile = ReconciliationLoop(
        truth_source=client,  # type: ignore[arg-type]
        gate=gate,
        controller_config=ControllerConfig(target=3),
        breaker_config=BreakerConfig(),
        poll_interval=5.0,
    )
    defaults = dict(
        upstream_base_url="https://upstream.example.com",
        gate=gate,
        reconcile=reconcile,
    )
    defaults.update(kwargs)
    app = ProxyApp(**defaults)
    return app, reconcile


def test_reload_config_changes_poll_interval():
    app, reconcile = _make_app()
    assert reconcile.poll_interval == 5.0
    changes = app.reload_config(poll_interval=10.0)
    assert reconcile.poll_interval == 10.0
    assert "poll_interval" in changes
    assert "5.0 -> 10.0" in changes["poll_interval"]


def test_reload_config_changes_poll_interval_idle():
    app, reconcile = _make_app()
    assert reconcile.poll_interval_idle is None
    changes = app.reload_config(poll_interval_idle=30.0)
    assert reconcile.poll_interval_idle == 30.0
    assert "poll_interval_idle" in changes


def test_reload_config_changes_queue_timeout():
    app, _ = _make_app()
    assert app._queue_timeout == 30.0
    changes = app.reload_config(queue_timeout=60.0)
    assert app._queue_timeout == 60.0
    assert "queue_timeout" in changes


def test_reload_config_changes_cors_allow_origin():
    app, _ = _make_app()
    assert app._cors_allow_origin is None
    changes = app.reload_config(cors_allow_origin="*")
    assert app._cors_allow_origin == "*"
    assert "cors_allow_origin" in changes


def test_reload_config_changes_trusted_proxies():
    app, _ = _make_app()
    assert app._trusted_proxies == frozenset()
    new_proxies = parse_trusted_proxies("10.0.0.0/8")
    changes = app.reload_config(trusted_proxies=new_proxies)
    assert app._trusted_proxies == new_proxies
    assert "trusted_proxies" in changes


def test_reload_config_changes_max_request_body_bytes():
    app, _ = _make_app()
    assert app._max_request_body_bytes is None
    changes = app.reload_config(max_request_body_bytes=1024)
    assert app._max_request_body_bytes == 1024
    assert "max_request_body_bytes" in changes


def test_reload_config_disables_max_request_body_with_zero():
    app, _ = _make_app()
    app.reload_config(max_request_body_bytes=1024)
    assert app._max_request_body_bytes == 1024
    app.reload_config(max_request_body_bytes=0)
    assert app._max_request_body_bytes is None


def test_reload_config_no_changes_returns_empty():
    app, reconcile = _make_app()
    changes = app.reload_config(poll_interval=5.0)  # same as current
    assert changes == {}


def test_reload_config_ignores_unknown_fields():
    app, _ = _make_app()
    changes = app.reload_config(upstream="https://different.example.com")
    assert changes == {}
    assert app._upstream == "https://upstream.example.com"


def test_reload_from_config_file():
    with tempfile.TemporaryDirectory() as tmp:
        config_path = os.path.join(tmp, "test.toml")
        with open(config_path, "w") as f:
            f.write("""\
[serve]
poll_interval = 10
queue_timeout = 60
cors_allow_origin = "*"
""")
        app, reconcile = _make_app()
        app._config_path = config_path

        assert reconcile.poll_interval == 5.0
        changes = app._reload_from_config()
        assert reconcile.poll_interval == 10.0
        assert app._queue_timeout == 60.0
        assert app._cors_allow_origin == "*"
        assert "poll_interval" in changes
        assert "queue_timeout" in changes
        assert "cors_allow_origin" in changes


def test_reload_from_config_no_file_raises():
    app, _ = _make_app()
    app._config_path = None
    with pytest.raises(ValueError, match="no config file"):
        app._reload_from_config()


def test_reload_from_config_missing_file_raises():
    app, _ = _make_app()
    app._config_path = "/nonexistent/path/to/config.toml"
    with pytest.raises(FileNotFoundError):
        app._reload_from_config()


def test_reload_from_config_no_changes():
    with tempfile.TemporaryDirectory() as tmp:
        config_path = os.path.join(tmp, "test.toml")
        with open(config_path, "w") as f:
            f.write("""\
[serve]
poll_interval = 5
""")
        app, reconcile = _make_app()
        app._config_path = config_path
        # poll_interval is already 5.0
        changes = app._reload_from_config()
        assert changes == {}


async def test_admin_reload_endpoint_no_config():
    """POST /admin/reload returns 400 when no config file was specified."""
    app, _ = _make_app(admin_token="secret")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post(
            "/admin/reload",
            headers={"Authorization": "Bearer secret", "Content-Type": "application/json"},
        )
    assert resp.status_code == 400
    assert "no config file" in resp.json()["error"]


async def test_admin_reload_endpoint_success():
    """POST /admin/reload re-reads config and returns changes."""
    with tempfile.TemporaryDirectory() as tmp:
        config_path = os.path.join(tmp, "test.toml")
        with open(config_path, "w") as f:
            f.write("""\
[serve]
poll_interval = 10
""")
        app, _ = _make_app(admin_token="secret")
        app._config_path = config_path

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post(
                "/admin/reload",
                headers={"Authorization": "Bearer secret", "Content-Type": "application/json"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["reloaded"] is True
        assert "poll_interval" in body["changes"]


async def test_admin_reload_endpoint_requires_auth():
    app, _ = _make_app(admin_token="secret")
    app._config_path = "/dev/null"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/admin/reload", headers={"Content-Type": "application/json"})
    assert resp.status_code == 403


async def test_admin_reload_endpoint_no_token_returns_405():
    app, _ = _make_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/admin/reload", headers={"Content-Type": "application/json"})
    assert resp.status_code == 405
