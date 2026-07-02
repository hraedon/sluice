"""Tests for the singleton guard: NoopGuard contract and KubeLeaseGuard behaviour."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from sluice.singleton import KubeLeaseGuard, NoopGuard, SingletonGuard


# ---------------------------------------------------------------------------
# NoopGuard
# ---------------------------------------------------------------------------


async def test_noop_always_held():
    guard = NoopGuard()
    assert guard.is_held() is True


async def test_noop_acquire_returns_true():
    guard = NoopGuard()
    assert await guard.acquire() is True
    assert guard.is_held() is True


async def test_noop_renew_returns_true():
    guard = NoopGuard()
    assert await guard.renew() is True


async def test_noop_release_does_not_clear():
    guard = NoopGuard()
    await guard.release()
    assert guard.is_held() is True


async def test_noop_renewer_noop():
    guard = NoopGuard()
    await guard.start_renewer()
    await guard.stop_renewer()
    assert guard.is_held() is True


def test_noop_is_singleton_guard():
    assert isinstance(NoopGuard(), SingletonGuard)


# ---------------------------------------------------------------------------
# KubeLeaseGuard — fake k8s API
# ---------------------------------------------------------------------------


def _rfc3339(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000000Z")


class FakeLeaseAPI:
    """In-memory simulator of the k8s Lease API."""

    def __init__(
        self,
        *,
        holder: str | None = None,
        renew_time: datetime | None = None,
        lease_duration: int = 30,
    ) -> None:
        self.lease: dict | None = None
        if holder is not None:
            self.lease = {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": "sluice", "namespace": "default"},
                "spec": {
                    "holderIdentity": holder,
                    "leaseDurationSeconds": lease_duration,
                    "renewTime": _rfc3339(renew_time or datetime.now(timezone.utc)),
                    "acquireTime": _rfc3339(renew_time or datetime.now(timezone.utc)),
                },
            }
        self.deleted = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        method = request.method

        if method == "GET" and url.endswith("/leases/sluice"):
            if self.lease is None:
                return httpx.Response(404)
            return httpx.Response(200, json=self.lease)

        if method == "POST" and url.endswith("/leases"):
            body = request.read()
            import json

            data = json.loads(body)
            self.lease = data
            return httpx.Response(201, json=data)

        if method == "PUT" and url.endswith("/leases/sluice"):
            body = request.read()
            import json

            data = json.loads(body)
            self.lease = data
            return httpx.Response(200, json=data)

        if method == "DELETE" and url.endswith("/leases/sluice"):
            self.lease = None
            self.deleted = True
            return httpx.Response(200)

        return httpx.Response(404)

    def make_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url="https://kubernetes.default.svc",
            transport=httpx.MockTransport(self.handler),
        )


def _make_guard(api: FakeLeaseAPI, identity: str = "pod-1") -> KubeLeaseGuard:
    return KubeLeaseGuard(
        lease_name="sluice",
        namespace="default",
        identity=identity,
        client=api.make_client(),
    )


async def test_kube_acquire_when_free():
    api = FakeLeaseAPI()  # no lease exists
    guard = _make_guard(api)
    assert await guard.acquire() is True
    assert guard.is_held() is True


async def test_kube_refuse_when_held_by_live_peer():
    now = datetime.now(timezone.utc)
    api = FakeLeaseAPI(holder="pod-2", renew_time=now, lease_duration=300)
    guard = _make_guard(api, identity="pod-1")
    assert await guard.acquire() is False
    assert guard.is_held() is False


async def test_kube_reacquire_after_peer_expired():
    old = datetime.now(timezone.utc) - timedelta(seconds=600)
    api = FakeLeaseAPI(holder="pod-2", renew_time=old, lease_duration=30)
    guard = _make_guard(api, identity="pod-1")
    assert await guard.acquire() is True
    assert guard.is_held() is True


async def test_kube_already_held_by_us():
    now = datetime.now(timezone.utc)
    api = FakeLeaseAPI(holder="pod-1", renew_time=now, lease_duration=30)
    guard = _make_guard(api, identity="pod-1")
    assert await guard.acquire() is True
    assert guard.is_held() is True


async def test_kube_renew_success():
    now = datetime.now(timezone.utc)
    api = FakeLeaseAPI(holder="pod-1", renew_time=now, lease_duration=30)
    guard = _make_guard(api, identity="pod-1")
    await guard.acquire()
    assert await guard.renew() is True
    assert guard.is_held() is True


async def test_kube_renew_failure_flips_held():
    """If renew PUT fails, is_held() flips to False."""
    now = datetime.now(timezone.utc)
    api = FakeLeaseAPI(holder="pod-1", renew_time=now, lease_duration=30)

    # Override PUT to return 409 (conflict).
    original_handler = api.handler

    def failing_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            return httpx.Response(409, json={"error": "conflict"})
        return original_handler(request)

    api.lease = api.lease  # keep existing lease
    guard = KubeLeaseGuard(
        lease_name="sluice",
        namespace="default",
        identity="pod-1",
        client=httpx.AsyncClient(
            base_url="https://kubernetes.default.svc",
            transport=httpx.MockTransport(failing_handler),
        ),
    )
    await guard.acquire()
    assert guard.is_held() is True

    # GET succeeds (returns our lease), but PUT fails.
    assert await guard.renew() is False
    assert guard.is_held() is False


async def test_kube_renew_held_by_another_flips_held():
    """If another pod grabbed the lease, renew detects it and flips."""
    now = datetime.now(timezone.utc)
    api = FakeLeaseAPI(holder="pod-1", renew_time=now, lease_duration=30)
    guard = _make_guard(api, identity="pod-1")
    await guard.acquire()

    # Another pod takes the lease.
    api.lease["spec"]["holderIdentity"] = "pod-2"

    assert await guard.renew() is False
    assert guard.is_held() is False


async def test_kube_release_deletes_lease():
    now = datetime.now(timezone.utc)
    api = FakeLeaseAPI(holder="pod-1", renew_time=now, lease_duration=30)
    guard = _make_guard(api, identity="pod-1")
    await guard.acquire()
    await guard.release()
    assert api.deleted is True
    assert guard.is_held() is False


async def test_kube_acquire_failure_on_api_error():
    """If the k8s API is unreachable, acquire fails safe (returns False)."""
    guard = KubeLeaseGuard(
        lease_name="sluice",
        namespace="default",
        identity="pod-1",
        client=httpx.AsyncClient(
            base_url="https://kubernetes.default.svc",
            transport=httpx.MockTransport(lambda req: httpx.Response(500)),
        ),
    )
    assert await guard.acquire() is False
    assert guard.is_held() is False


# ---------------------------------------------------------------------------
# WI-003: is_held() checks local lease expiry (split-brain prevention)
# ---------------------------------------------------------------------------


async def test_kube_is_held_false_after_local_lease_expiry():
    """is_held() returns False when the local lease has expired.

    If the event loop was blocked for > lease_duration, the lease may have been
    taken by another pod. is_held() must return False so the gate sheds traffic
    rather than serving as a split-brain second leader.
    """
    m = [1000.0]
    api = FakeLeaseAPI()
    guard = KubeLeaseGuard(
        lease_name="sluice",
        namespace="default",
        identity="pod-1",
        client=api.make_client(),
        lease_duration_seconds=30,
        renew_interval=10.0,
        monotonic_clock=lambda: m[0],
    )
    assert await guard.acquire() is True
    assert guard.is_held() is True

    m[0] = 1031.0  # 31s later → past lease_duration
    assert guard.is_held() is False

    assert await guard.renew() is True
    assert guard.is_held() is True


async def test_kube_is_held_true_at_exact_lease_duration():
    """is_held() returns True at exactly lease_duration (boundary is exclusive)."""
    m = [1000.0]
    api = FakeLeaseAPI()
    guard = KubeLeaseGuard(
        lease_name="sluice",
        namespace="default",
        identity="pod-1",
        client=api.make_client(),
        lease_duration_seconds=30,
        renew_interval=10.0,
        monotonic_clock=lambda: m[0],
    )
    await guard.acquire()
    m[0] = 1030.0  # exactly 30s later
    assert guard.is_held() is True


# ---------------------------------------------------------------------------
# WI-005: renew_interval must be < lease_duration_seconds
# ---------------------------------------------------------------------------


def test_kube_renew_interval_equal_lease_duration_rejected():
    """KubeLeaseGuard rejects renew_interval == lease_duration_seconds."""
    with pytest.raises(ValueError, match="renew_interval"):
        KubeLeaseGuard(
            lease_name="sluice",
            namespace="default",
            identity="pod-1",
            lease_duration_seconds=30,
            renew_interval=30.0,
        )


def test_kube_renew_interval_greater_than_lease_duration_rejected():
    """KubeLeaseGuard rejects renew_interval > lease_duration_seconds."""
    with pytest.raises(ValueError, match="renew_interval"):
        KubeLeaseGuard(
            lease_name="sluice",
            namespace="default",
            identity="pod-1",
            lease_duration_seconds=30,
            renew_interval=31.0,
        )


# ---------------------------------------------------------------------------
# Renew-loop recovery: re-acquire after renew failure flips _held back to True
# ---------------------------------------------------------------------------


async def test_kube_renew_loop_reacquires_after_loss():
    """When renew() fails and _held goes False, _renew_loop re-acquires."""
    now = datetime.now(timezone.utc)
    api = FakeLeaseAPI(holder="pod-1", renew_time=now, lease_duration=30)

    original_handler = api.handler
    put_failing = [True]

    def flaky_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT" and put_failing[0]:
            put_failing[0] = False
            return httpx.Response(409, json={"error": "conflict"})
        return original_handler(request)

    guard = KubeLeaseGuard(
        lease_name="sluice",
        namespace="default",
        identity="pod-1",
        renew_interval=0.01,
        client=httpx.AsyncClient(
            base_url="https://kubernetes.default.svc",
            transport=httpx.MockTransport(flaky_handler),
        ),
    )
    assert await guard.acquire() is True
    assert guard.is_held() is True

    await guard.start_renewer()
    await asyncio.sleep(0.05)

    # After the failing renew, _held should have gone False then re-acquired.
    assert guard.is_held() is True

    await guard.stop_renewer()
