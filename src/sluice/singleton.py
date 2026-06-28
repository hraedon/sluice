"""Singleton guard — ensures only one sluice instance admits traffic.

sluice's entire value is being the **one** shared choke point: the account-wide
concurrency invariant can only live in a single process holding a single semaphore.
A second sluice quietly serving traffic means two independent semaphores and a
silently blown account-wide cap.

The guard makes this invariant **mechanical, not organisational**: a second instance
that cannot acquire the claim refuses to admit requests and says why.

* :class:`NoopGuard` — default, always holds.  For local/dev runs with no cluster.
* :class:`KubeLeaseGuard` — Kubernetes ``coordination.k8s.io/v1`` Lease backend.
  Uses raw REST via ``httpx`` (already a dependency); no heavy ``kubernetes`` client.
"""

from __future__ import annotations

import abc
import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

log = logging.getLogger("sluice.singleton")

_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_CA_CERT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
_API_BASE = "https://kubernetes.default.svc"


# ---------------------------------------------------------------------------
# Abstract interface + no-op default
# ---------------------------------------------------------------------------


class SingletonGuard(abc.ABC):
    """Ensures only one sluice instance admits traffic at a time."""

    @abc.abstractmethod
    async def acquire(self) -> bool:
        """Claim the singleton.  Returns *True* if acquired, *False* if held by another."""

    @abc.abstractmethod
    async def renew(self) -> bool:
        """Renew the claim.  Returns *True* if still held, *False* if lost."""

    @abc.abstractmethod
    def is_held(self) -> bool:
        """Whether this instance currently holds the singleton claim."""

    @abc.abstractmethod
    async def release(self) -> None:
        """Release the claim."""

    async def start_renewer(self) -> None:
        """Start a background renewal task (no-op for guards that don't need it)."""

    async def stop_renewer(self) -> None:
        """Stop the background renewal task (no-op for guards that don't need it)."""


class NoopGuard(SingletonGuard):
    """Default guard — always holds.  For local/dev runs with no cluster."""

    async def acquire(self) -> bool:
        return True

    async def renew(self) -> bool:
        return True

    def is_held(self) -> bool:
        return True

    async def release(self) -> None:
        pass

    async def start_renewer(self) -> None:
        pass

    async def stop_renewer(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Kubernetes Lease backend
# ---------------------------------------------------------------------------


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000Z")


def _parse_rfc3339(value: str) -> float:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.timestamp()


def _is_expired(renew_time: str, lease_duration_seconds: int) -> bool:
    renew_epoch = _parse_rfc3339(renew_time)
    return time.time() > renew_epoch + lease_duration_seconds


class KubeLeaseGuard(SingletonGuard):
    """Kubernetes ``coordination.k8s.io/v1`` Lease-based singleton guard.

    Uses raw REST via :mod:`httpx` — no heavy ``kubernetes`` client dependency.
    The in-cluster service-account token and CA cert are read from the standard
    mounted paths.

    On startup ``acquire()`` creates or grabs the Lease (name *sluice*, in the
    pod's namespace), stamping ``holderIdentity`` with the pod name.  If the Lease
    is held and unexpired by another holder, ``acquire()`` fails → the pod refuses
    to serve.  A background renewer updates ``renewTime`` every ``renew_interval``;
    if renewal fails (lost the lease / API unreachable), ``is_held()`` flips false
    → the gate sheds.
    """

    def __init__(
        self,
        *,
        lease_name: str = "sluice",
        namespace: str = "default",
        identity: str = "sluice",
        lease_duration_seconds: int = 30,
        renew_interval: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._lease_name = lease_name
        self._namespace = namespace
        self._identity = identity
        self._lease_duration = lease_duration_seconds
        self._renew_interval = renew_interval
        self._held = False
        self._client = client
        self._owns_client = client is None
        self._renewer_task: asyncio.Task[None] | None = None

    @property
    def _base_url(self) -> str:
        return f"{_API_BASE}/apis/coordination.k8s.io/v1/namespaces/{self._namespace}/leases"

    @property
    def _lease_url(self) -> str:
        return f"{self._base_url}/{self._lease_name}"

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            token = Path(_TOKEN_PATH).read_text().strip()
            self._client = httpx.AsyncClient(
                base_url=_API_BASE,
                headers={"Authorization": f"Bearer {token}"},
                verify=_CA_CERT_PATH,
                timeout=10.0,
            )
            self._owns_client = True
        return self._client

    def _lease_body(self, now: str) -> dict[str, object]:
        return {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": self._lease_name, "namespace": self._namespace},
            "spec": {
                "holderIdentity": self._identity,
                "leaseDurationSeconds": self._lease_duration,
                "renewTime": now,
                "acquireTime": now,
            },
        }

    async def acquire(self) -> bool:
        client = await self._ensure_client()
        now = _now_rfc3339()

        try:
            resp = await client.get(self._lease_url)

            if resp.status_code == 404:
                resp = await client.post(self._base_url, json=self._lease_body(now))
                if resp.status_code in (200, 201):
                    self._held = True
                    log.info("KubeLeaseGuard: acquired lease (created)")
                    return True
                log.warning("KubeLeaseGuard: create failed: %d", resp.status_code)
                return False

            if resp.status_code == 200:
                existing = resp.json()
                spec = existing.get("spec", {})
                holder = spec.get("holderIdentity")

                if holder == self._identity:
                    self._held = True
                    log.info("KubeLeaseGuard: already held by us")
                    return True

                renew_time = spec.get("renewTime")
                lease_dur = int(spec.get("leaseDurationSeconds", self._lease_duration))

                if renew_time and _is_expired(renew_time, lease_dur):
                    existing["spec"]["holderIdentity"] = self._identity
                    existing["spec"]["renewTime"] = now
                    existing["spec"]["acquireTime"] = now
                    existing["spec"]["leaseDurationSeconds"] = self._lease_duration

                    resp = await client.put(self._lease_url, json=existing)
                    if resp.status_code == 200:
                        self._held = True
                        log.info("KubeLeaseGuard: acquired lease (expired peer)")
                        return True
                    log.warning("KubeLeaseGuard: take-over failed: %d", resp.status_code)
                    return False

                log.info("KubeLeaseGuard: lease held by %s, not expired", holder)
                self._held = False
                return False

            log.warning("KubeLeaseGuard: unexpected status %d", resp.status_code)
            self._held = False
            return False

        except Exception:
            log.warning("KubeLeaseGuard: acquire failed", exc_info=True)
            self._held = False
            return False

    async def renew(self) -> bool:
        if not self._held:
            return False

        client = await self._ensure_client()
        now = _now_rfc3339()

        try:
            resp = await client.get(self._lease_url)
            if resp.status_code != 200:
                log.warning("KubeLeaseGuard: renew GET failed: %d", resp.status_code)
                self._held = False
                return False

            lease = resp.json()
            if lease.get("spec", {}).get("holderIdentity") != self._identity:
                log.warning("KubeLeaseGuard: lease held by another during renew")
                self._held = False
                return False

            lease["spec"]["renewTime"] = now
            resp = await client.put(self._lease_url, json=lease)
            if resp.status_code != 200:
                log.warning("KubeLeaseGuard: renew PUT failed: %d", resp.status_code)
                self._held = False
                return False

            return True

        except Exception:
            log.warning("KubeLeaseGuard: renew failed", exc_info=True)
            self._held = False
            return False

    def is_held(self) -> bool:
        return self._held

    async def release(self) -> None:
        was_held = self._held
        self._held = False
        if not was_held:
            return
        if self._client is not None and not self._client.is_closed:
            try:
                # Check we still hold the lease before deleting — don't nuke
                # a new leader's lease if ours expired and was taken over.
                resp = await self._client.get(self._lease_url)
                if resp.status_code == 200:
                    lease = resp.json()
                    if lease.get("spec", {}).get("holderIdentity") == self._identity:
                        await self._client.delete(self._lease_url)
                    else:
                        log.info("KubeLeaseGuard: lease held by another — not deleting")
            except Exception:
                pass
            if self._owns_client:
                await self._client.aclose()

    async def start_renewer(self) -> None:
        if self._renewer_task is None:
            self._renewer_task = asyncio.create_task(self._renew_loop())

    async def stop_renewer(self) -> None:
        if self._renewer_task is not None:
            self._renewer_task.cancel()
            try:
                await self._renewer_task
            except asyncio.CancelledError:
                pass
            self._renewer_task = None

    async def _renew_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._renew_interval)
                await self.renew()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("KubeLeaseGuard: renew loop error", exc_info=True)
