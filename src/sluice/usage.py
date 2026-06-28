"""Async client for the provider's ``/v1/usage`` endpoint.

Fetches and parses the usage payload into the controller's
:class:`~sluice.control.UsageReading`, with a last-known-good cache that serves
stale data on failure rather than inventing a zero.  Fail-safe: never widen the
gate on bad information.

The parser (:func:`parse_usage`) is a standalone pure function so it can be
tested without a network.  The :class:`UsageClient` wraps it with HTTP fetching
and LKG caching.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from sluice.control import UsageReading

log = logging.getLogger("sluice.usage")

_DEFAULT_LIMIT = 4
_DEFAULT_HARD_CAP = 8
_USAGE_PATH = "/v1/usage"
_HTTP_TIMEOUT = 30.0
_FAIL_SAFE_AGE = 99999.0  # effectively infinite staleness


class UsageParseError(Exception):
    """Raised when the usage payload cannot be parsed."""


# ---------------------------------------------------------------------------
# Parser (pure)
# ---------------------------------------------------------------------------


def _parse_iso_to_epoch(value: str | None) -> float | None:
    """Convert an ISO-8601 datetime string to epoch seconds, or *None*."""
    if value is None:
        return None
    dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def parse_usage(data: dict[str, object]) -> UsageReading:
    """Parse a ``/v1/usage`` JSON payload into a :class:`UsageReading`.

    Raises :class:`UsageParseError` if required fields are missing or malformed.
    The caller is expected to fall back to LKG on failure.
    """
    try:
        limits = data.get("limits")
        if not isinstance(limits, dict):
            limits = {}
        concurrency = limits.get("concurrency")
        if not isinstance(concurrency, dict):
            concurrency = {}
        limit = int(concurrency.get("limit", _DEFAULT_LIMIT))
        hard_cap = int(concurrency.get("hard_cap", _DEFAULT_HARD_CAP))

        usage = data.get("usage")
        if not isinstance(usage, dict):
            raise UsageParseError("usage section missing or not a dict")

        # concurrent_sessions is the single most important field — never
        # default to 0 (that would be failing open).
        cs_raw = usage.get("concurrent_sessions")
        if cs_raw is None:
            raise UsageParseError("concurrent_sessions missing from usage payload")
        concurrent_sessions = int(cs_raw)

        priority = usage.get("priority")
        if not isinstance(priority, dict):
            priority = {}
        priority_low = bool(priority.get("low", False))
        boxed_until_epoch = _parse_iso_to_epoch(priority.get("boxed_until"))
        resets_at_epoch = _parse_iso_to_epoch(priority.get("resets_at"))

        return UsageReading(
            concurrent_sessions=concurrent_sessions,
            limit=limit,
            hard_cap=hard_cap,
            priority_low=priority_low,
            boxed_until_epoch=boxed_until_epoch,
            resets_at_epoch=resets_at_epoch,
            age_seconds=0.0,
        )
    except UsageParseError:
        raise
    except (KeyError, ValueError, TypeError) as exc:
        raise UsageParseError(f"usage parse error: {type(exc).__name__}: {exc}") from exc


def fail_safe_reading(
    *, limit: int = _DEFAULT_LIMIT, hard_cap: int = _DEFAULT_HARD_CAP
) -> UsageReading:
    """Conservative reading for when no data is available.

    Assumes worst case: at the hard cap, deprioritised.  The caller sets the
    *fetched_at* timestamp to a very old value so the reconciliation loop's
    age computation applies the staleness penalty on top.
    """
    return UsageReading(
        concurrent_sessions=hard_cap,
        limit=limit,
        hard_cap=hard_cap,
        priority_low=True,
        boxed_until_epoch=None,
        resets_at_epoch=None,
        age_seconds=0.0,
    )


# ---------------------------------------------------------------------------
# Cached reading + async client
# ---------------------------------------------------------------------------


@dataclass
class CachedReading:
    """A usage reading paired with its fetch timestamp and success flag."""

    reading: UsageReading
    fetched_at_monotonic: float
    ok: bool  # True if the fetch succeeded; False if serving LKG / fail-safe


class UsageClient:
    """Async client for ``/v1/usage`` with last-known-good caching.

    On fetch failure, serves the LKG (marked ``ok=False``).  If no LKG exists,
    serves a :func:`fail_safe_reading` (conservative: assumes worst case).
    Never invents a zero ``concurrent_sessions``.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        auth_header: str = "authorization",
        timeout: float = _HTTP_TIMEOUT,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._url = base_url.rstrip("/") + _USAGE_PATH
        if auth_header.lower() == "x-api-key":
            self._headers: dict[str, str] = {
                "x-api-key": api_key,
                "Accept": "application/json",
            }
        else:
            self._headers = {
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
            }
        self._timeout = timeout
        self._transport = transport
        self._lkg: CachedReading | None = None
        self._client: httpx.AsyncClient | None = None

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                transport=self._transport,
            )
        return self._client

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        """Fetch ``/v1/usage``.  On failure: serve LKG or fail-safe, marked not ok."""
        try:
            client = await self._ensure_client()
            response = await client.get(self._url, headers=self._headers)
            response.raise_for_status()
            data = response.json()
            reading = parse_usage(data)
            cached = CachedReading(
                reading=reading,
                fetched_at_monotonic=now_monotonic,
                ok=True,
            )
            self._lkg = cached
            return cached
        except Exception as exc:
            log.warning("usage fetch failed: %s: %s", type(exc).__name__, exc)
            if self._lkg is not None:
                # Serve LKG — age (computed by caller) will reflect staleness.
                return CachedReading(
                    reading=self._lkg.reading,
                    fetched_at_monotonic=self._lkg.fetched_at_monotonic,
                    ok=False,
                )
            # No LKG — fail-safe: conservative reading, very old timestamp.
            reading = fail_safe_reading()
            cached = CachedReading(
                reading=reading,
                fetched_at_monotonic=now_monotonic - _FAIL_SAFE_AGE,
                ok=False,
            )
            self._lkg = cached
            return cached

    @property
    def last_cached(self) -> CachedReading | None:
        """The most recent cached reading (for status display)."""
        return self._lkg

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
