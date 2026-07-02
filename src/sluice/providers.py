"""Provider abstraction — truth sources, controller selection, and a registry.

Each provider (umans, Anthropic, OpenAI, generic) bundles:

* a :class:`TruthSource` — how the controller learns the provider's current
  limit state (polled, header-driven, or null);
* a controller strategy (``concurrency_reconcile`` or ``adaptive``);
* default base URL, auth header shape, and extra headers;
* a 429 classifier.

The registry is keyed by the ``--provider`` CLI flag.  An unknown provider
refuses to start (fail-safe, AGENTS.md rule 1).
"""

from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import httpx

from sluice.control import LimitState
from sluice.usage import CachedReading, UsageClient

log = logging.getLogger("sluice.providers")


# ---------------------------------------------------------------------------
# TruthSource protocol + implementations
# ---------------------------------------------------------------------------


@runtime_checkable
class TruthSource(Protocol):
    """How the controller learns the provider's current limit state."""

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        """Return the current reading (may be stale / fail-safe)."""
        ...

    @property
    def last_cached(self) -> CachedReading | None:
        """The most recent cached reading (for status display)."""
        ...

    async def close(self) -> None:
        """Release resources."""
        ...

    def record_response_headers(
        self, headers: dict[str, str], status: int, *, now_monotonic: float
    ) -> None:
        """Update truth from in-band response headers (header-driven only)."""
        ...


class PolledTruthSource:
    """Wraps :class:`UsageClient` — umans ``/v1/usage`` polling.

    Behaviour is identical to the pre-abstraction UsageClient: the poll is the
    truth, response headers are a no-op.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        auth_header: str = "authorization",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = UsageClient(
            base_url=base_url,
            api_key=api_key,
            auth_header=auth_header,
            transport=transport,
        )

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        return await self._client.fetch(now_monotonic=now_monotonic)

    @property
    def last_cached(self) -> CachedReading | None:
        return self._client.last_cached

    async def close(self) -> None:
        await self._client.close()

    def record_response_headers(
        self, headers: dict[str, str], status: int, *, now_monotonic: float
    ) -> None:
        pass  # the poll is the truth


class HeaderTruthSource:
    """Holds the latest :class:`LimitState` built from response ratelimit headers.

    Updated in-band by the proxy (via ``record_response_headers``); never polls.
    Like the umans cache, a header reading ages and, once stale, tightens.
    """

    def __init__(self, *, provider: str = "anthropic") -> None:
        self._provider = provider
        self._cached: CachedReading | None = None

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        if self._cached is None:
            # No headers seen yet — return a conservative but ok=True reading
            # so the proxy's ready gate opens and traffic can flow.  The AIMD
            # controller sees requests_remaining=0 → multiplicative decrease to
            # min_floor, which is fail-safe (tight, not closed).  Once real
            # response headers arrive via record_response_headers, the cached
            # state is replaced with the actual provider limits.
            #
            # This avoids the readiness deadlock: without ok=True, the proxy
            # fast-fails 503 before _forward runs, and record_response_headers
            # is never called.
            return CachedReading(
                reading=LimitState(
                    requests_remaining=0,
                    tokens_remaining=0,
                    provider=self._provider,
                    age_seconds=0.0,
                ),
                fetched_at_monotonic=now_monotonic,
                ok=True,
            )
        age = now_monotonic - self._cached.fetched_at_monotonic
        reading = dataclasses.replace(self._cached.reading, age_seconds=age)
        ok = age <= 15.0  # headers are considered ok if recent
        return CachedReading(
            reading=reading,
            fetched_at_monotonic=self._cached.fetched_at_monotonic,
            ok=ok,
        )

    @property
    def last_cached(self) -> CachedReading | None:
        return self._cached

    async def close(self) -> None:
        pass

    def record_response_headers(
        self, headers: dict[str, str], status: int, *, now_monotonic: float
    ) -> None:
        ls = parse_ratelimit_headers(headers, provider=self._provider)
        # Only mark ok=True if at least one ratelimit header was actually
        # parsed — otherwise a 500 with no ratelimit headers would produce
        # a fresh-timestamp ok=True reading, causing the AIMD controller to
        # additively increase permits during an upstream error (fail-open).
        has_data = (
            ls.requests_remaining is not None
            or ls.tokens_remaining is not None
        )
        if has_data:
            self._cached = CachedReading(
                reading=ls,
                fetched_at_monotonic=now_monotonic,
                ok=True,
            )
        elif self._cached is None:
            # First response with no ratelimit headers — keep the fail-safe
            # initial state (ok=True, remaining=0) rather than overwriting
            # with empty data.
            pass


class NullTruthSource:
    """No external truth — generic provider.

    Reflects only local in-flight + breaker.  The AIMD controller runs from
    local signals (429s, breaker state) without any provider-side reading.
    """

    def __init__(self, *, provider: str = "generic") -> None:
        self._provider = provider
        self._cached: CachedReading | None = None

    async def fetch(self, *, now_monotonic: float) -> CachedReading:
        # Always return a fresh reading — NullTruthSource has no external
        # data to age.  If we cached the first reading, its age would grow
        # unboundedly, causing the AIMD controller to treat it as stale and
        # permanently back off to min_floor.
        self._cached = CachedReading(
            reading=LimitState(provider=self._provider, age_seconds=0.0),
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
        pass  # null truth ignores headers


# ---------------------------------------------------------------------------
# Ratelimit header parsing (pure)
# ---------------------------------------------------------------------------

def _safe_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.strip())
    except (ValueError, TypeError):
        return None


def parse_ratelimit_headers(
    headers: dict[str, str], *, provider: str = "anthropic"
) -> LimitState:
    """Parse an allowlist of ratelimit headers into a :class:`LimitState`.

    Pure: no I/O, no clock.  ``now`` is not needed here — the caller stamps
    ``age_seconds`` and ``bucket_reset_epoch`` from the outside.  Only the
    headers in the allowlist are read; the body is never inspected.

    For Anthropic-style headers (``anthropic-ratelimit-*``):
        ``requests-remaining``, ``tokens-remaining``, etc.

    For OpenAI-style headers (``x-ratelimit-*``):
        ``remaining-requests``, ``remaining-tokens``, etc.

    Unrecognised / missing headers yield ``None`` — the controller treats
    missing data fail-safe (tightens, never widens).
    """
    # Normalise keys to lowercase for matching.
    h = {k.lower(): v for k, v in headers.items()}

    requests_limit = _safe_int(
        h.get("anthropic-ratelimit-requests-limit")
        or h.get("x-ratelimit-limit-requests")
    )
    requests_remaining = _safe_int(
        h.get("anthropic-ratelimit-requests-remaining")
        or h.get("x-ratelimit-remaining-requests")
    )
    tokens_limit = _safe_int(
        h.get("anthropic-ratelimit-tokens-limit")
        or h.get("x-ratelimit-limit-tokens")
    )
    tokens_remaining = _safe_int(
        h.get("anthropic-ratelimit-tokens-remaining")
        or h.get("x-ratelimit-remaining-tokens")
    )

    # The unified 40s window (Anthropic) is an alternative bucket shape.
    # If present, prefer it for requests_remaining.
    unified_remaining = _safe_int(
        h.get("anthropic-ratelimit-unified-40s-remaining")
    )
    if unified_remaining is not None and requests_remaining is None:
        requests_remaining = unified_remaining

    return LimitState(
        requests_limit=requests_limit,
        requests_remaining=requests_remaining,
        tokens_limit=tokens_limit,
        tokens_remaining=tokens_remaining,
        provider=provider,
        age_seconds=0.0,
    )


# ---------------------------------------------------------------------------
# 429 classifier
# ---------------------------------------------------------------------------


def default_429_classifier(retry_after: str | None) -> bool:
    """Default 429 classifier: a 429 without retry-after is a concurrency signal.

    Same logic as :func:`sluice.proxy._is_concurrency_429` but lives here so
    providers can override it.  Returns ``True`` when the 429 should be
    recorded in the breaker.
    """
    if retry_after is None:
        return True
    try:
        return int(retry_after.strip()) <= 0
    except (ValueError, TypeError):
        return True


# ---------------------------------------------------------------------------
# Provider bundle + registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Provider:
    """A provider's configuration bundle.

    Attributes:
        name: Provider identifier (``umans``, ``anthropic``, ``openai``, ``generic``).
        default_base_url: Default upstream base URL (overridable by ``--upstream``).
        auth_header: Auth header shape (``authorization`` or ``x-api-key``).
        auth_extra: Extra headers to send on usage polls (e.g. ``anthropic-version``).
        controller: Controller strategy (``concurrency_reconcile`` or ``adaptive``).
        needs_usage_key: Whether the provider requires a usage API key.
    """

    name: str
    default_base_url: str
    auth_header: str
    auth_extra: dict[str, str] = field(default_factory=dict)
    controller: str = "concurrency_reconcile"
    needs_usage_key: bool = True


_PROVIDERS: dict[str, Provider] = {
    "umans": Provider(
        name="umans",
        default_base_url="https://api.code.umans.ai",
        auth_header="authorization",
        auth_extra={},
        controller="concurrency_reconcile",
        needs_usage_key=True,
    ),
    "anthropic": Provider(
        name="anthropic",
        default_base_url="https://api.anthropic.com",
        auth_header="x-api-key",
        auth_extra={"anthropic-version": "2023-06-01"},
        controller="adaptive",
        needs_usage_key=False,
    ),
    "openai": Provider(
        name="openai",
        default_base_url="https://api.openai.com",
        auth_header="authorization",
        auth_extra={},
        controller="adaptive",
        needs_usage_key=False,
    ),
    "generic": Provider(
        name="generic",
        default_base_url="",
        auth_header="authorization",
        auth_extra={},
        controller="adaptive",
        needs_usage_key=False,
    ),
}


def get_provider(name: str) -> Provider:
    """Resolve a provider by name.  Raises ``ValueError`` if unknown."""
    if name not in _PROVIDERS:
        raise ValueError(
            f"unknown provider '{name}' — must be one of {sorted(_PROVIDERS)}"
        )
    return _PROVIDERS[name]


def make_truth_source(
    provider: Provider,
    *,
    base_url: str,
    api_key: str,
    auth_header: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> TruthSource:
    """Construct the appropriate :class:`TruthSource` for a provider.

    ``auth_header`` overrides the provider's default auth header shape (e.g.
    when the operator passes ``--usage-auth-header x-api-key`` for umans).
    """
    effective_auth_header = auth_header or provider.auth_header
    if provider.controller == "concurrency_reconcile":
        return PolledTruthSource(
            base_url=base_url,
            api_key=api_key,
            auth_header=effective_auth_header,
            transport=transport,
        )
    if provider.name == "generic":
        return NullTruthSource(provider=provider.name)
    return HeaderTruthSource(provider=provider.name)
