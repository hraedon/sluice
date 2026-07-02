"""Tests for the provider abstraction: truth sources, registry, header parsing.

Plan 006 WI-002 + WI-007.
"""

from __future__ import annotations

import pytest

from sluice.providers import (
    HeaderTruthSource,
    NullTruthSource,
    PolledTruthSource,
    TruthSource,
    get_provider,
    make_truth_source,
    parse_ratelimit_headers,
)


# ---------------------------------------------------------------------------
# Provider registry (WI-002)
# ---------------------------------------------------------------------------


def test_registry_resolves_umans():
    p = get_provider("umans")
    assert p.name == "umans"
    assert p.controller == "concurrency_reconcile"
    assert p.auth_header == "authorization"
    assert p.needs_usage_key is True


def test_registry_resolves_anthropic():
    p = get_provider("anthropic")
    assert p.name == "anthropic"
    assert p.controller == "adaptive"
    assert p.auth_header == "x-api-key"
    assert p.auth_extra.get("anthropic-version") == "2023-06-01"
    assert p.needs_usage_key is False


def test_registry_resolves_openai():
    p = get_provider("openai")
    assert p.name == "openai"
    assert p.controller == "adaptive"
    assert p.auth_header == "authorization"
    assert p.needs_usage_key is False


def test_registry_resolves_generic():
    p = get_provider("generic")
    assert p.name == "generic"
    assert p.controller == "adaptive"
    assert p.needs_usage_key is False


def test_registry_unknown_provider_raises():
    with pytest.raises(ValueError, match="unknown provider"):
        get_provider("nonexistent")


def test_all_providers_in_registry():
    for name in ("umans", "anthropic", "openai", "generic"):
        p = get_provider(name)
        assert p.name == name


# ---------------------------------------------------------------------------
# Header parsing (WI-002 / WI-007)
# ---------------------------------------------------------------------------


def test_parse_anthropic_headers():
    headers = {
        "anthropic-ratelimit-requests-remaining": "100",
        "anthropic-ratelimit-tokens-remaining": "50000",
    }
    ls = parse_ratelimit_headers(headers, provider="anthropic")
    assert ls.requests_remaining == 100
    assert ls.tokens_remaining == 50000
    assert ls.provider == "anthropic"


def test_parse_openai_headers():
    headers = {
        "x-ratelimit-remaining-requests": "50",
        "x-ratelimit-remaining-tokens": "25000",
    }
    ls = parse_ratelimit_headers(headers, provider="openai")
    assert ls.requests_remaining == 50
    assert ls.tokens_remaining == 25000
    assert ls.provider == "openai"


def test_parse_empty_headers():
    ls = parse_ratelimit_headers({}, provider="anthropic")
    assert ls.requests_remaining is None
    assert ls.tokens_remaining is None


def test_parse_malformed_headers():
    headers = {
        "anthropic-ratelimit-requests-remaining": "not-a-number",
        "x-ratelimit-remaining-tokens": "",
    }
    ls = parse_ratelimit_headers(headers, provider="anthropic")
    assert ls.requests_remaining is None
    assert ls.tokens_remaining is None


def test_parse_unified_40s_window():
    headers = {
        "anthropic-ratelimit-unified-40s-remaining": "42",
    }
    ls = parse_ratelimit_headers(headers, provider="anthropic")
    assert ls.requests_remaining == 42


def test_parse_prefers_explicit_remaining_over_unified():
    headers = {
        "anthropic-ratelimit-requests-remaining": "100",
        "anthropic-ratelimit-unified-40s-remaining": "42",
    }
    ls = parse_ratelimit_headers(headers, provider="anthropic")
    assert ls.requests_remaining == 100


def test_parse_anthropic_prefers_over_openai_style():
    """If both Anthropic and OpenAI style headers are present, Anthropic wins."""
    headers = {
        "anthropic-ratelimit-requests-remaining": "100",
        "x-ratelimit-remaining-requests": "50",
    }
    ls = parse_ratelimit_headers(headers, provider="anthropic")
    assert ls.requests_remaining == 100


# ---------------------------------------------------------------------------
# HeaderTruthSource (WI-002 / WI-007)
# ---------------------------------------------------------------------------


async def test_header_truth_source_no_data_is_conservative():
    """Before any response headers are seen, the truth source returns a
    conservative but ok=True reading so the proxy's ready gate opens.

    The AIMD controller sees requests_remaining=0 → multiplicative decrease
    to min_floor (fail-safe: tight, not closed).  Once real headers arrive,
    the cached state is replaced with actual provider limits.
    """
    ts = HeaderTruthSource(provider="anthropic")
    cached = await ts.fetch(now_monotonic=1000.0)
    assert cached.ok is True  # ready gate must open so traffic can flow
    assert cached.reading.requests_remaining == 0  # conservative
    assert cached.reading.tokens_remaining == 0
    assert cached.reading.provider == "anthropic"


async def test_header_truth_source_records_and_serves():
    """After record_response_headers, fetch returns the recorded state."""
    ts = HeaderTruthSource(provider="anthropic")
    ts.record_response_headers(
        {"anthropic-ratelimit-requests-remaining": "100",
         "anthropic-ratelimit-tokens-remaining": "50000"},
        status=200,
        now_monotonic=1000.0,
    )
    cached = await ts.fetch(now_monotonic=1000.0)
    assert cached.ok is True
    assert cached.reading.requests_remaining == 100
    assert cached.reading.tokens_remaining == 50000
    assert cached.reading.age_seconds == 0.0


async def test_header_truth_source_ages():
    """A header reading ages; once stale, ok=False."""
    ts = HeaderTruthSource(provider="anthropic")
    ts.record_response_headers(
        {"anthropic-ratelimit-requests-remaining": "100"},
        status=200,
        now_monotonic=1000.0,
    )
    # Fresh.
    cached = await ts.fetch(now_monotonic=1001.0)
    assert cached.ok is True
    assert cached.reading.age_seconds == 1.0

    # Stale.
    cached = await ts.fetch(now_monotonic=1050.0)
    assert cached.ok is False
    assert cached.reading.age_seconds == 50.0
    # Still has the data, just marked stale.
    assert cached.reading.requests_remaining == 100


async def test_header_truth_source_last_cached():
    """last_cached reflects the most recent recording."""
    ts = HeaderTruthSource(provider="anthropic")
    assert ts.last_cached is None

    ts.record_response_headers(
        {"anthropic-ratelimit-requests-remaining": "50"},
        status=200,
        now_monotonic=500.0,
    )
    assert ts.last_cached is not None
    assert ts.last_cached.reading.requests_remaining == 50


async def test_header_truth_source_updates_on_new_response():
    """Each response updates the cached state."""
    ts = HeaderTruthSource(provider="anthropic")
    ts.record_response_headers(
        {"anthropic-ratelimit-requests-remaining": "100"},
        status=200,
        now_monotonic=1000.0,
    )
    ts.record_response_headers(
        {"anthropic-ratelimit-requests-remaining": "80"},
        status=200,
        now_monotonic=1005.0,
    )
    cached = await ts.fetch(now_monotonic=1005.0)
    assert cached.reading.requests_remaining == 80


async def test_header_truth_source_close():
    """close() is a no-op that doesn't raise."""
    ts = HeaderTruthSource(provider="anthropic")
    await ts.close()


# ---------------------------------------------------------------------------
# NullTruthSource (WI-002 / WI-007)
# ---------------------------------------------------------------------------


async def test_null_truth_source_returns_minimal_state():
    """NullTruthSource returns a minimal state with no external data."""
    ts = NullTruthSource(provider="generic")
    cached = await ts.fetch(now_monotonic=1000.0)
    assert cached.ok is True
    assert cached.reading.provider == "generic"
    assert cached.reading.requests_remaining is None
    assert cached.reading.tokens_remaining is None


async def test_null_truth_source_ignores_headers():
    """NullTruthSource ignores response headers (no-op)."""
    ts = NullTruthSource(provider="generic")
    ts.record_response_headers(
        {"anthropic-ratelimit-requests-remaining": "100"},
        status=200,
        now_monotonic=1000.0,
    )
    cached = await ts.fetch(now_monotonic=1000.0)
    assert cached.reading.requests_remaining is None


async def test_null_truth_source_last_cached():
    ts = NullTruthSource(provider="generic")
    assert ts.last_cached is None
    await ts.fetch(now_monotonic=1000.0)
    assert ts.last_cached is not None


async def test_null_truth_source_close():
    ts = NullTruthSource(provider="generic")
    await ts.close()


# ---------------------------------------------------------------------------
# PolledTruthSource (WI-002 — wraps UsageClient)
# ---------------------------------------------------------------------------


def test_polled_truth_source_is_truth_source():
    """PolledTruthSource satisfies the TruthSource protocol."""
    import httpx
    ts = PolledTruthSource(
        base_url="https://api.example.com",
        api_key="test-key",
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json={})),
    )
    assert isinstance(ts, TruthSource)


# ---------------------------------------------------------------------------
# make_truth_source (WI-002)
# ---------------------------------------------------------------------------


def test_make_truth_source_umans():
    import httpx
    p = get_provider("umans")
    ts = make_truth_source(
        p,
        base_url="https://api.example.com",
        api_key="test-key",
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json={})),
    )
    assert isinstance(ts, PolledTruthSource)


def test_make_truth_source_anthropic():
    p = get_provider("anthropic")
    ts = make_truth_source(p, base_url="https://api.anthropic.com", api_key="")
    assert isinstance(ts, HeaderTruthSource)


def test_make_truth_source_openai():
    p = get_provider("openai")
    ts = make_truth_source(p, base_url="https://api.openai.com", api_key="")
    assert isinstance(ts, HeaderTruthSource)


def test_make_truth_source_generic():
    p = get_provider("generic")
    ts = make_truth_source(p, base_url="https://localhost:8080", api_key="")
    assert isinstance(ts, NullTruthSource)
