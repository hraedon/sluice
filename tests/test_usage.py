"""Tests for the usage client: parser, fail-safe, LKG caching."""

from __future__ import annotations

import httpx
import pytest

from sluice.usage import (
    UsageClient,
    UsageParseError,
    fail_safe_reading,
    parse_usage,
)

# Representative /v1/usage payload (matches docs/concurrency-model.md §1).
SAMPLE_PAYLOAD: dict[str, object] = {
    "limits": {
        "concurrency": {"limit": 4, "hard_cap": 8, "burst_pct": 1.0},
        "requests": {"limit": 200, "hard_cap": 400, "window_seconds": 18000},
    },
    "usage": {
        "concurrent_sessions": 1,
        "requests_in_window": 48,
        "remaining_requests": 152,
        "priority": {"low": False, "boxed_until": None, "reason": None},
    },
}


# --- parse_usage -----------------------------------------------------------


def test_parse_basic():
    r = parse_usage(SAMPLE_PAYLOAD)
    assert r.concurrent_sessions == 1
    assert r.limit == 4
    assert r.hard_cap == 8
    assert r.priority_low is False
    assert r.boxed_until_epoch is None
    assert r.age_seconds == 0.0


def test_parse_request_window_fields():
    r = parse_usage(SAMPLE_PAYLOAD)
    assert r.requests_limit == 200
    assert r.requests_hard_cap == 400
    assert r.requests_window_seconds == 18000
    assert r.requests_in_window == 48
    assert r.requests_remaining == 152


def test_parse_request_window_absent():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 1,
            "priority": {"low": False, "boxed_until": None},
        },
    }
    r = parse_usage(payload)
    assert r.requests_limit is None
    assert r.requests_hard_cap is None
    assert r.requests_window_seconds is None
    assert r.requests_in_window is None
    assert r.requests_remaining is None


def test_parse_priority_low():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 5,
            "priority": {"low": True, "boxed_until": None, "reason": "concurrency"},
        },
    }
    r = parse_usage(payload)
    assert r.priority_low is True
    assert r.concurrent_sessions == 5


def test_parse_boxed_until():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 0,
            "priority": {"low": False, "boxed_until": "2025-06-27T12:00:00Z", "reason": "boxed"},
        },
    }
    r = parse_usage(payload)
    assert r.boxed_until_epoch is not None
    assert r.boxed_until_epoch > 0
    assert r.priority_low is False
    assert r.priority_reason == "boxed"


def test_parse_priority_reason_rate_limited():
    # The deprioritization rung as captured live 2026-07-03.
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 0,
            "priority": {
                "low": True,
                "boxed_until": "2026-07-03T12:23:04.763728+00:00",
                "reason": "rate_limited",
            },
        },
    }
    r = parse_usage(payload)
    assert r.priority_reason == "rate_limited"
    assert r.boxed_until_epoch is not None


def test_parse_priority_reason_non_string_is_none():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 0,
            "priority": {"low": False, "boxed_until": None, "reason": 7},
        },
    }
    assert parse_usage(payload).priority_reason is None


def test_parse_priority_reason_missing_is_none():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 0,
            "priority": {"low": False, "boxed_until": None},
        },
    }
    assert parse_usage(payload).priority_reason is None


def test_parse_resets_at():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 0,
            "priority": {
                "low": True,
                "boxed_until": "2025-06-27T12:00:00Z",
                "resets_at": "2025-06-27T17:00:00Z",
                "reason": "boxed",
            },
        },
    }
    r = parse_usage(payload)
    assert r.boxed_until_epoch is not None
    assert r.resets_at_epoch is not None
    assert r.resets_at_epoch > r.boxed_until_epoch
    assert r.priority_low is True


def test_parse_resets_at_absent():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 1,
            "priority": {"low": False, "boxed_until": None},
        },
    }
    r = parse_usage(payload)
    assert r.resets_at_epoch is None


def test_parse_missing_concurrent_sessions_raises():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {"priority": {"low": False, "boxed_until": None}},
    }
    with pytest.raises(UsageParseError, match="concurrent_sessions"):
        parse_usage(payload)


def test_parse_missing_limits_uses_defaults():
    payload: dict[str, object] = {
        "usage": {
            "concurrent_sessions": 2,
            "priority": {"low": False, "boxed_until": None},
        },
    }
    r = parse_usage(payload)
    assert r.limit == 4
    assert r.hard_cap == 8
    assert r.concurrent_sessions == 2


def test_parse_missing_priority_defaults_normal():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {"concurrent_sessions": 3},
    }
    r = parse_usage(payload)
    assert r.priority_low is False
    assert r.boxed_until_epoch is None


def test_parse_malformed_concurrent_sessions_raises():
    payload: dict[str, object] = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {"concurrent_sessions": "not-a-number"},
    }
    with pytest.raises(UsageParseError):
        parse_usage(payload)


# --- fail_safe_reading -----------------------------------------------------


def test_fail_safe_is_conservative():
    r = fail_safe_reading()
    assert r.concurrent_sessions == 8  # at hard_cap
    assert r.priority_low is True
    assert r.boxed_until_epoch is None


# --- UsageClient with MockTransport ----------------------------------------


def _make_client(handler, **kw) -> UsageClient:
    return UsageClient(
        base_url="https://api.code.umans.ai",
        api_key="test-key",
        transport=httpx.MockTransport(handler),
        **kw,
    )


async def test_client_fetch_success():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/usage"
        assert request.headers["Authorization"] == "Bearer test-key"
        return httpx.Response(200, json=SAMPLE_PAYLOAD)

    client = _make_client(handler)
    try:
        cached = await client.fetch(now_monotonic=1000.0)
        assert cached.ok is True
        assert cached.reading.concurrent_sessions == 1
        assert cached.fetched_at_monotonic == 1000.0
    finally:
        await client.close()


async def test_client_failure_serves_lkg():
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(200, json=SAMPLE_PAYLOAD)
        raise httpx.ConnectError("connection refused")

    client = _make_client(handler)
    try:
        # First fetch succeeds.
        cached1 = await client.fetch(now_monotonic=1000.0)
        assert cached1.ok is True
        assert cached1.reading.concurrent_sessions == 1

        # Second fetch fails — serves LKG, marked stale.
        cached2 = await client.fetch(now_monotonic=1020.0)
        assert cached2.ok is False
        assert cached2.reading.concurrent_sessions == 1  # LKG reading
        assert cached2.fetched_at_monotonic == 1000.0  # original fetch time
    finally:
        await client.close()


async def test_client_no_lkg_returns_fail_safe():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = _make_client(handler)
    try:
        cached = await client.fetch(now_monotonic=1000.0)
        assert cached.ok is False
        assert cached.reading.concurrent_sessions == 8  # fail-safe: at hard_cap
        assert cached.reading.priority_low is True
        # fetched_at is very old → age will be large → staleness penalty.
        assert cached.fetched_at_monotonic < 1000.0
    finally:
        await client.close()


async def test_client_http_error_serves_lkg():
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(200, json=SAMPLE_PAYLOAD)
        return httpx.Response(500, text="internal server error")

    client = _make_client(handler)
    try:
        cached1 = await client.fetch(now_monotonic=1000.0)
        assert cached1.ok is True

        cached2 = await client.fetch(now_monotonic=1005.0)
        assert cached2.ok is False
        assert cached2.reading.concurrent_sessions == 1  # LKG
    finally:
        await client.close()


async def test_client_parse_error_serves_lkg():
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(200, json=SAMPLE_PAYLOAD)
        # Missing concurrent_sessions → parse error.
        return httpx.Response(200, json={"limits": {}, "usage": {}})

    client = _make_client(handler)
    try:
        cached1 = await client.fetch(now_monotonic=1000.0)
        assert cached1.ok is True

        cached2 = await client.fetch(now_monotonic=1005.0)
        assert cached2.ok is False
        assert cached2.reading.concurrent_sessions == 1  # LKG, not the bad payload
    finally:
        await client.close()


async def test_client_x_api_key_auth():
    received_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received_headers.update(request.headers)
        return httpx.Response(200, json=SAMPLE_PAYLOAD)

    client = UsageClient(
        base_url="https://api.code.umans.ai",
        api_key="my-key",
        auth_header="x-api-key",
        transport=httpx.MockTransport(handler),
    )
    try:
        await client.fetch(now_monotonic=1000.0)
        assert received_headers["x-api-key"] == "my-key"
        assert "Authorization" not in received_headers
    finally:
        await client.close()


# --- service_mode / low-interactivity (Plan 010 Feature 0) -----------------
# Field shape confirmed live 2026-07-14 (samples/service-mode-capture-2026-07-14.md).


def test_parse_service_mode_low_interactivity():
    payload = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 1,
            "tokens_in": 39321689,
            "tokens_out": 221557,
            "priority": {"low": False, "boxed_until": None, "reason": None},
            "service_mode": {
                "current": "low_interactivity",
                "resets_at": "2026-07-14T23:02:32.660799+00:00",
            },
        },
    }
    r = parse_usage(payload)
    assert r.service_mode == "low_interactivity"
    assert r.service_mode_resets_at_epoch is not None
    assert r.service_mode_resets_at_epoch > 0
    assert r.tokens_in == 39321689
    assert r.tokens_out == 221557


def test_parse_service_mode_absent_is_none():
    r = parse_usage(SAMPLE_PAYLOAD)
    assert r.service_mode is None
    assert r.service_mode_resets_at_epoch is None
    assert r.tokens_in is None
    assert r.tokens_out is None


def test_parse_service_mode_non_dict_is_ignored():
    payload = {
        "limits": {"concurrency": {"limit": 4, "hard_cap": 8}},
        "usage": {
            "concurrent_sessions": 1,
            "priority": {"low": False},
            "service_mode": "low_interactivity",  # malformed: not a dict
        },
    }
    r = parse_usage(payload)
    assert r.service_mode is None
    assert r.service_mode_resets_at_epoch is None
