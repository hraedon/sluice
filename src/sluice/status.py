"""Status projection — pure snapshot of sluice's control state for dashboards.

The projection is assembled in the shell from the pure core's state plus shell
counters.  **Counts only — never request content.**  A test asserts no body text
can reach a status payload (the "inert in-path" guarantee made visible).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sluice.reconcile import ReconciliationLoop
from sluice.singleton import SingletonGuard


@dataclass
class StatusSnapshot:
    """A point-in-time snapshot of sluice's control state."""

    # Reading
    concurrent_sessions: int | None
    limit: int | None
    hard_cap: int | None
    priority_low: bool
    priority_reason: str | None
    boxed_until: float | None
    resets_at: float | None
    usage_age: float
    stale: bool

    # Computed
    effective_permits: int
    band: str
    phantom_estimate: int
    breaker: str
    breaker_half_open_age_seconds: float | None
    recent_429s: int
    total_429s: int
    gateway_429s: int

    # Operational
    target: int
    queue_depth: int
    local_in_flight: int
    cooling_down: int
    avg_wait_seconds: float
    p95_wait_seconds: float
    queue_timeouts: int
    ready: bool
    gate_closed_reason: str
    config: dict[str, Any]

    # Request-window budget (umans Code Pro: limits.requests + usage.requests_in_window)
    requests_in_window: int | None
    requests_limit: int | None
    requests_remaining: int | None
    requests_hard_cap: int | None
    requests_window_seconds: int | None
    local_requests_in_window: int | None
    request_window_delta: int | None
    total_requests_forwarded: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "concurrent_sessions": self.concurrent_sessions,
            "limit": self.limit,
            "hard_cap": self.hard_cap,
            "priority_low": self.priority_low,
            "priority_reason": self.priority_reason,
            "boxed_until": self.boxed_until,
            "resets_at": self.resets_at,
            "usage_age": round(self.usage_age, 1),
            "stale": self.stale,
            "effective_permits": self.effective_permits,
            "band": self.band,
            "phantom_estimate": self.phantom_estimate,
            "breaker": self.breaker,
            "breaker_half_open_age_seconds": (
                round(self.breaker_half_open_age_seconds, 1)
                if self.breaker_half_open_age_seconds is not None
                else None
            ),
            "recent_429s": self.recent_429s,
            "total_429s": self.total_429s,
            "gateway_429s": self.gateway_429s,
            "target": self.target,
            "queue_depth": self.queue_depth,
            "local_in_flight": self.local_in_flight,
            "cooling_down": self.cooling_down,
            "avg_wait_seconds": round(self.avg_wait_seconds, 2),
            "p95_wait_seconds": round(self.p95_wait_seconds, 2),
            "queue_timeouts": self.queue_timeouts,
            "ready": self.ready,
            "gate_closed_reason": self.gate_closed_reason,
            "config": self.config,
            "requests_in_window": self.requests_in_window,
            "requests_limit": self.requests_limit,
            "requests_remaining": self.requests_remaining,
            "requests_hard_cap": self.requests_hard_cap,
            "requests_window_seconds": self.requests_window_seconds,
            "local_requests_in_window": self.local_requests_in_window,
            "request_window_delta": self.request_window_delta,
            "total_requests_forwarded": self.total_requests_forwarded,
        }


def snapshot(reconcile: ReconciliationLoop, guard: SingletonGuard | None = None) -> StatusSnapshot:
    """Build a :class:`StatusSnapshot` from the reconciliation loop's current state."""
    reading = None
    if reconcile.last_reading is not None:
        reading = reconcile.last_reading.reading

    ready = reconcile.ready
    if guard is not None:
        ready = ready and guard.is_held()

    return StatusSnapshot(
        concurrent_sessions=reconcile.observed_concurrent_sessions,
        limit=reading.limit if reading else None,
        hard_cap=reading.hard_cap if reading else None,
        priority_low=reading.priority_low if reading else False,
        priority_reason=reading.priority_reason if reading else None,
        boxed_until=reading.boxed_until_epoch if reading else None,
        resets_at=reading.resets_at_epoch if reading else None,
        usage_age=reconcile.last_age_seconds,
        stale=not (reconcile.last_fetch_ok),
        effective_permits=reconcile.effective_permits_count,
        band=reconcile.band.value,
        phantom_estimate=reconcile.phantom_estimate_value,
        breaker=reconcile.breaker_state.value,
        breaker_half_open_age_seconds=reconcile.breaker_half_open_age_seconds,
        recent_429s=reconcile.recent_429_count,
        total_429s=reconcile.total_429s,
        gateway_429s=reconcile.gateway_429s,
        target=reconcile.target,
        queue_depth=reconcile.queue_depth,
        local_in_flight=reconcile.in_flight,
        cooling_down=reconcile.cooling_down,
        avg_wait_seconds=reconcile.avg_wait_seconds,
        p95_wait_seconds=reconcile.p95_wait_seconds,
        queue_timeouts=reconcile.queue_timeouts,
        ready=ready,
        gate_closed_reason=reconcile.gate_closed_reason(),
        config={
            "target": reconcile.target,
            "min_floor": reconcile.min_floor,
            "poll_interval": reconcile.poll_interval,
            "usage_fresh_ttl": reconcile.usage_fresh_ttl,
            "phantom_window": reconcile.phantom_window,
            "breaker_threshold": reconcile.breaker_threshold,
            "breaker_window_seconds": reconcile.breaker_window_seconds,
            "breaker_cooldown_seconds": reconcile.breaker_cooldown_seconds,
            "provider": reconcile.provider_name,
            "controller": reconcile.controller_name,
        },
        requests_in_window=reading.requests_in_window if reading else None,
        requests_limit=reading.requests_limit if reading else None,
        requests_remaining=reading.requests_remaining if reading else None,
        requests_hard_cap=reading.requests_hard_cap if reading else None,
        requests_window_seconds=reading.requests_window_seconds if reading else None,
        local_requests_in_window=reconcile.local_requests_in_window,
        request_window_delta=reconcile.request_window_delta,
        total_requests_forwarded=reconcile.total_requests_forwarded,
    )


def to_prometheus(snap: StatusSnapshot) -> str:
    """Render a snapshot as OpenMetrics text exposition."""
    lines: list[str] = []

    def gauge(name: str, help_text: str, value: int | float | None) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name} {value if value is not None else float('nan')}")

    def enum_gauge(name: str, help_text: str, value: str, *states: str) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        for state in states:
            lines.append(f'{name}{{state="{state}"}} {1 if value == state else 0}')

    gauge("sluice_in_flight", "Currently held permits", snap.local_in_flight)
    gauge("sluice_effective_permits", "Current effective permit count", snap.effective_permits)
    gauge("sluice_observed_sessions", "Provider-reported concurrent sessions", snap.concurrent_sessions)
    gauge("sluice_phantom_estimate", "Windowed phantom estimate (sustained excess)", snap.phantom_estimate)
    gauge("sluice_recent_429s", "Recent 429 count (within breaker window)", snap.recent_429s)
    gauge("sluice_total_429s", "Total 429s since startup", snap.total_429s)
    gauge("sluice_gateway_429s", "Upstream 429s from CDN/gateway (not fed to breaker)", snap.gateway_429s)
    gauge(
        "sluice_breaker_half_open_age_seconds",
        "Seconds since breaker entered HALF_OPEN (None if not half-open)",
        snap.breaker_half_open_age_seconds,
    )
    gauge("sluice_queue_depth", "Requests waiting for a permit", snap.queue_depth)
    gauge("sluice_cooling_down", "Permits in release cooldown", snap.cooling_down)
    gauge("sluice_queue_wait_avg_seconds", "Mean queue wait over recent blocked grants", round(snap.avg_wait_seconds, 3))
    gauge("sluice_queue_wait_p95_seconds", "95th-pct queue wait over recent blocked grants", round(snap.p95_wait_seconds, 3))
    gauge("sluice_queue_timeouts_total", "Requests that gave up waiting for a permit", snap.queue_timeouts)
    enum_gauge(
        "sluice_band",
        "Current enforcement band",
        snap.band,
        "normal", "low", "reject", "boxed",
    )
    enum_gauge(
        "sluice_breaker",
        "Circuit breaker state",
        snap.breaker,
        "closed", "open", "half_open",
    )
    enum_gauge(
        "sluice_gate_closed_reason",
        "Why the gate is closed",
        snap.gate_closed_reason,
        "open", "boxed", "breaker", "saturated",
    )
    gauge("sluice_requests_in_window", "Provider-reported requests used in the current window", snap.requests_in_window)
    gauge("sluice_requests_limit", "Provider request limit for the window", snap.requests_limit)
    gauge("sluice_requests_remaining", "Provider-reported remaining requests in the window", snap.requests_remaining)
    gauge("sluice_local_requests_in_window", "Sluice forwarded requests within the provider's window", snap.local_requests_in_window)
    gauge("sluice_request_window_delta", "Provider requests_in_window minus sluice local count (leakage)", snap.request_window_delta)
    gauge("sluice_total_requests_forwarded", "Total requests forwarded upstream since startup", snap.total_requests_forwarded)

    return "\n".join(lines) + "\n"
