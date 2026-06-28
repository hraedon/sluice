"""Status projection — pure snapshot of sluice's control state for dashboards.

The projection is assembled in the shell from the pure core's state plus shell
counters.  **Counts only — never request content.**  A test asserts no body text
can reach a status payload (the "inert in-path" guarantee made visible).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sluice.reconcile import ReconciliationLoop


@dataclass
class StatusSnapshot:
    """A point-in-time snapshot of sluice's control state."""

    # Reading
    concurrent_sessions: int | None
    limit: int | None
    hard_cap: int | None
    priority_low: bool
    boxed_until: float | None
    resets_at: float | None
    usage_age: float
    stale: bool

    # Computed
    effective_permits: int
    band: str
    phantom_estimate: int
    breaker: str
    recent_429s: int
    total_429s: int

    # Operational
    target: int
    queue_depth: int
    local_in_flight: int
    cooling_down: int
    ready: bool
    gate_closed_reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "concurrent_sessions": self.concurrent_sessions,
            "limit": self.limit,
            "hard_cap": self.hard_cap,
            "priority_low": self.priority_low,
            "boxed_until": self.boxed_until,
            "resets_at": self.resets_at,
            "usage_age": round(self.usage_age, 1),
            "stale": self.stale,
            "effective_permits": self.effective_permits,
            "band": self.band,
            "phantom_estimate": self.phantom_estimate,
            "breaker": self.breaker,
            "recent_429s": self.recent_429s,
            "total_429s": self.total_429s,
            "target": self.target,
            "queue_depth": self.queue_depth,
            "local_in_flight": self.local_in_flight,
            "cooling_down": self.cooling_down,
            "ready": self.ready,
            "gate_closed_reason": self.gate_closed_reason,
        }


def snapshot(reconcile: ReconciliationLoop) -> StatusSnapshot:
    """Build a :class:`StatusSnapshot` from the reconciliation loop's current state."""
    reading = None
    if reconcile._last_reading_cached is not None:
        reading = reconcile._last_reading_cached.reading

    return StatusSnapshot(
        concurrent_sessions=reconcile.observed_concurrent_sessions,
        limit=reading.limit if reading else None,
        hard_cap=reading.hard_cap if reading else None,
        priority_low=reading.priority_low if reading else False,
        boxed_until=reading.boxed_until_epoch if reading else None,
        resets_at=reading.resets_at_epoch if reading else None,
        usage_age=reconcile.last_age_seconds,
        stale=not (reconcile.last_fetch_ok),
        effective_permits=reconcile.effective_permits_count,
        band=reconcile.band.value,
        phantom_estimate=reconcile.phantom_estimate_value,
        breaker=reconcile.breaker_state.value,
        recent_429s=reconcile.recent_429_count,
        total_429s=reconcile.total_429s,
        target=reconcile._ctrl_cfg.target,
        queue_depth=reconcile.queue_depth,
        local_in_flight=reconcile.in_flight,
        cooling_down=0,  # not tracked at reconcile level
        ready=reconcile.ready,
        gate_closed_reason=reconcile.gate_closed_reason(),
    )


def to_prometheus(snap: StatusSnapshot) -> str:
    """Render a snapshot as OpenMetrics text exposition."""
    lines: list[str] = []

    def gauge(name: str, help_text: str, value: int | float | None) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        if value is not None:
            lines.append(f"{name} {value}")

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
    gauge("sluice_queue_depth", "Requests waiting for a permit", snap.queue_depth)
    gauge("sluice_cooling_down", "Permits in release cooldown", snap.cooling_down)
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

    return "\n".join(lines) + "\n"
