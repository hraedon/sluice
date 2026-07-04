"""Bounded ring buffer of control-state snapshots for trend analysis.

Records one :class:`HistoryEntry` per reconciliation tick so the dashboard and
external tools can observe how concurrency, phantom load, breaker state, and
gate sizing have evolved over time — not just the current point-in-time reading.

This is a **shell-level** module: it stores operational telemetry about sluice's
own control loop (counts, states, timestamps).  It never touches request or
response bodies (the "inert in-path" guarantee).  It is not part of the pure
core — it holds mutable state — but it performs no I/O and reads no clock:
timestamps are supplied by the caller.

Design:
    * A :class:`collections.deque` with ``maxlen`` provides O(1) append and
      automatic eviction of the oldest entry when full.
    * The buffer is bounded by ``maxlen`` (default 2880, ~4 hours at a 5 s poll
      interval) so memory is predictable regardless of uptime.
    * Serialisation is via :meth:`HistoryEntry.to_dict` → JSON, surfaced at
      ``/history.json`` with an optional ``?limit=N`` query parameter.

Compact field names in ``to_dict()`` (used by /history.json):

    ===  ==========================
    ts   timestamp (epoch seconds)
    obs  concurrent_sessions
    loc  local_in_flight
    ph   phantom_estimate
    ep   effective_permits
    lim  limit (provider concurrency limit)
    hc   hard_cap (provider reject threshold)
    band band (normal/low/reject/boxed)
    brk  breaker state (closed/open/half_open)
    pl   priority_low
    age  usage_age (seconds since reading)
    stl  stale (reading ok flag)
    r429 recent_429s
    t429 total_429s
    rl429 rate_limit_429s
    qd   queue_depth
    qt   queue_timeouts
    err  tick_failed (true if this entry was recorded during a tick exception)
    rwin requests_in_window
    rlim requests_limit
    rrem requests_remaining
    rlw  local_requests_in_window
    rdelta request_window_delta
    ===  ==========================
    """

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class HistoryEntry:
    """One reconciliation tick's control-state snapshot, for trend analysis.

    Fields mirror the point-in-time :class:`~sluice.status.StatusSnapshot` but
    are frozen at capture time so the history forms an immutable time series.
    """

    timestamp: float  # wall-clock epoch seconds (supplied by caller)
    concurrent_sessions: int | None
    local_in_flight: int
    phantom_estimate: int
    effective_permits: int
    limit: int | None
    hard_cap: int | None
    band: str
    breaker: str
    priority_low: bool
    usage_age: float
    stale: bool
    recent_429s: int
    total_429s: int
    queue_depth: int
    queue_timeouts: int
    rate_limit_429s: int = 0  # 429s classified as rate-limit (fed to breaker, tracked separately)
    # Request-window budget (None when provider reports no request limit)
    requests_in_window: int | None = None
    requests_limit: int | None = None
    requests_remaining: int | None = None
    local_requests_in_window: int | None = None
    request_window_delta: int | None = None
    tick_failed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": round(self.timestamp, 1),
            "obs": self.concurrent_sessions,
            "loc": self.local_in_flight,
            "ph": self.phantom_estimate,
            "ep": self.effective_permits,
            "lim": self.limit,
            "hc": self.hard_cap,
            "band": self.band,
            "brk": self.breaker,
            "pl": self.priority_low,
            "age": round(self.usage_age, 1),
            "stl": self.stale,
            "r429": self.recent_429s,
            "t429": self.total_429s,
            "rl429": self.rate_limit_429s,
            "qd": self.queue_depth,
            "qt": self.queue_timeouts,
            "err": self.tick_failed,
            "rwin": self.requests_in_window,
            "rlim": self.requests_limit,
            "rrem": self.requests_remaining,
            "rlw": self.local_requests_in_window,
            "rdelta": self.request_window_delta,
        }


class History:
    """Bounded ring buffer of :class:`HistoryEntry` snapshots.

    Thread-unsafe by design — the reconciliation loop is single-threaded
    (one ``tick()`` at a time, driven by ``asyncio``).  Reads from the proxy's
    status handler happen on the same event loop, so no locking is needed.
    """

    def __init__(self, maxlen: int = 2880) -> None:
        if maxlen < 1:
            raise ValueError(f"maxlen must be >= 1, got {maxlen}")
        self._entries: deque[HistoryEntry] = deque(maxlen=maxlen)
        self._maxlen = maxlen

    def append(self, entry: HistoryEntry) -> None:
        """Append a snapshot.  Oldest entries are evicted when full."""
        self._entries.append(entry)

    def entries(self) -> list[HistoryEntry]:
        """Return a copy of all entries (oldest first)."""
        return list(self._entries)

    def to_dict_list(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        """Serialise to a list of dicts for JSON responses.

        If ``limit`` is given and > 0, only the last ``limit`` entries are
        returned.  ``limit=0`` or negative returns an empty list.
        """
        if limit is None:
            return [e.to_dict() for e in self._entries]
        if limit <= 0:
            return []
        return [e.to_dict() for e in list(self._entries)[-limit:]]

    def clear(self) -> None:
        """Remove all entries."""
        self._entries.clear()

    @property
    def length(self) -> int:
        """Number of entries currently stored."""
        return len(self._entries)

    @property
    def maxlen(self) -> int:
        """Maximum number of entries the buffer can hold."""
        return self._maxlen
