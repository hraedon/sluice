"""Optional SQLite persistence for the history ring buffer.

When configured (``--history-store PATH``), every :class:`HistoryEntry` is
written to a SQLite database in addition to the in-memory ring buffer.  On
startup, the in-memory buffer is warmed from the store so the dashboard has
immediate trend data after a restart.

This is a **shell-level** module: it performs file I/O but never touches request
or response bodies (the "inert in-path" guarantee).  It is not part of the pure
core.  All public methods are fail-safe: any SQLite error is logged and silently
ignored — the store is telemetry, not the truth path, and losing it must never
stop the reconciliation loop.

Design:
    * ``sqlite3`` (stdlib) in WAL mode for concurrent reads without blocking.
    * Single ``history`` table with the :class:`HistoryEntry` fields.
    * Column names match the compact keys in :meth:`HistoryEntry.to_dict` for
      consistency.
    * Writes are synchronous (one INSERT per tick, ~0.1 ms in WAL mode) and
      happen at the poll cadence (default 5 s), not per request.
    * Pruning is periodic (every N ticks) and bounded by a configurable TTL.

The connection must only be accessed from the event loop thread.
``check_same_thread=False`` disables Python's thread-affinity check but does not
provide thread safety — a future contributor adding multi-threaded access must
introduce their own locking.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Protocol, runtime_checkable

from sluice.history import HistoryEntry

log = logging.getLogger("sluice.history_store")

_CONNECT_TIMEOUT = 5.0


@runtime_checkable
class HistoryStore(Protocol):
    """Optional persistence layer for :class:`~sluice.history.History`."""

    def append(self, entry: HistoryEntry) -> None:
        """Persist one entry.  Fail-safe: logs on error, never raises."""
        ...

    def load_recent(self, limit: int) -> list[HistoryEntry]:
        """Return the last *limit* entries, oldest-first.  Empty on error."""
        ...

    def prune(self, *, ttl_seconds: float, now: float) -> int:
        """Delete entries older than ``now - ttl_seconds``.  Returns count deleted."""
        ...

    def close(self) -> None:
        """Close the underlying connection."""
        ...


_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS history (
    ts  REAL    NOT NULL,
    obs INTEGER,
    loc INTEGER NOT NULL,
    ph  INTEGER NOT NULL,
    ep  INTEGER NOT NULL,
    lim INTEGER,
    hc  INTEGER,
    band TEXT    NOT NULL,
    brk  TEXT    NOT NULL,
    pl   INTEGER NOT NULL,
    age  REAL    NOT NULL,
    stl  INTEGER NOT NULL,
    r429 INTEGER NOT NULL,
    t429 INTEGER NOT NULL,
    qd   INTEGER NOT NULL,
    qt   INTEGER NOT NULL,
    err  INTEGER NOT NULL,
    rwin INTEGER,
    rlim INTEGER,
    rrem INTEGER,
    rlw  INTEGER,
    rdelta INTEGER
)
"""

_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_history_ts ON history(ts)"

# Migration: add request-window columns to pre-existing tables.
_MIGRATIONS = [
    "ALTER TABLE history ADD COLUMN rwin INTEGER",
    "ALTER TABLE history ADD COLUMN rlim INTEGER",
    "ALTER TABLE history ADD COLUMN rrem INTEGER",
    "ALTER TABLE history ADD COLUMN rlw INTEGER",
    "ALTER TABLE history ADD COLUMN rdelta INTEGER",
]

_INSERT = """\
INSERT INTO history (ts, obs, loc, ph, ep, lim, hc, band, brk, pl, age, stl, r429, t429, qd, qt, err, rwin, rlim, rrem, rlw, rdelta)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_SELECT = """\
SELECT ts, obs, loc, ph, ep, lim, hc, band, brk, pl, age, stl, r429, t429, qd, qt, err, rwin, rlim, rrem, rlw, rdelta
FROM history ORDER BY ts DESC, rowid DESC LIMIT ?
"""


class SQLiteHistoryStore:
    """SQLite-backed persistence for :class:`HistoryEntry` snapshots.

    All public methods are fail-safe: any :class:`sqlite3.Error` (or generic
    :class:`Exception`) is caught and logged.  The store is telemetry, not the
    truth path — losing it is a degraded-but-safe state.
    """

    def __init__(self, path: str) -> None:
        self._conn: sqlite3.Connection | None = None
        try:
            self._conn = sqlite3.connect(
                path,
                check_same_thread=False,
                isolation_level=None,
                timeout=_CONNECT_TIMEOUT,
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA wal_autocheckpoint=1000")
            self._conn.execute(_CREATE_TABLE)
            self._conn.execute(_CREATE_INDEX)
            for stmt in _MIGRATIONS:
                try:
                    self._conn.execute(stmt)
                except Exception:
                    pass  # column already exists
        except Exception:
            log.exception("failed to open history store at %s — store disabled", path)
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._conn = None

    @property
    def is_available(self) -> bool:
        """True if the store opened successfully and is accepting writes."""
        return self._conn is not None

    def append(self, entry: HistoryEntry) -> None:
        conn = self._conn
        if conn is None:
            return
        try:
            conn.execute(
                _INSERT,
                (
                    entry.timestamp,
                    entry.concurrent_sessions,
                    entry.local_in_flight,
                    entry.phantom_estimate,
                    entry.effective_permits,
                    entry.limit,
                    entry.hard_cap,
                    entry.band,
                    entry.breaker,
                    entry.priority_low,
                    entry.usage_age,
                    entry.stale,
                    entry.recent_429s,
                    entry.total_429s,
                    entry.queue_depth,
                    entry.queue_timeouts,
                    entry.tick_failed,
                    entry.requests_in_window,
                    entry.requests_limit,
                    entry.requests_remaining,
                    entry.local_requests_in_window,
                    entry.request_window_delta,
                ),
            )
        except Exception:
            log.warning("history store append failed", exc_info=True)

    def load_recent(self, limit: int) -> list[HistoryEntry]:
        conn = self._conn
        if conn is None or limit <= 0:
            return []
        try:
            cur = conn.execute(_SELECT, (limit,))
            rows = cur.fetchall()
        except Exception:
            log.warning("history store load_recent failed", exc_info=True)
            return []
        rows.reverse()
        return [
            HistoryEntry(
                timestamp=row[0],
                concurrent_sessions=row[1],
                local_in_flight=row[2],
                phantom_estimate=row[3],
                effective_permits=row[4],
                limit=row[5],
                hard_cap=row[6],
                band=row[7],
                breaker=row[8],
                priority_low=bool(row[9]),
                usage_age=row[10],
                stale=bool(row[11]),
                recent_429s=row[12],
                total_429s=row[13],
                queue_depth=row[14],
                queue_timeouts=row[15],
                tick_failed=bool(row[16]),
                requests_in_window=row[17] if len(row) > 17 else None,
                requests_limit=row[18] if len(row) > 18 else None,
                requests_remaining=row[19] if len(row) > 19 else None,
                local_requests_in_window=row[20] if len(row) > 20 else None,
                request_window_delta=row[21] if len(row) > 21 else None,
            )
            for row in rows
        ]

    def prune(self, *, ttl_seconds: float, now: float) -> int:
        conn = self._conn
        if conn is None:
            return 0
        cutoff = now - ttl_seconds
        try:
            cur = conn.execute("DELETE FROM history WHERE ts < ?", (cutoff,))
            return cur.rowcount
        except Exception:
            log.warning("history store prune failed", exc_info=True)
            return 0

    def close(self) -> None:
        conn = self._conn
        if conn is None:
            return
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.close()
        except Exception:
            log.warning("history store close failed", exc_info=True)
        finally:
            self._conn = None
