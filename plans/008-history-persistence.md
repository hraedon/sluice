# Plan 008 — History persistence (optional SQLite cold store)

The in-memory ring buffer (Plan: history.py) is bounded, fast, and never touches
the wire — but it is **ephemeral**. Every pod restart, redeploy, or crash wipes
the trend. The four most operationally interesting moments (did the breaker trip
before the restart? was the account trending toward boxed?) are invisible.

Goal: **an optional SQLite-backed persistence layer that survives restarts,
enables crash forensics, and extends the trend horizon beyond the in-memory
buffer's ~4 h — without modifying the hot store, the pure core, or the
inert-in-path guarantee.**

> **`sqlite3` is stdlib.** The import-boundary test still passes: `control.py`
> imports nothing new; `history_store.py` joins the shell layer alongside
> `history.py`.

## Design constraints (carried from AGENTS.md)

- **`History` (in-memory) stays unchanged.** It remains the primary read source
  for `/history.json`. The store is a secondary write target and a cold read
  source for startup warming.
- **Fail safe — the store is telemetry, not the truth path.** Any SQLite error
  (corrupt file, unwritable path, full disk) degrades to in-memory-only: log a
  warning and continue. The reconciliation loop must never stop because of a
  store failure.
- **Never block the reconciliation loop.** Writes are synchronous (WAL mode,
  ~0.1 ms per INSERT) and happen once per poll interval (default 5 s), not per
  request. The `fetch()` network I/O in `tick()` is orders of magnitude slower.
- **Inert in-path.** The store captures the same control-state counts and
  timestamps as the in-memory buffer — never request/response bodies.
- **Pure core untouched.** `history_store.py` lives in the shell; `control.py`
  has no reference to it.

## Work items

### WI-001 — `HistoryStore` protocol + `SQLiteHistoryStore` (`sluice.history_store`)

A `HistoryStore` is an optional persistence layer:

- `append(entry)` — INSERT one row. Fail-safe (log, never raise).
- `load_recent(limit)` — SELECT last N entries ordered by timestamp, return
  oldest-first as `HistoryEntry` objects. Used for startup warming.
- `prune(ttl_seconds, now)` — DELETE rows older than `now - ttl`. Called
  periodically from the reconciliation loop.
- `close()` — close the connection.

`SQLiteHistoryStore` implementation:

- `sqlite3` with WAL mode (`PRAGMA journal_mode=WAL`) for concurrent reads
  without blocking writers.
- Schema: single `history` table with the 16 `HistoryEntry` fields using the
  same compact column names as `to_dict()`.
- `check_same_thread=False` — the reconcile loop is single-threaded asyncio,
  but the flag avoids surprises if the connection is ever touched from a
  different thread.
- All public methods catch `sqlite3.Error` (and generic `Exception`) and log.
  None ever raise to the caller.

### WI-002 — Wire into `ReconciliationLoop` (`sluice.reconcile`)

- Add `history_store: HistoryStore | None` parameter.
- In `tick()`, after the in-memory `self._history.append(entry)`, also call
  `self._history_store.append(entry)` if configured.
- In `_record_failed_tick()`, same.
- In `run()`, every `_prune_interval` ticks (default 60, = 5 min at 5 s poll),
  call `self._history_store.prune(ttl_seconds, now)`.
- All store calls wrapped in `try/except Exception` — never propagate.

### WI-003 — CLI integration + buffer warming (`sluice.cli`)

- `--history-store PATH` flag (optional, defaults to None = in-memory only,
  same as today).
- `--history-ttl SECONDS` flag (optional, default 604800 = 7 days).
- On startup, if store is configured, `load_recent(history_size)` entries from
  SQLite and warm the in-memory `History` buffer. The dashboard has immediate
  trend data after a restart.
- Log store configuration (path, TTL, warmed entry count).

### WI-004 — Fix pre-existing bugs found in review

- **`_last_permits` on tick failure** — set `self._last_permits = 0` in the
  `run()` exception handler so `/status.json` and `gate_closed_reason()` agree
  with the closed gate.
- **`_record_failed_tick()` safety** — wrap the call in `try/except` so a store
  or `wall()` error cannot kill the reconciliation loop.
- **`_prune_429s()` in default controller** — call at the start of every `tick()`,
  not just the adaptive path, so `recent_429s` reflects the window, not the
  raw deque length.
- **Dashboard JS race** — `initHistory().then(poll)` instead of
  `initHistory(); poll()` so the initial fetch doesn't overwrite a real-time
  entry pushed by the first poll.
- **`to_dict_list()` negative limit** — guard `limit <= 0` → return `[]`.

### WI-005 — Tests

- Unit tests for `SQLiteHistoryStore`: append/load_recent round-trip, pruning,
  fail-safe on corrupt/missing file, WAL mode, duplicate timestamps.
- Integration test: tick writes to store, `load_recent` reads back.
- Integration test: store failure (closed handle) degrades gracefully —
  reconcile loop continues, in-memory buffer still works.
- Test: buffer warming on startup populates in-memory from store.
- Test: pruning removes old entries, keeps recent ones.
- Test: negative `?limit` and malformed query params on `/history.json`.
- Import boundary test updated to include `sluice.history_store`.

### WI-006 — Plan document (this file)

## Sequencing

WI-001 → WI-002 → WI-004 (bug fixes, independent) → WI-003 → WI-005.

## Done when

- `--history-store /path/to/db` persists history across a simulated restart
  (stop + start reads back the prior ticks).
- `/history.json` shows warmed data immediately after restart.
- Store failure (missing file, corrupt DB) does not crash the reconcile loop
  and `/history.json` still works (from in-memory).
- All pre-existing bugs (WI-004) are fixed with regression tests.
- All existing tests pass + new tests pass.
- `mypy --strict` clean, `ruff` clean, import-boundary test passes.
