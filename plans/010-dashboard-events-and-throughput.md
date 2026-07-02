# Plan 010 — Dashboard depth: recent-events panel + throughput spark

The full-width-sparkline layout pass (2026-07-02) fixed the dashboard's
geometry: the spark card no longer flexes against the Reading table, Reading
and Config pair up below it, and every legend/Reading/Config entry carries a
hover explanation. What remains is the two content gaps identified in that
review:

1. The event ticks and band ribbon are **loud but ephemeral** — a 429 tick at
   03:00 is a 1px line you have to hover to date. Nothing on the page answers
   "what happened overnight?" as text.
2. Nothing distinguishes **idle from healthy**. A flat sparkline at 0 observed
   sessions looks identical whether sluice forwarded 400 requests in the last
   hour or none.

## Work items

### WI-001 — Recent-events panel (client-side only)

A third card in the Reading/Config row: the last ~20 noteworthy transitions
derived client-side from `/history.json`, newest first, each with a timestamp.
Event kinds, all computed by diffing consecutive entries (the same
increment-detection `withIncs` already does for ticks):

- band transition (`normal → low`, `low → normal`, …) — warn/crit colored by
  the worse side
- breaker transition (`closed → open`, `open → half_open`, …)
- 429 increment (`t429` advanced; show the delta)
- queue-timeout increment (`qt` advanced; show the delta)
- stale-gap edges (`stl` flipped false→true / true→false)

No new endpoints, no server changes: the panel derives from the same
`/history.json?limit=2880` fetch the 4h range already uses (fetch on load +
piggyback on the long-range refresh cadence). Empty state: "no events in the
last 4h" — expected to be the norm, which is exactly why the panel earns its
place when it isn't.

Rendering: table rows `[time] [kind] [detail]`, patina-minimal, no scrollback
beyond the 4h history window (that's Prometheus's job).

### WI-002 — Throughput spark (one server-side field)

Per-tick forwarded-request rate under the queue spark, answering idle-vs-healthy.

- **Server**: record `total_requests_forwarded` on `HistoryEntry` (compact key
  `tfwd`), following the existing pattern: dataclass field with a `None`
  default, `to_dict` key, and an `ALTER TABLE history ADD COLUMN tfwd INTEGER`
  appended to the history-store migration list (Plan 008's request-window
  columns prove the path).
- **Client**: render the per-bucket delta of `tfwd` as small bars (rate, not
  cumulative — diff consecutive samples, clamp negatives to 0 across restarts
  since the counter resets). 5m view can also derive it from live
  `/status.json` polls (`total_requests_forwarded` is already in the payload),
  so the spark works immediately even before persisted history has the column.
- Bucketing: **sum** of deltas per bucket (unlike the max-for-levels rule —
  throughput is a count, not a level; max would undercount).

### WI-003 — Contract tests

Extend the dashboard contract tests: events-panel element + derivation code
path present, `tfwd` in the live-buffer push and the history mapper, throughput
spark element, and a history-store test that a pre-010 database gains the
`tfwd` column on open.

## Non-goals / deferred (unchanged from Plan 009)

- **Queue-wait series** — avg/p95 wait are still not per-tick fields; a
  data-model change with less payoff than throughput. Next candidate after
  this plan if live use wants it.
- **Min/max envelope on 1h/4h**, **server-side bucketing** — same reasoning as
  Plan 009.
- **Token/request/model/cost panels** — usage-dashboard's charter, not
  sluice's. The events panel and throughput spark complete the control-loop
  story; usage analytics stay out.
- **Event-log persistence beyond the history window** — the panel is a
  projection of history, not a new log. Durable audit belongs to Prometheus
  scraping `/metrics`.

## Done when

- Events panel renders real transitions from a live instance (deliberate
  breaker trip or a synthetic history fixture) and the empty state otherwise.
- Throughput spark shows nonzero bars under real traffic and zero when idle,
  on all three ranges; counter-reset (restart) does not render a negative or
  spike artifact.
- Pre-existing history databases migrate in place (new column, old rows read
  back with `tfwd` null).
- Full suite + mypy --strict + ruff green.
