# Plan 009 — Sparkline card depth: queue spark, band ribbon, time-horizon toggle

The sparkline card is tall (flexed against the Reading table) but shallow: three
series over five minutes, no scale, and none of the queue story. The 2026-07-02
deliberate-overload run made the gap concrete — queue depth went 0→13 with 46
timeouts and the card showed nothing. Plan 008 gave the dashboard a 4-hour
(persistent) history it doesn't render.

Goal: **fill the card with the data it already collects.** No new collection, no
new endpoints, no server changes — `/history.json?limit=N` and the per-tick
`HistoryEntry` fields (`qd`, `band`, `qt`, `t429`) already carry everything.

## Work items

### WI-001 — Queue-depth companion spark

Second small SVG under the main sparkline: `qd` as a filled area + line with its
own max scale (queue depth shares no meaningful axis with concurrency). Max
label rendered in-SVG. This is the "what's waiting behind the gate" half of the
pressure picture.

### WI-002 — Band ribbon

A 4px SVG strip between the sparks: one segment per sample, colored by band
(`normal` → transparent, `low` → warn, `reject`/`boxed` → crit). Answers "when
did we last leave normal?" at a glance. Currently expected to be blank almost
always; becomes load-bearing if dispatch is ever tuned more aggressively
(user's explicit rationale for keeping it).

### WI-003 — Event ticks

Vertical tick marks on the queue spark where `queue_timeouts` (warn) or
`total_429s` (crit) incremented between samples. Rare by design — which is why
they must be loud when present.

### WI-004 — Time-horizon toggle (5m / 1h / 4h)

Range buttons in the card header. `5m` keeps the live per-poll buffer
(unchanged behavior). `1h`/`4h` fetch `/history.json?limit=720|2880` (5s tick
cadence), refreshed at most every 15s while the view is active, and downsample
client-side to ≤120 buckets. Bucket aggregation: **max** for numeric series
(preserves spikes — mean would erase exactly the events worth seeing), **worst**
for band, **any** for event ticks. This is Plan 008's persistence finally
getting a UI expression.

### WI-005 — Scale labels

Small `max N` text in each spark so the lines have a scale. No full axes or
gridlines beyond the existing limit line — patina minimalism stands.

## Non-goals / deferred

- **Hover tooltip** (nearest-sample readout) — useful at 4h resolution, but
  meaningful JS surface; separate follow-up if the toggle sees real use.
- **Queue-wait series** — `HistoryEntry` doesn't carry wait percentiles; adding
  fields is a data-model change, out of scope here.
- **Server-side bucketing** — 2880 compact entries ≈ 400 KB on an admin-gated
  LAN endpoint; client-side downsampling is fine at this scale.
- **Token/request/model panels** — usage-dashboard's charter, not sluice's.

## Done when

- All five WIs render correctly against live data.
- Dashboard HTML contract tests cover the new elements (range buttons, queue
  spark, ribbon) and the long-range fetch limits.
- Full suite + mypy --strict + ruff green.
