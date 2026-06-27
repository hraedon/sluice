# Plan 001 — Data plane + reconciliation loop

Turn `docs/concurrency-model.md` into a running proxy: the pure deterministic controller,
the streaming reverse-proxy shell for both umans surfaces, and the `/v1/usage`
reconciliation loop that ties them together.

Goal of this plan: **a single `sluice serve` process that opencode, open-webui, and the
hermes agent (on three different hosts) can all point at, that keeps umans-observed
`concurrent_sessions` at or below `target` and never marches into the box.**

## Work items

### WI-001 — Pure controller (`sluice.control`)
The heart, written first because everything tests against it.
- `effective_permits(state, *, now) -> int` exactly as in §3 of the spine.
- Band classification (`normal` / `low` / `reject` / `boxed`) from a usage reading.
- Breaker state machine (closed → open on N concurrency-429s within a window →
  half-open probe → closed/open).
- `phantom_estimate`, staleness penalty, clamp to `[min_floor, target]`.
- **No imports outside the stdlib. No I/O. No clock** — `now` is an argument.
- Tests: each band; phantom absorption (observed>local shrinks gate); staleness tightens;
  box → 0; breaker-open → 0; monotonicity (no uncertain input ever raises the result).

### WI-002 — Usage client (`sluice.usage`)
- `GET /v1/usage` with the account key (`x-api-key` or `Authorization: Bearer`).
- Parse into the controller's input dataclass: `concurrent_sessions`, `limit`, `hard_cap`,
  `priority.{low,boxed_until,reason}`, `resets_at`. Borrow the field handling from
  usage-dashboard's `fetch_umans.py` (which already decodes `priority`/`boxed_until`/
  `resets_at`); **add `concurrent_sessions`**, which that parser predates.
- Last-known-good cache with a TTL; on fetch failure, serve LKG and mark it stale (so the
  controller applies the staleness penalty, per §3) — never invent a zero.
- Tests: parse a representative payload; staleness flagged on failure; missing fields →
  fail safe (treat as low/uncertain), never crash open.

### WI-003 — Reconciliation loop
- Background task: every `poll_interval`, fetch usage, recompute `effective_permits`,
  resize the live semaphore, update breaker/box state.
- Resizing a busy semaphore safely (shrinking below current holders = no new grants until
  drain; never force-revoke an in-flight request).
- On `boxed_until`: set permits 0, record `resets_at`; reopen at the deadline.
- Tests (with a fake clock + fake usage source): phantom appears → gate shrinks next tick;
  phantom clears → gate reopens; box → gate closed until `resets_at`.

### WI-004 — Streaming reverse-proxy shell (`sluice.proxy`)
- ASGI app proxying **both** `POST /v1/messages` and `POST /v1/chat/completions` (and any
  other path, transparently) to the configured upstream.
- Acquire a permit (bounded queue wait → `503` + `Retry-After` on timeout) before
  forwarding; release on completion **or** downstream disconnect.
- **On downstream disconnect, cancel the upstream request** (clean close) — phantom
  prevention, the load-bearing detail.
- True streaming passthrough: stream request and response bytes; never buffer a full body.
- Pass auth header through unchanged; sluice holds no key of its own beyond the one used by
  the usage poller (configurable: same key, or a dedicated read key).
- Tests: streamed response arrives incrementally (assert chunks, not just final body);
  permit released on disconnect; upstream cancelled on disconnect; 503 on queue timeout.

### WI-005 — `sluice serve` CLI + config
- `sluice serve --upstream <url> --target N --listen host:port [--poll-interval ...]
  [--release-cooldown ...] [--usage-key-env ...]`.
- Config precedence: flags → env → file; sane Code Max defaults (`target=4`... but ship the
  default at **3**, one slot of headroom below the limit, per the operating decision).
- `sluice status` — print the current reading, computed permits, band, breaker, in-flight.

### WI-006 — Import-boundary + architecture test
- Assert `sluice.control` imports only stdlib (AST scan or import probe).
- Assert the dependency direction: `proxy`/`usage`/`cli` may import `control`; `control`
  imports none of them.

### WI-007 — Operability
- `/healthz` (liveness) and `/metrics` (in-flight, effective_permits, observed, band,
  breaker, 429s-today, queue depth) — counts only, no request content.
- Structured startup log of the resolved config.
- Optional (stretch, not required this plan): a small live TUI like ai-concurrency-shaper's.

## Sequencing
WI-001 → WI-006 (lock the boundary early) → WI-002 → WI-003 → WI-004 → WI-005 → WI-007.
Controller and boundary first so the shell is built against a frozen, tested core.

## Done when
- One `sluice serve` fronts umans; all three clients route through it by base-URL swap.
- A deliberately induced phantom (open a stream, kill the client) is **prevented** (upstream
  cancelled) and, if forced in via a second raw client, **absorbed** (gate shrinks on the
  next poll) — observed live against `/v1/usage`.
- Streaming verified end-to-end through a real client (tokens arrive incrementally), not
  just in unit tests.
- CI green on 3.12 + 3.13.

## Validation notes
- The honest test is **live against umans**, watching `/v1/usage.concurrent_sessions` —
  unit tests can't prove the phantom/box behaviour (per the family lesson that
  positive live validation catches what negative/mocked paths hide).
- Don't validate the box path by actually getting boxed; assert the `boxed_until` branch
  with a synthetic usage payload, and verify the *avoidance* live (target=3 keeps
  `concurrent_sessions` ≤ 4 under multi-client load).
