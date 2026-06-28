# Plan 005 — Client fairness under load (don't starve the human behind the agents)

sluice admits through a **single FIFO** across every client (`PermitGate`). With the real
mix — interactive open-webui chat alongside long-running agent runs on hermes/opencode — a
burst of agent requests can fill every permit and an interactive request then waits the full
`queue_timeout` (~30 s) and gets a `503`. The concurrency model explicitly defers per-client
fairness ("Home-lab scale; a single FIFO ... is enough"), and for a first build that's the
right call. But it is the **second thing real use will surface**, and the failure is a UX
cliff for the one client a human is watching.

This plan is deliberately **two-staged and gated**: document the behaviour and ship a cheap
floor *now*; build the full weighted queue **only if a pilot actually shows starvation**.
Do not build Stage 2 speculatively.

## Design constraints (carried from AGENTS.md)

- **The global cap is sacred.** Any fairness scheme partitions *who* gets the permits, never
  *how many exist*. The account-wide invariant (sum of admissions ≤ `effective_permits`) is
  unchanged — classes share one pool, they do not each get their own cap.
- **Inert in-path.** Classification keys on connection *metadata* (a configured client
  label, or source host from `docs/client-configuration.md`'s host→client map) — never on
  request body or prompt content.
- **Fail safe.** An *unclassified* connection falls into the lowest-priority default class,
  never a privileged one. Misconfiguration cannot promote a client.
- **Pure core untouched.** Fairness is an admission-ordering concern in the shell
  (`sluice.gate`/`proxy`); `sluice.control`'s permit *math* does not change.
- **Cache-transparency (AGENTS.md hard rule 7).** QoS classification keys on a sluice
  control header (or host map), and that header is **consumed and stripped before the
  request is forwarded upstream** — it must never reach umans, so the upstream's prompt
  cache key is identical with or without sluice in path. QoS changes *ordering*, never the
  bytes on the wire.

## Stage 1 — document + cheap floor (do now)

### WI-000 — Lock cache-transparency first (`tests`, `sluice.proxy`)
Independent of the rest of this plan, and worth landing **before** any header-classification
logic exists — it is the regression net that keeps QoS from silently breaking upstream
prompt caching.

- A `_CONTROL_HEADERS` strip set in the proxy (alongside `_HOP_BY_HOP`) for sluice's own
  headers (the QoS client label, any future internal header). Request egress filters out
  both sets; the upstream sees neither.
- Tests, asserting sluice is **wire-indistinguishable from a direct client**:
  - **Body byte-identity:** the bytes forwarded upstream equal the bytes the client sent,
    exactly — same order, same whitespace (catches any future "parse the body to classify
    by model" refactor that would change the cache key). Use a body whose re-serialisation
    would differ (non-sorted keys, specific spacing) so the test actually bites.
  - **Header passthrough:** `anthropic-*`, `authorization` / `x-api-key`, content headers,
    and arbitrary client headers reach the upstream unchanged; only hop-by-hop + `host` +
    `_CONTROL_HEADERS` are dropped.
  - **No sluice header leaks:** a request carrying the QoS client-label header forwards
    upstream **without** that header.
- This WI has no dependency on WI-001/002 and should ship even if Stage 1's floor slips.

### WI-001 — Name the behaviour (`docs/concurrency-model.md`, README)
- Add a short "Fairness and head-of-line blocking" subsection to §6 ("What sluice
  deliberately does not model"): single FIFO across clients; under saturation an interactive
  request can queue behind agent runs and time out; this is **known, expected, and bounded
  by `queue_timeout`**. State the mitigation (WI-002) and the trigger for Stage 2.
- This is the "document now" half of the gap — make the trade-off explicit so it's a
  decision on record, not a surprise in the pilot.

### WI-002 — Per-class reserved floor (cheap, opt-in)
A minimal mitigation that needs no weighted-fair-queue machinery: reserve a small slice of
the pool for an "interactive" class so an agent flood cannot drive it to zero.

- Config: optional `--reserve interactive=1` (default: none → today's pure FIFO, no
  behaviour change). Classes come from a `--client-label` the client sends via a header
  sluice reads (set per client in `client-configuration.md`), falling back to a source-host
  map, falling back to the default class. The label header is stripped before forwarding
  (WI-000) so it never reaches umans or perturbs the upstream cache key.
- Admission rule: a request in a reserved class may use the reserved slot(s) **or** the
  shared pool; a request in a non-reserved class may use only the shared (non-reserved)
  pool. The reserved floor only *bites* under saturation; below saturation it is invisible.
- Keep it inside `PermitGate` as a per-class available-count check, or a thin wrapper — the
  global capacity and resize logic are unchanged.
- Tests: with `reserve interactive=1` and the pool saturated by the default class, an
  interactive request is admitted (reserved slot) while a second default request waits;
  with no reserve configured, behaviour is byte-for-byte the old FIFO.

## Stage 2 — weighted fair queue (build ONLY if the pilot shows starvation)

> Gate: do not start Stage 2 until a real pilot (or the Plan 002 dashboard under live load)
> shows interactive requests actually timing out behind agent traffic *despite* the WI-002
> floor. If the floor suffices, Stage 2 is YAGNI — record that outcome and stop.

### WI-003 — Weighted/round-robin admission across classes
- Replace strict FIFO with a small **deficit/weighted round-robin** over class queues so
  permits are shared in configured proportions (e.g. interactive : agents) rather than
  first-come-first-served, while still never exceeding `effective_permits` in total.
- Bounded per-class queues; a full class queue fast-fails with `503` rather than growing
  unbounded.
- Tests: under sustained saturation, long-run admission ratio across classes tracks the
  configured weights within tolerance; the global in-flight count never exceeds
  `effective_permits` (the invariant assertion).

### WI-004 — Expose fairness in status/metrics (extends Plan 002)
- Per-class counters (in-flight, queued, admitted, timed-out) on `/metrics` and the
  dashboard, so an operator can *see* whether a class is being starved — the signal that
  justified building Stage 2 in the first place. Counts/metadata only, no content.

## Sequencing
WI-000 (cache-transparency net — land first, it gates every later header change) → WI-001 →
WI-002 (ship Stage 1 together; that is the deliverable for "document now, fix later").
WI-003 → WI-004 are **conditional** on the Stage-2 gate above.

## Caching interaction (why QoS *helps* the cache, and its one risk)

QoS and upstream prompt caching pull the same direction for the client that matters:

- An interactive, multi-turn conversation is exactly the workload that benefits most from a
  warm prompt cache (each turn reuses the system+history prefix). Keeping it moving via the
  reserved slot keeps its inter-turn gap **short**, so the prefix stays inside the
  provider's cache TTL — the reserved slot protects cache *hit-rate*, not just latency.
- The one risk is timing, not transform: under heavy throttling (gate shrunk to 1) a
  request can sit in queue or get spaced out far enough that the upstream cache entry
  expires before it arrives → a cache *miss* (correctness fine, cost worse). Mitigations:
  keep `queue_timeout` (~30 s) well under any plausible cache TTL so one queued request
  never alone blows the window, and treat sustained throttling that spaces turns past the
  TTL as a *provisioning* signal (visible on the Plan 002 dashboard), not a sluice bug.
- sluice never re-issues or coalesces requests, so it can neither double-write nor poison a
  cache entry; it only ever delays or drops, never duplicates.

## Done when (Stage 1)
- The fairness behaviour and its bound are documented in the concurrency model.
- With a reserved interactive slot configured, an agent flood that saturates the pool no
  longer starves an interactive request (it takes the reserved slot); with no reserve set,
  the gate behaves exactly as before (regression-free).
- The global-cap invariant (total in-flight ≤ `effective_permits`) holds under all class
  configurations — asserted by test.
- CI green on 3.12 + 3.13.

## Validation notes
- The trigger for Stage 2 is *evidence*, not anticipation: the Plan 002 dashboard's
  per-class view (WI-004) or pilot logs showing interactive timeouts. Capture that signal
  before writing the weighted queue.
- Live check for Stage 1: drive agent saturation from hermes/opencode while issuing
  interactive open-webui requests; confirm the interactive request is admitted via the
  reserved slot and `/v1/usage.concurrent_sessions` still stays ≤ 4 (fairness must not cost
  the global invariant).
- Cache-transparency check (WI-000), live: send the *same* prompt directly to umans and
  through sluice; if umans reports cache metrics (e.g. cache-read tokens in the response),
  the second hit through sluice must register a cache **hit**, proving sluice didn't change
  the request the provider keys on. If umans surfaces no cache signal, fall back to the
  byte-identity unit test as the guarantee.
