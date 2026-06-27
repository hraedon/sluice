# sluice

A small, central reverse proxy that **meters concurrency** to an LLM API by reconciling
its own in-flight count against the provider's **usage endpoint as ground truth** — so a
fleet of agents on different hosts never collectively exceeds the provider's concurrency
limit and never trips its penalty box.

Built first for **umans Code** (`api.code.umans.ai`), but provider-neutral: any upstream
that exposes a concurrent-sessions reading can be fronted.

```
opencode    ─┐
open-webui  ─┼─▶ [ sluice ] ─▶ umans
hermes      ─┘       ▲
                     └── /v1/usage  (concurrent_sessions = truth, incl. phantoms)
```

## Why this exists

umans Code enforces **concurrency**, not just rate. On Code Max the only meaningful limit
is "requests in flight" (`concurrency.limit: 4`, `hard_cap: 8`; no request window). The
enforcement ladder is:

| Observed `concurrent_sessions` | Consequence |
|---|---|
| ≤ `limit` (4) | normal |
| `limit`..`hard_cap` (4–8) | `priority.low` — deprioritised routing |
| > `hard_cap` (~8) | HTTP 429 concurrency errors |
| > 10 concurrency-429s **in a day** | **boxed** — 5-hour pause (`boxed_until`), self-reactivate ≤ 5×/week |

Two facts make a naive limiter insufficient:

1. **The cap is account-wide, but no single agent knows the global count.** Three agents
   on three hosts each stay under their own ceiling while the *sum* tips over. The only
   place a cross-host invariant can live is one shared choke point.
2. **Phantoms live upstream.** A recent umans bug counted client disconnects as still-live
   requests; those phantoms exist only in umans' counter, so a purely local semaphore
   can't see them. The `/v1/usage` reading can.

sluice is that shared choke point, and it closes the loop against upstream truth.

## What it does

- **One shared semaphore**, every client routes through it — the global concurrency
  invariant in one place.
- **Reconciles against `/v1/usage`.** A background loop reads `concurrent_sessions` and
  shrinks the effective permit count to absorb phantoms it didn't create
  (`effective = target − max(0, observed − local_in_flight)`).
- **Prevents phantoms** rather than only absorbing them: when a downstream client
  disconnects, sluice issues a clean cancel upstream instead of leaving a dangling stream.
- **Respects the box.** On `boxed_until` it closes the gate and returns `503 Retry-After`
  until `resets_at`, instead of hammering a locked account toward a longer pause.
- **Both API surfaces.** Transparent streaming passthrough for the Anthropic
  (`/v1/messages`) and OpenAI (`/v1/chat/completions`) routes.

## Scope

**In:** reverse-proxy data plane (streaming, both surfaces); a deterministic concurrency
controller (bands, reconciliation, permit math); the `/v1/usage` reconciliation loop;
release cooldown + circuit breaker; minimal operational metrics.

**Out:** request *content* inspection, prompt logging, caching, or model routing — sluice
is a concurrency governor, not a gateway. It does not transform bodies.

**Non-goals:** being a general API gateway; per-prompt billing/analytics (that's
[[project-usage-dashboard]]'s job — observability, read-only); reselling concurrency by
key-rotation (sluice exists so you *don't* have to rotate keys or buy a concurrency pack).

## Boundary vs. siblings

- **usage-dashboard** *observes* umans usage (read-only display). sluice *enforces* against
  the same signal (in-path). Clean split: dashboard watches, sluice acts. sluice borrows
  the dashboard's umans-usage parser; it does not depend on the dashboard.
- **ai-concurrency-shaper** (joeycumines, Go) is the off-the-shelf local semaphore +
  cooldown + breaker. sluice borrows those ideas and adds the one thing it lacks:
  reconciliation against the provider's own usage reading. Run the shaper at concurrency=3
  as a zero-build stopgap; sluice is the truth-aware successor.

## Design principles

1. **Deterministic core, no AI in the truth path.** The concurrency decision is pure
   stdlib functions over observed state — testable without a network or a model.
2. **Upstream truth wins.** Local bookkeeping is a fast approximation; the provider's
   reading is authority, and divergence is treated as phantoms to absorb.
3. **Fail safe, not open.** Uncertainty (stale usage, breaker open, box) tightens the gate.
4. **In-path but inert.** sluice gates and cancels; it never reads, stores, or rewrites
   request content.

Status: **charter**. See `docs/concurrency-model.md` for the data model and
`plans/001-data-plane-and-reconciliation.md` for the first build.
