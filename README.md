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
| daily concurrency-429 allowance exceeded | **boxed** — 5-hour pause (`boxed_until`), limited self-reactivations |

Ladder and numbers per the [official usage docs](https://app.umans.ai/offers/code/docs#usage).
The exact thresholds drift (the docs say the box trips past 10 concurrency-429s a day;
the dashboard currently shows a 20-hit daily allowance) — sluice doesn't hard-code any of
them. It reacts to what `/v1/usage` itself reports (`priority.low`, `boxed_until`), so the
ladder can move without a code change.

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
- **Remembers the trend.** Every tick lands in a bounded in-memory history
  (dashboard sparklines + `/history.json`), optionally persisted to SQLite
  (`--history-store`) so a restart doesn't wipe the picture of what led up to it.
  The dashboard renders it at 5m/1h/4h ranges: concurrency series, queue depth,
  a band ribbon, and tick marks where queue timeouts or 429s actually happened.

![sluice live dashboard during a deliberate fleet overload: local in-flight pinned at the limit of 4, provider-observed sessions below it, deep queue, zero 429s](docs/dashboard.png)

*The dashboard mid-way through a deliberate fleet overload (~4× oversubscribed): local
in-flight (yellow) rides the limit of 4 and never crosses it, the excess demand shows up
as queue wait instead of provider 429s — `total_429s: 0` — and the gap between the
provider's observed count (blue) and local truth is exactly the kind of divergence the
reconciliation loop exists to watch.*

## Quickstart

sluice needs an **upstream URL** and an **API key it can use to poll `/v1/usage`** (the same
key your clients send). It listens on `:8800`, serves a live dashboard at `/`, and proxies
every other path straight through to the upstream.

**pip / pipx** (not on PyPI — install from git):

```sh
pip install "sluice @ git+https://github.com/hraedon/sluice.git@main"
export SLUICE_USAGE_KEY=sk-...        # key used only for /v1/usage polling
sluice serve --upstream https://api.code.umans.ai --listen 127.0.0.1:8800
```

**Docker** (the `ENTRYPOINT` is `sluice`, so pass only the subcommand):

```sh
docker build -t sluice:local .
docker run --rm -p 8800:8800 -e SLUICE_USAGE_KEY=sk-... sluice:local \
  serve --upstream https://api.code.umans.ai --listen 0.0.0.0:8800
```

Then point your clients (opencode, open-webui, …) at `http://127.0.0.1:8800` instead of the
provider, and open `http://127.0.0.1:8800/` for the dashboard. `--target` is the concurrency
sluice aims to hold (default **3**, one below umans Code Max's limit of 4 — pass `--target 4`
to use the full limit, trading the safety buffer). See `docs/client-configuration.md` for
per-client setup and `deploy/` for the Kubernetes / ArgoCD manifests.

## Why not LiteLLM (or nginx, or a Redis semaphore)?

Because every generic concurrency limiter counts **what you sent** — its own in-flight
requests. sluice counts **what the provider sees** — `concurrent_sessions` from
`/v1/usage`, reconciled every few seconds. That difference is the entire point:

- A local semaphore (nginx `limit_conn`, a Redis counter, LiteLLM's parallel-request cap)
  is blind to **phantoms** — sessions the provider still counts as live after a client
  disconnects. They exist only in the provider's counter, and they're exactly what tips you
  over the cliff. sluice shrinks its own permits to absorb an excess it didn't create
  (`effective = target − max(0, observed − local_in_flight)`); a limiter that only knows its
  own number can't see the gap.
- sluice models the provider's **specific enforcement ladder** — `priority.low` at the
  limit, 429s past `hard_cap`, and the day-scale **penalty box** (5-hour pause, a handful of
  self-reactivations per week). It holds one slot below the cliff and respects `boxed_until`
  instead of hammering a locked account. A generic limiter has no concept of a box.

If your provider doesn't expose a live concurrency reading — or you don't share one account
across hosts — you probably **don't** need sluice; reach for LiteLLM or `limit_conn`. sluice
earns its place only where the provider's own count is the number that punishes you, and
that number can drift from yours.

## Scope

**In:** reverse-proxy data plane (streaming, both surfaces); a deterministic concurrency
controller (bands, reconciliation, permit math); the `/v1/usage` reconciliation loop;
release cooldown + circuit breaker; minimal operational metrics.

**Out:** request *content* inspection, prompt logging, caching, or model routing — sluice
is a concurrency governor, not a gateway. It does not transform bodies.

**Non-goals:** being a general API gateway; per-prompt billing/analytics (that's a separate
observability concern, read-only); reselling concurrency by
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

Status: **1.0 — deployed and live** (internal-only, GitOps via ArgoCD); live-validated against
real streaming agent traffic (opencode → umans on the OpenAI surface: 200s, zero 429s, not
boxed). See `docs/concurrency-model.md` for the data model,
`docs/client-configuration.md` to point clients at it, and `deploy/README.md` for the
deployment and the external-exposure toggle.
