# AGENTS.md — sluice

Conventions and hard rules for working in this repo. sluice is part of the
cert-watch / gpo-lens / adcs-lens tool family, but it is the family's **first in-path
tool** — it sits in the live request path rather than analysing copies after the fact.
Some family tenets adapt; the adaptations are stated explicitly below so they aren't
silently dropped.

## Family conventions (adapted for an in-path tool)

- **Deterministic, stdlib-only core — no AI in the truth path.** The concurrency decision
  logic lives in `sluice.control` as pure functions over plain data: bands, permit math,
  reconciliation, breaker state. No I/O, no async, no httpx, no model calls. It must be
  unit-testable with no network. An import-boundary test enforces that `sluice.control`
  imports nothing outside the stdlib.
- **Thin shell around the core.** The async reverse proxy (`sluice.proxy`), the usage
  poller (`sluice.usage`), and the CLI (`sluice.cli`) import `control`, never the reverse.
  The shell does I/O; the core decides. Same deterministic-core / optional-extras shape as
  the siblings, re-authored for this domain.
- **Read-only → does not apply literally; replaced by "inert in-path."** sluice forwards
  live traffic, so it is *not* read-only. The transferred guarantee is: sluice **never
  reads, logs, stores, or rewrites request/response bodies.** It gates and streams bytes
  through untouched. It is a concurrency governor, not a gateway or a logger.
- **Flag-don't-probe → "use the sanctioned signal, don't probe limits."** Never discover
  the provider's limits by deliberately overshooting. The only truth source is the
  provider's documented usage endpoint (`GET /v1/usage`), polled at a sane cadence.
- **No work-domain identifiers in committed files.** Placeholders only. The umans API base
  URL is public and fine to commit; account keys, host names, and any work-domain
  identifiers are not. `samples/` (if ever added) is gitignored and never committed.

## Hard rules

1. **Fail safe.** Any uncertainty — stale or unreachable `/v1/usage`, breaker open,
   `boxed_until` set, a parse error — must **tighten** the gate (fewer permits / closed),
   never loosen it. Defaults bias toward staying out of the box, not toward throughput.
2. **The core is pure.** No clock, no randomness, no I/O inside `sluice.control`. Pass time
   and observations in as arguments so decisions are reproducible and testable. If you need
   `time.monotonic()`, it's read in the shell and handed to the core.
3. **Streaming is sacred.** Both routes stream Server-Sent Events. Never buffer a full
   response body; proxy bytes as they arrive. A change that breaks token streaming is a
   regression even if tests pass.
4. **Release on disconnect, and cancel upstream.** A permit is held for the life of the
   upstream request and released when it completes *or* when the downstream client
   disconnects — and on disconnect, the upstream request is cancelled (clean close), so
   sluice does not itself manufacture the phantoms it exists to prevent.
5. **Both surfaces, identically gated.** `/v1/messages` and `/v1/chat/completions` are the
   same concurrency unit; the gate is surface-agnostic. Don't special-case one.
6. **Validate on CI early; distrust green local gates.** Push to a branch and watch CI
   (3.12 + 3.13) before trusting. Async/streaming behaviour is easy to get locally-green
   and actually-broken.
7. **Cache-transparency — be indistinguishable from a direct client.** Prompt caching
   lives entirely upstream and is keyed off the request the provider receives. So the
   request sluice *egresses* must be byte-for-byte what the client sent — same body bytes
   (never parse/re-serialise/reorder/buffer a body), same content and `anthropic-*` /
   cache-control / `authorization` headers — minus only hop-by-hop headers, plus **nothing
   sluice-internal**. Any sluice control header (e.g. a QoS client label) is consumed and
   **stripped before forwarding**, never sent upstream. We don't know umans' cache
   internals and don't need to: a request through sluice must hash identically to the same
   request sent directly, so whatever the provider caches is unaffected by our presence.
   The only caching effect sluice may have is *timing* (queuing/throttling can space a
   client's turns past the provider's cache TTL) — that is a provisioning cost, surfaced in
   metrics, not a transform of the request.

## Layout

```
src/sluice/
  control.py   # PURE deterministic core: bands, permit math, reconciliation, breaker
  usage.py     # /v1/usage client + parser (borrows usage-dashboard's umans logic)
  providers.py # provider registry: truth sources + controller choice per upstream
  proxy.py     # async reverse proxy shell (streaming, both routes, disconnect→cancel)
  admin.py     # admin route handlers: health, ready, status, metrics, history, dashboard, static
  gate.py      # resizable permit gate (FIFO queue, optional QoS reserve)
  reconcile.py # background loop: fetch truth → core decides → resize gate
  status.py    # point-in-time snapshot for /status.json and /metrics
  history.py   # bounded ring buffer of per-tick snapshots (/history.json, sparkline)
  history_store.py # optional fail-safe SQLite persistence for history (--history-store)
  singleton.py # single-instance guard (the semaphore only works if there's one)
  cli.py       # `sluice serve ...` entry point
  static/      # dashboard assets (css, fonts, theme.js, dashboard.html)
tests/
  test_control.py        # pure-core unit tests, no network
  test_import_boundary.py # control imports stdlib only; shell→core one-way
docs/concurrency-model.md  # the design spine — read this first
plans/                     # numbered implementation plans
```

## Don't

- Don't put the limit decision in the proxy layer "to save a function call." The decision
  is the asset; keep it pure and isolated.
- Don't fail open when `/v1/usage` is unreachable. Hold the last-known-good reading for a
  bounded TTL, then tighten — never assume zero phantoms.
- Don't add response caching, prompt logging, or model routing. Those are out of scope and
  break the "inert in-path" guarantee.
- Don't use kustomize `commonLabels` in `deploy/k8s/` — it rewrites selectors too,
  including the NetworkPolicy's `from.podSelector` for traefik, which blocks all ingress
  traffic (prod 502, 2026-07-02). Use `labels` (pairs, no `includeSelectors`) instead.
- Don't add, rename, reorder, or buffer anything on the wire to the upstream — not headers,
  not body bytes. The upstream's prompt cache keys off the exact request; a body sluice
  re-serialised (even with identical JSON, different key order/whitespace) is a different
  cache key and a silent cache miss. Classify QoS by metadata, never by peeking at or
  reshaping the body.
