# Plan 006 — Multi-provider (truth-source abstraction + Anthropic/OpenAI-compatible upstreams)

Extend sluice from a single umans front to **any Anthropic-/OpenAI-compatible upstream**,
without touching the transparent data plane or the cache-transparency / inert-in-path
guarantees. The data plane is *already* provider-neutral (any path forwarded byte-for-byte;
both surfaces gated identically; both auth shapes supported). What does **not** generalize is
the **truth signal**: umans uniquely exposes a pollable live concurrency count
(`/v1/usage.concurrent_sessions`). Anthropic and OpenAI do not — they enforce token/request
*buckets*, surface state in **response headers** (`anthropic-ratelimit-*`, `x-ratelimit-*`)
and `429 + Retry-After`, and have no "boxed" concept. So this plan abstracts *how truth is
obtained* and *which controller strategy applies*, keeping umans as the reference
concurrency-reconcile provider.

Goal: **`sluice serve --provider {umans|anthropic|openai|generic}` fronts the chosen
upstream; umans behaves exactly as before (regression-gated), and a provider with no
concurrency reading is governed by a header/429-driven adaptive controller — all without
reading a single response body.**

> **Sequencing gate:** land *after* the umans path (Plans 003–005) is **live-validated**.
> Live validation of the umans controller is what tells us where the `TruthSource` /
> controller boundary actually belongs; abstracting on theory risks a `LimitState` shaped
> around guesses (the family's "scaffolding propagates drift" lesson). Depends on Plan 003
> (reuses its `gate_closed_reason` / `Retry-After` machinery for header-driven backpressure).

## What does and doesn't carry off umans

| | umans | Anthropic / OpenAI | generic compatible |
|---|---|---|---|
| truth signal | polled `/v1/usage` (concurrent_sessions) | **in-band response headers** + 429/Retry-After | none → local + 429 only |
| enforcement | concurrency bands + box | token/request bucket, no box | unknown |
| controller | `ConcurrencyReconciler` (phantom-absorbing) | `AdaptiveRateController` (AIMD) | `AdaptiveRateController` |

**Honest scope:** sluice's *differentiator* (reconcile against a real concurrency reading)
is umans-specific and does not transfer — off umans there is no concurrency ground truth to
absorb phantoms against. What *does* carry over is still worth having: the **single
cross-host choke point** (the account-wide-invariant argument holds for any shared key), the
**circuit breaker**, **Retry-After respect**, and the **QoS fair queue**. State this plainly
in the docs so the reduced guarantee off umans is on record, not discovered.

## Design constraints (carried from AGENTS.md)

- **Cache-transparency + inert-in-path (rules 4/7).** Per-provider control reads response
  **headers only** — never a body. The request on the wire is unchanged regardless of
  provider. Token-level accounting (which *does* require the body) is **not** a control
  input; it lives behind the explicit, default-off seam in WI-006 and is owned by
  observability, not the controller.
- **Pure core stays pure.** Each controller strategy is a pure function over a normalized
  `LimitState`; `now`, ages, and observations are arguments. `test_import_boundary` still
  passes — the provider registry and truth sources live in the shell.
- **Fail safe per provider.** Missing/unknown/stale limit state tightens the gate for
  *every* provider; an unrecognized provider config refuses to start rather than guessing.
- **Both surfaces, identically gated, per provider.** `--provider` selects truth + control,
  not which routes are gated.

## Work items

### WI-001 — Normalize the observed state (`sluice.control`)
- Generalize `UsageReading` → `LimitState`: optional **concurrency** fields
  (`concurrent_sessions`, `limit`, `hard_cap`, `priority_low`, `boxed_until_epoch`,
  `resets_at`) *and* optional **token-bucket** fields (`requests_remaining`,
  `tokens_remaining`, `bucket_reset_epoch`), plus `age_seconds` and a `provider` tag.
- Keep the umans concurrency fields as the existing dataclass shape (alias `UsageReading` →
  `LimitState` so Plans 001/003 code and tests don't churn).
- Pure; no new imports. Tests: a concurrency-only state and a bucket-only state both
  round-trip and classify without the other's fields.

### WI-002 — `TruthSource` protocol + provider registry (`sluice.providers`)
- `TruthSource` protocol: `current(now) -> CachedReading`. Three implementations:
  - `PolledTruthSource` — today's `UsageClient` (umans `/v1/usage`), unchanged behaviour.
  - `HeaderTruthSource` — holds the latest `LimitState` built from response ratelimit
    headers; updated in-band by the proxy (WI-004), never polls. LKG + staleness identical
    in spirit to the umans cache: a header reading ages and, once stale, tightens.
  - `NullTruthSource` — no external truth; reflects only local in-flight + breaker (for
    `generic`).
- `Provider` adapter bundles: default `base_url`, auth header shape (`x-api-key` +
  `anthropic-version` for Anthropic; `Authorization: Bearer` for OpenAI/umans), the
  `TruthSource`, the controller strategy, and a 429 classifier. A registry keyed by
  `--provider`.
- Tests: registry resolves each provider to the right bundle; an unknown provider raises.

### WI-003 — Controller strategies behind one interface (`sluice.control`)
- Refactor the current decision into `ConcurrencyReconciler` (umans: bands, phantom
  absorption, box) — behaviour-preserving; the existing controller tests must pass
  unchanged (this is the regression gate).
- Add `AdaptiveRateController` for header/429-driven providers: **AIMD** over permits —
  additive-increase toward `target` while budget headers are healthy and no recent 429s;
  multiplicative-decrease on a 429, on `tokens_remaining`/`requests_remaining` falling below
  a fraction of the bucket, or on a `Retry-After`. Pure; monotone-safe under uncertainty
  (every bad signal can only lower permits), same contract as `effective_permits`.
- Both strategies share the existing breaker. Tests: AIMD increases under healthy budget,
  backs off on 429 and on low-remaining, never exceeds `target`, and tightens on stale
  headers.

### WI-004 — Proxy feeds response headers to truth (`sluice.proxy`, `sluice.reconcile`)
- Add `record_response_headers(headers, status)` called once per proxied response. It
  parses an **allowlist** of ratelimit / `retry-after` headers into the `HeaderTruthSource`
  and routes `Retry-After` into Plan 003's `gate_closed_reason` / `retry_after_seconds`.
- **Headers only, allowlisted** — the body is never read; downstream still receives all
  response headers unchanged (we *read* a copy, we don't strip them from the client).
- For `PolledTruthSource` providers (umans) this is a no-op — the poll remains the truth.
- Tests: an Anthropic-style 429 with `Retry-After` closes the gate with reason `rate_limit`
  and the matching Retry-After; healthy `*-remaining` headers feed the adaptive controller;
  a umans response leaves the polled truth untouched.

### WI-005 — Per-provider config + `--provider` (`sluice.cli`)
- `--provider umans|anthropic|openai|generic` (default `umans`), each with sane defaults
  (base URL, auth, truth mode, controller, `target`); flags still override.
- `generic` = `NullTruthSource` + `AdaptiveRateController`: a safe fallback for any
  Anthropic-/OpenAI-compatible endpoint (OpenRouter, LiteLLM, Bedrock/Vertex Anthropic,
  z.ai, a local gateway) — concurrency choke point + breaker + QoS, no truth poll.
- `sluice status` prints the active provider, truth mode, and controller.
- Tests: each provider resolves end to end; `generic` runs with no usage endpoint.

### WI-006 — Optional usage-tap seam (default OFF — the answer to "but token counts?")
The one thing header-only cannot give is **per-request token counts** (they live in the
response body: Anthropic's terminal `message_delta.usage`; OpenAI's final chunk under
`stream_options.include_usage`). This is precluded by *our guarantee*, not by physics — so
expose it as an explicit, bounded relaxation rather than smuggling it into the control path.

- `--usage-tap` (default **off**): when on, extract **only** the terminal `usage` object
  from a response (tee the final SSE event / read the final JSON's `usage` key) and emit it
  to observability — handed to [[project-usage-dashboard]], **never** consumed by the
  controller.
- Must not buffer the stream (tee the final event only), must not alter forwarded bytes, and
  is a documented, opt-in exception to inert-in-path — clearly flagged as such in `--help`
  and docs.
- Prefer the provider's own billing/usage API for token accounting where one exists; the tap
  is the fallback for compatible upstreams that have none.
- Tests: off by default; **with it off, assert no response body byte is ever inspected**
  (the inert-in-path guarantee holds unless explicitly relaxed); with it on, the forwarded
  bytes are still identical and only the `usage` object is surfaced.

### WI-007 — Per-provider test matrix + live validation
- Unit: header→`LimitState` parsing per provider; AIMD trajectory; umans regression
  (controller traces identical pre/post abstraction).
- Live: each provider validated against its real endpoint (per AGENTS.md #6 — mocks can't
  prove header semantics). For Anthropic, observe `*-ratelimit-*` headers under normal load
  and one *controlled* low-`max_tokens` burst to see a single 429 → back-off, **without** a
  429-storm.

## Sequencing
WI-001 → WI-002 → WI-003 (umans refactored behind the abstraction, existing tests green =
regression gate) → WI-004 → WI-005 → WI-007. **WI-006 last and optional** — only if
observability actually needs per-request tokens the provider's own API can't supply.

## Done when
- `--provider umans` produces **byte-identical control behaviour** to today (effective-permit
  traces diff clean — the regression gate).
- An Anthropic key fronted via `--provider anthropic` adaptively limits on its ratelimit
  headers + 429/Retry-After, **reading no response body**, and respects Retry-After via the
  Plan 003 path.
- `--provider generic` fronts an arbitrary Anthropic-/OpenAI-compatible endpoint as a shared
  choke point + breaker + QoS with no truth poll.
- Cache-transparency (Plan 005 WI-000) and inert-in-path tests still green; with
  `--usage-tap` off, no body is read.
- CI green on 3.12 + 3.13.

## Validation notes
- **umans regression is the primary gate.** The abstraction must not change umans behaviour;
  diff `effective_permits` traces over a scripted reading sequence before and after.
- **Be honest about the reduced guarantee off umans.** Document that non-umans providers get
  feedback-adaptive control, not truth-reconciliation — there is no concurrency ground truth
  to absorb phantoms against. The differentiator is umans-specific; the *shared choke point*
  is the portable value.
- Don't 429-storm a real provider to test back-off; provoke at most one controlled 429 and
  assert the single back-off, then rely on the pure AIMD tests for the rest.
