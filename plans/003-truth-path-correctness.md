# Plan 003 — Truth-path correctness (phantom-lag + box-aware backpressure)

Two defects in the live decision path, both **before any live pilot** (per the Plan 001
validation note that unit-green ≠ in-path-correct):

1. **The phantom estimator over-throttles under normal churn.** `phantom_estimate =
   max(0, observed − local_in_flight)` (`control.py`) compares a *lagged* `/v1/usage`
   snapshot against the *current* live permit count. A request that completed moments ago
   has already left `local_in_flight` but still sits in `observed`, so it is scored as a
   phantom. Under steady high turnover sluice reads its own release-lag as foreign load and
   shrinks the gate below `target` for no reason — and this is the core loop the whole tool
   is built on.
2. **The box promise in the README is not kept by the code.** The charter says: on
   `boxed_until`, "returns `503 Retry-After` until `resets_at`." The proxy hardcodes
   `retry_after=5` on *every* 503 (`proxy.py` `_RETRY_AFTER_DEFAULT`), and when the gate is
   at 0 (boxed) a request blocks for the full `queue_timeout` (~30 s) before that 503. The
   result is clients told to retry every 5 s against a locked account for what may be hours
   — the exact sustained hammering the box rule punishes.

Goal of this plan: **sluice neither throttles itself on its own lag, nor invites clients to
march on a boxed account — the controller's two failure directions (fail-open phantom miss,
fail-closed self-throttle) are both bounded and tested with a deterministic simulation.**

## Design constraints (carried from AGENTS.md)

- **The core stays pure.** Both fixes keep `sluice.control` stdlib-only, clockless, I/O-free
  (`test_import_boundary` still passes). Any new windowed input is *computed in the shell*
  and handed to the core as an argument.
- **Fail safe is directional.** The phantom fix moves the bias from "over-tighten" toward
  "slightly-under-tighten"; the plan must prove the under-tighten is **bounded and
  transient**, with the breaker/box as the backstop. We do not trade a throughput bug for a
  box risk.
- **Use the sanctioned signal.** No probing. The box retry interval is read from the
  provider's own `resets_at`, never guessed.

## Work items

### WI-001 — Persistence-windowed phantom estimator (`sluice.control`)
Replace the instantaneous excess with a **sustained** excess, so transient self-lag washes
out while a real phantom (present every tick) survives.

- New pure helper: `phantom_estimate(samples: Sequence[tuple[int, int]]) -> int` where each
  sample is `(observed_i, local_in_flight_i)` captured *at the time reading i was taken*.
  Return `max(0, min_i(observed_i − local_in_flight_i))` over the window.
  - Rationale: sluice's own upstream sessions can lag but cannot exceed what sluice ever
    admitted, so a one-tick spike of `observed − local` from a just-completed request is
    transient and the windowed `min` drops it. A genuine phantom is present in *every*
    sample, so its excess is the floor the `min` selects.
- `effective_permits` consumes the windowed estimate instead of the single-reading one.
- Window length `K` (default 3) and the sample pairing live in config/shell, not the core.
- **Fail-safe bound:** a brand-new real phantom is only ignored for at most `K−1` polls
  (≈ `K × poll_interval`), strictly shorter than the breaker window and far shorter than the
  day-scale box accumulation — so the residual fail-open is bounded and the slow backstops
  still catch sustained overload.
- Tests (pure, no network):
  - Churn trace: admit/release rapidly while `observed` lags one tick high → estimate is 0,
    gate stays at `target` (the regression this WI exists to kill).
  - Sustained phantom: `observed` runs 2 over `local` for the whole window → estimate is 2,
    gate shrinks to `target − 2`.
  - Single-tick spike inside an otherwise-clean window → estimate stays 0.
  - Monotonicity preserved (a higher sustained `observed` never *raises* permits).

### WI-002 — Shell captures the reading↔local pairing (`sluice.reconcile`)
- On each `tick`, after `fetch`, record the pair `(reading.concurrent_sessions,
  gate.held)` into a bounded `deque(maxlen=K)`; pass the window to `effective_permits`.
- Pairing must use `gate.held` *as sampled this tick* (not a stale cache) so each
  `observed_i` is matched to the local count contemporaneous with that poll.
- Expose `phantom_estimate` (the windowed value) as a read-only property for `/metrics`
  and the Plan 002 ladder — the shaded gap should now reflect *sustained* phantoms.
- Tests (fake clock + fake usage source): feed a scripted sequence of readings and held
  counts; assert the window resizes the gate exactly as WI-001's pure tests predict, end to
  end through `tick`.

### WI-003 — Gate-closed reason + provider-derived Retry-After (`sluice.reconcile`)
The proxy needs to know *why* the gate is shut to answer the client correctly.

- `reconcile` exposes `gate_closed_reason() -> Literal["open","boxed","breaker","saturated"]`
  and `retry_after_seconds(now_wall) -> int`:
  - **boxed:** `ceil(resets_at − now)`, floored at a sane minimum (e.g. 30 s) and clamped to
    a max header value — the honest "come back after the box lifts."
  - **breaker open:** the remaining cooldown (`cooldown_seconds − elapsed`).
  - **saturated (transient queue-timeout):** the short default (`5 s`) — correct here.
- `resets_at` must be parsed and carried on the reading. `usage.parse_usage` currently
  decodes `boxed_until` but **not** `resets_at`; add it (ISO→epoch, same helper) and thread
  it onto `UsageReading`. Until a real boxed payload is on record, treat `resets_at` absent
  while `boxed_until` present as "boxed for an unknown interval" → use the floor.
- Tests: synthetic boxed reading → `gate_closed_reason == "boxed"` and `retry_after_seconds`
  tracks `resets_at`; breaker-open → cooldown remainder; saturated → 5 s.

### WI-004 — Proxy: fast-fail when shut, honest Retry-After (`sluice.proxy`)
- Before the bounded `acquire`, check `reconcile.gate_closed_reason()`. If it is `boxed` or
  `breaker`, **fast-fail immediately** with `503` + `Retry-After: reconcile.retry_after_
  seconds(...)` — do not burn the 30 s queue wait against a gate that cannot open.
- On a genuine `acquire` timeout (`saturated`), return `503` with the short Retry-After as
  today.
- Drop the hardcoded `_RETRY_AFTER_DEFAULT` from the boxed path; keep it only as the
  saturated-case default.
- The 503 body carries a machine-readable `reason` so a client (or the dashboard) can tell
  "try again shortly" from "the account is paused."
- Tests: boxed state → request returns 503 **without** waiting `queue_timeout` (assert it
  returns fast, e.g. under a small bound) and carries the long Retry-After; saturated →
  still waits then 503 with short Retry-After.

## Sequencing
WI-001 (pure estimator + its tests — freeze the math first) → WI-002 (wire the window
through the loop) → WI-003 (reason + Retry-After source) → WI-004 (proxy consumes it).
Estimator before plumbing, same as Plan 001's controller-first discipline.

## Done when
- A deterministic **simulation** (scripted admit/release/poll trace, no network — the core
  is pure, so this is just a test) shows: under churn at/under `target` the gate **does not
  dip below `target`**, and an injected sustained phantom **is** absorbed within `K` polls
  and **drains** when it clears. This sim is the artifact that proves gap #1 closed.
- A synthetic `boxed_until`/`resets_at` payload makes the proxy fast-fail with a
  `Retry-After` that tracks `resets_at`, not a fixed 5 s.
- `sluice.control` still imports stdlib only; `test_import_boundary` green.
- CI green on 3.12 + 3.13.

## Validation notes
- **Do not get boxed to test the box path.** Assert the boxed branch against a synthetic
  payload (per Plan 001's standing rule); the live proof is *avoidance*.
- The phantom fix's honest live check is the same one Plan 001 defers to: run multi-client
  load and watch `/v1/usage.concurrent_sessions` stay ≤ 4 **while** `/metrics`
  `effective_permits` no longer collapses to 1–2 during steady churn. The sim makes this
  predictable; the live run confirms the lag model matches umans' real accounting.
- Capture one real boxed payload if it ever occurs naturally (don't induce it) to confirm
  `resets_at` field names — the parser currently predates a real boxed sample.
