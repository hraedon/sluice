# The concurrency model

This is sluice's design spine: it defines the data the controller reasons over and the
exact decision it makes. Everything else (the proxy, the CLI, the metrics) is plumbing
around the function described here. Read this before touching code.

## 1. The provider's enforcement model (umans Code)

umans enforces **concurrent requests in flight**, surfaced by `GET /v1/usage`:

```json
{
  "limits": {
    "concurrency": { "limit": 4, "hard_cap": 8, "burst_pct": 1.0 },
    "requests":    { "limit": 200, "hard_cap": 400, "window_seconds": 18000 }
  },
  "usage": {
    "concurrent_sessions": 1,
    "requests_in_window": 48,
    "remaining_requests": 152,
    "priority": { "low": false, "boxed_until": null, "reason": null }
  }
}
```

The enforcement ladder, by observed `concurrent_sessions`:

| band | range (Max defaults) | provider behaviour |
|---|---|---|
| **normal**  | `0 .. limit` (≤4)        | full priority |
| **low**     | `limit .. hard_cap` (4–8) | `priority.low = true`, deprioritised routing |
| **reject**  | `> hard_cap` (>8)        | HTTP **429** concurrency errors |
| **boxed**   | (accumulated) >10 concurrency-429s in a day | **5-hour pause**, `boxed_until` set; self-reactivate ≤5×/week |

Key consequences that shape the design:

- **The box is not a single overshoot.** It's *accumulated* 429s. So the failure is
  gradual: you must avoid a sustained march of 429s, not just one spike. That makes a
  closed-loop controller (vs. a one-shot limiter) the right tool.
- **On Code Max the request window is moot** ("unlimited tokens, no request window").
  Concurrency is the *only* axis. (Code Pro adds a 200-req / 5h window; sluice models it as
  a second, independent gate but it is secondary.)
- **`concurrent_sessions` is ground truth, including phantoms.** This is the single most
  important field. Local in-flight bookkeeping can disagree with it (phantoms from
  disconnects, lag); when they disagree, **the provider's number is authority.**

## 2. State sluice tracks

Pure data, all held/derived in `sluice.control`:

| symbol | meaning | source |
|---|---|---|
| `target` | the concurrency sluice aims to keep `concurrent_sessions` at or below | config; default = `limits.concurrency.limit` (stay in *normal*, never even *low*) |
| `hard_cap` | provider reject threshold | usage reading |
| `local_in_flight` | permits currently held by sluice | the semaphore |
| `observed` | `usage.concurrent_sessions` | last `/v1/usage` poll |
| `usage_age` | seconds since the usage reading | shell clock |
| `phantom_estimate` | windowed: `max(0, min over K polls of (observed − local_in_flight))` — **sustained** excess (a transient lag spike appears in one sample and is dropped by the `min`); instantaneous `max(0, observed − local_in_flight)` is only the per-sample building block | derived |
| `priority_low`, `boxed_until`, `resets_at` | provider priority signals | usage reading |
| `breaker` | closed / open / half-open | 429 + error history |
| `recent_429s_today` | rolling count of concurrency-429s | response stream |

## 3. The decision (the pure function)

`effective_permits(state) -> int` — how many permits the gate should currently allow:

```
if boxed_until is set and now < resets_at:
    return 0                       # gate closed; clients get 503 + Retry-After

if breaker is open:
    return 0                       # back off until half-open probe succeeds

base = target
if priority_low:                   # already in the 'low' band → drain back under target
    base = max(min_floor, target - 1)

# absorb phantoms we did not create (only when the usage reading is fresh enough)
if usage_age <= usage_fresh_ttl:
    base = base - phantom_estimate
else:
    base = min(base, target - stale_penalty)   # stale → don't assume zero phantoms

return clamp(base, min_floor, target)
```

Properties this guarantees (and that tests assert):

- **Monotone-safe under uncertainty.** Every uncertain input (`priority_low`, staleness,
  breaker, box) can only *lower* the result. Never widens the gate on bad information.
- **Phantom-absorbing.** If umans sees 6 and sluice holds 4 *across the whole window*,
  `phantom_estimate = 2`, so the gate shrinks to `target − 2`, letting the phantoms age out
  of umans' window before sluice adds more. As phantoms clear, `observed` falls, the gate
  reopens. The window means a single lagged sample (one just-completed request still counted
  in `observed`) does **not** throttle — only excess that persists across K polls does
  (Plan 003 truth-path correctness; replaced the original instantaneous estimate that
  over-throttled under churn).
- **Pure.** `now`, `usage_age`, and the reading are arguments. No I/O, no global clock.

## 4. Admission, release, and the two timescales

Two control loops at different speeds — the classic fast-inner / slow-outer pattern:

- **Fast (synchronous, per request):** acquire a permit from a semaphore sized to the
  latest `effective_permits` before forwarding; block (with a bounded queue timeout) when
  full. Release the permit when the upstream request completes **or** the downstream client
  disconnects. On disconnect, cancel the upstream request so no phantom is born.
- **Slow (background, every `poll_interval`):** poll `/v1/usage`, recompute
  `effective_permits`, and resize the semaphore. Update breaker and box state.

The poll is lagged, so it can never be the *admission* gate by itself — it tunes the fast
gate's size. Admission is always the synchronous semaphore; truth only adjusts its width.

### Release cooldown

Borrowed from ai-concurrency-shaper: a freed permit is not immediately reusable; it rests
for `release_cooldown` so umans' own accounting can decrement before sluice fills the slot
again. This blunts the lag race that turns a clean release into an apparent overshoot.

## 5. Phantom handling: prevent first, absorb second

1. **Prevent (primary):** sluice is a single well-behaved upstream client. On downstream
   disconnect it cancels/closes the upstream stream cleanly, so umans sees a terminated
   request, not an abandoned one.
2. **Absorb (backstop):** reconciliation (§3) catches phantoms that prevention missed —
   including ones created *outside* sluice or by provider-side bugs.

Prevention shrinks the problem; absorption guarantees correctness even when prevention or
the provider is imperfect.

## 6. What sluice deliberately does not model

- Request *content*, tokens-per-request, or cost. sluice gates on count alone.
- Fairness beyond FIFO. Home-lab scale; a single FIFO queue across clients is enough.
  Per-client weighting is a possible later extension, explicitly out of the first build.
- Model routing / failover. One upstream, passthrough only.

## 7. Fairness and head-of-line blocking

sluice admits through a **single FIFO queue** across every client. This is deliberate
for home-lab scale: a single queue is simple, fair in the "first come, first served"
sense, and avoids the complexity of per-client weighting.

**The trade-off:** under saturation (all permits held), an interactive request (e.g. an
open-webui chat turn) can queue behind long-running agent requests (opencode, hermes) and
wait up to `queue_timeout` (~30 s) before receiving a `503`. This is **known, expected, and
bounded by `queue_timeout`** — it is not a bug.

**Mitigation (opt-in):** `--reserve interactive=1` reserves a small slice of the permit
pool for an "interactive" class so an agent flood cannot drive it to zero. A request in a
reserved class may use the reserved slot(s) *or* the shared pool; a non-reserved request may
use only the shared pool. The reserved floor only *bites* under saturation — below saturation
it is invisible. Classification keys on a `x-sluice-client-label` header (stripped before
forwarding per Rule 7), falling back to the default class. Without `--reserve`, behaviour
is exactly the old FIFO.

**When to consider weighted fair queuing:** only if a real pilot shows interactive requests
actually timing out behind agent traffic *despite* the reserved floor. That would be a
future extension (Plan 005 Stage 2), not a 1.0 concern.
