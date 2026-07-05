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

**`boxed_until` alone does not mean boxed.** Observed live (2026-07-03, see
`docs/wi-024-429-capture-2026-07-03.md`): a *single* limit hit sets
`boxed_until` for a 5-hour window with `priority.reason = "rate_limited"` —
and the account **keeps serving normally at low priority** (an in-window
probe returned 200 in 0.9 s). The `reason` field discriminates the rung:

- `reason == "rate_limited"` → **deprioritization** — classified as band
  *low*; the gate stays open, capped at the account `limit` (or `limit − 1`
  when `target` already consumes the full limit), never fully closed.
- `reason` absent or anything else → **hard box** — band *boxed*, gate
  closed (fail safe: an unrecognized penalty is treated as the worst case).

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
if hard_boxed:                     # boxed_until set, reason NOT "rate_limited"
    return 0                       # gate closed; clients get 503 + Retry-After

if breaker is open:
    return 0                       # back off until half-open probe succeeds

base = target
if priority_low:                   # already in the 'low' band → drain back under target
    base = max(min_floor, target - 1)

if deprioritized:                  # boxed_until set, reason == "rate_limited"
    cap = limit if target < limit else max(1, limit - 1)
    base = min(base, cap)          # serve reduced, never fully closed

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
for `release_cooldown` (CLI: `--release-cooldown`; env: `SLUICE_RELEASE_COOLDOWN`; default
**2.0s**) so umans' own accounting can decrement before sluice fills the slot again. This
blunts the lag race that turns a clean release into an apparent overshoot.

The cooldown is what makes the dashboard sometimes show queued requests alongside
"free" slots — the slots are freed (`local_in_flight` decremented) but still resting
(`cooling_down` > 0, not yet acquirable). Override with `--release-cooldown 0` to disable
it entirely for maximally aggressive slot reuse, at the cost of transient overshoots when
umans' lagged accounting hasn't caught up. Raise it (e.g. `--release-cooldown 5`) if phantom
estimates climb after burst-and-drain churn.

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
- **`notify_all` → `notify(1)` FIFO handoff.** The release-side `notify_all` wakes all
  waiters to race for (typically) one permit — a scramble, not FIFO, and retries don't
  keep their place in line. Rejected: with QoS reserve classes (Plan 005 WI-002), a
  single wakeup can land on a waiter whose class cannot use the freed permit → lost
  wakeup. `notify_all` is the *safe* choice, and the scramble cost at home-lab queue
  depths is nil.
- **Per-client escalation state.** Server-side escalating backoff would need per-client
  retry tracking. Clients already own escalation (SDK exponential backoff); an honest
  global pressure signal composes with it. Stateless server, stateful client.
- **Feeding wait/hold samples into permit math.** "Never a control input" stands.

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

## 8. The body_done / disconnect_watcher handoff window

When a request body finishes uploading, `body_stream()` sets `body_done` and returns.
A separate `disconnect_watcher` task — waiting on `body_done.wait()` — then takes over
`receive()` to listen for client disconnects during the response phase.

There is a narrow scheduling window between `body_done.set()` and the watcher's first
`receive()` call. If a client disconnect arrives during this window, the `http.disconnect`
event is **queued by the ASGI server** (not lost) and the watcher picks it up as soon as it
calls `receive()`. The disconnect is not missed — it is delayed by at most one event-loop
turn.

The residual risk: if the upstream responds *before* the watcher picks up the queued
disconnect, the proxy may attempt one `send()` to a client that has already gone. This
is caught by the `send()` exception handler (which sets `disconnect` and breaks), so the
proxy exits cleanly — it just wastes one write attempt. This is not a correctness issue
(the permit is released, the upstream is cancelled), only a minor efficiency one.

Closing the window entirely would require a non-blocking `receive()` poll (which the ASGI
spec does not provide) or a more complex two-channel design. The cost of the fix exceeds
the benefit of closing a window whose consequence is one wasted `send()`. The current
behaviour is pinned by `test_body_done_disconnect_watcher_handoff` in `tests/test_proxy.py`.

## 9. Multi-provider: reduced guarantee off umans

sluice supports four providers via the `--provider` flag: `umans` (default),
`anthropic`, `openai`, and `generic`. Each bundles a truth source and a
controller strategy (see `sluice.providers`).

**`umans`** uses the `concurrency_reconcile` controller — the full truth-based
model described in §1–§5. It polls `/v1/usage` for `concurrent_sessions`, which
is ground truth including phantoms. This is what makes phantom absorption
(§5) possible: when the provider sees more sessions than sluice holds, the gate
shrinks to let the phantoms age out.

**Anthropic, OpenAI, and generic** use the `adaptive` (AIMD) controller. These
providers have no concurrency ground-truth endpoint — there is no
`/v1/usage`-equivalent that reports in-flight sessions. sluice can only react
to 429s and parse ratelimit headers (Anthropic/OpenAI); the `generic` provider
has no external truth at all and runs purely off local signals (breaker state,
429 counts). AIMD multiplicatively decreases permits on 429 and additively
increases on success, but it cannot absorb phantoms because it has no way to
observe them.

The fail-safe guarantee (§3, AGENTS.md rule 1) still holds: uncertainty
tightens the gate. But precision is reduced off umans — sluice reacts to
429s *after* they happen rather than preventing them via ground-truth
reconciliation. Phantom absorption is umans-specific and requires
`concurrent_sessions` from `/v1/usage`.

## 10. Backpressure honesty — the Retry-After contract

When sluice returns a `503`, it carries a `Retry-After` header and a matching
`retry_after` field in the JSON body. Each `reason` has a distinct contract:

| reason | What happened | Retry-After promise | Jittered? |
|---|---|---|---|
| **saturated** (queue-timeout) | Permits > 0 but all held; the request waited the full `queue_timeout` and lost. | Pressure-derived: `ceil((queue_depth + 1) × avg_hold_seconds / capacity)`, ±15 %, clamped to `[5, 60]`. | Yes — load estimate, not a deadline. |
| **saturated** (structural, `_last_permits == 0`) | The reconcile loop set permits to 0 (e.g. phantoms ate all). Nothing can change until the next poll. | `max(pressure_estimate, ceil(poll_interval))`, ±15 %, clamped to `[max(5, min(60, ceil(poll_interval))), 60]`. | Yes. |
| **boxed** | Account paused by the provider (`boxed_until`, reason ≠ `rate_limited`). | `ceil(resets_at - now)`, floored at 30 s. | No — a deadline. |
| **breaker** | Circuit breaker open after sustained 429s. | Remaining breaker cooldown. | No — a deadline. |
| **not_ready** | First usage poll not yet completed. | `max(2, ceil(poll_interval))` — tracks configuration. | No. |
| **draining** / **not_leader** | Instance is shutting down / not the singleton leader. | Short constant (5 s) — the honest value is unknowable (routing concern). | No. |

### Why the saturated value is pressure-derived

The old fixed `Retry-After: 5` was worse than sending nothing: the Anthropic and OpenAI
SDKs do exponential backoff with jitter by default, but when a plausible `Retry-After`
header is present they use it *verbatim instead* — so the fixed header flattened every
client's built-in escalating backoff into a constant 5-second hammer. Under sustained
saturation the observed behaviour was exactly that: retry, re-queue, burn another
`queue_timeout`, 503, retry in 5, repeat.

The honest signal is forward-looking queue pressure:

```
expected_wait ≈ (queue_depth + 1) × avg_hold_seconds / capacity
```

The `+1` is the retrying client itself rejoining the back of the scramble. Hold-time
sampling (acquire→release duration) is the missing observation, added symmetrically to
the existing wait sampler. The estimate is floored at 5 s so it can never promise a
*faster* retry than today, and capped at 60 s — both major SDKs ignore a `Retry-After`
above ~60 s and fall back to exponential backoff. A cap the client discards is not
honesty, it's noise.

`boxed` may still exceed 60 s — its value is genuinely the window reset and the body
carries it for sophisticated clients. Document the SDK-cap interplay rather than
distort it.

### Why jitter

SDKs apply `Retry-After` verbatim with no client-side jitter. A shared exact value
marches every rejected client back in a synchronized wave — retry storm, re-saturation,
repeat. The ±15 % jitter spreads returns across a window. `boxed`/`breaker` values stay
unjittered — they are deadlines, not load estimates.

### Why not the existing queue-wait stats

`PermitGate.avg_wait_seconds` / `p95_wait_seconds` sample only requests that blocked
**and were eventually granted** — every sample is below `queue_timeout` by construction.
The client receiving a saturated 503 is precisely one whose true wait *exceeded*
`queue_timeout`; at the moment we stamp the header, those stats systematically
underestimate. Their docstring already says "never a control input" — the pressure
estimator keeps that true and documents *why*.

### Client reality (SDK cap verification)

The 60-second cap was verified against the source of the SDKs the sluice clients
actually use:

- **Anthropic Python SDK** (`src/anthropic/_base_client.py`): `_calculate_retry_timeout()`
  uses a `Retry-After` header only if `0 < retry_after <= 60`; larger values are
  ignored and the SDK falls back to its own exponential backoff (max ~8 s).
- **OpenAI Python SDK** (`src/openai/_base_client.py`): identical logic — honored
  only if `0 < retry_after <= 60`, otherwise falls back to exponential backoff.
- **Anthropic TypeScript SDK** (`src/client.ts`): honors `Retry-After` **verbatim**
  with no 60 s cap. A header of `120` causes the SDK to wait 120 seconds. The
  only cap is on the *fallback* exponential backoff path (`maxRetryDelay = 8.0 s`).
- **Claude Code** (`@anthropic-ai/claude-code`, closed-source; values come from
  recovered source, not a public release tag): its wrapper does **not** globally
  cap `Retry-After` at 60 s. Normal retries return `retry-after * 1000` verbatim;
  fast-mode has a 20 s short-retry threshold before switching to cooldown, and
  persistent-mode cooldowns are measured in minutes. A 503 with a 30–60 s
  `Retry-After` may be honored, but fast-mode paths may override it.
- **Open WebUI** (`backend/open_webui/routers/openai.py`): does not retry upstream
  errors at all — it re-wraps the upstream error response as a new
  `JSONResponse` / `PlainTextResponse`, dropping upstream headers including
  `Retry-After`. The status code and body are surfaced to the caller, but the
  header is lost.
- **opencode (via Vercel AI SDK / `@ai-sdk/openai-compatible`)**: the Vercel
  AI SDK does not currently honor upstream `Retry-After` headers and uses its own
  exponential backoff instead (vercel/ai#7247). The header is therefore ignored
  by opencode regardless of the cap.
- **umans / hermes**: no public repository was found; cap behavior is unknown.

This is why the saturated value is capped at 60 s: the two most widely used
programmatic SDKs (Anthropic and OpenAI Python) discard values above 60 s and fall
back to short exponential backoff. A cap the client ignores is noise, not honesty.
`boxed` may still exceed 60 s in the JSON body because it carries the real window
reset deadline for sophisticated clients; the header is still capped because the
Anthropic/OpenAI Python SDKs would ignore anything larger.
