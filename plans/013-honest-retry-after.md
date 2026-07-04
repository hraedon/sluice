# Plan 013 — Honest Retry-After under saturation (pressure-derived backoff)

sluice's saturated `503` (permit-queue timeout, `proxy.py`) carries a **fixed
`Retry-After: 5`**. That number is worse than sending nothing: the Anthropic and OpenAI
SDKs do exponential backoff with jitter by default, but when a plausible `Retry-After`
header is present they use it *verbatim instead* — so the fixed header flattens every
client's built-in escalating backoff into a constant 5-second hammer. Under sustained
saturation the observed behaviour is exactly that: retry, re-queue, burn another
`queue_timeout`, 503, retry in 5, repeat.

The irony is that `Reconcile.retry_after_seconds()` is already documented as "Honest
Retry-After based on the gate-closed reason" and *is* honest for `boxed` (time to window
reset) and `breaker` (remaining cooldown) — the `saturated` branch is the one leftover
hardcoded `return 5`, and the proxy's queue-timeout 503 doesn't even call it (it uses
`_RETRY_AFTER_DEFAULT` directly). This plan finishes the honesty story: derive the
saturated value from live queue pressure, jitter it so rejected clients don't return in
a synchronized wave, and sweep the remaining fixed-`5` emission sites.

## Why not the existing queue-wait stats

`PermitGate.avg_wait_seconds` / `p95_wait_seconds` sample only requests that blocked
**and were eventually granted** — every sample is below `queue_timeout` by construction.
The client receiving a saturated 503 is precisely one whose true wait *exceeded*
`queue_timeout`; at the moment we stamp the header, those stats systematically
underestimate. (Their docstring already says "never a control input" — this plan keeps
that true and documents *why*.) The honest signal is forward-looking queue pressure:

```
expected_wait ≈ (queue_depth + 1) × avg_hold_seconds / capacity
```

`queue_depth` and `capacity` already exist on the gate; **hold-time** sampling
(acquire→release duration) is the missing observation, added symmetrically to the
existing wait sampler.

## Design constraints (carried from AGENTS.md)

- **The core is pure (hard rule 2).** The estimator is a pure function in
  `sluice.control` — observations in, integer out, no clock, no randomness. Jitter is
  randomness, so it is applied in the shell (`reconcile`), via an injectable RNG so
  tests stay deterministic.
- **Advisory, never a control input.** The hint shapes the *header*, never the permit
  math. `effective_permits` / band logic are untouched. Hold samples feed the hint only.
- **Fail safe means don't advertise too soon.** Floor at the current default (5 s) so
  the estimator can never promise a *faster* retry than today; when there is no data
  (no hold samples yet), fall back to the floor, not to zero.
- **Stay actionable at the client.** Cap the saturated value at 60 s: both major SDKs
  ignore a `Retry-After` above ~60 s and fall back to exponential backoff (verify the
  exact thresholds against the pinned SDK versions during WI-006 — do not trust this
  from memory). A cap the client discards is not honesty, it's noise. (`boxed` may
  still exceed 60 s — its value is genuinely the window reset and the body carries it
  for sophisticated clients; document the SDK-cap interplay rather than distort it.)
- **Cache-transparency (hard rule 7) is untouched.** Everything here is response-side
  on sluice's own 503s; no request byte changes.

## WI-001 — Hold-time sampling in `PermitGate` (`sluice.gate`)

- Sample acquire→release durations into a `deque(maxlen=wait_window)` (same window, 64,
  as the wait sampler). The gate cannot correlate a `release()` to its `acquire()` on
  its own; the proxy is the sole caller and already brackets the pair — measure there
  with the same monotonic clock and pass `hold_seconds` into
  `release(..., hold_seconds=...)` (`None` from any caller that can't measure → not
  sampled).
- Expose `avg_hold_seconds` (0.0 when empty), mirroring `avg_wait_seconds`.
- Document the sampling bias in the docstring: only *completed* holds are sampled, so
  long-running streams still in flight are invisible and the average skews short under
  mixed workloads. Acceptable for an advisory hint; would not be acceptable for control.
- Tests: sampling window rolls; unsampled releases don't perturb; property is 0.0 cold.

## WI-002 — Pure estimator in `sluice.control`

```python
def saturation_retry_after(
    *, queue_depth: int, capacity: int, avg_hold_seconds: float,
    floor: int = 5, cap: int = 60,
) -> int
```

- `ceil((queue_depth + 1) × avg_hold_seconds / capacity)`, clamped to `[floor, cap]`.
  The `+1` is the retrying client itself rejoining the back of the scramble.
- Degenerate inputs fail safe: `avg_hold_seconds <= 0` (no samples yet) → `floor`;
  `capacity <= 0` → `cap` (a zero-width gate cannot drain by itself; the *caller*
  layers in the poll cadence, see WI-003 — the pure core does not know the interval).
- Tests: deterministic; monotone non-decreasing in `queue_depth` and in
  `avg_hold_seconds`; bounded `[floor, cap]` for all inputs; the degenerate branches.

## WI-003 — Wire the saturated paths through one computation, with jitter (`sluice.reconcile`, `sluice.proxy`)

There are **two saturation flavours** and both must use the estimator:

- **(a) queue-timeout saturated** (`proxy._proxy_request`, the main event): permits > 0
  but all held; the request waited the full `queue_timeout` and lost. Replace the
  `_RETRY_AFTER_DEFAULT` literal with a new `Reconcile.saturation_retry_after()` that
  feeds the estimator from `gate.queue_depth` / `gate.capacity` /
  `gate.avg_hold_seconds`. Note this path can fire while `gate_closed_reason()` is
  `"open"` — do **not** dispatch on the reason here.
- **(b) structurally saturated** (`retry_after_seconds()`'s `saturated` branch,
  `_last_permits == 0`): nothing can change until the reconcile loop next resizes, so
  the honest value is `max(estimator_result, ceil(poll_interval))` — the shell owns the
  poll cadence and layers it on top of the pure hint.
- **Jitter, shell-side:** multiply the estimate by `U(0.85, 1.15)` before rounding,
  re-clamp to `[floor, cap]`. SDKs apply `Retry-After` verbatim with no client-side
  jitter, so a shared exact value marches every rejected client back in a synchronized
  wave. RNG is injected (`random.random` default) so tests pin it. Boundary pile-up at
  the cap is accepted at this scale. `boxed`/`breaker` values stay **un**jittered —
  they are deadlines, not load estimates.
- The JSON body `retry_after` field and the header carry the **same** post-jitter value.
- Tests: deep-queue scenario yields a larger value than shallow-queue; idle/low-pressure
  saturation returns the floor (today's behaviour — regression-compatible at low load);
  jitter bounds hold with a pinned RNG; flavour (b) never advertises below the poll
  interval.

## WI-004 — Sweep the remaining fixed-`5` sites (the adjacent rough edges)

- **`not_ready`** (`proxy.py`): the honest value is "until the first successful poll" —
  use `max(2, ceil(poll_interval))` instead of the flat 5. Small, real: with the
  default 5 s cadence it's a wash, but the header now tracks configuration.
- **`draining` / `not_leader`** (`proxy.py`, and the two **inline literal `5`s** in
  `admin.py` mutation handlers): the honest value is genuinely unknowable (the retry
  should land on a replacement instance / the leader — that's a routing concern, not a
  timing one). Keep a short constant, but route every emission through the one shared
  constant and record the *rationale* in `retry_after_seconds()`'s docstring, which
  becomes the single narrative for every reason. Dedupe the `admin.py` literals.
- Tests: grep-level guard is overkill; instead assert in the handler tests that the
  emitted values come from the shared constants/helpers (no naked literals reappear in
  the JSON bodies).

## WI-005 — Observability (`sluice.status`, dashboard)

- Snapshot + `/status.json`: `avg_hold_seconds`, `retry_after_hint` (the current
  un-jittered estimator output — jitter is per-response, the hint is the trend).
- `/metrics`: `sluice_hold_avg_seconds`, `sluice_retry_after_hint_seconds` gauges next
  to the existing queue-wait gauges.
- Dashboard: show the hint alongside queue depth when the gate is under pressure —
  this is the operator's view of "what are we telling clients right now".

## WI-006 — Documentation: the backpressure contract

- `docs/concurrency-model.md`, new subsection **"Backpressure honesty"**: what each 503
  `reason` means, what its `Retry-After` promises, and the client reality — SDKs honor
  the header verbatim below their cap and fall back to exponential backoff above it
  (**verify the caps against the pinned `anthropic`/`openai` SDK sources and cite the
  code**, per the design constraint above); why the saturated value is pressure-derived
  and jittered; why `boxed` may exceed the SDK cap on purpose.
- `gate.py`: annotate the wait-sample properties with the survivorship bias (granted-
  only, capped below `queue_timeout`) and the consequence — *these are why the
  saturated Retry-After uses hold-time × queue-depth, not the wait stats*.

## Deliberately not doing (assessed, kept as-is)

- **`notify_all` → `notify(1)` FIFO handoff.** The release-side `notify_all` wakes all
  waiters to race for (typically) one permit — a scramble, not FIFO, and retries don't
  keep their place in line. Rejected: with QoS reserve classes (Plan 005 WI-002), a
  single wakeup can land on a waiter whose class cannot use the freed permit → lost
  wakeup. `notify_all` is the *safe* choice, and the scramble cost at home-lab queue
  depths is nil. Record this trade-off in concurrency-model §6 so it reads as a
  decision, not an accident.
- **Per-client escalation state in sluice.** Server-side escalating backoff would need
  per-client retry tracking. Clients already own escalation (SDK exponential backoff);
  an honest global pressure signal composes with it. Stateless server, stateful client.
- **Feeding wait/hold samples into permit math.** "Never a control input" stands.

## Sequencing

WI-001 → WI-002 → WI-003 is the spine and ships together (sampling alone changes
nothing; the estimator alone has no caller). WI-004 and WI-005 follow independently;
WI-006 lands with or immediately after WI-003 (the contract must describe shipped
behaviour, not intent).

## Done when

- A saturated 503's `Retry-After` (header and body, equal) is pressure-derived and
  jittered within `[5, 60]`; deep queues advertise longer waits than shallow ones;
  at low pressure the value degrades to today's 5 (regression-compatible).
- Every `retry_after` sluice emits flows through `retry_after_seconds()`, the new
  saturation helper, or the one shared constant — zero naked literals.
- Hold-time and the hint are visible on `/status.json`, `/metrics`, and the dashboard.
- CI green on 3.12 + 3.13 (hard rule 6 — async timing is easy to get locally-green).

## Validation notes (live, in the WI-024 capture style)

- Drive real saturation (hermes/opencode flood) and capture an actual saturated 503
  with headers, before and after — the "before" artifact shows the fixed 5, the
  "after" shows a pressure-scaled, jittered value. Save alongside
  `docs/wi-024-429-capture-2026-07-03.md`.
- Watch a real SDK client's retry cadence change: before = metronomic 5 s; after =
  spread and scaled with queue depth. Confirm per-client which of the live clients
  (Claude Code / hermes / opencode / open-webui) actually honor `Retry-After` on 503,
  and record the findings — if a client ignores the header entirely, its behaviour is
  unchanged and that's worth knowing too.
- Confirm the global invariant is untouched: `/v1/usage.concurrent_sessions` stays
  within bounds throughout — this plan changes what we *say*, never what we *admit*.
