# Plan 013 capture: honest Retry-After under local saturation (2026-07-04)

Controlled local validation of Plan 013's pressure-derived Retry-After logic.
The live instance (`sluice.k8s.REDACTED`) is still running build `a7a3c70`
(pre-Plan 013), so this capture was run locally against the current source
(`daef71e` + doc updates on `followup/sdk-cap-verification`) to prove the new
503 behaviour before any prod deploy.

## Setup

- sluice target = 2, capacity = 2, queue_timeout = 2 s
- Mock upstream holds every permit for ~15 s (SSE stream)
- Two warmup requests complete first so `PermitGate` has real hold-time samples
- Then 8–30 concurrent clients are fired at `/v1/messages`

## Captures

### Deep saturation (30 concurrent clients)

Status snapshot:

```json
{
  "effective_permits": 2,
  "queue_depth": 0,
  "local_in_flight": 2,
  "avg_hold_seconds": 15.01,
  "avg_wait_seconds": 0.0,
  "queue_timeouts": 28,
  "target": 2,
  "band": "normal",
  "breaker": "closed",
  "gate_closed_reason": "open"
}
```

503 response:

```http
HTTP/1.1 503 Service Unavailable
retry-after: 60
content-type: application/json

{"error": "concurrency limit reached", "reason": "saturated", "retry_after": 60}
```

### Moderate saturation (8 concurrent clients)

Status snapshot:

```json
{
  "effective_permits": 2,
  "queue_depth": 0,
  "local_in_flight": 2,
  "avg_hold_seconds": 15.02,
  "avg_wait_seconds": 0.0,
  "queue_timeouts": 6,
  "target": 2,
  "band": "normal",
  "breaker": "closed",
  "gate_closed_reason": "open"
}
```

503 response:

```http
HTTP/1.1 503 Service Unavailable
retry-after: 52
content-type: application/json

{"error": "concurrency limit reached", "reason": "saturated", "retry_after": 52}
```

## Notes on reading the snapshots

The status snapshots were taken **after** the burst subsided, by which time every
queued client that had not acquired a permit had already hit `queue_timeout` and
cleared from the gate. That is why `queue_depth` reads `0` while `queue_timeouts`
is non-zero. The `Retry-After` values stamped on the 503s reflect the queue
depth **at the moment each request timed out**:

- Moderate burst (`retry_after: 52`): peak depth was large enough that the
  unjittered estimate reached ~52 s before the ±15 % jitter window.
- Deep burst (`retry_after: 60`): peak depth produced an estimate well above
  60 s, so the cap bit. The actual unjittered estimate would have been
  `ceil((qd + 1) × 15 / 2)` for some `qd` at failure time, so the cap applied.

## Findings

1. **Retry-After is now pressure-scaled, not fixed at 5 s.** With real hold-time
   samples, a moderate queue advertises ~52 s and a deep queue hits the 60 s cap.

2. **Header and body match** — both carry the same integer value, satisfying
   Plan 013 WI-003.

3. **`avg_hold_seconds` is populated** by hold-time sampling on successful
   upstream completions; the estimator uses it directly.

4. **The 60 s cap is real and observable.** Early timeouts in the deep burst
   produced the capped value `60`. Later timeouts in the same burst produced
   smaller values as the queue drained; the captured value is the maximum
   observed, not the only one.

5. **All 503s carried `reason: "saturated"`** — the proxy fast-fail path for
   queue-timeout requests is wired correctly.

## Caveats

- This is a local mock-upstream capture, not the live umans instance. The live
  instance still needs an image bump to activate Plan 013.
- SDK-level retry cadence (Claude Code / hermes / opencode / open-webui) was
  not observed live here; only the sluice egress header was captured.
