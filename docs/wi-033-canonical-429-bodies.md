# WI-033: Canonical 429 bodies of each umans penalty rung

This document captures the observable shape of each rung on the umans
enforcement ladder, with emphasis on the signals sluice uses to classify 429
responses and discriminate the deprioritization rung from a hard box.

**Scope caveat:** sluice is an *inert in-path* proxy (AGENTS.md rule 7). It
**never reads, logs, stores, or rewrites response bodies.** The "canonical 429
body" here refers to the *observable envelope* — HTTP status, response headers,
and the accompanying `/v1/usage` priority state — not the response body bytes,
which sluice streams through untouched. The response body content of an umans
429 is out of scope for sluice to capture; what follows is what sluice *does*
observe and reason over.

## The enforcement ladder

umans enforces **concurrent requests in flight**, surfaced by `GET /v1/usage`.
The ladder has four rungs, defined by observed `concurrent_sessions` and the
`priority` object:

| rung | trigger | `priority.low` | `priority.boxed_until` | `priority.reason` | provider behaviour |
|---|---|---|---|---|---|
| **normal** | `concurrent_sessions <= limit` (≤4) | `false` | `null` | `null` | full priority |
| **low** | `limit < concurrent_sessions <= hard_cap` (4–8) | `true` | `null` | `null` | deprioritised routing, still serving |
| **reject** | `concurrent_sessions > hard_cap` (>8) | varies | varies | varies | HTTP **429** concurrency rejection |
| **boxed** | accumulated >10 concurrency-429s in a day | `true` | set (future) | absent or ≠ `"rate_limited"` | **5-hour pause**, gate closed |

A fifth state — **deprioritization window** — is not a separate rung in the
ladder but a *modifier* of the boxed rung: `boxed_until` is set **and**
`priority.reason == "rate_limited"`. The provider keeps serving normally at
low priority; this is the rung that was conflated with a hard box before
`priority.reason` was parsed (see `docs/wi-024-429-capture-2026-07-03.md`).

## Canonical `/v1/usage` payload (the priority object)

The `/v1/usage` endpoint is the single truth source. Its `usage.priority`
object discriminates the rung:

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

Parsed by `parse_usage()` in `src/sluice/usage.py:62`. The `priority` fields
map onto `LimitState` (`src/sluice/control.py:38`):

| `priority` field | `LimitState` field | type | notes |
|---|---|---|---|
| `low` | `priority_low` | `bool` | true when in the low/deprioritized band |
| `boxed_until` | `boxed_until_epoch` | `float \| None` | ISO-8601 → epoch seconds via `_parse_iso_to_epoch` |
| `resets_at` | `resets_at_epoch` | `float \| None` | when the box lifts |
| `reason` | `priority_reason` | `str \| None` | **the discriminator** — only `"rate_limited"` is known-soft |

### Per-rung `priority` shapes

**Normal:**
```json
"priority": { "low": false, "boxed_until": null, "reason": null }
```

**Low (deprioritized, no box):**
```json
"priority": { "low": true, "boxed_until": null, "reason": null }
```

**Reject (transient concurrency 429):** the `/v1/usage` payload reflects
whatever band the account was in when the 429 fired — `priority` does not
change *because* of a single 429. The 429 is an in-band HTTP response, not a
`/v1/usage` state transition. `concurrent_sessions` will read above
`hard_cap`.

**Deprioritization window (soft box — `reason == "rate_limited"`):**
```json
"priority": {
  "low": true,
  "boxed_until": "2026-07-03T12:23:04.763728+00:00",
  "reason": "rate_limited"
}
```
Observed live 2026-07-03 (capture doc). The account **keeps serving normally
at low priority** — an in-window probe returned HTTP 200 in 0.9 s. The
`boxed_until` timestamp anchors at the triggering event with a duration equal
to `requests.window_seconds` (18000 s = 5 hours, to within 80 ms).

**Hard box (reason absent or unrecognized):**
```json
"priority": {
  "low": true,
  "boxed_until": "2026-07-03T12:23:04.763728+00:00",
  "reason": null
}
```
(or `reason` set to any value other than `"rate_limited"`.) Gate closed,
`effective_permits` returns 0. Fail safe: an unrecognized reason is treated
as the worst case.

## The `priority.reason` discrimination

The `reason` field is the single discriminator between a soft penalty
(deprioritization, still serving) and a hard box (gate closed). The logic
lives in `src/sluice/control.py`:

- `is_deprioritized(reading, now=now)` — `control.py:129` — returns `True`
  when `boxed_until` is in the future **and** `priority_reason ==
  "rate_limited"`. This is the known-soft rung.
- `is_hard_boxed(reading, now=now)` — `control.py:140` — returns `True` when
  `boxed_until` is in the future **and** `priority_reason != "rate_limited"`.
  Fail safe (AGENTS.md rule 1): a missing or unrecognized reason closes the
  gate, exactly as before the field was parsed.

The decision in `effective_permits()` (`control.py:200`):

- **Hard box** (`is_hard_boxed` → true): `return 0` — gate closed.
- **Deprioritization** (`is_deprioritized` → true): serve at the account
  `limit` (or `limit − 1` when `target` already consumes the full limit),
  never fully closed. The provider keeps serving on this rung.
- **Low band** (`classify_band` → `Band.LOW`): drain back under `target` by
  `low_penalty` (default 1).

`classify_band()` (`control.py:150`) maps the observation onto the ladder,
checking `is_hard_boxed` first, then `concurrent_sessions` against
`limit`/`hard_cap`, then `priority_low`/`is_deprioritized` for the LOW band.

## 429 response header behavior

When the upstream returns HTTP 429, sluice observes the response **headers**
only (the body is streamed through, never read). The relevant headers:

| header | reject (concurrency) | deprioritization window | hard box |
|---|---|---|---|
| `Retry-After` | observed `1` (capture 2026-07-03) | n/a — provider serves 200 | n/a — gate closed by `/v1/usage` |
| `Server` | `uvicorn` (app-origin, no CDN) | n/a | n/a |
| CDN headers (`cf-ray`, etc.) | absent | absent | absent |

**Key finding (capture 2026-07-03):** genuine umans 429s carry `Retry-After: 1`
— a positive value — and `Server: uvicorn` with no CDN headers. The
`Retry-After` value is small (≈1 s) and does **not** represent a rate-limit
window duration; it is a concurrency rejection signal that happens to carry a
positive retry-after.

## 429 classification: `_classify_429`

The classification logic is in `src/sluice/proxy.py:122`
(`_classify_429`). It classifies every upstream 429 into one of three
categories, in priority order:

1. **`gateway`** — CDN/gateway headers are present (`cf-ray`,
   `x-amz-cf-id`, `x-served-by`, `x-fastly-request-id`, `x-vercel-id`,
   `fly-request-id`), or `Server` contains `cloudflare`. The 429 was
   rejected at the edge, not by the upstream's concurrency enforcement.
   Tracked separately (`record_gateway_429`), **not** fed to the breaker
   (WI-024).

2. **`concurrency`** — no `Retry-After` header, or `Retry-After <= 0`.
   Fed to the breaker via `record_429()`.

3. **`rate_limit`** — `Retry-After > 0` (a positive integer or a valid
   HTTP-date). Fed to the breaker via `record_rate_limit_429()`, tracked in
   a separate counter (`rate_limit_429s`).

### The retry-after heuristic and its limitation

The retry-after heuristic is **unreliable** for distinguishing concurrency
from rate-limit on the umans provider. Capture 2026-07-03 proved that umans
sends `Retry-After: 1` on genuine concurrency 429s — these are classified
as `rate_limit` by the heuristic. For this reason, **both `concurrency` and
`rate_limit` classifications feed the breaker** (the distinction is for
telemetry only). The breaker threshold (5 in 5 minutes, `BreakerConfig` in
`control.py:263`) prevents a single rate-limit event from tripping, but
sustained rate-limiting should trip it.

Per AGENTS.md rule 1 (fail safe): any `Retry-After` value that is neither a
positive integer nor a valid HTTP-date is treated as `concurrency` (the
fail-closed classification).

### Recording path (proxy → reconcile)

In `src/sluice/proxy.py:642-664`, the 429 handling:

```
if response.status_code == 429:
    retry_after_raw = response.headers.get("retry-after")
    classification = _classify_429(retry_after_raw, response.headers)
    log.warning("upstream 429: retry_after=%r classification=%s server=%s", ...)

    if classification == "concurrency":
        self._reconcile.record_429()           # feeds breaker, total_429s++
    elif classification == "rate_limit":
        self._reconcile.record_rate_limit_429() # feeds breaker, rate_limit_429s++
    elif classification == "gateway":
        self._reconcile.record_gateway_429()    # does NOT feed breaker
```

`record_429()` and `record_rate_limit_429()` both append to `_recent_429s`
and call `breaker_on_429()` (`src/sluice/reconcile.py:211, 230`).
`record_gateway_429()` only increments `_total_gateway_429s`
(`reconcile.py:224`).

## What sluice does not observe

- **Response body of the 429.** sluice streams response bytes through
  untouched (inert in-path, rule 7). The JSON error body umans returns on a
  429 is never parsed, logged, or stored. If the body contains a `reason`
  or `type` field that disambiguates the rung, sluice does not see it —
  only the `/v1/usage` `priority.reason` is available, and only on the
  next poll.
- **The exact moment a 429 transitions to a box.** The `/v1/usage` poll is
  lagged (every `poll_interval` seconds). A 429 that triggers a box is
  observed as a 429 first, then as a `boxed_until` on the next successful
  poll.
- **The response body's `retry-after` field.** Some providers echo a
  `retry_after` in the JSON body; sluice does not read it. Only the
  `Retry-After` HTTP header is observed.

## Open questions (WI-033 scope)

- The capture is n=1 (a single 429 observation on 2026-07-03). The
  `Retry-After: 1` value and `Server: uvicorn` header are from one event;
  they may not be canonical for every 429 umans emits.
- The hard-box rung (`reason` absent or ≠ `"rate_limited"`) has not been
  observed live — the 2026-07-03 event was a deprioritization, not a hard
  box. The hard-box `priority` shape above is inferred from the
  fail-safe logic in `is_hard_boxed()`, not from a captured payload.
- The 429 response *body* (which sluice does not read) may contain
  additional rung-discriminating fields. Capturing it would require an
  out-of-band probe (not sluice itself), which is out of scope for the
  in-path proxy.

## References

- `docs/concurrency-model.md` — the design spine (§1 the enforcement ladder,
  §3 the decision function, §10 the Retry-After contract)
- `docs/wi-024-429-capture-2026-07-03.md` — the single live capture that
  proved `reason == "rate_limited"` is deprioritization, not a hard box
- `src/sluice/proxy.py:122` — `_classify_429` (the classification logic)
- `src/sluice/proxy.py:642-664` — 429 handling in the proxy (recording path)
- `src/sluice/control.py:129` — `is_deprioritized` (the reason discrimination)
- `src/sluice/control.py:140` — `is_hard_boxed` (the fail-safe hard box)
- `src/sluice/control.py:150` — `classify_band` (ladder mapping)
- `src/sluice/control.py:200` — `effective_permits` (the decision function)
- `src/sluice/usage.py:62` — `parse_usage` (the `/v1/usage` parser)
- `src/sluice/reconcile.py:211` — `record_429` (concurrency 429 → breaker)
- `src/sluice/reconcile.py:230` — `record_rate_limit_429` (rate-limit 429 → breaker)
- `src/sluice/reconcile.py:224` — `record_gateway_429` (gateway 429, no breaker)
