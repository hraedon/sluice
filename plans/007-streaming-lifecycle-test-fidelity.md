# Plan 007 — Streaming/lifecycle test fidelity + fault injection

> **Status: Complete.** All six work items landed:
> - WI-1 (streaming mock transport / backpressure): `_StreamingMockTransport` + `test_backpressure_real`
> - WI-2 (latency-injecting transport / race the races): `_StreamingMockTransport(header_delay/chunk_delay)` + `test_mid_body_upload_disconnect_with_latency`
> - WI-3 (startup-window honesty): `first_poll_ok` parameter + `test_startup_window_closes_on_first_poll`
> - WI-4 (body_done/disconnect_watcher handoff): pinning test `test_body_done_disconnect_watcher_handoff`
> - WI-5 (fault-injection soak harness): `scripts/soak.py`
> - WI-6 (breaker_half_open_age_seconds): exposed in `status.py` + rendered in dashboard

sluice's risk does not live in the pure core — `control.py` is deterministic, exhaustively
tested, and has survived two adversarial reviews. It lives in the **async lifecycle edges of
the proxy** (disconnect, cancellation, backpressure, startup), and the last two sessions'
reflections say plainly that this is the part hardest to test locally and most likely to
break under load. The WI-013–020 fixes closed the *known* bugs there; this plan closes the
gap that let them exist unnoticed: **the test suite does not actually exercise the streaming
and racing behaviour the code claims.** Concretely, today:

- `_AsyncMockTransport` calls `await request.aread()` (`tests/test_proxy.py:59`), buffering
  the entire request body — so **no test exercises `body_stream()`'s chunk-by-chunk
  backpressure**; the docstring's backpressure claim is untested.
- The mock transport responds **immediately**, so the WI-014 racing logic
  (`entry_task` vs `disconnect_task`) never actually races; disconnect-during-body-upload
  has zero coverage (only disconnect-during-response-streaming is tested).
- `_make_app` sets `_first_poll_ok = True` globally (`tests/test_proxy.py:121`), so the
  startup fail-closed window (WI-018) is bypassed by almost every proxy test.
- The `body_done.set()` → `disconnect_watcher` arming handoff has a known narrow window
  (`proxy.py:360-366`) where a disconnect can be missed — currently neither closed nor
  regression-guarded.

Goal: **a test double and a fault-injection harness faithful enough that the next
lifecycle bug is caught by the suite, not by an adversarial reviewer or the live pod.**

## Design constraints (carried from AGENTS.md)

- **Pure core untouched.** Everything here is shell/tests/scripts; `sluice.control` does
  not change (except WI-6's read-only status field plumbing, which touches no decision
  logic).
- **No test-only hooks in the hot path.** Fidelity comes from better *transports and
  fixtures*, not from `if TESTING:` branches in `proxy.py`. Where injection is needed
  (clocks, events), use the constructor-injection pattern `PermitGate` already uses.
- **Inert in-path / cache-transparency (rule 7) are invariants to *assert*, not features
  to extend** — the soak harness must verify byte-identical egress, not weaken it.

## WI-1 — Streaming mock transport (kill the `aread()` lie)

Replace `_AsyncMockTransport`'s `await request.aread()` with consumption of
`request.stream` chunk-by-chunk, with a configurable per-chunk gate (an `asyncio.Event`
the test controls). Add a test that sends a body larger than one chunk and asserts the
client-side `receive()` is **not** called for chunk N+1 until the transport has consumed
chunk N — i.e. backpressure is real, not buffered away.

**AC:** the new transport is the default for proxy tests; at least one test fails if
`body_stream()` is ever replaced with a buffering implementation.

## WI-2 — Race the races: latency-injecting transport + upload-disconnect coverage

Extend the WI-1 transport with configurable delays *before headers* and *between response
chunks*. Then add the missing scenarios:

- client disconnects **mid-body-upload** → upstream request is cancelled (the WI-014
  machinery on the upload path), permit released exactly once;
- client disconnects **while waiting for upstream headers** with real latency in the way
  (today's instant mock means the `asyncio.wait` race never races);
- upstream raises after headers are sent (regression guard for WI-013's
  `response_started` flag under the streaming transport).

**AC:** each scenario has a deterministic test (event-gated, no sleeps-as-synchronization);
`held` returns to 0 and no double `http.response.start` in all three.

## WI-3 — Startup-window honesty in the fixture

Make `_first_poll_ok` an explicit `_make_app(..., first_poll_ok=True)` parameter instead
of a buried default, and add not-ready-path tests: requests during the startup window get
the documented fail-closed response, and the window closes on first successful poll.

**AC:** grep shows no test relying on the buried default; the startup window has direct
coverage both sides (open/closed).

## WI-4 — Close or fence the `body_done`/`disconnect_watcher` handoff race

Decide the narrow window at `proxy.py:360-366` (disconnect lands after `body_done.set()`
but before the watcher arms). Preferred: have the watcher check the ASGI receive queue
state (or perform one non-blocking `receive()` poll) on arming. If the fix costs more
complexity than the window justifies, document the window in `docs/concurrency-model.md`
and add a test that pins the *current* behaviour so a future change is deliberate.

**AC:** either a test proving the disconnect is caught, or a doc section + pinning test.
No silent third state.

## WI-5 — Fault-injection soak harness (`scripts/soak.py`)

A manual (and optionally nightly-CI) harness: a local upstream with configurable
latency / mid-stream abort / hang / 429-burst behaviours, plus a client mix (N concurrent
streamers, slow-loris uploader, mid-response disconnector). Runs for a bounded duration
against a real `sluice serve` process and asserts invariants from `/status.json` at the
end: `held == 0`, no permit leak, breaker returned to `CLOSED`, zero 5xx from sluice
itself (upstream-originated errors excluded), and byte-identical egress on a checksummed
echo route (rule 7 asserted under fault load).

**AC:** `uv run scripts/soak.py --duration 60` exits 0 on current main; any permit leak or
double-start reproduces as a non-zero exit with the offending scenario named. CI wiring
optional, behind a manual `workflow_dispatch`.

## WI-6 — Surface `half_opened_at` (breaker probe age)

`control.py` tracks `half_opened_at` but neither `/status.json` nor the dashboard shows
it — a half-open breaker is currently indistinguishable from a fresh one. Expose
`breaker_half_open_age_seconds` in status and render it beside breaker state in the
dashboard.

**AC:** status field present and `None`-safe; dashboard shows the age only in HALF_OPEN.

## Sequencing

WI-1 → WI-2 (transport first, races second); WI-3/WI-4/WI-6 independent; WI-5 last (it
leans on everything before it for its assertions). No dependency on Plan 006 — but Plan
006's provider abstraction should **reuse the WI-1/WI-2 transport** for its own tests, so
landing this first is cheaper for both.
