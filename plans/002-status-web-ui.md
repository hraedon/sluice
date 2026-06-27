# Plan 002 — Status web UI

Expose sluice's live control state through a small built-in dashboard, plus a
Prometheus-compatible metrics endpoint for operators who already run Grafana. Depends on
Plan 001 (the controller and proxy shell must exist; this plan reads their state).

Goal: **at a glance, know whether you are safe (normal), warned (low), throttled (phantoms
absorbed), or stalled (boxed) — and which host is using the concurrency** — without
shelling into the box or reading the proxy logs.

## Design constraints (carried from AGENTS.md)

- **Counts only — never request content.** The dashboard and every status payload expose
  concurrency counts, control state, and connection *metadata* (surface, source host,
  duration). They never expose prompt/response bodies, tokens of content, or the API key.
  This is the "inert in-path" guarantee made visible. A test asserts no body text can reach
  a status payload.
- **The projection is pure.** The status snapshot is a function of `ControllerState` plus
  shell counters — assembled in the shell, derived from the pure core, no new truth
  invented in the view layer.
- **Fail safe extends to exposure.** sluice listens on a routable interface by design (it
  fronts three hosts), so the dashboard must not be open to the LAN unauthenticated — see
  WI-005.

## Subject signature: the gate ladder

Each family tool has a signature instrument (cert-watch: the cert chain; gpo-lens: the
precedence chain). sluice's is **the gate ladder** — a live vertical gauge of the
enforcement bands with the current position marked:

```
  boxed   ┃                 ← red banner + countdown to resets_at when active
  reject  ┃ > 8  (hard_cap)
  low     ┃ 5–8             ● observed concurrent_sessions (provider truth)
  ──────  ┃ ─── target (3) ───────────────────────
  normal  ┃ ≤ 4             ▣ local in-flight (what sluice is holding)
          ┗━━━━━━━━━━━━━━━━
                 gap between ● and ▣ = phantom estimate (shaded)
```

The whole point is legibility: the distance between "what sluice holds" (▣) and "what
umans sees" (●) is the phantom load, drawn as a shaded gap. The band the ● sits in is the
page's status color.

## Work items

### WI-001 — Status projection (`sluice.status`)
- `snapshot(state, counters) -> StatusSnapshot` (pure dataclass → dict). Fields:
  - reading: `concurrent_sessions`, `limit`, `hard_cap`, `priority_low`, `boxed_until`,
    `resets_at`, `usage_age`, `stale`
  - computed: `effective_permits`, `band`, `phantom_estimate`, `breaker`, `recent_429s_today`
  - operational: `target`, `upstream`, `poll_interval`, `release_cooldown`, `queue_depth`,
    `local_in_flight`
  - `slots`: per-active-request **metadata only** — surface (`messages`/`chat`), source
    host/IP, age. No identifiers beyond what's needed to answer "who's holding a slot."
- Test: snapshot of a state carrying a fake request body contains no body text anywhere.

### WI-002 — `GET /status.json`
- Returns the current snapshot. Cheap, pollable, the dashboard's fallback transport and a
  scriptable status source (`sluice status` in Plan 001 can call this same projection).

### WI-003 — `GET /status/stream` (SSE)
- Pushes a snapshot on each poll tick and on every transition (admission, release,
  band change, breaker flip, box enter/exit). Reuses the proxy's existing SSE plumbing.
- Dashboard subscribes here; falls back to polling `/status.json` if SSE drops.

### WI-004 — The dashboard page (`GET /`)
- Server-rendered HTML + vanilla JS (no SPA, no build step — family house style).
- Renders the **gate ladder** (WI subject signature), a reading panel, a prominent
  **box/breaker banner** with live countdown to `resets_at`, and the active-slots list
  (metadata only).
- Live via the SSE stream; degrades to 1 s polling.
- Color-by-band: normal = accent, low = warn, reject/boxed = alarm.

### WI-005 — Exposure & auth (load-bearing — sluice is network-reachable)
- The **proxy data plane** and the **dashboard/metrics** are separated:
  - proxy listens on the shared service port (the one clients hit);
  - dashboard + `/status*` + `/metrics` bind to a **separate admin listener**, default
    `127.0.0.1` only, opt-in to a LAN interface behind a bearer token (`--admin-token` /
    `SLUICE_ADMIN_TOKEN`).
- The status payload **never** includes the upstream API key, even when authed.
- Test: admin routes refuse unauthenticated requests when bound to a non-loopback address.

### WI-006 — Prometheus metrics (`GET /metrics`)
- The "or similar dashboard" path: OpenMetrics text exposition of the same counters
  (`sluice_in_flight`, `sluice_effective_permits`, `sluice_observed_sessions`,
  `sluice_phantom_estimate`, `sluice_band` (enum gauge), `sluice_429s_today`,
  `sluice_queue_depth`, `sluice_boxed{state}`). Lets existing Grafana users skip the
  built-in UI entirely. Counts only — same content guarantee.

### WI-007 — patina onboarding
- Vendor patina (`tokens.css` + `theme.js` + `sync.sh`) per the family design system;
  dark-default, IBM Plex Mono.
- Pick sluice's **accent**: proposed cool cyan/aqua (the flow/water register), distinct
  from cert-watch bronze and gpo-lens verdigris — confirm during onboarding.
- Register sluice + its accent + the gate-ladder signature in the family-UI design-system
  memory so the next UI inherits the convention.

## Sequencing
WI-001 (pure projection + content-leak test) → WI-002 → WI-005 (lock exposure before
anything binds to a routable port) → WI-003 → WI-004 → WI-007 → WI-006.

## Done when
- Open the dashboard on the admin port; the gate ladder tracks live as clients connect —
  ▣ rises with real load, ● tracks `/v1/usage`, and an induced phantom shows as a visible
  shaded gap that drains away as reconciliation absorbs it.
- The box banner + countdown render correctly against a synthetic `boxed_until` payload.
- Admin routes are loopback-only by default and token-gated when exposed; no key or body
  content appears in any payload (asserted).
- `/metrics` scrapes cleanly in Prometheus.
- CI green on 3.12 + 3.13.
