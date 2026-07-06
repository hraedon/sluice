# Changelog

All notable changes to sluice are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/).

## [1.1.0] — 2026-07-06

sluice 1.1 is the first feature release since the 1.0 deployment. It adds
operational observability (history trends, throughput, per-client metrics),
dashboard authentication, multi-provider support, idle poll backoff, config
reload, and hardens the 429 classification and security surface.

### Features

- **Plan 008 — History trend buffer.** Bounded in-memory ring buffer of
  per-tick snapshots, surfaced via `/history.json` and the dashboard
  sparkline. Optional SQLite persistence (`--history-store`) so a restart
  doesn't wipe the trend. Depth and retention tunable via `--history-size`
  and `--history-ttl`.
- **Plan 009 — Sparkline depth.** Queue-depth spark, band ribbon, and
  time-horizon toggle (5m / 1h / 4h) on the dashboard. Hover tooltip shows
  per-tick detail.
- **Plan 010 — Dashboard events and throughput.** Full-width sparkline
  layout, hover explanations on every Reading/Config row, and tick marks
  where queue timeouts or 429s actually happened.
- **Plan 011 — Runtime target override.** Dashboard config mutation
  endpoints (`/admin/config/target`) to step or revert the target
  concurrency without a restart. Requires `--admin-token`.
- **Plan 012 — Dashboard login page.** Session cookie authentication
  (HttpOnly, SameSite=Strict, 30-day TTL). Three credential forms: session
  cookie (browser), Bearer token (API/Prometheus), HTTP Basic. CSRF
  protection on all mutation endpoints. Global login throttle.
- **Plan 013 — Honest Retry-After under saturation.** Saturated 503s now
  carry a pressure-derived, jittered `Retry-After` based on queue depth ×
  hold-time / capacity, replacing the fixed `5` constant. Boxed/breaker
  deadlines are unjittered.
- **WI-021 — HALF_OPEN breaker state.** Dashboard renders
  `breaker_half_open_age_seconds` when the breaker is in HALF_OPEN, with
  a "probing" banner.
- **WI-022 — Idle poll backoff.** Two-speed poll cadence: fast when active
  (default 5s), slow when idle (default 30s, capped at
  `usage_fresh_ttl × 0.8`). An `asyncio.Event` wakes the loop promptly when
  traffic resumes. Disable with `--poll-interval-idle 0`.
- **WI-023 — Throughput bars and per-client metrics.** Per-tick throughput
  bars at the bottom of the sparkline distinguish idle from healthy
  traffic. Per-client counters (forwarded, succeeded, 429s, queue
  timeouts) keyed by `x-sluice-client-label`, surfaced in `/status.json`
  and `/metrics` (Prometheus format).
- **WI-026 — Dashboard error feedback.** Visible error banner on failed
  config override, dismissible by click.
- **Multi-provider support.** `--provider` flag for `umans` (default),
  `anthropic`, `openai`, and `generic`. Non-umans providers use in-band
  response headers as truth and an AIMD controller.
- **QoS reserve.** `--reserve interactive=1` sets aside dedicated permit
  slots for a priority class. Clients tag themselves via
  `x-sluice-client-label` (stripped before forwarding).
- **Config reload.** `SIGHUP` and `POST /admin/reload` re-read the config
  file and apply safe runtime changes (poll intervals, queue timeout,
  trusted proxies, CORS, body limit, idle timeout) without restart.
- **Running commit in dashboard.** `/status.json` and the dashboard header
  show the build SHA (`GIT_SHA` Docker build arg).
- **High availability.** `--singleton-guard kube-lease` uses a Kubernetes
  coordination Lease so only one pod is leader at a time.

### Fixes

- **WI-024 — 429 classification.** Gateway/CDN 429s (Cloudflare, Vercel,
  Fastly, etc.) are classified separately and do not feed the circuit
  breaker. The `rate_limited` penalty window serves at reduced permits
  instead of a full stop.
- **WI-028 — Security hardening.** QoS header spoofing gated behind
  trusted-proxy allowlist, request body size limit (`--max-request-body`),
  upstream idle timeout, CORS origin control, structured audit logging,
  and migration safety for the history SQLite store.
- **WI-029/030/031 — Fable security fixes.** Cookie stripping from
  forwarded requests, CSRF on mutation endpoints, session handling
  improvements.
- **Prod 502 fix.** `commonLabels` in kustomize was clobbering the
  NetworkPolicy traefik selector; switched to `labels` (no
  `includeSelectors`).
- **KubeLeaseGuard.** `resourceVersion` 409 conflict handling for
  leader election.
- **429 breaker blindness.** Extracted lifecycle module, added graceful
  drain on shutdown.
- **Dashboard repairs.** Fixed ternary syntax error that blanked the
  dashboard, and double-encoded UTF-8 from the HTML extraction.
- **Comprehensive adversarial review sweep.** Bug fixes across the
  codebase from two-round adversarial review (GLM + Kimi).

### CI / Infrastructure

- Python 3.14 added to the test matrix (3.12 + 3.13 + 3.14).
- Identifier gate (`scripts/check_committed_identifiers.py`).
- Dependabot auto-merge workflow.
- `pip-audit` on every CI run.
- Trivy vulnerability scanning on Docker images (CRITICAL + HIGH gate).
- Multi-arch Docker builds (amd64 + arm64) with GHA cache.
- Automated kustomize image-tag bump on release.
- Dependabot dependency updates (uvicorn, docker actions, trivy-action).

## [1.0.0] — 2026-07-01

Initial release. Concurrency-metering reverse proxy for LLM APIs,
reconciled against the provider's usage endpoint. Deployed and live
(internal-only, GitOps via ArgoCD).

- Deterministic pure-core concurrency controller (bands, permit math,
  reconciliation, breaker) with stdlib-only import boundary.
- Async reverse proxy with streaming passthrough for both
  `/v1/messages` and `/v1/chat/completions` routes.
- `/v1/usage` reconciliation loop with phantom absorption.
- Release cooldown, circuit breaker, queue timeout.
- Live dashboard with concurrency chart, queue depth, band ribbon.
- `/healthz`, `/readyz`, `/status.json`, `/metrics` endpoints.
- Kubernetes deployment manifests with singleton guard.
