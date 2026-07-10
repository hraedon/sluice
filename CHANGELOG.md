# Changelog

All notable changes to sluice are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/).

## [1.3.0] — 2026-07-10

### Fixes

- **Orphaned permits from ungracefully-disconnected clients ("local
  phantoms").** When a client host was restarted or power-cycled mid-stream,
  its TCP connection went silent without a FIN/RST. The proxy's streaming
  loop only checked for a client disconnect *between* upstream chunks, so a
  request whose upstream had gone idle blocked forever in `__anext__`, never
  releasing its concurrency permit. The slot was held indefinitely —
  `local_in_flight` stuck above the provider's `concurrent_sessions`, which
  the reconciler's phantom absorption (`max(0, observed − local)`) cannot
  reclaim because it only models the *opposite* skew. Observed live: three
  permits pinned across 33h after two hosts were restarted, driving 297
  saturation-503s. Two coordinated fixes:
  - **Streaming loop races each upstream read against the disconnect event**
    (`proxy.py`), so a detected disconnect frees the permit immediately even
    when the upstream has gone silent — no longer waiting for a next chunk
    that may never arrive.
  - **TCP keepalive on client connections** (`cli.py`, on by default) so an
    ungracefully-dropped peer is detected by the kernel (~120s with the
    default 60s idle) and surfaced as `http.disconnect`, instead of the OS
    default ~2h. New flags: `--tcp-keepalive` / `--no-tcp-keepalive` and
    `--tcp-keepalive-idle` (env: `SLUICE_TCP_KEEPALIVE`,
    `SLUICE_TCP_KEEPALIVE_IDLE`). Detection keys on genuine liveness, so
    legitimately long streams to a live client are never truncated.

## [1.2.3] — 2026-07-08

### Fixes

- **Fail-safe: stale-reading path now respects LKG `hard_cap` as a ceiling.**
  Previously, when the `/v1/usage` reading was stale, the `effective_permits`
  computation dropped the `hard_cap` clamp entirely — clamping only to
  `target`. If a provider downgrade occurred during a poll outage (e.g.
  `hard_cap` dropped from 8 to 2), sluice would forward permits above the
  real limit — a fail-open window (AGENTS.md rule 1). The stale path now
  clamps to `min(target, hard_cap)`, using the last-known-good `hard_cap` as
  the ceiling. The `stale_penalty` still tightens `base`; the clamp further
  restricts the ceiling to the last-known safe bound.

- **Adaptive controller gains the stale-reading + rate_limit 429 safety net.**
  The shell-level safety net (`permits = min(permits, min_floor)` when the
  reading is stale AND recent rate_limit 429s exist) was only applied to the
  concurrency-reconcile controller (umans). The adaptive controller (Anthropic,
  OpenAI, generic) had no equivalent — its AIMD stale-decrease is gated by
  `min_decrease_interval` (30s), so permits could be held steady into a
  rejecting upstream during that window. The safety net now applies to both
  controllers.

- **Idle detection now considers rate_limit 429s.** The `_idle` predicate
  (which triggers slow poll backoff, WI-022) previously checked only
  `_recent_429s` (concurrency 429s). A system actively receiving rate_limit
  429s (which don't feed the breaker but do wake the poll) could declare
  itself idle and oscillate the poll cadence. The predicate now also checks
  `len(self._recent_rate_limit_429s) == 0`.

- **`min_floor` validation.** `ControllerConfig` and `AdaptiveConfig` now
  reject `min_floor < 1` at construction. A `min_floor=0` would void the
  "never fully closed on uncertainty alone" guarantee (AGENTS.md). The
  `__post_init__` raises `ValueError` so misconfiguration fails fast at
  startup, not silently at runtime.

- **`resize()` clamps negative capacity to 0.** `PermitGate.resize()` now
  guards against negative values, clamping to 0 (fail-safe: closed gate)
  instead of accepting them silently, which would inflate `_available()`.

- **RFC 7230 §6.1: Connection header hop-by-hop parsing.** The proxy now
  parses the `Connection` header to identify additional hop-by-hop headers
  (headers named in `Connection` must not be forwarded). Previously, only a
  fixed set was stripped — custom headers named in `Connection` would leak
  to the upstream, breaking cache-transparency (AGENTS.md rule 7). Applied
  to both request and response header filtering.

- **Session fixation prevention.** Upstream `Set-Cookie` values that set
  `sluice_session=` are now stripped from the response. The upstream should
  never set this cookie, but if it does, it must not reach the client's
  browser and overwrite the admin session cookie.

- **IPv4-mapped IPv6 loopback in Secure cookie decision.** `_should_set_secure()`
  now recognizes `::ffff:127.0.0.1` as a loopback address, so the `Secure`
  attribute is correctly set on IPv4-mapped IPv6 localhost connections.

- **Prometheus label escaping in `ClientMetrics.to_prometheus()`.** Label
  values are now escaped (`\`, `"`, `\n`) to match the escaping already
  present in `status.py`'s `to_prometheus()`. Previously, a client label
  containing special characters would produce invalid Prometheus text.

### Tests

- Fixed `test_429_with_unparseable_retry_after_is_recorded`: moved `"-1"`
  (a valid integer that parses as concurrency) to the zero-variants test,
  replaced with `"1.5"` (a float that is genuinely unparseable).
- Renamed `test_effective_permits_not_clamped_by_hard_cap_when_stale` →
  `test_effective_permits_clamped_by_hard_cap_when_stale` with updated
  assertion reflecting the fail-safe behavior.
- Added tests for: `min_floor` validation, adaptive controller safety net,
  idle detection with rate_limit 429s, rate_limit 429 aging out of safety
  net, negative `resize()` clamping.

## [1.2.1] — 2026-07-07

### Features

- **Docker Compose deployment.** `deploy/compose.yaml` + `deploy/.env.example`
  provide a self-contained single-host deployment with log rotation, health
  checks, SQLite history persistence (named volume), and the same security
  hardening as the k8s manifest (read-only root FS, all capabilities dropped,
  non-root user). All config via `SLUICE_*` env vars; compose makes the admin
  token mandatory (fail-safe). Builds from source by default, with a commented
  `ghcr.io` line to switch to the pre-built image. Validated end-to-end:
  build, proxy request to umans (200), SQLite persistence across restart,
  security settings verified.

  The Dockerfile now creates `/data` with `sluice:sluice` ownership so named
  Docker volumes are writable by the non-root user (no effect on the k8s
  deployment — the PVC overlays it).

## [1.2.0] — 2026-07-07

### Features

- **Plan 014 — Windows Service support.** sluice can now run as a native
  Windows service. An install script (`scripts/install-windows.ps1`)
  finds Python 3.12+ (borrowing cert-watch's Install Manager and shared-
  Python logic), creates a venv, installs sluice with the `[windows]`
  extra (pywin32), and registers a Windows service via `New-Service`.
  The service spawns `sluice serve` as a subprocess; the dashboard is
  available at `http://localhost:8800/`. An uninstall script
  (`scripts/uninstall-windows.ps1`) removes the service. Config via a
  TOML file at `C:\ProgramData\sluice\sluice.toml`.

  The service hosts uvicorn **in-process** (not a `sluice serve`
  subprocess): the SCM supervises the real server, and `SvcStop` sets
  uvicorn's `should_exit` for a graceful drain instead of a hard kill.
  Runs via `pythonw.exe`. Service logging goes to a size-rotated
  `logs\service.log` (5 MB × 5), with notable events (`WARNING`+) also
  mirrored to the Windows Event Log (source `sluice`) for Event Viewer / WEF.
  File logging + rotation is Windows-service-only; elsewhere sluice logs to
  stdout and the platform rotates. `_cmd_serve` was split into a shared
  `_build_serve_app()` (config → app) and the `uvicorn.run` call so the
  service reuses the identical app.

  The install script relaxes `$ErrorActionPreference` around its pip calls
  and judges them by exit code — otherwise pip's stderr notices (e.g. a
  fresh-cache warning) are promoted to a terminating error and abort the
  install on a clean machine (masked whenever the pip cache is warm).

  Validated end-to-end on Windows Server 2025 (Python 3.14): fresh install,
  service reaches Running via the SCM dispatcher, in-process uvicorn logs to
  `logs\service.log`, `SvcStop` performs a graceful drain (observed
  "Application shutdown complete"), `/v1/usage` reconciliation is live, and
  real client requests proxy through to umans (`/v1/models` and
  `/v1/chat/completions` both 200). The install script env-forces
  `SLUICE_PROVIDER` (not just `SLUICE_UPSTREAM`) so a re-install over an
  existing config actually applies `-Provider` instead of silently keeping
  the old provider and yielding an incoherent provider/upstream pair.

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
