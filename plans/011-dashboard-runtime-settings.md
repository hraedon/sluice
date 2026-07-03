# Plan 011 — Runtime settings from the dashboard (target first)

## Motivation

The WI-024 capture night (2026-07-03) made the cost of static-only config
concrete. Changing `--target` requires: edit deployment.yaml → commit → push →
CI → gitops image/manifest bump → ArgoCD sync → **pod replacement**. That is
minutes of latency when the operator wants seconds, and the replacement
destroys pod-local state — the capture pod was killed by its own image-bump
rollout, taking the only copy of the 429 log and the history buffer with it.

Operational tuning of the permit count is a runtime decision, not a deploy
decision. The gate is already resizable every tick (`reconcile` calls
`gate.resize` from `effective_permits`); what's missing is an authenticated
way to move the *config* input to that math without a rollout.

Scope: `target` only in the first cut. The mechanism (override store +
endpoint + UI + audit) generalizes, but every additional mutable knob is
added deliberately, never by reflection over the config dataclass.

## Design decisions

1. **Overrides are ephemeral by design.** Git/ArgoCD stays the durable source
   of truth; a dashboard change is an operational override, morally the same
   as `kubectl scale` on an HPA-less deployment. Pod restart reverts to the
   manifest args. No writable config volume, no persistence — drift that
   survives restarts must go through git.
2. **Drift must be visible.** `/status.json` gains an `overrides` object
   (`{"target": {"boot": 4, "override": 6, "since": <epoch>}}`, empty when
   none). The dashboard Config card shows an `override` badge and a revert
   control. Anyone comparing the manifest to live state can see what was
   changed at runtime, by whom (audit log), and since when.
3. **Mutation requires the admin token, full stop.** Read routes may stay
   tokenless (current deployment), but if `SLUICE_ADMIN_TOKEN` is unset the
   mutation endpoint is disabled (405/403 with a clear body). This forces the
   WI-008 question (enabling the token blanks the tokenless dashboard status
   fetch) to be settled as part of this plan — likely: dashboard served with
   basic auth (already supported), same credentials authorize the POST.
4. **Bounds are provider-aware, validated in the core.** A pure function
   `validate_target_override(value, reading, config) -> str | None` in
   `sluice.control`: reject `< 1`, reject `> hard_cap` from the latest
   reading (never allow configuring a value the provider will punish),
   accept-with-warning above `limit` (that is exactly the WI-024 experiment
   shape, sometimes wanted — the response body carries the warning, the
   audit log records it).
5. **Leader-only.** Only the singleton leader applies overrides (non-leaders
   fast-fail requests anyway); an override does not survive leader failover
   (it lives in the leader's process memory — consistent with ephemerality).

## Work items

### WI-001 — Override store + core validation

`ReconciliationLoop.apply_override("target", value)` / `clear_override("target")`
guarded by a whitelist. The loop rebuilds its `ControllerConfig` via
`dataclasses.replace` and the next tick resizes the gate — no new resize
path. `validate_target_override` in `control.py` (pure, unit-tested).
Boot values retained for revert and for the `overrides` status object.

### WI-002 — Admin mutation endpoint

`POST /admin/config` with JSON body `{"target": 6}` in `admin.py`;
`DELETE /admin/config/target` (or `{"target": null}`) reverts. 400 +
reason on validation failure, 403 without a valid admin token, disabled
when no token is configured. Every accepted change logs
`config override: target 4 -> 6 (basic-auth user=..., remote=...)`.

### WI-003 — Status + metrics surfacing

`overrides` in `/status.json`; `sluice_config_target` gauge already derivable —
add `sluice_config_overridden{field="target"}` 0/1 so alerting can catch
forgotten overrides (e.g. warn after 24 h overridden).

### WI-004 — Dashboard UI

Config card: the `target` row gains a patina-minimal stepper (−/value/+) and
an `override` badge + revert link when active. Fetch with the same
credentials the page was loaded with (basic auth); on 403, render read-only.
No optimistic update — re-fetch `/status.json` after the POST and render
what the server says.

### WI-005 — Tests + CI

Unit: validation bounds (0, 1, limit, limit+1→warning, hard_cap, hard_cap+1),
whitelist rejection, revert restores boot value. Integration: POST → next
tick resizes gate; unauthorized POST rejected; no-token deployment has the
endpoint disabled; override visible in status. UI logic covered by the
existing dashboard test approach (static asset asserts) where feasible.

## Explicitly out of scope

- Persisting overrides across restarts (git owns durable config).
- Mutating upstream URL, provider, auth, or listen address at runtime
  (security surface, no operational need).
- Multi-replica override propagation (singleton model; leader-only).
- Arbitrary config-field mutation (whitelist grows one deliberate field at
  a time; `queue_timeout` and `reserve` are the plausible next two).

## Acceptance

- Changing target from the dashboard takes effect within one poll interval,
  with no pod restart, and reverts cleanly (button or restart).
- An operator reading `/status.json` or the dashboard can always tell
  boot-config from override, and the audit log answers who/when/what.
- A deployment with no admin token has no mutation surface at all.
