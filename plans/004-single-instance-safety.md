# Plan 004 — Single-instance safety (make the invariant mechanical, not organizational)

sluice's entire value is being the **one** shared choke point: the account-wide concurrency
invariant can only live in a single process holding a single semaphore. Today that is
defended only by `deploy/k8s/deployment.yaml` (`replicas: 1`, `strategy: Recreate`) and a
CRITICAL code comment. If anyone ever sets `replicas: 2`, or a rollout briefly overlaps two
pods, the result is two independent semaphores, a silently blown account-wide cap, and **no
signal at all** until the box hits hours later. For a tool whose only job is to be the
single point of truth, the safety is currently a deployment convention, not a mechanism.

Goal of this plan: **a second sluice cannot quietly serve traffic. Running two fails
*loudly* — the loser refuses to admit requests and says why — and a pod is never marked
ready until it both holds the singleton claim and has a real usage reading.** This also
closes the reflection's filed WI-002 (`/readyz`).

## Design constraints (carried from AGENTS.md)

- **Fail safe.** A pod that cannot establish it is the singleton must **not** open its gate.
  Uncertainty about peers tightens to closed, never opens.
- **Portable core, pluggable guard.** The singleton mechanism is an *operational* concern;
  it must not leak into the pure controller or make local/dev runs require a cluster. A
  `SingletonGuard` interface with a no-op default (local) and a real backend (cluster) keeps
  `sluice serve` runnable on a laptop with zero infra.
- **In-path but inert.** The guard gates *admission*; it never touches request content.

## Work items

### WI-001 — `SingletonGuard` abstraction + no-op default (`sluice.singleton`)
- A small interface: `acquire()` (claim or fail), `renew()` (keep the claim), `is_held() ->
  bool`, `release()`. Pure-ish shell module; the proxy/reconcile consult `is_held()`.
- Default implementation `NoopGuard` always holds — preserves today's local-run behaviour;
  the cluster overlay swaps in the real backend. Default-off keeps the laptop path trivial.
- Tests: no-op always-held; interface contract (acquire→held, release→not-held).

### WI-002 — Kubernetes Lease backend (`KubeLeaseGuard`)
The proportionate mechanism for the family's k8s deployment: a `coordination.k8s.io/v1`
`Lease` is the standard leader-election primitive and needs no database.

- On startup `acquire()` creates/grabs the Lease (name `sluice`, in sluice's namespace),
  stamping `holderIdentity` with the pod name. If the Lease is **held and unexpired by
  another holder**, `acquire()` **fails** → the pod refuses to serve (see WI-004).
- A background renewer updates `renewTime` every `lease_renew_interval`; `lease_duration`
  is a few renew intervals. If renewal fails (lost the lease / API unreachable past the
  duration), `is_held()` flips false → the gate sheds (WI-004).
- Reads identity from the downward API (`POD_NAME`, `POD_NAMESPACE` env, already idiomatic);
  in-cluster client via the mounted service-account token. Behind the optional `kubernetes`
  extra so the base install stays dependency-light.
- Tests: against a faked Lease API (no real cluster) — acquire when free; refuse when held
  by a live peer; reacquire after a peer's lease expires; `is_held()` flips on renew
  failure. (Per AGENTS.md #6, the *real* proof is a cluster run, not these mocks.)

### WI-003 — RBAC + deploy wiring (`deploy/k8s`)
- A `Role`/`RoleBinding` granting `get/create/update` on `leases` in
  `coordination.k8s.io`, scoped to the `sluice` lease name where possible; bound to
  sluice's ServiceAccount.
- Inject `POD_NAME`/`POD_NAMESPACE` via the downward API in the Deployment.
- Enable `KubeLeaseGuard` in the cluster overlay (env/flag), leaving the base/local config
  on `NoopGuard`.
- Keep `replicas: 1` + `Recreate` as defence-in-depth — the Lease makes a *misconfigured*
  `replicas: 2` safe (one pod sheds) instead of silently doubling the cap.

### WI-004 — Admission + readiness honour the guard (`sluice.proxy`, `sluice.reconcile`)
- **Admission:** if `guard.is_held()` is false, the proxy fast-fails with `503` +
  `Retry-After` and `reason: "not_leader"` (reuse Plan 003's gate-closed-reason path). A
  non-leader pod streams nothing.
- **Reconciliation:** a non-leader pod must **not** poll `/v1/usage` and must hold the gate
  closed — it is not the authority and should add no load (not even read load beyond what's
  needed to stay a hot standby).
- **`/readyz`** (closes reflection WI-002): ready iff `guard.is_held()` **and** the first
  usage poll has succeeded. Liveness (`/healthz`) stays independent so a standby is live but
  not ready. Wire `readinessProbe: /readyz` in the Deployment so a standby never receives
  Service traffic.
- Tests: non-leader → 503 `not_leader` + no usage poll; `/readyz` 503 before first poll,
  200 after; `/healthz` 200 throughout.

## Sequencing
WI-001 (interface + no-op, nothing breaks locally) → WI-004 (admission/readiness consult the
guard, proven against the no-op) → WI-002 (real Lease backend) → WI-003 (RBAC + overlay).
Wire the consumers against the trivial guard first, then drop in the real backend.

## Done when
- Two sluice pods scheduled at once (deliberately set `replicas: 2` in a throwaway test):
  exactly one becomes ready and serves; the other stays **not ready**, refuses admission
  with `not_leader`, and does **not** poll `/v1/usage`. Kill the leader → the standby
  acquires the lease and goes ready within `lease_duration`.
- Local `sluice serve` with no cluster still runs (no-op guard) — no regression for dev.
- `/readyz` gates Service traffic; `/healthz` unaffected.
- CI green on 3.12 + 3.13.

## Validation notes
- The honest proof is the **two-pod cluster experiment above**, watching `/v1/usage` show a
  *single* poller's cadence (not doubled) and the standby holding closed — mocks can't show
  the real lease race (AGENTS.md #6).
- Confirm a rollout (`kubectl rollout restart`) hands the lease over cleanly: brief overlap
  must not produce two *admitting* pods — the new pod waits for the old to drop the lease.
- Failure mode to check explicitly: if the k8s API is unreachable, the holder keeps serving
  until `lease_duration` lapses, then sheds. Verify it sheds (fail-safe) rather than
  serving indefinitely on a stale claim.
