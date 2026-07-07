# Deploying sluice

sluice runs as a **single instance** — the concurrency invariant requires
exactly one sluice admitting traffic (never scale past 1). Two deployment
options are provided:

- **Docker Compose** — single-host, home-lab friendly. See [below](#docker-compose).
- **Kubernetes (ArgoCD GitOps)** — production, internal Traefik ingress. See
  [GitOps](#gitops-argocd).

## Docker Compose

`deploy/compose.yaml` is a self-contained single-service deployment with log
rotation, health checks, history persistence, and the same security hardening
as the k8s manifest (read-only root FS, all capabilities dropped, non-root
user).

### Quickstart

```sh
cd deploy/
cp .env.example .env
# edit .env:
#   SLUICE_USAGE_KEY=sk-...                        # your umans API key
#   SLUICE_ADMIN_TOKEN=$(openssl rand -hex 32)     # dashboard login token

docker compose up -d        # start
docker compose logs -f      # tail
docker compose down         # stop
```

The dashboard is at `http://<host>:8800/`. Compose makes `SLUICE_ADMIN_TOKEN`
mandatory (unlike standalone `docker run`), so visit `/` in a browser and paste
the token at the login page to get a session cookie. Bearer (`Authorization:
Bearer <token>`) and HTTP Basic auth also work for `curl` / Prometheus. See
[Securing the dashboard](../README.md#securing-the-dashboard) for details.

Point clients (opencode, open-webui, …) at `http://<host>:8800` instead of the
upstream provider.

### Configuration

All tunables are environment variables set in `.env` (see `.env.example` for
the full list with defaults). The compose file builds from source by default;
to use the pre-built image from GitHub Container Registry instead, comment out
`build:`/`image:` in `compose.yaml` and uncomment the `ghcr.io` line.

The compose default for `SLUICE_TARGET` is **4** (the provider's full
concurrency limit), matching the k8s deployment and unlike the CLI default of
3 (one-slot safety buffer). Set `SLUICE_TARGET=3` in `.env` for the
conservative buffer. See the main README's
[Tuning](../README.md#tuning) section for the trade-off.

History (sparkline data) persists across restarts in a named Docker volume
mounted at `/data`.

### Behind a reverse proxy

If you run behind Traefik, nginx, or Caddy, set `SLUICE_TRUSTED_PROXIES` in
`.env` to the proxy's CIDR so the `x-sluice-client-label` QoS header is
trusted:

```env
SLUICE_TRUSTED_PROXIES=172.16.0.0/12
```

## GitOps (ArgoCD)

`argocd/application.yaml` is the source of truth. ArgoCD watches `deploy/k8s` on
`main` (auto-sync, `prune` + `selfHeal`) and reconciles the cluster to it.

Image flow on each push to `main`:

1. `release.yml` builds + Trivy-scans the image, pushes `:latest` and
   `:<short-sha>` to `ghcr.io/hraedon/sluice`.
2. It then `kustomize edit set image` bumps `deploy/k8s/kustomization.yaml` to
   the immutable `:<short-sha>` and commits back.
3. ArgoCD sees the bumped kustomization and rolls out that exact image.

So the running tag is always an immutable digest tag, never a moving `:latest`.

### First-time bootstrap

The secret is **not** in git. Create it before the app first syncs:

```sh
kubectl create namespace sluice   # or let CreateNamespace=true handle it
kubectl create secret generic sluice-secrets \
  --namespace sluice \
  --from-literal=umans-api-key='sk-...' \
  --from-literal=admin-token="$(openssl rand -hex 32)"
```

`admin-token` is **required**: the deployment sets `SLUICE_ADMIN_TOKEN` from it,
which gates the admin routes (`/`, `/status.json`, `/metrics`, `/history.json`)
and enables the dashboard's config-mutation endpoints. The ServiceMonitor
presents the same token so Prometheus scrapes keep working.

Dashboard login is a **cookie-based login page** (not Basic-auth popup). Visit `/`
in a browser and paste the token at the login form — a signed session cookie is
set (30-day TTL, HttpOnly, SameSite=Strict). Bearer-token auth (for `sluice
status`, Prometheus, and `curl`) and HTTP Basic auth (`curl -u`) still work
unchanged for preemptive clients. Challenge-response clients (e.g. `wget`
without `--auth-no-challenge`, PowerShell `-Credential`) will loop, since the
WWW-Authenticate challenge is no longer sent. Retrieve the token later with:

```sh
kubectl get secret sluice-secrets -n sluice -o jsonpath='{.data.admin-token}' | base64 -d
```

Then register the app (one-time):

```sh
kubectl apply -f deploy/argocd/application.yaml
```

ArgoCD adopts any resources already present (e.g. a prior manual `kubectl apply`)
rather than recreating them.

## Exposing sluice externally (currently internal-only)

The manifests are kept ready but external exposure is **off**. The admin routes
are already token-gated (see above), so turning it on is one step:

1. Add `- ingress-external.yaml` to `kustomization.yaml` resources.
2. The proxy routes (`/v1/messages`, `/v1/chat/completions`) are always
   auth-bound — clients must present their own upstream key; sluice holds no
   key of its own for proxying.

The NetworkPolicy already admits the `traefik-external` namespace, so no
network-policy change is needed.

> The dashboard's JS fetch sends `credentials:'include'`, so the session cookie
> from the login page authorizes the `/status.json` poll automatically — no
> separate token-in-browser auth needed. Bearer-token clients (Prometheus,
> `sluice status`, `curl`) are unaffected.
