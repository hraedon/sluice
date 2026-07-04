# Deploying sluice

sluice runs as a **single-replica** (`Recreate`, never scale past 1 — the
concurrency invariant requires exactly one instance) Deployment in its own
namespace, fronted by an internal Traefik ingress.

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
presents the same token so Prometheus scrapes keep working. Dashboard login is
HTTP Basic — any username, the token as password. Retrieve it later with:

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

> The dashboard's JS fetch sends `credentials:'include'`, so browser-cached
> Basic auth from the dashboard login authorizes the `/status.json` poll
> automatically — no separate token-in-browser auth needed.
