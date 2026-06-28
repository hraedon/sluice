# Configuring clients to route through sluice

sluice only works if **every** client points at it instead of at umans directly. The
concurrency invariant is account-wide; a single client that still talks to
`api.code.umans.ai` is invisible to sluice and silently breaks the global count — that one
bypass reintroduces exactly the phantom/over-count problem sluice exists to prevent.

> **Rule:** all clients route through sluice, or the accounting is wrong. No exceptions.

## The one idea

Every client keeps its **umans API key and model names unchanged**, and only swaps its
provider **base URL** from umans to sluice. sluice forwards transparently and serves both
of umans' API surfaces, so pick the base URL matching the surface your client speaks:

| Client speaks | umans base URL (before) | sluice base URL (after) |
|---|---|---|
| OpenAI (`/v1/chat/completions`) | `https://api.code.umans.ai/v1` | `https://sluice.k8s.hraedon.com/v1` |
| Anthropic (`/v1/messages`) | `https://api.code.umans.ai` | `https://sluice.k8s.hraedon.com` |

The deployed instance is **`https://sluice.k8s.hraedon.com`** — the internal Traefik ingress,
real Let's Encrypt TLS on 443 (no port suffix; the container's `:8800` is internal to the
cluster). Reachable from the LAN only. Models stay `umans-coder`, `umans-flash`,
`umans-kimi-k2.7`, `umans-glm-5.2`.

### TLS

The deployed endpoint terminates TLS at the Traefik ingress with a real Let's Encrypt
certificate (`sluice-internal-tls`, issued for `sluice.k8s.hraedon.com`), so clients just
use the `https://` URL — no self-signed cert to trust, no plaintext fallback needed. This
also satisfies clients that refuse non-TLS base URLs. (If you run sluice locally for dev
instead of through the cluster, it listens on plain HTTP at `127.0.0.1:8800` — use that
`http://` URL there.)

---

## opencode

opencode uses an OpenAI-compatible provider via the AI SDK. Two equivalent routes:

**A. Let umans write the config, then redirect the base URL (lowest-effort).**
1. `umans opencode --setup` — writes the umans provider block into opencode's config.
2. In the file it wrote, change the provider's `options.baseURL` from
   `https://api.code.umans.ai/v1` to `https://sluice.k8s.hraedon.com/v1`. Leave the key and
   model list untouched.

**B. Configure the provider by hand** in `opencode.json` (project dir; `$schema` enables
validation):

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "umans": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "umans (via sluice)",
      "options": {
        "baseURL": "https://sluice.k8s.hraedon.com/v1",
        "apiKey": "{env:UMANS_API_KEY}"
      },
      "models": {
        "umans-coder": { "name": "umans-coder" },
        "umans-flash": { "name": "umans-flash" }
      }
    }
  }
}
```

Export the key (`export UMANS_API_KEY=sk-...`), then run `/models` in opencode to confirm
the provider appears. The provider id (`umans`) is what you select with `/connect`.

## Open WebUI

Open WebUI speaks the OpenAI surface, so use the `/v1` base URL.

1. **Settings → Admin Settings → Connections** (you must be an admin).
2. Under **OpenAI API**, add/manage a connection:
   - **API Base URL:** `https://sluice.k8s.hraedon.com/v1`
   - **API Key:** your umans key (`sk-...`)
3. Save, then verify the connection.

**Model-discovery gotcha:** Open WebUI auto-populates models by calling the OpenAI-standard
`GET /v1/models`. umans exposes model info at `/v1/models/info`, so the list may **not**
auto-fill. If it doesn't, add the model IDs manually in the connection's model settings
(`umans-coder`, `umans-flash`, `umans-kimi-k2.7`, `umans-glm-5.2`). This is a umans/OpenAI
path mismatch, not a sluice issue — sluice passes whatever umans returns through unchanged.

## hermes agent

Hermes (NousResearch) drives any **OpenAI-compatible** endpoint via `/v1/chat/completions`,
so use the `/v1` base URL. Config lives in `~/.hermes/config.yaml`; secrets in
`~/.hermes/.env`. Set a **custom** provider and point its base URL at sluice.

**Config file** (`~/.hermes/config.yaml`):

```yaml
model:
  provider: custom            # custom OpenAI-compatible endpoint
  default: umans-coder        # umans model name
  base_url: https://sluice.k8s.hraedon.com/v1
  api_key: ""                 # leave blank to fall back to ~/.hermes/.env
```

Put the key in `~/.hermes/.env` (or let it read `OPENAI_API_KEY`):

```
OPENAI_API_KEY=sk-...
```

**Or set it at runtime** (secrets auto-route to `.env`, config to `config.yaml`):

```bash
hermes config set model.base_url https://sluice.k8s.hraedon.com/v1
hermes config set model.provider custom
hermes config set OPENAI_API_KEY sk-...
```

Equivalently, the env vars `OPENAI_BASE_URL` / `OPENAI_API_KEY` set the endpoint and key
without editing the file. Precedence is CLI flags → `config.yaml` → `.env` → defaults, so a
stray `--model` or a leftover `OPENAI_BASE_URL` pointing at umans will silently bypass
sluice — check those first if hermes traffic doesn't show up in the dashboard.

---

## Verify each client after pointing it at sluice

1. Open the live dashboard at **`https://sluice.k8s.hraedon.com/`** (or poll JSON:
   `curl -s https://sluice.k8s.hraedon.com/status.json`). Both are unauthenticated and
   counts-only on the current internal deployment.
2. Send one test completion from the client.
3. Confirm `local_in_flight` increments while the request is in flight and releases when the
   response finishes; watch `concurrent_sessions` (umans ground truth) and `band` track it.

> The `sluice status` CLI targets `http://<host>` and is meant for a local/dev instance
> (`127.0.0.1:8800`) or a `kubectl port-forward`, not the TLS ingress — use the dashboard or
> `curl https://…/status.json` against the deployed instance.

If traffic does **not** appear in sluice but the client still gets responses, that client is
bypassing sluice — recheck its base URL (and any stray `OPENAI_BASE_URL` still pointing at
umans).

## Rollback

Point the client's base URL back at `https://api.code.umans.ai[/v1]`. No other change. Do
this per client; remember that any client left on the umans URL breaks the shared count for
everyone (see the rule at the top).
