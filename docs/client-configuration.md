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
| OpenAI (`/v1/chat/completions`) | `https://api.code.umans.ai/v1` | `http(s)://<sluice-host>:<port>/v1` |
| Anthropic (`/v1/messages`) | `https://api.code.umans.ai` | `http(s)://<sluice-host>:<port>` |

Assume sluice runs at `sluice.lab:8800` below; substitute your host/port. Models stay
`umans-coder`, `umans-flash`, `umans-kimi-k2.7`, `umans-glm-5.2`.

### A note on TLS before you start

umans is HTTPS. Pointing a client at `http://sluice.lab:8800` works only if the client
permits a plaintext base URL — some clients refuse non-TLS or non-localhost endpoints. Two
options:

- **Trusted LAN, plaintext:** run sluice on HTTP; simplest, fine for a closed home lab.
- **TLS:** put a cert on sluice (self-signed trusted on each client host, or a lab CA — you
  already run one) and use `https://sluice.lab:8800`. Prefer this if any client is strict.

Decide once; the per-client steps below are identical apart from the `http`/`https` scheme.

---

## opencode

opencode uses an OpenAI-compatible provider via the AI SDK. Two equivalent routes:

**A. Let umans write the config, then redirect the base URL (lowest-effort).**
1. `umans opencode --setup` — writes the umans provider block into opencode's config.
2. In the file it wrote, change the provider's `options.baseURL` from
   `https://api.code.umans.ai/v1` to `http(s)://sluice.lab:8800/v1`. Leave the key and
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
        "baseURL": "http://sluice.lab:8800/v1",
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
   - **API Base URL:** `http(s)://sluice.lab:8800/v1`
   - **API Key:** your umans key (`sk-...`)
3. Save, then verify the connection.

**Model-discovery gotcha:** Open WebUI auto-populates models by calling the OpenAI-standard
`GET /v1/models`. umans exposes model info at `/v1/models/info`, so the list may **not**
auto-fill. If it doesn't, add the model IDs manually in the connection's model settings
(`umans-coder`, `umans-flash`, `umans-kimi-k2.7`, `umans-glm-5.2`). This is a umans/OpenAI
path mismatch, not a sluice issue — sluice passes whatever umans returns through unchanged.

## hermes agent

> **Needs your input to make concrete.** Confirm (a) whether the hermes agent speaks the
> OpenAI or the Anthropic surface, and (b) where it reads its provider base URL + key
> (config file / env var / launch flag). The general pattern is all that changes:

- Find hermes' model/provider configuration (base URL + API key).
- Point the base URL at the **matching** sluice surface:
  - OpenAI-compatible → `http(s)://sluice.lab:8800/v1`
  - Anthropic-compatible → `http(s)://sluice.lab:8800`
- Leave the umans key and model names as they were.
- If hermes authenticates Anthropic-style, it sends `x-api-key`; OpenAI-style sends
  `Authorization: Bearer`. sluice forwards either header unchanged — no change needed on
  sluice's side.

_(Once you confirm hermes' specifics, this section gets the same concrete treatment as the
two above.)_

---

## Verify each client after pointing it at sluice

1. Open the sluice dashboard (admin port) or run `sluice status`.
2. Send one test completion from the client.
3. Confirm the client's request shows up as an active slot (its source host appears) and
   `local in-flight` increments, then releases when the response finishes.

If traffic does **not** appear in sluice but the client still gets responses, that client is
bypassing sluice — recheck its base URL.

## Rollback

Point the client's base URL back at `https://api.code.umans.ai[/v1]`. No other change. Do
this per client; remember that any client left on the umans URL breaks the shared count for
everyone (see the rule at the top).
