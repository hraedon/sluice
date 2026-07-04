# Plan 012 — Dashboard login page (replace the Basic-auth popup)

**Status:** Proposed 2026-07-04

## Motivation

Enabling `SLUICE_ADMIN_TOKEN` (2026-07-04) gates the dashboard behind HTTP
Basic auth. That works in a desktop browser but **fails on iOS Home-Screen
web-app shortcuts**: a site saved to the Home Screen runs in standalone
webview mode, which does not render the Basic-auth dialog — the navigation
just dies on the 401. The phone is exactly where a glanceable concurrency
dashboard earns its keep, so the popup has to go.

Secondary irritation: the Basic dialog demands a username that sluice
ignores. The credential is one token; the login surface should say so —
a single token field, Vault-style, no username to invent.

## Design decisions

1. **Cookie session is an additive third path, not a replacement.**
   `check_admin_auth` today accepts Bearer (API clients, Prometheus
   ServiceMonitor, `sluice status`) and Basic (curl `-u`). Both stay exactly
   as they are — every documented consumer keeps working. Browsers gain a
   third path: a signed session cookie minted by a login page. No existing
   caller changes.

2. **`/` serves the login page when unauthenticated; JSON routes never
   challenge.** The popup is triggered by `WWW-Authenticate: Basic` on the
   401. New behavior:
   - `GET /` without valid auth → **200 with the login page** (no 401, no
     challenge header, popup never fires anywhere).
   - `/status.json`, `/history.json`, `/metrics` without valid auth →
     **401 JSON body, no `WWW-Authenticate` header.** API consumers send
     credentials proactively (curl `-u` and Bearer both do), so nothing
     depended on the challenge; the dashboard JS redirects to `/` on a 401
     (session expired mid-view).
   The login page itself leaks nothing (the dashboard behind it is
   counts-only anyway); it reveals only that a sluice lives here, which the
   TLS certificate already does.

3. **Token-only login form.** One `<input type="password">` labelled
   "admin token", one submit button, minimal inline CSS matching the
   dashboard. No username field at all (decision: *don't display it*, the
   Vault pattern, rather than a displayed stock name — there is nothing for
   a user to remember). `POST /login` verifies with the existing
   constant-time compare, sets the cookie, 303-redirects to `/`.

4. **Session mechanics — stdlib only, no new secret, no server-side state.**
   Cookie value is `expiry.hmac_sha256(session_key, expiry)` where
   `session_key` is derived from the admin token
   (`sha256(b"sluice-session-v1" + admin_token)`), so:
   - no new secret to provision or back up;
   - sessions survive pod restarts / image bumps (Recreate rolls on every
     release — forcing the phone to re-login weekly would be self-defeating);
   - rotating the admin token invalidates every session, which is the
     revocation story and is correct.
   Verification is a pure function (mint/verify pair unit-tested without
   ASGI, constant-time, rejects expired/garbled/None). Cookie attributes:
   `HttpOnly; SameSite=Strict; Path=/; Max-Age=<--session-ttl>`, default
   30 days (the device is trusted and the token itself never leaves the
   secret store; `/logout` exists for untrusted-device hygiene).

   **`Secure` is auto, not unconditional — the Docker/local case.** A
   `Secure` cookie is silently dropped by browsers on a plain-HTTP,
   non-localhost origin — which is exactly the README's Docker quickstart
   (`http://<lan-ip>:8800`); an unconditional `Secure` would produce an
   unexplainable login loop there. Rule: set `Secure` when the effective
   scheme is https (`scope["scheme"]`, or `X-Forwarded-Proto: https` —
   the k8s pod sees plain http behind Traefik's TLS termination, so the
   forwarded header is the k8s truth) or the host is
   localhost/127.0.0.1 (secure contexts per spec). A plain-HTTP LAN
   origin gets a non-`Secure` cookie plus one logged warning naming the
   trade-off — degrading visibly beats breaking silently, and the
   alternative (refusing login) would regress the documented Docker
   path. Trusting a spoofed `X-Forwarded-Proto: https` only *adds* the
   `Secure` attribute, which is harmless.

5. **CSRF: SameSite=Strict + fetch-metadata check on mutations.** Cookie
   auth makes `POST /admin/config` CSRF-relevant (cached Basic auth had the
   same property — this plan *improves* the posture, it doesn't create the
   exposure). Two layers, no token plumbing in JS: the cookie is
   `SameSite=Strict` (cross-site requests never carry it), and mutation
   handlers additionally reject cookie-authenticated requests whose
   `Sec-Fetch-Site` header is present and not `same-origin`. Bearer-token
   mutations are exempt from the fetch-metadata check (headers can't be set
   cross-site by an attacker page).

6. **Rule 7: the session cookie is sluice-internal and must not egress.**
   The proxy already strips `Authorization` values matching the admin token
   before forwarding. The sluice session cookie gets the same treatment: the
   `sluice_session` cookie is removed from the `Cookie` header on proxied
   requests (other cookies pass through untouched — byte-transparency for
   everything that isn't ours). Covered by the cache-transparency test net.

7. **Login throttling is global, not per-IP.** The ingress masks client IPs
   (known homelab gotcha), so per-IP limits are theater. A global in-memory
   limiter (e.g. 10 failures / 5 minutes → login disabled for the window,
   constant small delay on every failure, each failure logged) is honest for
   a single-operator tool and keeps the token un-bruteforceable in any
   realistic window (it's 64 hex chars regardless).

8. **Leader/readiness semantics unchanged.** Login works on a non-leader
   (it's a read of the token, not a mutation); mutations keep their existing
   leader-only 503.

## Work items

### WI-001 — Session primitives (pure)
`mint_session(admin_token, now, ttl) -> str` and
`verify_session(cookie_value, admin_token, now) -> bool` (new small module or
`admin.py` section; stdlib `hmac`/`hashlib`/`base64` only). Constant-time
verify; expired, malformed, empty, and None values all fail closed; a token
rotation invalidates outstanding cookies.
**AC:** unit tests cover round-trip, expiry boundary, tamper (flip any byte),
token rotation, and garbage input; no ASGI plumbing needed to test.

### WI-002 — `/login` + `/logout` routes
`GET /login` serves the static token-only form (also returned by
unauthenticated `GET /`); `POST /login` (form-encoded) verifies via the
existing constant-time compare, applies the WI-005 throttle, sets the cookie,
303 → `/`; failure re-renders the form with a neutral error (no
valid/invalid-shape distinction). `POST /logout` clears the cookie and 303s
to `/login`. All three respond 404 when `SLUICE_ADMIN_TOKEN` is unset
(tokenless deployments keep today's open behavior, nothing new to reach).
**AC:** correct token → cookie set with the §4 attributes + redirect; wrong
token → no cookie, neutral error; logout clears; tokenless mode: routes 404
and `/` serves the dashboard directly as today; `Secure` present on
https/forwarded-https/localhost requests and absent (with the logged
warning) on plain-HTTP LAN requests — one test per origin class.

### WI-003 — Auth acceptance + challenge removal
`check_admin_auth` accepts a valid session cookie alongside Bearer/Basic.
Unauthenticated `GET /` → login page (200); unauthenticated JSON/metrics
routes → 401 **without** `WWW-Authenticate`; dashboard JS redirects to `/`
on any 401 from its fetches; dashboard gains a logout control.
**AC:** all three credential forms authorize every admin route; no response
anywhere carries `WWW-Authenticate` anymore; curl `-u` and Bearer paths
still pass (regression tests); expired cookie mid-session lands the browser
back on the login form, not a popup and not a blank page.

### WI-004 — CSRF + rule-7 egress hygiene
Mutation handlers apply the §5 fetch-metadata check to cookie-authenticated
requests. The proxy request-header filter drops the `sluice_session` cookie
(and only it) from forwarded `Cookie` headers.
**AC:** cookie-auth'd mutation with `Sec-Fetch-Site: cross-site` → 403;
same-origin and Bearer paths unaffected; cache-transparency test proves a
proxied request with `Cookie: a=1; sluice_session=x; b=2` egresses with
`Cookie: a=1; b=2` byte-identical otherwise, and one with no sluice cookie
egresses untouched.

### WI-005 — Global login throttle
In-memory global failure window per §7 with logging; unit-tested clock-driven.
**AC:** N failures within the window lock the form (429 with Retry-After)
until the window slides; success resets nothing retroactive (no lockout
bypass); every failure emits one log line.

### WI-006 — Home-Screen polish (the actual use case)
`manifest.webmanifest` (name, `display: standalone`, theme color) +
`apple-touch-icon` (small inline-generated PNG, no external assets) served
from static; `<link>`/meta tags in dashboard and login pages.
**AC:** saving `https://sluice…/` to the iOS Home Screen yields a named,
iconed, standalone app that lands on the login form and, after one token
paste, on the live dashboard; cookie persists across shortcut launches for
the session TTL.

### WI-007 — Docs
`deploy/README.md` (login flow replaces the Basic-auth login note; token
retrieval command unchanged), `docs/client-configuration.md` (browser step
says "paste the token at the login page"; the authenticated-curl step is
unchanged), README dashboard section — including a Docker/local note: with
`SLUICE_ADMIN_TOKEN` unset nothing changes at all, and on plain-HTTP LAN
access the session cookie is non-`Secure` by design (§4).
**AC:** docs describe the login page; no doc anywhere still instructs the
reader to expect a Basic-auth prompt in a browser; the Docker quickstart
works as documented with and without a token.

## Sequencing & notes

- Order: WI-001 → WI-002/003 together (the route flip and the acceptance
  change ship in one commit — splitting them strands the login page behind a
  popup) → WI-004/005 → WI-006/007.
- **Live validation is the user's iPhone**, not CI: add the shortcut, cold
  launch, login, relaunch after >5 min (webview process death), confirm no
  popup and no re-login inside the TTL. CI can't see this; say so in the
  closeout rather than claiming it.
- Prometheus/ServiceMonitor, `sluice status`, and the authenticated-curl
  docs are deliberately untouched surfaces — a regression test pins each.
- Non-goals: multi-user accounts (one token, one operator — per-user
  identity lives in the suite's dossier/regista world, not here), remember-me
  UI (the TTL is the policy), password managers (they fill single-field
  forms fine).
