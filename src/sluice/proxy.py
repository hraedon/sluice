"""Async reverse-proxy shell — streaming passthrough with concurrency gating.

Forwards both ``/v1/messages`` and ``/v1/chat/completions`` (and any other path,
transparently) to the configured upstream.  Acquires a permit before forwarding;
releases on completion **or** downstream disconnect.  On disconnect, exits the
upstream streaming context promptly so the upstream sees a terminated request,
not an abandoned one — phantom prevention.

True streaming: request and response bytes are forwarded as they arrive, never
buffered into a full body.  Auth headers pass through unchanged; sluice holds no
key of its own beyond the usage poller's.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import httpx

from sluice.gate import PermitGate
from sluice.reconcile import ReconciliationLoop
from sluice.singleton import SingletonGuard
from sluice.status import snapshot as status_snapshot
from sluice.status import to_prometheus

log = logging.getLogger("sluice.proxy")

# ASGI callable types.
Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]

# RFC 7230 hop-by-hop headers — never forwarded in either direction.
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)

# Sluice-internal control headers — consumed and stripped before forwarding.
# These are QoS / routing metadata sluice uses; they must never reach the upstream
# so the request hashes identically to a direct client (cache-transparency, AGENTS.md #7).
_CONTROL_HEADERS = frozenset(
    {
        "x-sluice-client-label",  # QoS client label (Plan 005)
        "x-sluice-qos",           # future QoS class
    }
)

# All headers stripped from the request before forwarding upstream.
_STRIP_REQUEST = _HOP_BY_HOP | _CONTROL_HEADERS | frozenset({"host"})

_QUEUE_TIMEOUT_DEFAULT = 30.0
_RETRY_AFTER_DEFAULT = 5


class ProxyApp:
    """ASGI reverse proxy with concurrency gating."""

    def __init__(
        self,
        *,
        upstream_base_url: str,
        gate: PermitGate,
        reconcile: ReconciliationLoop,
        queue_timeout: float = _QUEUE_TIMEOUT_DEFAULT,
        upstream_client: httpx.AsyncClient | None = None,
        guard: SingletonGuard | None = None,
        admin_token: str | None = None,
    ) -> None:
        self._upstream = upstream_base_url.rstrip("/")
        self._gate = gate
        self._reconcile = reconcile
        self._queue_timeout = queue_timeout
        self._client = upstream_client or httpx.AsyncClient(timeout=None)
        self._owns_client = upstream_client is None
        self._guard = guard
        self._admin_token = admin_token

    def _check_admin_auth(self, scope: Scope) -> bool:
        """Return True if the request is authorized for admin routes."""
        if self._admin_token is None:
            return True
        for k, v in scope.get("headers", []):
            if k == b"authorization":
                expected = f"Bearer {self._admin_token}"
                if v.decode("latin-1") == expected:
                    return True
        return False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await self._handle_lifespan(receive, send)
            return
        if scope["type"] != "http":
            return

        path = scope["path"]
        if path == "/healthz":
            await self._send_json(send, 200, {"status": "ok"})
            return
        if path == "/readyz":
            ready = self._reconcile.ready
            if self._guard is not None:
                ready = ready and self._guard.is_held()
            if ready:
                await self._send_json(send, 200, {"status": "ready"})
            else:
                await self._send_json(send, 503, {"status": "not ready"})
            return

        # Admin routes — token-gated when admin_token is set.
        if path in ("/", "/status.json", "/metrics"):
            if not self._check_admin_auth(scope):
                await self._send_text(send, 401, "Unauthorized", content_type="text/plain")
                return
            if path == "/":
                await self._send_dashboard(send)
                return
            if path == "/status.json":
                await self._send_status_json(send)
                return
            if path == "/metrics":
                await self._send_prometheus(send)
                return

        await self._proxy_request(scope, receive, send)

    async def _handle_lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                if self._guard is not None:
                    acquired = await self._guard.acquire()
                    if not acquired:
                        log.warning("singleton guard acquire failed — starting as non-leader")
                    else:
                        await self._guard.start_renewer()
                        await self._reconcile.start()
                else:
                    await self._reconcile.start()
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                await self._reconcile.stop()
                if self._guard is not None:
                    await self._guard.stop_renewer()
                    await self._guard.release()
                if self._owns_client:
                    await self._client.aclose()
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _proxy_request(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Non-leader fast-fail: if the singleton guard is not held, refuse admission.
        if self._guard is not None and not self._guard.is_held():
            log.info("not leader — fast-failing 503")
            await self._send_json(
                send,
                503,
                {"error": "not_leader", "reason": "not_leader", "retry_after": _RETRY_AFTER_DEFAULT},
                retry_after=_RETRY_AFTER_DEFAULT,
            )
            return

        # Fast-fail if the gate is closed for a structural reason (boxed / breaker).
        # Don't burn the queue timeout against a gate that cannot open.
        reason = self._reconcile.gate_closed_reason()
        if reason in ("boxed", "breaker"):
            retry_after = self._reconcile.retry_after_seconds()
            log.info("gate closed (%s) — fast-failing 503", reason)
            await self._send_json(
                send,
                503,
                {"error": reason, "reason": reason, "retry_after": retry_after},
                retry_after=retry_after,
            )
            return

        acquired = await self._gate.acquire(timeout=self._queue_timeout)
        if not acquired:
            log.info("permit queue timeout — returning 503")
            await self._send_json(
                send,
                503,
                {"error": "concurrency limit reached", "reason": "saturated", "retry_after": _RETRY_AFTER_DEFAULT},
                retry_after=_RETRY_AFTER_DEFAULT,
            )
            return

        try:
            await self._forward(scope, receive, send)
        except Exception:
            log.exception("proxy forward failed")
        finally:
            await self._gate.release()

    async def _forward(self, scope: Scope, receive: Receive, send: Send) -> None:
        url = self._build_url(scope)
        headers = self._filter_request_headers(scope["headers"])
        method = scope["method"]

        disconnect = asyncio.Event()
        body_queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def pump_receive() -> None:
            """Single consumer of the ASGI receive callable."""
            while True:
                event = await receive()
                etype = event["type"]
                if etype == "http.disconnect":
                    disconnect.set()
                    await body_queue.put(None)  # unblock body_stream
                    return
                if etype == "http.request":
                    data = event.get("body", b"")
                    if data:
                        await body_queue.put(data)
                    if not event.get("more_body", False):
                        await body_queue.put(None)
                        # Keep listening for disconnect during response phase.

        async def body_stream() -> AsyncIterator[bytes]:
            while True:
                chunk = await body_queue.get()
                if chunk is None:
                    return
                yield chunk

        pump_task = asyncio.create_task(pump_receive())

        try:
            async with self._client.stream(
                method, url, headers=headers, content=body_stream()
            ) as response:
                # Report status to the reconciliation loop.
                if response.status_code == 429:
                    self._reconcile.record_429()
                elif 200 <= response.status_code < 400:
                    self._reconcile.record_success()

                await send(
                    {
                        "type": "http.response.start",
                        "status": response.status_code,
                        "headers": self._encode_response_headers(response),
                    }
                )

                async for chunk in response.aiter_raw():
                    if disconnect.is_set():
                        break
                    try:
                        await send(
                            {
                                "type": "http.response.body",
                                "body": chunk,
                                "more_body": True,
                            }
                        )
                    except Exception:
                        # send failed — client gone
                        disconnect.set()
                        break

                if not disconnect.is_set():
                    await send(
                        {"type": "http.response.body", "body": b"", "more_body": False}
                    )
        except httpx.RequestError as exc:
            if not disconnect.is_set():
                log.warning("upstream error: %s: %s", type(exc).__name__, exc)
                try:
                    await self._send_json(send, 502, {"error": "upstream error"})
                except Exception:
                    pass
        finally:
            if not pump_task.done():
                pump_task.cancel()
                try:
                    await pump_task
                except asyncio.CancelledError:
                    pass

    def _build_url(self, scope: Scope) -> str:
        path: str = scope["path"]
        qs: bytes = scope.get("query_string", b"")
        if qs:
            path += "?" + qs.decode("latin-1")
        return self._upstream + path

    @staticmethod
    def _filter_request_headers(scope_headers: list[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
        result: list[tuple[str, str]] = []
        for k, v in scope_headers:
            name = k.decode("latin-1").lower()
            if name in _STRIP_REQUEST:
                continue
            result.append((k.decode("latin-1"), v.decode("latin-1")))
        return result

    @staticmethod
    def _encode_response_headers(response: httpx.Response) -> list[tuple[bytes, bytes]]:
        return [
            (k.encode("latin-1"), v.encode("latin-1"))
            for k, v in response.headers.items()
            if k.lower() not in _HOP_BY_HOP
        ]

    async def _send_json(
        self,
        send: Send,
        status: int,
        body: dict[str, Any],
        *,
        retry_after: int | None = None,
    ) -> None:
        payload = json.dumps(body).encode()
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(payload)).encode()),
        ]
        if retry_after is not None:
            headers.append((b"retry-after", str(retry_after).encode()))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": payload, "more_body": False})

    async def _send_status_json(self, send: Send) -> None:
        snap = status_snapshot(self._reconcile)
        await self._send_json(send, 200, snap.to_dict())

    async def _send_prometheus(self, send: Send) -> None:
        snap = status_snapshot(self._reconcile)
        text = to_prometheus(snap)
        await self._send_text(send, 200, text, content_type="text/plain; version=0.0.4; charset=utf-8")

    async def _send_dashboard(self, send: Send) -> None:
        await self._send_text(send, 200, _DASHBOARD_HTML, content_type="text/html; charset=utf-8")

    async def _send_text(
        self,
        send: Send,
        status: int,
        body: str,
        *,
        content_type: str = "text/plain",
    ) -> None:
        payload = body.encode()
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", content_type.encode()),
            (b"content-length", str(len(payload)).encode()),
        ]
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": payload, "more_body": False})


_DASHBOARD_HTML = """\
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>sluice</title>
<style>
:root{--bg:#0d1117;--fg:#c9d1d9;--accent:#58a6ff;--warn:#d29922;--alarm:#f85149;
--card:#161b22;--border:#30363d;--mono:monospace;--gap:#6e7681}
*{box-sizing:border-box}body{font-family:var(--mono);background:var(--bg);color:var(--fg);margin:0;padding:1rem}
h1{font-size:1rem;font-weight:400;margin:0 0 1rem}
.row{display:flex;gap:1rem;flex-wrap:wrap}
.card{background:var(--card);border:1px solid var(--border);border-radius:6px;padding:1rem;min-width:260px}
.ladder{display:flex;flex-direction:column;gap:2px;width:100%}
.band{display:flex;align-items:center;gap:.5rem;padding:.4rem .6rem;border-radius:4px;font-size:.8rem;position:relative}
.boxed{background:rgba(248,81,73,.15);border:1px solid var(--alarm);color:var(--alarm)}
.reject{background:rgba(248,81,73,.08);border:1px solid rgba(248,81,73,.3)}
.low{background:rgba(210,153,34,.08);border:1px solid rgba(210,153,34,.3)}
.normal{background:rgba(88,166,255,.05);border:1px solid var(--border)}
.target-line{border-top:1px dashed var(--accent);padding:.3rem .6rem;font-size:.7rem;color:var(--accent)}
.marker{font-size:.9rem}.obs{color:var(--accent)}.loc{color:var(--warn)}.phantom{color:var(--gap)}
table{width:100%;border-collapse:collapse;font-size:.75rem}
td,th{padding:.2rem .4rem;text-align:left;border-bottom:1px solid var(--border)}
th{color:var(--gap);font-weight:400}
.banner{padding:.6rem 1rem;border-radius:4px;margin-bottom:1rem;font-size:.8rem;display:none}
.banner.boxed{display:block;background:rgba(248,81,73,.15);color:var(--alarm);border:1px solid var(--alarm)}
.banner.breaker{display:block;background:rgba(210,153,34,.15);color:var(--warn);border:1px solid var(--warn)}
#countdown{font-weight:700}
.stale{color:var(--gap);font-size:.7rem}
</style>
</head>
<body>
<h1>sluice</h1>
<div id="banner-boxed" class="banner boxed">ACCOUNT BOXED — retry after <span id="countdown">?</span>s</div>
<div id="banner-breaker" class="banner breaker">CIRCUIT BREAKER OPEN — backing off</div>
<div class="row">
  <div class="card">
    <h2 style="font-size:.8rem;font-weight:400;color:var(--gap)">Gate Ladder</h2>
    <div class="ladder" id="ladder"></div>
  </div>
  <div class="card">
    <h2 style="font-size:.8rem;font-weight:400;color:var(--gap)">Reading</h2>
    <table id="stats"></table>
  </div>
</div>
<script>
async function poll(){
 try{
  const r=await fetch('/status.json');
  if(!r.ok)return;
  const d=await r.json();
  render(d);
 }catch(e){}
 setTimeout(poll,1000);
}
function render(d){
 const limit=d.limit||4,hc=d.hard_cap||8,tgt=d.target||3;
 const obs=d.concurrent_sessions,loc=d.local_in_flight;
 const bands=[
  {n:'boxed',lo:hc+1,hi:null,c:'boxed',label:'> '+hc},
  {n:'low',lo:limit+1,hi:hc,c:'low',label:(limit+1)+'\u2013'+hc},
  {n:'normal',lo:0,hi:limit,c:'normal',label:'0\u2013'+limit},
 ];
 let html='';
 html+='<div class="target-line">target = '+tgt+'</div>';
 for(const b of bands){
  let cls=b.c;
  let mk='';
  if(obs!=null&&((b.n==='boxed'&&obs>hc)||(b.n==='low'&&obs>limit&&obs<=hc)||(b.n==='normal'&&obs<=limit))) mk+='<span class="marker obs">\u25cf</span> obs='+obs;
  if(loc!=null&&((b.n==='boxed'&&loc>hc)||(b.n==='low'&&loc>limit&&loc<=hc)||(b.n==='normal'&&loc<=limit))) mk+=' <span class="marker loc">\u25a3</span> loc='+loc;
  if(d.phantom_estimate>0&&b.n==='low') mk+=' <span class="phantom">gap='+d.phantom_estimate+'</span>';
  html+='<div class="band '+cls+'">'+b.label+' '+mk+'</div>';
 }
 document.getElementById('ladder').innerHTML=html;
 const rows=[
  ['band',d.band],['effective_permits',d.effective_permits],
  ['concurrent_sessions',obs],['local_in_flight',loc],
  ['phantom_estimate',d.phantom_estimate],
  ['breaker',d.breaker],['recent_429s',d.recent_429s],
  ['total_429s',d.total_429s],['queue_depth',d.queue_depth],
  ['gate_closed',d.gate_closed_reason],['ready',d.ready],
  ['usage_age',d.usage_age+'s'+(d.stale?' (stale)':'')],
 ];
 document.getElementById('stats').innerHTML=rows.map(r=>'<tr><th>'+r[0]+'</th><td>'+r[1]+'</td></tr>').join('');
 const bb=document.getElementById('banner-boxed');
 if(d.band==='boxed'){
  bb.style.display='block';
  const ra=d.resets_at?Math.max(0,Math.round(d.resets_at-Date.now()/1000)):'?';
  document.getElementById('countdown').textContent=ra;
 }else bb.style.display='none';
 const br=document.getElementById('banner-breaker');
 br.style.display=(d.breaker==='open')?'block':'none';
}
poll();
</script>
</body>
</html>
"""
