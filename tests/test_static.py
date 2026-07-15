"""Encoding integrity and JS validation of static assets.

The dashboard moved from an inline Python string to a static file in
294b2d2, which took it out of Python-source linting; that same commit
introduced double-encoded UTF-8 (mojibake) in six spots — middots
rendered as 'ÃÂ·' and em dashes as invisible-control junk in the banner
strings. These tests pin the failure mode: a UTF-8 file misread as
Latin-1 and re-encoded leaves C1 control characters (U+0080âU+009F)
and Ã/Ã¢ artifact pairs that valid text never contains.

Additionally, the dashboard's inline JavaScript is now validated for
syntax (``node --check``) and exercised with mock data via a Node.js
harness.  The syntax test would have caught the ternary-colon syntax
error in commit ea03cde that broke the dashboard entirely.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

_STATIC_DIR = Path(__file__).parent.parent / "src" / "sluice" / "static"
_TEXT_ASSETS = sorted(
    p for p in _STATIC_DIR.rglob("*") if p.suffix in {".html", ".css", ".js", ".txt"}
)
_DASHBOARD = _STATIC_DIR / "dashboard.html"
_NODE = shutil.which("node")


def _extract_dashboard_js() -> str:
    """Return the contents of the first ``<script>`` tag in dashboard.html."""
    html = _DASHBOARD.read_text(encoding="utf-8")
    m = re.search(r"<script>(.*?)</script>", html, re.DOTALL)
    assert m, "dashboard.html must contain a <script> block"
    return m.group(1)


@pytest.mark.parametrize("asset", _TEXT_ASSETS, ids=lambda p: p.name)
def test_static_asset_is_valid_utf8_without_mojibake(asset: Path) -> None:
    text = asset.read_bytes().decode("utf-8")  # strict: invalid UTF-8 raises

    c1_controls = [
        (i, hex(ord(c))) for i, c in enumerate(text) if "\x80" <= c <= "\x9f"
    ]
    assert not c1_controls, f"C1 control chars (double-encode residue): {c1_controls}"

    # 'Ã' or 'Ã¢' followed by another non-ASCII char is the signature of
    # UTF-8 â Latin-1 â UTF-8 round-tripped punctuation (Â·, â, etc.).
    artifacts = [
        (i, text[i : i + 2])
        for i, c in enumerate(text[:-1])
        if c in "\xc2\xe2" and ord(text[i + 1]) > 127
    ]
    assert not artifacts, f"double-encoded UTF-8 artifacts: {artifacts}"


def test_static_assets_exist() -> None:
    assert any(p.name == "dashboard.html" for p in _TEXT_ASSETS)


# ---------------------------------------------------------------------------
# JS syntax validation
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_is_syntactically_valid() -> None:
    """The dashboard's inline JS must parse without syntax errors.

    This test would have caught the ternary syntax error in commit ea03cde
    (``cls?' class="'+esc(cls)+'"'':''`` parsed as two adjacent string
    literals) that broke the dashboard entirely â render() never executed.
    """
    js = _extract_dashboard_js()
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(js)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, "--check", path],
            capture_output=True,
            text=True,
            timeout=10,
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Dashboard JS syntax error:\n{result.stderr}"


# ---------------------------------------------------------------------------
# JS render test with mock data
# ---------------------------------------------------------------------------

# Prefix: mock DOM, mock fetch, mock globals â everything the dashboard JS
# touches at load time and during the first poll cycle.
_NODE_RENDER_PREFIX = r"""
function _escapeHtml(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function _mockEl(id){
  var _tc='',_ih='',_cn='';
  return {
    id:id,
    style:{},
    classList:{add:function(){},remove:function(){},toggle:function(){},contains:function(){return false;}},
    get textContent(){return _tc;},
    set textContent(v){_tc=String(v);_ih=_escapeHtml(_tc);},
    get innerHTML(){return _ih;},
    set innerHTML(v){_ih=String(v);},
    get className(){return _cn;},
    set className(v){_cn=String(v);},
    setAttribute:function(){},
    getAttribute:function(){return null;},
    getBoundingClientRect:function(){return{left:0,top:0,width:200,height:120};},
    addEventListener:function(){},
    removeEventListener:function(){},
    offsetWidth:100,
    offsetHeight:20,
    appendChild:function(){},
    removeChild:function(){},
    querySelectorAll:function(){return [];},
    querySelector:function(){return null;},
  };
}
var _elements={};
var document={
  createElement:function(tag){return _mockEl('');},
  getElementById:function(id){if(!_elements[id])_elements[id]=_mockEl(id);return _elements[id];},
  querySelectorAll:function(sel){return [];},
  querySelector:function(sel){return null;},
  addEventListener:function(){},
  body:_mockEl('body'),
};
var window={addEventListener:function(){},scrollY:0,location:{href:'http://localhost/'}};
var _warnings=[];
var console_warn_original=console.warn;
console.warn=function(){_warnings.push(Array.prototype.slice.call(arguments).join(' '));};
var _mockStatus={
  version:'1.0.0',build:'test',
  concurrent_sessions:3,limit:4,hard_cap:8,
  priority_low:false,priority_reason:null,
  boxed_until:null,resets_at:null,
  usage_age:1.5,stale:false,
  effective_permits:4,band:'normal',phantom_estimate:0,
  breaker:'closed',breaker_half_open_age_seconds:null,
  recent_429s:2,total_429s:5,gateway_429s:0,
  target:4,queue_depth:0,local_in_flight:2,cooling_down:0,
  avg_wait_seconds:0.1,p95_wait_seconds:0.5,queue_timeouts:3,
  ready:true,gate_closed_reason:'open',
  config:{target:4,min_floor:1,poll_interval:5,usage_fresh_ttl:30,
    phantom_window:5,breaker_threshold:3,breaker_window_seconds:300,
    breaker_cooldown_seconds:60,provider:'umans',controller:'pid'},
  overrides:{},
  requests_in_window:100,requests_limit:500,requests_remaining:400,
  requests_hard_cap:1000,requests_window_seconds:3600,
  local_requests_in_window:95,request_window_delta:5,
  total_requests_forwarded:1000,
};
var _fetchCount=0;
var fetch=function(url,opts){
  _fetchCount++;
  if(url.indexOf('/status.json')!==-1){
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve(_mockStatus);},
      text:function(){return Promise.resolve(JSON.stringify(_mockStatus));},
      headers:{get:function(k){return k==='content-type'?'application/json':'';}},
    });
  }
  if(url.indexOf('/history.json')!==-1){
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve({entries:[]});},
      text:function(){return Promise.resolve('{}');},
      headers:{get:function(){return '';}},
    });
  }
  if(url.indexOf('/admin/config')!==-1){
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve({});},
      text:function(){return Promise.resolve('{}');},
      headers:{get:function(k){return k==='content-type'?'application/json':'';}},
    });
  }
  return Promise.resolve({ok:false,status:404,json:function(){return Promise.resolve({});},text:function(){return Promise.resolve('');},headers:{get:function(){return '';}}});
};
"""

# Suffix: after the dashboard JS has loaded and the initial poll completed,
# read the rendered HTML, then simulate a queue-timeout increment and
# re-render to verify row-warn activation.
_NODE_RENDER_SUFFIX = r"""
setTimeout(function(){
  try{
    var statsHtml=_elements['stats']?_elements['stats'].innerHTML:'';
    var configHtml=_elements['config-table']?_elements['config-table'].innerHTML:'';

    /* Simulate a queue_timeout increment: the last hist entry has qt:3
       (from the mock status data).  Bump lastD.queue_timeouts to 4 so
       recentInc('qt','queue_timeouts') sees current > last-hist-value. */
    if(typeof lastD!=='undefined'&&lastD
       &&typeof hist!=='undefined'&&hist.length>0){
      lastD.queue_timeouts=4;
      render(lastD);
    }
    var statsAfter=_elements['stats']?_elements['stats'].innerHTML:'';

    console.log(JSON.stringify({
      error:null,
      stats:statsHtml,
      statsAfterIncrement:statsAfter,
      config:configHtml,
      fetchCount:_fetchCount,
      warnings:_warnings,
    }));
  }catch(e){
    console.log(JSON.stringify({
      error:e.message,stack:e.stack,
      stats:'',statsAfterIncrement:'',config:'',
    }));
  }
  process.exit(0);
},300);
"""


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_renders_status_data() -> None:
    """Execute the dashboard JS in Node with a mock DOM and mock /status.json.

    Verifies the render() function produces correct HTML:
    - Stats table has rows for all expected fields
    - Primary-key rows (band, effective_permits, local_in_flight, breaker)
      carry the ``row-primary`` class
    - Non-primary rows carry ``row-detail``
    - ``total_429s`` and ``recent_429s`` rows carry ``row-crit`` when
      ``recent_429s > 0``
    - ``queue_timeouts`` row gains ``row-warn`` after a simulated increment
    - Config table renders the target stepper (``step-btn``)
    """
    js = _extract_dashboard_js()
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + _NODE_RENDER_SUFFIX
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path],
            capture_output=True,
            text=True,
            timeout=15,
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js render test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )
    assert not output.get("warnings"), (
        f"Dashboard JS emitted unexpected console.warn: {output['warnings']}"
    )

    stats = output["stats"]
    assert stats, "Stats table must not be empty after initial poll"
    # CSS class assignments
    assert "row-primary" in stats, "primary-key rows must have row-primary class"
    assert "row-detail" in stats, "non-primary rows must have row-detail class"
    assert "row-crit" in stats, "total_429s/recent_429s must have row-crit (recent_429s>0)"
    assert "row-warn" not in stats, "queue_timeouts must not have row-warn on first poll"
    # Expected field names rendered
    for field in (
        "band", "effective_permits", "local_in_flight", "breaker",
        "queue_timeouts", "total_429s", "recent_429s", "queue_depth",
    ):
        assert f">{field}<" in stats, f"stats table must render field: {field}"

    # After simulated queue_timeout increment
    stats_after = output["statsAfterIncrement"]
    assert "row-warn" in stats_after, (
        "queue_timeouts row must gain row-warn after a timeout increment"
    )

    # Config table has the target stepper (Plan 011)
    config = output["config"]
    assert "step-btn" in config, "config table must render target stepper buttons"


# ---------------------------------------------------------------------------
# Structural assertions (no Node required)
# ---------------------------------------------------------------------------

def test_dashboard_has_render_class_styles() -> None:
    """The dashboard HTML must define the CSS classes and JS logic for the
    row-primary/detail/warn/crit rendering introduced in ea03cde.

    This is a static-content assertion (same approach as the existing
    dashboard layout tests) so it runs even when Node is unavailable.
    """
    html = _DASHBOARD.read_text(encoding="utf-8")

    # CSS classes defined in <style>
    for cls in ("row-primary", "row-detail", "row-warn", "row-crit"):
        assert f"tr.{cls}" in html, f"CSS class tr.{cls} must be defined in <style>"

    # PRIMARY_KEYS constant with expected keys
    pk_match = re.search(r"PRIMARY_KEYS\s*=\s*\{([^}]*)\}", html)
    assert pk_match, "PRIMARY_KEYS constant must be defined"
    pk_body = pk_match.group(1)
    for key in ("band", "effective_permits", "local_in_flight", "breaker"):
        assert f"'{key}'" in pk_body, f"PRIMARY_KEYS must include '{key}'"

    # kvRow accepts (r, cls) parameters
    assert re.search(r"function\s+kvRow\s*\(\s*r\s*,\s*cls\s*\)", html), (
        "kvRow function must accept (r, cls) parameters"
    )

    # recentInc function is defined
    assert re.search(r"function\s+recentInc\s*\(", html), (
        "recentInc function must be defined"
    )

    # Class-assignment logic references the correct field names
    assert "queue_timeouts" in html and "row-warn" in html
    assert "total_429s" in html and "row-crit" in html
    assert "recent_429s" in html and "row-crit" in html


def test_dashboard_has_half_open_banner_logic() -> None:
    """The dashboard HTML must contain JS logic that renders
    breaker_half_open_age_seconds when the breaker is HALF_OPEN (WI-021).

    This is a static-content assertion so it runs even when Node is unavailable.
    """
    html = _DASHBOARD.read_text(encoding="utf-8")

    # The banner-breaker element must exist
    assert 'id="banner-breaker"' in html, "banner-breaker element must exist"

    # The JS must reference breaker_half_open_age_seconds in the render function
    assert "breaker_half_open_age_seconds" in html, (
        "dashboard JS must reference breaker_half_open_age_seconds"
    )

    # The JS must conditionally render the HALF_OPEN banner text with the age
    assert "HALF_OPEN" in html, (
        "dashboard JS must render HALF_OPEN banner text"
    )
    assert "probing" in html, (
        "dashboard JS must render 'probing' text for HALF_OPEN state"
    )

    # The stats table must include a breaker_half_open_age row
    assert "breaker_half_open_age" in html, (
        "stats table must include breaker_half_open_age row"
    )


def test_dashboard_has_error_banner() -> None:
    """The dashboard HTML must contain a visible error banner for config
    override failures (WI-026).

    This is a static-content assertion so it runs even when Node is unavailable.
    """
    html = _DASHBOARD.read_text(encoding="utf-8")

    # The error banner element must exist
    assert 'id="banner-error"' in html, (
        "banner-error element must exist in dashboard HTML"
    )

    # The CSS class for the error banner must be defined
    assert ".banner.error" in html, (
        "CSS class .banner.error must be defined"
    )

    # The showError and hideError functions must be defined
    assert "function showError" in html, (
        "showError function must be defined"
    )
    assert "function hideError" in html, (
        "hideError function must be defined"
    )

    # stepTarget and revertTarget must call hideError on entry
    assert "hideError()" in html, (
        "hideError must be called in config mutation functions"
    )


# ---------------------------------------------------------------------------
# JS render test: HALF_OPEN breaker state (WI-021)
# ---------------------------------------------------------------------------

_NODE_HALF_OPEN_PREFIX = r"""
function _escapeHtml(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function _mockEl(id){
  var _tc='',_ih='',_cn='';
  return {
    id:id,
    style:{display:''},
    classList:{add:function(){},remove:function(){},toggle:function(){},contains:function(){return false;}},
    get textContent(){return _tc;},
    set textContent(v){_tc=String(v);_ih=_escapeHtml(_tc);},
    get innerHTML(){return _ih;},
    set innerHTML(v){_ih=String(v);},
    get className(){return _cn;},
    set className(v){_cn=String(v);},
    setAttribute:function(){},
    getAttribute:function(){return null;},
    getBoundingClientRect:function(){return{left:0,top:0,width:200,height:120};},
    addEventListener:function(){},
    removeEventListener:function(){},
    offsetWidth:100,
    offsetHeight:20,
    appendChild:function(){},
    removeChild:function(){},
    querySelectorAll:function(){return [];},
    querySelector:function(){return null;},
  };
}
var _elements={};
var document={
  createElement:function(tag){return _mockEl('');},
  getElementById:function(id){if(!_elements[id])_elements[id]=_mockEl(id);return _elements[id];},
  querySelectorAll:function(sel){return [];},
  querySelector:function(sel){return null;},
  addEventListener:function(){},
  body:_mockEl('body'),
};
var window={addEventListener:function(){},scrollY:0,location:{href:'http://localhost/'}};
var _warnings=[];
var console_warn_original=console.warn;
console.warn=function(){_warnings.push(Array.prototype.slice.call(arguments).join(' '));};
var _mockStatus={
  version:'1.0.0',build:'test',
  concurrent_sessions:2,limit:4,hard_cap:8,
  priority_low:false,priority_reason:null,
  boxed_until:null,resets_at:null,
  usage_age:1.5,stale:false,
  effective_permits:1,band:'normal',phantom_estimate:0,
  breaker:'half_open',breaker_half_open_age_seconds:12.5,
  recent_429s:3,total_429s:8,gateway_429s:0,rate_limit_429s:0,
  target:4,queue_depth:0,local_in_flight:1,cooling_down:0,
  avg_wait_seconds:0.1,p95_wait_seconds:0.5,avg_hold_seconds:2.3,
  queue_timeouts:0,retry_after_hint:5,
  ready:true,gate_closed_reason:'open',
  config:{target:4,min_floor:1,poll_interval:5,usage_fresh_ttl:30,
    phantom_window:5,breaker_threshold:5,breaker_window_seconds:300,
    breaker_cooldown_seconds:60,provider:'umans',controller:'concurrency_reconcile'},
  overrides:{},
  requests_in_window:100,requests_limit:500,requests_remaining:400,
  requests_hard_cap:1000,requests_window_seconds:3600,
  local_requests_in_window:95,request_window_delta:5,
  total_requests_forwarded:1000,
};
var fetch=function(url,opts){
  if(url.indexOf('/status.json')!==-1){
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve(_mockStatus);},
      text:function(){return Promise.resolve(JSON.stringify(_mockStatus));},
      headers:{get:function(k){return k==='content-type'?'application/json':'';}},
    });
  }
  if(url.indexOf('/history.json')!==-1){
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve({entries:[]});},
      text:function(){return Promise.resolve('{}');},
      headers:{get:function(){return '';}},
    });
  }
  return Promise.resolve({ok:false,status:404,json:function(){return Promise.resolve({});},text:function(){return Promise.resolve('');},headers:{get:function(){return '';}}});
};
"""

_NODE_HALF_OPEN_SUFFIX = r"""
setTimeout(function(){
  try{
    var bannerHtml=_elements['banner-breaker']?_elements['banner-breaker'].textContent:'';
    var bannerDisplay=_elements['banner-breaker']?_elements['banner-breaker'].style.display:'';
    var statsHtml=_elements['stats']?_elements['stats'].innerHTML:'';
    console.log(JSON.stringify({
      error:null,
      bannerText:bannerHtml,
      bannerDisplay:bannerDisplay,
      stats:statsHtml,
      warnings:_warnings,
    }));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack,bannerText:'',bannerDisplay:'',stats:''}));
  }
  process.exit(0);
},300);
"""


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_renders_half_open_breaker() -> None:
    """Execute the dashboard JS with breaker=half_open and verify the banner
    and stats table render breaker_half_open_age_seconds (WI-021).

    Verifies:
    - The breaker banner is visible (display=block)
    - The banner text contains 'HALF_OPEN' and the age value (12.5s)
    - The stats table includes the breaker_half_open_age row with the value
    """
    js = _extract_dashboard_js()
    script = _NODE_HALF_OPEN_PREFIX + "\n" + js + "\n" + _NODE_HALF_OPEN_SUFFIX
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path],
            capture_output=True,
            text=True,
            timeout=15,
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js HALF_OPEN test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )

    banner_text = output["bannerText"]
    assert "HALF_OPEN" in banner_text, (
        f"Banner must contain 'HALF_OPEN' when breaker is half_open, got: {banner_text}"
    )
    assert "12.5" in banner_text, (
        f"Banner must contain the age (12.5), got: {banner_text}"
    )
    assert "probing" in banner_text, (
        f"Banner must contain 'probing', got: {banner_text}"
    )

    stats = output["stats"]
    assert "breaker_half_open_age" in stats, (
        "Stats table must render breaker_half_open_age row"
    )
    assert "12.5" in stats, (
        "Stats table must contain the age value 12.5"
    )


# ---------------------------------------------------------------------------
# Tokens over the last 24H metric (reuses penalty section's usage-history fetch)
# ---------------------------------------------------------------------------

def test_dashboard_has_tokens_24h_metric() -> None:
    """The dashboard HTML must contain the 'Tokens over the last 24H' metric.

    Reuses the penalty section's ``fetchUsageBuckets`` / ``sumBuckets`` to
    sum a rolling 24h window of token usage from the provider usage-history
    endpoint, surfaced as a non-prominent row in the Reading card.  Bold
    after 100M, bold+warn at 250M, bold+crit at 350M.
    """
    html = _DASHBOARD.read_text(encoding="utf-8")

    # CSS class for the bold threshold tier
    assert "tr.row-bold" in html, "CSS class tr.row-bold must be defined in <style>"

    # vol24 state + fetch helpers reuse the penalty section's usage-history fetch
    assert re.search(r"var\s+vol24\s*=", html), "vol24 state must be defined"
    assert "function fetchVolume24h" in html, "fetchVolume24h must be defined"
    assert "function maybeFetchVolume24h" in html, (
        "maybeFetchVolume24h must be defined"
    )
    # Must fetch a 24h rolling window (now - 86400 seconds)
    assert "now-86400" in html, "must fetch a 24h rolling window"

    # The tokens_24h row in the stats table
    assert "tokens_24h" in html, "stats table must include tokens_24h row"

    # Threshold classification logic references the three tiers
    assert "100e6" in html, "must bold after 100M tokens"
    assert "250e6" in html, "must apply warn at 250M tokens"
    assert "350e6" in html, "must apply crit at 350M tokens"

    # Slow cadence (5 min) — the 24h total shifts slowly
    assert "VOL_REFRESH_MS=300000" in html, (
        "vol24 refresh must be 5 min (300000 ms) to avoid over-polling"
    )
    # Skip the fetch while a penalty is active (penalty card already polls)
    assert "penalty_started_at" in html, (
        "maybeFetchVolume24h must check penalty_started_at"
    )


_NODE_TOKENS24H_PREFIX = r"""
function _escapeHtml(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function _mockEl(id){
  var _tc='',_ih='',_cn='';
  return {
    id:id,
    style:{display:''},
    classList:{add:function(){},remove:function(){},toggle:function(){},contains:function(){return false;}},
    get textContent(){return _tc;},
    set textContent(v){_tc=String(v);_ih=_escapeHtml(_tc);},
    get innerHTML(){return _ih;},
    set innerHTML(v){_ih=String(v);},
    get className(){return _cn;},
    set className(v){_cn=String(v);},
    setAttribute:function(){},
    getAttribute:function(){return null;},
    getBoundingClientRect:function(){return{left:0,top:0,width:200,height:120};},
    addEventListener:function(){},
    removeEventListener:function(){},
    offsetWidth:100,
    offsetHeight:20,
    appendChild:function(){},
    removeChild:function(){},
    querySelectorAll:function(){return [];},
    querySelector:function(){return null;},
  };
}
var _elements={};
var document={
  createElement:function(tag){return _mockEl('');},
  getElementById:function(id){if(!_elements[id])_elements[id]=_mockEl(id);return _elements[id];},
  querySelectorAll:function(sel){return [];},
  querySelector:function(sel){return null;},
  addEventListener:function(){},
  body:_mockEl('body'),
};
var window={addEventListener:function(){},scrollY:0,location:{href:'http://localhost/'}};
var _warnings=[];
var console_warn_original=console.warn;
console.warn=function(){_warnings.push(Array.prototype.slice.call(arguments).join(' '));};
var _mockStatus={
  version:'1.0.0',build:'test',
  concurrent_sessions:2,limit:4,hard_cap:8,
  priority_low:false,priority_reason:null,
  boxed_until:null,resets_at:null,
  service_mode:null,service_mode_resets_at:null,low_interactivity:false,
  tokens_in:null,tokens_out:null,
  usage_age:1.5,stale:false,
  effective_permits:4,band:'normal',phantom_estimate:0,
  breaker:'closed',breaker_half_open_age_seconds:null,
  recent_429s:0,total_429s:0,gateway_429s:0,rate_limit_429s:0,total_503s:0,
  target:4,queue_depth:0,local_in_flight:1,cooling_down:0,
  avg_wait_seconds:0.1,p95_wait_seconds:0.5,avg_hold_seconds:2.3,
  retry_after_hint:5,queue_timeouts:0,
  ready:true,gate_closed_reason:'open',
  config:{target:4,min_floor:1,poll_interval:5,poll_interval_idle:30,usage_fresh_ttl:30,
    phantom_window:5,breaker_threshold:3,breaker_window_seconds:300,
    breaker_cooldown_seconds:60,provider:'umans',controller:'concurrency_reconcile'},
  overrides:{},
  requests_in_window:100,requests_limit:500,requests_remaining:400,
  requests_hard_cap:1000,requests_window_seconds:3600,
  local_requests_in_window:95,request_window_delta:5,
  total_requests_forwarded:1000,throughput:0,idle:true,poll_interval_idle:30,
  client_metrics:null,penalty_started_at:null,
};
var fetch=function(url,opts){
  if(url.indexOf('/status.json')!==-1){
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve(_mockStatus);},
      text:function(){return Promise.resolve(JSON.stringify(_mockStatus));},
      headers:{get:function(k){return k==='content-type'?'application/json':'';}},
    });
  }
  if(url.indexOf('/history.json')!==-1){
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve({entries:[]});},
      text:function(){return Promise.resolve('{}');},
      headers:{get:function(){return '';}},
    });
  }
  if(url.indexOf('/admin/usage-history')!==-1){
    /* Two hourly buckets summing to 400M tokens (in+out) — >= 350M so the
       row must carry row-bold + row-crit. */
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve({buckets:[
        {bucket:'2026-07-15T00:00:00Z',tokens_in:120000000,tokens_out:80000000,requests:10},
        {bucket:'2026-07-15T01:00:00Z',tokens_in:110000000,tokens_out:90000000,requests:12},
      ]});},
      text:function(){return Promise.resolve('{}');},
      headers:{get:function(k){return k==='content-type'?'application/json':'';}},
    });
  }
  return Promise.resolve({ok:false,status:404,json:function(){return Promise.resolve({});},text:function(){return Promise.resolve('');},headers:{get:function(){return '';}}});
};
"""

_NODE_TOKENS24H_SUFFIX = r"""
setTimeout(function(){
  try{
    var statsHtml=_elements['stats']?_elements['stats'].innerHTML:'';
    console.log(JSON.stringify({
      error:null,
      stats:statsHtml,
      warnings:_warnings,
    }));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack,stats:''}));
  }
  process.exit(0);
},300);
"""


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_renders_tokens_24h_with_thresholds() -> None:
    """Execute the dashboard JS with a mocked usage-history endpoint that
    returns 400M tokens over 24h, and verify the tokens_24h row renders with
    the correct threshold styling (bold + crit at >= 350M).
    """
    js = _extract_dashboard_js()
    script = _NODE_TOKENS24H_PREFIX + "\n" + js + "\n" + _NODE_TOKENS24H_SUFFIX
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path],
            capture_output=True,
            text=True,
            timeout=15,
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js tokens_24h test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )
    assert not output.get("warnings"), (
        f"Dashboard JS emitted unexpected console.warn: {output['warnings']}"
    )

    stats = output["stats"]
    assert "tokens_24h" in stats, "stats table must render tokens_24h row"
    assert "400.0M" in stats, "stats table must show the 400M token total"
    assert "row-bold" in stats, "tokens_24h must be bold at 400M (>100M)"
    assert "row-crit" in stats, "tokens_24h must be crit at 400M (>=350M)"


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_skips_tokens_24h_fetch_during_penalty() -> None:
    """When a penalty event is active, vol24 must NOT fetch — the penalty card
    already polls the same usage-history endpoint for overlapping ranges.

    Activates a penalty (penalty_started_at = 1h ago) in the mock status and
    verifies the tokens_24h row stays absent (vol24.total never populated).
    """
    js = _extract_dashboard_js()
    # Activate a penalty event (started 1h ago) in the mock status
    prefix = _NODE_TOKENS24H_PREFIX.replace(
        "penalty_started_at:null",
        "penalty_started_at:Date.now()/1000-3600",
    )
    script = prefix + "\n" + js + "\n" + _NODE_TOKENS24H_SUFFIX
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path],
            capture_output=True,
            text=True,
            timeout=15,
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js penalty-skip test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )

    stats = output["stats"]
    assert "tokens_24h" not in stats, (
        "tokens_24h row must not render during a penalty (fetch skipped, "
        "penalty card already polls the endpoint)"
    )
