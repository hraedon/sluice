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
  var _tc='',_ih='',_cn='',_attrs={};
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
    setAttribute:function(name,value){_attrs[name]=String(value);},
    removeAttribute:function(name){delete _attrs[name];},
    getAttribute:function(name){return _attrs[name]||null;},
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


@pytest.mark.parametrize("viewport", [280, 320, 360, 400, 1280])
def test_dashboard_responsive_controls_preserve_touch_targets(viewport: int) -> None:
    """Narrow layouts fit override controls without shrinking tap targets."""
    html = _DASHBOARD.read_text(encoding="utf-8")

    assert viewport >= 280
    assert "@media (max-width:400px)" in html
    assert ".header{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap" in html
    assert ".controls{display:flex;gap:var(--space-2);align-items:center;flex-wrap:wrap}" in html
    assert "min-height:44px" in html
    assert "min-width:min(260px,100%)" in html
    assert "#config-table{table-layout:fixed}" in html
    assert "*,*::before,*::after{box-sizing:border-box}" in html
    assert ".target-control{display:grid;grid-template-columns:44px minmax(1.5em,1fr) 44px" in html
    assert ".target-control .ov-badge{grid-column:1 / 3;min-width:0;margin:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" in html
    assert ".target-control .ov-revert{grid-column:3;grid-row:2" in html


def test_dashboard_chart_and_event_accessibility_semantics() -> None:
    """Charts and event history retain names and non-pointer access to values."""
    html = _DASHBOARD.read_text(encoding="utf-8")

    for svg_id in ("spark", "qspark", "rspark"):
        assert f'id="{svg_id}" role="img"' in html
    assert 'id="spark-summary" class="sr-only"' in html
    assert 'id="spark-summary" class="sr-only" aria-live=' not in html
    assert 'id="rspark-summary" class="sr-only"' in html
    assert 'aria-describedby="rspark-summary"' in html
    assert 'id="rspark-summary" class="sr-only" aria-live=' not in html
    assert 'aria-pressed="true" aria-current="true"' in html
    assert "button.setAttribute('aria-pressed'" in html
    assert "button.setAttribute('aria-current','true')" in html
    assert '<caption>Recent state transitions from the last four hours</caption>' in html
    assert '<th scope="col">Time</th><th scope="col">Event</th>' in html
    assert '<button type="button" class="ov-revert"' in html
    assert "<a class=\"ov-revert\"" not in html
    assert 'aria-label="Decrease target"' in html
    assert 'aria-label="Increase target"' in html


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_exposes_override_and_reconciliation_summaries() -> None:
    """Override controls retain button semantics and rspark has a text summary."""
    js = _extract_dashboard_js()
    suffix = r'''
setTimeout(function(){
  try{
    _mockStatus.overrides={target:{boot:'long-boot-identifier-1234567890',override:4}};
    render(_mockStatus);
    hist=[
      {ts:1,obs:1,loc:1,ph:0,ep:4,lim:4,hc:8,band:'normal',brk:'closed',stl:false,
       age:0,qd:0,qt:0,t429:0,t503:0,li:false,rwin:100,rlim:500,rlw:95,rdelta:5,tp:0,cp:0},
      {ts:2,obs:2,loc:1,ph:0,ep:4,lim:4,hc:8,band:'normal',brk:'closed',stl:false,
       age:0,qd:0,qt:0,t429:0,t503:0,li:false,rwin:110,rlim:500,rlw:100,rdelta:10,tp:0,cp:0}
    ];
    lastD=_mockStatus;
    renderSparks();
    var config=_elements['config-table'].innerHTML;
    console.log(JSON.stringify({
      error:null,config:config,summary:_elements['rspark-summary'].textContent,
    }));
  }catch(e){console.log(JSON.stringify({error:e.message,stack:e.stack}));}
  process.exit(0);
},300);
'''
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + suffix
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path], capture_output=True, text=True, timeout=15
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js accessibility test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, output.get("stack", "")
    assert 'type="button"' in output["config"]
    assert 'aria-label="Decrease target"' in output["config"]
    assert 'aria-label="Increase target"' in output["config"]
    assert 'aria-label="Revert target override"' in output["config"]
    assert "Override active; boot value long-boot-identifier-1234567890" in output["config"]
    assert output["summary"] == (
        "Request budget chart: provider count 110; sluice count 100; "
        "difference 10; provider limit 500."
    )


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_surfaces_config_failures_and_clears_on_success() -> None:
    """Config mutation failures are visible, while a later success clears them.

    Exercise both mutation paths with mocked responses.  This guards the
    operator-facing behavior rather than only checking that the helper names
    exist in the HTML.
    """
    js = _extract_dashboard_js()
    prefix = _NODE_RENDER_PREFIX + r'''
var _realFetch=fetch;
var _postCalls=0;
var _deleteCalls=0;
fetch=function(url,opts){
  if(url.indexOf('/admin/config/target')!==-1&&opts&&opts.method==='DELETE'){
    _deleteCalls++;
    return Promise.resolve({
      ok:false,status:500,
      json:function(){return Promise.resolve({error:'target revert failed upstream'});},
      text:function(){return Promise.resolve('target revert failed upstream');},
      headers:{get:function(){return 'application/json';}},
    });
  }
  if(url.indexOf('/admin/config')!==-1&&opts&&opts.method==='POST'){
    _postCalls++;
    if(_postCalls===1){
      return Promise.resolve({
        ok:false,status:400,
        json:function(){return Promise.resolve({error:'target exceeds hard_cap'});},
        text:function(){return Promise.resolve('target exceeds hard_cap');},
        headers:{get:function(){return 'application/json';}},
      });
    }
    return Promise.resolve({
      ok:true,status:200,
      json:function(){return Promise.resolve({target:5,overridden:true});},
      text:function(){return Promise.resolve('{}');},
      headers:{get:function(){return 'application/json';}},
    });
  }
  return _realFetch(url,opts);
};
'''
    suffix = r'''
setTimeout(function(){
  (async function(){
    try{
      await stepTarget(1);
      var failedText=_elements['banner-error'].textContent;
      var failedDisplay=_elements['banner-error'].style.display;
      await stepTarget(1);
      var successDisplay=_elements['banner-error'].style.display;
      await revertTarget();
      var revertText=_elements['banner-error'].textContent;
      var revertDisplay=_elements['banner-error'].style.display;
      console.log(JSON.stringify({
        error:null,
        failedText:failedText,
        failedDisplay:failedDisplay,
        successDisplay:successDisplay,
        revertText:revertText,
        revertDisplay:revertDisplay,
        postCalls:_postCalls,
        deleteCalls:_deleteCalls,
      }));
    }catch(e){
      console.log(JSON.stringify({error:e.message,stack:e.stack}));
    }
    process.exit(0);
  })();
},300);
'''
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(prefix + "\n" + js + "\n" + suffix)
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
    assert result.returncode == 0, f"Node.js config mutation test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )
    assert output["failedDisplay"] == "block"
    assert "target exceeds hard_cap" in output["failedText"]
    assert output["successDisplay"] == "none"
    assert output["revertDisplay"] == "block"
    assert "target revert failed upstream" in output["revertText"]
    assert output["postCalls"] == 2
    assert output["deleteCalls"] == 1


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_disables_stepper_at_target_boundaries() -> None:
    """The target stepper cannot move below one or above hard_cap."""
    js = _extract_dashboard_js()
    suffix = r'''
setTimeout(function(){
  try{
    _mockStatus.config.target=1;
    _mockStatus.target=1;
    render(_mockStatus);
    var atMinimum=_elements['config-table'].innerHTML;
    _mockStatus.config.target=8;
    _mockStatus.target=8;
    render(_mockStatus);
    var atHardCap=_elements['config-table'].innerHTML;
    console.log(JSON.stringify({error:null,atMinimum:atMinimum,atHardCap:atHardCap}));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack,atMinimum:'',atHardCap:''}));
  }
  process.exit(0);
},300);
'''
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + suffix
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
    assert result.returncode == 0, f"Node.js stepper test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )

    minimum = output["atMinimum"]
    assert re.search(r'onclick="stepTarget\(-1\)" disabled', minimum)
    assert re.search(r'onclick="stepTarget\(1\)">', minimum)

    hard_cap = output["atHardCap"]
    assert re.search(r'onclick="stepTarget\(-1\)">', hard_cap)
    assert re.search(r'onclick="stepTarget\(1\)" disabled', hard_cap)


# ---------------------------------------------------------------------------
# JS render test: completion bars (WI-023)
# ---------------------------------------------------------------------------


def test_dashboard_has_completion_signal_and_background_bars() -> None:
    """The dashboard maps the compact completion field and draws it as a
    background layer, while retaining zero for older history payloads."""
    html = _DASHBOARD.read_text(encoding="utf-8")

    assert "cp:e.cp||0" in html
    assert "cp:d.completions||0" in html
    assert "if((s.cp||0)>b.cp)" in html, "completion buckets must use max aggregation"
    assert "s.obs!=null&&!s.stl" in html
    assert ".spark-tp" in html
    assert ".spark-cp" in html
    assert "throughput max" in html
    assert "faint bars behind the spark" in html


_NODE_COMPLETIONS_SUFFIX = r"""
function _completionSample(ts,cp,stale,obs,tp){
  return {ts:ts,obs:obs===undefined?1:obs,loc:0,ph:0,ep:3,lim:4,hc:8,band:'normal',brk:'closed',
    stl:!!stale,age:0,qd:0,qt:0,t429:0,t503:0,li:false,cp:cp,tp:tp||0};
}
setTimeout(function(){
  try{
    var legacy=fromHistEntry({tp:9});
    var buckets=bucketize(withIncs([
      _completionSample(1,1),_completionSample(2,6),_completionSample(3,2)
    ]),2);
    var mixedBuckets=bucketize(withIncs([
      _completionSample(4,1,false,1),
      _completionSample(5,7,true,null),
      _completionSample(6,3,false,2)
    ]),2);

    lastD=_mockStatus;
    viewRange='5m';
    hist=[_completionSample(10,0),_completionSample(15,5)];
    longHist=[];
    renderSparks();
    var withTraffic=_elements['spark'].innerHTML;
    var barAt=withTraffic.indexOf('class="spark-cp"');
    var lineAt=withTraffic.indexOf('class="spark-obs"');

    hist=[_completionSample(20,0),_completionSample(25,0)];
    renderSparks();
    var zeroTraffic=_elements['spark'].innerHTML;

    hist=[_completionSample(30,4,true,null),_completionSample(35,2,true,null)];
    renderSparks();
    var staleTraffic=_elements['spark'].innerHTML;

    hist=[_completionSample(40,1,false,1),_completionSample(45,7,true,null),
          _completionSample(50,3,false,2),_completionSample(55,0,false,3)];
    renderSparks();
    var mixedTraffic=_elements['spark'].innerHTML;

    hist=[_completionSample(60,0,false,1,5),_completionSample(65,0,false,2,3)];
    renderSparks();
    var tpOnlyTraffic=_elements['spark'].innerHTML;

    hist=[_completionSample(70,4,false,1,0),_completionSample(75,2,false,2,0)];
    renderSparks();
    var cpOnlyTraffic=_elements['spark'].innerHTML;

    hist=[_completionSample(80,0,false,1,0),_completionSample(85,6,true,null,5),
          _completionSample(90,0,false,2,0)];
    renderSparks();
    var combinedTraffic=_elements['spark'].innerHTML;
    var staleX=(combinedTraffic.match(/<line x1="([^"]+)"[^>]+class="tick-stale"/)||[])[1];
    var tpBar=(combinedTraffic.match(/<rect class="spark-tp" x="([^"]+)"[^>]+width="([^"]+)"/)||[]);
    var cpBar=(combinedTraffic.match(/<rect class="spark-cp" x="([^"]+)"[^>]+width="([^"]+)"/)||[]);

    var longRaw=[];
    for(var li=0;li<121;li++)
      longRaw.push(_completionSample(100+li,li===60?9:0,li===60,li===60?null:1));
    viewRange='1h';
    longHist=longRaw;
    hist=[];
    renderSparks();
    var longTraffic=_elements['spark'].innerHTML;
    console.log(JSON.stringify({
      error:null,
      legacyCp:legacy.cp,
      legacyTp:legacy.tp,
      bucketCompletions:[buckets[0].cp,buckets[1].cp],
      barCount:(withTraffic.match(/class="spark-cp"/g)||[]).length,
      barBeforeLine:barAt>=0&&lineAt>=0&&barAt<lineAt,
      zeroHasBars:zeroTraffic.indexOf('spark-cp')!==-1,
      staleHasBars:staleTraffic.indexOf('spark-cp')!==-1,
      staleHasMainLine:staleTraffic.indexOf('spark-obs')!==-1,
      mixedLineCount:(mixedTraffic.match(/class="spark-obs"/g)||[]).length,
      mixedStaleMarker:mixedTraffic.indexOf('class="tick-stale"')!==-1,
      mixedBucketFresh:mixedBuckets[0].fresh,
      mixedBucketStale:mixedBuckets[0].stl,
      mixedBucketObs:mixedBuckets[0].obs,
      mixedBucketCp:mixedBuckets[0].cp,
      longHasBars:longTraffic.indexOf('spark-cp')!==-1,
      longHasStaleMarker:longTraffic.indexOf('class="tick-stale"')!==-1,
      tpOnlyHasTpBars:tpOnlyTraffic.indexOf('class="spark-tp"')!==-1,
      tpOnlyHasCpBars:tpOnlyTraffic.indexOf('class="spark-cp"')!==-1,
      cpOnlyHasTpBars:cpOnlyTraffic.indexOf('class="spark-tp"')!==-1,
      cpOnlyHasCpBars:cpOnlyTraffic.indexOf('class="spark-cp"')!==-1,
      combinedHasTpBars:combinedTraffic.indexOf('class="spark-tp"')!==-1,
      combinedHasCpBars:combinedTraffic.indexOf('class="spark-cp"')!==-1,
      combinedHasStaleMarker:staleX!==undefined,
      combinedTpAligned:tpBar.length===3&&(parseFloat(tpBar[1])+parseFloat(tpBar[2])/2).toFixed(1)===staleX,
      combinedCpAligned:cpBar.length===3&&(parseFloat(cpBar[1])+parseFloat(cpBar[2])/2).toFixed(1)===staleX,
    }));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack}));
  }
  process.exit(0);
},300);
"""


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_uses_max_completion_buckets_and_hides_idle_bars() -> None:
    """Completion bars preserve the bucket maximum and disappear at zero."""
    js = _extract_dashboard_js()
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + _NODE_COMPLETIONS_SUFFIX
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
    assert result.returncode == 0, f"Node.js completion-bar test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )
    assert output["legacyCp"] == 0
    assert output["legacyTp"] == 9
    assert output["bucketCompletions"] == [6, 2]
    assert output["barCount"] == 1
    assert output["barBeforeLine"] is True
    assert output["zeroHasBars"] is False
    assert output["staleHasBars"] is True
    assert output["staleHasMainLine"] is False
    assert output["mixedLineCount"] == 2
    assert output["mixedStaleMarker"] is True
    assert output["mixedBucketFresh"] is False
    assert output["mixedBucketStale"] is True
    assert output["mixedBucketObs"] == 1
    assert output["mixedBucketCp"] == 7
    assert output["longHasBars"] is True
    assert output["longHasStaleMarker"] is True
    assert output["tpOnlyHasTpBars"] is True
    assert output["tpOnlyHasCpBars"] is False
    assert output["cpOnlyHasTpBars"] is False
    assert output["cpOnlyHasCpBars"] is True
    assert output["combinedHasTpBars"] is True
    assert output["combinedHasCpBars"] is True
    assert output["combinedHasStaleMarker"] is True
    assert output["combinedTpAligned"] is True
    assert output["combinedCpAligned"] is True


# ---------------------------------------------------------------------------
# JS hover test: stale-gap crosshair selection
# ---------------------------------------------------------------------------


_NODE_HOVER_SUFFIX = r"""
function _hoverSample(ts,obs,stale){
  return {ts:ts,obs:obs,loc:0,ph:0,ep:3,lim:4,hc:8,band:'normal',brk:'closed',
    stl:!!stale,age:0,qd:0,qt:0,t429:0,t503:0,li:false,cp:0,tp:0};
}
setTimeout(function(){
  try{
    lastD=_mockStatus;
    viewRange='5m';
    hist=[_hoverSample(1,10,false),_hoverSample(2,null,true),
          _hoverSample(3,null,true),_hoverSample(4,40,false)];
    longHist=[];
    renderSparks();

    var attrs={};
    var crosshair=document.getElementById('crosshair-main');
    crosshair.setAttribute=function(name,value){attrs[name]=String(value);};
    var hoverIndex=2;
    var hoverX=3+(hoverIndex/59)*(200-6);
    onSparkHover({currentTarget:_elements['spark'],clientX:hoverX,clientY:10});
    var selectedX=(3+(3/59)*(200-6)).toFixed(1);
    console.log(JSON.stringify({
      error:null,
      crosshair:attrs.x1,
      expected:selectedX,
      selectedObs:_elements['tip-obs'].textContent,
    }));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack}));
  }
  process.exit(0);
},300);
"""


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_aligns_hover_crosshair_to_fresh_sample_after_stale_gap() -> None:
    """The crosshair follows the selected fresh point, not stale-gap pixels."""
    js = _extract_dashboard_js()
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + _NODE_HOVER_SUFFIX
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
    assert result.returncode == 0, f"Node.js hover test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )
    assert output["crosshair"] == output["expected"]
    assert output["selectedObs"] == "40"


# ---------------------------------------------------------------------------
# JS live polling test: one sample per reconciliation tick
# ---------------------------------------------------------------------------


_NODE_LIVE_SAMPLE_SUFFIX = r"""
function _liveStatus(boot,sequence,tp,cp){
  var d=Object.assign({},_mockStatus);
  if(boot!==null)d.sample_id=boot+':'+sequence;
  d.sample_sequence=sequence;
  d.throughput=tp;
  d.completions=cp;
  return d;
}
function _resetLiveState(){
  hist=[];
  lastLiveSampleId=null;
  lastLiveSampleSequence=null;
  lastLiveBootId=null;
}
function _statusResponse(payload){
  return {ok:true,status:200,json:function(){return Promise.resolve(payload);},
    text:function(){return Promise.resolve(JSON.stringify(payload));},
    headers:{get:function(){return 'application/json';}}};
}
var _realLiveFetch=fetch;
var _liveQueue=[];
var _historyPayload=null;
fetch=function(url,opts){
  if(url.indexOf('/status.json')!==-1&&_liveQueue.length)
    return Promise.resolve(_statusResponse(_liveQueue.shift()));
  if(url.indexOf('/history.json')!==-1&&_historyPayload!==null)
    return Promise.resolve(_statusResponse({entries:_historyPayload}));
  return _realLiveFetch(url,opts);
};
setTimeout(function(){
  (async function(){
    try{
      _resetLiveState();
      _liveQueue=[
        _liveStatus('boot-a',7,2,1),
        _liveStatus('boot-a',7,2,1),
        _liveStatus('boot-a',8,0,3),
        _liveStatus('boot-b',8,5,4),
        _liveStatus('boot-b',8,5,4),
      ];
      while(_liveQueue.length) await doPoll();
      var restartSamples=hist.map(function(h){return {tp:h.tp,cp:h.cp,sid:h.sid};});

      _resetLiveState();
      var legacyOne=_liveStatus(null,1,2,2);
      var legacyTwo=_liveStatus(null,2,3,3);
      var oldOne=_liveStatus(null,undefined,9,0);
      var oldTwo=_liveStatus(null,undefined,9,0);
      delete legacyOne.sample_id;
      delete legacyTwo.sample_id;
      delete oldOne.sample_id;
      delete oldOne.sample_sequence;
      delete oldTwo.sample_id;
      delete oldTwo.sample_sequence;
      _liveQueue=[
        _liveStatus('boot-c',1,1,1),
        legacyOne,
        legacyTwo,
        _liveStatus('boot-c',2,4,4),
        _liveStatus('boot-d',2,5,5),
        oldOne,
        oldTwo,
      ];
      while(_liveQueue.length) await doPoll();
      var alternationSamples=hist.map(function(h){return {tp:h.tp,cp:h.cp,sid:h.sid};});

      _resetLiveState();
      _historyPayload=[{ts:1,obs:1,loc:0,ph:0,ep:3,lim:4,hc:8,band:'normal',brk:'closed',
        stl:false,age:0,qd:0,qt:0,t429:0,t503:0,li:false,tp:4,cp:6,sid:'boot-h:4'}];
      await initHistory();
      var seededCount=hist.length;
      _liveQueue=[_liveStatus('boot-h',4,4,6),_liveStatus('boot-h',5,0,2)];
      while(_liveQueue.length) await doPoll();
      var historySamples=hist.map(function(h){return {tp:h.tp,cp:h.cp,sid:h.sid};});
      console.log(JSON.stringify({
        error:null,
        restartSamples:restartSamples,
        alternationSamples:alternationSamples,
        seededCount:seededCount,
        historySamples:historySamples,
      }));
    }catch(e){
      console.log(JSON.stringify({error:e.message,stack:e.stack}));
    }
    process.exit(0);
  })();
},300);
"""


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_deduplicates_restart_safe_live_samples_and_seeds_history() -> None:
    """Live polling handles repeated ticks, restarts, legacy payloads, and warm history."""
    js = _extract_dashboard_js()
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + _NODE_LIVE_SAMPLE_SUFFIX
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
    assert result.returncode == 0, f"Node.js live-sample test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )
    assert output["restartSamples"] == [
        {"tp": 2, "cp": 1, "sid": "boot-a:7"},
        {"tp": 0, "cp": 3, "sid": "boot-a:8"},
        {"tp": 5, "cp": 4, "sid": "boot-b:8"},
    ]
    assert output["alternationSamples"] == [
        {"tp": 1, "cp": 1, "sid": "boot-c:1"},
        {"tp": 3, "cp": 3, "sid": None},
        {"tp": 5, "cp": 5, "sid": "boot-d:2"},
        {"tp": 9, "cp": 0, "sid": None},
        {"tp": 9, "cp": 0, "sid": None},
    ]
    assert output["seededCount"] == 1
    assert output["historySamples"] == [
        {"tp": 4, "cp": 6, "sid": "boot-h:4"},
        {"tp": 0, "cp": 2, "sid": "boot-h:5"},
    ]


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


# ---------------------------------------------------------------------------
# JS config-error handling regressions (WI-026)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_shows_plain_text_and_alternate_json_errors() -> None:
    """Failed mutations surface text bodies and non-error JSON keys."""
    js = _extract_dashboard_js()
    suffix = r'''
var _realDashboardFetch=fetch;
var _mutationCalls=0;
fetch=function(url,opts){
  if(url.indexOf('/admin/config')!==-1&&opts&&opts.method==='POST'){
    _mutationCalls++;
    if(_mutationCalls===1){
      return Promise.resolve({
        ok:false,status:502,
        text:function(){return Promise.resolve('upstream plain-text failure');},
        json:function(){return Promise.reject(new Error('not json'));},
      });
    }
    return Promise.resolve({
      ok:false,status:400,
      text:function(){return Promise.resolve(JSON.stringify({detail:'alternate detail'}));},
      json:function(){return Promise.resolve({detail:'alternate detail'});},
    });
  }
  return _realDashboardFetch(url,opts);
};
setTimeout(function(){
  (async function(){
    try{
      await stepTarget(1);
      var plain=_elements['banner-error'].textContent;
      await stepTarget(1);
      var alternate=_elements['banner-error'].textContent;
      console.log(JSON.stringify({
        error:null,plain:plain,alternate:alternate,
        saving:configSaving,calls:_mutationCalls,
      }));
    }catch(e){
      console.log(JSON.stringify({error:e.message,stack:e.stack}));
    }
    process.exit(0);
  })();
},300);
'''
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + suffix
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
    assert result.returncode == 0, f"Node.js config-error test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )
    assert output["plain"] == "upstream plain-text failure"
    assert output["alternate"] == "alternate detail"
    assert output["saving"] is False
    assert output["calls"] == 2


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_missing_config_does_not_stick_config_saving() -> None:
    """A status payload without config fails cleanly and unlocks the stepper."""
    js = _extract_dashboard_js()
    suffix = r'''
setTimeout(function(){
  (async function(){
    try{
      delete _mockStatus.config;
      delete _mockStatus.target;
      lastD=_mockStatus;
      await stepTarget(1);
      console.log(JSON.stringify({
        error:null,
        text:_elements['banner-error'].textContent,
        display:_elements['banner-error'].style.display,
        saving:configSaving,
      }));
    }catch(e){
      console.log(JSON.stringify({error:e.message,stack:e.stack}));
    }
    process.exit(0);
  })();
},300);
'''
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + suffix
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
    assert result.returncode == 0, f"Node.js missing-config test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, (
        f"Dashboard JS runtime error: {output['error']}\n{output.get('stack','')}"
    )
    assert "current target unavailable" in output["text"]
    assert output["display"] == "block"
    assert output["saving"] is False


# ---------------------------------------------------------------------------
# JS dashboard follow-ups: events, stale request telemetry, adaptive truth
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_events_and_provider_telemetry_are_truthful() -> None:
    """Recent events are bounded, stale budget gaps do not bridge, and AIMD
    providers do not present LimitState defaults as concurrency truth."""
    js = _extract_dashboard_js()
    suffix = r'''
function _dashboardSample(ts,band,stale){
  return {ts:ts,obs:2,loc:1,ph:0,ep:3,lim:4,hc:8,band:band,brk:'closed',
    stl:!!stale,age:0,qd:0,qt:0,t429:0,t503:0,li:false,
    rwin:10+ts,rlim:100,rrem:90,rlw:8+ts,rdelta:2,tp:0,cp:0};
}
setTimeout(function(){
  try{
    lastD=_mockStatus;
    viewRange='5m'; longHist=[]; hist=[];
    for(var i=0;i<25;i++){
      var sample=_dashboardSample(i+1,i%2?'low':'normal',i===20);
      sample.brk=i===8?'open':'closed';
      sample.t429=i===12?1:0;
      sample.qt=i===16?1:0;
      hist.push(sample);
    }
    renderSparks();
    var events=_elements['recent-events'].innerHTML;
    var eventBody=(events.match(/<tbody>([\s\S]*)<\/tbody>/)||[])[1]||'';
    var eventRows=(eventBody.match(/<tr/g)||[]).length;

    hist=[_dashboardSample(30,'normal',false),_dashboardSample(31,'normal',true),
          _dashboardSample(32,'normal',false)];
    renderSparks();
    var budgetSpark=_elements['rspark'].innerHTML;

    _mockStatus.config.controller='adaptive';
    _mockStatus.concurrent_sessions=0;
    _mockStatus.limit=4;
    _mockStatus.hard_cap=8;
    render(_mockStatus);
    renderSparks();
    console.log(JSON.stringify({
      error:null,eventRows:eventRows,has429:events.indexOf('429 +1')!==-1,
      hasTimeout:events.indexOf('queue timeout +1')!==-1,
      hasStale:events.indexOf('usage became stale')!==-1,
      providerSegments:(budgetSpark.match(/class="spark-rwin"/g)||[]).length,
      localSegments:(budgetSpark.match(/class="spark-rlw"/g)||[]).length,
      gauge:_elements['gauge'].innerHTML,stats:_elements['stats'].innerHTML,
      spark:_elements['spark'].innerHTML,
    }));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack}));
  }
  process.exit(0);
},300);
'''
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + suffix
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path], capture_output=True, text=True, timeout=15
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js dashboard follow-up test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, output.get("stack", "")
    assert output["eventRows"] == 20
    assert output["has429"] and output["hasTimeout"] and output["hasStale"]
    assert output["providerSegments"] == 2
    assert output["localSegments"] == 2
    assert "concurrency unavailable" in output["gauge"]
    assert ">unavailable<" in output["stats"]
    assert "class=\"spark-obs\"" not in output["spark"]
    assert "spark-lim" not in output["spark"]


# ---------------------------------------------------------------------------
# JS dashboard regressions: range ownership, stale buckets, event severity
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_keeps_latest_range_and_does_not_draw_mixed_stale_buckets() -> None:
    """A delayed old range cannot replace the selected one, and a bucket with
    any stale input remains a gap rather than a misleading joined line."""
    js = _extract_dashboard_js()
    prefix = _NODE_RENDER_PREFIX + r'''
var _baseFetch=fetch;
var _historyMode='initial';
var _historyRequests=[];
var _initialHistoryUrl=null;
function _response(entries){
  return {ok:true,status:200,json:function(){return Promise.resolve({entries:entries});},
    text:function(){return Promise.resolve('{}');},headers:{get:function(){return '';}}};
}
fetch=function(url,opts){
  if(url.indexOf('/history.json')!==-1){
    if(_historyMode==='initial'){
      _initialHistoryUrl=url;
      return Promise.resolve(_response([]));
    }
    return new Promise(function(resolve){
      _historyRequests.push({url:url,opts:opts,resolve:resolve});
    });
  }
  return _baseFetch(url,opts);
};
'''
    suffix = r'''
function _sample(ts,band,stale){
  return {ts:ts,obs:1,loc:0,ph:0,ep:2,lim:4,hc:8,band:band,brk:'closed',
    stl:!!stale,age:0,qd:0,qt:0,t429:0,t503:0,li:false,tp:0,cp:0};
}
setTimeout(function(){
  (async function(){
    try{
      _historyMode='deferred';
      setRange('1h');
      setRange('4h');
      var first=_historyRequests[0],second=_historyRequests[1];
      second.resolve(_response([_sample(400,'normal',false)]));
      await Promise.resolve(); await Promise.resolve();
      first.resolve(_response([_sample(100,'normal',false)]));
      await Promise.resolve(); await Promise.resolve();
      var rangeAfterResponses=viewRange;
      var latestTimestamp=longHist[0]&&longHist[0].ts;

      var mixed=bucketize(withIncs([
        _sample(1,'normal',false),_sample(2,'normal',true),_sample(3,'normal',false)
      ]),2);
      lastD=_mockStatus; viewRange='5m'; longHist=[];
      hist=[_sample(10,'normal',false),_sample(11,'reject',false),
            _sample(12,'normal',false),_sample(13,'low',false)];
      renderSparks();
      var events=_elements['recent-events'].innerHTML;
      console.log(JSON.stringify({
        error:null, initialHistoryUrl:_initialHistoryUrl,
        requestUrls:_historyRequests.map(function(r){return r.url;}),
        firstAborted:!!(first.opts&&first.opts.signal&&first.opts.signal.aborted),
        rangeAfterResponses:rangeAfterResponses, latestTimestamp:latestTimestamp,
        mixedFresh:mixed[0].fresh, mixedStale:mixed[0].stl,
        criticalTransitions:(events.match(/row-crit/g)||[]).length,
        warningTransitions:(events.match(/row-warn/g)||[]).length,
      }));
    }catch(e){
      console.log(JSON.stringify({error:e.message,stack:e.stack}));
    }
    process.exit(0);
  })();
},300);
'''
    script = prefix + "\n" + js + "\n" + suffix
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path], capture_output=True, text=True, timeout=15
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js range ownership test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, output.get("stack", "")
    assert output["initialHistoryUrl"].endswith("limit=2880")
    assert output["requestUrls"] == ["/history.json?limit=720", "/history.json?limit=2880"]
    assert output["firstAborted"] is True
    assert output["rangeAfterResponses"] == "4h"
    assert output["latestTimestamp"] == 400
    assert output["mixedFresh"] is False
    assert output["mixedStale"] is True
    assert output["criticalTransitions"] == 2
    assert output["warningTransitions"] == 1


# ---------------------------------------------------------------------------
# JS dashboard regressions: independent event horizon and stale reset
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_keeps_four_hour_events_when_chart_range_changes() -> None:
    """Chart requests must not replace the dedicated four-hour event source."""
    js = _extract_dashboard_js()
    suffix = r'''
function _eventSample(ts,band,stale){
  return {ts:ts,obs:2,loc:1,ph:0,ep:3,lim:4,hc:8,band:band,brk:'closed',
    stl:!!stale,age:0,qd:0,qt:0,t429:0,t503:0,li:false,
    rwin:10,rlim:100,rrem:90,rlw:8,rdelta:2,tp:0,cp:0};
}
setTimeout(function(){
  try{
    lastD=_mockStatus;
    eventHist=[_eventSample(1,'normal',false),_eventSample(2,'low',false)];
    hist=[];
    viewRange='1h';
    longHist=[_eventSample(100,'normal',false),_eventSample(101,'normal',false)];
    renderSparks();
    var firstEvents=_elements['recent-events'].innerHTML;

    viewRange='5m';
    hist=[_eventSample(200,'normal',false),_eventSample(201,'normal',false)];
    renderSparks();
    var secondEvents=_elements['recent-events'].innerHTML;
    setRange('1h');
    console.log(JSON.stringify({
      error:null,
      eventCount:eventHist.length,
      chartCount:longHist.length,
      firstHasTransition:firstEvents.indexOf('band normal → low')!==-1,
      secondHasTransition:secondEvents.indexOf('band normal → low')!==-1,
      selectedPressed:_elements['r-1h'].getAttribute('aria-pressed'),
      selectedCurrent:_elements['r-1h'].getAttribute('aria-current'),
      unselectedPressed:_elements['r-5m'].getAttribute('aria-pressed'),
    }));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack}));
  }
  process.exit(0);
},300);
'''
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + suffix
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path], capture_output=True, text=True, timeout=15
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js event history test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, output.get("stack", "")
    assert output["eventCount"] == 2
    assert output["chartCount"] == 2
    assert output["firstHasTransition"] and output["secondHasTransition"]
    assert output["selectedPressed"] == "true"
    assert output["selectedCurrent"] == "true"
    assert output["unselectedPressed"] == "false"


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_all_stale_redraw_clears_hover_and_reconciliation() -> None:
    """An all-stale redraw cannot leave a tooltip or stale reconciliation text."""
    js = _extract_dashboard_js()
    suffix = r'''
function _staleSample(ts,stale){
  return {ts:ts,obs:2,loc:1,ph:0,ep:3,lim:4,hc:8,band:'normal',brk:'closed',
    stl:!!stale,age:0,qd:0,qt:0,t429:0,t503:0,li:false,
    rwin:10,rlim:100,rrem:90,rlw:8,rdelta:2,tp:0,cp:0};
}
setTimeout(function(){
  try{
    lastD=_mockStatus; viewRange='5m'; eventHist=[];
    hist=[_staleSample(1,false),_staleSample(2,false)];
    renderSparks();
    onSparkHover({currentTarget:_elements['spark'],clientX:10,clientY:10});
    var shown=_elements['spark-tip'].style.display;
    viewRange='1h';
    longHist=[_staleSample(3,true),_staleSample(4,true)];
    renderSparks();
    console.log(JSON.stringify({
      error:null,shown:shown,hidden:_elements['spark-tip'].style.display,
      info:_elements['spark-info'].textContent,
      reconciliation:_elements['rspark-info'].textContent,
      delta:_elements['rdelta-text'].textContent,
      summary:_elements['spark-summary'].textContent,
    }));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack}));
  }
  process.exit(0);
},300);
'''
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + suffix
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path], capture_output=True, text=True, timeout=15
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js stale reset test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, output.get("stack", "")
    assert output["shown"] == "block"
    assert output["hidden"] == "none"
    assert output["info"] == "No fresh chart data"
    assert output["reconciliation"] == ""
    assert output["delta"] == ""
    assert "No fresh chart data" in output["summary"]


@pytest.mark.skipif(not _NODE, reason="node not available")
def test_dashboard_js_skips_restart_events_and_clamps_narrow_tooltips() -> None:
    """Known boot changes do not manufacture events; tooltips stay on-card."""
    js = _extract_dashboard_js()
    suffix = r'''
function _restartSample(ts,band,sid){
  return {ts:ts,obs:2,loc:1,ph:0,ep:3,lim:4,hc:8,band:band,brk:'closed',
    stl:false,age:0,qd:0,qt:0,t429:0,t503:0,li:false,tp:0,cp:0,sid:sid};
}
setTimeout(function(){
  try{
    lastD=_mockStatus;
    eventHist=[_restartSample(1,'normal','boot-a:4'),_restartSample(2,'low','boot-b:1')];
    hist=[];
    renderSparks();
    var events=_elements['recent-events'].innerHTML;

    viewRange='5m'; eventHist=[];
    hist=[_restartSample(3,'normal','boot-c:1'),_restartSample(4,'normal','boot-c:2')];
    var card=document.getElementById('spark-card');
    var tip=document.getElementById('spark-tip');
    card.getBoundingClientRect=function(){return{left:0,top:0,width:10,height:10};};
    tip.offsetWidth=100;
    tip.offsetHeight=20;
    renderSparks();
    onSparkHover({currentTarget:_elements['spark'],clientX:100,clientY:100});
    console.log(JSON.stringify({
      error:null,
      restartTransition:events.indexOf('band normal → low')!==-1,
      left:tip.style.left,
      top:tip.style.top,
    }));
  }catch(e){
    console.log(JSON.stringify({error:e.message,stack:e.stack}));
  }
  process.exit(0);
},300);
'''
    script = _NODE_RENDER_PREFIX + "\n" + js + "\n" + suffix
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        result = subprocess.run(
            [_NODE, path], capture_output=True, text=True, timeout=15
        )
    finally:
        os.unlink(path)
    assert result.returncode == 0, f"Node.js dashboard regression test failed:\n{result.stderr}"
    output = json.loads(result.stdout)
    assert output["error"] is None, output.get("stack", "")
    assert output["restartTransition"] is False
    assert output["left"] == "0px"
    assert output["top"] == "0px"
