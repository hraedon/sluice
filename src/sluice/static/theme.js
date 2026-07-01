/* patina theme toggle — shared across the tool family.
   Load this synchronously in <head> (before the stylesheet) so the theme is
   applied before first paint and there is no flash. The family CSP is
   script-src 'self', which forbids inline JS, so this lives as a file.

   The localStorage key defaults to "patina-theme"; a tool can scope its own by
   setting <html data-theme-key="cw-theme"> (so multiple family apps on one
   origin do not share a preference unexpectedly). Default theme is dark. */
(function () {
  var root = document.documentElement;
  var KEY = root.getAttribute("data-theme-key") || "patina-theme";
  function get() {
    try { return localStorage.getItem(KEY) || "dark"; } catch (e) { return "dark"; }
  }
  function apply(t) { root.setAttribute("data-theme", t); }

  apply(get());

  function updateIcons() {
    var dark = root.getAttribute("data-theme") === "dark";
    var d = document.getElementById("theme-icon-dark");
    var l = document.getElementById("theme-icon-light");
    if (d) d.style.display = dark ? "none" : "block";
    if (l) l.style.display = dark ? "block" : "none";
  }

  function wire() {
    updateIcons();
    var btn = document.getElementById("theme-toggle");
    if (!btn) return;
    btn.addEventListener("click", function () {
      var next = root.getAttribute("data-theme") === "dark" ? "light" : "dark";
      apply(next);
      try { localStorage.setItem(KEY, next); } catch (e) {}
      updateIcons();
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", wire);
  } else {
    wire();
  }
})();
