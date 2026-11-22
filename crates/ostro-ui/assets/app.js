// Ostro UI script (plan 11-15). Progressive enhancement only: every page works without it.
// Rules (D-P11-15): no eval or Function constructor, no inline handlers; served with
// script-src 'self'.
//
// htmx does not swap 4xx/5xx responses; this shows the server's error text in the
// page's #flash region instead of failing silently.
(function () {
  "use strict";
  document.addEventListener("htmx:responseError", function (ev) {
    var flash = document.getElementById("flash");
    if (!flash) {
      return;
    }
    var xhr = ev.detail && ev.detail.xhr;
    var status = xhr ? xhr.status : 0;
    flash.textContent = "Request failed (" + status + "). Reload the page to retry.";
    flash.hidden = false;
  });
  document.addEventListener("htmx:sendError", function () {
    var flash = document.getElementById("flash");
    if (flash) {
      flash.textContent = "The server is unreachable.";
      flash.hidden = false;
    }
  });
})();
