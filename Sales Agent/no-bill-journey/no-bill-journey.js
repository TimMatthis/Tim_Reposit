/**
 * Gatekeeper: enable primary CTA only when assignment checkbox is checked.
 */
(function () {
  var checkbox = document.getElementById("nb-assignment-ack");
  var btn = document.getElementById("nb-unlock-upload");
  if (!checkbox || !btn) return;

  var uploadUrl = btn.getAttribute("data-upload-url") || "upload-portal.html";

  function sync() {
    var ok = checkbox.checked;
    btn.disabled = !ok;
    btn.classList.toggle("nb-btn--disabled", !ok);
    btn.setAttribute("aria-disabled", ok ? "false" : "true");
  }

  btn.addEventListener("click", function () {
    if (!btn.disabled) {
      window.location.href = uploadUrl;
    }
  });

  checkbox.addEventListener("change", sync);
  sync();
})();
