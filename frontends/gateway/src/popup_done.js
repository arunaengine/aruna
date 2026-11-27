// Pop-up sign-in completion: hands the code back to the same-origin framing applet.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
(() => {
  const script = document.currentScript;
  if (!script || !window.opener) return;
  const { code, state } = script.dataset;
  if (!code || !state) return;
  window.opener.postMessage(
    { type: "aruna:auth", code, state },
    window.location.origin,
  );
  window.history.replaceState(null, "", window.location.pathname);
  window.close();
})();
