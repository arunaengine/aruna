// Embedded iframe sign-in launcher: opens a top-level pop-up and redeems in this frame.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
(() => {
  const script = document.currentScript;
  if (!script) return;
  const { popupUrl, state, returnPath } = script.dataset;
  const button = document.getElementById("aruna-signin");
  if (!popupUrl || !state || !button) return;
  const back = returnPath || "/";
  let popup = null;
  window.addEventListener("message", (event) => {
    if (event.origin !== window.location.origin || event.source !== popup) return;
    const data = event.data;
    if (!data || data.type !== "aruna:auth") return;
    if (data.state !== state || typeof data.code !== "string") return;
    const query = new URLSearchParams({ code: data.code, state, return: back });
    window.location.replace(`/_aruna/auth/callback?${query.toString()}`);
  });
  button.addEventListener("click", () => {
    popup = window.open(popupUrl, "aruna-signin", "width=520,height=680");
  });
})();
