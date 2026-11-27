// Portal builder preview sign-in handshake: sends the partitioned state to the portal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
(() => {
  const script = document.currentScript;
  if (!script || window.parent === window) return;
  const { portal, applet, host, node, state } = script.dataset;
  if (!portal || !applet || !host || !state) return;
  window.parent.postMessage(
    { type: "aruna:preview-signin", applet, host, node: node || "", state },
    portal,
  );
})();
