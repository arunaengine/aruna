// Preview helper of the applet gateway: inspect mode and variant switching for the portal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
(() => {
  const script = document.currentScript;
  const portal = script && script.dataset.portal;
  if (!portal || window.parent === window) return;
  const slots = "[data-aruna-part][data-aruna-option]";
  let entries = null;
  let inspecting = false;
  let overlay = null;
  let hovered = null;

  const send = (message) => window.parent.postMessage(message, portal);

  const sourceEntries = () => {
    entries ??= fetch("/_aruna/preview-source-map.json", { credentials: "same-origin" })
      .then((response) => (response.ok ? response.json() : null))
      .then((map) => (map && Array.isArray(map.entries) ? map.entries : []))
      .catch(() => []);
    return entries;
  };

  const domPath = (element) => {
    const parts = [];
    for (let node = element; node && node !== document.documentElement; node = node.parentElement) {
      if (node.id) {
        parts.unshift(`${node.localName}#${CSS.escape(node.id)}`);
        break;
      }
      const same = node.parentElement
        ? Array.from(node.parentElement.children).filter((c) => c.localName === node.localName)
        : [node];
      const nth = same.length > 1 ? `:nth-of-type(${same.indexOf(node) + 1})` : "";
      parts.unshift(node.localName + nth);
    }
    return parts.join(" > ");
  };

  // The longest matching selector of the nearest matching element names the source.
  const findSource = (element, list) => {
    for (let node = element; node; node = node.parentElement) {
      let best = null;
      for (const entry of list) {
        try {
          if (node.matches(entry.selector) && (!best || entry.selector.length > best.selector.length)) {
            best = entry;
          }
        } catch {
          // A selector this browser cannot parse names nothing.
        }
      }
      if (best) return { file: best.file, line: best.line, column: best.column };
    }
    return null;
  };

  const highlight = (element) => {
    if (!overlay) {
      overlay = document.createElement("div");
      Object.assign(overlay.style, {
        position: "fixed",
        pointerEvents: "none",
        zIndex: "2147483647",
        border: "2px solid #2563eb",
        background: "rgba(37, 99, 235, 0.08)",
      });
      document.documentElement.appendChild(overlay);
    }
    const box = element.getBoundingClientRect();
    Object.assign(overlay.style, {
      display: "block",
      left: `${box.left}px`,
      top: `${box.top}px`,
      width: `${box.width}px`,
      height: `${box.height}px`,
    });
  };

  const onMove = (event) => {
    if (event.target instanceof Element && event.target !== overlay) {
      hovered = event.target;
      highlight(hovered);
    }
  };

  const onClick = async (event) => {
    event.preventDefault();
    event.stopPropagation();
    const element = event.target instanceof Element ? event.target : hovered;
    if (!element) return;
    const box = element.getBoundingClientRect();
    const text = (element.textContent || "").trim().replace(/\s+/g, " ").slice(0, 200);
    send({
      type: "aruna:selection",
      path: domPath(element),
      text,
      rect: { x: box.x, y: box.y, width: box.width, height: box.height },
      source: findSource(element, await sourceEntries()),
    });
  };

  const setInspect = (enabled) => {
    if (enabled === inspecting) return;
    inspecting = enabled;
    const change = enabled ? "addEventListener" : "removeEventListener";
    document[change]("mousemove", onMove, true);
    document[change]("click", onClick, true);
    if (!enabled && overlay) overlay.style.display = "none";
  };

  const variants = () => {
    const parts = new Map();
    for (const element of document.querySelectorAll(slots)) {
      const { arunaPart: part, arunaOption: option } = element.dataset;
      if (!parts.has(part)) parts.set(part, { part, options: [], active: null });
      const entry = parts.get(part);
      if (!entry.options.includes(option)) entry.options.push(option);
      if (!element.hidden) entry.active = option;
    }
    return Array.from(parts.values());
  };

  const setVariant = (part, option) => {
    const marked = Array.from(document.querySelectorAll(slots)).filter(
      (element) => element.dataset.arunaPart === part,
    );
    if (!marked.some((element) => element.dataset.arunaOption === option)) return;
    for (const element of marked) element.hidden = element.dataset.arunaOption !== option;
  };

  // Only the portal that framed this preview may steer it, and only with these two messages.
  window.addEventListener("message", (event) => {
    if (event.origin !== portal || event.source !== window.parent) return;
    const data = event.data;
    if (!data || typeof data !== "object") return;
    if (data.type === "aruna:inspect" && typeof data.enabled === "boolean") {
      setInspect(data.enabled);
    } else if (
      data.type === "aruna:variant" &&
      typeof data.part === "string" &&
      typeof data.option === "string"
    ) {
      setVariant(data.part, data.option);
    }
  });

  const ready = () => send({ type: "aruna:ready", variants: variants() });
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", ready);
  else ready();
})();
