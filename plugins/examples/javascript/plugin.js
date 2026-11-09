// Example plugin: posts the title of each new dataset to a catalog and, when granted,
// records the catalog id on the dataset.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
import { granted } from "aruna:plugin/grants@0.2.0";
import { get } from "aruna:plugin/metadata-read@0.2.0";
import { patch } from "aruna:plugin/metadata-write@0.2.0";
import { send } from "aruna:plugin/connections@0.2.0";

const json = (value) => new TextEncoder().encode(JSON.stringify(value));

const handlers = {
  dataset_created(ctx, event) {
    const dataset = event.kind === "manual" ? event.input?.dataset ?? event.input : event.resource;
    const doc = get(dataset);
    const title = JSON.parse(doc.json).name ?? null;
    const headers = [{ name: "content-type", value: "application/json" }];
    const request = { connection: "catalog", method: "post", path: "", query: [], headers, body: json({ id: dataset, title }) };
    const reply = send(request);
    if (granted("metadata.write", dataset)) {
      const id = JSON.parse(reply.json ?? "{}")["/id"];
      patch(dataset, JSON.stringify({ identifier: { catalog: id } }), doc.revision, undefined);
    }
  },
};

// Calls the named handler with a context and a plain event; a thrown error fails the run.
export function handle(name, invocation) {
  const handler = handlers[name];
  if (!handler) {
    throw { kind: "invalid-input", message: `${name} is not a handler of this plugin` };
  }
  const ctx = {
    run: invocation.run,
    install: invocation.install,
    group: invocation.group,
    settings: JSON.parse(invocation.settings),
  };
  const { tag, val } = invocation.trigger;
  const event = tag === "manual" ? { kind: tag, input: val ? JSON.parse(val) : null } : { kind: tag, ...val };
  try {
    const result = handler(ctx, event);
    return result === undefined ? undefined : typeof result === "string" ? result : JSON.stringify(result);
  } catch (error) {
    const message = error?.payload?.message ?? error?.message ?? String(error);
    throw { kind: "permanent", message };
  }
}
