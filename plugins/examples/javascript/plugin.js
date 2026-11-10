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
      const fields = JSON.parse(reply.json ?? "{}");
      if (!("/id" in fields)) {
        throw { kind: "permanent", message: "the response has no field `/id`" };
      }
      const identifier = { catalog: fields["/id"] };
      patch(dataset, JSON.stringify({ identifier }), doc.revision, undefined);
    }
  },
};

// Calls the named handler with a context and a plain event and awaits it; a thrown error fails
// the run with the kind the Rust SDK would give it.
export async function handle(name, invocation) {
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
  try {
    const result = await handler(ctx, toEvent(invocation.trigger));
    return result === undefined ? undefined : typeof result === "string" ? result : JSON.stringify(result, bigints);
  } catch (error) {
    throw failure(error);
  }
}

// JSON has no BigInt, so a timer tick or another 64-bit number is written as a decimal string.
const bigints = (_, value) => (typeof value === "bigint" ? value.toString() : value);

// The plain event of a trigger: manual input as JSON, the timer tick, or the record's fields.
function toEvent({ tag, val }) {
  if (tag === "manual") return { kind: tag, input: val ? JSON.parse(val) : null };
  if (tag === "timer") return { kind: tag, tick: val };
  return { kind: tag, ...val };
}

const FAILURE_KINDS = ["permanent", "retryable", "invalid-input"];

// A handler's own failure stays; an unavailable host is retryable, any other error permanent.
function failure(error) {
  if (FAILURE_KINDS.includes(error?.kind) && typeof error.message === "string") return error;
  const host = error?.payload;
  if (typeof host?.kind === "string") {
    const kind = host.kind === "unavailable" ? "retryable" : "permanent";
    return { kind, message: `host call failed (${host.kind}): ${host.message}` };
  }
  return { kind: "permanent", message: error?.message ?? String(error) };
}
