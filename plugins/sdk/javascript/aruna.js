// The JavaScript library for Aruna plugins: register handlers by trigger with `aruna.on`, and reach
// datasets, objects, services and state through plain calls; every check stays on the node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

export class ArunaError extends Error {
  // How an uncaught error ends the run: "permanent", "retryable" or "invalid-input".
  outcome = "permanent";

  constructor(message) {
    super(message);
    this.name = new.target.name;
  }
}

// The install may not read or write what the call names.
export class NotAllowed extends ArunaError {}
export class NotFound extends ArunaError {}
// Something changed since it was read.
export class Conflict extends ArunaError {}
export class LimitReached extends ArunaError {}
// The call's arguments are not valid.
export class InvalidCall extends ArunaError {}
// The host cannot answer now; the run is tried again later.
export class Unavailable extends ArunaError {
  outcome = "retryable";
}
// A change may have happened without its answer; a person decides.
export class UnknownOutcome extends ArunaError {}
// A service answered with a status outside 200 to 299.
export class ServiceError extends ArunaError {
  constructor(status, message) {
    super(`the service answered ${status}: ${message}`);
    this.status = status;
  }
}
// The plugin gives up; the run fails.
export class Fail extends ArunaError {}
// The plugin asks for the run to be tried again later.
export class Retry extends ArunaError {
  outcome = "retryable";
}
// The run's input does not fit the plugin.
export class InvalidInput extends ArunaError {
  outcome = "invalid-input";
}

const BY_KIND = {
  "not-granted": NotAllowed,
  denied: NotAllowed,
  "not-found": NotFound,
  conflict: Conflict,
  limit: LimitReached,
  invalid: InvalidCall,
  unavailable: Unavailable,
  "unknown-outcome": UnknownOutcome,
};

/** The library's error for a host error of `kind`. */
export function raised(kind, message) {
  return new (BY_KIND[kind] ?? InvalidCall)(message);
}

let host = null;
const handlers = new Map();
const fields = new Map();
// How an event trigger's subject is loaded from its resource, by trigger.
const loaders = new Map();
// How a typed form field is loaded from its value, by field type.
const fieldLoaders = new Map();
// What every run starts with, such as forgetting the state versions of an earlier run.
const starts = [];
const decoder = new TextDecoder();

/** Puts `calls` in place of the component's imports, with the type of each `[manual]` field; the
 * build's entry and tests call it. */
export function use(calls, types = {}) {
  host = calls;
  fields.clear();
  for (const [name, kind] of Object.entries(types)) fields.set(name, kind);
}

function calls() {
  if (host === null) throw new Unavailable("the host is only reachable while a handler runs");
  return host;
}

/** Registers `handler` for the named trigger or triggers; it takes the subject or nothing, and may
 * be async. */
export function on(names, handler) {
  for (const name of [names].flat()) handlers.set(name, handler);
  return handler;
}

/** The triggers handlers are registered for; the build checks them against plugin.toml. */
export function triggers() {
  return [...handlers.keys()].sort();
}

/** Who runs the handler, and the raw event of an event trigger. */
export const run = { id: "", install: "", group: "", event: null };
/** The install's settings by name. */
export const settings = {};

/** A verified webhook request; authentication headers are already removed. */
export class Request {
  constructor({ deliveryId, request }) {
    this.method = request.method;
    this.query = request.query;
    this.body = Uint8Array.from(request.body);
    this.deliveryId = deliveryId ?? null;
    this.headers = Object.fromEntries(request.headers.map(({ name, value }) => [name.toLowerCase(), value]));
  }

  get text() {
    return decoder.decode(this.body);
  }

  json() {
    return JSON.parse(this.text);
  }
}

/** A sync relationship's job that completed or failed, below the source path. */
export class SyncEvent {
  constructor(event) {
    this.ok = event.kind === "sync_completed";
    this.path = event.path;
    this.relationship = event.resource;
    this.job = event.revision;
  }
}

function subject({ tag, val }) {
  if (tag === "manual") {
    const form = val ? JSON.parse(val) : {};
    if (form === null || typeof form !== "object" || Array.isArray(form)) {
      throw new InvalidInput("the form is not a JSON object");
    }
    for (const [name, kind] of fields) {
      const load = fieldLoaders.get(kind);
      if (load && typeof form[name] === "string") form[name] = load(form[name]);
    }
    return form;
  }
  if (tag === "timer") return new Date(Number(val));
  if (tag === "webhook") return new Request(val);
  if (val.kind.startsWith("sync_")) return new SyncEvent(val);
  const loader = loaders.get(val.kind);
  return loader ? loader(val.resource) : { ...val };
}

/** Lines of the run's log; `console` writes to it too while a handler runs. */
export const log = Object.fromEntries(
  ["debug", "info", "warn", "error"].map((level) => [level, (message) => calls().log(level, String(message))]),
);
const CONSOLE = { debug: "debug", info: "info", log: "info", warn: "warn", error: "error" };

// Runs `work` with `console` writing to the run's log, as `aruna.log` does.
async function logged(work) {
  const saved = {};
  const line = (parts) => parts.map((part) => (typeof part === "string" ? part : json(part))).join(" ");
  for (const [name, level] of Object.entries(CONSOLE)) {
    saved[name] = console[name];
    console[name] = (...parts) => log[level](line(parts));
  }
  try {
    return await work();
  } finally {
    Object.assign(console, saved);
  }
}

// JSON with sorted keys, in which a BigInt becomes a decimal string.
function sorted(_, value) {
  if (typeof value === "bigint") return value.toString();
  if (value === null || typeof value !== "object" || Array.isArray(value)) return value;
  const names = Object.keys(value).sort();
  return Object.fromEntries(names.map((name) => [name, value[name]]));
}
const json = (value) => JSON.stringify(value, sorted);

/** Runs the handler of one invocation and answers its JSON output; a thrown error becomes the
 * failure the run ends with. */
export async function handle(invocation) {
  const { tag, val } = invocation.trigger;
  const name = tag === "event" ? val.kind : tag;
  try {
    const handler = handlers.get(name);
    if (!handler) throw new InvalidInput(`this plugin has no handler for the trigger \`${name}\``);
    Object.assign(run, { id: invocation.run, install: invocation.install, group: invocation.group });
    run.event = tag === "event" ? { ...val } : null;
    for (const key of Object.keys(settings)) delete settings[key];
    Object.assign(settings, JSON.parse(invocation.settings || "{}"));
    for (const start of starts) start();
    const result = await logged(() => (handler.length === 0 ? handler() : handler(subject(invocation.trigger))));
    return result === undefined || result === null ? undefined : json(result);
  } catch (error) {
    if (error instanceof ArunaError) throw { kind: error.outcome, message: error.message };
    const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    throw { kind: "permanent", message };
  }
}

export default {
  on, run, settings, log, Request, SyncEvent,
  ArunaError, Conflict, Fail, InvalidCall, InvalidInput, LimitReached, NotAllowed, NotFound, Retry,
  ServiceError, Unavailable, UnknownOutcome,
};
