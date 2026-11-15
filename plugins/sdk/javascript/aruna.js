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
const encoder = new TextEncoder();
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

// The items of paged host answers, one page fetched at a time.
function* pages(fetch) {
  let cursor = null;
  do {
    const page = fetch(cursor);
    yield* page.items;
    cursor = page.cursor ?? null;
  } while (cursor !== null);
}

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const copied = (value) => JSON.parse(json(value));
const filled = (value) =>
  value !== null && value !== undefined && value !== "" && (typeof value !== "object" || Object.keys(value).length > 0);

/** The JSON merge patch that turns `old` into `new`: what changed, `null` for each name removed,
 * and the same rule inside objects on both sides, as the host applies a merge patch. */
export function mergePatch(old, now) {
  const patch = {};
  for (const name of Object.keys(old)) if (!(name in now)) patch[name] = null;
  for (const [name, value] of Object.entries(now)) {
    if (isObject(old[name]) && isObject(value)) {
      const inner = mergePatch(old[name], value);
      if (Object.keys(inner).length > 0) patch[name] = inner;
    } else if (!(name in old) || json(old[name]) !== json(value)) patch[name] = value;
  }
  return patch;
}

/** One dataset at the revision it was read; other properties by name with `get` and `set`. */
export class Dataset {
  #read = {};
  #properties = {};

  constructor(id, revision, entity) {
    this.id = id;
    this.revision = revision;
    Object.assign(this, { name: "", description: "", keywords: [], license: null, identifiers: {} });
    for (const [name, value] of Object.entries(entity)) this.set(name, value);
    // Compared in the form a save writes, so a field read as text is unchanged until changed;
    // a copy, so editing a nested value in place still counts as a change.
    this.#read = copied(this.#entity());
  }

  get(name) {
    return this.#entity()[name];
  }

  set(name, value) {
    if (name === "name" || name === "description") this[name] = typeof value === "string" ? value : "";
    else if (name === "keywords") {
      const words = typeof value === "string" ? value.split(",") : Array.isArray(value) ? value : [];
      this.keywords = words.filter((word) => typeof word === "string" && word.trim()).map((word) => word.trim());
    } else if (name === "license") {
      const id = isObject(value) ? value["@id"] : value;
      this.license = typeof id === "string" && id ? id : null;
    } else if (name === "identifier" || name === "identifiers") this.identifiers = isObject(value) ? { ...value } : {};
    else if (value === null || value === undefined) delete this.#properties[name];
    else this.#properties[name] = value;
  }

  remove(name) {
    this.set(name, null);
  }

  #entity() {
    const entity = { ...this.#properties };
    const own = { name: this.name, description: this.description, keywords: [...this.keywords] };
    Object.assign(own, { license: this.license, identifier: { ...this.identifiers } });
    // An empty field is left out, so a save removes it.
    for (const [name, value] of Object.entries(own)) if (filled(value)) entity[name] = value;
    return entity;
  }

  /** Sends the fields that changed with the revision read and answers their names; nothing changed
   * makes no call. A change made meanwhile throws `Conflict`. */
  save() {
    const now = this.#entity();
    const patch = mergePatch(this.#read, now);
    for (const name of Object.keys(patch)) if (name.startsWith("@")) delete patch[name];
    const changed = Object.keys(patch).sort();
    if (changed.length === 0) return [];
    this.revision = calls().datasetPatch(this.id, json(patch), this.revision);
    this.#read = copied(now);
    return changed.map((name) => (name === "identifier" ? "identifiers" : name));
  }

  /** Reads the dataset again; changes not saved are lost. */
  reload() {
    const fresh = datasets.get(this.id);
    Object.assign(this, fresh);
    this.#read = fresh.#read;
    this.#properties = fresh.#properties;
  }

  /** The dataset entity with its file links. */
  crate() {
    return JSON.parse(calls().datasetGet(this.id, true).json);
  }

  /** The dataset's `File` entities, one page at a time. */
  *files() {
    for (const file of pages((cursor) => calls().datasetFiles(this.id, cursor))) yield JSON.parse(file);
  }

  /** Links an `Object`, or the object at a bucket path, as a `File` entity whose `contentUrl` is its
   * `s3://` address; named by `id`, else by that address. */
  addFile(object, { id, ...properties } = {}) {
    const linked = typeof object === "string" ? objects.get(object) : object;
    const name = linked.path.split("/").pop();
    const file = { "@id": id ?? linked.url, "@type": "File", name, contentSize: linked.size, contentUrl: linked.url };
    this.putFile({ ...file, ...properties });
  }

  /** Adds or replaces one `File` entity the plugin built itself. */
  putFile(file) {
    this.revision = calls().filePut(this.id, json(file), this.revision);
  }

  removeFile(id) {
    this.revision = calls().fileRemove(this.id, id, this.revision);
  }
}

export const datasets = {
  /** The dataset `id`, without its file links. */
  get(id) {
    const document = calls().datasetGet(id, false);
    return new Dataset(id, document.revision, JSON.parse(document.json));
  },

  /** Every dataset of the group the install may read, each loaded, one page of ids at a time. */
  *list() {
    for (const id of pages((cursor) => calls().datasetList(cursor))) yield datasets.get(id);
  },

  /** Creates the dataset at `path` below the group's metadata root; the path names it, so creating
   * it again answers the same dataset. */
  create(path, { identifiers, ...fields } = {}) {
    const given = identifiers === undefined ? fields : { ...fields, identifier: identifiers };
    const entity = Object.fromEntries(Object.entries(given).filter(([, value]) => value != null));
    if (Object.keys(entity).length === 0) throw new InvalidCall("a dataset needs at least one field");
    const created = calls().datasetCreate(path, json(entity));
    return new Dataset(created.id, created.revision, entity);
  },
};
loaders.set("dataset_created", datasets.get).set("dataset_updated", datasets.get);
fieldLoaders.set("dataset", datasets.get);

/** One object version. A `marked` version was written by a job with a secret: a plugin cannot read,
 * copy or upload it, though it may still link it into a dataset. */
class BucketObject {
  constructor({ path, size, version, marked = false }) {
    Object.assign(this, { path, size, version, marked });
  }

  /** The canonical address, `s3://bucket/key`. */
  get url() {
    return `s3://${this.path}`;
  }

  /** The bytes of this version, at most 1 MiB; a larger object is for a task. */
  read() {
    return calls().objectRead(this.path, this.version || null);
  }

  readText() {
    return decoder.decode(this.read());
  }

  readJson() {
    return JSON.parse(this.readText());
  }
}
export { BucketObject as Object };

export const objects = {
  /** The current version of the object at `path`. */
  get(path) {
    return new BucketObject(calls().objectInfo(path));
  },

  /** Every object below `prefix` the install may read, one page at a time. */
  *list(prefix) {
    for (const info of pages((cursor) => calls().objectList(prefix, cursor))) yield new BucketObject(info);
  },

  /** Starts a copy of the object at `source` to `target` and answers its `Job`. */
  copy(source, target, options) {
    return jobs.copy(source, target, options);
  },
};
loaders.set("data_uploaded", objects.get);
fieldLoaders.set("object", objects.get);

/** A service's whole answer. */
export class Reply {
  constructor({ status, headers, body }) {
    this.status = status;
    this.body = Uint8Array.from(body);
    this.headers = Object.fromEntries(headers.map(([name, value]) => [name.toLowerCase(), value]));
  }

  get text() {
    return decoder.decode(this.body);
  }

  json() {
    return JSON.parse(this.text);
  }
}

const pairs = (values) => Object.entries(values ?? {}).map(([name, value]) => [String(name), String(value)]);

/** The service the manifest declares as `[services.<name>]`; each method takes a path below its
 * address, options, or both. */
export class Service {
  constructor(name) {
    this.name = name;
  }

  /** Sends one request. A status outside 200 to 299 throws `ServiceError` unless `check` is false. */
  request(method, path = "", { json: value, body, query, headers = {}, check = true } = {}) {
    const sent = { ...headers };
    if (value !== undefined) {
      body = json(value);
      sent["content-type"] ??= "application/json";
    }
    const bytes = typeof body === "string" ? encoder.encode(body) : (body ?? new Uint8Array());
    const request = { service: this.name, method, path, query: pairs(query), headers: pairs(sent), body: bytes };
    const reply = new Reply(calls().send(request));
    if (check && !(reply.status >= 200 && reply.status < 300)) {
      throw new ServiceError(reply.status, reply.text.slice(0, 200));
    }
    return reply;
  }

  get(path, options) {
    return this.#call("get", path, options);
  }

  head(path, options) {
    return this.#call("head", path, options);
  }

  post(path, options) {
    return this.#call("post", path, options);
  }

  put(path, options) {
    return this.#call("put", path, options);
  }

  patch(path, options) {
    return this.#call("patch", path, options);
  }

  delete(path, options) {
    return this.#call("delete", path, options);
  }

  // The path may be left out: `catalog.post({ json })`.
  #call(method, path, options) {
    return typeof path === "string" ? this.request(method, path, options) : this.request(method, "", path);
  }
}

export function service(name) {
  return new Service(name);
}

/** The fields of a secret the manifest lists under `[secrets]`; they are the code's data then.
 * Secrets of services are added by the node and never reach the code. */
export function secret(name) {
  return Object.fromEntries(calls().secret(name));
}

// The version of each key this run read, for compare and swap writes.
const versions = new Map();
// How often `update` reads and writes again after a conflict.
const TRIES = 5;

/** Small private values of the install as JSON. A write uses the version this run read, or makes
 * a new value when the run did not read the key. */
export const state = {
  get(key, fallback) {
    const entry = calls().stateGet(key);
    versions.set(key, entry ? entry.version : null);
    return entry ? JSON.parse(decoder.decode(entry.value)) : fallback;
  },

  set(key, value) {
    const text = encoder.encode(json(value));
    versions.set(key, calls().statePut(key, text, versions.get(key) ?? null));
  },

  /** Reads `key`, applies `change` and writes the result, again after a conflict up to five times;
   * answers the value written. */
  update(key, change, fallback) {
    for (let tried = 1; ; tried++) {
      const value = change(state.get(key, fallback));
      try {
        state.set(key, value);
        return value;
      } catch (error) {
        if (!(error instanceof Conflict) || tried === TRIES) throw error;
      }
    }
  },

  /** Removes `key` at the version this run read, reading it first when it did not. */
  remove(key) {
    if (!versions.has(key)) state.get(key);
    const version = versions.get(key);
    if (version !== null) calls().stateRemove(key, version);
    versions.set(key, null);
  },
};
starts.push(() => versions.clear());

/** One job of the install. `state` is queued, running, succeeded, failed, error (an attempt failed
 * and is retried), cancelled or unknown. */
export class Job {
  constructor({ id, kind, path = "", state = "queued", message = null, bytes = null, version = null,
    exitCode = null, reply = null, report = null }) {
    Object.assign(this, { id, kind, path, state, message, bytes, version, exitCode, report });
    this.reply = reply === null ? null : new Reply(reply);
  }

  get ok() {
    return this.state === "succeeded";
  }

  /** Reads the job again. */
  refresh() {
    Object.assign(this, jobs.get(this.id));
  }

  /** Asks the job to stop; a job that ended stays as it ended. */
  cancel() {
    calls().jobCancel(this.id);
  }

  /** The objects a task wrote, one page at a time. */
  *outputs() {
    for (const info of pages((cursor) => calls().jobOutputs(this.id, cursor))) yield new BucketObject(info);
  }

  /** The report as JSON, or null when it was left out. */
  reportJson() {
    return this.report === null ? null : JSON.parse(this.report);
  }
}

/** Jobs the install started: copies now, downloads, uploads and tasks later. A start answers the
 * job at once; `job_finished` delivers it once it ended and the run that started it succeeded. */
export const jobs = {
  /** The job `id` of the install, as it stands now. */
  get(id) {
    return new Job(calls().jobGet(id));
  },

  /** Copies the object at `source`, pinned to `version` when given, to `target`; it needs read on
   * the source and write on the target. A rerun of the run gets the same job back. */
  copy(source, target, { version = null } = {}) {
    return new Job({ id: calls().jobCopy(source, version, target), kind: "copy", path: target });
  },
};
loaders.set("job_finished", jobs.get);

export default {
  on, run, settings, log, datasets, objects, jobs, service, secret, state,
  Dataset, Job, Object: BucketObject, Reply, Request, Service, SyncEvent,
  ArunaError, Conflict, Fail, InvalidCall, InvalidInput, LimitReached, NotAllowed, NotFound, Retry,
  ServiceError, Unavailable, UnknownOutcome,
};
