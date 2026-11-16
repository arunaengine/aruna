// A fake host for the library's tests: datasets, objects, service answers, secrets and state live
// in memory, and it keeps the log and every call it got.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import { Conflict, NotAllowed, NotFound, handle, use } from "../aruna.js";

const encoder = new TextEncoder();
const text = (value) => JSON.stringify(value);

const JSON_TYPE = [["Content-Type", "application/json"]];
const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);

/** `target` with the JSON merge patch `patch` applied, by the host's rules; the host and every fake
 * are tested against `merge_cases.json`. */
export function merged(target, patch) {
  if (!isObject(patch)) return patch;
  const result = isObject(target) ? { ...target } : {};
  for (const [name, value] of Object.entries(patch)) {
    if (value === null) delete result[name];
    else result[name] = merged(result[name], value);
  }
  return result;
}

export class Fake {
  // Items of one page of a listing.
  page = 2;

  constructor(types = {}) {
    Object.assign(this, { lines: [], calls: [], settings: {}, secrets: {}, version: 0n });
    Object.assign(this, { datasets: new Map(), objects: new Map(), replies: new Map(), values: new Map() });
    // Earlier object versions by path and version.
    this.earlier = new Map();
    // Jobs by id: their status, as `aruna.jobs.get` answers it, and their outputs.
    this.jobs = new Map();
    use(this, types);
  }

  /** Stores dataset `id` at revision 1. */
  dataset(id, entity) {
    this.datasets.set(id, { revision: 1, entity, files: [] });
  }

  /** Stores a new current version of the object at `path`; the one before stays readable. */
  object(path, content = "", { version = "v1", marked = false } = {}) {
    const before = this.objects.get(path);
    if (before) this.earlier.set(`${path} ${before.version}`, before);
    this.objects.set(path, { content: encoder.encode(content), version, marked });
  }

  // The object at `path`: the current version, or `version`.
  objectAt(path, version) {
    const current = this.objects.get(path);
    const older = current && version && version !== current.version;
    const found = older ? this.earlier.get(`${path} ${version}`) : current;
    if (!found) throw new NotFound(`no object ${path}`);
    return found;
  }

  /** What `service` answers for `path`. */
  reply(service, path, { status = 200, json, body = "", headers = JSON_TYPE } = {}) {
    this.replies.set(`${service} ${path}`, { status, headers, body: json === undefined ? body : text(json) });
  }

  /** Stores job `id` as it stands, with the paths of its outputs. */
  job(id, { outputs = [], ...status } = {}) {
    this.jobs.set(id, { status: { id, kind: "copy", path: "", state: "succeeded", ...status }, outputs });
  }

  /** Runs the handler of `tag` with `val`, as the node would. */
  invoke(tag, val) {
    const invocation = { run: "r-1", install: "i-1", group: "g-1", settings: text(this.settings) };
    return handle({ ...invocation, trigger: { tag, val } });
  }

  log(level, message) {
    this.lines.push([level, message]);
  }

  paged(items, cursor) {
    const start = Number(cursor ?? 0);
    const next = start + this.page;
    return { items: items.slice(start, next), cursor: next < items.length ? String(next) : null };
  }

  stored(id) {
    if (!this.datasets.has(id)) throw new NotFound(`no dataset ${id}`);
    return this.datasets.get(id);
  }

  write(id, expected, change) {
    const stored = this.stored(id);
    if (String(stored.revision) !== expected) throw new Conflict(`dataset ${id} changed meanwhile`);
    change(stored);
    stored.revision += 1;
    return String(stored.revision);
  }

  datasetGet(id, whole) {
    const stored = this.stored(id);
    const entity = whole ? { ...stored.entity, hasPart: stored.files.map((file) => ({ "@id": file["@id"] })) } : stored.entity;
    return { revision: String(stored.revision), json: text(entity) };
  }

  datasetList(cursor) {
    return this.paged([...this.datasets.keys()].sort(), cursor);
  }

  datasetFiles(id, cursor) {
    return this.paged(this.stored(id).files.map(text), cursor);
  }

  datasetPatch(id, patch, expected) {
    this.calls.push(["patch", id, JSON.parse(patch)]);
    return this.write(id, expected, (stored) => {
      stored.entity = merged(stored.entity, JSON.parse(patch));
    });
  }

  filePut(id, file, expected) {
    const entity = JSON.parse(file);
    this.calls.push(["put_file", id, entity]);
    return this.write(id, expected, (stored) => {
      stored.files = [...stored.files.filter((kept) => kept["@id"] !== entity["@id"]), entity];
    });
  }

  fileRemove(id, file, expected) {
    return this.write(id, expected, (stored) => {
      stored.files = stored.files.filter((kept) => kept["@id"] !== file);
    });
  }

  datasetCreate(path, dataset) {
    const id = `D-${path}`;
    if (!this.datasets.has(id)) this.dataset(id, JSON.parse(dataset));
    return { id, revision: String(this.datasets.get(id).revision) };
  }

  objectInfo(path, version = null) {
    const stored = this.objectAt(path, version);
    return { path, size: stored.content.length, version: stored.version, marked: stored.marked };
  }

  objectList(prefix, cursor) {
    const paths = [...this.objects.keys()].filter((path) => path.startsWith(prefix)).sort();
    return this.paged(paths.map((path) => this.objectInfo(path)), cursor);
  }

  objectRead(path, version) {
    const stored = this.objectAt(path, version);
    if (stored.marked) throw new NotAllowed("this object was written by a job with a secret");
    this.calls.push(["read", path, version]);
    return stored.content;
  }

  send(request) {
    this.calls.push(["send", request]);
    const missing = { status: 404, headers: [], body: "not found" };
    const reply = this.replies.get(`${request.service} ${request.path}`) ?? missing;
    return { status: reply.status, headers: reply.headers, body: encoder.encode(reply.body) };
  }

  secret(name) {
    if (!(name in this.secrets)) throw new NotAllowed(`the install has no secret ${name}`);
    return Object.entries(this.secrets[name]);
  }

  jobCopy(source, version, target) {
    this.calls.push(["copy", source, version, target]);
    return `job-${this.calls.length}`;
  }

  jobDownload(request) {
    this.calls.push(["download", request]);
    const id = `job-${this.calls.length}`;
    this.job(id, { kind: "download", path: request.target, state: "queued" });
    return id;
  }

  jobUpload(request) {
    this.calls.push(["upload", request]);
    const id = `job-${this.calls.length}`;
    this.job(id, { kind: "upload", path: request.source, state: "queued" });
    return id;
  }

  jobGet(id) {
    if (!this.jobs.has(id)) throw new NotFound(`the install has no job ${id}`);
    return this.jobs.get(id).status;
  }

  jobOutputs(id, cursor) {
    const infos = this.jobs.get(id).outputs.map((path) => ({ path, size: 0, version: "v1", marked: false }));
    return this.paged(infos, cursor);
  }

  jobCancel(id) {
    this.calls.push(["cancel", id]);
  }

  stateGet(key) {
    return this.values.get(key) ?? null;
  }

  statePut(key, value, expected) {
    if ((this.values.get(key)?.version ?? null) !== expected) throw new Conflict(`state ${key} changed meanwhile`);
    this.version += 1n;
    this.values.set(key, { version: this.version, value });
    return this.version;
  }

  stateRemove(key, expected) {
    if (this.values.get(key)?.version !== expected) throw new Conflict(`state ${key} changed meanwhile`);
    this.values.delete(key);
  }
}
