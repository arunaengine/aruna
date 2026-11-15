// A fake host for the library's tests: datasets, objects, service answers, secrets and state live
// in memory, and it keeps the log and every call it got.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import { Conflict, NotAllowed, NotFound, handle, use } from "../aruna.js";

const encoder = new TextEncoder();
const text = (value) => JSON.stringify(value);

export class Fake {
  // Items of one page of a listing.
  page = 2;

  constructor(types = {}) {
    Object.assign(this, { lines: [], calls: [], settings: {} });
    Object.assign(this, { datasets: new Map(), objects: new Map() });
    use(this, types);
  }

  /** Stores dataset `id` at revision 1. */
  dataset(id, entity) {
    this.datasets.set(id, { revision: 1, entity, files: [] });
  }

  object(path, content = "", { version = "v1", marked = false } = {}) {
    this.objects.set(path, { content: encoder.encode(content), version, marked });
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
      const entity = { ...stored.entity, ...JSON.parse(patch) };
      stored.entity = Object.fromEntries(Object.entries(entity).filter(([, value]) => value !== null));
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

  objectInfo(path) {
    const stored = this.objects.get(path);
    if (!stored) throw new NotFound(`no object ${path}`);
    return { path, size: stored.content.length, version: stored.version, marked: stored.marked };
  }

  objectList(prefix, cursor) {
    const paths = [...this.objects.keys()].filter((path) => path.startsWith(prefix)).sort();
    return this.paged(paths.map((path) => this.objectInfo(path)), cursor);
  }

  objectRead(path, version) {
    const info = this.objectInfo(path);
    if (info.marked) throw new NotAllowed("this object was written by a job with a secret");
    this.calls.push(["read", path, version]);
    return this.objects.get(path).content;
  }
}
