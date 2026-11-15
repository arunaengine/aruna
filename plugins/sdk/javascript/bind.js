// The component's own imports behind the library's host calls, in the plain values it uses; the
// build's entry puts them in place with `use`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import { cancel, copy, get as status, outputs } from "aruna:plugin/jobs@0.3.0";
import { log } from "aruna:plugin/log@0.3.0";
import { create } from "aruna:plugin/metadata-create@0.3.0";
import { files, get, list, summary } from "aruna:plugin/metadata-read@0.3.0";
import { patch, putFile, removeFile } from "aruna:plugin/metadata-write@0.3.0";
import { info, list as objectList, read } from "aruna:plugin/objects-read@0.3.0";
import { value } from "aruna:plugin/secrets@0.3.0";
import { send } from "aruna:plugin/services@0.3.0";
import { get as stateGet, put as statePut, remove as stateRemove } from "aruna:plugin/state@0.3.0";

import { raised } from "./aruna.js";

// A host error a binding threw as the library's error.
function call(work) {
  try {
    return work();
  } catch (error) {
    const host = error?.payload;
    if (typeof host?.kind === "string") throw raised(host.kind, host.message);
    throw error;
  }
}

const some = (value) => value ?? undefined;
const number = (value) => (value === undefined || value === null ? null : Number(value));
const pairs = (fields) => fields.map(({ name, value }) => [name, value]);
const fields = (pairs) => pairs.map(([name, value]) => ({ name, value }));
const object = ({ path, size, version, marked }) => ({ path, size: Number(size), version, marked });
const objects = (page) => ({ items: page.objects.map(object), cursor: some(page.cursor) });

export default {
  log(level, message) {
    log(level, message);
  },

  datasetGet(id, whole) {
    return call(() => (whole ? get : summary)(id));
  },

  datasetList(cursor) {
    const page = call(() => list(some(cursor)));
    return { items: page.resources, cursor: some(page.cursor) };
  },

  datasetFiles(id, cursor) {
    const page = call(() => files(id, some(cursor)));
    return { items: page.files, cursor: some(page.cursor) };
  },

  datasetPatch(id, text, expected) {
    return call(() => patch(id, text, expected));
  },

  filePut(id, file, expected) {
    return call(() => putFile(id, file, expected));
  },

  fileRemove(id, file, expected) {
    return call(() => removeFile(id, file, expected));
  },

  datasetCreate(path, dataset) {
    const created = call(() => create(path, dataset));
    return { id: created.resource, revision: created.revision };
  },

  objectInfo(path) {
    return object(call(() => info(path)));
  },

  objectList(prefix, cursor) {
    return objects(call(() => objectList(prefix, some(cursor))));
  },

  objectRead(path, version) {
    return call(() => read(path, some(version)));
  },

  send(request) {
    const sent = { ...request, query: fields(request.query), headers: fields(request.headers) };
    const reply = call(() => send(sent));
    return { status: reply.status, headers: pairs(reply.headers), body: reply.body };
  },

  secret(name) {
    return pairs(call(() => value(name)));
  },

  stateGet(key) {
    return call(() => stateGet(key)) ?? null;
  },

  statePut(key, value, expected) {
    return call(() => statePut(key, value, some(expected)));
  },

  stateRemove(key, expected) {
    call(() => stateRemove(key, expected));
  },

  jobCopy(source, version, target) {
    return call(() => copy(source, some(version), target));
  },

  jobGet(id) {
    const job = call(() => status(id));
    const answered = job.replyStatus ?? null;
    const body = job.replyBody ?? new Uint8Array();
    const reply = answered === null ? null : { status: answered, headers: pairs(job.replyHeaders), body };
    return { id: job.id, kind: job.kind, path: job.path, state: job.state, message: job.message ?? null,
      bytes: number(job.bytes), version: job.version ?? null, exitCode: job.exitCode ?? null, reply,
      report: job.report ?? null };
  },

  jobOutputs(id, cursor) {
    return objects(call(() => outputs(id, some(cursor))));
  },

  jobCancel(id) {
    call(() => cancel(id));
  },
};
