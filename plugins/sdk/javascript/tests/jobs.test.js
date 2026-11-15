// Tests jobs: a copy answers its job at once, a job by id with its upload answer and report,
// outputs page by page, cancel, and the finished job as the subject of `job_finished`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import assert from "node:assert/strict";
import { test } from "node:test";

import aruna from "../aruna.js";
import { Fake } from "./fake.js";

test("starts copies", () => {
  const fake = new Fake();
  const job = aruna.objects.copy("in/a", "out/a", { version: "v7" });
  assert.deepEqual([job.id, job.kind, job.path, job.state, job.ok], ["job-1", "copy", "out/a", "queued", false]);
  assert.deepEqual(fake.calls, [["copy", "in/a", "v7", "out/a"]], "a start makes no other call");
  assert.equal(aruna.jobs.copy("in/b", "out/b").id, "job-2");
});

test("reads jobs", () => {
  const fake = new Fake();
  const reply = { status: 201, headers: [["ETag", "e1"]], body: new TextEncoder().encode('{"checksum": "md5:1"}') };
  fake.job("j1", { kind: "upload", path: "raw/a", reply, report: '{"reads": 9}', outputs: ["out/0", "out/1", "out/2"] });
  const job = aruna.jobs.get("j1");
  assert.equal(job.ok, true);
  assert.deepEqual([job.reply.status, job.reply.headers.etag, job.reply.json().checksum], [201, "e1", "md5:1"]);
  assert.deepEqual(job.reportJson(), { reads: 9 });
  assert.deepEqual([...job.outputs()].map((output) => output.path), ["out/0", "out/1", "out/2"]);
  job.cancel();
  assert.deepEqual(fake.calls, [["cancel", "j1"]]);
  fake.job("j1", { state: "cancelled" });
  job.refresh();
  assert.deepEqual([job.state, job.ok, job.reply], ["cancelled", false, null]);
  assert.throws(() => aruna.jobs.get("j2"), aruna.NotFound);
});

test("delivers finished jobs", async () => {
  const fake = new Fake();
  fake.job("j1", { state: "failed", message: "the source is gone" });
  aruna.on("job_finished", (job) => [job.id, job.ok, job.message]);
  const event = { kind: "job_finished", path: "", resource: "j1", revision: "failed" };
  assert.deepEqual(JSON.parse(await fake.invoke("event", event)), ["j1", false, "the source is gone"]);
});
