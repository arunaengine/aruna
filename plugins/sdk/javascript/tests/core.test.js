// Tests the library core: handlers by trigger, their subjects, JSON answers, the run outcome of
// errors, and console lines in the run's log.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import assert from "node:assert/strict";
import { test } from "node:test";

import aruna, { triggers } from "../aruna.js";
import { Fake } from "./fake.js";

test("answers JSON", async () => {
  const fake = new Fake();
  fake.settings = { channel: "c-9" };
  aruna.on(["manual", "timer"], (subject) => ({ subject, channel: aruna.settings.channel, run: aruna.run.id }));
  const answer = await fake.invoke("manual", '{"b": 1, "a": 2}');
  assert.equal(answer, '{"channel":"c-9","run":"r-1","subject":{"a":2,"b":1}}');
  const ticked = JSON.parse(await fake.invoke("timer", 1762700000000n));
  assert.equal(ticked.subject, new Date(1762700000000).toISOString());
  aruna.on("manual", async () => undefined);
  assert.equal(await fake.invoke("manual", ""), undefined);
  assert.deepEqual(triggers(), ["manual", "timer"]);
});

test("maps errors", async () => {
  const fake = new Fake();
  const failed = (tag, val) => fake.invoke(tag, val).then(() => assert.fail("the run must fail"), (failure) => failure);
  aruna.on("manual", (form) => {
    if (form.input === "retry") throw new aruna.Retry("try later");
    if (form.input === "type") return null.field;
    if (form.input === "async") return Promise.reject(new aruna.Unavailable("later"));
    throw new aruna.InvalidInput("no such input");
  });
  assert.deepEqual(await failed("manual", '{"input": "retry"}'), { kind: "retryable", message: "try later" });
  assert.deepEqual(await failed("manual", '{"input": "async"}'), { kind: "retryable", message: "later" });
  assert.equal((await failed("manual", '{"input": "type"}')).message.split(":")[0], "TypeError");
  assert.equal((await failed("manual", '{"input": "other"}')).kind, "invalid-input");
  assert.equal((await failed("manual", "[1]")).message, "the form is not a JSON object");
  const missing = await failed("webhook", { deliveryId: null, request: {} });
  assert.deepEqual(missing, { kind: "invalid-input", message: "this plugin has no handler for the trigger `webhook`" });
});

test("loads subjects", async () => {
  const fake = new Fake();
  const headers = [{ name: "X-Token-Id", value: "t" }];
  const request = { method: "POST", query: "a=1", headers, body: new TextEncoder().encode('{"id": 7}') };
  aruna.on("webhook", (got) => [got.method, got.query, got.headers, got.json(), got.deliveryId]);
  const answer = JSON.parse(await fake.invoke("webhook", { deliveryId: "d-1", request }));
  assert.deepEqual(answer, ["POST", "a=1", { "x-token-id": "t" }, { id: 7 }, "d-1"]);
  aruna.on("sync_failed", (event) => [event.ok, event.path, event.relationship, event.job, aruna.run.event.kind]);
  const event = { kind: "sync_failed", path: "raw/", resource: "rel-1", revision: "job-1" };
  assert.deepEqual(JSON.parse(await fake.invoke("event", event)), [false, "raw/", "rel-1", "job-1", "sync_failed"]);
  const write = console.log;
  aruna.on("timer", () => console.log("tick", { at: 1 }));
  await fake.invoke("timer", 0n);
  assert.deepEqual(fake.lines, [["info", 'tick {"at":1}']]);
  assert.equal(console.log, write);
});
