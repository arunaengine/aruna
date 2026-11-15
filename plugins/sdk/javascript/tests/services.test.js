// Tests services and state: requests and their whole answers, status checks, state written by
// compare and swap at the version a run read, and secret fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import assert from "node:assert/strict";
import { test } from "node:test";

import aruna from "../aruna.js";
import { Fake } from "./fake.js";

test("sends requests", () => {
  const fake = new Fake();
  fake.reply("catalog", "", { json: { id: "c-1" } });
  fake.reply("catalog", "busy", { status: 503, body: "try later" });
  const catalog = aruna.service("catalog");
  const record = catalog.post({ json: { title: "Run 7", id: "D1" }, query: { dry: true } });
  assert.deepEqual([record.status, record.json().id, record.header("content-type")], [200, "c-1", "application/json"]);
  const [, sent] = fake.calls[0];
  assert.deepEqual([sent.method, sent.path, sent.query, sent.headers], ["post", "", [["dry", "true"]],
    [["content-type", "application/json"]]]);
  assert.equal(new TextDecoder().decode(sent.body), '{"id":"D1","title":"Run 7"}');
  assert.throws(() => catalog.get("busy"), (error) => error instanceof aruna.ServiceError && error.status === 503);
  assert.equal(catalog.get("busy", { check: false }).text, "try later");
  catalog.put("busy", { body: "plain", headers: { accept: "text/plain" }, check: false });
  assert.deepEqual(fake.calls.at(-1)[1].headers, [["accept", "text/plain"]]);
});

test("keeps repeated headers", async () => {
  const fake = new Fake();
  const links = [["Link", '<https://c.example.org/2>; rel="next"'], ["link", '<https://c.example.org/9>; rel="last"']];
  fake.reply("catalog", "records", { json: [], headers: links });
  const reply = aruna.service("catalog").get("records");
  assert.deepEqual(reply.headers, links);
  assert.equal(reply.header("LINK"), links[0][1]);
  assert.equal(reply.header("etag"), null);
  aruna.on("webhook", (request) => [request.headers, request.header("x-tag")]);
  const headers = [{ name: "X-Tag", value: "a" }, { name: "x-tag", value: "b" }];
  const request = { method: "POST", query: "", headers, body: new Uint8Array() };
  const answer = JSON.parse(await fake.invoke("webhook", { deliveryId: null, request }));
  assert.deepEqual(answer, [[["X-Tag", "a"], ["x-tag", "b"]], "a"]);
});

test("keeps state", async () => {
  const fake = new Fake();
  const { state } = aruna;
  assert.equal(state.get("seen", 0), 0);
  state.set("seen", 1);
  assert.equal(state.update("seen", (seen) => seen + 1), 2);
  assert.deepEqual(state.get("seen"), 2);
  fake.statePut("other", new TextEncoder().encode("1"), null);
  assert.throws(() => state.set("other", 2), aruna.Conflict, "a key the run did not read must be new");
  state.remove("other");
  assert.equal(state.get("other"), undefined);
  // A new run forgets the versions an earlier run read.
  aruna.on("timer", () => state.set("seen", 3));
  await assert.rejects(fake.invoke("timer", 0n), (failure) => failure.message.includes("changed meanwhile"));
});

test("reads secrets", () => {
  const fake = new Fake();
  fake.secrets.vault = { user: "u", token: "t" };
  assert.deepEqual(aruna.secret("vault"), { user: "u", token: "t" });
  assert.throws(() => aruna.secret("other"), aruna.NotAllowed);
});
