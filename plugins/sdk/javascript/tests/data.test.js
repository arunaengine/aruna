// Tests datasets and objects: a save of only what changed at the revision read, a conflict, file
// entities, listings page by page, creates named by their path, reads, and loaded subjects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

import assert from "node:assert/strict";
import { test } from "node:test";

import { readFileSync } from "node:fs";

import aruna, { mergePatch } from "../aruna.js";
import { Fake, merged } from "./fake.js";

const CASES = JSON.parse(readFileSync(new URL("../../merge_cases.json", import.meta.url), "utf8"));
const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);

function seeded(types) {
  const fake = new Fake(types);
  fake.dataset("D1", { "@id": "./", name: "Run 7", keywords: "pcr, gel", license: { "@id": "CC-BY-4.0" },
    identifier: { doi: "10.1/x" }, publisher: "Lab" });
  fake.object("raw/reads.fq", "ACGT");
  fake.object("raw/out.json", '{"reads": 9}', { marked: true });
  return fake;
}

test("saves changes", () => {
  const fake = seeded();
  const dataset = aruna.datasets.get("D1");
  assert.deepEqual([dataset.keywords, dataset.license, dataset.get("publisher")], [["pcr", "gel"], "CC-BY-4.0", "Lab"]);
  assert.deepEqual(dataset.save(), [], "nothing changed, no call");
  assert.equal(fake.calls.length, 0);
  dataset.identifiers.catalog = "c-1";
  dataset.remove("publisher");
  assert.deepEqual(dataset.save(), ["identifiers", "publisher"]);
  const patch = { identifier: { catalog: "c-1" }, publisher: null };
  assert.deepEqual(fake.calls.at(-1), ["patch", "D1", patch]);
  const stale = aruna.datasets.get("D1");
  dataset.name = "Run 8";
  dataset.save();
  stale.name = "Run 9";
  assert.throws(() => stale.save(), aruna.Conflict);
  stale.reload();
  assert.equal(stale.name, "Run 8");
  stale.keywords = [];
  assert.deepEqual(stale.save(), ["keywords"]);
  assert.deepEqual(aruna.datasets.get("D1").keywords, []);
});

test("saves nested edits", () => {
  const fake = seeded();
  fake.dataset("D2", { name: "Gel", author: ["Bob"] });
  const dataset = aruna.datasets.get("D2");
  dataset.get("author").push("Alice");
  assert.deepEqual(dataset.save(), ["author"]);
  dataset.get("author").push("Carol");
  assert.deepEqual(dataset.save(), ["author"]);
  assert.deepEqual(aruna.datasets.get("D2").get("author"), ["Bob", "Alice", "Carol"]);
});

test("creates again", () => {
  const fake = seeded();
  aruna.datasets.create("runs/8", { name: "Original" });
  const again = aruna.datasets.create("runs/8", { name: "Changed" });
  assert.equal(again.name, "Original", "a repeat answers the stored dataset");
  again.name = "Changed";
  assert.deepEqual(again.save(), ["name"]);
  assert.deepEqual(fake.calls.at(-1), ["patch", "D-runs/8", { name: "Changed" }]);
});

test("applies merge cases", () => {
  // The host's own merge patch is tested against the same cases.
  for (const [target, patch, result] of CASES) {
    assert.deepEqual(merged(target, patch), result, JSON.stringify([target, patch]));
    if (isObject(target) && isObject(result)) assert.deepEqual(merged(target, mergePatch(target, result)), result);
  }
});

test("removes nested values", () => {
  const fake = seeded();
  const dataset = aruna.datasets.get("D1");
  dataset.identifiers.catalog = "c1";
  dataset.save();
  delete dataset.identifiers.doi;
  assert.deepEqual(dataset.save(), ["identifiers"]);
  assert.deepEqual(fake.calls.at(-1), ["patch", "D1", { identifier: { doi: null } }]);
  assert.deepEqual(aruna.datasets.get("D1").identifiers, { catalog: "c1" });
});

test("links files", () => {
  const fake = seeded();
  const dataset = aruna.datasets.get("D1");
  dataset.addFile(aruna.objects.get("raw/reads.fq"), { encodingFormat: "text/plain" });
  dataset.addFile("raw/out.json", { id: "out" });
  const linked = { "@id": "s3://raw/reads.fq", "@type": "File", name: "reads.fq", contentSize: 4,
    contentUrl: "s3://raw/reads.fq", encodingFormat: "text/plain" };
  assert.deepEqual(fake.calls[0], ["put_file", "D1", linked]);
  assert.deepEqual([...dataset.files()].map((file) => file["@id"]), ["s3://raw/reads.fq", "out"]);
  assert.deepEqual(dataset.crate().hasPart, [{ "@id": "s3://raw/reads.fq" }, { "@id": "out" }]);
  dataset.removeFile("out");
  assert.equal(dataset.revision, "4");
  fake.page = 1;
  const created = aruna.datasets.create("runs/8", { name: "Run 8", identifiers: { lab: "8" } });
  assert.deepEqual([created.id, created.identifiers], ["D-runs/8", { lab: "8" }]);
  assert.equal(aruna.datasets.create("runs/8", { name: "Run 8" }).id, "D-runs/8");
  assert.deepEqual([...aruna.datasets.list()].map((found) => found.id), ["D-runs/8", "D1"]);
  assert.throws(() => aruna.datasets.create("runs/9"), aruna.InvalidCall);
});

test("reads objects", () => {
  seeded();
  const object = aruna.objects.get("raw/reads.fq");
  assert.deepEqual([object.url, object.size, object.version, object.marked], ["s3://raw/reads.fq", 4, "v1", false]);
  assert.equal(object.readText(), "ACGT");
  const marked = aruna.objects.get("raw/out.json");
  assert.throws(() => marked.readJson(), aruna.NotAllowed);
  assert.deepEqual([...aruna.objects.list("raw/")].map((found) => found.path), ["raw/out.json", "raw/reads.fq"]);
  assert.throws(() => aruna.objects.get("raw/none"), aruna.NotFound);
});

test("loads subjects", async () => {
  const fake = seeded({ dataset: "dataset", object: "object", note: "string" });
  aruna.on(["manual", "dataset_created", "data_uploaded"], (subject) => subject);
  aruna.on("manual", (form) => [form.dataset.name, form.object.size, form.note]);
  const form = '{"dataset": "D1", "object": "raw/reads.fq", "note": "D1"}';
  assert.deepEqual(JSON.parse(await fake.invoke("manual", form)), ["Run 7", 4, "D1"]);
  aruna.on("dataset_created", (dataset) => dataset.name);
  aruna.on("data_uploaded", (object) => object.url);
  const event = (kind, resource) => ({ kind, path: `meta/${resource}`, resource, revision: "1" });
  assert.equal(await fake.invoke("event", event("dataset_created", "D1")), '"Run 7"');
  assert.equal(await fake.invoke("event", event("data_uploaded", "raw/reads.fq")), '"s3://raw/reads.fq"');
});
