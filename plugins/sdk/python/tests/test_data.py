"""Tests datasets and objects against the fake host: a save of only what changed, files,
listings, creates, small reads, a marked version, and both as subjects."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
import tempfile
import unittest
from pathlib import Path

import aruna
from aruna.datasets import merge_patch
from aruna.testing import Fake, merged

CASES = Path(__file__).resolve().parents[2] / "merge_cases.json"


class DataTest(unittest.TestCase):
    def setUp(self):
        self.fake = Fake()
        self.fake.dataset("D1", name="Run 7", keywords="pcr, gel", license={"@id": "CC-BY-4.0"},
                          identifier={"doi": "10.1/x"}, publisher="Lab")
        self.fake.object("raw/reads.fq", b"ACGT")
        self.fake.object("raw/out.json", b'{"reads": 9}', marked=True)

    def test_save_changes(self):
        dataset = aruna.datasets.get("D1")
        self.assertEqual((dataset.keywords, dataset.license, dataset["publisher"]), (["pcr", "gel"], "CC-BY-4.0", "Lab"))
        self.assertEqual(dataset.save(), [], "nothing changed, no call")
        dataset.identifiers["catalog"] = "c-1"
        del dataset["publisher"]
        self.assertEqual(dataset.save(), ["identifiers", "publisher"])
        self.assertEqual(self.fake.calls[-1], ("patch", "D1", {"identifier": {"catalog": "c-1"}, "publisher": None}))
        stale = aruna.datasets.get("D1")
        dataset.name = "Run 8"
        dataset.save()
        stale.name = "Run 9"
        with self.assertRaises(aruna.Conflict):
            stale.save()
        stale.reload()
        self.assertEqual(stale.name, "Run 8")

    def test_nested_edits(self):
        self.fake.dataset("D2", name="Gel", author=["Bob"])
        dataset = aruna.datasets.get("D2")
        dataset["author"].append("Alice")
        self.assertEqual(dataset.save(), ["author"])
        dataset["author"].append("Carol")
        self.assertEqual(dataset.save(), ["author"])
        self.assertEqual(aruna.datasets.get("D2")["author"], ["Bob", "Alice", "Carol"])

    def test_create_again(self):
        aruna.datasets.create("runs/8", name="Original")
        again = aruna.datasets.create("runs/8", name="Changed")
        self.assertEqual(again.name, "Original", "a repeat answers the stored dataset")
        again.name = "Changed"
        self.assertEqual(again.save(), ["name"])
        self.assertEqual(self.fake.calls[-1], ("patch", "D-runs/8", {"name": "Changed"}))

    def test_merge_cases(self):
        # The host's own merge patch is tested against the same cases.
        for target, patch, result in json.loads(CASES.read_text()):
            self.assertEqual(merged(target, patch), result, (target, patch))
            if isinstance(target, dict) and isinstance(result, dict):
                self.assertEqual(merged(target, merge_patch(target, result)), result)

    def test_remove_nested(self):
        dataset = aruna.datasets.get("D1")
        dataset.identifiers["catalog"] = "c1"
        dataset.save()
        del dataset.identifiers["doi"]
        self.assertEqual(dataset.save(), ["identifiers"])
        self.assertEqual(self.fake.calls[-1], ("patch", "D1", {"identifier": {"doi": None}}))
        self.assertEqual(aruna.datasets.get("D1").identifiers, {"catalog": "c1"})

    def test_clear_fields(self):
        dataset = aruna.datasets.get("D1")
        dataset.keywords, dataset.license = [], None
        self.assertEqual(dataset.save(), ["keywords", "license"])
        self.assertEqual(self.fake.calls[-1], ("patch", "D1", {"keywords": None, "license": None}))
        self.assertEqual(aruna.datasets.get("D1").keywords, [])

    def test_files_and_listings(self):
        dataset = aruna.datasets.get("D1")
        dataset.add_file("raw/reads.fq", id="reads/x", encodingFormat="text/plain")
        expected = {"@id": "reads/x", "@type": "File", "name": "reads.fq", "contentSize": 4,
                    "contentUrl": "s3://raw/reads.fq", "encodingFormat": "text/plain"}
        self.assertEqual(list(dataset.files()), [expected])
        self.assertEqual(dataset.crate()["hasPart"], [{"@id": "reads/x"}])
        dataset.remove_file("reads/x")
        self.assertEqual(list(dataset.files()), [])
        for n in range(2, 5):
            self.fake.dataset(f"D{n}", name=f"Run {n}")
        self.assertEqual([d.name for d in aruna.datasets.list()], ["Run 7", "Run 2", "Run 3", "Run 4"])
        created = aruna.datasets.create("zenodo/1", name="Zenodo 1", identifiers={"zenodo": "1"})
        self.assertEqual(created.identifiers, {"zenodo": "1"})
        self.assertEqual(aruna.datasets.create("zenodo/1", name="again").id, created.id)

    def test_objects(self):
        reads = aruna.objects.get("raw/reads.fq")
        self.assertEqual((reads.url, reads.size, reads.read_text()), ("s3://raw/reads.fq", 4, "ACGT"))
        listed = [item.path for item in aruna.objects.list("raw/")]
        self.assertEqual(listed, ["raw/out.json", "raw/reads.fq"])
        marked = aruna.objects.get("raw/out.json")
        self.assertTrue(marked.marked)
        with self.assertRaises(aruna.NotAllowed):
            marked.read_json()
        with self.assertRaises(aruna.NotFound):
            aruna.objects.get("raw/none")

    def test_subjects_loaded(self):
        aruna.on("dataset_created")(lambda dataset: dataset.name)
        self.assertEqual(self.fake.event("dataset_created", "D1"), "Run 7")
        aruna.on("data_uploaded")(lambda upload: upload.size)
        self.assertEqual(self.fake.event("data_uploaded", "raw/reads.fq"), 4)
        with tempfile.TemporaryDirectory() as folder:
            manifest = Path(folder) / "plugin.toml"
            manifest.write_text('[manual]\ndataset = { type = "dataset" }\nfile = { type = "object" }\nnote = "Note"\n')
            fake = Fake(manifest=manifest)
        fake.datasets, fake.objects = self.fake.datasets, self.fake.objects
        aruna.on("manual")(lambda form: [form.dataset.license, form.file.size, form.note])
        self.assertEqual(fake.manual(dataset="D1", file="raw/reads.fq", note="hi"), ["CC-BY-4.0", 4, "hi"])


if __name__ == "__main__":
    unittest.main()
