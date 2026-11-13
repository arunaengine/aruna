"""One link: the sync rules, the pending marker, enrolment, sizes, write access and uploads."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import unittest

from elab import LIMIT, encoded
from tests.fake import Plugin, symfony, trap

UPLOAD = {"id": 4, "real_name": "a.csv", "long_name": "ab/abc.csv", "hash": "h1", "filesize": 3, "state": 1}


class LinkTests(unittest.TestCase):
    def setUp(self):
        self.plugin = Plugin()
        self.team, self.host = self.plugin.elab, self.plugin.host

    def enrolled(self, **given):
        given = {"title": "A", "body": "<p>a</p>", "tags": ("aruna", "pcr")} | given
        self.item = self.team.add(1, **given)
        result = self.plugin.manual({"experiment": 1})
        self.dataset = self.plugin.link(1)["dataset"]
        return result

    def sync(self):
        return self.plugin.manual({"experiment": 1})["mark"]

    def patches(self):
        return [path for method, path in self.team.sent if method in ("PATCH", "POST")]

    def kill_at_save(self, count):
        self.host.trap = trap("save", count, "kill", lambda args: args[0] == "link/1")

    def test_enrolment(self):
        self.enrolled(uploads=[UPLOAD])
        doc = self.plugin.dataset(1)
        self.assertEqual((doc["name"], doc["keywords"], doc["text"]), ("A", ["pcr"], "<p>a</p>"))
        self.assertEqual((doc["description"], doc["encodingFormat"]), ("a", "text/html"))
        self.assertEqual(doc["identifier"]["elabftw"]["id"], "1")
        fields = self.item["metadata"]["extra_fields"]
        self.assertEqual(fields["Aruna dataset id"], {"type": "text", "value": self.dataset})
        self.assertTrue(fields["Aruna dataset"]["value"].endswith(f"/datasets/{self.dataset}"))
        files = self.host.docs[self.dataset]["files"]
        self.assertEqual(files["elabftw/uploads/4"]["contentUrl"], "/r/g/lab/data/n/b/elabftw/ab/abc.csv")
        self.assertIsNone(self.plugin.link(1)["mark"])

    def test_one_side(self):
        self.enrolled()
        self.item.update(title="B", body="<p>b</p>")
        self.team.tag(1, "gel")
        self.assertIsNone(self.sync())
        doc = self.plugin.dataset(1)
        self.assertEqual((doc["name"], doc["keywords"], doc["text"]), ("B", ["gel", "pcr"], "<p>b</p>"))
        self.host.edit(self.dataset, name="C", keywords=["gel"], text="<p>c<br>d</p>")
        self.assertIsNone(self.sync())
        self.assertEqual((self.item["title"], self.item["tags"]), ("C", ["aruna", "gel"]))
        self.assertEqual(self.item["body"], "<p>c<br />d</p>")
        doc = self.plugin.dataset(1)
        self.assertEqual((doc["text"], doc["description"]), ("<p>c<br />d</p>", "c\nd"))
        revision, sent = self.host.docs[self.dataset]["revision"], len(self.patches())
        self.assertIsNone(self.sync())
        self.assertEqual((self.host.docs[self.dataset]["revision"], len(self.patches())), (revision, sent))

    def test_body_description(self):
        # eLabFTW stores the body as sent; the dataset's description still follows it.
        self.enrolled()
        self.host.edit(self.dataset, text="<p>b</p>")
        self.assertIsNone(self.sync())
        self.assertEqual((self.item["body"], self.plugin.dataset(1)["description"]), ("<p>b</p>", "b"))

    def test_both_sides(self):
        self.enrolled()
        self.item["title"] = "B"
        self.host.edit(self.dataset, name="C")
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertEqual((self.item["title"], self.plugin.dataset(1)["name"]), ("B", "C"))
        self.assertIsNone(self.plugin.manual({"resolve": 1, "keep": "aruna"})["mark"])
        self.assertEqual(self.item["title"], "C")
        self.item["title"], self.plugin.dataset(1)["name"] = "D", "E"
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertIsNone(self.plugin.manual({"resolve": 1, "keep": "elabftw"})["mark"])
        self.assertEqual(self.plugin.dataset(1)["name"], "D")

    def test_undo_aruna(self):
        # Review r1: Aruna A to B, B reaches eLabFTW, the run stops, Aruna goes back to A.
        self.enrolled()
        self.host.edit(self.dataset, name="B")
        self.kill_at_save(2)
        self.assertEqual(self.plugin.manual({"experiment": 1}), "killed")
        self.assertEqual(self.item["title"], "B")
        self.host.edit(self.dataset, name="A")
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertEqual((self.item["title"], self.plugin.dataset(1)["name"]), ("B", "A"))

    def test_undo_elab(self):
        self.enrolled()
        self.item["title"] = "B"
        self.kill_at_save(2)
        self.assertEqual(self.plugin.manual({"experiment": 1}), "killed")
        self.assertEqual(self.plugin.dataset(1)["name"], "B")
        self.item["title"] = "A"
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertEqual((self.item["title"], self.plugin.dataset(1)["name"]), ("A", "B"))

    def test_finished_write(self):
        self.enrolled()
        self.host.edit(self.dataset, name="B")
        self.kill_at_save(2)
        self.plugin.manual({"experiment": 1})
        self.assertEqual(self.plugin.link(1)["pending"], "write")
        self.assertIsNone(self.sync())
        self.assertIsNone(self.plugin.link(1)["pending"])

    def test_size_bound(self):
        body = "<p>x</p>" * 20480
        self.assertEqual(2 * encoded(body), 1_187_844)
        for text in (body, 'a/"é😀\\\'<&>', "\n\t\x01"):
            self.assertEqual(encoded(text), len(symfony(text)))
        self.enrolled()
        self.host.edit(self.dataset, text=body)
        found = self.sync()
        self.assertEqual(found["kind"], "too_large")
        self.assertEqual(self.patches()[-1], "1")  # the enrolment's link fields, nothing since
        self.assertIsNone(self.plugin.link(1)["pending"])
        self.host.edit(self.dataset, name="x" * (LIMIT // 2))
        self.assertEqual(self.sync()["kind"], "too_large")

    def test_grown_answer(self):
        # Review r3: eLabFTW's cleaner repeats `<strong>` inside each block, past the host limit.
        self.enrolled()
        self.host.edit(self.dataset, text="<strong>" * 16 + "<p>x</p>" * 990 + "</strong>" * 16)
        self.assertEqual(self.sync()["kind"], "too_large")
        self.assertEqual(self.plugin.link(1)["pending"], "write")
        self.assertEqual(self.item["body"].count("<strong>"), 16 * 990)
        self.assertEqual(self.sync()["kind"], "too_large")
        self.item["body"] = "<p>short</p>"
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertIsNone(self.plugin.manual({"resolve": 1, "keep": "elabftw"})["mark"])
        self.assertEqual(self.plugin.dataset(1)["text"], "<p>short</p>")

    def test_grown_markdown(self):
        self.enrolled(content_type=2, body="a")
        links = "[r]: https://example.org/" + "x" * 1000 + "\n" + "[a][r] " * 1100
        self.host.edit(self.dataset, text=links, encodingFormat="text/markdown")
        self.assertEqual(self.sync()["kind"], "too_large")
        self.assertEqual(self.plugin.link(1)["pending"], "write")

    def test_write_access(self):
        self.enrolled()
        self.item["canwrite_base"] = 20
        self.host.edit(self.dataset, name="B")
        self.assertEqual(self.sync()["kind"], "no_write")
        self.assertEqual(self.item["title"], "A")
        self.item["users"] = [7]
        self.assertIsNone(self.sync())
        self.assertEqual(self.item["title"], "B")
        self.item["canwrite_base"], self.item["users"], self.item["title"] = 20, [], "C"
        self.assertEqual(self.sync(), None)
        self.assertEqual(self.plugin.dataset(1)["name"], "C")

    def test_edit_mode(self):
        self.enrolled()
        self.team.locked.add(1)
        self.host.edit(self.dataset, name="B")
        found = self.sync()
        self.assertEqual(found["kind"], "failed")
        self.assertIn("being edited", found["reason"])
        self.assertIsNone(self.plugin.link(1)["pending"])
        self.team.locked.clear()
        self.assertIsNone(self.sync())
        self.assertEqual(self.item["title"], "B")

    def test_refused_removal(self):
        # The second tag removal is refused after the first one landed; a later undo must stay.
        self.enrolled()
        self.team.tag(1, "gel")
        self.sync()
        self.host.edit(self.dataset, keywords=None)
        lock = trap("elab", 2, lambda: self.team.locked.add(1), lambda args: args[0] == "PATCH")
        self.host.trap = lock
        self.assertEqual(self.sync()["kind"], "failed")
        self.team.locked.clear()
        self.host.edit(self.dataset, keywords=["gel", "pcr"])
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertEqual(self.plugin.dataset(1)["keywords"], ["gel", "pcr"])

    def test_uploads(self):
        self.enrolled(uploads=[UPLOAD])
        files = self.host.docs[self.dataset]["files"]
        files["elabftw/uploads/4"]["license"] = "CC0"
        files["other.csv"] = {"@id": "other.csv", "@type": "File"}
        self.item["uploads"] = [UPLOAD | {"hash": "h2"}, UPLOAD | {"id": 5, "long_name": "e.txt"}]
        self.assertIsNone(self.sync())
        self.assertEqual(files["elabftw/uploads/4"]["sha256"], "h2")
        self.assertEqual(files["elabftw/uploads/4"]["license"], "CC0")
        self.assertIn("elabftw/uploads/5", files)
        self.item["uploads"] = [UPLOAD | {"id": 5, "state": 3}]
        self.assertIsNone(self.sync())
        self.assertEqual(sorted(files), ["other.csv"])

    def test_parallel_enrolment(self):
        # Another run finishes the same enrolment between this run's create and its record save.
        self.team.add(1, title="A")

        def other():
            self.host.state["link/1"] = (9, '{"dataset": "elsewhere"}')

        self.host.trap = trap("save", 2, other, lambda args: args[0] == "link/1")
        self.plugin.manual({"experiment": 1})
        self.assertEqual(self.plugin.link(1), {"dataset": "elsewhere"})
        self.assertIn("experiment 1 was left to another run", self.host.logs)
        self.assertEqual(len(self.host.docs), 1)

    def test_lost_create(self):
        # Review r2: the create commits, its answer is lost, the experiment changes to B.
        self.team.add(1, title="A")
        self.host.trap = trap("create", 1, "lose")
        self.assertEqual(self.plugin.manual({"experiment": 1}), "killed")
        self.team.items[1]["title"] = "B"
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertEqual((self.plugin.dataset(1)["name"], self.team.items[1]["title"]), ("A", "B"))
        self.assertIsNone(self.plugin.manual({"resolve": 1, "keep": "elabftw"})["mark"])
        self.assertEqual(self.plugin.dataset(1)["name"], "B")
        self.assertEqual(len(self.host.docs), 1)

    def test_lost_fields(self):
        # The retried enrolment's link field write fails: the differing records still conflict.
        self.team.add(1, title="A")
        self.host.trap = trap("create", 1, "lose")
        self.plugin.manual({"experiment": 1})
        self.team.items[1]["title"] = "B"
        self.team.locked.add(1)
        self.assertEqual(self.sync()["kind"], "failed")
        self.team.locked.clear()
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertEqual(self.plugin.dataset(1)["name"], "A")

    def test_lost_resolve(self):
        self.team.add(1, title="A")
        self.host.trap = trap("create", 1, "lose")
        self.plugin.manual({"experiment": 1})
        self.team.items[1]["title"] = "B"
        self.assertEqual(self.sync()["kind"], "conflict")
        self.assertIsNone(self.plugin.manual({"resolve": 1, "keep": "aruna"})["mark"])
        self.assertEqual(self.team.items[1]["title"], "A")

    def test_enrolment_stops(self):
        for name, count in (("create", 1), ("save", 2)):
            with self.subTest(name):
                self.setUp()
                self.team.add(1, title="A")
                self.host.trap = trap(name, count, "kill", None if name == "create" else lambda args: args[0] == "link/1")
                self.assertEqual(self.plugin.manual({"experiment": 1}), "killed")
                self.assertEqual(self.plugin.link(1)["pending"], "enrol")
                self.assertIsNone(self.sync())
                self.assertEqual(len(self.host.docs), 1)
                self.assertEqual(self.plugin.dataset(1)["name"], "A")


if __name__ == "__main__":
    unittest.main()
