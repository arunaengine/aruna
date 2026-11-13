"""The walk: link lookup by ULID, cycles, stops, removal, the live offset, budget and report."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
import re
import unittest

from elab import Fault
from tests.fake import MIB, Plugin, trap

CONFLICT = {"kind": "conflict", "reason": "changes on both sides: a manual run picks a side"}


class WalkTests(unittest.TestCase):
    def setUp(self):
        self.plugin = Plugin()
        self.team, self.host = self.plugin.elab, self.plugin.host

    def tagged(self, *numbers, **given):
        for number in numbers:
            self.team.add(number, title=f"T{number}", **given)
        return self.plugin.poll()

    def dataset(self, number):
        return self.plugin.link(number)["dataset"]

    def marks(self, *numbers):
        return [(self.plugin.link(number)["mark"] or {}).get("kind") for number in numbers]

    def report(self):
        pages, after = [], None
        while True:
            found = self.plugin.manual({"report": True, "next": after})
            self.assertLess(len(json.dumps(found)), 64 << 10)
            pages.append(found["marks"])
            after = found["next"]
            if after is None:
                return pages

    def test_lookup(self):
        self.tagged(1)
        dataset = self.dataset(1)
        self.assertRegex(dataset, r"^[0-9A-HJKMNP-TV-Z]{26}$")
        self.host.edit(dataset, name="B")
        self.assertIsNone(self.plugin.changed(dataset)["mark"])
        self.assertEqual(self.team.items[1]["title"], "B")
        copy = self.host.make({"name": "copy", "identifier": {"elabftw": {"id": "1"}}})
        self.assertEqual(self.plugin.changed(copy)["mark"]["kind"], "unlinked")
        self.assertEqual(self.dataset(1), dataset)
        self.host.edit(dataset, identifier={"elabftw": {"id": "99"}})
        self.plugin.poll()
        self.assertEqual(self.host.docs[dataset]["json"]["identifier"]["elabftw"]["id"], "1")

    def test_aruna_dataset(self):
        made = self.host.make({"name": "Made in Aruna"})
        self.tagged(1)
        self.assertEqual([path for _, path in self.team.sent if path not in ("", "1", "1/uploads")], [])
        self.assertEqual(self.report(), [[{"dataset": made, "mark": {"kind": "unlinked", "reason": "no experiment is linked"}}]])
        self.host.bucket = False
        self.assertRaises(Fault, self.plugin.poll)

    def test_identifiers(self):
        # Other identifier shapes mean an unlinked dataset; they never stop the walk.
        doi = self.host.make({"name": "x", "identifier": "doi:10.1234/example"})
        bare = self.host.make({"name": "y", "identifier": {"elabftw": "12"}})
        self.tagged(1)
        self.assertEqual(self.marks(1), [None])
        entries = sum(self.report(), [])
        self.assertEqual([entry["dataset"] for entry in entries], [doi, bare])

    def test_cycles(self):
        self.assertEqual(self.tagged(1, 2, 3, 4, 5), {"cycle": 2, "visits": 5})
        self.team.sent.clear()
        self.assertEqual(self.plugin.poll(), {"cycle": 3, "visits": 5})
        self.assertEqual(sorted(path for _, path in self.team.sent if path.isdigit()), list("12345"))
        self.assertEqual({self.plugin.link(number)["seen"] for number in range(1, 6)}, {2})

    def test_backlog(self):
        self.host.tick = 1.0
        for number in range(1, 13):
            self.team.add(number)
        linked = []
        for _ in range(12):
            self.plugin.poll()
            linked.append(sum(1 for number in range(1, 13) if self.plugin.link(number)))
            if linked[-1] == 12:
                break
        self.assertEqual(linked[-1], 12)
        self.assertEqual(linked, sorted(set(linked)))

    def test_retry(self):
        self.tagged(1)
        self.team.locked.add(1)
        self.host.edit(self.dataset(1), name="B")
        self.plugin.poll()
        self.assertEqual(self.marks(1), ["failed"])
        self.team.locked.clear()
        self.plugin.poll()
        self.assertEqual((self.marks(1), self.team.items[1]["title"]), ([None], "B"))

    def test_hard_stop(self):
        self.tagged(1, 2, 3, 4)
        self.host.trap = (lambda name, args: name == "elab" and args[1] == "2", "kill", "always")
        self.assertEqual(self.plugin.poll(), "killed")
        self.assertEqual(self.host.value("walk")["current"], 2)
        for _ in range(4):
            self.plugin.poll()
        self.assertEqual(self.marks(1, 2, 3, 4), [None, "failed", None, None])
        self.assertIn("run stopped", self.plugin.link(2)["mark"]["reason"])
        self.assertGreaterEqual(min(self.plugin.link(number)["seen"] for number in (1, 3, 4)), 3)
        self.host.trap = None
        self.plugin.poll()
        self.plugin.poll()
        self.assertEqual(self.marks(2), [None])

    def test_completed_stop(self):
        # Review r3: the run ends after link 1's record save and before the next cursor save.
        self.tagged(1, 2)
        self.host.trap = trap("save", 1, "kill", lambda args: args[0] == "walk" and '"current": 2' in args[1])
        self.assertEqual(self.plugin.poll(), "killed")
        self.assertEqual(self.host.value("walk")["current"], 1)
        self.plugin.poll()
        self.assertEqual(self.marks(1, 2), [None, None])
        self.assertIsNone(self.host.value("walk")["current"])

    def test_caught_stop(self):
        for number in range(1, 7):
            self.team.add(number)
        while not all(self.plugin.link(number) for number in range(1, 7)):
            self.plugin.poll()
        self.host.tick = 5.0
        self.plugin.poll()
        cursor = self.host.value("walk")
        self.assertIsNone(cursor["current"])
        place = int(cursor["page"] or 0) + cursor["index"]
        seen = [self.plugin.link(number)["seen"] for number in range(1, 7)]
        self.assertEqual(seen[:place], [cursor["cycle"]] * place)
        self.assertLess(seen[place], cursor["cycle"])
        self.host.tick = 0.0
        self.plugin.poll()
        self.assertEqual(self.plugin.link(place + 1)["seen"], cursor["cycle"])

    def test_removal(self):
        self.tagged(1, 2, 3, 4)
        del self.team.items[1]
        self.team.items[2]["state"] = 3
        self.team.items[3]["tags"].remove("aruna")
        self.host.delete(self.dataset(4))
        self.plugin.poll()
        found = [self.plugin.link(number)["mark"] for number in range(1, 5)]
        self.assertEqual([(mark["kind"], mark["reason"]) for mark in found], [
            ("gone", "experiment"), ("gone", "experiment"), ("gone", "tag"), ("gone", "dataset")])

    def test_live_offset(self):
        # Review r2: a dataset deleted between listing pages shifts the offset past dataset 4.
        self.tagged(1, 2, 3, 4, 5, 6)
        gone = self.dataset(2)
        self.host.trap = trap("listing", 1, lambda: self.host.delete(gone), lambda args: args == "3")
        self.team.sent.clear()
        self.plugin.poll()
        self.assertGreater(self.team.sent.index(("GET", "4")), self.team.sent.index(("GET", "")))
        self.assertEqual(self.marks(4), [None])
        self.assertEqual(self.plugin.link(4)["seen"], self.plugin.link(5)["seen"])
        self.plugin.poll()
        self.assertEqual(self.marks(2, 4), ["gone", None])
        listed = [entry["experiment"] for entry in sum(self.report(), [])]
        self.assertEqual(listed, [2])

    def test_budget(self):
        tags = ("aruna", *(f"t{index}" for index in range(60)))
        # Four links copy about 2 MiB each, so the 60 tag removals (about 9.5 MiB) wait a run.
        for number in (1, 2, 3, 4):
            self.team.add(number, body="<p>" + "y" * 350_000 + "</p>")
        self.team.add(5, body="<p>" + "x" * 80_000 + "</p>", tags=tags)
        while not all(self.plugin.link(number) for number in range(1, 6)):
            self.plugin.poll()
        self.assertEqual(self.marks(1, 2, 3, 4, 5), [None] * 5)
        for number in (1, 2, 3, 4):
            self.team.items[number]["body"] = "<p>" + "z" * 350_000 + "</p>"
        self.host.edit(self.dataset(5), keywords=None)
        self.plugin.poll()
        self.assertEqual(len(self.team.items[5]["tags"]), 61)
        self.assertEqual(self.plugin.dataset(4)["text"][3], "z")
        self.assertIsNone(self.host.value("walk")["current"])
        self.plugin.poll()
        self.assertEqual((self.team.items[5]["tags"], self.marks(5)), (["aruna"], [None]))
        self.assertFalse(self.host.broken)
        self.assertLessEqual(self.host.peak, 16 * MIB)

    def test_wide_text(self):
        # Non-ASCII text costs six bytes per character in the JSON the host receives.
        for number in range(1, 15):
            self.team.add(number, body="<p>" + "é" * 60_000 + "</p>")
        for _ in range(6):
            self.plugin.poll()
        self.assertEqual(self.marks(*range(1, 15)), [None] * 14)
        self.assertFalse(self.host.broken)

    def test_budget_fresh(self):
        tags = ("aruna", *(f"t{index}" for index in range(120)))
        self.tagged(1, body="<p>" + "x" * 80_000 + "</p>", tags=tags)
        self.host.edit(self.dataset(1), keywords=None)
        self.plugin.poll()
        self.assertEqual(self.marks(1), ["too_large"])
        self.assertEqual(len(self.team.items[1]["tags"]), 121)
        self.assertFalse(self.host.broken)

    def test_report_pages(self):
        self.plugin = Plugin(page=50)
        self.host = self.plugin.host
        for number in range(1, 401):
            dataset = self.host.make({"identifier": {"elabftw": {"id": str(number)}}})
            self.host.state[f"link/{number}"] = (1, json.dumps({"dataset": dataset, "mark": CONFLICT}))
        pages = self.report()
        self.assertGreater(len(pages), 1)
        self.assertEqual(sorted(entry["experiment"] for entry in sum(pages, [])), list(range(1, 401)))

    def test_report_enrolments(self):
        # Review r4: a create that committed before its ULID was saved, then a removed tag.
        self.team.add(1)
        self.team.add(2)
        self.host.state["link/1"] = (1, json.dumps({"pending": "enrol"}))
        self.host.trap = trap("create", 1, "lose", lambda args: args[0] == "elabftw/2")
        self.assertEqual(self.plugin.manual({"experiment": 2}), "killed")
        created = self.host.keys["elabftw/2"]
        entries = sum(self.report(), [])
        unfinished = sorted(entry["experiment"] for entry in entries if entry["mark"]["kind"] == "unfinished")
        self.assertEqual(unfinished, [1, 2])
        self.assertIn({"dataset": created, "mark": {"kind": "unlinked", "reason": "no experiment is linked"}}, entries)
        self.team.items[2]["tags"].remove("aruna")
        entries = sum(self.report(), [])
        self.assertEqual([entry.get("experiment") for entry in entries], [None, 1])
        self.team.tag(2, "aruna")
        self.plugin.poll()
        self.assertEqual(self.dataset(2), created)
        self.assertEqual(re.findall(r"unlinked", json.dumps(self.report())), [])


if __name__ == "__main__":
    unittest.main()
