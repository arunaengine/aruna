"""Tests handlers by trigger: each subject, the run and its settings, logs and printed lines, and
the outcome of every kind of error."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import unittest
from datetime import datetime, timezone

import aruna
from aruna import _run
from aruna.testing import Fake


class CoreTest(unittest.TestCase):
    def setUp(self):
        self.fake = Fake(settings={"limit": 3})

    def test_subjects(self):
        aruna.on("manual")(lambda form: {"n": form.n * 2, "missing": form.get("other", "-")})
        self.assertEqual(self.fake.manual(n=2), {"n": 4, "missing": "-"})
        aruna.on("timer")(lambda tick: tick.isoformat())
        self.assertEqual(self.fake.timer(1_500), datetime.fromtimestamp(1.5, tz=timezone.utc).isoformat())
        aruna.on("webhook")(lambda request: [request.json()["ref"], request.header("x-kind"), request.delivery_id])
        pushed = self.fake.webhook(body=b'{"ref": "main"}', headers=[("X-Kind", "push")], delivery_id="d1")
        self.assertEqual(pushed, ["main", "push", "d1"])
        aruna.on("sync_completed", "sync_failed")(lambda sync: [sync.ok, sync.relationship, sync.job])
        self.assertEqual(self.fake.event("sync_failed", "rel-1", revision="job-1"), [False, "rel-1", "job-1"])

    def test_run_and_settings(self):
        @aruna.on("manual")
        def seen():
            return [aruna.run.id, aruna.run.install, aruna.settings.limit, aruna.settings.get("none", 0)]

        self.assertEqual(self.fake.manual(), ["run-1", "install-1", 3, 0])
        with self.assertRaises(AttributeError):
            aruna.settings.other

    def test_logs_and_prints(self):
        @aruna.on("manual")
        def chatty():
            aruna.log.warn("slow")
            print("3 done")

        self.assertIsNone(self.fake.manual())
        self.assertEqual(self.fake.lines, [("warn", "slow"), ("info", "3 done")])

    def test_outcomes(self):
        cases = [
            (aruna.Retry("later"), "retryable"),
            (aruna.Unavailable("busy"), "retryable"),
            (aruna.InvalidInput("no"), "invalid-input"),
            (aruna.Fail("gave up"), "permanent"),
            (aruna.NotAllowed("the install may not write datasets/run-7"), "permanent"),
            (ValueError("bad"), "permanent"),
        ]
        for error, expected in cases:
            self.assertEqual(_run.outcome(error)[0], expected, error)
        self.assertEqual(_run.outcome(ValueError("bad"))[1], "ValueError: bad")
        with self.assertRaises(aruna.InvalidInput):
            self.fake.event("dataset_updated", "D1")
        aruna.on("manual")(lambda form: None)
        with self.assertRaises(aruna.InvalidInput):
            self.fake._invoke("manual", "[1]")


if __name__ == "__main__":
    unittest.main()
