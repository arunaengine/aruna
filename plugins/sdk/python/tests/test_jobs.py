"""Tests jobs against the fake host: a copy answers its job at once, a job by id with its upload
answer and report, outputs, cancel, and the finished job as the subject of `job_finished`."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import unittest

import aruna
from aruna.testing import Fake


class JobsTest(unittest.TestCase):
    def setUp(self):
        self.fake = Fake()

    def test_copy_starts(self):
        job = aruna.objects.copy("in/a", "out/a", version="v7")
        self.assertEqual((job.id, job.kind, job.path, job.state, job.ok), ("job-1", "copy", "out/a", "queued", False))
        self.assertEqual(self.fake.calls, [("copy", "in/a", "v7", "out/a")], "a start makes no other call")

    def test_job_by_id(self):
        reply = (201, [("ETag", "e1")], b'{"checksum": "md5:1"}')
        self.fake.job("j1", kind="upload", path="raw/a", reply=reply, report='{"reads": 9}', outputs=["out/0", "out/1", "out/2"])
        job = aruna.jobs.get("j1")
        self.assertTrue(job.ok)
        self.assertEqual((job.reply.status, job.reply.header("etag"), job.reply["checksum"]), (201, "e1", "md5:1"))
        self.assertEqual(job.report_json(), {"reads": 9})
        self.assertEqual([output.path for output in job.outputs()], ["out/0", "out/1", "out/2"])
        job.cancel()
        self.fake.job("j1", state="cancelled")
        job.refresh()
        self.assertEqual((job.state, job.ok, job.reply), ("cancelled", False, None))
        with self.assertRaises(aruna.NotFound):
            aruna.jobs.get("j2")

    def test_finished_subject(self):
        self.fake.job("j1", state="failed", message="the source is gone")
        aruna.on("job_finished")(lambda job: [job.id, job.ok, job.message])
        self.assertEqual(self.fake.event("job_finished", "j1", revision="failed"), ["j1", False, "the source is gone"])


    def test_transfers_start(self):
        ena = aruna.service("ena")
        job = ena.download("files/r.fq", to="raw/r.fq", size_limit=9, checksums={"md5": "00"})
        self.assertEqual((job.id, job.kind, job.path, job.state), ("job-1", "download", "raw/r.fq", "queued"))
        self.assertEqual(self.fake.calls[-1], ("download", "ena", "files/r.fq", "raw/r.fq", 9, {"md5": "00"}))
        zenodo = aruna.service("zenodo")
        job = zenodo.upload("raw/r.fq", "deposit", method="POST", file_field="file", fields={"title": "Run 7"})
        self.assertEqual((job.kind, job.path), ("upload", "raw/r.fq"))
        sent = ("upload", "zenodo", "POST", "deposit", "raw/r.fq", None, "file", {"title": "Run 7"})
        self.assertEqual(self.fake.calls[-1], sent)


if __name__ == "__main__":
    unittest.main()
