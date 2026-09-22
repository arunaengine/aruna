"""Focused checks for content validation and the PoC HTTP boundary."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aiohttp.test_utils import TestClient, TestServer

from arc import inspect_arc, pointer, scaffold
from bridge import application
from receive import receive
from store import Store, object_id


class ArcTests(unittest.TestCase):
    def test_scaffold_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold(directory)
            root = Path(directory)
            self.assertEqual(len(list(root.rglob("*.xlsx"))), 3)
            metadata = inspect_arc(root)
            investigation = next(item for item in metadata["@graph"] if item["@id"] == "./")
            investigation["name"] = "different investigation"
            (root / "ro-crate-metadata.json").write_text(json.dumps(metadata))
            with self.assertRaisesRegex(ValueError, "disagree"):
                inspect_arc(root)

    def test_missing_workbook(self):
        with tempfile.TemporaryDirectory() as directory:
            scaffold(directory)
            (Path(directory) / "assays/assay/isa.assay.xlsx").unlink()
            with self.assertRaises(ValueError):
                inspect_arc(directory)

    def test_pointer_validation(self):
        payload = b"version https://git-lfs.github.com/spec/v1\noid sha256:" + b"a" * 64 + b"\nsize 42\n"
        self.assertEqual(pointer(payload), ("a" * 64, 42))
        self.assertIsNone(pointer(b"normal file"))
        with self.assertRaises(ValueError):
            pointer(payload.replace(b"size 42", b"size -1"))
        with self.assertRaises(ValueError):
            object_id("../" + "a" * 64)

    def test_digest_rejected(self):
        store = Store.__new__(Store)
        with self.assertRaisesRegex(ValueError, "digest"):
            store.put("a" * 64, io.BytesIO(b"wrong bytes"), 11)

    def test_ref_limit(self):
        with self.assertRaisesRegex(ValueError, "ref update list"):
            receive("x" * 65537, None)

    def test_binding_preserved(self):
        config = {"ARUNA_BUCKET": "private", "ARUNA_GROUP_ID": "group", "ARUNA_TOKEN": "token",
                  "ARUNA_API_URL": "http://127.0.0.1:1", "ARUNA_S3_URL": "http://127.0.0.1:2",
                  "AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"}
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, config), patch("store.boto3.client"):
            Store(directory, "crate")
            Store(directory, "crate")
            with self.assertRaisesRegex(ValueError, "different Aruna repository"):
                Store(directory, "another")


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.token = "test-operator-token-at-least-24-characters"
        self.store = type("StoreFixture", (), {"name": "crate", "find": lambda _, oid: None})()
        with patch("bridge.initialize"):
            self.client = TestClient(TestServer(application(self.store, self.token)))
        self.addAsyncCleanup(self.client.close)
        await self.client.start_server()

    async def test_authentication(self):
        response = await self.client.get("/status")
        self.assertEqual(response.status, 401)
        response = await self.client.get("/status", headers={"Authorization": "Basic not-base64!"})
        self.assertEqual(response.status, 401)

    async def test_missing_download(self):
        response = await self.client.post("/crate.git/info/lfs/objects/batch",
                                         headers={"Authorization": "Bearer " + self.token},
                                         json={"operation": "download", "objects": [{"oid": "a" * 64, "size": 7}]})
        self.assertEqual(response.status, 200)
        result = await response.json()
        self.assertEqual(result["objects"][0]["error"]["code"], 404)

    async def test_invalid_batch(self):
        for body in ([], {"operation": "delete", "objects": []},
                     {"operation": "upload", "transfers": "basic", "objects": []},
                     {"operation": "upload", "objects": [{"oid": "a" * 64, "size": -1}]},
                     {"operation": "upload", "objects": [{"oid": "../bad", "size": 1}]}):
            response = await self.client.post("/crate.git/info/lfs/objects/batch",
                                             headers={"Authorization": "Bearer " + self.token}, json=body)
            self.assertEqual(response.status, 422)

    async def test_unknown_object(self):
        response = await self.client.get("/lfs/" + "a" * 64,
                                         headers={"Authorization": "Bearer " + self.token})
        self.assertEqual(response.status, 404)


if __name__ == "__main__":
    unittest.main()
