"""Tests services, secrets and state against the fake host: whole answers, a status outside 2xx,
readable secrets, and compare-and-swap writes retried by `update`."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import unittest

import aruna
from aruna.testing import Fake


class ServicesTest(unittest.TestCase):
    def setUp(self):
        self.fake = Fake()

    def test_services_and_secrets(self):
        self.fake.reply("catalog", json={"id": "c-1"}, status=201)
        self.fake.reply("catalog", "down", status=500, body=b"broken")
        catalog = aruna.service("catalog")
        record = catalog.post(json={"title": "Run 7"})
        self.assertEqual((record.status, record["id"], record.header("content-type")), (201, "c-1", "application/json"))
        self.assertEqual(self.fake.calls[-1], ("send", "catalog", "post", "", b'{"title": "Run 7"}'))
        with self.assertRaises(aruna.ServiceError) as raised:
            catalog.get("down")
        self.assertEqual(raised.exception.status, 500)
        self.assertEqual(catalog.get("down", check=False).text, "broken")
        self.fake.secrets["signing"] = {"key": "s3cret"}
        self.assertEqual(aruna.secret("signing"), {"key": "s3cret"})
        with self.assertRaises(aruna.NotAllowed):
            aruna.secret("other")

    def test_repeated_headers(self):
        links = [("Link", '<https://c.example.org/2>; rel="next"'), ("link", '<https://c.example.org/9>; rel="last"')]
        self.fake.reply("catalog", "records", json=[], headers=links)
        reply = aruna.service("catalog").get("records")
        self.assertEqual(reply.headers, links)
        self.assertEqual(reply.header("LINK"), links[0][1])
        self.assertIsNone(reply.header("etag"))
        aruna.on("webhook")(lambda request: [request.headers, request.header("x-tag")])
        pushed = self.fake.webhook(headers=[("X-Tag", "a"), ("x-tag", "b")])
        self.assertEqual(pushed, [[["X-Tag", "a"], ["x-tag", "b"]], "a"])

    def test_state(self):
        state = aruna.state
        self.assertIsNone(state.get("cursor"))
        state["cursor"] = 1
        self.assertEqual(state["cursor"], 1)
        state["cursor"] = 2
        self.fake.values["cursor"] = (99, b"7")
        with self.assertRaises(aruna.Conflict):
            state["cursor"] = 3
        state.forget()
        with self.assertRaises(aruna.Conflict, msg="an unread key is new"):
            state["cursor"] = 3
        self.assertEqual(state.update("seen", lambda seen: seen + 1, 0), 1)
        del state["cursor"]
        self.assertNotIn("cursor", state)
        del state["cursor"]
        first = [True]

        def change(names):
            if first:
                # Another run writes between this read and the write.
                first.pop()
                self.fake.values["list"] = (50, b'["other"]')
            return names + ["mine"]

        state.forget()
        self.assertEqual(state.update("list", change, []), ["other", "mine"])


if __name__ == "__main__":
    unittest.main()
