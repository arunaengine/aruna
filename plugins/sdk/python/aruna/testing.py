"""Runs a plugin's handlers in plain Python against a fake host, for tests of the plugin."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
import tomllib

from . import _host, _run


class Fake:
    """A fake host: what the plugin logged, and runs of its handlers by trigger. Give `manifest`,
    a plugin.toml path, so the form's `dataset` and `object` fields load as they do on the node."""

    def __init__(self, settings=None, manifest=None):
        self.settings = dict(settings or {})
        self.lines = []
        self.runs = 0
        _run.fields.clear()
        if manifest is not None:
            with open(manifest, "rb") as file:
                form = tomllib.load(file).get("manual", {})
            for name, field in form.items():
                kind = field.get("type", "string") if isinstance(field, dict) else "string"
                _run.fields[name] = kind
        _host.use(self)

    def log(self, level, message):
        self.lines.append((level, message))

    def _invoke(self, trigger, payload):
        self.runs += 1
        call = {"run": f"run-{self.runs}", "install": "install-1", "group": "group-1",
                "settings": json.dumps(self.settings), "trigger": trigger, "payload": payload}
        output = _run.dispatch(call)
        return None if output is None else json.loads(output)

    def manual(self, **form):
        return self._invoke("manual", json.dumps(form))

    def timer(self, tick_ms):
        return self._invoke("timer", tick_ms)

    def webhook(self, body=b"", headers=(), method="POST", query="", delivery_id=None):
        return self._invoke("webhook", (method, query, list(headers), body, delivery_id))

    def event(self, kind, resource, path="", revision=""):
        return self._invoke(kind, {"kind": kind, "path": path, "resource": resource, "revision": revision})
