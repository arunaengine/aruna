"""Runs a plugin's handlers in plain Python against a fake host, for tests of the plugin."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
import tomllib

from . import _host, _run
from .errors import Conflict, NotAllowed, NotFound


class Fake:
    """A fake host: what the plugin logged, and runs of its handlers by trigger. Give `manifest`,
    a plugin.toml path, so the form's `dataset` and `object` fields load as they do on the node.
    Datasets, objects, service answers, secrets and state live in its dicts."""

    #: Items of one page of a listing.
    page = 2

    def __init__(self, settings=None, manifest=None):
        self.settings = dict(settings or {})
        self.lines, self.calls, self.runs = [], [], 0
        self.datasets, self.objects, self.replies, self.secrets = {}, {}, {}, {}
        self.values, self.versions = {}, 0
        _run.fields.clear()
        if manifest is not None:
            with open(manifest, "rb") as file:
                form = tomllib.load(file).get("manual", {})
            for name, field in form.items():
                kind = field.get("type", "string") if isinstance(field, dict) else "string"
                _run.fields[name] = kind
        _host.use(self)

    def dataset(self, id, **entity):
        """Stores dataset `id` at revision 1."""
        self.datasets[id] = {"revision": 1, "entity": entity, "files": []}

    def object(self, path, content=b"", version="v1", marked=False):
        self.objects[path] = (content, version, marked)

    def reply(self, service, path="", status=200, json=None, body=b""):
        """What `service` answers for `path`."""
        self.replies[(service, path)] = (status, body if json is None else _json(json))

    def log(self, level, message):
        self.lines.append((level, message))

    def _paged(self, items, cursor):
        start = int(cursor or 0)
        following = start + self.page
        return items[start:following], (str(following) if following < len(items) else None)

    def _stored(self, id):
        if id not in self.datasets:
            raise NotFound(f"no dataset {id}")
        return self.datasets[id]

    def _write(self, id, expected, change):
        stored = self._stored(id)
        if str(stored["revision"]) != expected:
            raise Conflict(f"dataset {id} changed meanwhile")
        change(stored)
        stored["revision"] += 1
        return str(stored["revision"])

    def dataset_get(self, id, whole):
        stored = self._stored(id)
        entity = dict(stored["entity"])
        if whole:
            entity["hasPart"] = [{"@id": file["@id"]} for file in stored["files"]]
        return str(stored["revision"]), _json(entity).decode()

    def dataset_list(self, cursor):
        return self._paged(sorted(self.datasets), cursor)

    def dataset_files(self, id, cursor):
        return self._paged([_json(file).decode() for file in self._stored(id)["files"]], cursor)

    def dataset_patch(self, id, patch, expected):
        changes = json.loads(patch)
        self.calls.append(("patch", id, changes))

        def apply(stored):
            for name, value in changes.items():
                if value is None:
                    stored["entity"].pop(name, None)
                else:
                    stored["entity"][name] = value

        return self._write(id, expected, apply)

    def file_put(self, id, file, expected):
        file = json.loads(file)

        def put(stored):
            stored["files"] = [kept for kept in stored["files"] if kept["@id"] != file["@id"]] + [file]

        return self._write(id, expected, put)

    def file_remove(self, id, file, expected):
        def remove(stored):
            stored["files"] = [kept for kept in stored["files"] if kept["@id"] != file]

        return self._write(id, expected, remove)

    def dataset_create(self, path, dataset):
        id = f"D-{path}"
        if id not in self.datasets:
            self.dataset(id, **json.loads(dataset))
        return id, "1"

    def object_info(self, path):
        if path not in self.objects:
            raise NotFound(f"no object at {path}")
        content, version, marked = self.objects[path]
        return {"path": path, "size": len(content), "version": version, "marked": marked}

    def object_list(self, prefix, cursor):
        infos = [self.object_info(path) for path in sorted(self.objects) if path.startswith(prefix)]
        return self._paged(infos, cursor)

    def object_read(self, path, version):
        if self.object_info(path)["marked"]:
            raise NotAllowed("this object was written by a job with a secret; plugins cannot read it")
        return self.objects[path][0]

    def send(self, service, method, path, query, headers, body):
        self.calls.append(("send", service, method, path, body))
        status, answer = self.replies.get((service, path), (404, b"not found"))
        return status, [("Content-Type", "application/json")], answer

    def secret(self, name):
        if name not in self.secrets:
            raise NotAllowed(f"the install may not read the secret {name}")
        return list(self.secrets[name].items())

    def state_get(self, key):
        return self.values.get(key)

    def state_put(self, key, value, expected):
        if (self.values[key][0] if key in self.values else None) != expected:
            raise Conflict(f"the value of {key} changed meanwhile")
        self.versions += 1
        self.values[key] = (self.versions, bytes(value))
        return self.versions

    def state_remove(self, key, expected):
        if key not in self.values or self.values[key][0] != expected:
            raise Conflict(f"the value of {key} changed meanwhile")
        del self.values[key]

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


def _json(value):
    return json.dumps(value, sort_keys=True).encode()
