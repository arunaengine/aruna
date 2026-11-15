"""Requests to the services the manifest declares, answered whole; the node adds their secrets."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json as jsonlib

from ._host import host
from .errors import ServiceError


class Reply:
    """A service's whole answer; `headers` holds every header as a name and value pair, in order,
    and `reply["id"]` reads a field of a JSON answer."""

    def __init__(self, status, headers, body):
        self.status, self.body = status, body
        self.headers = [(name, value) for name, value in headers]

    def header(self, name):
        """The value of the first header named `name`, in any case, or None."""
        return next((value for header, value in self.headers if header.lower() == name.lower()), None)

    @property
    def text(self):
        return self.body.decode()

    def json(self):
        return jsonlib.loads(self.body)

    def __getitem__(self, name):
        return self.json()[name]


class Service:
    """The service the manifest declares as `[services.<name>]`."""

    def __init__(self, name):
        self.name = name

    def request(self, method, path="", *, json=None, body=None, query=None, headers=None, check=True):
        """Sends one request to `path` below the service's address. A status outside 200 to 299
        raises `ServiceError` unless `check` is false."""
        headers = dict(headers or {})
        if json is not None:
            body = jsonlib.dumps(json, sort_keys=True).encode()
            headers.setdefault("content-type", "application/json")
        if isinstance(body, str):
            body = body.encode()
        query = _pairs(query)
        status, answer_headers, answer = host().send(self.name, method, path, query, _pairs(headers), body or b"")
        reply = Reply(status, answer_headers, answer)
        if check and not 200 <= status < 300:
            raise ServiceError(status, answer.decode(errors="replace")[:200])
        return reply

    def get(self, path="", **options):
        return self.request("get", path, **options)

    def head(self, path="", **options):
        return self.request("head", path, **options)

    def post(self, path="", **options):
        return self.request("post", path, **options)

    def put(self, path="", **options):
        return self.request("put", path, **options)

    def patch(self, path="", **options):
        return self.request("patch", path, **options)

    def delete(self, path="", **options):
        return self.request("delete", path, **options)


def _pairs(values):
    """A dict or list of pairs as a list of name and value pairs."""
    if not values:
        return []
    return [(str(name), str(value)) for name, value in (values.items() if isinstance(values, dict) else values)]


def service(name):
    return Service(name)


def secret(name):
    """The fields of a secret the manifest lists under `[secrets]`; they are the code's data then.
    Secrets of services are added by the node and never reach the code."""
    return dict(host().secret(name))
