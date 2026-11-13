"""eLabFTW API v2 calls through the `elab` connection, and the size of eLabFTW's JSON answers."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json

LIMIT = (1 << 20) - (16 << 10)
PAGE = 50
# Symfony's JsonResponse writes these as six-byte \u00XX escapes, where json.dumps adds less.
WIDER = {"<": 5, ">": 5, "&": 5, "'": 5, '"': 4, "/": 1}


class Fault(Exception):
    """A failed call with a host error kind; `refused` is an eLabFTW answer that changed nothing."""

    def __init__(self, kind, message, refused=False):
        super().__init__(message)
        self.kind, self.message, self.refused = kind, message, refused


class Stop(Exception):
    """The run's budget or time is used up; the walk saves its place and ends."""


def encoded(text):
    """Bytes of `text` as a JSON string in eLabFTW's answers."""
    return len(json.dumps(text)) + sum(WIDER.get(char, 0) for char in text)


def fetch(host, method, path="", query=(), body=None):
    """Sends one request and returns the parsed answer and its size, or raises a `Fault`."""
    data = json.dumps(body).encode() if body is not None else b""
    if len(data) > LIMIT:
        raise Fault("limit", f"an eLabFTW request of {len(data)} bytes is too large")
    status, reply = host.elab(method, path, list(query), data)
    if 200 <= status < 300:
        return (json.loads(reply) if reply else None), len(reply)
    text = reply[:200].decode(errors="replace")
    if status == 404:
        kind = "not_found"
    elif status in (401, 403):
        kind = "denied"
    elif status >= 500 or "being edited" in text:
        kind = "unavailable"
    else:
        kind = "invalid"
    raise Fault(kind, f"eLabFTW answered {status}: {text}", refused=status < 500)


def call(host, method, path="", query=(), body=None):
    return fetch(host, method, path, query, body)[0]


def experiment(host, number):
    """The experiment with its answer size in `_size`, or None when it is missing or hidden."""
    try:
        found, size = fetch(host, "GET", str(number))
    except Fault as fault:
        if fault.kind in ("not_found", "denied"):
            return None
        raise
    found["_size"] = size
    return found


def uploads(host, number):
    return call(host, "GET", f"{number}/uploads") or []


def tagged(host, tag, offset):
    """One page of experiments with the tag, in id order."""
    query = [("tags[]", tag), ("order", "id"), ("sort", "asc"), ("limit", str(PAGE))]
    return call(host, "GET", "", query + [("offset", str(offset))]) or []


def patch(host, number, fields):
    """Changes the experiment; eLabFTW answers the whole experiment."""
    return call(host, "PATCH", str(number), body=fields)


def add_tag(host, number, tag):
    call(host, "POST", f"{number}/tags", body={"tag": tag})


def remove_tag(host, number, tag):
    return call(host, "PATCH", f"{number}/tags/{tag}", body={"action": "unreference"})


def estimate(found, body, title, tags):
    """A plain bound of the experiment answer once body, title and tags are written."""
    old = encoded(found.get("body") or "") + encoded(found.get("body_html") or "")
    return found["_size"] - old + 2 * encoded(body) + encoded(title) + encoded("|".join(tags))
