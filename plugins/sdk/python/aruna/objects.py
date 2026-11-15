"""Objects of the install's group on its node, named by bucket paths such as `raw-data/run-7/x.gz`:
information, listings and the content of small ones."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json

from ._host import host
from .datasets import _pages


class Object:
    """One object version. A `marked` version was written by a job with a secret: a plugin cannot
    read, copy or upload it, though it may still link it into a dataset."""

    def __init__(self, path, size, version, marked=False):
        self.path, self.size, self.version, self.marked = path, size, version, marked

    @property
    def url(self):
        """The canonical address, `s3://bucket/key`."""
        return f"s3://{self.path}"

    def read(self):
        """The bytes of this version, at most 1 MiB; a larger object is for a task."""
        return host().object_read(self.path, self.version or None)

    def read_text(self):
        return self.read().decode()

    def read_json(self):
        return json.loads(self.read())

    def __repr__(self):
        return f"Object({self.path!r}, version={self.version!r})"


def get(path):
    """The current version of the object at `path`."""
    return Object(**host().object_info(path))


def list(prefix):
    """Every object below `prefix` the install may read, one page at a time."""
    pages = _pages(lambda cursor: host().object_list(prefix, cursor))
    return (Object(**info) for info in pages)
