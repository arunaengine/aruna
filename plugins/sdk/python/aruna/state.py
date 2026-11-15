"""Small private values of the install as JSON, changed by compare and swap: a write uses the
version this run read, or makes a new value when the run did not read the key."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json

from ._host import host
from .errors import Conflict

#: How often `update` reads and writes again after a conflict.
TRIES = 5


class State:
    """`state.get(key, default)`, `state[key] = value`, `state.update(key, fn, default)`, `del state[key]`."""

    def __init__(self):
        self._read = {}

    def forget(self):
        self._read.clear()

    def get(self, key, default=None):
        """The value of `key`, remembering its version for a later write."""
        entry = host().state_get(key)
        self._read[key] = None if entry is None else entry[0]
        return default if entry is None else json.loads(entry[1])

    def __getitem__(self, key):
        value = self.get(key, _MISSING)
        if value is _MISSING:
            raise KeyError(key)
        return value

    def __contains__(self, key):
        return self.get(key, _MISSING) is not _MISSING

    def __setitem__(self, key, value):
        """Writes at the version this run read; a key it did not read must not exist yet."""
        text = json.dumps(value, sort_keys=True).encode()
        self._read[key] = host().state_put(key, text, self._read.get(key))

    def update(self, key, change, default=None):
        """Reads `key`, applies `change` and writes the result, again after a conflict up to five
        times; answers the value written."""
        for tried in range(TRIES):
            value = change(self.get(key, default))
            try:
                self[key] = value
                return value
            except Conflict:
                if tried + 1 == TRIES:
                    raise

    def __delitem__(self, key):
        """Removes `key` at the version this run read, reading it first when it did not."""
        if key not in self._read:
            self.get(key)
        version = self._read.get(key)
        if version is not None:
            host().state_remove(key, version)
        self._read[key] = None


_MISSING = object()
state = State()
