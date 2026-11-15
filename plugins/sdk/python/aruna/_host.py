"""The host calls the library makes: the component's imports, or a fake from aruna.testing."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

from .errors import BY_KIND

_current = None


def use(host):
    """Puts `host` in place of the component's imports."""
    global _current
    _current = host


def host():
    """The host calls go to: a fake when one is in use, else the component's imports."""
    global _current
    if _current is None:
        _current = Imports()
    return _current


def raised(error):
    """The library's error for a host error a binding raised."""
    value = getattr(error, "value", None)
    kind = getattr(getattr(value, "kind", None), "name", None)
    if kind is None:
        return error
    name = kind.lower().replace("_", "-")
    return BY_KIND.get(name, BY_KIND["invalid"])(value.message)


class Imports:
    """The component's own imports, in the plain values the library uses."""

    def __init__(self):
        from wit_world import imports

        self.imports = imports

    def call(self, function, *args):
        from componentize_py_types import Err

        try:
            return function(*args)
        except Err as error:
            raise raised(error) from None

    def log(self, level, message):
        log = self.imports.log
        self.imports.log.log(getattr(log.Level, level.upper()), message)
