"""The run a handler serves: registration by trigger, the subject each handler gets, and dispatch."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import contextlib
import inspect
import json
from datetime import datetime, timezone

from .errors import ArunaError, Fail, InvalidInput
from ._host import host

_handlers = {}

#: The type of each `[manual]` field, written by the build next to the library.
fields = {}


def on(*triggers):
    """Registers a function as the handler of the named triggers; it takes the subject or nothing."""

    def register(function):
        for trigger in triggers:
            _handlers[trigger] = function
        return function

    return register


def triggers():
    return sorted(_handlers)


class Run:
    """Who runs the handler, and the raw event of an event trigger."""

    def __init__(self):
        self.id = self.install = self.group = ""
        self.event = None


run = Run()


class Settings:
    """The install's settings, by attribute or by name."""

    def __init__(self):
        self._values = {}

    def __getattr__(self, name):
        try:
            return self._values[name]
        except KeyError:
            raise AttributeError(f"the install has no setting {name!r}") from None

    def __getitem__(self, name):
        return self._values[name]

    def get(self, name, default=None):
        return self._values.get(name, default)

    def keys(self):
        return self._values.keys()


settings = Settings()


class Form:
    """The `[manual]` fields a person gave, defaults filled in; `dataset` and `object` fields loaded."""

    def __init__(self, values):
        self._values = values

    def __getattr__(self, name):
        try:
            return self._values[name]
        except KeyError:
            raise AttributeError(f"the form has no field {name!r}") from None

    def __getitem__(self, name):
        return self._values[name]

    def get(self, name, default=None):
        return self._values.get(name, default)

    def keys(self):
        return self._values.keys()


class Request:
    """A verified webhook request; authentication headers are already removed."""

    def __init__(self, method, query, headers, body, delivery_id):
        self.method, self.query, self.body, self.delivery_id = method, query, body, delivery_id
        self.headers = {name.lower(): value for name, value in headers}

    @property
    def text(self):
        return self.body.decode()

    def json(self):
        return json.loads(self.body)


class SyncEvent:
    """A sync relationship's job that completed or failed, below the source path."""

    def __init__(self, event):
        self.ok = event["kind"] == "sync_completed"
        self.path, self.relationship, self.job = event["path"], event["resource"], event["revision"]


def _subject(trigger, payload):
    """What the handler of `trigger` takes."""
    if trigger == "manual":
        values = json.loads(payload) if payload else {}
        if not isinstance(values, dict):
            raise InvalidInput("the form is not a JSON object")
        return Form(_loaded(values))
    if trigger == "timer":
        return datetime.fromtimestamp(payload / 1000, tz=timezone.utc)
    if trigger == "webhook":
        return Request(*payload)
    if trigger.startswith("sync_"):
        return SyncEvent(payload)
    loader = _loaders.get(trigger)
    if loader is None:
        return payload
    return loader(payload)


#: How an event trigger's subject is loaded from its raw event, by trigger.
_loaders = {}

#: How a typed form field is loaded from its value, by field type.
_field_loaders = {}


def _loaded(values):
    for name, kind in fields.items():
        loader = _field_loaders.get(kind)
        if loader is not None and isinstance(values.get(name), str):
            values[name] = loader(values[name])
    return values


def dispatch(call):
    """Runs the handler of one invocation and answers its JSON output, or None."""
    function = _handlers.get(call["trigger"])
    if function is None:
        raise InvalidInput(f"this plugin has no handler for the trigger {call['trigger']!r}")
    run.id, run.install, run.group = call["run"], call["install"], call["group"]
    run.event = call["payload"] if isinstance(call["payload"], dict) else None
    settings._values = json.loads(call["settings"] or "{}")
    for start in starts:
        start()
    # Printed lines go to the run's log, as aruna.log.info does.
    with contextlib.redirect_stdout(_Lines()):
        if inspect.signature(function).parameters:
            result = function(_subject(call["trigger"], call["payload"]))
        else:
            result = function()
    return None if result is None else json.dumps(result, sort_keys=True)


#: What every run starts with, such as forgetting the state versions of an earlier run.
starts = []


class _Lines:
    """A stream whose lines are info lines of the run's log."""

    def __init__(self):
        self.pending = ""

    def write(self, text):
        self.pending += text
        *lines, self.pending = self.pending.split("\n")
        for line in lines:
            host().log("info", line)
        return len(text)

    def flush(self):
        if self.pending:
            host().log("info", self.pending)
            self.pending = ""


def outcome(error):
    """The run outcome and message of an uncaught error."""
    if isinstance(error, ArunaError):
        return error.outcome, str(error)
    return Fail.outcome, f"{type(error).__name__}: {error}"


class Log:
    """Lines of the run's log."""

    def debug(self, message):
        host().log("debug", str(message))

    def info(self, message):
        host().log("info", str(message))

    def warn(self, message):
        host().log("warn", str(message))

    def error(self, message):
        host().log("error", str(message))


log = Log()
