"""The component's entry: imports the plugin module, which registers its handlers, and runs them."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import logging

import wit_world
from componentize_py_types import Err
from wit_world.imports import types

# The library's host calls find only the interface modules imported while the component is built.
from wit_world.imports import jobs, metadata_create, metadata_read, metadata_write  # noqa: F401
from wit_world.imports import log as _, objects_read, secrets, services, state  # noqa: F401

from . import _run, log

import plugin  # noqa: E402, F401  the author's module registers its handlers when imported


class _ToLog(logging.Handler):
    """Puts `logging` records into the run's log."""

    def emit(self, record):
        level = {"WARNING": "warn", "CRITICAL": "error"}.get(record.levelname, record.levelname.lower())
        getattr(log, level, log.info)(self.format(record))


logging.getLogger().addHandler(_ToLog())
logging.getLogger().setLevel(logging.INFO)

_KINDS = {
    "permanent": types.FailureKind.PERMANENT,
    "retryable": types.FailureKind.RETRYABLE,
    "invalid-input": types.FailureKind.INVALID_INPUT,
}


def _call(invocation):
    """An invocation in the plain form `_run.dispatch` takes."""
    trigger = invocation.trigger
    if isinstance(trigger, types.Trigger_Manual):
        name, payload = "manual", trigger.value
    elif isinstance(trigger, types.Trigger_Timer):
        name, payload = "timer", trigger.value
    elif isinstance(trigger, types.Trigger_Webhook):
        request = trigger.value.request
        headers = [(field.name, field.value) for field in request.headers]
        payload = (request.method, request.query, headers, bytes(request.body), trigger.value.delivery_id)
        name = "webhook"
    else:
        event = trigger.value
        name = event.kind
        payload = {"kind": event.kind, "path": event.path, "resource": event.resource, "revision": event.revision}
    return {"run": invocation.run, "install": invocation.install, "group": invocation.group,
            "settings": invocation.settings, "trigger": name, "payload": payload}


class WitWorld(wit_world.WitWorld):
    def handle(self, invocation):
        try:
            return _run.dispatch(_call(invocation))
        except Exception as error:
            kind, message = _run.outcome(error)
            raise Err(types.Failure(_KINDS[kind], message)) from None

    def triggers(self):
        return _run.triggers()
