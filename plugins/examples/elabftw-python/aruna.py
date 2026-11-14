"""Calls plugin handlers by trigger: builds the context and event, and turns errors into failures."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
from dataclasses import dataclass
from typing import Any, Callable

import wit_world
from componentize_py_types import Err
from wit_world.imports import types

_handlers: dict[str, Callable] = {}


@dataclass
class Context:
    run: str
    install: str
    group: str
    settings: dict


def on(*triggers: str) -> Callable:
    """Registers a function as the handler of the named triggers of plugin.toml."""

    def register(function: Callable) -> Callable:
        for trigger in triggers:
            _handlers[trigger] = function
        return function

    return register


def _trigger_name(trigger: Any) -> str:
    if isinstance(trigger, types.Trigger_Manual):
        return "manual"
    if isinstance(trigger, types.Trigger_Event):
        return trigger.value.kind
    if isinstance(trigger, types.Trigger_Webhook):
        return "webhook"
    return "timer"


def _event(trigger: Any) -> dict:
    if isinstance(trigger, types.Trigger_Manual):
        return {"kind": "manual", "input": json.loads(trigger.value) if trigger.value else None}
    if isinstance(trigger, types.Trigger_Event):
        return {"kind": "event", **vars(trigger.value)}
    if isinstance(trigger, types.Trigger_Webhook):
        return {"kind": "webhook", "delivery_id": trigger.value.delivery_id, "request": trigger.value.request}
    return {"kind": "timer", "tick": trigger.value}


def _failure(error: Exception) -> types.Failure:
    """A handler's own failure stays; an unavailable host is retryable, any other error permanent."""
    value = error.value if isinstance(error, Err) else None
    if isinstance(value, types.Failure):
        return value
    if isinstance(value, types.HostError):
        retry = value.kind == types.ErrorKind.UNAVAILABLE
        kind = types.FailureKind.RETRYABLE if retry else types.FailureKind.PERMANENT
        return types.Failure(kind, f"host call failed ({value.kind.name.lower()}): {value.message}")
    return types.Failure(types.FailureKind.PERMANENT, str(error if value is None else value))


class WitWorld(wit_world.WitWorld):
    def handle(self, invocation: types.Invocation) -> str | None:
        name = _trigger_name(invocation.trigger)
        function = _handlers.get(name)
        if function is None:
            raise Err(types.Failure(types.FailureKind.INVALID_INPUT, f"no handler for the trigger {name}"))
        ctx = Context(invocation.run, invocation.install, invocation.group, json.loads(invocation.settings))
        try:
            result = function(ctx, _event(invocation.trigger))
        except Exception as error:
            raise Err(_failure(error))
        if result is None or isinstance(result, str):
            return result
        return json.dumps(result)

    def triggers(self) -> list[str]:
        return sorted(_handlers)
