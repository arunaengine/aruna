"""Calls plugin handlers by name: builds the context and event, and turns errors into failures."""

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


def handler(function: Callable) -> Callable:
    """Registers a function as the handler with its own name."""
    _handlers[function.__name__] = function
    return function


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
    def handle(self, handler: str, invocation: types.Invocation) -> str | None:
        function = _handlers.get(handler)
        if function is None:
            raise Err(types.Failure(types.FailureKind.INVALID_INPUT, f"{handler} is not a handler"))
        ctx = Context(invocation.run, invocation.install, invocation.group, json.loads(invocation.settings))
        try:
            result = function(ctx, _event(invocation.trigger))
        except Exception as error:
            raise Err(_failure(error))
        if result is None or isinstance(result, str):
            return result
        return json.dumps(result)
