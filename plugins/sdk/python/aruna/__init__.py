"""The Python library for Aruna plugins: register handlers by trigger with `@aruna.on`, and reach
datasets, objects, services, jobs and state through module functions; every check stays on the node."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

from ._run import Form, Request, SyncEvent, fields, log, on, run, settings
from .errors import (
    ArunaError,
    Conflict,
    Fail,
    InvalidCall,
    InvalidInput,
    LimitReached,
    NotAllowed,
    NotFound,
    Retry,
    ServiceError,
    Unavailable,
    UnknownOutcome,
)

try:
    from ._fields import FIELDS

    fields.update(FIELDS)
except ImportError:
    pass

__all__ = [
    "ArunaError", "Conflict", "Fail", "Form", "InvalidCall", "InvalidInput", "LimitReached",
    "NotAllowed", "NotFound", "Request", "Retry", "ServiceError", "SyncEvent", "Unavailable",
    "UnknownOutcome", "log", "on", "run", "settings",
]
