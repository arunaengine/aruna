"""The Python library for Aruna plugins: register handlers by trigger with `@aruna.on`, and reach
datasets, objects, services, jobs and state through module functions; every check stays on the node."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

from . import datasets, objects
from ._run import Form, Request, SyncEvent, _field_loaders, _loaders, fields, log, on, run, settings
from .datasets import Dataset
from .objects import Object
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

_loaders.update(dataset_created=lambda event: datasets.get(event["resource"]),
                dataset_updated=lambda event: datasets.get(event["resource"]),
                data_uploaded=lambda event: objects.get(event["resource"]))
_field_loaders.update(dataset=datasets.get, object=objects.get)

try:
    from ._fields import FIELDS

    fields.update(FIELDS)
except ImportError:
    pass

__all__ = [
    "ArunaError", "Conflict", "Dataset", "Fail", "Form", "InvalidCall", "InvalidInput",
    "LimitReached", "NotAllowed", "NotFound", "Object", "Request", "Retry", "ServiceError",
    "SyncEvent", "Unavailable", "UnknownOutcome", "datasets", "log", "objects", "on", "run",
    "settings",
]
