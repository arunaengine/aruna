"""The Python library for Aruna plugins: register handlers by trigger with `@aruna.on`, and reach
datasets, objects, services, jobs and state through module functions; every check stays on the node."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

from . import datasets, jobs, objects
from ._run import Form, Request, SyncEvent, _field_loaders, _loaders, fields, log, on, run, settings, starts
from .datasets import Dataset
from .jobs import Job
from .objects import Object
from .services import Reply, Service, secret, service
from .state import state
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
                data_uploaded=lambda event: objects.get(event["resource"], event["revision"]),
                job_finished=lambda event: jobs.get(event["resource"]))
_field_loaders.update(dataset=datasets.get, object=objects.get)
starts.append(state.forget)

try:
    from ._fields import FIELDS

    fields.update(FIELDS)
except ImportError:
    pass

__all__ = [
    "ArunaError", "Conflict", "Dataset", "Fail", "Form", "InvalidCall", "InvalidInput", "Job",
    "LimitReached", "NotAllowed", "NotFound", "Object", "Reply", "Request", "Retry", "Service",
    "ServiceError", "SyncEvent", "Unavailable", "UnknownOutcome", "datasets", "jobs", "log", "objects",
    "on", "run", "secret", "service", "settings", "state",
]
