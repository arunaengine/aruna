"""Jobs the install started: copies now, downloads, uploads and tasks later. A start answers the
job at once; `job_finished` delivers it once it ended and the run that started it succeeded."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json

from ._host import host
from .datasets import _pages
from .objects import Object
from .services import Reply


class Job:
    """One job of the install. `state` is queued, running, succeeded, failed, error (an attempt
    failed and is retried), cancelled or unknown."""

    def __init__(self, id, kind, path="", state="queued", message=None, bytes=None, version=None,
                 exit_code=None, reply=None, report=None):
        self.id, self.kind, self.path, self.state = id, kind, path, state
        self.message, self.bytes, self.version, self.exit_code = message, bytes, version, exit_code
        self.reply, self.report = reply, report

    @property
    def ok(self):
        return self.state == "succeeded"

    def refresh(self):
        """Reads the job again."""
        self.__dict__.update(get(self.id).__dict__)

    def cancel(self):
        """Asks the job to stop; a job that ended stays as it ended."""
        host().job_cancel(self.id)

    def outputs(self):
        """The objects a task wrote, one page at a time."""
        return (Object(**info) for info in _pages(lambda cursor: host().job_outputs(self.id, cursor)))

    def report_json(self):
        """The report as JSON, or None when it was left out."""
        return None if self.report is None else json.loads(self.report)

    def __repr__(self):
        return f"Job({self.id!r}, {self.kind!r}, state={self.state!r})"


def get(id):
    """The job `id` of the install, as it stands now."""
    status = dict(host().job_get(id))
    reply = status.pop("reply", None)
    return Job(**status, reply=None if reply is None else Reply(*reply))


def copy(source, target, version=None):
    """Copies the object at `source`, pinned to `version` when given, to `target`; it needs read on
    the source and write on the target. A rerun of the run gets the same job back."""
    return Job(host().job_copy(source, version, target), "copy", target)
