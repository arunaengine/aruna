"""eLabFTW example plugin: links tagged experiments with Aruna datasets and syncs them both ways."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
import time

import aruna
import walk
from componentize_py_types import Err
from elab import Fault, Stop
from sync import Guard, Settings
from wit_world.imports import log, metadata_create, metadata_read, metadata_write, services
from wit_world.imports import objects_read, state, types


def hosted(function):
    """Turns a host error into a `Fault` with the error kind's name."""

    def call(*args):
        try:
            return function(*args)
        except Err as error:
            if isinstance(error.value, types.HostError):
                raise Fault(error.value.kind.name.lower(), error.value.message) from None
            raise

    return call


class Host:
    """The host calls the plugin uses, with plain Python values."""

    @hosted
    def elab(self, method, path, query, body):
        headers = [types.Field("content-type", "application/json")] if body else []
        query = [types.Field(name, value) for name, value in query]
        verb = getattr(services.Method, method)
        reply = services.send(services.Request("elab", verb, path, query, headers, body))
        return reply.status, bytes(reply.body)

    @hosted
    def get(self, resource):
        doc = metadata_read.get(resource)
        return doc.revision, doc.json

    @hosted
    def listing(self, cursor):
        found = metadata_read.list(cursor)
        return found.resources, found.cursor

    @hosted
    def files(self, resource, cursor):
        found = metadata_read.files(resource, cursor)
        return found.files, found.cursor

    @hosted
    def patch(self, resource, fields, revision):
        return metadata_write.patch(resource, json.dumps(fields), revision)

    @hosted
    def put_file(self, resource, entity, revision):
        return metadata_write.put_file(resource, json.dumps(entity), revision)

    @hosted
    def remove_file(self, resource, key, revision):
        return metadata_write.remove_file(resource, key, revision)

    @hosted
    def create(self, path, entity, key):
        # The host names a create by its path, which the sync also uses as its key.
        return metadata_create.create(path, json.dumps(entity)).resource

    @hosted
    def objects(self, prefix):
        return [found.path for found in objects_read.list(prefix, None).objects]

    @hosted
    def load(self, key):
        entry = state.get(key)
        return (entry.version, json.loads(bytes(entry.value))) if entry else (None, None)

    @hosted
    def save(self, key, value, version):
        return state.put(key, json.dumps(value).encode(), version)

    def log(self, message):
        log.log(log.Level.INFO, message)


def started(work, *args):
    """Runs the work with a guarded host; a fault fails the run with its message."""
    try:
        return work(Guard(Host(), time.monotonic), Settings(dict(aruna.settings)), *args)
    except Stop as stop:
        return {"stopped": str(stop)}
    except Fault as fault:
        error = aruna.Unavailable if fault.kind == "unavailable" else aruna.Fail
        raise error(fault.message) from None


@aruna.on("timer")
def poll():
    return started(walk.poll)


@aruna.on("dataset_updated")
def changed():
    return started(walk.changed, aruna.run.event["resource"])


@aruna.on("manual")
def manual(form):
    return started(walk.manual, dict(form))
