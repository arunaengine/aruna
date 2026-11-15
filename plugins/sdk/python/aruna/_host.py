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
        log.log(getattr(log.Level, level.upper()), message)

    def dataset_get(self, id, whole):
        read = self.imports.metadata_read
        document = self.call(read.get if whole else read.summary, id)
        return document.revision, document.json

    def dataset_list(self, cursor):
        listing = self.call(self.imports.metadata_read.list, cursor)
        return listing.resources, listing.cursor

    def dataset_files(self, id, cursor):
        page = self.call(self.imports.metadata_read.files, id, cursor)
        return page.files, page.cursor

    def dataset_patch(self, id, patch, expected):
        return self.call(self.imports.metadata_write.patch, id, patch, expected)

    def file_put(self, id, file, expected):
        return self.call(self.imports.metadata_write.put_file, id, file, expected)

    def file_remove(self, id, file, expected):
        return self.call(self.imports.metadata_write.remove_file, id, file, expected)

    def dataset_create(self, path, dataset):
        created = self.call(self.imports.metadata_create.create, path, dataset)
        return created.resource, created.revision

    def object_info(self, path):
        return _info(self.call(self.imports.objects_read.info, path))

    def object_list(self, prefix, cursor):
        page = self.call(self.imports.objects_read.list, prefix, cursor)
        return [_info(info) for info in page.objects], page.cursor

    def object_read(self, path, version):
        return bytes(self.call(self.imports.objects_read.read, path, version))

    def send(self, service, method, path, query, headers, body):
        services, field = self.imports.services, self.imports.types.Field
        method = getattr(services.Method, method.upper())
        query, headers = [field(*pair) for pair in query], [field(*pair) for pair in headers]
        request = services.Request(service, method, path, query, headers, body)
        response = self.call(services.send, request)
        answer_headers = [(header.name, header.value) for header in response.headers]
        return response.status, answer_headers, bytes(response.body)

    def secret(self, name):
        return [(field.name, field.value) for field in self.call(self.imports.secrets.value, name)]

    def state_get(self, key):
        entry = self.call(self.imports.state.get, key)
        return None if entry is None else (entry.version, bytes(entry.value))

    def state_put(self, key, value, expected):
        return self.call(self.imports.state.put, key, value, expected)

    def state_remove(self, key, expected):
        self.call(self.imports.state.remove, key, expected)


def _info(info):
    return {"path": info.path, "size": info.size, "version": info.version, "marked": info.marked}
