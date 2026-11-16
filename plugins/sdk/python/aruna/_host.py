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

    def object_info(self, path, version=None):
        return _info(self.call(self.imports.objects_read.info, path, version))

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

    def job_copy(self, source, version, target):
        return self.call(self.imports.jobs.copy, source, version, target)

    def job_download(self, service, path, query, headers, target, size_limit, checksums):
        jobs, field = self.imports.jobs, self.imports.types.Field
        fields = lambda pairs: [field(*pair) for pair in pairs]  # noqa: E731
        request = jobs.DownloadRequest(service, path, fields(query), fields(headers), target, size_limit,
                                       fields(checksums))
        return self.call(jobs.download, request)

    def job_upload(self, service, request, source, form):
        jobs, field = self.imports.jobs, self.imports.types.Field
        fields = lambda pairs: [field(*pair) for pair in pairs]  # noqa: E731
        (method, path, query, headers), (source, version), (file_field, form_fields) = request, source, form
        method = getattr(self.imports.services.Method, method.upper())
        request = jobs.UploadRequest(service, method, path, fields(query), fields(headers), source, version,
                                     file_field, fields(form_fields))
        return self.call(jobs.upload, request)

    def job_get(self, id):
        status = self.call(self.imports.jobs.get, id)
        reply = None
        if status.reply_status is not None:
            headers = [(field.name, field.value) for field in status.reply_headers]
            reply = (status.reply_status, headers, bytes(status.reply_body or b""))
        return {"id": status.id, "kind": status.kind, "path": status.path, "state": status.state,
                "message": status.message, "bytes": status.bytes, "version": status.version,
                "exit_code": status.exit_code, "reply": reply, "report": status.report}

    def job_outputs(self, id, cursor):
        page = self.call(self.imports.jobs.outputs, id, cursor)
        return [_info(info) for info in page.objects], page.cursor

    def job_cancel(self, id):
        self.call(self.imports.jobs.cancel, id)


def _info(info):
    return {"path": info.path, "size": info.size, "version": info.version, "marked": info.marked}
