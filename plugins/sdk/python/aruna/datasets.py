"""Datasets of the install's group: fields changed in place and saved as only what changed at the
revision read, file entities, and creates named by a path."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import copy
import json

from ._host import host
from .errors import InvalidCall

class Dataset:
    """One dataset at the revision it was read; any other property by name, as `dataset["publisher"]`."""

    def __init__(self, id, revision, entity):
        self.id, self.revision = id, revision
        self._read, self._properties = {}, {}
        self.name, self.description, self.keywords, self.license = "", "", [], None
        self.identifiers = {}
        for name, value in entity.items():
            self._put(name, value)
        # Compared in the form a save writes, so a field read as text is unchanged until changed;
        # a copy, so editing a nested value in place still counts as a change.
        self._read = copy.deepcopy(self._entity())

    def _put(self, name, value):
        if name in ("name", "description"):
            setattr(self, name, value or "")
        elif name == "keywords":
            words = value.split(",") if isinstance(value, str) else (value or [])
            self.keywords = [word.strip() for word in words if isinstance(word, str) and word.strip()]
        elif name == "license":
            self.license = value.get("@id") if isinstance(value, dict) else value
        elif name in ("identifier", "identifiers"):
            self.identifiers = dict(value) if isinstance(value, dict) else {}
        elif value is None:
            self._properties.pop(name, None)
        else:
            self._properties[name] = value

    def _entity(self):
        entity = dict(self._properties)
        fields = {"name": self.name, "description": self.description, "keywords": [*self.keywords],
                  "license": self.license, "identifier": dict(self.identifiers)}
        # An empty field is left out, so a save removes it.
        entity.update((name, value) for name, value in fields.items() if value)
        return entity

    def __getitem__(self, name):
        return self._entity()[name]

    def __setitem__(self, name, value):
        self._put(name, value)

    def __delitem__(self, name):
        self._put(name, None)

    def get(self, name, default=None):
        return self._entity().get(name, default)

    def save(self):
        """Sends the fields that changed with the revision read and answers their names; nothing
        changed makes no call. A change made meanwhile raises `Conflict`."""
        now = self._entity()
        patch = {name: value for name, value in merge_patch(self._read, now).items() if not name.startswith("@")}
        if not patch:
            return []
        text = json.dumps(patch, sort_keys=True)
        self.revision = host().dataset_patch(self.id, text, self.revision)
        self._read = copy.deepcopy(now)
        return ["identifiers" if name == "identifier" else name for name in sorted(patch)]

    def reload(self):
        """Reads the dataset again; changes not saved are lost."""
        self.__dict__.update(get(self.id).__dict__)

    def crate(self):
        """The dataset entity with its file links."""
        return json.loads(host().dataset_get(self.id, True)[1])

    def files(self):
        """The dataset's `File` entities, one page at a time."""
        return (json.loads(file) for file in _pages(lambda cursor: host().dataset_files(self.id, cursor)))

    def add_file(self, object, id=None, **properties):
        """Links an `Object`, or the object at a bucket path, as a `File` entity whose `contentUrl`
        is its `s3://` address; named by `id`, else by that address."""
        if isinstance(object, str):
            from . import objects

            object = objects.get(object)
        file = {"@id": id or object.url, "@type": "File", "name": object.path.rsplit("/", 1)[-1],
                "contentSize": object.size, "contentUrl": object.url, **properties}
        self.put_file(file)

    def put_file(self, file):
        """Adds or replaces one `File` entity the plugin built itself."""
        self.revision = host().file_put(self.id, json.dumps(file, sort_keys=True), self.revision)

    def remove_file(self, id):
        self.revision = host().file_remove(self.id, id, self.revision)


def merge_patch(old, new):
    """The JSON merge patch that turns `old` into `new`: what changed, `None` for each name removed,
    and the same rule inside objects on both sides, as the host applies a merge patch."""
    patch = {name: None for name in old if name not in new}
    for name, value in new.items():
        before = old.get(name)
        if isinstance(before, dict) and isinstance(value, dict):
            inner = merge_patch(before, value)
            if inner:
                patch[name] = inner
        elif name not in old or before != value:
            patch[name] = value
    return patch


def get(id):
    """The dataset `id`, without its file links."""
    revision, text = host().dataset_get(id, False)
    return Dataset(id, revision, json.loads(text))


def list():
    """Every dataset of the group the install may read, each loaded, one page of ids at a time."""
    return (get(id) for id in _pages(host().dataset_list))


def create(path, **fields):
    """Creates the dataset at `path` below the group's metadata root; the path names it, so creating
    it again answers the same dataset."""
    if "identifiers" in fields:
        fields["identifier"] = fields.pop("identifiers")
    entity = {name: value for name, value in fields.items() if value is not None}
    if not entity:
        raise InvalidCall("a dataset needs at least one field")
    id, revision = host().dataset_create(path, json.dumps(entity, sort_keys=True))
    return Dataset(id, revision, entity)


def _pages(fetch):
    """The items of paged host answers, one page fetched at a time."""
    cursor = None
    while True:
        items, cursor = fetch(cursor)
        yield from items
        if cursor is None:
            return
