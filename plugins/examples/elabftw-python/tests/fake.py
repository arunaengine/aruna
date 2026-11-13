"""Fake eLabFTW 6.0.9 and fake Aruna host for the plugin's tests, with the host's run bounds."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
import re

import walk
from elab import Fault
from sync import Guard, Settings

MIB = 1 << 20
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
SYMFONY = {'\\"': "\\u0022", "<": "\\u003C", ">": "\\u003E", "&": "\\u0026", "'": "\\u0027", "/": "\\/"}
NAMES = {}


class Killed(BaseException):
    """The runtime ended the run: no plugin code runs after it."""


def symfony(value):
    """JSON as Symfony's JsonResponse writes it with its default flags."""
    text = json.dumps(value, separators=(",", ":"))
    return re.sub(r'\\.|[<>&\'/]', lambda found: SYMFONY.get(found[0], found[0]), text)


def purify(body):
    """A small HTML cleaner: drops scripts, closes `<br>`, repeats formatting around blocks."""
    body = re.sub(r"<script.*?</script>", "", body, flags=re.S).replace("<br>", "<br />")
    outer = re.fullmatch(r"((?:<strong>)+)(.*?)((?:</strong>)+)", body, flags=re.S)
    if outer and "<p>" in outer[2]:
        body = outer[2].replace("<p>", "<p>" + outer[1]).replace("</p>", outer[3] + "</p>")
    return body


def render(body):
    """Markdown as eLabFTW renders it into `body_html`: paragraphs and reference links."""
    links = dict(re.findall(r"^\[(\w+)\]: (\S+)$", body, flags=re.M))
    text = re.sub(r"^\[\w+\]: \S+$", "", body, flags=re.M)
    text = re.sub(r"\[([^\]]*)\]\[(\w+)\]", lambda found: f'<a href="{links.get(found[2], "")}">{found[1]}</a>', text)
    return "".join(f"<p>{line}</p>" for line in text.splitlines() if line)


def answer(status, value):
    return status, symfony(value).encode()


class Elab:
    """Experiments of one eLabFTW team, answered as eLabFTW 6.0.9 answers its API v2."""

    def __init__(self, user=7):
        self.user, self.items, self.locked, self.sent = user, {}, set(), []

    def add(self, number, title="T", body="<p>b</p>", tags=("aruna",), **given):
        item = {"id": number, "title": title, "body": body, "content_type": 1, "tags": [], "state": 1}
        item |= {"metadata": {}, "canwrite_base": 30, "users": [], "userid": 1, "is_pinned": 0}
        self.items[number] = item | {"uploads": []} | given
        for tag in tags:
            self.tag(number, tag)
        return self.items[number]

    def tag(self, number, name):
        NAMES.setdefault(name, len(NAMES) + 100)
        if name not in self.items[number]["tags"]:
            self.items[number]["tags"].append(name)

    def full(self, item):
        html = render(item["body"]) if item["content_type"] == 2 else item["body"]
        canwrite = json.dumps({"users": item["users"], "teamgroups": []})
        tags = "|".join(item["tags"])
        ids = ",".join(str(NAMES[name]) for name in item["tags"])
        fields = {"body_html": html, "tags": tags, "tags_id": ids, "canwrite": canwrite}
        fields |= {"metadata": json.dumps(item["metadata"]), "metadata_decoded": item["metadata"]}
        return {key: value for key, value in item.items() if key not in ("users", "uploads")} | fields

    def writable(self, item):
        owner = item["userid"] == self.user
        return owner or item["canwrite_base"] >= 30 or self.user in item["users"]

    def handle(self, method, path, query, body):
        self.sent.append((method, path))
        given = json.loads(body) if body else {}
        if method == "GET" and path == "":
            return self.listing(dict(query))
        parts = path.split("/")
        item = self.items.get(int(parts[0]))
        if item is None:
            return answer(404, {"code": 404, "message": "Not Found", "description": "Nothing here"})
        if method == "GET" and len(parts) == 1:
            return answer(403, {"code": 403}) if item.get("hidden") else answer(200, self.full(item))
        if method == "GET":
            return answer(200, item["uploads"])
        if not self.writable(item):
            return answer(403, {"code": 403, "message": "Forbidden", "description": "No write access"})
        if item["id"] in self.locked:
            return answer(400, {"code": 400, "message": "This entry is being edited by Ada Lovelace."})
        if method == "POST":
            self.tag(item["id"], given["tag"])
            return 201, b""
        if len(parts) == 3:
            item["tags"] = [name for name in item["tags"] if str(NAMES[name]) != parts[2]]
            return answer(200, self.full(item))
        return self.change(item, {key: str(value) for key, value in given.items()})

    def change(self, item, given):
        """An entity PATCH: every value arrives as a string; `metadatamerge` is JSON text."""
        merged = json.loads(given.pop("metadatamerge", "{}"))
        fields = item["metadata"].setdefault("extra_fields", {})
        for name, field in merged.get("extra_fields", {}).items():
            if name in fields and fields[name].get("type") != field.get("type"):
                return answer(400, {"code": 400, "message": f"Metadata field {name} has incompatible type."})
            fields[name] = field
        if "content_type" in given:
            item["content_type"] = int(given.pop("content_type"))
        if "body" in given:
            item["body"] = purify(given.pop("body"))
        item.update(given)
        return answer(200, self.full(item))

    def listing(self, query):
        rows = [item for item in self.items.values() if query["tags[]"] in item["tags"] and item["state"] != 3]
        rows.sort(key=lambda item: (-item["is_pinned"], item["id"]))
        offset, limit = int(query["offset"]), int(query["limit"])
        return answer(200, [{"id": item["id"], "title": item["title"]} for item in rows[offset : offset + limit]])


def measure(value):
    if isinstance(value, (bytes, str)):
        return len(value.encode() if isinstance(value, str) else value)
    if isinstance(value, dict):
        return sum(measure(key) + measure(item) + 2 for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return sum(measure(item) + 1 for item in value)
    return 8


class Host:
    """An Aruna group as the plugin host serves it, metered like one plugin run."""

    def __init__(self, team, page=3):
        self.team, self.page, self.docs, self.order, self.keys = team, page, {}, [], {}
        self.state, self.logs, self.bucket, self.broken = {}, [], True, False
        self.trap, self.tick, self.now, self.made = None, 0.0, 0.0, 0
        self.start()

    def start(self):
        self.calls, self.used, self.peak = 0, 0, 0

    def metered(self, name, args, work):
        """One host call: the request, the reply held back, the trap, then the reply."""
        self.calls += 1
        self.now += self.tick
        self.used += measure(args)
        if self.calls > 1000 or self.used > 16 * MIB:
            self.broken = True
            raise Killed()
        if self.used + MIB > 16 * MIB:
            raise Fault("limit", "the run has too few bytes left for a reply")
        if self.trap and self.trap[0](name, args):
            action, self.trap = self.trap[1], self.trap if len(self.trap) > 2 else None
            if callable(action):
                action()
            elif action == "lose":
                work()
                raise Killed()
            else:
                raise Killed()
        reply = work()
        if measure(reply) > MIB:
            raise Fault("limit", f"the reply is larger than {MIB} bytes")
        self.used += measure(reply)
        self.peak = max(self.peak, self.used)
        return reply

    def doc(self, resource):
        if resource not in self.docs:
            raise Fault("not_found", f"{resource} does not exist")
        return self.docs[resource]

    def bump(self, resource):
        self.made += 1
        self.docs[resource]["revision"] = f"r{self.made}"
        return self.docs[resource]["revision"]

    def checked(self, resource, revision):
        if self.doc(resource)["revision"] != revision:
            raise Fault("conflict", f"{resource} changed since {revision}")

    def make(self, entity):
        """A dataset below `meta/elabftw/`, named by a new ULID."""
        number, letters = len(self.docs) + len(self.keys) + 1, ""
        for _ in range(10):
            number, rest = divmod(number, 32)
            letters = CROCKFORD[rest] + letters
        resource = "01JD" + "0" * 12 + letters
        self.docs[resource] = {"json": entity, "files": {}, "revision": ""}
        self.order.append(resource)
        self.bump(resource)
        return resource

    def edit(self, resource, **fields):
        self.docs[resource]["json"] = merge(self.docs[resource]["json"], fields)
        self.bump(resource)

    def delete(self, resource):
        del self.docs[resource]
        self.order.remove(resource)

    def value(self, key):
        return json.loads(self.state[key][1]) if key in self.state else None

    def elab(self, method, path, query, body):
        return self.metered("elab", (method, path, query, body), lambda: self.team.handle(method, path, query, body))

    def get(self, resource):
        def work():
            return self.doc(resource)["revision"], json.loads(json.dumps(self.doc(resource)["json"]))

        return self.metered("get", resource, work)

    def listing(self, cursor):
        def work():
            offset = int(cursor or 0)
            resources = self.order[offset : offset + self.page]
            return resources, (str(offset + self.page) if len(resources) == self.page else None)

        return self.metered("listing", cursor or "", work)

    def files(self, resource, cursor):
        def work():
            entities = sorted(self.doc(resource)["files"].values(), key=lambda entity: entity["@id"])
            offset = int(cursor or 0)
            chosen = [json.dumps(entity) for entity in entities[offset : offset + self.page]]
            return chosen, (str(offset + self.page) if offset + self.page < len(entities) else None)

        return self.metered("files", (resource, cursor or ""), work)

    def changed(self, name, resource, revision, change, args):
        def work():
            self.checked(resource, revision)
            change(self.docs[resource])
            return self.bump(resource)

        return self.metered(name, (resource, revision) + args, work)

    def patch(self, resource, fields, revision):
        def change(doc):
            doc["json"] = merge(doc["json"], fields)

        return self.changed("patch", resource, revision, change, (json.dumps(fields),))

    def put_file(self, resource, entity, revision):
        def change(doc):
            doc["files"][entity["@id"]] = entity

        return self.changed("put_file", resource, revision, change, (json.dumps(entity),))

    def remove_file(self, resource, key, revision):
        def change(doc):
            del doc["files"][key]

        return self.changed("remove_file", resource, revision, change, (key,))

    def create(self, path, entity, key):
        def work():
            if key not in self.keys:
                self.keys[key] = self.make(entity)
            return self.keys[key]

        return self.metered("create", (path, json.dumps(entity), key), work)

    def objects(self, prefix):
        def work():
            if not self.bucket:
                raise Fault("not_found", f"{prefix} does not exist")
            return []

        return self.metered("objects", prefix, work)

    def load(self, key):
        def work():
            entry = self.state.get(key)
            return (entry[0], json.loads(entry[1])) if entry else (None, None)

        return self.metered("load", key, work)

    def save(self, key, value, version):
        def work():
            if self.state.get(key, (None,))[0] != version:
                raise Fault("conflict", f"{key} is at another version")
            self.state[key] = ((version or 0) + 1, json.dumps(value))
            return (version or 0) + 1

        return self.metered("save", (key, json.dumps(value)), work)

    def log(self, message):
        self.metered("log", message, lambda: self.logs.append(message))


def merge(target, patch):
    """A JSON merge patch: null removes a field, objects merge field by field."""
    result = dict(target)
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = value
    return result


class Plugin:
    """The plugin against the fakes: each call is one guarded run with fresh host bounds."""

    def __init__(self, page=3):
        self.elab = Elab()
        self.host = Host(self.elab, page)
        given = {"elab_url": "https://elab.example.org", "sync_user": 7, "bucket": "/r/g/lab/data/n/b"}
        self.settings = Settings(given | {"prefix": "elabftw", "aruna_url": "https://aruna.example.org"})

    def run(self, work, *args):
        self.host.start()
        try:
            return work(Guard(self.host, lambda: self.host.now), self.settings, *args)
        except Killed:
            return "killed"

    def poll(self):
        return self.run(walk.poll)

    def manual(self, given):
        return self.run(walk.manual, given)

    def changed(self, dataset):
        return self.run(walk.changed, dataset)

    def link(self, number):
        return self.host.value(f"link/{number}")

    def dataset(self, number):
        """The dataset JSON of a linked experiment."""
        return self.host.docs[self.link(number)["dataset"]]["json"]


def trap(name, count, action, test=None):
    """A host trap that fires at the `count`th call of `name` whose arguments pass `test`."""
    seen = [0]

    def match(call, args):
        if call == name and (test is None or test(args)):
            seen[0] += 1
            return seen[0] == count
        return False

    return (match, action)
