"""One link between an eLabFTW experiment and an Aruna dataset: compare, copy, enrol and resolve."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import hashlib
import json
from html.parser import HTMLParser

import elab
from elab import LIMIT, Fault, Stop

MIB = 1 << 20
RUN_BYTES = 16 * MIB
CALLS = 1000
KEEP = MIB + MIB // 4
PREFIX = "elabftw/uploads/"
MIMES = {"html": "text/html", "markdown": "text/markdown"}
DATASET_FIELD = "Aruna dataset"
ID_FIELD = "Aruna dataset id"
BLOCKS = {"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "pre", "blockquote"}


def weight(value):
    """Bytes a host call moves: its strings and bytes, also inside lists and dicts."""
    if isinstance(value, str):
        return len(value.encode())
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, dict):
        return sum(weight(key) + weight(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return sum(weight(item) for item in value)
    return 8


class Guard:
    """Counts the run's host calls and bytes and refuses a call that leaves no room to save."""

    def __init__(self, host, clock):
        self.host, self.clock, self.start = host, clock, clock()
        self.calls = self.used = 0
        self.keep, self.spare = KEEP, 3

    def fits(self, size=0, calls=1):
        room = self.used + size + MIB + self.keep <= RUN_BYTES
        return room and self.calls + calls + self.spare <= CALLS

    def fresh(self, size, calls):
        return size + 2 * MIB + KEEP <= RUN_BYTES and calls + 10 <= CALLS

    def late(self):
        return self.clock() - self.start > 40

    def closing(self):
        """Opens the kept room for the last saves of a stopped run."""
        self.keep = self.spare = 0

    def log(self, message):
        """Logs when there is room; the host counts a log line as a call."""
        if self.fits(len(message)):
            self.calls += 1
            self.used += len(message)
            self.host.log(message)

    def __getattr__(self, name):
        function = getattr(self.host, name)

        def guarded(*args):
            size = weight(args)
            if not self.fits(size):
                raise Stop(f"the run's budget is used up before `{name}`")
            self.calls += 1
            self.used += size
            try:
                reply = function(*args)
            except Fault as fault:
                self.used += len(fault.message)
                raise
            self.used += weight(reply)
            return reply

        return guarded


class Settings:
    def __init__(self, given):
        self.elab_url = given["elab_url"].rstrip("/")
        self.user = int(given["sync_user"])
        bucket, prefix = given["bucket"].rstrip("/"), given.get("prefix", "elabftw").strip("/")
        self.uploads = f"{bucket}/{prefix}/" if prefix else f"{bucket}/"
        self.tag = given.get("tag") or "aruna"
        self.aruna_url = given["aruna_url"].rstrip("/")


class Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts, self.hidden = [], 0

    def handle_starttag(self, tag, attrs):
        self.hidden += tag in ("script", "style")
        self.parts.append("\n" if tag in BLOCKS else "")

    def handle_endtag(self, tag):
        self.hidden -= tag in ("script", "style")
        self.parts.append("\n" if tag in BLOCKS else "")

    def handle_data(self, data):
        self.parts.append("" if self.hidden else data)


def plain(body, format):
    """The body as plain text for the dataset's `description`."""
    if format != "html":
        return body
    parser = Text()
    parser.feed(body)
    parser.close()
    lines = (line.strip() for line in "".join(parser.parts).splitlines())
    return "\n".join(line for line in lines if line)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def mark(kind, reason):
    return {"kind": kind, "reason": reason}


def from_experiment(found, tag):
    names = [name for name in (found.get("tags") or "").split("|") if name and name != tag]
    format = "markdown" if str(found.get("content_type")) == "2" else "html"
    title, body = found.get("title") or "", found.get("body") or ""
    return {"title": title, "tags": sorted(set(names)), "body": body, "format": format}


def from_dataset(doc):
    keywords = doc.get("keywords") or []
    keywords = [keywords] if isinstance(keywords, str) else keywords
    format = "markdown" if doc.get("encodingFormat") == MIMES["markdown"] else "html"
    title, body = doc.get("name") or "", doc.get("text") or ""
    return {"title": title, "tags": sorted(set(keywords)), "body": body, "format": format}


def dataset_patch(record):
    """The dataset fields of a record; empty values remove the field."""
    fields = {
        "name": record["title"],
        "keywords": record["tags"],
        "text": record["body"],
        "description": plain(record["body"], record["format"]),
        "encodingFormat": MIMES[record["format"]],
    }
    return {key: value or None for key, value in fields.items()}


def identified(doc):
    """The experiment id a dataset names in `identifier.elabftw.id`; other shapes name none."""
    found = doc.get("identifier")
    for key in ("elabftw", "id"):
        found = found.get(key) if isinstance(found, dict) else None
    return int(found) if isinstance(found, str) and found.isdigit() else None


def writable(found, user):
    """Whether the sync user may write the experiment, as eLabFTW 6.0 decides it."""
    canwrite = found.get("canwrite") or {}
    canwrite = json.loads(canwrite) if isinstance(canwrite, str) else canwrite
    base = int(found.get("canwrite_base") or canwrite.get("base") or 20)
    users = [int(user) for user in canwrite.get("users") or []]
    owner = int(found.get("userid") or 0) == user
    return owner or base >= 30 or user in users or bool(canwrite.get("teamgroups"))


class Link:
    """One experiment and its dataset; the record is the state value `link/<experiment id>`."""

    def __init__(self, host, settings, number, cycle):
        self.host, self.settings = host, settings
        self.number, self.cycle, self.key = number, cycle, f"link/{number}"
        self.version, state = host.load(self.key)
        self.state = state or {}

    def save(self, **changes):
        self.state.update(changes)
        self.version = self.host.save(self.key, self.state, self.version)

    def run(self, known=None, keep=None):
        """Brings the link in step and saves its record; returns the link's mark, if any."""
        try:
            found = elab.experiment(self.host, self.number)
            result = self.gone(found)
            if result is None and self.state.get("dataset"):
                result = self.sync(found, known, keep)
            elif result is None:
                result = self.enrol(found)
        except Fault as fault:
            result = mark("too_large" if fault.kind == "limit" else "failed", fault.message)
        self.finish(result)
        return result

    def finish(self, result):
        self.state["mark"] = result
        if self.cycle is not None:
            self.state["seen"] = self.cycle
        try:
            self.save()
        except Fault as fault:
            if fault.kind != "conflict":
                raise
            self.host.log(f"experiment {self.number} was left to another run")

    def gone(self, found):
        if found is None or str(found.get("state")) == "3":
            return mark("gone", "experiment")
        tags = (found.get("tags") or "").split("|")
        return None if self.settings.tag in tags else mark("gone", "tag")

    def enrol(self, found):
        """Creates the dataset of a newly tagged experiment, once per experiment."""
        if not self.state:
            self.save(pending="enrol")
        record = from_experiment(found, self.settings.tag)
        entity = {key: value for key, value in dataset_patch(record).items() if value}
        entity |= {"@type": "Dataset", "identifier": self.identifier()}
        path = f"elabftw/{self.number}"
        dataset = self.host.create(path, entity, path)
        revision, doc = self.host.get(dataset)
        same = from_dataset(doc) == record
        self.state.update(dataset=dataset, pending=None, synced=fingerprint(record) if same else None)
        result = self.extras(found, revision, doc)
        if not same:
            return mark("conflict", "the dataset and the experiment differ after a retried enrolment")
        return result

    def sync(self, found, known, keep):
        try:
            revision, doc = known or self.host.get(self.state["dataset"])
        except Fault as fault:
            if fault.kind == "not_found":
                return mark("gone", "dataset")
            raise
        theirs, ours = from_experiment(found, self.settings.tag), from_dataset(doc)
        side = keep or self.choose(fingerprint(theirs), fingerprint(ours))
        result = None
        if side == "elabftw":
            revision = self.to_aruna(theirs, revision)
        elif side == "aruna" and not writable(found, self.settings.user):
            result = mark("no_write", "the sync user may not write the experiment")
        elif side == "aruna":
            found, revision = self.to_elab(found, theirs, ours, revision)
        elif side == "same":
            self.state.update(synced=fingerprint(ours), pending=None)
        else:
            why = "an interrupted write" if self.state.get("pending") else "changes on both sides"
            result = mark("conflict", f"{why}: a manual run picks a side")
        return result or self.extras(found, revision, doc)

    def choose(self, elab_print, aruna_print):
        """Which side's record to copy: the one that changed since the last sync."""
        synced = self.state.get("synced")
        if elab_print == aruna_print:
            return "same"
        if self.state.get("pending"):
            return None
        if aruna_print == synced:
            return "elabftw"
        return "aruna" if elab_print == synced else None

    def to_aruna(self, record, revision):
        fields = dataset_patch(record)
        if len(json.dumps(fields)) > LIMIT:
            raise Fault("limit", "the dataset write is larger than one host call")
        self.save(pending="write")
        revision = self.host.patch(self.state["dataset"], fields, revision)
        self.state.update(synced=fingerprint(record), pending=None)
        return revision

    def to_elab(self, found, theirs, ours, revision):
        """Writes Aruna's record into the experiment, then eLabFTW's stored form back."""
        ids = dict(zip((found.get("tags") or "").split("|"), str(found.get("tags_id")).split(",")))
        adds = [tag for tag in ours["tags"] if tag not in theirs["tags"]]
        removes = [ids[tag] for tag in theirs["tags"] if tag not in ours["tags"]]
        retitle = ours["title"] != theirs["title"]
        rebody = (ours["body"], ours["format"]) != (theirs["body"], theirs["format"])
        answer = elab.estimate(found, ours["body"], ours["title"], ours["tags"])
        if answer > LIMIT:
            raise Fault("limit", f"eLabFTW's answer would be about {answer} bytes")
        answers = retitle + rebody + len(removes)
        size, calls = answers * answer + len(adds) * 1024, answers + len(adds) + 4
        if not self.host.fits(size, calls):
            if not self.host.fresh(size, calls):
                raise Fault("limit", "the experiment's writes do not fit one run")
            raise Stop("the experiment's writes need a fresh run")
        self.save(pending="write")
        try:
            after = self.write(adds, ours if retitle else None, ours if rebody else None, removes)
        except Fault as fault:
            if fault.refused and not self.wrote:
                self.state["pending"] = None
            raise
        stored = from_experiment(after, self.settings.tag)
        if stored != ours or rebody:  # the description follows every body write
            revision = self.host.patch(self.state["dataset"], dataset_patch(stored), revision)
        self.state.update(synced=fingerprint(stored), pending=None)
        return after, revision

    def write(self, adds, title, body, removes):
        """The experiment writes in order; the last answer shows what eLabFTW stored. A refused
        first write changed nothing, so `wrote` tells whether the pending marker can go."""
        self.wrote, after = False, None
        for tag in adds:
            elab.add_tag(self.host, self.number, tag)
            self.wrote = True
        if title:
            after = elab.patch(self.host, self.number, {"title": title["title"]})
            self.wrote = True
        if body:
            content = 2 if body["format"] == "markdown" else 1
            after = elab.patch(self.host, self.number, {"body": body["body"], "content_type": content})
            self.wrote = True
        for tag in removes:
            after = elab.remove_tag(self.host, self.number, tag)
            self.wrote = True
        return after or elab.call(self.host, "GET", str(self.number))

    def extras(self, found, revision, doc):
        """Uploads as file entities, the dataset's identifier and the experiment's link fields."""
        revision = self.files(revision)
        if identified(doc) != self.number:
            revision = self.host.patch(self.state["dataset"], {"identifier": self.identifier()}, revision)
        metadata = found.get("metadata_decoded") or json.loads(found.get("metadata") or "{}")
        fields = (metadata or {}).get("extra_fields") or {}
        if DATASET_FIELD in fields and ID_FIELD in fields:
            return None
        if not writable(found, self.settings.user):
            return mark("no_write", "the sync user may not write the experiment")
        try:
            elab.patch(self.host, self.number, {"metadatamerge": json.dumps(self.link_fields())})
        except Fault as fault:
            if fault.kind != "invalid":
                raise
            return mark("blocked", fault.message)
        return None

    def files(self, revision):
        """Brings the dataset's eLabFTW file entities in step; other properties on them stay."""
        dataset, facts = self.state["dataset"], {}
        for upload in elab.uploads(self.host, self.number):
            if str(upload.get("state")) != "3":
                facts[f"{PREFIX}{upload['id']}"] = {
                    "name": upload.get("real_name") or "",
                    "contentUrl": self.settings.uploads + (upload.get("long_name") or ""),
                    "contentSize": str(upload.get("filesize") or 0),
                    "sha256": upload.get("hash") or "",
                }
        if fingerprint(facts) == self.state.get("files"):
            return revision
        stored, cursor = {}, None
        while True:
            page, cursor = self.host.files(dataset, cursor)
            entities = [json.loads(text) for text in page]
            stored |= {entity["@id"]: entity for entity in entities if entity["@id"].startswith(PREFIX)}
            if cursor is None:
                break
        for key, fact in facts.items():
            entity = {"@type": "File"} | stored.get(key, {}) | {"@id": key} | fact
            if entity != stored.get(key):
                revision = self.host.put_file(dataset, entity, revision)
        for key in stored.keys() - facts.keys():
            revision = self.host.remove_file(dataset, key, revision)
        self.state["files"] = fingerprint(facts)
        return revision

    def identifier(self):
        url = f"{self.settings.elab_url}/experiments.php?mode=view&id={self.number}"
        return {"elabftw": {"id": str(self.number), "url": url}}

    def link_fields(self):
        dataset = self.state["dataset"]
        url = {"type": "url", "value": f"{self.settings.aruna_url}/datasets/{dataset}"}
        return {"extra_fields": {DATASET_FIELD: url, ID_FIELD: {"type": "text", "value": dataset}}}
