"""The cyclic walk over datasets and tagged experiments, the handlers' work and the report."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json

import elab
from elab import Fault, Stop
from sync import Link, identified, mark, read

START = {"phase": "datasets", "page": None, "index": 0, "cycle": 1, "current": None}
REPORT_BYTES = 48 << 10


def page(host, settings, cursor):
    """One page of the cursor's phase: dataset ULIDs or experiment ids, and the next page."""
    if cursor["phase"] == "datasets":
        return host.listing(cursor["page"])
    offset = cursor["page"] or 0
    rows = elab.tagged(host, settings.tag, offset)
    return [int(row["id"]) for row in rows], (offset + len(rows) if len(rows) == elab.PAGE else None)


UNLINKED = {"kind": "unlinked", "reason": "no experiment is linked"}


def linked(host, settings, dataset, cycle):
    """The dataset's document and link, or None and why: gone, not linked or too large to read."""
    try:
        revision, doc = read(host, dataset)
    except Fault as fault:
        if fault.kind == "limit":
            return None, mark("too_large", f"the dataset cannot be read: {fault.message}")
        if fault.kind == "not_found":
            return None, None
        raise
    number = identified(doc)
    link = Link(host, settings, number, cycle) if number is not None else None
    if link and link.state.get("dataset") == dataset:
        return (revision, doc, link), None
    return None, UNLINKED


class Walk:
    """The timer run: goes on from the saved cursor until the run's time or budget ends."""

    def __init__(self, host, settings):
        self.host, self.settings = host, settings
        self.version, cursor = host.load("walk")
        self.cursor, self.at, self.visits = cursor or dict(START), None, 0

    def save(self, **changes):
        self.cursor.update(changes)
        self.version = self.host.save("walk", self.cursor, self.version)

    def recover(self):
        """A run that ended without a save: the link it was on failed unless its visit ended."""
        number = self.cursor["current"]
        link = Link(self.host, self.settings, number, None)
        if link.state.get("seen") != self.cursor["cycle"] or link.state.get("pending"):
            link.finish(mark("failed", "the run stopped during this link"))
        self.save(current=None)

    def run(self):
        if self.cursor["current"] is not None:
            self.recover()
        cycle = self.cursor["cycle"]
        try:
            while self.cursor["cycle"] == cycle:
                items, after = page(self.host, self.settings, self.cursor)
                for index in range(self.cursor["index"], len(items)):
                    self.item(items[index], index)
                self.turn(after)
        except Stop:
            self.host.closing()
            self.save(index=self.cursor["index"] if self.at is None else self.at, current=None)
        return {"cycle": self.cursor["cycle"], "visits": self.visits}

    def item(self, entry, index):
        if self.host.late():
            raise Stop("the run's time is used up")
        cycle, known = self.cursor["cycle"], None
        if self.cursor["phase"] == "datasets":
            found, why = linked(self.host, self.settings, entry, cycle)
            if found is None:
                if why and why["kind"] == "too_large":
                    self.host.log(f"dataset {entry} is skipped: {why['reason']}")
                return
            revision, doc, link = found
            known = (revision, doc)
        else:
            link = Link(self.host, self.settings, entry, cycle)
            if link.state.get("dataset") and link.state.get("seen", 0) >= cycle:
                return
        self.at = index
        self.save(index=index + 1, current=link.number)
        link.run(known)
        self.at, self.visits = None, self.visits + 1

    def turn(self, after):
        """Moves to the next page, the next phase, or the next cycle."""
        if after is not None:
            self.save(page=after, index=0, current=None)
        elif self.cursor["phase"] == "datasets":
            self.save(phase="experiments", page=None, index=0, current=None)
        else:
            self.host.log(f"elabftw-python: cycle {self.cursor['cycle']} done")
            self.save(phase="datasets", page=None, index=0, current=None, cycle=self.cursor["cycle"] + 1)


def poll(host, settings):
    try:
        host.objects(settings.uploads)
    except Fault as fault:
        hint = "create the bucket on the install's receiver node"
        raise Fault(fault.kind, f"{settings.uploads} cannot be listed ({hint}): {fault.message}")
    return Walk(host, settings).run()


def changed(host, settings, dataset):
    found, why = linked(host, settings, dataset, None)
    if found is None:
        return {"dataset": dataset, "mark": why or UNLINKED}
    revision, doc, link = found
    return {"experiment": link.number, "mark": link.run((revision, doc))}


def manual(host, settings, given):
    given = given if isinstance(given, dict) else {}
    if given.get("report"):
        return report(host, settings, given.get("next"))
    if "resolve" in given and given.get("keep") in ("elabftw", "aruna"):
        link = Link(host, settings, int(given["resolve"]), None)
        return {"experiment": link.number, "mark": link.run(keep=given["keep"])}
    if "experiment" in given:
        link = Link(host, settings, int(given["experiment"]), None)
        return {"experiment": link.number, "mark": link.run()}
    raise Fault("invalid", 'expected {"report": true}, {"resolve": <id>, "keep": ...} or {"experiment": <id>}')


def describe(host, settings, phase, entry):
    """The report line of one listed dataset or experiment, or None when it has nothing to say."""
    if phase == "datasets":
        found, why = linked(host, settings, entry, None)
        if found is None:
            return {"dataset": entry, "mark": why or UNLINKED}
        link = found[2]
        return {"experiment": link.number, "dataset": entry, "mark": link.state["mark"]} if link.state.get("mark") else None
    _, state = host.load(f"link/{entry}")
    result = (state or {}).get("mark") or {}
    if state and state.get("pending") == "enrol":
        result = mark("unfinished", result.get("reason") or "the enrolment did not finish")
    elif result != mark("gone", "dataset"):
        return None
    return {"experiment": entry, "dataset": state.get("dataset"), "mark": result}


def report(host, settings, start):
    """One page of marked links; `next` continues it while more follow."""
    cursor = dict(start or {"phase": "datasets", "page": None, "index": 0})
    marks, size = [], 2
    try:
        while True:
            items, after = page(host, settings, cursor)
            for index in range(cursor["index"], len(items)):
                cursor["index"] = index
                entry = describe(host, settings, cursor["phase"], items[index])
                if entry is None:
                    continue
                if size + len(json.dumps(entry)) + 1 > REPORT_BYTES:
                    return {"marks": marks, "next": cursor}
                marks.append(entry)
                size += len(json.dumps(entry)) + 1
            if after is not None:
                cursor.update(page=after, index=0)
            elif cursor["phase"] == "datasets":
                cursor.update(phase="experiments", page=None, index=0)
            else:
                return {"marks": marks, "next": None}
    except Stop:
        return {"marks": marks, "next": cursor}
