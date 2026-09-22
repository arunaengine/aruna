"""Reads a bounded ARC subset with ARCtrl without changing committed file bytes."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
import re
import subprocess
import zipfile
from pathlib import Path, PurePosixPath

from arctrl import ARC, ArcAssay, ArcStudy, ArcTable, CompositeCell, CompositeHeader, IOType


COMMIT = re.compile(r"[0-9a-f]{40}\Z")
POINTER = re.compile(rb"version https://git-lfs.github.com/spec/v1\noid sha256:([0-9a-f]{64})\nsize ([0-9]+)\n\Z")
MAX_FILE = 1024 * 1024


def git(repo, *args):
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, timeout=120)
    if result.returncode:
        raise ValueError("Git object or reference operation failed")
    return result.stdout


def pointer(data):
    match = POINTER.fullmatch(data)
    if match:
        return match[1].decode(), int(match[2])
    if data.startswith(b"version https://git-lfs.github.com/spec/"):
        raise ValueError("invalid or unsupported LFS pointer")
    return None


def materialize(repo, commit, destination, store):
    if not COMMIT.fullmatch(commit):
        raise ValueError("an exact SHA-1 commit ID is required")
    if git(repo, "cat-file", "-t", commit).strip() != b"commit":
        raise ValueError("revision is not a commit")
    entries = git(repo, "ls-tree", "-rlz", commit).split(b"\0")[:-1]
    if not entries or len(entries) > 200:
        raise ValueError("PoC requires between 1 and 200 files")
    files = []
    total = 0
    for entry in entries:
        header, raw_path = entry.split(b"\t", 1)
        mode, kind, oid, size = header.split()
        path = raw_path.decode("utf-8")
        parts = PurePosixPath(path).parts
        if (not parts or path.startswith("/") or "\\" in path
                or any(part in (".", "..", ".git") for part in parts)):
            raise ValueError("unsafe repository path")
        if path == ".lfsconfig":
            raise ValueError("PoC uses the repository's own LFS endpoint")
        if kind != b"blob" or mode not in (b"100644", b"100755"):
            raise ValueError("PoC does not support symlinks or submodules")
        if int(size) > MAX_FILE:
            raise ValueError("files above 1 MiB must use LFS")
        total += int(size)
        if total > 16 * MAX_FILE:
            raise ValueError("Git tree exceeds the PoC limit")
        data = git(repo, "cat-file", "blob", oid.decode())
        payload = pointer(data)
        if payload:
            if path.endswith(".xlsx") or path == "ro-crate-metadata.json":
                raise ValueError("PoC metadata must be ordinary Git files")
            store.check(*payload)
            files.append((path, *payload))
        target = Path(destination) / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    return files


def inspect_arc(directory):
    root = Path(directory)
    if not (root / "isa.investigation.xlsx").is_file():
        raise ValueError("missing isa.investigation.xlsx")
    for workbook in root.rglob("*.xlsx"):
        with zipfile.ZipFile(workbook) as archive:
            entries = archive.infolist()
            if len(entries) > 2000 or sum(item.file_size for item in entries) > 16 * MAX_FILE:
                raise ValueError("expanded workbook exceeds the PoC limit")
    arc = ARC.load(str(root))
    if not arc.Identifier or not arc.Title or not arc.Description:
        raise ValueError("investigation identifier, title and description are required")
    if arc.Workflows or arc.Runs:
        raise ValueError("PoC supports study/assay ARCs; workflow/run validation is not implemented")
    if not arc.Studies or not arc.Assays:
        raise ValueError("PoC requires at least one study and assay")
    if set(arc.RegisteredStudyIdentifiers) != {item.Identifier for item in arc.Studies}:
        raise ValueError("all studies must be registered in the investigation")
    linked = set()
    for study in arc.Studies:
        if not (root / "studies" / study.Identifier / "isa.study.xlsx").is_file():
            raise ValueError("registered study workbook is missing")
        linked.update(study.RegisteredAssayIdentifiers)
    if linked != {item.Identifier for item in arc.Assays}:
        raise ValueError("assay registrations do not match the investigation")
    for assay in arc.Assays:
        if not (root / "assays" / assay.Identifier / "isa.assay.xlsx").is_file():
            raise ValueError("registered assay workbook is missing")
    document = json.loads((root / "ro-crate-metadata.json").read_text())
    entities = {entity["@id"]: entity for entity in document["@graph"]}
    investigation = entities["./"]
    if investigation.get("name") != arc.Title or investigation.get("identifier") != arc.Identifier:
        raise ValueError("RO-Crate and ISA investigation disagree")
    if entities["ro-crate-metadata.json"].get("about") != {"@id": "./"}:
        raise ValueError("RO-Crate descriptor does not identify its root")
    for item, folder in [(item, "studies") for item in arc.Studies] + [(item, "assays") for item in arc.Assays]:
        if entities.get(f"{folder}/{item.Identifier}/", {}).get("identifier") != item.Identifier:
            raise ValueError("RO-Crate is missing an ISA study or assay")
    return document


def scaffold(directory):
    root = Path(directory)
    if root.exists() and any(root.iterdir()):
        raise ValueError("scaffold destination must be empty")
    arc = ARC("aruna-poc", title="Synthetic ARC experiment", description="Synthetic bridge fixture",
              public_release_date="2026-09-22")
    study = ArcStudy("study", title="Synthetic study")
    table = ArcTable("measurement", headers=[CompositeHeader.input(IOType.sample()),
                                               CompositeHeader.output(IOType.data())])
    table.AddRow([CompositeCell.create_free_text("sample-1"),
                  CompositeCell.create_data_from_string("assays/assay/dataset/measurements.bin")])
    assay = ArcAssay("assay", title="Synthetic assay", tables=[table])
    arc.AddRegisteredStudy(study)
    arc.AddAssay(assay)
    study.RegisterAssay("assay")
    arc.Write(str(root))
    (root / "ro-crate-metadata.json").write_text(arc.ToROCrateJsonString() + "\n")
    (root / ".gitattributes").write_text("*.bin filter=lfs diff=lfs merge=lfs -text\n")
    (root / "assays/assay/dataset/measurements.bin").write_bytes(b"synthetic measurement\n" * 4096)
    inspect_arc(root)
