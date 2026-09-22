"""Converts ISA RO-Crate metadata through the pinned DataPLANT ARCtrl library."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import base64
import copy
import importlib.metadata
import json
import resource
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

from arctrl import ARC
from openpyxl import load_workbook

LIMIT = 64 * 1024 * 1024


def confined(value):
    path = PurePosixPath(value)
    if (not value or path.is_absolute() or "\\" in value or "\0" in value
            or any(part in ("..", ".git") for part in path.parts)):
        raise ValueError("unsafe ARC path")
    return path


def remap(value, before):
    if isinstance(value, list):
        return [remap(item, before) for item in value]
    if isinstance(value, dict):
        return {key: "./" if key == "@id" and item == before else remap(item, before)
                for key, item in value.items()}
    return value


def prepare(source, identifier):
    document = json.loads(source)
    graph = document.get("@graph")
    if not isinstance(graph, list):
        raise ValueError("RO-Crate @graph is required")
    descriptors = [item for item in graph if item.get("@id") == "ro-crate-metadata.json"]
    if len(descriptors) != 1:
        raise ValueError("one RO-Crate metadata descriptor is required")
    root_id = descriptors[0].get("about", {}).get("@id")
    roots = [item for item in graph if item.get("@id") == root_id]
    if len(roots) != 1 or not root_id:
        raise ValueError("one RO-Crate root Dataset is required")
    document = remap(copy.deepcopy(document), root_id)
    root = next(item for item in document["@graph"] if item.get("@id") == "./")
    for field in ("name", "description"):
        if not isinstance(root.get(field), str) or not root[field].strip():
            raise ValueError("ISA investigation requires a textual " + field)
    root.setdefault("identifier", identifier)
    root.setdefault("additionalType", "Investigation")
    template = ARC(identifier, title=root["name"], description=root["description"])
    document["@context"] = json.loads(template.ToROCrateJsonString())["@context"]
    return ARC.from_rocrate_json_string(json.dumps(document))


def workbook(path):
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if len(entries) > 2000 or sum(item.file_size for item in entries) > LIMIT:
            raise ValueError("expanded ISA workbook exceeds the limit")
    names = {"isa.investigation.xlsx": "isa_investigation", "isa.study.xlsx": "isa_study",
             "isa.assay.xlsx": "isa_assay"}
    sheet = names.get(path.name)
    if sheet:
        book = load_workbook(path, read_only=True, data_only=False)
        try:
            if sheet not in book.sheetnames:
                raise ValueError("ISA workbook is missing its metadata sheet")
            if path.name == "isa.investigation.xlsx":
                labels = {row[0] for row in book[sheet].iter_rows(values_only=True) if row}
                required = {"ONTOLOGY SOURCE REFERENCE", "INVESTIGATION", "INVESTIGATION PUBLICATIONS",
                            "INVESTIGATION CONTACTS", "Investigation Identifier", "Investigation Title",
                            "Investigation Description", "Investigation Submission Date",
                            "Investigation Public Release Date"}
                if not required <= labels:
                    raise ValueError("ISA investigation is missing required sections or labels")
        finally:
            book.close()


def data_paths(document):
    paths = []
    for entity in document["@graph"]:
        types = entity.get("@type", [])
        if "File" not in (types if isinstance(types, list) else [types]):
            continue
        value = entity["@id"]
        url = urlsplit(value)
        if url.scheme in ("http", "https"):
            continue
        if url.scheme or url.netloc:
            raise ValueError("unsupported ARC data reference")
        path = str(confined(unquote(url.path)))
        paths.append(path)
    return paths


def cwl(root):
    paths = list(root.rglob("*.cwl"))
    if not paths:
        return
    from cwltool.context import LoadingContext
    from cwltool.load_tool import fetch_document, resolve_and_validate_document
    from schema_salad.fetcher import DefaultFetcher

    class LocalFetcher(DefaultFetcher):
        def fetch_text(self, url, content_types=None):
            if isinstance(self.cache.get(url), str):
                return self.cache[url]
            parsed = urlsplit(url)
            if parsed.scheme != "file" or parsed.netloc:
                raise ValueError("CWL imports must stay inside the ARC")
            path = Path(unquote(parsed.path)).resolve()
            if not path.is_relative_to(root) or path.stat().st_size > LIMIT:
                raise ValueError("CWL import is outside the ARC or exceeds its limit")
            return super().fetch_text(url, content_types)

        def check_exists(self, url):
            parsed = urlsplit(url)
            if parsed.scheme != "file" or parsed.netloc:
                return url in self.cache
            path = Path(unquote(parsed.path)).resolve()
            return path.is_relative_to(root) and path.exists()

    for path in paths:
        try:
            context = LoadingContext({"fetcher_constructor": LocalFetcher, "do_update": False})
            context, document, uri = fetch_document(path.as_uri(), context)
            if document.get("cwlVersion") != "v1.2" or document.get("class") not in ("Workflow", "CommandLineTool"):
                raise ValueError("ARC workflows require CWL v1.2 Workflow or CommandLineTool")
            resolve_and_validate_document(context, document, uri)
        except Exception as error:
            raise ValueError("invalid or non-local CWL v1.2 description") from error


def inspect(root, require_data=True):
    if not (root / "isa.investigation.xlsx").is_file():
        raise ValueError("missing ISA investigation")
    for path in root.rglob("*.xlsx"):
        if path.name.startswith("isa."):
            workbook(path)
    cwl(root)
    arc = ARC.load(str(root))
    if not all(isinstance(value, str) and value.strip() for value in [arc.Identifier, arc.Title, arc.Description]):
        raise ValueError("ISA investigation identifier, title and description are required")
    studies = {item.Identifier: item for item in arc.Studies}
    assays = {item.Identifier: item for item in arc.Assays}
    for identifier in arc.RegisteredStudyIdentifiers:
        confined(identifier)
        if identifier not in studies or not (root / "studies" / identifier / "isa.study.xlsx").is_file():
            raise ValueError("registered ISA study is missing")
    for study in studies.values():
        for identifier in study.RegisteredAssayIdentifiers:
            confined(identifier)
            if identifier not in assays or not (root / "assays" / identifier / "isa.assay.xlsx").is_file():
                raise ValueError("registered ISA assay is missing")
    document = json.loads(arc.ToROCrateJsonString())
    if require_data and any(not (root / path).is_file() for path in data_paths(document)):
        raise ValueError("referenced ARC data is missing")
    return document


def generate(request, root):
    source = request["jsonld"]
    arc = prepare(source, request["document_id"])
    contracts = arc.GetWriteContracts()
    if len(contracts) > 10000:
        raise ValueError("generated ARC exceeds the path limit")
    for contract in contracts:
        confined(contract.path)
    arc.Write(str(root))
    document = inspect(root, require_data=False)
    (root / "ro-crate-metadata.json").write_text(json.dumps(document, indent=2) + "\n")
    (root / "aruna-metadata.json").write_text(source)
    (root / ".gitattributes").write_text("*.bin filter=lfs diff=lfs merge=lfs -text\n")
    paths = sorted(path for path in root.rglob("*") if path.is_file())
    if len(paths) > 10000 or sum(path.stat().st_size for path in paths) > LIMIT // 2:
        raise ValueError("generated ARC exceeds the repository limit")
    return {"files": {str(path.relative_to(root)): base64.b64encode(path.read_bytes()).decode() for path in paths},
            "required": data_paths(document)}


def convert(request):
    with tempfile.TemporaryDirectory(prefix="aruna-isa-") as directory:
        root = Path(directory)
        if request["mode"] == "generate":
            return generate(request, root)
        if request["mode"] != "inspect":
            raise ValueError("unsupported ARC conversion")
        files = request["files"]
        if len(files) > 10000:
            raise ValueError("ARC tree exceeds the path limit")
        for name, content in files.items():
            path = root / confined(name)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(content, validate=True) if content else b"")
        return {"rocrate": inspect(root)}


if __name__ == "__main__":
    resource.setrlimit(resource.RLIMIT_AS, (768 * 1024 * 1024, 768 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_CPU, (120, 120))
    if importlib.metadata.version("arctrl") != "3.2.1":
        raise RuntimeError("ARCtrl 3.2.1 is required")
    data = sys.stdin.buffer.read(LIMIT + 1)
    if len(data) > LIMIT:
        raise RuntimeError("ARC conversion input exceeds limit")
    try:
        result = convert(json.loads(data))
    except Exception:
        result = {"error": "ISA/ARC conversion failed: check required metadata, registrations and paths"}
    sys.stdout.write(json.dumps(result))
