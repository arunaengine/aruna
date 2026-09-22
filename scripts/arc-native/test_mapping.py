"""Checks ISA conversion, metadata preservation and malformed ARC rejection."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import base64
import importlib.util
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from arctrl import ARC, CompositeCell, CompositeHeader, OntologyAnnotation, Person
from openpyxl import load_workbook
from schema_salad.exceptions import ValidationException
from arc import scaffold

spec = importlib.util.spec_from_file_location("aruna_conversion", Path(__file__).resolve().parents[2] / "blob/src/arc.py")
conversion = importlib.util.module_from_spec(spec)
spec.loader.exec_module(conversion)


def source():
    return {"@context": "https://w3id.org/ro/crate/1.3/context", "@graph": [
        {"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
         "about": {"@id": "urn:aruna:source"}, "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3"}},
        {"@id": "urn:aruna:source", "@type": "Dataset", "identifier": "original-identifier",
         "name": "Supplied title", "description": "Supplied description", "datePublished": "2026-09-22",
         "https://example.org/custom": {"@value": "unaltered extension", "@language": "en"}},
        {"@id": "#context", "@type": "Thing", "name": "Unmapped contextual entity"},
    ]}


def files(root):
    return {str(path.relative_to(root)): base64.b64encode(path.read_bytes()).decode()
            for path in root.rglob("*") if path.is_file()}


class MappingTests(unittest.TestCase):
    def test_minimal_preserved(self):
        original = json.dumps(source())
        result = conversion.convert({"mode": "generate", "document_id": "document-id", "jsonld": original})
        self.assertEqual(base64.b64decode(result["files"]["aruna-metadata.json"]).decode(), original)
        self.assertFalse(any(name.endswith(("isa.study.xlsx", "isa.assay.xlsx")) for name in result["files"]))
        derived = conversion.convert({"mode": "inspect", "files": result["files"]})["rocrate"]
        root = next(item for item in derived["@graph"] if item.get("@id") == "./")
        self.assertEqual(root["name"], "Supplied title")
        self.assertEqual(root["description"], "Supplied description")
        self.assertEqual(root["identifier"], "original-identifier")
        self.assertEqual(root["additionalType"], "Investigation")

    def test_rich_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scaffold(root)
            arc = ARC.load(directory)
            arc.Contacts = [Person(first_name="Ada", last_name="Example", email="ada@example.invalid")]
            table = arc.Assays[0].Tables[0]
            table.AddColumn(CompositeHeader.parameter(OntologyAnnotation("temperature", "PATO", "PATO:0000146")),
                            [CompositeCell.create_unitized("37", OntologyAnnotation("degree Celsius", "UO", "UO:0000027"))])
            original = arc.ToROCrateJsonString()
            generated = conversion.convert({"mode": "generate", "document_id": "document-id", "jsonld": original})
            payload = "assays/assay/dataset/measurements.bin"
            self.assertIn(payload, generated["required"])
            self.assertNotIn(payload, generated["files"], "converter must not fabricate data bytes")
            generated["files"][payload] = base64.b64encode((root / payload).read_bytes()).decode()
            restored = conversion.convert({"mode": "inspect", "files": generated["files"]})["rocrate"]
            entities = restored["@graph"]
            self.assertTrue(any(item.get("additionalType") == "Study" and item.get("identifier") == "study" for item in entities))
            self.assertTrue(any(item.get("additionalType") == "Assay" and item.get("identifier") == "assay" for item in entities))
            self.assertTrue(any(item.get("@type") == "LabProcess" and item.get("object") and item.get("result") for item in entities))
            self.assertTrue(any(item.get("givenName") == "Ada" and item.get("familyName") == "Example" for item in entities))
            value = next(item for item in entities if item.get("@type") == "PropertyValue" and str(item.get("value")) == "37")
            self.assertEqual(value["unitText"], "degree Celsius")
            self.assertIn("0000027", value["unitCode"])
            self.assertIn("0000146", value["propertyID"])

    def test_sections_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scaffold(root)
            path = root / "isa.investigation.xlsx"
            book = load_workbook(path)
            sheet = book["isa_investigation"]
            row = next(index for index, values in enumerate(sheet.iter_rows(values_only=True), 1) if values[0] == "INVESTIGATION CONTACTS")
            sheet.delete_rows(row)
            book.save(path)
            book.close()
            with self.assertRaisesRegex(ValueError, "required sections"):
                conversion.convert({"mode": "inspect", "files": files(root)})

    def test_isa_authoritative(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scaffold(root)
            path = root / "isa.investigation.xlsx"
            book = load_workbook(path)
            sheet = book["isa_investigation"]
            row = next(index for index, values in enumerate(sheet.iter_rows(values_only=True), 1) if values[0] == "Investigation Title")
            sheet.cell(row, 2, "Changed ISA title")
            book.save(path)
            book.close()
            result = conversion.convert({"mode": "inspect", "files": files(root)})
            dataset = next(item for item in result["rocrate"]["@graph"] if item.get("@id") == "./")
            self.assertEqual(dataset["name"], "Changed ISA title")

    def test_registration_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scaffold(root)
            request = files(root)
            del request["studies/study/isa.study.xlsx"]
            with self.assertRaisesRegex(ValueError, "registered ISA study"):
                conversion.convert({"mode": "inspect", "files": request})

    def test_data_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scaffold(root)
            request = files(root)
            del request["assays/assay/dataset/measurements.bin"]
            with self.assertRaisesRegex(ValueError, "data is missing"):
                conversion.convert({"mode": "inspect", "files": request})

    def test_paths_confined(self):
        for path in ["../escape", "/tmp/escape", ".git/config", "folder/../../escape"]:
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "unsafe ARC path"):
                conversion.convert({"mode": "inspect", "files": {path: ""}})

    def test_external_entities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "arc"
            scaffold(root)
            private = Path(directory) / "outside.txt"
            private.write_text("outside marker")
            path = root / "isa.investigation.xlsx"
            with zipfile.ZipFile(path) as archive:
                entries = {name: archive.read(name) for name in archive.namelist()}
            name = "xl/worksheets/sheet1.xml"
            xml = entries[name]
            self.assertIn(b"Synthetic ARC experiment", xml)
            xml = xml.replace(b"Synthetic ARC experiment", b"&outside;")
            declaration = ('<!DOCTYPE worksheet [<!ENTITY outside SYSTEM "' + private.as_uri() + '">]>').encode()
            if xml.startswith(b"<?xml"):
                prefix, xml = xml.split(b"?>", 1)
                xml = prefix + b"?>" + declaration + xml
            else:
                xml = declaration + xml
            entries[name] = xml
            with zipfile.ZipFile(path, "w") as archive:
                for name, content in entries.items():
                    archive.writestr(name, content)
            with self.assertRaises(Exception):
                conversion.convert({"mode": "inspect", "files": files(root)})

    def test_facts_required(self):
        document = source()
        del document["@graph"][1]["description"]
        with self.assertRaisesRegex(ValueError, "description"):
            conversion.convert({"mode": "generate", "document_id": "document-id", "jsonld": json.dumps(document)})

    def test_cwl_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "workflow.cwl"
            path.write_text("cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\ninputs: []\noutputs: []\n")
            with patch("requests.sessions.Session.request", side_effect=AssertionError("network access forbidden")):
                conversion.cwl(root)
                path.write_text("cwlVersion: v1.2\nclass: CommandLineTool\ninputs: wrong\noutputs: []\n")
                with self.assertRaisesRegex(ValueError, "CWL v1.2"):
                    conversion.cwl(root)

    def test_cwl_confined(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "workflow.cwl"
            for target in ["https://example.invalid/secret.cwl", "file:///etc/passwd"]:
                path.write_text("$import: " + target + "\n")
                with patch("requests.sessions.Session.request", side_effect=AssertionError("network access forbidden")):
                    with self.subTest(target=target), self.assertRaises((ValueError, ValidationException)):
                        conversion.cwl(root)


if __name__ == "__main__":
    unittest.main()
