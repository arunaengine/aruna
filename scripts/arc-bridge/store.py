"""Stores verified LFS versions and publishes private metadata through Aruna APIs."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import base64
import hashlib
import json
import os
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import boto3
from botocore.config import Config


OID = re.compile(r"[0-9a-f]{64}\Z")
NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")
MAX_OBJECT = 64 * 1024 * 1024


def object_id(value):
    if not isinstance(value, str) or not OID.fullmatch(value):
        raise ValueError("invalid LFS SHA-256")
    return value


class Store:
    def __init__(self, state, name):
        if not NAME.fullmatch(name):
            raise ValueError("repository name must be a lowercase slug")
        self.state = Path(state).resolve()
        self.state.mkdir(parents=True, exist_ok=True)
        self.name = name
        self.bucket = os.environ["ARUNA_BUCKET"]
        self.group = os.environ["ARUNA_GROUP_ID"]
        self.api = os.environ["ARUNA_API_URL"].rstrip("/") + "/api/v1"
        self.token = os.environ["ARUNA_TOKEN"]
        self.endpoint = os.environ["ARUNA_S3_URL"].rstrip("/")
        self.s3 = boto3.client(
            "s3", endpoint_url=self.endpoint,
            region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                          retries={"max_attempts": 2}, connect_timeout=15, read_timeout=120),
        )
        with self.database() as db:
            db.execute("CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT)")
            binding = json.dumps([name, self.endpoint, self.bucket, self.api, self.group])
            db.execute("INSERT OR IGNORE INTO settings VALUES ('binding', ?)", (binding,))
            if db.execute("SELECT value FROM settings WHERE name='binding'").fetchone()[0] != binding:
                raise ValueError("state directory belongs to a different Aruna repository binding")
            db.execute("CREATE TABLE IF NOT EXISTS objects (oid TEXT PRIMARY KEY, size INTEGER, version TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS publications (commit_id TEXT PRIMARY KEY, document TEXT)")

    @contextmanager
    def database(self):
        connection = sqlite3.connect(self.state / "bridge.sqlite", timeout=30)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def key(self, oid):
        return f"arc-lfs/{self.name}/{object_id(oid)}"

    def find(self, oid):
        with self.database() as db:
            return db.execute("SELECT size, version FROM objects WHERE oid=?", (object_id(oid),)).fetchone()

    def check(self, oid, size):
        record = self.find(oid)
        if record is None or record[0] != size:
            raise ValueError("LFS object is not verified for this repository")
        head = self.s3.head_object(Bucket=self.bucket, Key=self.key(oid), VersionId=record[1])
        if head["ContentLength"] != size:
            raise ValueError("stored LFS size changed")
        return record

    def put(self, oid, source, size):
        object_id(oid)
        if not 0 <= size <= MAX_OBJECT:
            raise ValueError("LFS payload exceeds the PoC limit")
        length = source.seek(0, 2)
        source.seek(0)
        digest = hashlib.file_digest(source, "sha256").hexdigest()
        if digest != oid or length != size:
            raise ValueError("LFS payload digest or size mismatch")
        if self.find(oid):
            return self.check(oid, size)
        source.seek(0)
        result = self.s3.put_object(
            Bucket=self.bucket, Key=self.key(oid), Body=source, ContentLength=size,
            ChecksumSHA256=base64.b64encode(bytes.fromhex(oid)).decode(),
        )
        version = result.get("VersionId")
        if not version:
            raise ValueError("Aruna did not return an exact object version")
        with self.database() as db:
            db.execute("INSERT OR IGNORE INTO objects VALUES (?, ?, ?)", (oid, size, version))
        return self.check(oid, size)

    def get(self, oid):
        record = self.find(oid)
        if record is None:
            raise ValueError("unknown LFS object")
        return self.s3.get_object(Bucket=self.bucket, Key=self.key(oid), VersionId=record[1])

    def request(self, method, route, body=None):
        payload = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            self.api + route, data=payload, method=method,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)

    def publish(self, commit, metadata, files):
        with self.database() as db:
            saved = db.execute("SELECT document FROM publications WHERE commit_id=?", (commit,)).fetchone()
        if saved:
            return json.loads(saved[0])
        path = f"arc-poc/{self.name}/{commit}"
        query = urllib.parse.urlencode({"path": path})
        try:
            document = self.request("GET", f"/metadata/groups/{self.group}/path?{query}")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            graph = metadata["@graph"]
            root = next(entity for entity in graph if entity.get("@id") == "./")
            root["version"] = commit
            root.setdefault("hasPart", [])
            for path, oid, size in files:
                record = self.check(oid, size)
                url = (self.endpoint + "/" + self.bucket + "/" + self.key(oid)
                       + "?" + urllib.parse.urlencode({"versionId": record[1]}))
                entity_id = urllib.parse.quote(path, safe="/")
                entity = next((item for item in graph if item.get("@id") == entity_id), None)
                if entity is None:
                    entity = {"@id": entity_id, "@type": "File", "name": path}
                    graph.append(entity)
                entity.update(contentSize=str(size), contentUrl=url,
                              sha256=oid, encodingFormat="application/octet-stream")
                if {"@id": entity_id} not in root["hasPart"]:
                    root["hasPart"].append({"@id": entity_id})
            document = self.request("POST", "/metadata", {
                "group_id": self.group, "path": f"arc-poc/{self.name}/{commit}",
                "public": False, "rocrate": metadata,
            })
        with self.database() as db:
            db.execute("INSERT OR REPLACE INTO publications VALUES (?, ?)", (commit, json.dumps(document)))
        return document
