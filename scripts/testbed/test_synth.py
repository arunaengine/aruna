"""Checks the synthetic source: paging, delimiters, HEAD and GET, manifests and huge layouts."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import hashlib
import http.client
import threading
import time
import tracemalloc
import xml.etree.ElementTree as ET
from contextlib import contextmanager

import pytest

from synth import Layout, Source, parse_range

NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


@contextmanager
def serve(shape=(2, 5, 3), **options):
    config = {key: options.pop(key) for key in ("seed", "sizes", "manifests") if key in options}
    layout = Layout(shape, **config)
    server = Source(("127.0.0.1", 0), layout, **options)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def call(server, path, method="GET", headers=None):
    conn = http.client.HTTPConnection(*server.server_address)
    conn.request(method, path, headers=headers or {})
    resp = conn.getresponse()
    body = resp.read()
    conn.close()
    return resp, body


def page(server, query, bucket="bucket-0"):
    resp, body = call(server, f"/{bucket}?list-type=2&{query}")
    assert resp.status == 200
    root = ET.fromstring(body)
    return {
        "keys": [
            (
                item.findtext(f"{NS}Key"),
                int(item.findtext(f"{NS}Size")),
                item.findtext(f"{NS}ETag").strip('"'),
            )
            for item in root.findall(f"{NS}Contents")
        ],
        "prefixes": [item.findtext(f"{NS}Prefix") for item in root.findall(f"{NS}CommonPrefixes")],
        "token": root.findtext(f"{NS}NextContinuationToken"),
        "truncated": root.findtext(f"{NS}IsTruncated") == "true",
        "count": int(root.findtext(f"{NS}KeyCount")),
    }


def walk(server, query, bucket="bucket-0"):
    keys, prefixes, token = [], [], None
    while True:
        more = f"&continuation-token={token}" if token else ""
        got = page(server, query + more, bucket)
        keys += got["keys"]
        prefixes += got["prefixes"]
        token = got["token"]
        assert got["truncated"] == (token is not None)
        if not token:
            return keys, prefixes


def expected(folders, files, manifests=()):
    fw, nw = len(str(folders - 1)), len(str(files - 1))
    names = sorted(list(manifests) + [f"file-{n:0{nw}d}.dat" for n in range(files)])
    return [f"folder-{f:0{fw}d}/{name}" for f in range(folders) for name in names]


def test_pages_match():
    with serve((2, 12, 4)) as server:
        whole = walk(server, "max-keys=1000")[0]
        assert [key for key, _, _ in whole] == expected(12, 4)
        for size in (1, 3, 4, 5, 47, 48, 49):
            assert walk(server, f"max-keys={size}")[0] == whole


def test_start_after():
    with serve((1, 4, 3), manifests=["md5", "sha256"]) as server:
        keys = [key for key, _, _ in walk(server, "")[0]]
        assert keys == expected(4, 3, ["MD5SUMS", "SHA256SUMS"])
        for index, key in enumerate(keys):
            got = page(server, f"start-after={key}&max-keys=2")
            assert [item[0] for item in got["keys"]] == keys[index + 1 : index + 3]
        got = page(server, "start-after=folder-1/&max-keys=1")
        assert got["keys"][0][0] == "folder-1/MD5SUMS"


def test_prefix_bounds():
    with serve((1, 12, 3)) as server:
        keys = [key for key, _, _ in walk(server, "prefix=folder-1")[0]]
        assert keys == [key for key in expected(12, 3) if key.startswith("folder-1")]
        assert walk(server, "prefix=nothing")[0] == []
        assert walk(server, "prefix=folder-03/file-1")[0][0][0] == "folder-03/file-1.dat"


def test_delimiter_list():
    with serve((1, 9, 3)) as server:
        keys, prefixes = walk(server, "delimiter=/&max-keys=2")
        assert keys == []
        assert prefixes == [f"folder-{n}/" for n in range(9)]
        keys, prefixes = walk(server, "delimiter=/&prefix=folder-4/&max-keys=2")
        assert prefixes == [] and len(keys) == 3
        got = page(server, "delimiter=/&prefix=folder-&max-keys=4")
        assert got["prefixes"] == [f"folder-{n}/" for n in range(4)] and got["count"] == 4
        assert walk(server, "delimiter=-&prefix=folder-1/&max-keys=2")[1] == ["folder-1/file-"]


def test_head_get():
    with serve((1, 3, 4), sizes="uniform:0:300000", seed=7) as server:
        for key, size, etag in walk(server, "")[0]:
            head, empty = call(server, f"/bucket-0/{key}", "HEAD")
            got, body = call(server, f"/bucket-0/{key}")
            assert empty == b"" and head.status == got.status == 200
            assert int(head.getheader("Content-Length")) == size == len(body)
            assert head.getheader("ETag") == got.getheader("ETag") == f'"{etag}"'
            assert hashlib.md5(body).hexdigest() == etag
            assert call(server, f"/bucket-0/{key}")[1] == body
    assert fetch(7, "folder-1/file-2.dat") == fetch(7, "folder-1/file-2.dat")
    assert fetch(7, "folder-1/file-2.dat") != fetch(8, "folder-1/file-2.dat")


def fetch(seed, key):
    with serve((1, 3, 4), sizes="uniform:1:300000", seed=seed) as server:
        return call(server, f"/bucket-0/{key}")[1]


def test_ranges():
    with serve((1, 2, 2), sizes="fixed:200000") as server:
        path = "/bucket-0/folder-0/file-1.dat"
        full = call(server, path)[1]
        for header, part in (
            ("bytes=0-9", full[0:10]),
            ("bytes=65530-65540", full[65530:65541]),
            ("bytes=131000-", full[131000:]),
            ("bytes=-70000", full[-70000:]),
            ("bytes=199990-999999", full[199990:]),
        ):
            resp, body = call(server, path, headers={"Range": header})
            assert resp.status == 206 and body == part
            assert int(resp.getheader("Content-Length")) == len(part)
        assert call(server, path, headers={"Range": "bytes=200000-"})[0].status == 416
        etag = call(server, path)[0].getheader("ETag")
        assert call(server, path, headers={"If-Match": etag})[0].status == 200
        assert call(server, path, headers={"If-Match": '"0"'})[0].status == 412
        assert call(server, path, headers={"If-None-Match": etag})[0].status == 304
        assert parse_range("bytes=1-2,4-5", 10) is None


def test_manifests():
    with serve((1, 3, 5), sizes="uniform:0:70000", manifests=["md5", "sha256"]) as server:
        keys = [key for key, _, _ in walk(server, "prefix=folder-2/")[0]]
        assert keys[:2] == ["folder-2/MD5SUMS", "folder-2/SHA256SUMS"]
        files = {key: call(server, f"/bucket-0/{key}")[1] for key in keys}
        for name, algo in (("MD5SUMS", "md5"), ("SHA256SUMS", "sha256")):
            lines = files[f"folder-2/{name}"].decode().splitlines()
            assert len(lines) == 5
            for line in lines:
                digest, rest = line.split("  ")
                data = files[f"folder-2/{rest}"]
                assert digest == hashlib.new(algo, data).hexdigest()
        listed = {key: (size, etag) for key, size, etag in walk(server, "prefix=folder-2/")[0]}
        text = files["folder-2/MD5SUMS"]
        assert listed["folder-2/MD5SUMS"] == (len(text), hashlib.md5(text).hexdigest())


def test_huge_layout():
    with serve((1, 10**6, 10**6), sizes="fixed:16") as huge, serve((1, 100, 100)) as small:
        deep = "start-after=folder-999990/file-500000.dat&max-keys=1000"
        near = "start-after=folder-05/file-05.dat&max-keys=1000"
        tracemalloc.start()
        peaks = []
        for server, query in ((small, near), (huge, deep)):
            tracemalloc.reset_peak()
            started = time.monotonic()
            resp, _ = call(server, f"/bucket-0?list-type=2&{query}")
            peaks.append(tracemalloc.get_traced_memory()[1])
            assert resp.status == 200 and time.monotonic() - started < 30
        tracemalloc.stop()
        assert peaks[1] <= peaks[0] * 2 + (1 << 20)
        got = page(huge, deep)
        assert got["count"] == 1000 and got["truncated"]
        assert got["keys"][0][0] == "folder-999990/file-500001.dat"
        got = page(huge, "delimiter=/&max-keys=1000&start-after=folder-777777/z")
        assert got["prefixes"][0] == "folder-777778/" and len(got["prefixes"]) == 1000
        resp, body = call(huge, "/bucket-0/folder-999999/file-999999.dat")
        assert resp.status == 200 and hashlib.md5(body).hexdigest() == resp.getheader("ETag").strip('"')
        assert call(huge, "/bucket-0/folder-1000000/file-000000.dat")[0].status == 404


def test_server_limits():
    with serve((2, 4, 4), page_cap=5) as server:
        got = page(server, "max-keys=1000")
        assert got["count"] == 5 and got["truncated"]
        assert call(server, "/bucket-2?list-type=2")[0].status == 404
        assert call(server, "/other?list-type=2")[0].status == 404
        assert call(server, "/bucket-0/folder-0/missing")[0].status == 404
        assert call(server, "/bucket-0?list-type=2&max-keys=-1")[0].status == 400
        assert call(server, "/bucket-0?list-type=2&continuation-token=%ff")[0].status == 400
        assert call(server, "/bucket-0/folder-0/file-0.dat", "PUT")[0].status == 403
        creds = {"Authorization": "AWS4-HMAC-SHA256 Credential=any"}
        assert call(server, "/bucket-0/folder-0/file-0.dat", headers=creds)[0].status == 200
    with serve(rate=0.001) as server:
        statuses = [call(server, "/bucket-0?list-type=2")[0].status for _ in range(3)]
        assert statuses == [200, 503, 503]
    with serve(latency=0.2) as server:
        started = time.monotonic()
        call(server, "/bucket-0?list-type=2")
        assert time.monotonic() - started >= 0.2


def test_bad_layout():
    with pytest.raises(ValueError):
        Layout((1, 1, 1), names={"file": "A-{}"}, manifests=["md5"])
    with pytest.raises(ValueError):
        Layout((1, 1, 1), names={"folder": "no-slot"})
    with pytest.raises(ValueError):
        Layout((1, 1, 1), sizes="bogus:1")


def test_boto_client():
    boto3 = pytest.importorskip("boto3")
    from botocore import UNSIGNED
    from botocore.config import Config

    with serve((1, 6, 4)) as server:
        host, port = server.server_address
        config = Config(signature_version=UNSIGNED, s3={"addressing_style": "path"})
        client = boto3.client("s3", endpoint_url=f"http://{host}:{port}", config=config)
        pager = client.get_paginator("list_objects_v2")
        keys = [
            item["Key"]
            for got in pager.paginate(Bucket="bucket-0", PaginationConfig={"PageSize": 5})
            for item in got.get("Contents", [])
        ]
        assert keys == expected(6, 4)
        head = client.head_object(Bucket="bucket-0", Key=keys[3])
        body = client.get_object(Bucket="bucket-0", Key=keys[3])["Body"].read()
        assert head["ContentLength"] == len(body)
        assert head["ETag"] == f'"{hashlib.md5(body).hexdigest()}"'
