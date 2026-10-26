"""Checks the load generator statistics and its modes against the synthetic source."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import json
from argparse import Namespace
from unittest.mock import MagicMock

import pytest

import loadgen
from test_synth import serve


def test_percentile():
    values = list(range(1, 101))
    assert loadgen.percentile(values, 50) == 50
    assert loadgen.percentile(values, 99) == 99
    assert loadgen.percentile([7], 99) == 7
    assert loadgen.percentile([], 50) == 0.0


def test_run_ops():
    def task(index):
        if index == 3:
            raise RuntimeError("boom")
        return 10

    samples, errors, seconds = loadgen.run_ops(6, 3, task)
    result = loadgen.summarize("get", samples, errors, seconds)
    assert result["ops"] == 5 and result["errors"] == 1 and result["bytes"] == 50
    assert errors == ["RuntimeError: boom"]


def test_http_mode(capsys):
    with serve((1, 3, 2), sizes="fixed:5000") as server:
        host, port = server.server_address
        base = f"http://{host}:{port}"
        paths = ["bucket-0/folder-0/file-0.dat", "bucket-0/folder-1/file-1.dat"]
        argv = ["http", "--rest-url", base + "/", "--count", "6", "--concurrency", "3", "--json"]
        for path in paths:
            argv += ["--path", path]
        assert loadgen.main(argv) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["ops"] == 6 and result["errors"] == 0 and result["bytes"] == 30000
        assert loadgen.main(argv[:-1] + ["bucket-0/missing"]) == 1


def test_get_modes(capsys):
    pytest.importorskip("boto3")
    with serve((1, 3, 4), sizes="fixed:100000") as server:
        host, port = server.server_address
        base = ["--s3-url", f"http://{host}:{port}", "--bucket", "bucket-0", "--anonymous"]
        base += ["--prefix", "folder-1/", "--count", "8", "--concurrency", "4", "--json"]
        assert loadgen.main(["get"] + base) == 0
        assert json.loads(capsys.readouterr().out)["bytes"] == 800000
        assert loadgen.main(["range"] + base + ["--range-size", "1000"]) == 0
        assert json.loads(capsys.readouterr().out)["bytes"] == 8000


def test_multipart_calls():
    client = MagicMock()
    client.create_multipart_upload.return_value = {"UploadId": "u1"}
    client.upload_part.return_value = {"ETag": "e"}
    args = Namespace(bucket="b", prefix="p/", seed=1, size=25, part_size=10)
    task = loadgen.multipart_task(client, args)
    assert task(2) == 25
    sizes = [len(call.kwargs["Body"]) for call in client.upload_part.call_args_list]
    assert sizes == [10, 10, 5]
    done = client.complete_multipart_upload.call_args.kwargs["MultipartUpload"]["Parts"]
    assert [part["PartNumber"] for part in done] == [1, 2, 3]
    assert client.complete_multipart_upload.call_args.kwargs["Key"] == "p/00000002"
    client.upload_part.side_effect = RuntimeError("down")
    with pytest.raises(RuntimeError):
        task(3)
    client.abort_multipart_upload.assert_called_once()


def test_put_calls():
    client = MagicMock()
    task = loadgen.put_task(client, Namespace(bucket="b", prefix="p/", seed=1, size=64))
    assert task(7) == 64
    call = client.put_object.call_args.kwargs
    assert call["Key"] == "p/00000007" and len(call["Body"]) == 64
