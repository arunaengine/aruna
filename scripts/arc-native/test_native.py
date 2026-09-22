"""Exercises the native Aruna endpoint with real Git/LFS and optional ARCitect."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import datetime
import hashlib
import json
import os
import subprocess
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

from arc import scaffold
import boto3
from botocore.config import Config


def command(directory, env, *args, success=True):
    result = subprocess.run(["git", "-c", "credential.helper=", "-C", str(directory), *args],
                            env=env, capture_output=True, timeout=180)
    if (result.returncode == 0) != success:
        raise AssertionError(f"Git {args[0]} returned {result.returncode}: {result.stderr.decode()}")
    return result.stdout


def commit(directory, env, subject):
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    env = dict(env, GIT_AUTHOR_DATE=timestamp, GIT_COMMITTER_DATE=timestamp)
    command(directory, env, "diff", "--cached", "--check")
    command(directory, env, "diff", "--cached", "--")
    command(directory, env, "commit", "-S", "-m", subject)
    return command(directory, env, "rev-parse", "HEAD").decode().strip()


def http(url, method="GET", body=None, token=None):
    headers = {"Authorization": "Bearer " + (token or os.environ["ARUNA_TOKEN"])}
    if isinstance(body, dict):
        body = json.dumps(body).encode()
        headers["Content-Type"] = "application/vnd.git-lfs+json"
    request = urllib.request.Request(url, method=method, data=body, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def exercise(root):
    url = os.environ["ARUNA_GIT_URL"]
    source = root / "source"
    scaffold(source)
    askpass = root / "askpass"
    askpass.write_text('#!/bin/sh\ncase "$1" in *Username*) printf "aruna\\n";; *) printf "%s\\n" "$ARUNA_TOKEN";; esac\n')
    askpass.chmod(0o700)
    env = dict(os.environ, GIT_ASKPASS=str(askpass), GIT_TERMINAL_PROMPT="0")
    command(source, env, "init", "--initial-branch=main")
    command(source, env, "lfs", "install", "--local", "--skip-repo")
    command(source, env, "remote", "add", "origin", url)
    command(source, env, "add", ".gitattributes", "isa.investigation.xlsx", "ro-crate-metadata.json", "studies", "assays", "runs", "workflows")
    first = commit(source, env, "test: create native ARC revision")
    payload = "assays/assay/dataset/measurements.bin"
    original = (source / payload).read_bytes()
    oid = hashlib.sha256(original).hexdigest()
    command(source, env, "lfs", "push", "origin", "main")
    command(source, env, "push", "--atomic", "origin", "main")
    assert http(url + "/info/refs?service=git-upload-pack", token="invalid")[0] == 401
    if os.environ.get("ARUNA_READ_TOKEN"):
        assert http(url + "/info/refs?service=git-upload-pack", token=os.environ["ARUNA_READ_TOKEN"])[0] == 200
        assert http(url + "/info/refs?service=git-receive-pack", token=os.environ["ARUNA_READ_TOKEN"])[0] == 403
    assert http(url + "/info/lfs/objects/" + "a" * 64, "PUT", b"incorrect")[0] == 400

    (source / payload).write_bytes(b"native second revision\n" * 4096)
    command(source, env, "add", payload)
    second = commit(source, env, "test: update native ARC payload")
    command(source, env, "lfs", "push", "origin", "main")
    command(source, env, "push", "--atomic", "origin", "main")
    s3 = boto3.client("s3", endpoint_url=os.environ["ARUNA_S3_URL"],
                      config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
    key = f"git-lfs/{os.environ['ARUNA_DOCUMENT_ID']}/{oid}"
    s3.put_object(Bucket=os.environ["ARUNA_BUCKET"], Key=key, Body=b"overwritten key head")
    command(root, dict(env, GIT_LFS_SKIP_SMUDGE="1"), "clone", url, "clone")
    clone = root / "clone"
    command(clone, env, "lfs", "install", "--local", "--skip-repo")
    command(clone, env, "checkout", first)
    command(clone, env, "lfs", "pull")
    assert (clone / payload).read_bytes() == original
    print("PASS: native Git/LFS, exact historical versions and Aruna authentication", flush=True)

    command(source, env, "checkout", "-b", "missing", "main")
    (source / payload).write_bytes(b"missing native LFS payload")
    command(source, env, "add", payload)
    commit(source, env, "test: reject unavailable native payload")
    command(source, env, "push", "origin", "missing", success=False)
    assert not command(source, env, "ls-remote", "origin", "refs/heads/missing")
    command(source, env, "checkout", "-b", "invalid", "main")
    command(source, env, "rm", "isa.investigation.xlsx")
    commit(source, env, "test: reject invalid native ARC")
    command(source, env, "push", "--atomic", "origin", "main:atomic-good", "invalid:atomic-bad", success=False)
    assert not command(source, env, "ls-remote", "origin", "refs/heads/atomic-*")
    assert second in command(source, env, "ls-remote", "origin", "refs/heads/main").decode()
    print("PASS: missing LFS and invalid ARC rejected; atomic push publishes neither ref", flush=True)

    command(source, env, "branch", "feature", first)
    command(source, env, "tag", "snapshot", first)
    command(source, env, "tag", "-s", "release", "-m", "test: label ARC release", second)
    command(source, env, "push", "--atomic", "origin", "feature", "refs/tags/snapshot", "refs/tags/release")
    refs = command(source, env, "ls-remote", "origin").decode()
    assert f"{first}\trefs/heads/feature" in refs
    assert f"{first}\trefs/tags/snapshot" in refs
    assert f"{second}\trefs/tags/release^{{}}" in refs
    command(source, env, "tag", "-f", "snapshot", second)
    command(source, env, "push", "--force", "origin", "refs/tags/snapshot", success=False)
    command(source, env, "push", "origin", ":refs/heads/feature", ":refs/tags/snapshot")
    assert not command(source, env, "ls-remote", "origin", "refs/heads/feature", "refs/tags/snapshot")
    print("PASS: multiple branches, lightweight/signed annotated tags and ref deletion", flush=True)

    if os.environ.get("ARUNA_ARCITECT"):
        subprocess.run(["node", str(Path(__file__).with_name("test_arcitect.mjs")), str(root)],
                       env=env, check=True, timeout=600)
    else:
        print("ARCitect application test not requested in this invocation", flush=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="aruna-native-git-") as directory:
        exercise(Path(directory))
