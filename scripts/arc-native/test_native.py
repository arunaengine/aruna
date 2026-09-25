"""Exercises the native Aruna endpoint with real Git/LFS and optional ARCitect."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import datetime
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from arc import scaffold
from openpyxl import load_workbook
import boto3
from botocore.config import Config


def command(directory, env, *args, success=True):
    result = subprocess.run(["git", "-c", "credential.helper=", "-C", str(directory), *args],
                            env=env, capture_output=True, timeout=180)
    if success is None:
        return result.stdout if result.returncode == 0 else None
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


def api(url, method="GET", body=None, headers=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, method=method, data=data, headers={
        "Authorization": "Bearer " + os.environ["ARUNA_TOKEN"],
        "Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            status, text = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, text = error.code, error.read()
    return status, json.loads(text) if text.startswith(b"{") else text


def wait_snapshot(url, previous):
    deadline = time.monotonic() + 300
    while True:
        status, body = http(url + "/git")
        assert status in (200, 503), body
        if status == 200:
            updated = json.loads(body)
            assert updated["error"] is None, updated
            if updated["commit"] != previous:
                return updated["commit"]
        assert time.monotonic() < deadline, "metadata snapshot did not advance"
        time.sleep(0.1)


def remote_main(directory, env):
    return command(directory, env, "ls-remote", "origin", "refs/heads/main").decode().split()[0]


def wait_main(directory, env, previous):
    deadline = time.monotonic() + 300
    while (current := remote_main(directory, env)) == previous:
        assert time.monotonic() < deadline, "metadata was not merged into main"
        time.sleep(0.1)
    return current


def graph(metadata_url):
    status, body = http(metadata_url + "/rocrate?view=raw")
    assert status == 200, body
    return json.loads(body)["raw"]


def wait_graph(metadata_url, name):
    deadline = time.monotonic() + 300
    while root_entity(current := graph(metadata_url))["name"] != name:
        assert time.monotonic() < deadline, "pushed ISA edit did not reach the metadata graph"
        time.sleep(0.1)
    return current


def root_entity(document):
    descriptor = next(item for item in document["@graph"] if item.get("@id") == "ro-crate-metadata.json")
    return next(item for item in document["@graph"] if item.get("@id") == descriptor["about"]["@id"])


def derived_difference(metadata_url, *revisions):
    graphs = []
    for revision in revisions:
        status, body = http(metadata_url + "/git/rocrate?revision=" + revision)
        assert status == 200, body
        graphs.append({json.dumps(item, sort_keys=True) for item in json.loads(body)["rocrate"]["@graph"]})
    return sorted(graphs[0] ^ graphs[1])


def exercise(root):
    url = os.environ["ARUNA_GIT_URL"]
    metadata_url = os.environ["ARUNA_API_URL"] + "/api/v1/metadata/" + os.environ["ARUNA_DOCUMENT_ID"]
    source = root / "source"
    askpass = root / "askpass"
    askpass.write_text('#!/bin/sh\ncase "$1" in *Username*) printf "aruna\\n";; *) printf "%s\\n" "$ARUNA_TOKEN";; esac\n')
    askpass.chmod(0o700)
    env = dict(os.environ, GIT_ASKPASS=str(askpass), GIT_TERMINAL_PROMPT="0")
    command(root, env, "clone", url, str(source))
    initial = command(source, env, "rev-parse", "HEAD").decode().strip()
    assert command(source, env, "log", "-1", "--format=%G?").strip() == b"G"
    assert (source / "isa.investigation.xlsx").is_file()
    assert (source / "ro-crate-metadata.json").is_file()
    generated = json.loads((source / "ro-crate-metadata.json").read_text())
    generated_root = next(item for item in generated["@graph"] if item.get("@id") == "./")
    assert generated_root["license"] == {"@id": "https://creativecommons.org/licenses/by/4.0/"}
    assert not list(source.glob("studies/*/isa.study.xlsx"))
    assert not list(source.glob("assays/*/isa.assay.xlsx"))
    original_metadata = json.loads((source / "aruna-metadata.json").read_text())
    for _ in range(2):
        status, body = http(metadata_url + "/git")
        assert status == 200 and json.loads(body)["commit"] == initial
    descriptor = next(item for item in original_metadata["@graph"] if item.get("@id") == "ro-crate-metadata.json")
    dataset = next(item for item in original_metadata["@graph"] if item.get("@id") == descriptor["about"]["@id"])
    dataset["name"] = "First metadata update"
    assert http(metadata_url + "/rocrate", "PUT", {"rocrate": original_metadata})[0] == 200
    initial = wait_snapshot(metadata_url, initial)
    assert initial in command(source, env, "ls-remote", "origin", "refs/heads/main").decode()
    command(source, env, "pull", "--ff-only", "origin", "main")
    original_metadata = json.loads((source / "aruna-metadata.json").read_text())
    fixture = root / "fixture"
    scaffold(fixture)
    shutil.copytree(fixture, source, dirs_exist_ok=True)
    command(source, env, "lfs", "install", "--local", "--skip-repo")
    command(source, env, "add", ".gitattributes", "isa.investigation.xlsx", "ro-crate-metadata.json", "studies", "assays", "runs", "workflows")
    first = commit(source, env, "test: create native ARC revision")
    payload = "assays/assay/dataset/measurements.bin"
    original = (source / payload).read_bytes()
    oid = hashlib.sha256(original).hexdigest()
    command(source, env, "lfs", "push", "origin", "main")
    command(source, env, "push", "--atomic", "origin", "main")
    merged = wait_main(source, env, first)
    command(source, env, "pull", "--ff-only", "origin", "main")
    assert command(source, env, "rev-parse", "HEAD^1").decode().strip() == first
    assert not command(source, env, "diff", "--name-only", first, "HEAD", "--", "*.xlsx"), \
        derived_difference(metadata_url, first, merged)
    assert any(item.get("additionalType") == "Study" for item in graph(metadata_url)["@graph"])
    assert any(item.get("additionalType") == "Study"
               for item in json.loads((source / "aruna-metadata.json").read_text())["@graph"])
    print("PASS: pushed ISA studies merged into metadata and back without rewriting workbooks", flush=True)
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
    command(source, env, "push", "origin", ":main", success=False)
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

    command(source, env, "push", "origin", "main:aruna", success=False)
    status, body = http(metadata_url + "/git/rocrate?revision=" + first)
    assert status == 200
    exported = json.loads(body)
    assert exported["commit"] == first
    assert any(item.get("additionalType") == "Study" for item in exported["rocrate"]["@graph"])
    snapshot = json.loads(http(metadata_url + "/git")[1])["commit"]
    current = graph(metadata_url)
    root_entity(current)["name"] = "Updated native metadata"
    root_entity(current)["https://example.org/custom"] = "preserved extension"
    assert http(metadata_url + "/rocrate", "PUT", {"rocrate": current})[0] == 200
    wait_snapshot(metadata_url, snapshot)
    command(source, env, "checkout", "main")
    command(source, env, "pull", "--ff-only", "origin", "main")
    command(source, env, "merge-base", "--is-ancestor", second, "HEAD")
    assert "preserved extension" in (source / "aruna-metadata.json").read_text()
    status, body = http(metadata_url + "/git/rocrate?revision=main")
    assert status == 200 and root_entity(json.loads(body)["rocrate"])["name"] == "Updated native metadata"
    assert not command(source, env, "diff", "--name-only", second, "HEAD", "--", payload)
    print("PASS: graph edits merged into the edited main branch with client files intact", flush=True)

    workbook = load_workbook(source / "isa.investigation.xlsx")
    sheet = workbook["isa_investigation"]
    row = next(row for row in sheet.iter_rows() if row[0].value == "Investigation Title")
    row[1].value = "Edited through Git"
    workbook.save(source / "isa.investigation.xlsx")
    command(source, env, "add", "isa.investigation.xlsx")
    edited = commit(source, env, "test: edit investigation title")
    command(source, env, "push", "origin", "main")
    current = wait_graph(metadata_url, "Edited through Git")
    assert root_entity(current)["https://example.org/custom"] == "preserved extension"
    wait_main(source, env, edited)
    command(source, env, "pull", "--ff-only", "origin", "main")
    assert not command(source, env, "diff", "--name-only", edited, "HEAD", "--", "*.xlsx")
    command(source, env, "checkout", "-b", "draft", "main")
    row[1].value = "Draft title"
    workbook.save(source / "isa.investigation.xlsx")
    command(source, env, "add", "isa.investigation.xlsx")
    commit(source, env, "test: draft investigation title")
    command(source, env, "push", "origin", "draft")
    assert root_entity(graph(metadata_url))["name"] == "Edited through Git"
    command(source, env, "checkout", "main")
    print("PASS: ISA edits on main update metadata; branches stay drafts; signed ARC export", flush=True)

    raw = b"existing Aruna object\n" * 4096
    version = s3.put_object(Bucket=os.environ["ARUNA_BUCKET"], Key="datasets/raw.bin", Body=raw)["VersionId"]
    arn = f"{os.environ['ARUNA_ARN_PREFIX']}/{os.environ['ARUNA_BUCKET']}/datasets/raw.bin@{version}"
    snapshot = json.loads(http(metadata_url + "/git")[1])["commit"]
    current = graph(metadata_url)
    current["@graph"].append({"@id": "#raw", "@type": "File", "name": "raw.bin", "contentUrl": arn})
    parts = root_entity(current).get("hasPart", [])
    root_entity(current)["hasPart"] = (parts if isinstance(parts, list) else [parts]) + [{"@id": "#raw"}]
    status, body = http(metadata_url + "/rocrate", "PUT", {"rocrate": current})
    assert status == 200, body
    wait_snapshot(metadata_url, snapshot)
    command(source, env, "pull", "--ff-only", "origin", "main")
    assert (source / "dataset/raw.bin").read_bytes() == raw
    assert "/dataset/raw.bin filter=lfs" in (source / ".gitattributes").read_text()
    print("PASS: an existing Aruna object appears in the ARC as an LFS file with exact content", flush=True)

    other_env = dict(env, ARUNA_TOKEN=os.environ["ARUNA_OTHER_TOKEN"])
    command(root, other_env, "clone", url, "other")
    other = root / "other"
    command(other, other_env, "lfs", "install", "--local", "--skip-repo")
    command(source, env, "lfs", "lock", "isa.investigation.xlsx")
    assert "isa.investigation.xlsx" in command(other, other_env, "lfs", "locks").decode()
    workbook = load_workbook(other / "isa.investigation.xlsx")
    sheet = workbook["isa_investigation"]
    row = next(row for row in sheet.iter_rows() if row[0].value == "Investigation Title")
    row[1].value = "Edited by another user"
    workbook.save(other / "isa.investigation.xlsx")
    command(other, other_env, "add", "isa.investigation.xlsx")
    commit(other, other_env, "test: edit a locked workbook")
    locked = remote_main(other, other_env)
    command(other, dict(other_env, GIT_LFS_SKIP_PUSH="1"), "-c", "lfs.locksverify=false",
            "push", "origin", "main", success=False)
    assert remote_main(other, other_env) == locked
    assert root_entity(graph(metadata_url))["name"] != "Edited by another user"
    command(source, env, "lfs", "unlock", "isa.investigation.xlsx")
    command(other, other_env, "push", "origin", "main")
    wait_graph(metadata_url, "Edited by another user")
    print("PASS: LFS locks block other users' pushes until released", flush=True)

    locks = url + "/info/lfs/locks"
    for _ in range(140):
        status, body = http(locks, "POST", {"path": "notes/checkpoint.txt"})
        assert status == 201, body
        status, body = http(f"{locks}/{json.loads(body)['lock']['id']}/unlock", "POST", {"force": False})
        assert status == 200, body
    assert http(metadata_url + "/git")[0] == 200
    print("PASS: many Git records fold into a checkpoint instead of reaching the record cap", flush=True)

    head = remote_main(source, env)
    status, page = api(metadata_url + "/versions?limit=2")
    assert status == 200 and page["versions"][0]["version"] == head, page
    assert "main" in page["versions"][0]["branches"] and page["next_cursor"], page
    status, older = api(metadata_url + "/versions?limit=2&cursor=" + page["next_cursor"])
    assert status == 200 and older["versions"][0]["version"] != page["versions"][1]["version"], older
    status, detail = api(metadata_url + "/versions/" + head)
    assert status == 200 and detail["version"] == head and "files" in detail, detail
    status, created = api(metadata_url + "/branches", "POST", {"name": "rest/draft", "from": "main"})
    assert status == 201 and created["version"] == head, created
    assert api(metadata_url + "/branches", "POST", {"name": "rest/draft", "from": "main"})[0] == 409
    status, tag = api(metadata_url + "/tags", "POST", {"name": "rest-v1", "version": head})
    assert status == 201, tag
    draft_url = metadata_url + "/branches/rest%2Fdraft"
    draft = graph(metadata_url)
    root_entity(draft)["name"] = "Edited through REST"
    stale = {"If-Match": '"' + "0" * 40 + '"'}
    assert api(draft_url + "/rocrate", "PUT", {"rocrate": draft}, stale)[0] == 412
    status, edited = api(draft_url + "/rocrate", "PUT",
                         {"rocrate": draft, "message": "Rename through REST"}, {"If-Match": head})
    assert status == 200 and edited["version"] != head and edited["parents"] == [head], edited
    assert edited["message"] == "Rename through REST" and edited["author"]["user_id"], edited
    assert root_entity(graph(metadata_url))["name"] == "Edited by another user"
    status, own = api(metadata_url + "/versions?branch=rest%2Fdraft&since=main")
    assert status == 200 and [item["version"] for item in own["versions"]] == [edited["version"]], own
    status, missing = api(metadata_url + "/versions?branch=nothing-here")
    assert status == 404 and missing["code"] == "branch_missing", missing
    status, whole = api(metadata_url + "/compare?to=rest%2Fdraft")
    assert status == 200 and whole["from"] is None and whole["files"], whole
    status, heads = api(metadata_url + "/branches")
    assert any(item["name"] == "rest/draft" and item["head"]["version"] == edited["version"]
               for item in heads["branches"]), heads
    claim = json.dumps({"refs": [{"name": "refs/heads/main", "old": head, "new": "a" * 40}],
                        "lfs": [], "paths": []}).encode()
    forged = urllib.request.Request(metadata_url + "/git/push", method="POST",
                                    data=len(claim).to_bytes(4, "big") + claim,
                                    headers={"Authorization": "Bearer " + os.environ["ARUNA_TOKEN"]})
    try:
        urllib.request.urlopen(forged, timeout=60)
        raise AssertionError("a push record was accepted outside a receive hook")
    except urllib.error.HTTPError as error:
        assert error.code == 403, error.code
    status, comparison = api(metadata_url + "/compare?from=main&to=rest%2Fdraft")
    assert status == 200 and any(file["path"] == "aruna-metadata.json" for file in comparison["files"])
    assert "Edited through REST" in json.dumps(comparison["entities"]), comparison
    status, merged = api(draft_url + "/merge", "POST", {"into": "main"}, {"If-Match": head})
    assert status == 200, merged
    wait_graph(metadata_url, "Edited through REST")
    print("PASS: REST versions, branches, tags, draft edits and merges into the live metadata", flush=True)

    for name in ("rest/one", "rest/two"):
        assert api(metadata_url + "/branches", "POST", {"name": name, "from": "main"})[0] == 201
    for name, title in (("rest%2Fone", "First competing title"), ("rest%2Ftwo", "Second competing title")):
        draft = graph(metadata_url)
        root_entity(draft)["name"] = title
        assert api(f"{metadata_url}/branches/{name}/rocrate", "PUT", {"rocrate": draft})[0] == 200
    assert api(metadata_url + "/branches/rest%2Fone/merge", "POST", {"into": "main"})[0] == 200
    wait_graph(metadata_url, "First competing title")
    status, conflict = api(metadata_url + "/branches/rest%2Ftwo/merge", "POST", {"into": "main"})
    assert status == 409 and any(item["property"] == "name" for item in conflict["properties"]), conflict
    assert root_entity(graph(metadata_url))["name"] == "First competing title"
    draft = graph(metadata_url)
    assert api(metadata_url + "/branches/rest%2Ftwo/rocrate", "PUT", {"rocrate": draft})[0] == 200
    assert api(metadata_url + "/branches/rest%2Ftwo/merge", "POST", {"into": "main"})[0] == 200
    for name in ("rest%2Fdraft", "rest%2Fone", "rest%2Ftwo"):
        assert api(f"{metadata_url}/branches/{name}", "DELETE")[0] == 204
    assert api(metadata_url + "/branches/main", "DELETE")[0] == 400
    assert api(metadata_url + "/tags/rest-v1", "DELETE")[0] == 204
    status, branches = api(metadata_url + "/branches")
    assert status == 200 and not any(item["name"].startswith("rest/") for item in branches["branches"])
    status, kept = api(metadata_url + "/conflicts")
    assert status == 200 and kept["conflicts"] == [], kept
    print("PASS: conflicting REST merges refuse with the property and succeed once resolved", flush=True)

    before = command(source, env, "ls-remote", "origin").decode()
    shutil.rmtree(Path(os.environ["ARUNA_GIT_ROOT"]) / f"{os.environ['ARUNA_DOCUMENT_ID']}.git")
    assert command(source, env, "ls-remote", "origin").decode() == before
    command(root, env, "clone", url, "rebuilt")
    rebuilt = root / "rebuilt"
    command(rebuilt, env, "fsck", "--strict")
    command(rebuilt, env, "fetch", "origin", "aruna")
    assert command(rebuilt, env, "log", "-1", "--format=%G?", "FETCH_HEAD").strip() == b"G"
    print("PASS: a deleted repository cache rebuilds with identical refs and signed commits", flush=True)

    if os.environ.get("ARUNA_ARCITECT"):
        subprocess.run(["node", str(Path(__file__).with_name("test_arcitect.mjs")), str(root)],
                       env=env, check=True, timeout=600)
    else:
        print("ARCitect application test not requested in this invocation", flush=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="aruna-native-git-") as directory:
        exercise(Path(directory))
