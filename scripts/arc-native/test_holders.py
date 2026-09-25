"""Checks that two holders serve one ARC from replicated Git records."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path

from arc import scaffold
from test_native import command, commit, http


def wait(description, check):
    deadline = time.monotonic() + 300
    while not (result := check()):
        assert time.monotonic() < deadline, (description, refs(os.environ["ARUNA_API_A"]),
                                              refs(os.environ["ARUNA_API_B"]))
        time.sleep(0.25)
    return result


def refs(api):
    status, body = http(f"{api}/api/v1/metadata/{os.environ['ARUNA_DOCUMENT_ID']}/git")
    return json.loads(body)["refs"] if status == 200 else None


def converged():
    first, second = refs(os.environ["ARUNA_API_A"]), refs(os.environ["ARUNA_API_B"])
    return first if first is not None and first == second else None


def exercise(root):
    url_a, url_b = os.environ["ARUNA_GIT_URL_A"], os.environ["ARUNA_GIT_URL_B"]
    askpass = root / "askpass"
    askpass.write_text('#!/bin/sh\ncase "$1" in *Username*) printf "aruna\\n";; *) printf "%s\\n" "$ARUNA_TOKEN";; esac\n')
    askpass.chmod(0o700)
    env = dict(os.environ, GIT_ASKPASS=str(askpass), GIT_TERMINAL_PROMPT="0")
    first = wait("holders did not agree on the initial snapshot", converged)
    assert "refs/heads/main" in first and first["refs/heads/main"] == first["refs/heads/aruna"]

    command(root, env, "clone", url_a, "a")
    a = root / "a"
    command(a, env, "lfs", "install", "--local", "--skip-repo")
    fixture = root / "fixture"
    scaffold(fixture)
    shutil.copytree(fixture, a, dirs_exist_ok=True)
    command(a, env, "add", ".gitattributes", "isa.investigation.xlsx", "ro-crate-metadata.json",
            "studies", "assays", "runs", "workflows")
    pushed = commit(a, env, "test: push to the first holder")
    payload = "assays/assay/dataset/measurements.bin"
    content = (a / payload).read_bytes()
    command(a, env, "lfs", "push", "origin", "main")
    command(a, env, "push", "origin", "main")
    state = wait("the second holder did not receive the push",
                 lambda: (s := converged()) and s["refs/heads/main"] != first["refs/heads/main"] and s)
    main = state["refs/heads/main"]
    command(a, env, "fetch", "origin")
    command(a, env, "merge-base", "--is-ancestor", pushed, main)
    print("PASS: a push to one holder replicates its commits and refs to the other", flush=True)

    command(root, dict(env, GIT_LFS_SKIP_SMUDGE="1"), "clone", url_b, "b")
    b = root / "b"
    command(b, env, "lfs", "install", "--local", "--skip-repo")
    assert command(b, env, "rev-parse", "HEAD").decode().strip() == main
    command(b, env, "lfs", "pull")
    assert hashlib.sha256((b / payload).read_bytes()).digest() == hashlib.sha256(content).digest()
    print("PASS: the second holder serves the same commits and the first holder's LFS content", flush=True)

    (b / "notes.txt").write_text("written through the second holder\n")
    command(b, env, "add", "notes.txt")
    second = commit(b, env, "test: push to the second holder")
    command(b, env, "push", "origin", "main")
    wait("the first holder did not receive the second push",
         lambda: (s := converged()) and s["refs/heads/main"] != main and s)
    command(a, env, "pull", "--ff-only", "origin", "main")
    command(a, env, "merge-base", "--is-ancestor", second, "HEAD")
    assert (a / "notes.txt").read_text() == "written through the second holder\n"
    print("PASS: pushes through either holder reach both, and both holders converge", flush=True)

    command(a, env, "checkout", "-b", "race", pushed)
    (a / "race.txt").write_text("first holder\n")
    command(a, env, "add", "race.txt")
    commit(a, env, "test: race through the first holder")
    command(b, env, "checkout", "-b", "race", pushed)
    (b / "race.txt").write_text("second holder\n")
    command(b, env, "add", "race.txt")
    commit(b, env, "test: race through the second holder")
    accepted = [command(clone, env, "push", "origin", "race", success=None) is not None for clone in (a, b)]
    assert any(accepted)
    final = wait("holders did not converge after competing pushes",
                 lambda: (s := converged()) and "refs/heads/race" in s and s)
    kept = [name for name in final if name.startswith("refs/conflicts/heads/race/")]
    assert len(kept) == accepted.count(True) - 1
    print("PASS: competing branch pushes converge to one branch and keep the other as a conflict ref", flush=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="aruna-git-holders-") as directory:
        exercise(Path(directory))
