"""Checks every newly reachable commit before the PoC accepts branch updates."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import os
import sys
import tempfile

from arc import COMMIT, git, inspect_arc, materialize
from store import Store


def receive(updates, store):
    if len(updates) > 65536:
        raise ValueError("ref update list exceeds the PoC limit")
    repo = store.state / f"{store.name}.git"
    commits = set()
    for line in updates.splitlines():
        old, new, ref = line.split()
        if not COMMIT.fullmatch(old) or not COMMIT.fullmatch(new) or not ref.startswith("refs/heads/"):
            raise ValueError("PoC accepts branch updates only")
        if new == "0" * 40:
            raise ValueError("branch deletion is not supported by this PoC")
        if old != "0" * 40:
            git(repo, "merge-base", "--is-ancestor", old, new)
        commits.add(new)
        commits.update(git(repo, "rev-list", "--max-count=33", new, "--not", "--all").decode().splitlines())
        if len(commits) > 32:
            raise ValueError("PoC accepts at most 32 new commits per push")
    for commit in commits:
        with tempfile.TemporaryDirectory(prefix="arc-receive-", dir=store.state) as directory:
            materialize(repo, commit, directory, store)
            inspect_arc(directory)


if __name__ == "__main__":
    try:
        receive(sys.stdin.read(65537), Store(os.environ["ARC_STATE"], os.environ["ARC_REPOSITORY"]))
    except Exception as error:
        message = str(error) if isinstance(error, ValueError) else "ARC validation or Aruna storage unavailable"
        print(f"ARC PoC rejected push: {message}", file=sys.stderr)
        sys.exit(1)
