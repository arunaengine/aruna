"""Runs real Git/LFS clients against the bridge and the supplied temporary Aruna node."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import asyncio
import datetime
import hashlib
import os
import secrets
import subprocess
import tempfile
from pathlib import Path

from aiohttp import ClientSession, web

from arc import git, scaffold
from bridge import application
from store import Store


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


async def exercise(directory):
    root = Path(directory)
    token = secrets.token_urlsafe(32)
    store = Store(root / "server", "crate")
    runner = web.AppRunner(application(store, token), access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    askpass = root / "askpass"
    askpass.write_text('#!/bin/sh\ncase "$1" in *Username*) printf "operator\\n";; *) printf "%s\\n" "$ARC_BRIDGE_TOKEN";; esac\n')
    askpass.chmod(0o700)
    env = dict(os.environ, ARC_BRIDGE_TOKEN=token, GIT_ASKPASS=str(askpass), GIT_TERMINAL_PROMPT="0")
    source = root / "source"

    async def run(path, *args, **kwargs):
        return await asyncio.to_thread(command, path, env, *args, **kwargs)

    try:
        await asyncio.to_thread(scaffold, source)
        await run(source, "init", "--initial-branch=main")
        await run(source, "lfs", "install", "--local", "--skip-repo")
        await run(source, "remote", "add", "origin", base + "/crate.git")
        await run(source, "add", ".gitattributes", "ro-crate-metadata.json", "isa.investigation.xlsx",
                  "assays", "studies", "workflows", "runs")
        first = await asyncio.to_thread(commit, source, env, "test: create ARC fixture")
        payload_path = "assays/assay/dataset/measurements.bin"
        original = (source / payload_path).read_bytes()
        oid = hashlib.sha256(original).hexdigest()
        await run(source, "lfs", "push", "origin", "main")
        await run(source, "push", "origin", "main")
        assert store.find(oid)[0] == len(original)

        (source / payload_path).write_bytes(b"updated synthetic measurement\n" * 4096)
        await run(source, "add", payload_path)
        second = await asyncio.to_thread(commit, source, env, "test: revise ARC payload")
        await run(source, "lfs", "push", "origin", "main")
        await run(source, "push", "origin", "main")
        assert first != second

        await asyncio.to_thread(store.s3.put_object, Bucket=store.bucket, Key=store.key(oid), Body=b"new key head")
        clone_env = dict(env, GIT_LFS_SKIP_SMUDGE="1")
        await asyncio.to_thread(command, root, clone_env, "clone", base + "/crate.git", "clone")
        clone = root / "clone"
        await run(clone, "lfs", "install", "--local", "--skip-repo")
        await run(clone, "checkout", first)
        await run(clone, "lfs", "pull")
        assert (clone / payload_path).read_bytes() == original
        print("PASS: push, clone and historical LFS checkout use exact Aruna versions", flush=True)

        async with ClientSession() as client:
            async with client.get(base + "/status") as response:
                assert response.status == 401
            headers = {"Authorization": f"Bearer {token}"}
            async with client.put(base + "/lfs/" + "a" * 64, data=b"wrong", headers=headers) as response:
                assert response.status == 422
            assert store.find("a" * 64) is None
            async with client.post(base + "/crate.git/info/lfs/objects/batch", headers=headers,
                                   json={"operation": "download", "objects": [{"oid": "b" * 64, "size": 1}]}) as response:
                assert response.status == 200
                assert (await response.json())["objects"][0]["error"]["code"] == 404
            async with client.post(base + "/publish/" + first, headers=headers) as response:
                text = await response.text()
                assert response.status == 200, text
                publication = await response.json()
            async with client.post(base + "/publish/" + first, headers=headers) as response:
                assert response.status == 200
                assert (await response.json()) == publication
            assert publication["metadata"]["document_id"]
            assert publication["metadata"]["public"] is False
            print("PASS: authenticated private RO-Crate publication is idempotent", flush=True)

        await run(source, "checkout", "-b", "missing-lfs", "main")
        (source / payload_path).write_bytes(b"not uploaded to Aruna")
        await run(source, "add", payload_path)
        await asyncio.to_thread(commit, source, env, "test: require uploaded LFS data")
        await run(source, "push", "origin", "missing-lfs", success=False)
        assert not await run(source, "ls-remote", "origin", "refs/heads/missing-lfs")

        await run(source, "checkout", "-b", "invalid-arc", "main")
        await run(source, "rm", "isa.investigation.xlsx")
        await asyncio.to_thread(commit, source, env, "test: reject missing investigation")
        await run(source, "push", "origin", "invalid-arc", success=False)
        assert not await run(source, "ls-remote", "origin", "refs/heads/invalid-arc")
        assert git(store.state / "crate.git", "rev-parse", "main").decode().strip() == second
        print("PASS: invalid non-default branches and missing LFS objects are rejected", flush=True)
        restored = Store(store.state, store.name)
        content = await asyncio.to_thread(restored.get, oid)
        try:
            assert await asyncio.to_thread(content["Body"].read) == original
        finally:
            content["Body"].close()
        print("PASS: persistent LFS bindings survive a fresh bridge store instance", flush=True)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="aruna-arc-poc-") as directory:
        asyncio.run(exercise(directory))
