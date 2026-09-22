"""Runs a loopback, single-operator Git/LFS bridge to an existing Aruna group."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import argparse
import asyncio
import base64
import hmac
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

from aiohttp import web
from botocore.exceptions import BotoCoreError, ClientError

from arc import COMMIT, git, inspect_arc, materialize, scaffold
from store import MAX_OBJECT, Store, object_id


LFS_TYPE = "application/vnd.git-lfs+json"


def initialize(store):
    repo = store.state / f"{store.name}.git"
    if not repo.exists():
        subprocess.run(["git", "init", "--bare", "--object-format=sha1", "--initial-branch=main", str(repo)],
                       check=True, capture_output=True, timeout=30)
    hooks = repo / "hooks"
    hooks.mkdir(exist_ok=True)
    command = shlex.join([sys.executable, str(Path(__file__).with_name("receive.py").resolve())])
    hook = hooks / "pre-receive"
    expected = f"#!/bin/sh\nexec {command}\n"
    if hook.exists() and hook.read_text() != expected:
        raise ValueError("refusing to replace an existing receive hook")
    hook.write_text(expected)
    hook.chmod(0o700)
    git(repo, "config", "core.hooksPath", str(hooks))
    git(repo, "config", "http.receivepack", "true")
    git(repo, "config", "http.getanyfile", "false")
    git(repo, "config", "receive.fsckObjects", "true")
    git(repo, "config", "receive.denyNonFastForwards", "true")
    (repo / "git-daemon-export-ok").touch()
    return repo


def git_http(store, request, body):
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_PROJECT_ROOT=str(store.state), PATH_INFO=request["path"],
               REQUEST_METHOD=request["method"], QUERY_STRING=request["query"],
               CONTENT_TYPE=request["type"], CONTENT_LENGTH=str(len(body)),
               REMOTE_USER="arc-operator", REMOTE_ADDR="127.0.0.1",
               GIT_PROTOCOL=request["protocol"], ARC_STATE=str(store.state),
               ARC_REPOSITORY=store.name)
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(["git", "http-backend"], input=body, stdout=output,
                                stderr=subprocess.PIPE, env=env, timeout=180)
        if result.returncode or output.tell() > MAX_OBJECT:
            raise ValueError("Git backend failed or response exceeds the PoC limit")
        output.seek(0)
        headers = {}
        status = 200
        for _ in range(100):
            line = output.readline(8192)
            if line in (b"\r\n", b"\n"):
                break
            key, value = line.decode().strip().split(":", 1)
            if key.lower() == "status":
                status = int(value.strip().split()[0])
            else:
                headers[key] = value.strip()
        else:
            raise ValueError("invalid Git backend headers")
        return status, headers, output.read()


def publish(store, commit):
    repo = store.state / f"{store.name}.git"
    if not COMMIT.fullmatch(commit):
        raise ValueError("an exact commit ID is required")
    if not git(repo, "for-each-ref", f"--contains={commit}", "--format=%(refname)", "refs/heads/").strip():
        raise ValueError("commit is not reachable from an accepted branch")
    with tempfile.TemporaryDirectory(prefix="arc-publish-", dir=store.state) as directory:
        files = materialize(repo, commit, directory, store)
        document = inspect_arc(directory)
        return store.publish(commit, document, files)


def application(store, token):
    if len(token) < 24:
        raise ValueError("ARC_BRIDGE_TOKEN must contain at least 24 characters")
    initialize(store)
    gate = asyncio.Lock()
    expected = f"Bearer {token}"

    @web.middleware
    async def authorize(request, handler):
        supplied = request.headers.get("Authorization", "")
        if supplied.startswith("Basic "):
            try:
                supplied = "Bearer " + base64.b64decode(supplied[6:], validate=True).decode().split(":", 1)[1]
            except (ValueError, IndexError, UnicodeError):
                supplied = ""
        if not hmac.compare_digest(supplied.encode(), expected.encode()):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="ARC PoC"'})
        try:
            return await handler(request)
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            return web.json_response({"message": "invalid ARC bridge request or content"}, status=422)
        except (BotoCoreError, ClientError, OSError, subprocess.SubprocessError):
            return web.json_response({"message": "Aruna or Git service unavailable"}, status=503)

    async def status(request):
        return web.json_response({"repository": store.name, "validation": "poc-arc-subset",
                                  "git": f"/{store.name}.git", "max_lfs_bytes": MAX_OBJECT})

    async def batch(request):
        body = await request.json()
        operation = body["operation"]
        if operation not in ("upload", "download") or body.get("hash_algo", "sha256") != "sha256":
            raise ValueError("unsupported LFS operation")
        if "basic" not in body.get("transfers", ["basic"]):
            raise ValueError("only the basic LFS transfer is supported")
        objects = body["objects"]
        if not isinstance(objects, list) or len(objects) > 100:
            raise ValueError("invalid LFS batch size")
        results = []
        for item in objects:
            oid, size = object_id(item["oid"]), item["size"]
            if type(size) is not int or not 0 <= size <= MAX_OBJECT:
                raise ValueError("invalid LFS object size")
            result = {"oid": oid, "size": size, "authenticated": True}
            record = await asyncio.to_thread(store.find, oid)
            if record and record[0] != size:
                result["error"] = {"code": 422, "message": "size mismatch"}
            elif operation == "download" and record is None:
                result["error"] = {"code": 404, "message": "object not found"}
            elif record is None or operation == "download":
                port = request.transport.get_extra_info("sockname")[1]
                result["actions"] = {operation: {
                    "href": f"http://127.0.0.1:{port}/lfs/{oid}",
                    "header": {"Authorization": expected},
                }}
            else:
                await asyncio.to_thread(store.check, oid, size)
            results.append(result)
        return web.json_response({"transfer": "basic", "objects": results}, content_type=LFS_TYPE)

    async def upload(request):
        oid = object_id(request.match_info["oid"])
        size = request.content_length
        if size is None or not 0 <= size <= MAX_OBJECT:
            raise ValueError("a bounded Content-Length is required")
        async with gate:
            with tempfile.TemporaryFile() as output:
                received = 0
                async for chunk in request.content.iter_chunked(65536):
                    received += len(chunk)
                    if received > size:
                        raise ValueError("oversized upload")
                    output.write(chunk)
                if received != size:
                    raise ValueError("incomplete upload")
                await asyncio.to_thread(store.put, oid, output, size)
        return web.Response(status=200)

    async def download(request):
        oid = object_id(request.match_info["oid"])
        result = await asyncio.to_thread(store.get, oid)
        response = web.StreamResponse(headers={"Content-Type": "application/octet-stream",
                                               "Content-Length": str(result["ContentLength"])})
        await response.prepare(request)
        try:
            while chunk := await asyncio.to_thread(result["Body"].read, 65536):
                await response.write(chunk)
        finally:
            result["Body"].close()
        await response.write_eof()
        return response

    async def transport(request):
        if request.match_info["action"] not in ("info/refs", "git-upload-pack", "git-receive-pack"):
            raise web.HTTPNotFound()
        body = await request.read()
        request_data = {"path": request.path, "method": request.method, "query": request.query_string,
                        "type": request.content_type, "protocol": request.headers.get("Git-Protocol", "")}
        async with gate:
            code, headers, payload = await asyncio.to_thread(git_http, store, request_data, body)
        return web.Response(status=code, headers=headers, body=payload)

    async def publish_revision(request):
        async with gate:
            document = await asyncio.to_thread(publish, store, request.match_info["commit"])
        return web.json_response({"metadata": document, "validation": "poc-arc-subset"})

    app = web.Application(middlewares=[authorize], client_max_size=16 * 1024 * 1024)
    app.router.add_get("/status", status)
    app.router.add_post(f"/{store.name}.git/info/lfs/objects/batch", batch)
    app.router.add_put("/lfs/{oid}", upload)
    app.router.add_get("/lfs/{oid}", download)
    app.router.add_post("/publish/{commit}", publish_revision)
    app.router.add_route("*", f"/{store.name}.git/{{action:.*}}", transport)
    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("scaffold", help="create a synthetic study/assay ARC")
    create.add_argument("directory")
    serve = commands.add_parser("serve", help="serve one private operator repository on loopback")
    serve.add_argument("--state", required=True)
    serve.add_argument("--name", default="crate")
    serve.add_argument("--port", type=int, default=8097)
    args = parser.parse_args()
    if args.command == "scaffold":
        scaffold(args.directory)
    else:
        os.umask(0o077)
        store = Store(args.state, args.name)
        web.run_app(application(store, os.environ["ARC_BRIDGE_TOKEN"]), host="127.0.0.1", port=args.port,
                    access_log=None, handler_args={"handler_cancellation": False})


if __name__ == "__main__":
    main()
