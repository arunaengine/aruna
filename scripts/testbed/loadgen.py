#!/usr/bin/env python3
"""Load generator for S3 PUT, multipart, GET, ranged GET and HTTP downloads with latency stats."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import argparse
import json
import math
import random
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

MIB = 1 << 20
CHUNK = 1 << 20


def parse_size(text):
    units = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
    if text.lower()[-1] in units:
        return int(float(text[:-1]) * units[text.lower()[-1]])
    return int(text)


def percentile(values, pct):
    """Nearest rank percentile of a list; zero for an empty list."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(math.ceil(pct / 100 * len(ordered)) - 1, 0)]


def run_ops(count, workers, task):
    """Run task(i) for every i below count; return (samples, errors, seconds)."""
    samples, errors = [], []
    lock = threading.Lock()

    def one(index):
        started = time.perf_counter()
        try:
            moved = task(index)
        except Exception as exc:
            with lock:
                errors.append(f"{type(exc).__name__}: {exc}")
            return
        elapsed = time.perf_counter() - started
        with lock:
            samples.append((elapsed, moved))

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, range(count)))
    return samples, errors, time.perf_counter() - started


def summarize(mode, samples, errors, seconds):
    times = [item[0] * 1000 for item in samples]
    moved = sum(item[1] for item in samples)
    return {
        "mode": mode,
        "ops": len(samples),
        "errors": len(errors),
        "bytes": moved,
        "seconds": round(seconds, 3),
        "ops_per_sec": round(len(samples) / seconds, 2) if seconds else 0.0,
        "mib_per_sec": round(moved / MIB / seconds, 2) if seconds else 0.0,
        "p50_ms": round(percentile(times, 50), 2),
        "p99_ms": round(percentile(times, 99), 2),
    }


def make_client(args):
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    config = Config(
        signature_version=UNSIGNED if args.anonymous else "s3v4",
        s3={"addressing_style": "path"},
        max_pool_connections=max(args.concurrency, 10),
        retries={"max_attempts": 1},
    )
    return boto3.client(
        "s3",
        endpoint_url=args.s3_url,
        region_name=args.region,
        aws_access_key_id=None if args.anonymous else args.access_key,
        aws_secret_access_key=None if args.anonymous else args.secret_key,
        config=config,
        verify=not args.insecure,
    )


def list_keys(client, args):
    """Return (key, size) pairs under the prefix, at most count of them."""
    found, token = [], None
    while len(found) < args.count:
        extra = {"ContinuationToken": token} if token else {}
        got = client.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix, **extra)
        found += [(item["Key"], item["Size"]) for item in got.get("Contents", [])]
        token = got.get("NextContinuationToken")
        if not token:
            break
    if not found:
        raise SystemExit(f"no keys under prefix {args.prefix!r} in bucket {args.bucket!r}")
    return found[: args.count]


def drain(body):
    moved = 0
    while chunk := body.read(CHUNK):
        moved += len(chunk)
    return moved


def put_task(client, args):
    data = random.Random(args.seed).randbytes(args.size)

    def task(index):
        client.put_object(Bucket=args.bucket, Key=f"{args.prefix}{index:08d}", Body=data)
        return len(data)

    return task


def multipart_task(client, args):
    part = random.Random(args.seed).randbytes(min(args.part_size, args.size))

    def task(index):
        key = f"{args.prefix}{index:08d}"
        upload = client.create_multipart_upload(Bucket=args.bucket, Key=key)["UploadId"]
        done, left, number = [], args.size, 1
        try:
            while left > 0:
                body = part[: min(left, len(part))]
                sent = client.upload_part(
                    Bucket=args.bucket, Key=key, UploadId=upload, PartNumber=number, Body=body
                )
                done.append({"ETag": sent["ETag"], "PartNumber": number})
                left -= len(body)
                number += 1
            client.complete_multipart_upload(
                Bucket=args.bucket, Key=key, UploadId=upload, MultipartUpload={"Parts": done}
            )
        except Exception:
            client.abort_multipart_upload(Bucket=args.bucket, Key=key, UploadId=upload)
            raise
        return args.size

    return task


def get_task(client, args):
    keys = list_keys(client, args)

    def task(index):
        key = keys[index % len(keys)][0]
        return drain(client.get_object(Bucket=args.bucket, Key=key)["Body"])

    return task


def range_task(client, args):
    keys = [item for item in list_keys(client, args) if item[1] > 0]
    if not keys:
        raise SystemExit("no non-empty keys to read ranges from")

    def task(index):
        key, size = keys[index % len(keys)]
        span = min(args.range_size, size)
        start = random.Random(args.seed * 1000003 + index).randint(0, size - span)
        got = client.get_object(
            Bucket=args.bucket, Key=key, Range=f"bytes={start}-{start + span - 1}"
        )
        return drain(got["Body"])

    return task


def http_task(args):
    urls = [urljoin(args.rest_url, path) for path in args.path]
    if not urls:
        raise SystemExit("http mode needs at least one --path")
    headers = {"Authorization": f"Bearer {args.token}"} if args.token else {}

    def task(index):
        request = urllib.request.Request(urls[index % len(urls)], headers=headers)
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            return drain(response)

    return task


def build_task(args):
    if args.mode == "http":
        return http_task(args)
    client = make_client(args)
    builders = {"put": put_task, "multipart": multipart_task, "get": get_task, "range": range_task}
    return builders[args.mode](client, args)


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["put", "multipart", "get", "range", "http"])
    parser.add_argument("--count", type=int, default=100, help="number of operations")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--json", action="store_true", help="print the result as JSON")
    parser.add_argument("--s3-url", default="http://127.0.0.1:9000")
    parser.add_argument("--bucket", default="loadgen")
    parser.add_argument("--prefix", default="loadgen/")
    parser.add_argument("--access-key", default=None)
    parser.add_argument("--secret-key", default=None)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--anonymous", action="store_true", help="send unsigned requests")
    parser.add_argument("--insecure", action="store_true", help="skip TLS certificate checks")
    parser.add_argument("--size", type=parse_size, default=MIB, help="object size, e.g. 4k, 8m")
    parser.add_argument("--part-size", type=parse_size, default=8 * MIB)
    parser.add_argument("--range-size", type=parse_size, default=64 * 1024)
    parser.add_argument("--rest-url", default="http://127.0.0.1:8080")
    parser.add_argument("--path", action="append", default=[], help="URL or path, repeatable")
    parser.add_argument("--token", default=None, help="bearer token for http mode")
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args(argv)
    if args.mode == "multipart" and args.part_size < 5 * MIB:
        parser.error("--part-size must be at least 5m for S3 multipart uploads")
    return args


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    task = build_task(args)
    samples, errors, seconds = run_ops(args.count, args.concurrency, task)
    result = summarize(args.mode, samples, errors, seconds)
    if args.json:
        print(json.dumps(result))
    else:
        print(" ".join(f"{name}={value}" for name, value in result.items()))
    for line in sorted(set(errors))[:5]:
        print(f"error: {line}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
