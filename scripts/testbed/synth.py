#!/usr/bin/env python3
"""Synthetic anonymous S3 source that stores nothing; keys come from a layout by arithmetic."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0

import argparse
import base64
import bisect
import hashlib
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlsplit
from xml.sax.saxutils import escape

SENTINEL = "\U0010ffff"
BLOCK = 65536
BASE_TIME = 1767225600
MANIFESTS = {"md5": ("MD5SUMS", "md5", 32), "sha256": ("SHA256SUMS", "sha256", 64)}


def parse_size(text):
    units = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
    last = text.lower()[-1]
    if last in units:
        return int(float(text[:-1]) * units[last])
    return int(text)


def make_sizer(spec):
    kind, *rest = spec.split(":")
    nums = [parse_size(item) for item in rest]
    if kind == "fixed" and len(nums) == 1:
        return lambda unit: nums[0]
    if kind == "uniform" and len(nums) == 2 and nums[0] <= nums[1]:
        return lambda unit: nums[0] + int(unit * (nums[1] - nums[0] + 1))
    if kind == "log" and len(nums) == 2 and 1 <= nums[0] <= nums[1]:
        return lambda unit: min(nums[1], int(nums[0] * (nums[1] / nums[0]) ** unit))
    raise ValueError(f"bad size spec {spec!r}; use fixed:N, uniform:A:B or log:A:B")


def fill_name(pattern, number, width):
    return pattern.replace("{}", str(number).zfill(width))


class Layout:
    """Buckets x folders x files, plus optional checksum manifests in every folder."""

    def __init__(self, shape, seed=0, sizes="fixed:4096", manifests=(), names=None, cache=100000):
        self.buckets, self.folders, self.files = shape
        patterns = names or {}
        self.bucket_pat = patterns.get("bucket", "bucket-{}")
        self.folder_pat = patterns.get("folder", "folder-{}")
        self.file_pat = patterns.get("file", "file-{}.dat")
        for pattern in (self.bucket_pat, self.folder_pat, self.file_pat):
            if pattern.count("{}") != 1 or "/" in pattern:
                raise ValueError(f"pattern {pattern!r} needs one {{}} and no slash")
        self.seed = seed
        self.sizer = make_sizer(sizes)
        self.slots = tuple(sorted(MANIFESTS[name] for name in manifests))
        self.per = len(self.slots) + self.files
        self.total = self.folders * self.per
        self.widths = [len(str(max(count - 1, 0))) for count in shape]
        if self.slots and self.files and self.file_name(0) <= self.slots[-1][0]:
            raise ValueError("file names must sort after the manifest names")
        self.cache = OrderedDict()
        self.cache_max = cache
        self.lock = threading.Lock()

    def bucket_name(self, index):
        return fill_name(self.bucket_pat, index, self.widths[0])

    def folder_name(self, index):
        return fill_name(self.folder_pat, index, self.widths[1])

    def file_name(self, index):
        return fill_name(self.file_pat, index, self.widths[2])

    def bucket_index(self, name):
        head, tail = self.bucket_pat.split("{}")
        middle = name[len(head) : len(name) - len(tail)]
        if not (name.startswith(head) and name.endswith(tail) and middle.isdigit()):
            return None
        index = int(middle)
        if index < self.buckets and self.bucket_name(index) == name:
            return index
        return None

    def key_at(self, position):
        folder, entry = divmod(position, self.per)
        if entry < len(self.slots):
            return f"{self.folder_name(folder)}/{self.slots[entry][0]}"
        return f"{self.folder_name(folder)}/{self.file_name(entry - len(self.slots))}"

    def find(self, key):
        """Return the position of an existing key, or None."""
        seq = Keys(self)
        position = bisect.bisect_left(seq, key)
        if position < self.total and seq[position] == key:
            return position
        return None

    def digest(self, obj, algo):
        """Hex digest of the object content, kept in a bounded least recently used cache."""
        ident = (obj.bucket, obj.position, algo)
        with self.lock:
            if ident in self.cache:
                self.cache.move_to_end(ident)
                return self.cache[ident]
        hasher = hashlib.new(algo)
        for part in obj.chunks(0, obj.size() - 1):
            hasher.update(part)
        value = hasher.hexdigest()
        with self.lock:
            self.cache[ident] = value
            while len(self.cache) > self.cache_max:
                self.cache.popitem(last=False)
        return value


class Keys:
    """Sorted keys of one bucket as a read-only sequence for bisect."""

    def __init__(self, layout):
        self.layout = layout

    def __len__(self):
        return self.layout.total

    def __getitem__(self, position):
        return self.layout.key_at(position)


class Obj:
    """One virtual object; size, content and modification time derive from the key."""

    def __init__(self, layout, bucket, position):
        self.layout = layout
        self.bucket = bucket
        self.position = position
        self.key = layout.key_at(position)
        folder, entry = divmod(position, layout.per)
        self.folder = folder
        self.slot = layout.slots[entry] if entry < len(layout.slots) else None
        ident = f"{layout.seed}\0{layout.bucket_name(bucket)}\0{self.key}"
        self.seed = hashlib.blake2b(ident.encode(), digest_size=16).digest()

    def size(self):
        if self.slot:
            width = self.slot[2] + 2 + len(self.layout.file_name(0)) + 1
            return self.layout.files * width
        return self.layout.sizer(int.from_bytes(self.seed[:8], "big") / 2**64)

    def etag(self):
        return self.layout.digest(self, "md5")

    def modified(self):
        return BASE_TIME + int.from_bytes(self.seed[8:12], "big") % 31536000

    def manifest(self):
        lay = self.layout
        base = self.folder * lay.per + len(lay.slots)
        lines = []
        for number in range(lay.files):
            peer = Obj(lay, self.bucket, base + number)
            lines.append(f"{lay.digest(peer, self.slot[1])}  {lay.file_name(number)}\n")
        return "".join(lines).encode()

    def block(self, index):
        length = min(BLOCK, self.size() - index * BLOCK)
        return hashlib.shake_128(self.seed + index.to_bytes(8, "big")).digest(length)

    def chunks(self, start, end):
        """Yield the bytes from start to end, both inclusive."""
        if end < start:
            return
        if self.slot:
            yield self.manifest()[start : end + 1]
            return
        for index in range(start // BLOCK, end // BLOCK + 1):
            low = max(start - index * BLOCK, 0)
            high = min(end - index * BLOCK + 1, BLOCK)
            yield self.block(index)[low:high]


def list_page(layout, prefix, delimiter, lower, limit):
    """Return (items, next key). Items are ("key", position) or ("prefix", text)."""
    seq = Keys(layout)
    pos = bisect.bisect_left(seq, max(prefix, lower))
    end = bisect.bisect_left(seq, prefix + SENTINEL)
    items = []
    while pos < end and len(items) < limit:
        key = seq[pos]
        cut = key.find(delimiter, len(prefix)) if delimiter else -1
        if cut < 0:
            items.append(("key", pos))
            pos += 1
            continue
        common = key[: cut + len(delimiter)]
        items.append(("prefix", common))
        pos = bisect.bisect_left(seq, common + SENTINEL, pos, end)
    return items, (seq[pos] if pos < end else None)


def parse_range(header, size):
    """Return (start, end) inclusive, None for the whole object, or raise ValueError."""
    if not header or not header.startswith("bytes=") or "," in header:
        return None
    first, _, last = header[6:].partition("-")
    try:
        if first == "":
            span = int(last)
            if span <= 0:
                raise ValueError("empty suffix")
            return max(size - span, 0), size - 1
        start = int(first)
        end = min(int(last), size - 1) if last else size - 1
    except ValueError:
        return None
    if start >= size or end < start:
        raise ValueError("unsatisfiable")
    return start, end


class Limiter:
    """Token bucket that refuses requests above the rate; zero turns it off."""

    def __init__(self, rate):
        self.rate = rate
        self.tokens = max(rate, 1)
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def take(self):
        if self.rate <= 0:
            return True
        with self.lock:
            now = time.monotonic()
            self.tokens = min(max(self.rate, 1), self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens < 1:
                return False
            self.tokens -= 1
            return True


def iso_time(stamp):
    return datetime.fromtimestamp(stamp, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        if self.server.verbose:
            super().log_message(*args)

    def do_GET(self):
        srv = self.server
        if not srv.limiter.take():
            return self.fail(503, "SlowDown", "Reduce your request rate", {"Retry-After": "1"})
        if srv.latency:
            time.sleep(srv.latency)
        url = urlsplit(self.path)
        self.query = parse_qs(url.query, keep_blank_values=True)
        name, _, key = unquote(url.path).lstrip("/").partition("/")
        bucket = srv.layout.bucket_index(name)
        if bucket is None:
            return self.fail(404, "NoSuchBucket", "The specified bucket does not exist")
        if key:
            return self.serve_object(bucket, key)
        if self.command == "HEAD":
            return self.reply(200)
        if self.arg("list-type") != "2":
            return self.fail(501, "NotImplemented", "Only ListObjectsV2 is served")
        self.serve_list(bucket)

    do_HEAD = do_GET

    def do_PUT(self):
        self.close_connection = True
        self.fail(403, "AccessDenied", "This source is read only")

    do_POST = do_DELETE = do_PUT

    def arg(self, name, default=""):
        return self.query.get(name, [default])[0]

    def reply(self, status, body=b"", headers=()):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def fail(self, status, code, message, extra=None):
        text = f"<Error><Code>{code}</Code><Message>{escape(message)}</Message></Error>"
        headers = [("Content-Type", "application/xml")] + list((extra or {}).items())
        self.reply(status, text.encode(), headers)

    def serve_list(self, bucket):
        srv, lay = self.server, self.server.layout
        try:
            limit = int(self.arg("max-keys", "1000"))
            token = self.arg("continuation-token")
            lower = base64.urlsafe_b64decode(token).decode() if token else ""
            if limit < 0:
                raise ValueError("negative max-keys")
        except ValueError:
            return self.fail(400, "InvalidArgument", "Bad max-keys or continuation-token")
        after = self.arg("start-after")
        if not token and after:
            lower = after + "\0"
        prefix, delimiter = self.arg("prefix"), self.arg("delimiter")
        limit = min(limit, srv.page_cap)
        items, more = list_page(lay, prefix, delimiter, lower, limit)
        url = self.arg("encoding-type") == "url"
        enc = (lambda text: quote(text, safe="/")) if url else (lambda text: text)
        out = ["<?xml version='1.0' encoding='UTF-8'?>"]
        out.append("<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>")
        out.append(f"<Name>{escape(lay.bucket_name(bucket))}</Name>")
        out.append(f"<Prefix>{escape(enc(prefix))}</Prefix>")
        if delimiter:
            out.append(f"<Delimiter>{escape(enc(delimiter))}</Delimiter>")
        out.append(f"<MaxKeys>{limit}</MaxKeys>")
        out.append("<EncodingType>url</EncodingType>" if url else "")
        out.append(f"<KeyCount>{len(items)}</KeyCount>")
        out.append(f"<IsTruncated>{'true' if more is not None and limit else 'false'}</IsTruncated>")
        if token:
            out.append(f"<ContinuationToken>{escape(token)}</ContinuationToken>")
        if more is not None and limit:
            out.append(f"<NextContinuationToken>{encode_token(more)}</NextContinuationToken>")
        if after:
            out.append(f"<StartAfter>{escape(enc(after))}</StartAfter>")
        for kind, value in items:
            if kind == "key":
                obj = Obj(lay, bucket, value)
                out.append(
                    f"<Contents><Key>{escape(enc(obj.key))}</Key>"
                    f"<LastModified>{iso_time(obj.modified())}</LastModified>"
                    f'<ETag>"{obj.etag()}"</ETag><Size>{obj.size()}</Size>'
                    "<StorageClass>STANDARD</StorageClass></Contents>"
                )
            else:
                out.append(f"<CommonPrefixes><Prefix>{escape(enc(value))}</Prefix></CommonPrefixes>")
        out.append("</ListBucketResult>")
        self.reply(200, "".join(out).encode(), [("Content-Type", "application/xml")])

    def serve_object(self, bucket, key):
        lay = self.server.layout
        position = lay.find(key)
        if position is None:
            return self.fail(404, "NoSuchKey", "The specified key does not exist")
        obj = Obj(lay, bucket, position)
        size, etag = obj.size(), obj.etag()
        stamp = formatdate(obj.modified(), usegmt=True)
        kind = "text/plain" if obj.slot else "application/octet-stream"
        base = [("ETag", f'"{etag}"'), ("Last-Modified", stamp), ("Accept-Ranges", "bytes")]
        match = self.headers.get("If-Match")
        if match and match.strip() != "*" and etag not in match:
            return self.fail(412, "PreconditionFailed", "At least one condition failed")
        none = self.headers.get("If-None-Match")
        if none and (none.strip() == "*" or etag in none):
            return self.reply(304, headers=base)
        try:
            span = parse_range(self.headers.get("Range"), size)
        except ValueError:
            return self.fail(416, "InvalidRange", "The requested range is not satisfiable")
        status, start, end = (200, 0, size - 1) if span is None else (206, *span)
        headers = [("Content-Type", kind)] + base
        if status == 206:
            headers.append(("Content-Range", f"bytes {start}-{end}/{size}"))
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(max(end - start + 1, 0)))
        self.end_headers()
        if self.command == "HEAD":
            return
        for part in obj.chunks(start, end):
            self.wfile.write(part)


def encode_token(key):
    return base64.urlsafe_b64encode(key.encode()).decode()


class Source(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256

    def __init__(self, address, layout, latency=0.0, page_cap=1000, rate=0.0, verbose=False):
        super().__init__(address, Handler)
        self.layout = layout
        self.latency = latency
        self.page_cap = page_cap
        self.limiter = Limiter(rate)
        self.verbose = verbose


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layout", default="2x1000x20", help="BUCKETSxFOLDERSxFILES")
    parser.add_argument("--bucket-pattern", default="bucket-{}")
    parser.add_argument("--folder-pattern", default="folder-{}")
    parser.add_argument("--file-pattern", default="file-{}.dat")
    parser.add_argument("--sizes", default="fixed:4096", help="fixed:N, uniform:A:B or log:A:B")
    parser.add_argument("--manifests", default="", help="comma list of md5 and sha256")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--latency-ms", type=float, default=0.0)
    parser.add_argument("--page-cap", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=0.0, help="requests per second, 0 is off")
    parser.add_argument("--cache-size", type=int, default=100000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    shape = tuple(int(float(part)) for part in args.layout.lower().split("x"))
    names = {"bucket": args.bucket_pattern, "folder": args.folder_pattern, "file": args.file_pattern}
    manifests = [name for name in args.manifests.split(",") if name]
    layout = Layout(shape, args.seed, args.sizes, manifests, names, args.cache_size)
    server = Source(
        (args.host, args.port), layout, args.latency_ms / 1000, args.page_cap, args.rate, args.verbose
    )
    keys = layout.buckets * layout.total
    print(f"serving {keys} virtual keys on http://{args.host}:{args.port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
