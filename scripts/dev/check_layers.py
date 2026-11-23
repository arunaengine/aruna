#!/usr/bin/env python3
"""Layer guard: counts async code, references, test habits and duplicates against layers_baseline.json.
Fails on a count above its baseline, async code in operations production or a new crate dependency."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0
import argparse
import collections
import fnmatch
import hashlib
import json
import os
import re
import sys
import tomllib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from check_style import CHAR_RE, iter_rust_files, read_source, scan_block

OPERATIONS_SRCS = ("domain/ops-identity/src", "domain/ops-data/src", "domain/ops-work/src")
OPERATIONS_CRATES = ("aruna_ops_identity", "aruna_ops_data")
BASELINE_PATH = "scripts/dev/layers_baseline.json"
# Domain groups of target-architecture.md section 5, lowest first. A module may
# use its own group and lower groups only.
GROUPS = (
    (
        "identity and realm",
        frozenset(
            "abe auth realm realm_vault node_vault secrets session users groups device_node onboarding "
            "placement shard sync owner_index document_repository endpoint_screening forward tasks".split()
        ),
    ),
    (
        "data",
        frozenset(
            "s3 blob staging replication distribution transfer metadata git shares connectors device "
            "holder_choice usage_publish".split()
        ),
    ),
    ("work and integrations", frozenset("jobs plugins assistant notifications federation harvest node".split())),
)
ANY_CRATE = "*"
# Internal normal dependencies the target table (target-architecture.md section 2) allows.
CRATE_TARGETS = {
    "aruna": ANY_CRATE,
    "aruna-api": (
        "aruna-core",
        "aruna-clock",
        "aruna-ops-identity",
        "aruna-ops-data",
        "aruna-ops-work",
        "aruna-runner",
        "aruna-wire",
        "aruna-test-harness",
    ),
    "aruna-doctor": (
        "aruna-core",
        "aruna-clock",
        "aruna-config",
        "aruna-storage",
        "aruna-blob",
        "aruna-ops-identity",
        "aruna-ops-data",
        "aruna-ops-work",
        "aruna-runner",
        "aruna-wire",
    ),
    # The node configuration, settings and identity that the node and the doctor share.
    "aruna-config": (
        "aruna-core",
        "aruna-clock",
        "aruna-wire",
    ),
    "aruna-cli": ("aruna-wire", "aruna-wasm-host", "aruna-plugin-contract"),
    "aruna-ops-identity": ("aruna-core", "aruna-plugin-contract"),
    "aruna-ops-data": ("aruna-core", "aruna-ops-identity"),
    "aruna-ops-work": (
        "aruna-core",
        "aruna-plugin-contract",
        "aruna-ops-identity",
        "aruna-ops-data",
    ),
    "aruna-runner": (
        "aruna-core",
        "aruna-clock",
        "aruna-storage",
        "aruna-p2p",
        "aruna-blob",
        "aruna-scheduler",
        "aruna-compute",
        "aruna-wasm-host",
        "aruna-rdf-graph",
        "aruna-egress",
    ),
    "aruna-storage": ("aruna-core", "aruna-clock"),
    "aruna-p2p": ("aruna-core", "aruna-clock"),
    "aruna-blob": ("aruna-core", "aruna-clock", "aruna-kpabe", "aruna-p2p"),
    "aruna-scheduler": ("aruna-core",),
    "aruna-compute": ("aruna-core", "aruna-clock"),
    "aruna-wasm-host": ("aruna-plugin-contract",),
    "aruna-plugin-contract": (),
    "aruna-rdf-graph": ("aruna-core",),
    "aruna-egress": ("aruna-core",),
    # Test helpers that drive operations; only dev-dependencies name it.
    "aruna-test-harness": (
        "aruna-core",
        "aruna-blob",
        "aruna-rdf-graph",
        "aruna-p2p",
        "aruna-ops-identity",
        "aruna-ops-data",
        "aruna-ops-work",
        "aruna-runner",
        "aruna-egress",
        "aruna-plugin-contract",
        "aruna-storage",
        "aruna-scheduler",
    ),
    "aruna-core": ("aruna-kpabe",),
    # Host clocks and the OS random source; core and operations never reach them.
    "aruna-clock": ("aruna-core",),
    "aruna-kpabe": (),
    "aruna-kpabe-wasm": ("aruna-kpabe",),
    "aruna-wire": ("aruna-core",),
    "aruna-plugin": (),
    "aruna-plugin-fixture": ("aruna-plugin",),
    "elabftw-sync": ("aruna-plugin",),
    "dataset-announcer": ("aruna-plugin",),
    "aruna-registry": ("aruna-core", "aruna-clock", "aruna-blob"),
    "aruna-compute-helper": (),
}
# Every crate keeps its table for build dependencies; backends also for test dependencies, except
# for the explicit dev-only storage test fixture allowance below (production edges stay forbidden).
BACKENDS = frozenset(
    "aruna-storage aruna-p2p aruna-blob aruna-scheduler aruna-compute aruna-wasm-host aruna-rdf-graph aruna-egress".split()
)
BACKEND_DEV_TARGETS = {
    "aruna-p2p": ("aruna-storage",),
    "aruna-blob": ("aruna-storage",),
}
# Layers are the first directory of a workspace member. Domain crates reach only domain
# crates; the operations crates reach only lower ones, in tests as well.
LAYERS = ("domain", "runtime", "frontends", "apps", "plugins", "testing")
OPS_ORDER = ("aruna-ops-identity", "aruna-ops-data", "aruna-ops-work")
RUNTIME_FOUNDATIONS = frozenset({"aruna-core", "aruna-kpabe", "aruna-plugin-contract"})
RUNTIME_EXTRAS = {"aruna-config": ("aruna-wire",)}
FRONTEND_RUNTIME = frozenset({"aruna-clock", "aruna-runner"})
API_SRC = "frontends/api/src/"
RAW_EFFECT = re.compile(r"(?<![\w#])(?:r#)?(?:Storage|Net|Blob)Effect::")
EFFECT_NAMES = frozenset({"StorageEffect", "NetEffect", "BlobEffect"})
EFFECT_USE = re.compile(r"(?<![\w#.:])\buse\s+([\w:#*{},\s]+?);|[{}]")
TRIGGER_MATCHES = (
    ("apps/node/src/tasks/incoming/mod.rs", "TaskKey::"),
    ("apps/node/src/inbound.rs", "Alpn::"),
)
TRIGGER_IMPLS = (("apps/node/src/plugin_host/broker.rs", "Broker"),)
# The one helper an arm without an inline async block may hand its work to, and the table that
# helper must look the operation up in before it drives it.
TRIGGER_HELPERS = {
    "apps/node/src/tasks/incoming/mod.rs": ("run_trigger", "timer_operation"),
    "apps/node/src/inbound.rs": ("serve_peer", "peer_operation"),
}
CALL_RE = re.compile(r"(?<![\w!])([a-z_]\w*)\s*\(")
DRIVE_ANY = re.compile(r"\bdrive(?:_retry(?:_until)?)?\s*\(")
SIMILAR_NAMES = (
    "*backoff*",
    "now_ms",
    "read_value",
    "read_row",
    "read_record",
    "hex_encode",
    "escape_xml",
    "actor",
    "map_*_error",
)
COUNTS = (
    ("mixed_files", "mixed files (an operation plus async code)"),
    ("pure_async_refs", "references from pure files to items defined only in async files"),
    ("upward_refs", "upward references between domain groups"),
    ("group_mutual_pairs", "mutual module pairs inside a domain group"),
    ("group_cycle_modules", "modules in cycle groups inside a domain group"),
    ("core_clocks", "clock and random calls in core production code"),
    ("operation_clocks", "clock, random and id-mint calls in operations production code"),
    ("trigger_arms", "trigger shell arms that do more than drive one operation"),
    ("state_clones", "match self.state.clone() in operations"),
    ("async_parallel", "files with parallel work only through async code"),
    ("test_sleeps", "real sleeps in tests without paused time"),
    ("silent_skips", "tests that print and return early"),
    ("duplicate_groups", "duplicate function bodies of six or more lines"),
    ("api_raw_effects", "raw storage, net and blob effects in API production code"),
)
DUPLICATE_LINES = 6

TOKEN_RE = re.compile(r"//|/\*|(?<!\w)(?:[bc]?r#*|[bc])?\"|'")
STRING_RE = re.compile(r'(?:[^"\\]|\\.)*"', re.S)
TEST_ATTR = re.compile(r"#\[\s*(?:cfg\s*\(\s*test\s*\)|test|rstest|tokio::test\b[^\]]*)\s*\]")
FILE_TEST = re.compile(r"#!\[\s*cfg\s*\(\s*test\s*\)\s*\]")
HOOK_ATTR = re.compile(
    r'#\[\s*cfg\s*\(\s*(?:any\s*\(\s*test\s*,\s*)?feature\s*=\s*"test-hooks"\s*\)?\s*\)\s*\]'
)
HOOK_MOD = re.compile(r"\bmod\s+(\w+)\s*;")
ATTR_RE = re.compile(r"\s*#\[")
BRACE_RE = re.compile(r"[{}]")
BRACKET_RE = re.compile(r"[\[\]]")
ITEM_RE = re.compile(r"[()\[\]{};]")
ARM_RE = re.compile(r"=>|[()\[\]{},]")
ASYNC_RE = re.compile(r"\basync\b|\.await\b")
OPERATION_RE = re.compile(r"\bimpl(?:\s*<[^{;]*?>)?\s+(?:[\w:]+::)?(?:Sub|Effect)?Operation\s+for\b")
DEF_RE = re.compile(r"\b(?:struct|enum|trait|type|fn|const|static|mod|union)\s+([A-Za-z_]\w*)")
USE_RE = re.compile(r"\buse\s+((?:crate|super|self|aruna_ops_identity|aruna_ops_data)\b[^;]*);")
INLINE_RE = re.compile(r"\b(?:crate|aruna_ops_identity|aruna_ops_data)(?:::\w+)+")
FN_RE = re.compile(r"\bfn\s+([A-Za-z_]\w*)")
DRIVE_RE = re.compile(r"\bdrive\s*\(")
MATCH_RE = re.compile(r"\bmatch\b")
STATE_CLONE = re.compile(r"\bmatch\s+self\s*\.\s*state\s*\.\s*clone\s*\(\s*\)")
PARALLEL_RE = re.compile(r"\b(?:join_all|FuturesUnordered|buffer_unordered|JoinSet|try_join\w*)\b")
SLEEP_RE = re.compile(r"(?<![\w.:])(?:(?:std::)?thread::|(?:tokio::)?time::)?sleep\s*\(")
CLOCK_RE = re.compile(
    r"\b(?:(?:SystemTime|Instant|Utc)::now|unix_timestamp_(?:secs|millis)|(?<!\.)(?<!fn )now_ms(?=\s*\()"
    r"|Ulid::(?:new|generate)|thread_rng|OsRng|rand::(?:random|rng)|Uuid::new_v4|getrandom)\b"
)
PAUSE_RE = re.compile(r"\bstart_paused\b|\bpause\s*\(\s*\)")
# The shared bounded poll helper; the one real sleep in tests the guard does not count.
POLL_HELPER = ("runtime/clock/src/testing.rs", "poll_until")
SKIP_RE = re.compile(r"\be?println!\s*\([^;]*\)\s*;\s*(?:return\b|None\s*[},])")
SPACE_RE = re.compile(r"\s+")
COMMA_RE = re.compile(r"\s*,")


def blank(text, spans):
    """Replace each span with spaces, keeping line breaks so offsets and line numbers hold."""
    pieces, last = [], 0
    for start, end in spans:
        pieces.append(text[last:start])
        pieces.append("\n".join(" " * len(part) for part in text[start:end].split("\n")))
        last = end
    pieces.append(text[last:])
    return "".join(pieces)


def string_close(text, found):
    mark = found.group()
    if "r" in mark:
        close = '"' + "#" * mark.count("#")
        end = text.find(close, found.end())
        return len(text) if end < 0 else end + len(close)
    tail = STRING_RE.match(text, found.end())
    return tail.end() if tail else len(text)


def mask_rust(text):
    """Return (code, masked): code has comments blanked; masked also blanks strings and chars."""
    comments, literals, pos = [], [], 0
    while True:
        found = TOKEN_RE.search(text, pos)
        if not found:
            break
        start, mark = found.start(), found.group()
        if mark == "//":
            end = text.find("\n", start)
            end = len(text) if end < 0 else end
            comments.append((start, end))
        elif mark == "/*":
            end = scan_block(text, start)
            comments.append((start, end))
        elif mark == "'":
            char = CHAR_RE.match(text, start)
            end = char.end() if char else start + 1
            if char:
                literals.append((start, end))
        else:
            end = string_close(text, found)
            literals.append((start, end))
        pos = end
    code = blank(text, comments)
    return code, blank(code, literals)


def pair_end(masked, start, pattern):
    depth = 0
    for found in pattern.finditer(masked, start):
        depth += 1 if found.group() in "{[" else -1
        if depth == 0:
            return found.end()
    return len(masked)


def item_end(masked, pos):
    """End of the item that follows pos, after any further attributes."""
    while True:
        attr = ATTR_RE.match(masked, pos)
        if not attr:
            break
        pos = pair_end(masked, attr.end() - 1, BRACKET_RE)
    depth = 0
    for found in ITEM_RE.finditer(masked, pos):
        mark = found.group()
        if mark in "([":
            depth += 1
        elif mark in ")]":
            depth -= 1
        elif depth == 0 and mark == ";":
            return found.end()
        elif depth == 0 and mark == "{":
            return pair_end(masked, found.start(), BRACE_RE)
    return len(masked)


def test_spans(masked):
    """Spans of items under #[cfg(test)], #[test], #[rstest] or #[tokio::test]."""
    if FILE_TEST.search(masked):
        return [(0, len(masked))]
    spans, pos = [], 0
    for found in TEST_ATTR.finditer(masked):
        if found.start() < pos:
            continue
        pos = item_end(masked, found.end())
        spans.append((found.start(), pos))
    return spans


def outside(spans, size):
    result, last = [], 0
    for start, end in spans:
        result.append((last, start))
        last = end
    result.append((last, size))
    return result


def is_test(rel):
    name = os.path.basename(rel)
    return (
        "/tests/" in "/" + rel
        or "_tests/" in rel
        or name == "tests.rs"
        or name.endswith("_tests.rs")
        or name.startswith(("test_", "tests_"))
    )


SourceFile = collections.namedtuple("SourceFile", "rel code masked test_masked test_code")


def load_source(root, rel):
    """Split one Rust file into production text and test text, both with comments blanked."""
    text = read_source(os.path.join(root, rel))
    code, masked = mask_rust(text)
    spans = [(0, len(text))] if is_test(rel) else test_spans(masked)
    production = outside(spans, len(text))
    return SourceFile(
        rel,
        blank(code, spans),
        blank(masked, spans),
        blank(masked, production),
        blank(code, production),
    )


def code_lines(text):
    return sum(1 for line in text.split("\n") if line.strip())


def rust_files(root):
    return sorted(os.path.relpath(path, root).replace(os.sep, "/") for path in iter_rust_files(root))


def ops_root(rel):
    for prefix in OPERATIONS_SRCS:
        if rel.startswith(prefix + "/"):
            return prefix
    return None


def module_path(rel):
    prefix = ops_root(rel)
    parts = rel[len(prefix) + 1 : -3].split("/")
    if parts[-1] in ("mod", "lib", "main"):
        parts = parts[:-1]
    return tuple(parts)


class Operations:
    """Production files of operations crates with their class, module and resolved references."""

    def __init__(self, sources):
        self.files = {}
        for source in sources:
            if not ops_root(source.rel) or is_test(source.rel):
                continue
            path = module_path(source.rel)
            has_op = bool(OPERATION_RE.search(source.masked))
            has_async = bool(ASYNC_RE.search(source.masked))
            kind = "mixed" if has_op and has_async else "op" if has_op else "async" if has_async else "plain"
            self.files[source.rel] = {
                "source": source,
                "path": path,
                "module": path[0] if path else None,
                "kind": kind,
                "lines": code_lines(source.code),
                "defs": set(DEF_RE.findall(source.masked)),
            }
        self.by_path = {info["path"]: rel for rel, info in self.files.items()}
        self.subtree = collections.defaultdict(list)
        for rel, info in sorted(self.files.items()):
            for size in range(len(info["path"]) + 1):
                self.subtree[info["path"][:size]].append(rel)
        self.cache = {}
        self.refs = {rel: list(self.references(rel)) for rel in self.files}

    def defining(self, path, item):
        key = (path, item)
        if key not in self.cache:
            found = [rel for rel in self.subtree.get(path, ()) if item in self.files[rel]["defs"]]
            self.cache[key] = found[0] if found else None
        return self.cache[key]

    def references(self, rel):
        """Files whose items this file names through crate, super or self paths."""
        info = self.files[rel]
        masked = info["source"].masked
        paths = []
        for tree in USE_RE.findall(masked):
            paths.extend(use_paths(SPACE_RE.sub(" ", tree)))
        for inline in INLINE_RE.findall(USE_RE.sub("", masked)):
            paths.append(tuple(inline.split("::")))
        for path in paths:
            target = self.resolve(path, info["path"])
            if target and target != rel:
                yield target

    def resolve(self, path, here):
        head, rest = path[0], list(path[1:])
        if head == "crate" or head in OPERATIONS_CRATES:
            base = []
        elif head == "self":
            base = list(here)
        elif head == "super":
            base, rest = list(here), list(path)
            while rest and rest[0] == "super":
                base, rest = base[:-1], rest[1:]
        else:
            return None
        full = base + rest
        if len(full) < 2 or full[-1] in ("*", "self"):
            return None
        module = tuple(full[:-1])
        while module and module not in self.by_path:
            module = module[:-1]
        if not module:
            return None
        return self.defining(module, full[-1])


def split_top(body):
    parts, depth, token = [], 0, ""
    for char in body:
        depth += char == "{"
        depth -= char == "}"
        if char == "," and depth == 0:
            parts.append(token)
            token = ""
        else:
            token += char
    parts.append(token)
    return parts


def use_paths(tree, prefix=()):
    """Expand a use tree such as crate::a::{b, c::{d as e}} into item paths."""
    tree = tree.strip()
    brace = tree.find("{")
    if brace < 0:
        return [prefix + segments(tree)]
    head = segments(tree[:brace])
    body = tree[brace + 1 : tree.rfind("}")]
    paths = []
    for part in split_top(body):
        if part.strip():
            paths.extend(use_paths(part, prefix + head))
    return paths


def segments(text):
    return tuple(part.split(" as ")[0].strip() for part in text.split("::") if part.strip())


def group_rank(module):
    for rank, (_, members) in enumerate(GROUPS):
        if module in members:
            return rank
    return None


def cycle_groups(edges):
    """Strongly connected groups of two or more modules."""
    graph = collections.defaultdict(set)
    for source, target in edges:
        graph[source].add(target)
    reach = {}
    for node in list(graph):
        seen, stack = set(), [node]
        while stack:
            for target in graph[stack.pop()]:
                if target not in seen:
                    seen.add(target)
                    stack.append(target)
        reach[node] = seen
    groups = set()
    for node in reach:
        group = frozenset({node} | {other for other in reach[node] if node in reach.get(other, ())})
        if len(group) > 1:
            groups.add(group)
    return sorted(sorted(group) for group in groups)


def layer_counts(ops):
    """Counts and details for mixed files, references and groups."""
    details = collections.defaultdict(list)
    upward, inside, pure_async = collections.Counter(), collections.Counter(), 0
    for rel, info in sorted(ops.files.items()):
        if info["kind"] == "mixed":
            details["mixed_files"].append(rel)
        for target in ops.refs[rel]:
            other = ops.files[target]
            source_mod, target_mod = info["module"], other["module"]
            if info["kind"] in ("op", "plain") and other["kind"] == "async":
                pure_async += 1
                details["pure_async_refs"].append(f"{rel} -> {target}")
            if source_mod == target_mod or source_mod is None or target_mod is None:
                continue
            source_rank, target_rank = group_rank(source_mod), group_rank(target_mod)
            if source_rank is None or target_rank is None:
                continue
            if target_rank > source_rank:
                upward[(source_mod, target_mod)] += 1
            elif target_rank == source_rank:
                inside[(source_mod, target_mod)] += 1
    mutual = sorted((a, b) for a, b in inside if a < b and (b, a) in inside)
    cycles = cycle_groups(inside)
    details["upward_refs"] = [f"{a} -> {b}: {count}" for (a, b), count in sorted(upward.items())]
    details["group_mutual_pairs"] = [f"{a} <-> {b}" for a, b in mutual]
    details["group_cycle_modules"] = [", ".join(group) for group in cycles]
    counts = {
        "mixed_files": len(details["mixed_files"]),
        "pure_async_refs": pure_async,
        "upward_refs": sum(upward.values()),
        "group_mutual_pairs": len(mutual),
        "group_cycle_modules": sum(len(group) for group in cycles),
    }
    return counts, details


def async_lines(ops):
    lines = collections.Counter()
    for info in ops.files.values():
        if info["module"] and info["kind"] in ("async", "mixed"):
            lines[info["module"]] += info["lines"]
    return dict(sorted(lines.items()))


def depth_find(masked, pos, stop, wanted):
    """First token in wanted at bracket depth zero between pos and stop."""
    depth = 0
    for found in ARM_RE.finditer(masked, pos, stop):
        mark = found.group()
        if mark in "([{":
            if depth == 0 and mark in wanted:
                return found
            depth += 1
        elif mark in ")]}":
            depth -= 1
        elif depth == 0 and mark in wanted:
            return found
    return None


def match_arms(masked):
    """Yield (pattern, body) for every arm of every match expression."""
    for found in MATCH_RE.finditer(masked):
        opening = depth_find(masked, found.end(), len(masked), ("{",))
        if not opening:
            continue
        stop = pair_end(masked, opening.start(), BRACE_RE) - 1
        pos = opening.end()
        while pos < stop:
            arrow = depth_find(masked, pos, stop, ("=>",))
            if not arrow:
                break
            body_start = arrow.end()
            while body_start < stop and masked[body_start].isspace():
                body_start += 1
            if masked[body_start : body_start + 1] == "{":
                body_end = pair_end(masked, body_start, BRACE_RE)
                comma = COMMA_RE.match(masked, body_end)
                next_pos = comma.end() if comma else body_end
            else:
                comma = depth_find(masked, body_start, stop, (",",))
                body_end = comma.start() if comma else stop
                next_pos = comma.end() if comma else stop
            yield masked[pos : arrow.start()], masked[body_start:body_end]
            pos = next_pos


def trigger_arms(sources):
    """Arms of the trigger shells that do more than drive one operation; a shell without its
    dispatch match counts too, so a moved shell cannot pass unchecked."""
    found = []
    for rel, prefix in TRIGGER_MATCHES:
        source = sources.get(rel)
        masked = source.masked if source else ""
        arms = [(pattern, body) for pattern, body in match_arms(masked) if prefix in pattern]
        if not arms:
            found.append(f"{rel}: no {prefix} dispatch")
        helper, table = TRIGGER_HELPERS.get(rel, ("", ""))
        handed = False
        for pattern, body in arms:
            name = SPACE_RE.sub(" ", pattern.strip())
            if ASYNC_RE.search(body):
                if not one_drive(body):
                    found.append(f"{rel}: {name}")
                continue
            # A helper future returned without an inline async block must be the table helper.
            for call in CALL_RE.findall(body):
                if call == helper:
                    handed = True
                elif call != "pin":
                    found.append(f"{rel}: {name} hands off to {call}")
        if handed:
            found.extend(helper_shape(rel, masked, helper, table))
    for rel, trait in TRIGGER_IMPLS:
        source = sources.get(rel)
        if not source:
            found.append(f"{rel}: no {trait} impl source")
            continue
        impl_re = re.compile(r"\bimpl\b[^{;]*\b" + trait + r"\s+for\b[^{;]*\{")
        for impl in impl_re.finditer(source.masked):
            body = source.masked[impl.end() - 1 : pair_end(source.masked, impl.end() - 1, BRACE_RE)]
            for name, keyword, start, end in function_bodies(body):
                if body[:keyword].rstrip().endswith("async") and not one_drive(body[start:end]):
                    found.append(f"{rel}: {trait}::{name}")
    return found


def helper_shape(rel, masked, helper, table):
    """Findings for a table helper that does not look its operation up once and drive it."""
    for name, _, start, end in function_bodies(masked):
        if name == helper:
            body = masked[start:end]
            lookups = len(re.findall(r"\b" + table + r"\s*\(", body))
            if lookups == 1 and DRIVE_ANY.search(body):
                return []
            return [f"{rel}: {helper} does not drive one {table} operation"]
    return [f"{rel}: no {helper} helper"]


def one_drive(body):
    """Whether an async arm only builds one operation, drives it and maps the result."""
    awaits = body.count(".await")
    return awaits == 0 or (awaits == 1 and DRIVE_RE.search(body) is not None)


def function_bodies(masked):
    """Yield (name, keyword, start, end) per fn with a body; start is the offset of its '{'."""
    for found in FN_RE.finditer(masked):
        opening = None
        depth = 0
        for mark in ITEM_RE.finditer(masked, found.end()):
            token = mark.group()
            if token in "([":
                depth += 1
            elif token in ")]":
                depth -= 1
            elif depth == 0:
                opening = mark if token == "{" else None
                break
        if opening:
            yield found.group(1), found.start(), opening.start(), pair_end(masked, opening.start(), BRACE_RE)


def test_habits(sources):
    """Real sleeps outside paused tests and test-hooks helpers, and tests that print and return
    early."""
    sleeps, skips = [], []
    hooked = hook_modules(sources)
    for source in sources:
        text = source.test_masked
        if "sleep" in text:
            sleeps.extend(real_sleeps(source.rel, text))
        hooks = hook_text(source, hooked)
        if "sleep" in hooks:
            sleeps.extend(real_sleeps(source.rel, hooks))
        if "println!" in text:
            for found in SKIP_RE.finditer(text):
                skips.append(f"{source.rel}:{text.count(chr(10), 0, found.start()) + 1}")
    return sleeps, skips


def hook_text(source, hooked):
    """The production text that only the test-hooks feature compiles: a gated module file whole,
    else its gated items."""
    if any(source.rel == f"{prefix}.rs" or source.rel.startswith(f"{prefix}/") for prefix in hooked):
        return source.masked
    spans = hook_spans(source)
    return blank(source.masked, outside(spans, len(source.masked))) if spans else ""


def real_sleeps(rel, text):
    """Sleep calls whose outermost enclosing fn neither starts paused nor pauses time, outside
    the one shared bounded poll helper."""
    outer, found_sleeps = [], []
    for name, keyword, start, end in function_bodies(text):
        if outer and start < outer[-1][2]:
            continue
        head = max(text.rfind("}", 0, keyword), text.rfind(";", 0, keyword), 0)
        outer.append((head, start, end, name))
    for found in SLEEP_RE.finditer(text):
        if text[: found.start()].rstrip().endswith("fn"):
            continue
        owner = next((item for item in outer if item[0] <= found.start() < item[2]), None)
        if owner and PAUSE_RE.search(text, owner[0], owner[2]):
            continue
        if owner and (rel, owner[3]) == POLL_HELPER:
            continue
        found_sleeps.append(f"{rel}:{text.count(chr(10), 0, found.start()) + 1}")
    return found_sleeps


def duplicate_groups(sources):
    """Groups of identical fn bodies with at least DUPLICATE_LINES code lines inside the braces."""
    groups = collections.defaultdict(list)
    for source in sources:
        for code, masked in ((source.code, source.masked), (source.test_code, source.test_masked)):
            for name, _, start, end in function_bodies(masked):
                inner = code[start + 1 : end - 1]
                if code_lines(inner) < DUPLICATE_LINES:
                    continue
                digest = hashlib.sha1(SPACE_RE.sub("", inner).encode()).hexdigest()
                line = code.count("\n", 0, start) + 1
                groups[digest].append(f"{source.rel}:{line} {name}")
    return sorted(sorted(members) for members in groups.values() if len(members) > 1)


def similar_names(sources):
    """Production fn names that match SIMILAR_NAMES, as (pattern, definitions, files)."""
    rows = []
    for pattern in SIMILAR_NAMES:
        hits = []
        for source in sources:
            for name in FN_RE.findall(source.masked):
                if fnmatch.fnmatchcase(name, pattern):
                    hits.append(source.rel)
        rows.append((pattern, len(hits), len(set(hits))))
    return rows


def workspace_members(root):
    """Map each workspace member directory to its manifest; also return the dependency renames."""
    with open(os.path.join(root, "Cargo.toml"), "rb") as handle:
        workspace = tomllib.load(handle).get("workspace", {})
    renames = {
        key: value.get("package", key)
        for key, value in workspace.get("dependencies", {}).items()
        if isinstance(value, dict)
    }
    manifests = {}
    for member in workspace.get("members", []):
        with open(os.path.join(root, member, "Cargo.toml"), "rb") as handle:
            manifests[member] = tomllib.load(handle)
    return manifests, renames


def crate_manifests(root):
    """Map each workspace crate name to its internal normal and build dependencies; backends add
    their dev-dependencies."""
    manifests, renames = workspace_members(root)
    names = {data["package"]["name"] for data in manifests.values()}
    result = {}
    for data in manifests.values():
        crate = data["package"]["name"]
        sections = [data, *data.get("target", {}).values()]
        normal_deps = set()
        for kind in ("dependencies", "build-dependencies"):
            for key, value in (item for section in sections for item in section.get(kind, {}).items()):
                name = value.get("package", renames.get(key, key)) if isinstance(value, dict) else key
                if name in names:
                    normal_deps.add(name)
        deps = set(normal_deps)
        if crate in BACKENDS:
            dev_allow = set(BACKEND_DEV_TARGETS.get(crate, ()))
            for table in (section.get("dev-dependencies", {}) for section in sections):
                for key, value in table.items():
                    name = value.get("package", renames.get(key, key)) if isinstance(value, dict) else key
                    if name in names and name not in dev_allow:
                        deps.add(name)
        result[crate] = deps
    return result


def crate_findings(root, allow):
    """Return (outside, unused): dependencies outside target and allow list, and unused allow entries."""
    outside_deps, used = [], set()
    for crate, deps in sorted(crate_manifests(root).items()):
        target = CRATE_TARGETS.get(crate, ())
        for dep in sorted(deps):
            if target == ANY_CRATE or dep in target:
                continue
            if dep in allow.get(crate, ()):
                used.add((crate, dep))
                continue
            outside_deps.append(f"{crate} -> {dep}")
    unused = [f"{crate} -> {dep}" for crate, deps in sorted(allow.items()) for dep in deps if (crate, dep) not in used]
    return outside_deps, unused


def member_deps(data, renames, names):
    """Internal normal and build dependencies with their optional flag, and internal dev-dependencies."""
    normal, dev = {}, set()
    for section in [data, *data.get("target", {}).values()]:
        for kind in ("dependencies", "build-dependencies", "dev-dependencies"):
            for key, value in section.get(kind, {}).items():
                spec = value if isinstance(value, dict) else {}
                name = spec.get("package", renames.get(key, key))
                if name not in names:
                    continue
                if kind == "dev-dependencies":
                    dev.add(name)
                else:
                    normal[name] = normal.get(name, True) and spec.get("optional", False)
    return normal, dev


def layer_rule(crate, layer, dep, dep_layer, optional, layers):
    """Why the normal dependency crate -> dep breaks the layer order, or None."""
    if dep_layer == "testing" and not optional and layer in ("apps", "domain", "runtime"):
        return "testing crates are never a required dependency here"
    if layer == "domain" and dep_layer != "domain":
        return "domain crates depend only on domain crates"
    if layer == "runtime":
        allowed = dep in RUNTIME_FOUNDATIONS or dep_layer == "runtime" or dep in RUNTIME_EXTRAS.get(crate, ())
        if not allowed:
            return "runtime crates depend only on runtime crates and domain foundations"
    if layer == "frontends":
        if dep_layer == "apps":
            return "frontends never depend on apps"
        if dep_layer == "runtime" and dep not in FRONTEND_RUNTIME:
            return "frontends reach the runtime only through the clock and the runner"
        if dep_layer == "testing" and not optional:
            return "frontends name test helpers only as an optional dependency"
    return None


def layer_findings(root):
    """Dependencies that break the directory-derived layer order, and crates the table lacks."""
    manifests, renames = workspace_members(root)
    layers = {data["package"]["name"]: member.split("/")[0] for member, data in manifests.items()}
    found = []
    for member, data in sorted(manifests.items()):
        crate = data["package"]["name"]
        layer = layers[crate]
        if layer not in LAYERS:
            found.append(f"{crate}: {member} lies outside the layer directories")
        if crate not in CRATE_TARGETS:
            found.append(f"{crate}: missing from CRATE_TARGETS")
        normal, dev = member_deps(data, renames, layers)
        for dep, optional in sorted(normal.items()):
            reason = layer_rule(crate, layer, dep, layers[dep], optional, layers)
            if reason:
                found.append(f"{crate} -> {dep}: {reason}")
        if crate in OPS_ORDER:
            higher = OPS_ORDER[OPS_ORDER.index(crate) + 1 :]
            for dep in sorted((set(normal) | dev) & set(higher)):
                found.append(f"{crate} -> {dep}: operations crates never reach a higher one")
    return found


def tree_aliases(tree, prefix=()):
    """Effect aliases bound by a `use` tree, including grouped and raw-identifier aliases."""
    tree = tree.strip()
    brace = tree.find("{")
    close = tree.rfind("}")
    if brace >= 0 and close > brace:
        head = segments(tree[:brace])
        body = tree[brace + 1 : close]
        found = []
        for part in split_top(body):
            if part.strip():
                found.extend(tree_aliases(part, prefix + head))
        return found
    parts = re.split(r"\s+as\s+", tree, maxsplit=1)
    if len(parts) != 2:
        return []
    alias = parts[1].strip()
    if not alias or alias == "_":
        return []
    path = prefix + tuple(part.strip() for part in parts[0].split("::") if part.strip())
    if path and path[-1] == "self":
        path = path[:-1]
    return [alias] if path and path[-1].removeprefix("r#") in EFFECT_NAMES else []


def scoped_aliases(masked):
    """Each effect alias paired with the `(start, end)` span of its enclosing block."""
    scopes = [[0, len(masked)]]
    stack = [0]
    bound = []
    for hit in EFFECT_USE.finditer(masked):
        tree = hit.group(1)
        if tree is not None:
            for alias in tree_aliases(SPACE_RE.sub(" ", tree)):
                bound.append((stack[-1], alias))
        elif hit.group() == "{":
            stack.append(len(scopes))
            scopes.append([hit.start(), len(masked)])
        elif len(stack) > 1:
            scopes[stack.pop()][1] = hit.end()
    return [(alias, tuple(scopes[scope])) for scope, alias in bound]


def raw_effects(sources):
    """Raw storage, net and blob effect paths in API production code, also through a `use` alias;
    handlers use operations."""
    found = []
    for source in sources:
        if not source.rel.startswith(API_SRC):
            continue
        starts = {hit.start() for hit in RAW_EFFECT.finditer(source.masked)}
        for alias, (start, end) in scoped_aliases(source.masked):
            name = alias.removeprefix("r#")
            pattern = re.compile(rf"(?<![\w#])(?:r#)?{re.escape(name)}::")
            starts.update(hit.start() for hit in pattern.finditer(source.masked, start, end))
        found.extend(f"{source.rel}:{source.masked.count(chr(10), 0, pos) + 1}" for pos in sorted(starts))
    return found


def measure(root):
    """Measure every count; returns (counts, details, async lines per module, operations, sources)."""
    sources = [load_source(root, rel) for rel in rust_files(root)]
    ops = Operations(sources)
    counts, details = layer_counts(ops)
    by_rel = {source.rel: source for source in sources}
    details["trigger_arms"] = trigger_arms(by_rel)
    operations = [ops.files[rel]["source"] for rel in sorted(ops.files)]
    details["state_clones"] = [
        f"{source.rel}:{source.masked.count(chr(10), 0, found.start()) + 1}"
        for source in operations
        for found in STATE_CLONE.finditer(source.masked)
    ]
    details["async_parallel"] = [source.rel for source in operations if PARALLEL_RE.search(source.masked)]
    details["test_sleeps"], details["silent_skips"] = test_habits(sources)
    details["core_clocks"] = core_clocks(sources)
    details["operation_clocks"] = core_clocks(sources, OPERATIONS_SRCS)
    duplicates = duplicate_groups(sources)
    details["duplicate_groups"] = [" | ".join(group) for group in duplicates]
    details["api_raw_effects"] = raw_effects(sources)
    for key in (
        "trigger_arms",
        "state_clones",
        "async_parallel",
        "test_sleeps",
        "silent_skips",
        "core_clocks",
        "operation_clocks",
        "duplicate_groups",
        "api_raw_effects",
    ):
        counts[key] = len(details[key])
    return counts, details, async_lines(ops), ops, sources


def hook_spans(source):
    """Spans of items that only tests or the test-hooks feature compile."""
    return [
        (found.start(), item_end(source.masked, found.end()))
        for found in HOOK_ATTR.finditer(source.code)
    ]


def hook_modules(sources):
    """Path prefixes of module files declared under a test-hooks gate."""
    prefixes = []
    for source in sources:
        stem = source.rel[:-3]
        base = os.path.dirname(stem) if os.path.basename(stem) in ("lib", "mod", "main") else stem
        for start, end in hook_spans(source):
            for found in HOOK_MOD.finditer(source.masked, start, end):
                prefixes.append(f"{base}/{found.group(1)}")
    return prefixes


def core_clocks(sources, root="domain/core/src/"):
    """Clock, random and id-mint calls in a crate's production code; test hooks are helpers."""
    prefixes = tuple(item.rstrip("/") + "/" for item in root) if isinstance(root, tuple) else (root,)
    core = [source for source in sources if source.rel.startswith(prefixes)]
    hooked = hook_modules(core)
    found = []
    for source in core:
        if any(source.rel == f"{prefix}.rs" or source.rel.startswith(f"{prefix}/") for prefix in hooked):
            continue
        spans = hook_spans(source)
        for hit in CLOCK_RE.finditer(source.masked):
            if not any(start <= hit.start() < end for start, end in spans):
                found.append(f"{source.rel}:{source.masked.count(chr(10), 0, hit.start()) + 1}")
    return found


def async_findings(ops):
    """Every production file of operations with async code, at its first async line."""
    found = []
    for rel, info in sorted(ops.files.items()):
        if info["kind"] in ("async", "mixed"):
            hit = ASYNC_RE.search(info["source"].masked)
            line = info["source"].masked.count("\n", 0, hit.start()) + 1
            found.append(f"{rel}:{line}")
    return found


def load_baseline(path):
    try:
        with open(path, encoding="utf-8") as handle:
            baseline = json.load(handle)
    except FileNotFoundError:
        baseline = {}
    missing = [key for key, _ in COUNTS if key not in baseline.get("structural", {})]
    if "crate_allow" not in baseline:
        missing.append("crate_allow")
    return baseline, missing


def report(root, baseline, show_all):
    """Print the guard report and return the number of errors."""
    counts, details, lines, ops, sources = measure(root)
    errors = 0
    print(f"async production lines in operations: {sum(lines.values())}")
    for module, count in lines.items():
        print(f"  {module}: {count}")
    for hit in async_findings(ops):
        errors += 1
        print(f"  error: async code in operations production at {hit}")
    for key, label in COUNTS:
        now, limit = counts[key], baseline["structural"][key]
        print(f"{label}: {now} (baseline {limit})")
        if now > limit:
            errors += 1
            print(f"  error: {key} rose above its baseline; fix the cause or raise it in {BASELINE_PATH}")
        elif now < limit:
            print(f"  note: {key} fell below its baseline; lower it in {BASELINE_PATH}")
        if show_all or now > limit:
            for line in details[key]:
                print(f"    {line}")
    outside_deps, unused = crate_findings(root, baseline["crate_allow"])
    print(f"crate dependencies outside the target table and allow list: {len(outside_deps)}")
    for dep in outside_deps:
        errors += 1
        print(f"  error: {dep}")
    for dep in unused:
        print(f"  note: allow list entry not used: {dep}")
    layer_breaks = layer_findings(root)
    print(f"layer order breaks: {len(layer_breaks)}")
    for finding in layer_breaks:
        errors += 1
        print(f"  error: {finding}")
    print("similar function names (report):")
    for pattern, defs, files in similar_names(sources):
        print(f"  {pattern}: {defs} definitions in {files} files")
    print(f"errors: {errors}")
    return errors


def current_json(root, baseline):
    counts, _, _, _, _ = measure(root)
    result = {
        "crate_allow": baseline.get("crate_allow", {}),
        "structural": {key: counts[key] for key, _ in COUNTS},
    }
    return json.dumps(result, indent=2) + "\n"


def main(argv):
    parser = argparse.ArgumentParser(description="Aruna layer guard")
    parser.add_argument("--root", default=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    parser.add_argument("--baseline", help=f"baseline file (default: <root>/{BASELINE_PATH})")
    parser.add_argument("--all", action="store_true", help="print details for every count")
    parser.add_argument("--json", action="store_true", help="print the measured values as baseline JSON")
    args = parser.parse_args(argv)
    root = os.path.abspath(args.root)
    baseline, missing = load_baseline(args.baseline or os.path.join(root, BASELINE_PATH))
    if args.json:
        sys.stdout.write(current_json(root, baseline))
        return 0
    if missing:
        print(f"{BASELINE_PATH} lacks: {', '.join(missing)}", file=sys.stderr)
        return 2
    return 1 if report(root, baseline, args.all) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
