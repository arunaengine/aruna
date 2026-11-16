#!/usr/bin/env python3
"""Layer guard: counts async code, references, test habits and duplicates against layers_baseline.json.
A structural count above its baseline, async code in a clean domain or a new crate dependency fails."""
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

OPERATIONS_SRC = "operations/src"
BASELINE_PATH = "scripts/dev/layers_baseline.json"
RUNNER = frozenset({"driver", "effect_adapters", "storage_read"})
# Domain groups of target-architecture.md section 5, lowest first. A module may
# use its own group and lower groups only.
GROUPS = (
    (
        "identity and realm",
        frozenset(
            "abe auth realm realm_vault node_vault secrets session users groups device onboarding "
            "placement shard sync owner_index document_repository endpoint_screening forward tasks".split()
        ),
    ),
    ("data", frozenset("s3 blob staging replication distribution metadata git shares connectors".split())),
    ("work and integrations", frozenset("jobs plugins assistant notifications federation harvest node".split())),
)
ANY_CRATE = "*"
# Internal normal dependencies the target table (target-architecture.md section 2) allows.
CRATE_TARGETS = {
    "aruna": ANY_CRATE,
    "aruna-api": ("aruna-core", "aruna-operations", "aruna-drivers", "aruna-wire"),
    "aruna-doctor": ("aruna-core", "aruna-storage", "aruna-blob", "aruna-operations", "aruna-drivers"),
    "aruna-cli": ("aruna-wire", "aruna-plugins"),
    "aruna-operations": ("aruna-core",),
    "aruna-drivers": (
        "aruna-core",
        "aruna-storage",
        "aruna-net",
        "aruna-blob",
        "aruna-tasks",
        "aruna-compute",
        "aruna-plugins",
        "aruna-graph",
        "aruna-outbound",
    ),
    "aruna-storage": ("aruna-core",),
    "aruna-net": ("aruna-core", "aruna-storage"),
    "aruna-blob": ("aruna-core", "aruna-kpabe", "aruna-storage", "aruna-net"),
    "aruna-tasks": ("aruna-core",),
    "aruna-compute": ("aruna-core",),
    "aruna-plugins": (),
    "aruna-graph": ("aruna-core",),
    "aruna-outbound": ("aruna-core",),
    "aruna-core": ("aruna-kpabe",),
    "aruna-kpabe": (),
    "aruna-kpabe-wasm": ("aruna-kpabe",),
    "aruna-wire": ("aruna-core",),
    "aruna-plugin": (),
    "aruna-plugin-fixture": ("aruna-plugin",),
    "elabftw-sync": ("aruna-plugin",),
    "dataset-announcer": ("aruna-plugin",),
    "aruna-registry": ("aruna-core", "aruna-blob"),
    "aruna-compute-helper": (),
}
TRIGGER_MATCHES = (
    ("operations/src/tasks/incoming/mod.rs", "TaskKey::"),
    ("operations/src/sync/incoming.rs", "Alpn::"),
)
TRIGGER_IMPLS = (("operations/src/plugins/broker.rs", "Broker"),)
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
    ("runner_refs", "runner references into domain modules"),
    ("trigger_arms", "trigger shell arms that call async code"),
    ("state_clones", "match self.state.clone() in operations"),
    ("async_parallel", "files with parallel work only through async code"),
    ("test_sleeps", "real sleeps in tests without paused time"),
    ("silent_skips", "tests that print and return early"),
    ("duplicate_groups", "duplicate function bodies of six or more lines"),
)
DUPLICATE_LINES = 6

TOKEN_RE = re.compile(r"//|/\*|(?<!\w)(?:[bc]?r#*|[bc])?\"|'")
STRING_RE = re.compile(r'(?:[^"\\]|\\.)*"', re.S)
TEST_ATTR = re.compile(r"#\[\s*(?:cfg\s*\(\s*test\s*\)|test|rstest|tokio::test\b[^\]]*)\s*\]")
FILE_TEST = re.compile(r"#!\[\s*cfg\s*\(\s*test\s*\)\s*\]")
ATTR_RE = re.compile(r"\s*#\[")
BRACE_RE = re.compile(r"[{}]")
BRACKET_RE = re.compile(r"[\[\]]")
ITEM_RE = re.compile(r"[()\[\]{};]")
ARM_RE = re.compile(r"=>|[()\[\]{},]")
ASYNC_RE = re.compile(r"\basync\s+(?:move\s+)?(?:fn\b|\{|\|)|\.await\b")
OPERATION_RE = re.compile(r"\bimpl(?:\s*<[^{;]*?>)?\s+(?:[\w:]+::)?(?:Sub)?Operation\s+for\b")
DEF_RE = re.compile(r"\b(?:struct|enum|trait|type|fn|const|static|mod|union)\s+([A-Za-z_]\w*)")
USE_RE = re.compile(r"\buse\s+((?:crate|super|self)\b[^;]*);")
INLINE_RE = re.compile(r"\bcrate(?:::\w+)+")
FN_RE = re.compile(r"\bfn\s+([A-Za-z_]\w*)")
MATCH_RE = re.compile(r"\bmatch\b")
STATE_CLONE = re.compile(r"\bmatch\s+self\s*\.\s*state\s*\.\s*clone\s*\(\s*\)")
PARALLEL_RE = re.compile(r"\b(?:join_all|FuturesUnordered|buffer_unordered|JoinSet|try_join\w*)\b")
SLEEP_RE = re.compile(r"(?<![\w.:])(?:(?:std::)?thread::|(?:tokio::)?time::)?sleep\s*\(")
PAUSE_RE = re.compile(r"\bstart_paused\b|\bpause\s*\(\s*\)")
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


def module_path(rel):
    parts = rel[len(OPERATIONS_SRC) + 1 : -3].split("/")
    if parts[-1] in ("mod", "lib", "main"):
        parts = parts[:-1]
    return tuple(parts)


class Operations:
    """Production files of operations/src with their class, module and resolved references."""

    def __init__(self, sources):
        self.files = {}
        for source in sources:
            if not source.rel.startswith(OPERATIONS_SRC + "/") or is_test(source.rel):
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
        if head == "crate":
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
    """Counts and details for mixed files, references, groups and the runner."""
    details = collections.defaultdict(list)
    upward, inside, pure_async, runner = collections.Counter(), collections.Counter(), 0, collections.Counter()
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
            if source_mod in RUNNER and target_mod not in RUNNER:
                runner[(source_mod, target_mod)] += 1
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
    details["runner_refs"] = [f"{a} -> {b}: {count}" for (a, b), count in sorted(runner.items())]
    counts = {
        "mixed_files": len(details["mixed_files"]),
        "pure_async_refs": pure_async,
        "upward_refs": sum(upward.values()),
        "group_mutual_pairs": len(mutual),
        "group_cycle_modules": sum(len(group) for group in cycles),
        "runner_refs": sum(runner.values()),
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
    """Arms of the trigger shells whose bodies still run async code."""
    found = []
    for rel, prefix in TRIGGER_MATCHES:
        source = sources.get(rel)
        if not source:
            continue
        for pattern, body in match_arms(source.masked):
            if prefix in pattern and ASYNC_RE.search(body):
                found.append(f"{rel}: {SPACE_RE.sub(' ', pattern.strip())}")
    for rel, trait in TRIGGER_IMPLS:
        source = sources.get(rel)
        if not source:
            continue
        impl_re = re.compile(r"\bimpl\b[^{;]*\b" + trait + r"\s+for\b[^{;]*\{")
        for impl in impl_re.finditer(source.masked):
            body = source.masked[impl.end() - 1 : pair_end(source.masked, impl.end() - 1, BRACE_RE)]
            for method in re.finditer(r"\basync\s+fn\s+(\w+)", body):
                found.append(f"{rel}: {trait}::{method.group(1)}")
    return found


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
    """Real sleeps outside paused tests, and tests that print and return early."""
    sleeps, skips = [], []
    for source in sources:
        text = source.test_masked
        if "sleep" in text:
            sleeps.extend(real_sleeps(source.rel, text))
        if "println!" in text:
            for found in SKIP_RE.finditer(text):
                skips.append(f"{source.rel}:{text.count(chr(10), 0, found.start()) + 1}")
    return sleeps, skips


def real_sleeps(rel, text):
    """Sleep calls whose outermost enclosing fn neither starts paused nor pauses time."""
    outer, found_sleeps = [], []
    for _, keyword, start, end in function_bodies(text):
        if outer and start < outer[-1][2]:
            continue
        head = max(text.rfind("}", 0, keyword), text.rfind(";", 0, keyword), 0)
        outer.append((head, start, end))
    for found in SLEEP_RE.finditer(text):
        if text[: found.start()].rstrip().endswith("fn"):
            continue
        owner = next((item for item in outer if item[0] <= found.start() < item[2]), None)
        if owner and PAUSE_RE.search(text, owner[0], owner[2]):
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


def crate_manifests(root):
    """Map each workspace crate name to its set of internal normal dependencies."""
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
    names = {data["package"]["name"] for data in manifests.values()}
    result = {}
    for data in manifests.values():
        tables = [data.get("dependencies", {})]
        tables.extend(target.get("dependencies", {}) for target in data.get("target", {}).values())
        deps = set()
        for table in tables:
            for key, value in table.items():
                name = value.get("package", renames.get(key, key)) if isinstance(value, dict) else key
                if name in names:
                    deps.add(name)
        result[data["package"]["name"]] = deps
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
    duplicates = duplicate_groups(sources)
    details["duplicate_groups"] = [" | ".join(group) for group in duplicates]
    for key in ("trigger_arms", "state_clones", "async_parallel", "test_sleeps", "silent_skips", "duplicate_groups"):
        counts[key] = len(details[key])
    return counts, details, async_lines(ops), ops, sources


def clean_findings(ops, clean):
    found = []
    for rel, info in sorted(ops.files.items()):
        if info["module"] in clean and info["kind"] in ("async", "mixed"):
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
    for key in ("clean_domains", "crate_allow", "async_lines"):
        if key not in baseline:
            missing.append(key)
    return baseline, missing


def report(root, baseline, show_all):
    """Print the guard report and return the number of errors."""
    counts, details, lines, ops, sources = measure(root)
    errors = 0
    total, base_total = sum(lines.values()), sum(baseline["async_lines"].values())
    print(f"async production lines in operations: {total} (baseline {base_total}), reported only")
    for module in sorted(set(lines) | set(baseline["async_lines"])):
        now, before = lines.get(module, 0), baseline["async_lines"].get(module, 0)
        if show_all or now != before:
            print(f"  {module}: {now} (baseline {before})")
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
    clean = baseline["clean_domains"]
    dirty = clean_findings(ops, set(clean))
    print(f"clean domains: {', '.join(clean) if clean else 'none'}")
    for hit in dirty:
        errors += 1
        print(f"  error: async code in a clean domain at {hit}")
    outside_deps, unused = crate_findings(root, baseline["crate_allow"])
    print(f"crate dependencies outside the target table and allow list: {len(outside_deps)}")
    for dep in outside_deps:
        errors += 1
        print(f"  error: {dep}")
    for dep in unused:
        print(f"  note: allow list entry not used: {dep}")
    print("similar function names (report):")
    for pattern, defs, files in similar_names(sources):
        print(f"  {pattern}: {defs} definitions in {files} files")
    print(f"errors: {errors}")
    return errors


def current_json(root, baseline):
    counts, _, lines, _, _ = measure(root)
    result = {
        "clean_domains": baseline.get("clean_domains", []),
        "crate_allow": baseline.get("crate_allow", {}),
        "structural": {key: counts[key] for key, _ in COUNTS},
        "async_lines": lines,
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
