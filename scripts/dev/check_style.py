#!/usr/bin/env python3
"""Style checks: folders >=5 files, fn/type/mod names and .rs stems <=3 terms, comments <=3 lines.
Terms split on _ and CamelCase; HTTP, UUID, S3, SHA256, OIDC and Ro-Crate count as one.
Skips src/tests/fixtures roots, assets, generated files, license notices, strings, attributes; EXTERNAL_NAMES needs a reason per name."""
import argparse
import bisect
import os
import re
import sys

SKIP_PARTS = frozenset(
    {"target", "vendor", "node_modules", "generated", ".git", ".github", ".cargo", ".config", ".claude"}
)
ASSET_PARTS = frozenset({"fixtures", "snapshots", "testdata"})
LICENSE_MARKS = ("spdx-license-identifier", "copyright", "licensed under", "license")
STRUCT_NAMES = frozenset({"bin", "benches", "examples"})
EXTERNAL_NAMES = {}
FOLDER_MIN = 5
TERM_MAX = 3
COMMENT_MAX = 3

WORD_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")
DECL_RE = re.compile(r"\b(fn|struct|enum|trait|type|mod)\s+([A-Za-z_]\w*)")
CHAR_RE = re.compile(r"'(?:\\.|\\u\{[0-9A-Fa-f_]+\}|[^'\\])'")
TOKEN_RE = re.compile(r"//|/\*|\"|'|#|[A-Za-z_]")
SCAN_RE = re.compile(r"//|/\*|\"|'|[A-Za-z_]|[()[\]{}]")
IDENT_RE = re.compile(r"[A-Za-z_]\w*")
BLANK_RE = re.compile(r"[^\n]")
TEST_ATTR_RE = re.compile(r"\b(?:test|rstest)\b")
MARK_RE = re.compile(r"^\s*(///|//!|/\*+|//|\*+/|\*)\s?")
PREFIXES = ("b", "c", "r", "br", "rb", "cr")


def string_prefix(text, start, end):
    return end - start <= 2 and text[start:end].lower() in PREFIXES and text[end : end + 1] in ('"', "#")


def split_terms(name):
    terms = []
    for part in name.split("_"):
        terms.extend(WORD_RE.findall(part))
    merged = []
    for term in terms:
        if merged:
            prev, low = merged[-1].lower(), term.lower()
            if (prev == "ro" and low == "crate") or (prev.isalpha() and low.isdigit()):
                merged[-1] = merged[-1] + term
                continue
        merged.append(term)
    return merged


def scan_block(text, start):
    depth, i, n = 1, start + 2, len(text)
    while i < n:
        if text.startswith("/*", i):
            depth, i = depth + 1, i + 2
        elif text.startswith("*/", i):
            depth, i = depth - 1, i + 2
            if depth == 0:
                return i
        else:
            i += 1
    return n


def string_end(text, start):
    i, n, prefix = start, len(text), ""
    while i < n and text[i] in "rbcu":
        prefix, i = prefix + text[i], i + 1
    if "r" in prefix:
        hashes = 0
        while i < n and text[i] == "#":
            hashes, i = hashes + 1, i + 1
        if i >= n or text[i] != '"':
            return None
        close = '"' + "#" * hashes
        end = text.find(close, i + 1)
        return n if end < 0 else end + len(close)
    if i >= n or text[i] != '"':
        return None
    i += 1
    while i < n:
        if text[i] == "\\":
            i += 2
        elif text[i] == '"':
            return i + 1
        else:
            i += 1
    return n


def record_comment(text, start, end, comments):
    block = text.startswith("/*", start)
    body = text[start + 2 : max(start + 2, end - 2)] if block else text[start + 2 : end]
    comments.append({"kind": "block" if block else "line", "start": start, "end": end, "body": body})


def balanced_end(text, start, open_ch, close_ch, comments=None):
    depth, i, n = 0, start, len(text)
    while i < n:
        match = SCAN_RE.search(text, i)
        if not match:
            return n
        i = match.start()
        ch = text[i]
        if ch in "()[]{}":
            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return i + 1
            i += 1
            continue
        if text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end < 0 else end
            if comments is not None:
                record_comment(text, i, end, comments)
            i = end
        elif text.startswith("/*", i):
            end = scan_block(text, i)
            if comments is not None:
                record_comment(text, i, end, comments)
            i = end
        elif ch == '"':
            i = string_end(text, i)
        elif ch == "'":
            match = CHAR_RE.match(text, i)
            i = match.end() if match else i + 1
        else:
            end = IDENT_RE.match(text, i).end()
            if string_prefix(text, i, end):
                skip = string_end(text, i)
                i = skip if skip is not None else end
            else:
                i = end
    return n


def mask_source(text):
    n = len(text)
    masked = list(text)
    comments, attrs = [], []

    def blank(start, end):
        masked[start:end] = BLANK_RE.sub(" ", text[start:end])

    i = 0
    while i < n:
        match = TOKEN_RE.search(text, i)
        if not match:
            break
        i = match.start()
        ch = text[i]
        if ch == "/":
            if text[i + 1 : i + 2] == "/":
                end = text.find("\n", i)
                if end < 0:
                    end = n
            else:
                end = scan_block(text, i)
            record_comment(text, i, end, comments)
            blank(i, end)
            i = end
        elif ch == '"':
            end = string_end(text, i)
            blank(i, end)
            i = end
        elif ch == "'":
            match = CHAR_RE.match(text, i)
            end = match.end() if match else i + 1
            blank(i, end)
            i = end
        elif ch == "#":
            j = i + 1 if text[i + 1 : i + 2] == "[" else i + 2
            if text[i + 1 : i + 2] in ("[", "!") and text[j : j + 1] == "[":
                end = balanced_end(text, j, "[", "]")
                attrs.append({"start": i, "end": end, "body": text[i:end]})
                blank(i, end)
                i = end
            else:
                masked[i] = " "
                i += 1
        else:
            j = IDENT_RE.match(text, i).end()
            if string_prefix(text, i, j):
                end = string_end(text, i)
                if end is not None:
                    blank(i, end)
                    i = end
                    continue
            k = j
            while k < n and text[k] in " \t\r\n":
                k += 1
            if text[k : k + 1] == "!":
                d = k + 1
                while d < n and text[d] in " \t\r\n":
                    d += 1
                if text[i:j] == "macro_rules":
                    named = IDENT_RE.match(text, d)
                    if named:
                        d = named.end()
                        while d < n and text[d] in " \t\r\n":
                            d += 1
                if text[d : d + 1] in ("(", "[", "{"):
                    open_ch = text[d]
                    close = {"(": ")", "[": "]", "{": "}"}[open_ch]
                    end = balanced_end(text, d, open_ch, close, comments)
                    blank(i, end)
                    i = end
                    continue
            i = j
    return "".join(masked), comments, attrs


def read_source(path):
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def class_file(text):
    for line in text.splitlines()[:12]:
        low = line.lower()
        if "@generated" in low or "code generated" in low or "do not edit" in low or "auto-generated" in low:
            return True
    return False


def iter_rust_files(root):
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        parts = [] if rel == "." else rel.split(os.sep)
        dirnames[:] = [d for d in dirnames if d not in SKIP_PARTS]
        if set(parts) & ASSET_PARTS:
            continue
        for name in sorted(filenames):
            if name.endswith(".rs"):
                yield os.path.join(dirpath, name)


def src_roots(root):
    for base, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_PARTS]
        if "Cargo.toml" in filenames:
            src = os.path.join(base, "src")
            if os.path.isdir(src):
                yield src


def check_folders(root):
    for src in src_roots(root):
        pkg = os.path.dirname(src)
        for dirpath, dirs, files in os.walk(src):
            dirs[:] = [d for d in dirs if d not in SKIP_PARTS and d not in ASSET_PARTS]
            if dirpath == src:
                continue
            if os.path.basename(dirpath) in STRUCT_NAMES and os.path.dirname(dirpath) == src:
                dirs[:] = []
                continue
            if os.path.basename(dirpath) == "tests" and os.path.dirname(dirpath) == pkg:
                dirs[:] = []
                continue
            if not any(f.endswith(".rs") for f in files):
                continue
            real = [f for f in files if not f.startswith(".") and f != "mod.rs"]
            if len(real) < FOLDER_MIN:
                yield (os.path.relpath(dirpath, root), len(real))


def decl_terms(name):
    return len(split_terms(name))


def attr_before(attrs, offset, masked):
    marks = [a for a in attrs if a["end"] <= offset]
    chosen = None
    for attr in reversed(marks):
        gap = offset if chosen is None else chosen["start"]
        if masked[attr["end"] : gap].strip():
            break
        chosen = attr
    return chosen


def check_sources(root):
    for path in iter_rust_files(root):
        rel = os.path.relpath(path, root)
        text = read_source(path)
        masked, comments, attrs = mask_source(text)
        yield from name_findings(rel, path, text, masked, attrs)
        if not class_file(text):
            yield from comment_findings(rel, text, masked, comments)


def name_findings(rel, path, text, masked, attrs):
    stem = os.path.basename(path)[:-3]
    if stem not in EXTERNAL_NAMES and decl_terms(stem) > TERM_MAX:
        yield ("file", rel, 0, f"filename '{stem}' has {decl_terms(stem)} terms")
    for match in DECL_RE.finditer(masked):
        kind, name = match.group(1), match.group(2)
        if name in EXTERNAL_NAMES:
            continue
        terms = decl_terms(name)
        if terms <= TERM_MAX:
            continue
        if kind == "fn":
            attr = attr_before(attrs, match.start(), masked)
            category = "testfn" if attr and TEST_ATTR_RE.search(attr["body"]) else "fn"
        elif kind == "mod":
            category = "mod"
        else:
            category = "type"
        line = 1 + text.count("\n", 0, match.start())
        yield (category, rel, line, f"{kind} '{name}' has {terms} terms")


def clean_comment(body):
    parts = [MARK_RE.sub("", line) for line in body.splitlines()]
    return re.sub(r"[^a-z0-9]", "", " ".join(parts).lower())


def comment_findings(rel, text, masked, comments):
    if not comments:
        return
    line_starts = [0] + [i + 1 for i, ch in enumerate(text) if ch == "\n"]
    groups = []
    for comment in comments:
        comment["start_line"] = bisect.bisect_right(line_starts, comment["start"])
        comment["end_line"] = bisect.bisect_right(line_starts, max(comment["start"], comment["end"] - 1))
        last = groups[-1] if groups else None
        if last and last["kind"] == "line" and comment["kind"] == "line" and comment["start_line"] == last["end_line"] + 1:
            last["end_line"] = comment["end_line"]
            last["body"] += "\n" + comment["body"]
        else:
            groups.append(dict(comment))
    decls = {}
    for match in DECL_RE.finditer(masked):
        line = 1 + text.count("\n", 0, match.start())
        decls.setdefault(line, match.group(2))
    lines = masked.split("\n")
    for group in groups:
        if any(mark in group["body"].lower() for mark in LICENSE_MARKS):
            continue
        span = group["end_line"] - group["start_line"] + 1
        if span > COMMENT_MAX:
            yield ("comment", rel, group["start_line"], f"comment spans {span} lines")
        row = group["end_line"]
        while row < len(lines) and not lines[row].strip():
            row += 1
        name = decls.get(row + 1)
        if name and clean_comment(group["body"]) == re.sub(r"[^a-z0-9]", "", name.lower()):
            yield ("restates", rel, group["start_line"], f"comment repeats '{name}'")


def report(findings, limit):
    order = ["folder", "fn", "testfn", "type", "mod", "file", "comment", "restates"]
    labels = {
        "folder": "folder",
        "fn": "fn",
        "testfn": "test fn",
        "type": "type",
        "mod": "mod",
        "file": "file",
        "comment": "comment",
        "restates": "warning",
    }
    counts = {key: 0 for key in order}
    shown = {key: 0 for key in order}
    for category, path, line, message in findings:
        counts[category] += 1
        if limit and shown[category] >= limit:
            continue
        shown[category] += 1
        where = path if not line else f"{path}:{line}"
        print(f"{labels[category]}: {where}: {message}")
    for category in order:
        if limit and counts[category] > shown[category]:
            print(f"{labels[category]}: ... {counts[category] - shown[category]} more")
    print("-- summary --")
    for category in order:
        print(f"{labels[category]}: {counts[category]}")
    errors = sum(counts[key] for key in order if key != "restates")
    print(f"errors: {errors}, warnings: {counts['restates']}")
    return 1 if errors else 0


def main(argv):
    parser = argparse.ArgumentParser(description="Aruna development style checks")
    parser.add_argument("--root", default=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    parser.add_argument("--limit", type=int, default=0, help="max entries per category (0 = all)")
    args = parser.parse_args(argv)
    root = os.path.abspath(args.root)
    findings = []
    findings.extend(("folder", path, 0, f"{count} of {FOLDER_MIN} files besides mod.rs") for path, count in check_folders(root))
    findings.extend(check_sources(root))
    return report(findings, args.limit)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
