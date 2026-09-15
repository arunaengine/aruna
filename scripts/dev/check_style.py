#!/usr/bin/env python3
"""Style checks: folders >=4 files, declaration names and .rs stems <=3 terms, comments <=3 lines.
Terms split on _ and CamelCase; HTTP, UUID, S3, SHA256, OIDC and Ro-Crate count as one.
Skips structural roots, fixtures assets, license notices, strings and attributes; EXTERNAL_NAMES
maps each exempt name to the reason it is exempt."""
import argparse
import bisect
import os
import re
import sys

SKIP_PARTS = frozenset(
    {"target", "vendor", "node_modules", ".git", ".github", ".cargo", ".config", ".claude"}
)
ASSET_PARTS = frozenset({"fixtures", "snapshots", "testdata"})
LICENSE_MARKS = ("spdx-license-identifier", "copyright", "licensed under", "license")
STRUCT_NAMES = frozenset({"bin", "benches", "examples"})
EXTERNAL_NAMES = {}
FOLDER_MIN = 4
TERM_MAX = 3
COMMENT_MAX = 3

WORD_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")
CHAR_RE = re.compile(r"'(?:\\.|\\u\{[0-9A-Fa-f_]+\}|[^'\\])'")
SCAN_RE = re.compile(r"//|/\*|\"|'|[A-Za-z_]|[()[\]{}]")
IDENT_RE = re.compile(r"[A-Za-z_]\w*")
BLANK_RE = re.compile(r"[^\n]")
TEST_ATTR_RE = re.compile(r"\b(?:test|rstest)\b")
MARK_RE = re.compile(r"^\s*(///|//!|/\*+|//|\*+/|\*)\s?")
PREFIXES = ("b", "c", "r", "br", "rb", "cr")
DECL_KEYWORDS = frozenset({"fn", "struct", "enum", "trait", "type", "mod", "const", "static", "union"})
TYPE_KEYWORDS = frozenset({"struct", "enum", "trait", "type", "union"})
RESTATE_KEYWORDS = frozenset({"fn", "struct", "enum", "trait", "type", "union", "mod"})
PAIR_OPEN = {"(": ")", "[": "]", "{": "}"}


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


def macro_body_start(text, start):
    i, n = start, len(text)
    while i < n and text[i] in " \t\r\n":
        i += 1
    if text[i : i + 1] != "!":
        return None
    i += 1
    while i < n and text[i] in " \t\r\n":
        i += 1
    named = IDENT_RE.match(text, i)
    if not named:
        return None
    i = named.end()
    while i < n and text[i] in " \t\r\n":
        i += 1
    return i if text[i : i + 1] in "([{" else None


def mask_source(text):
    """Return (masked, comments, attrs, tokens); strings, chars, comments and
    attributes are blanked, macro_rules definitions are skipped entirely, other
    macro token trees keep their tokens so handwritten declarations are seen."""
    n = len(text)
    masked = list(text)
    comments, attrs, tokens = [], [], []

    def blank(start, end):
        masked[start:end] = BLANK_RE.sub(" ", text[start:end])

    i = 0
    while i < n:
        ch = text[i]
        if ch in " \t\r\n":
            i += 1
        elif text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end < 0 else end
            record_comment(text, i, end, comments)
            blank(i, end)
            i = end
        elif text.startswith("/*", i):
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
            if match:
                end = match.end()
            else:
                end = i + 1
                while end < n and (text[end].isalnum() or text[end] == "_"):
                    end += 1
            blank(i, end)
            i = end
        elif ch == "#" and (
            text[i + 1 : i + 2] == "[" or (text[i + 1 : i + 2] == "!" and text[i + 2 : i + 3] == "[")
        ):
            start = i + 1 if text[i + 1 : i + 2] == "[" else i + 2
            end = balanced_end(text, start, "[", "]", comments)
            attrs.append({"start": i, "end": end, "body": text[i:end]})
            blank(i, end)
            i = end
        elif ch.isalpha() or ch == "_":
            j = IDENT_RE.match(text, i).end()
            if string_prefix(text, i, j):
                end = string_end(text, i)
                if end is not None:
                    blank(i, end)
                    i = end
                    continue
            if text[i:j] == "macro_rules":
                d = macro_body_start(text, j)
                if d is not None:
                    end = balanced_end(text, d, text[d], PAIR_OPEN[text[d]], comments)
                    blank(i, end)
                    i = end
                    continue
            tokens.append(("ident", text[i:j], i, j))
            i = j
        else:
            tokens.append(("punct", ch, i, i + 1))
            i += 1
    return "".join(masked), comments, attrs, tokens


def matching_punct(tokens, open_index):
    depth = 0
    for index in range(open_index, len(tokens)):
        if tokens[index][0] == "punct":
            if tokens[index][1] in PAIR_OPEN:
                depth += 1
            elif tokens[index][1] in ")]}":
                depth -= 1
                if depth == 0:
                    return index
    return len(tokens) - 1


def member_fields(tokens, open_index):
    close = matching_punct(tokens, open_index)
    depth, members = 0, []
    for index in range(open_index + 1, close):
        kind, word, start, _end = tokens[index]
        if kind == "punct":
            if word in PAIR_OPEN:
                depth += 1
            elif word in ")]}":
                depth -= 1
            continue
        if depth != 0 or index + 1 >= len(tokens):
            continue
        nxt = tokens[index + 1]
        prev = tokens[index - 1]
        if nxt[0] != "punct" or nxt[1] != ":" or prev[1] == ":":
            continue
        if index + 2 < len(tokens) and tokens[index + 2][0] == "punct" and tokens[index + 2][1] == ":":
            continue
        members.append((word, start))
    return members


def member_variants(tokens, open_index):
    close = matching_punct(tokens, open_index)
    depth, expect, members = 0, True, []
    index = open_index + 1
    while index < close:
        kind, word, start, _end = tokens[index]
        if kind == "punct":
            if word in PAIR_OPEN:
                if word == "{" and depth == 0 and not expect:
                    members.extend(("field", name, offset) for name, offset in member_fields(tokens, index))
                    index = matching_punct(tokens, index) + 1
                    continue
                depth += 1
            elif word in ")]}":
                depth -= 1
            elif word == "," and depth == 0:
                expect = True
            index += 1
            continue
        if depth == 0 and expect:
            members.append(("variant", word, start))
            expect = False
        index += 1
    return members


def find_body(tokens, start):
    depth = 0
    for index in range(start, len(tokens)):
        kind, word, _start, _end = tokens[index]
        if kind != "punct":
            continue
        if word in "([":
            depth += 1
        elif word in ")]":
            depth = max(0, depth - 1)
        elif word == "<":
            depth += 1
        elif word == ">" and depth > 0 and tokens[index - 1][1] != "-":
            depth -= 1
        elif depth == 0 and word in "{;":
            return index if word == "{" else None
    return None


def iter_declarations(tokens, masked, attrs):
    total = len(tokens)
    for index in range(total):
        kind, word, start, _end = tokens[index]
        if kind != "ident" or word not in DECL_KEYWORDS:
            continue
        nxt = tokens[index + 1] if index + 1 < total else None
        if word == "const" and nxt and nxt[0] == "ident" and nxt[1] == "fn":
            continue
        if word == "static":
            offset = index + 1
            while offset < total and tokens[offset][0] == "ident" and tokens[offset][1] in ("mut", "ref"):
                offset += 1
            nxt = tokens[offset] if offset < total else None
        if not nxt or nxt[0] != "ident":
            continue
        name, name_start = nxt[1], nxt[2]
        if word == "fn":
            chain = attrs_before(attrs, start, masked)
            category = "testfn" if any(TEST_ATTR_RE.search(attr["body"]) for attr in chain) else "fn"
        elif word in TYPE_KEYWORDS:
            category = "type"
            body = find_body(tokens, index + 2)
            if body is not None and tokens[body][1] == "{" and word in ("struct", "enum"):
                if word == "struct":
                    for member, offset in member_fields(tokens, body):
                        yield ("field", member, "field", offset)
                else:
                    for member_kind, member, offset in member_variants(tokens, body):
                        yield (member_kind, member, member_kind, offset)
        elif word == "mod":
            category = "mod"
        else:
            category = "const"
        yield (category, name, word, name_start)


def read_source(path):
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


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
            real = [f for f in files if not f.startswith(".") and f != "mod.rs"]
            if not real and not any(f.endswith(".rs") for f in files):
                continue
            if len(real) < FOLDER_MIN:
                yield (os.path.relpath(dirpath, root), len(real))


def decl_terms(name):
    return len(split_terms(name))


def attrs_before(attrs, offset, masked):
    chain = []
    for attr in reversed(attrs):
        if attr["end"] > offset:
            continue
        gap = offset if not chain else chain[-1]["start"]
        if masked[attr["end"] : gap].strip():
            break
        chain.append(attr)
    return chain


def check_sources(root):
    for path in iter_rust_files(root):
        rel = os.path.relpath(path, root)
        text = read_source(path)
        masked, comments, attrs, tokens = mask_source(text)
        yield from name_findings(rel, path, text, masked, attrs, tokens)
        yield from comment_findings(rel, text, masked, comments, tokens)


def name_findings(rel, path, text, masked, attrs, tokens):
    stem = os.path.basename(path)[:-3]
    if stem not in EXTERNAL_NAMES and decl_terms(stem) > TERM_MAX:
        yield ("file", rel, 0, f"filename '{stem}' has {decl_terms(stem)} terms")
    for category, name, keyword, offset in iter_declarations(tokens, masked, attrs):
        if name in EXTERNAL_NAMES:
            continue
        terms = decl_terms(name)
        if terms <= TERM_MAX:
            continue
        line = 1 + text.count("\n", 0, offset)
        yield (category, rel, line, f"{keyword} '{name}' has {terms} terms")


def clean_comment(body):
    parts = [MARK_RE.sub("", line) for line in body.splitlines()]
    return re.sub(r"[^a-z0-9]", "", " ".join(parts).lower())


def comment_findings(rel, text, masked, comments, tokens):
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
    for index, (kind, word, start, _end) in enumerate(tokens):
        if kind != "ident" or word not in RESTATE_KEYWORDS:
            continue
        nxt = tokens[index + 1] if index + 1 < len(tokens) else None
        if nxt and nxt[0] == "ident":
            decls.setdefault(1 + text.count("\n", 0, start), nxt[1])
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
    order = ["folder", "fn", "testfn", "type", "mod", "const", "field", "variant", "file", "comment", "restates"]
    labels = {
        "folder": "folder",
        "fn": "fn",
        "testfn": "test fn",
        "type": "type",
        "mod": "mod",
        "const": "const",
        "field": "field",
        "variant": "variant",
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


def external_without_reason():
    return [name for name, reason in EXTERNAL_NAMES.items() if not isinstance(reason, str) or not reason.strip()]


def main(argv):
    parser = argparse.ArgumentParser(description="Aruna development style checks")
    parser.add_argument("--root", default=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    parser.add_argument("--limit", type=int, default=0, help="max entries per category (0 = all)")
    args = parser.parse_args(argv)
    root = os.path.abspath(args.root)
    missing = external_without_reason()
    if missing:
        print(f"EXTERNAL_NAMES needs a reason for: {', '.join(sorted(missing))}", file=sys.stderr)
        return 2
    findings = []
    findings.extend(("folder", path, 0, f"{count} of {FOLDER_MIN} files besides mod.rs") for path, count in check_folders(root))
    findings.extend(check_sources(root))
    return report(findings, args.limit)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
