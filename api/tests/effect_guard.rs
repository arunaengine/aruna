use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[path = "support/syntax.rs"]
mod syntax;
use syntax::{read_ident, skip_space};

const SCAN_DIRS: &[&str] = &["src/mcp", "src/routes", "src/s3"];
const PATTERNS: &[&str] = &[
    "send_effect",
    "send_storage_effect",
    "send_metadata_effect",
    "Effect::Storage",
    "Effect::Metadata",
    "Effect::Net",
    "Effect::Task",
    "StorageEffect::",
    "MetadataEffect::",
    "TaskEffect::",
    "NetEffect::",
    "StorageEvent::",
    "MetadataEvent::",
    "use aruna_core::effects",
    "use aruna_core::events",
    "use aruna_core::handle::Handle",
    ".storage_handle",
    ".metadata_handle",
    ".task_handle",
    ".net_handle",
    "visible_registry::",
    "project_logged_events",
    "materialize_snapshot",
    "materialize_reference",
    "list_cached_group",
    "record_materialized_read",
    "export_rocrate_jsonld",
    "export_summary_jsonld",
    "export_rocrate_page",
    "run_metadata_fanout",
    "run_query_distributed",
    "run_search_distributed",
    "query_authorized_local",
    "search_authorized_local",
    "query_remote_graphs",
    "search_remote_graphs",
    "tokio::spawn",
];

const ALLOWLIST: &[(&str, usize, &str, &str)] = &[];

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GuardMatch {
    path: String,
    line: usize,
    patterns: String,
    text: String,
}

#[test]
fn effects_stay_allowlisted() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let actual = scan_sources(manifest_dir);
    let allowed = allowlist();

    let unexpected = actual.difference(&allowed).collect::<Vec<_>>();
    let stale = allowed.difference(&actual).collect::<Vec<_>>();

    if !unexpected.is_empty() || !stale.is_empty() {
        panic!(
            "API direct side-effect orchestration allowlist drifted. \
             Refactor production matches out of api routes/S3 instead of extending the allowlist.\n\n\
             Unexpected matches:\n{}\nStale allowlist entries:\n{}",
            format_matches(&unexpected),
            format_matches(&stale),
        );
    }
}

fn allowlist() -> BTreeSet<GuardMatch> {
    ALLOWLIST
        .iter()
        .map(|(path, line, patterns, text)| GuardMatch {
            path: (*path).to_owned(),
            line: *line,
            patterns: (*patterns).to_owned(),
            text: (*text).to_owned(),
        })
        .collect()
}

fn scan_sources(manifest_dir: &Path) -> BTreeSet<GuardMatch> {
    scan_tree(&read_sources(manifest_dir))
}

fn scan_tree(files: &BTreeMap<String, String>) -> BTreeSet<GuardMatch> {
    let regions = test_regions(files);

    files
        .iter()
        .filter(|(path, _)| !regions.test_only.contains(*path))
        .flat_map(|(path, source)| {
            let spans = regions
                .spans
                .get(path)
                .map_or(&[][..], |spans| spans.as_slice());
            scan_file(path, source, spans)
        })
        .collect()
}

fn read_sources(manifest_dir: &Path) -> BTreeMap<String, String> {
    let mut files = BTreeMap::new();

    for dir in SCAN_DIRS {
        collect_rs_files(&manifest_dir.join(dir), manifest_dir, &mut files);
    }

    files
}

fn collect_rs_files(dir: &Path, manifest_dir: &Path, files: &mut BTreeMap<String, String>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|err| panic!("failed to read {dir:?}: {err}")) {
        let path = entry
            .unwrap_or_else(|err| panic!("failed to read entry in {dir:?}: {err}"))
            .path();

        if path.is_dir() {
            collect_rs_files(&path, manifest_dir, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            let relative = path
                .strip_prefix(manifest_dir)
                .unwrap_or_else(|err| panic!("failed to make {path:?} relative: {err}"))
                .to_string_lossy()
                .replace('\\', "/");
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("failed to read source file {path:?}: {err}"));
            files.insert(relative, source);
        }
    }
}

/// Test-only files and byte spans, resolved from parsed items and `mod`
/// declarations. `cfg_attr`, `include!` and unreachable files fail loudly
/// because this scanner does not model macro-expanded or path-mapped modules.
#[derive(Default)]
struct TestRegions {
    test_only: BTreeSet<String>,
    spans: BTreeMap<String, Vec<(usize, usize)>>,
    visited: BTreeSet<String>,
}

impl TestRegions {
    fn visit(&mut self, files: &BTreeMap<String, String>, relative: &str, test_only: bool) {
        let source = files
            .get(relative)
            .unwrap_or_else(|| panic!("module {relative} has no source file"));
        assert!(
            self.visited.insert(relative.to_owned()),
            "module {relative} is declared twice"
        );

        let masked = syntax::mask(source);
        let test_only = test_only || inner_cfg_test(&masked);
        let mut parsed = Parsed::default();
        parse_items(&masked, 0, &module_dir(relative), test_only, &mut parsed);

        if test_only {
            self.test_only.insert(relative.to_owned());
        } else if !parsed.test_spans.is_empty() {
            self.spans.insert(relative.to_owned(), parsed.test_spans);
        }

        for child in parsed.children {
            let path = module_path(files, &child.dir, &child.name);
            self.visit(files, &path, child.test_only);
        }
    }
}

fn test_regions(files: &BTreeMap<String, String>) -> TestRegions {
    let mut regions = TestRegions::default();

    for dir in SCAN_DIRS {
        let root = format!("{dir}/mod.rs");
        assert!(files.contains_key(&root), "scan root {root} is missing");
        regions.visit(files, &root, false);
    }

    for path in files.keys() {
        assert!(
            regions.visited.contains(path),
            "source file {path} is not reachable through a module declaration; \
             extend the guard instead of leaving it unscanned"
        );
    }

    regions
}

#[derive(Default)]
struct Parsed {
    test_spans: Vec<(usize, usize)>,
    children: Vec<ChildModule>,
}

struct ChildModule {
    dir: String,
    name: String,
    test_only: bool,
}

/// Parses items in `src` (which starts at `base` in the masked file) far
/// enough to collect `#[cfg(test)]` spans and `mod` declarations. Bodies of
/// `fn` items are left to the line scan, so statements are never parsed.
fn parse_items(src: &str, base: usize, dir: &str, test_only: bool, parsed: &mut Parsed) {
    let mut pos = 0usize;

    while pos < src.len() {
        let byte = src.as_bytes()[pos];
        if byte.is_ascii_whitespace() || byte == b';' {
            pos += 1;
            continue;
        }

        let item_start = pos;
        let mut test_attr = false;
        loop {
            pos = skip_space(src, pos);
            let inner = src[pos..].starts_with("#![");
            if inner || src[pos..].starts_with("#[") {
                let bracket = if inner { pos + 2 } else { pos + 1 };
                let end = syntax::delimiter(src, bracket, b'[', b']');
                let attribute = &src[pos..=end];
                if syntax::cfg_attr(attribute) {
                    panic!("unsupported cfg_attr attribute: {attribute}");
                }
                if syntax::test_cfg(attribute) {
                    test_attr = true;
                }
                pos = end + 1;
                continue;
            }
            break;
        }

        pos = skip_space(src, pos);
        if pos >= src.len() {
            break;
        }

        let item_test_only = test_only || test_attr;
        let (head, after) = item_head(src, pos);
        pos = after;

        let bang = skip_space(src, pos);
        if src.as_bytes().get(bang) == Some(&b'!') {
            pos = macro_end(src, &head, bang);
            note_span(parsed, test_only, item_test_only, base, item_start, pos);
            continue;
        }

        match head.as_str() {
            "mod" => {
                let name_pos = skip_space(src, pos);
                let (name, after_name) = read_ident(src, name_pos);
                assert!(
                    is_plain_ident(&name),
                    "module declaration near byte {} is not a plain identifier",
                    base + item_start
                );
                pos = skip_space(src, after_name);
                match src.as_bytes().get(pos) {
                    Some(b';') => {
                        parsed.children.push(ChildModule {
                            dir: dir.to_owned(),
                            name,
                            test_only: item_test_only,
                        });
                        pos += 1;
                    }
                    Some(b'{') => {
                        let close = syntax::delimiter(src, pos, b'{', b'}');
                        note_span(
                            parsed,
                            test_only,
                            item_test_only,
                            base,
                            item_start,
                            close + 1,
                        );
                        let child_dir = join_dir(dir, &name);
                        parse_items(
                            &src[pos + 1..close],
                            base + pos + 1,
                            &child_dir,
                            item_test_only,
                            parsed,
                        );
                        pos = close + 1;
                    }
                    _ => panic!(
                        "module declaration near byte {} has no body or semicolon",
                        base + item_start
                    ),
                }
            }
            "use" | "type" | "static" | "const" | "extern" => {
                pos = statement_end(src, pos);
                note_span(parsed, test_only, item_test_only, base, item_start, pos);
            }
            "fn" | "struct" | "enum" | "union" => {
                pos = match body_range(src, pos) {
                    Some((_, close)) => close + 1,
                    None => statement_end(src, pos),
                };
                note_span(parsed, test_only, item_test_only, base, item_start, pos);
            }
            "impl" | "trait" => match body_range(src, pos) {
                Some((open, close)) => {
                    if item_test_only {
                        note_span(parsed, test_only, true, base, item_start, close + 1);
                    } else {
                        parse_items(&src[open + 1..close], base + open + 1, dir, false, parsed);
                    }
                    pos = close + 1;
                }
                None => {
                    pos = statement_end(src, pos);
                    note_span(parsed, test_only, item_test_only, base, item_start, pos);
                }
            },
            "" if src.as_bytes().get(pos) == Some(&b'{') => {
                let close = syntax::delimiter(src, pos, b'{', b'}');
                pos = close + 1;
            }
            _ => panic!(
                "unsupported item near byte {} in guarded source: {head}",
                base + item_start
            ),
        }
    }
}

fn note_span(
    parsed: &mut Parsed,
    test_only: bool,
    item_test_only: bool,
    base: usize,
    start: usize,
    end: usize,
) {
    if item_test_only && !test_only {
        parsed.test_spans.push((base + start, base + end));
    }
}

fn macro_end(src: &str, head: &str, bang: usize) -> usize {
    if head == "include" {
        panic!("unsupported include! source inclusion near byte {bang}");
    }

    let after_bang = skip_space(src, bang + 1);
    let (name, at) = read_ident(src, after_bang);
    let at = if name.is_empty() {
        after_bang
    } else {
        skip_space(src, at)
    };

    match src.as_bytes().get(at) {
        Some(b'{') => syntax::delimiter(src, at, b'{', b'}') + 1,
        Some(b'(') => curly_skip(src, syntax::delimiter(src, at, b'(', b')') + 1),
        Some(b'[') => curly_skip(src, syntax::delimiter(src, at, b'[', b']') + 1),
        _ => panic!("macro invocation near byte {bang} is not supported"),
    }
}

fn curly_skip(src: &str, mut pos: usize) -> usize {
    pos = skip_space(src, pos);
    if src.as_bytes().get(pos) == Some(&b';') {
        pos + 1
    } else {
        pos
    }
}

/// Skips visibility and qualifier keywords to find the item keyword.
fn item_head(src: &str, mut pos: usize) -> (String, usize) {
    loop {
        pos = skip_space(src, pos);
        let (word, after) = read_ident(src, pos);

        match word.as_str() {
            "pub" => {
                let after = skip_space(src, after);
                if src.as_bytes().get(after) == Some(&b'(') {
                    pos = syntax::delimiter(src, after, b'(', b')') + 1;
                } else {
                    pos = after;
                }
            }
            "async" | "unsafe" | "default" | "auto" => pos = after,
            "const" => {
                let next = skip_space(src, after);
                let (next_word, _) = read_ident(src, next);
                if next_word == "fn" {
                    pos = after
                } else {
                    return (word, after);
                }
            }
            "extern" => pos = skip_space(src, after),
            _ => return (word, after),
        }
    }
}

/// Finds the first `{` at paren/bracket depth zero and returns it with its
/// matching close, or `None` when a `;` ends the item first.
fn body_range(src: &str, mut pos: usize) -> Option<(usize, usize)> {
    let bytes = src.as_bytes();
    let mut depth = 0usize;

    while pos < bytes.len() {
        match bytes[pos] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth = depth.saturating_sub(1),
            b'{' if depth == 0 => {
                let close = syntax::delimiter(src, pos, b'{', b'}');
                return Some((pos, close));
            }
            b';' if depth == 0 => return None,
            _ => {}
        }
        pos += 1;
    }

    panic!("item starting near byte {pos} has no body");
}

/// Ends a `;`-terminated item, tracking all delimiter nesting so `use` groups
/// and brace expressions stay inside one item.
fn statement_end(src: &str, mut pos: usize) -> usize {
    let bytes = src.as_bytes();
    let mut depth = 0usize;

    while pos < bytes.len() {
        match bytes[pos] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            b';' if depth == 0 => return pos + 1,
            _ => {}
        }
        pos += 1;
    }

    panic!("item starting near byte {pos} has no terminator");
}

fn scan_file(path: &str, source: &str, test_spans: &[(usize, usize)]) -> Vec<GuardMatch> {
    let masked = syntax::mask(source);
    let mut matches = Vec::new();
    let mut offset = 0usize;

    for (index, chunk) in masked.split_inclusive('\n').enumerate() {
        let line = chunk.strip_suffix('\n').unwrap_or(chunk);

        if line
            .match_indices("include!")
            .any(|(at, _)| !in_spans(test_spans, offset + at))
        {
            panic!(
                "unsupported include! source inclusion in {path}:{}",
                index + 1
            );
        }

        let patterns = PATTERNS
            .iter()
            .copied()
            .filter(|pattern| {
                pattern_applies(pattern, path)
                    && line
                        .match_indices(pattern)
                        .any(|(at, _)| !in_spans(test_spans, offset + at))
            })
            .collect::<Vec<_>>();

        if !patterns.is_empty() {
            let text = source
                .split_inclusive('\n')
                .nth(index)
                .unwrap_or_default()
                .trim()
                .to_owned();
            matches.push(GuardMatch {
                path: path.to_owned(),
                line: index + 1,
                patterns: patterns.join("|"),
                text,
            });
        }

        offset += chunk.len();
    }

    matches
}

fn in_spans(spans: &[(usize, usize)], offset: usize) -> bool {
    spans
        .iter()
        .any(|(start, end)| (*start..*end).contains(&offset))
}

fn pattern_applies(pattern: &str, relative_path: &str) -> bool {
    // The S3 listener's connection tasks are infrastructure glue, so the D1
    // spawn rule skips server.rs while its direct-effect tokens are scanned.
    !(pattern == "tokio::spawn" && relative_path == "src/s3/server.rs")
}

fn inner_cfg_test(source: &str) -> bool {
    let mut pos = 0usize;

    loop {
        pos = skip_space(source, pos);
        if !source[pos..].starts_with("#![") {
            return false;
        }
        let end = syntax::delimiter(source, pos + 2, b'[', b']');
        let attribute = &source[pos..=end];
        if syntax::cfg_attr(attribute) {
            panic!("unsupported cfg_attr attribute: {attribute}");
        }
        if syntax::test_cfg(attribute) {
            return true;
        }
        pos = end + 1;
    }
}

fn is_plain_ident(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn module_dir(relative: &str) -> String {
    relative
        .strip_suffix("/mod.rs")
        .or_else(|| relative.strip_suffix(".rs"))
        .unwrap_or(relative)
        .to_owned()
}

fn join_dir(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

fn module_path(files: &BTreeMap<String, String>, dir: &str, name: &str) -> String {
    let candidates = [
        join_dir(dir, &format!("{name}.rs")),
        join_dir(&join_dir(dir, name), "mod.rs"),
    ];

    for candidate in candidates {
        if files.contains_key(&candidate) {
            return candidate;
        }
    }

    panic!("module {name} in {dir:?} has no source file");
}

fn format_matches(matches: &[&GuardMatch]) -> String {
    if matches.is_empty() {
        return "    none\n".to_owned();
    }

    matches
        .iter()
        .map(|mat| {
            format!(
                "    ({:?}, {}, {:?}, {:?}),\n",
                mat.path, mat.line, mat.patterns, mat.text
            )
        })
        .collect()
}

#[cfg(test)]
mod fixtures {
    use super::*;

    fn fixture(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        let mut files = BTreeMap::new();

        for root in SCAN_DIRS {
            files.insert(format!("{root}/mod.rs"), String::new());
        }
        for (path, source) in entries {
            files.insert((*path).to_owned(), (*source).to_owned());
        }

        files
    }

    #[test]
    fn trailing_production_scanned() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "#[cfg(test)]\nmod tests {\n    fn fixture() { send_effect(); }\n}\nfn production() { send_effect(); }\n",
        )]);

        let matches = scan_tree(&files);

        assert_eq!(matches.len(), 1);
        assert_eq!(matches.iter().next().unwrap().line, 5);
    }

    #[test]
    fn raw_text_ignored() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "const MARKER: &str = r#\"#![cfg(test)]\"#;\nfn production() { send_effect(); }\n",
        )]);

        let matches = scan_tree(&files);

        assert_eq!(matches.len(), 1);
        assert_eq!(matches.iter().next().unwrap().line, 2);
    }

    #[test]
    fn child_module_scanned() {
        let files = fixture(&[
            ("src/routes/mod.rs", "mod child;\n"),
            (
                "src/routes/child.rs",
                "fn production() { send_effect(); }\n",
            ),
        ]);

        let matches = scan_tree(&files);

        assert_eq!(matches.len(), 1);
        assert_eq!(matches.iter().next().unwrap().path, "src/routes/child.rs");
    }

    #[test]
    fn directory_production_scanned() {
        let files = fixture(&[
            ("src/routes/mod.rs", "mod tests;\n"),
            (
                "src/routes/tests/mod.rs",
                "fn production() { send_effect(); }\n",
            ),
        ]);

        let matches = scan_tree(&files);

        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches.iter().next().unwrap().path,
            "src/routes/tests/mod.rs"
        );
    }

    #[test]
    fn test_module_skipped() {
        let files = fixture(&[
            ("src/routes/mod.rs", "#[cfg(test)]\nmod tests;\n"),
            ("src/routes/tests.rs", "fn fixture() { send_effect(); }\n"),
        ]);

        assert!(scan_tree(&files).is_empty());
    }

    #[test]
    fn test_item_skipped() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "fn production() {}\n#[cfg(test)]\nfn fixture() { send_effect(); }\n",
        )]);

        assert!(scan_tree(&files).is_empty());
    }

    #[test]
    fn syntax_text_ignored() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "// send_effect()\nfn production() { let _ = \"send_effect\"; }\n",
        )]);

        assert!(scan_tree(&files).is_empty());
    }

    #[test]
    fn ordinary_item_scanned() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "#[cfg(not(test))]\nfn production() { send_effect(); }\n",
        )]);

        assert_eq!(scan_tree(&files).len(), 1);
    }

    #[test]
    fn test_mode_scanned() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "#[cfg(all(test_mode, unix))]\nfn production() { send_effect(); }\n",
        )]);

        assert_eq!(scan_tree(&files).len(), 1);
    }

    #[test]
    fn inner_attribute_parsed() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "#![cfg(test)]\nfn fixture() { send_effect(); }\n",
        )]);

        assert!(scan_tree(&files).is_empty());
    }

    #[test]
    fn allow_attribute_scanned() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "#![allow(clippy::result_large_err)]\nfn production() { send_effect(); }\n",
        )]);

        assert_eq!(scan_tree(&files).len(), 1);
    }

    #[test]
    #[should_panic(expected = "cfg_attr")]
    fn cfg_attr_rejected() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "#[cfg_attr(test, allow(dead_code))]\nfn production() {}\n",
        )]);

        let _ = scan_tree(&files);
    }

    #[test]
    #[should_panic(expected = "not reachable")]
    fn unreferenced_file_fails() {
        let files = fixture(&[
            ("src/routes/mod.rs", "\n"),
            ("src/routes/orphan.rs", "fn production() {}\n"),
        ]);

        let _ = scan_tree(&files);
    }

    #[test]
    #[should_panic(expected = "include!")]
    fn include_macro_rejected() {
        let files = fixture(&[(
            "src/routes/mod.rs",
            "fn production() { include!(\"generated.rs\"); }\n",
        )]);

        let _ = scan_tree(&files);
    }
}
