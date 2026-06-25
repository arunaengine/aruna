use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const SCAN_DIRS: &[&str] = &["src/routes", "src/s3"];
const PATTERNS: &[&str] = &[
    "send_effect",
    "send_storage_effect",
    "StorageEffect::",
    "MetadataEffect::",
    "TaskEffect::",
    "NetEffect::",
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
fn api_routes_and_s3_direct_side_effects_stay_allowlisted() {
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
    let mut files = Vec::new();

    for dir in SCAN_DIRS {
        collect_rs_files(&manifest_dir.join(dir), &mut files);
    }

    files.sort();

    files
        .into_iter()
        .flat_map(|path| scan_file(manifest_dir, &path))
        .collect()
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|err| panic!("failed to read {dir:?}: {err}")) {
        let path = entry
            .unwrap_or_else(|err| panic!("failed to read entry in {dir:?}: {err}"))
            .path();

        if path.is_dir() {
            collect_rs_files(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn scan_file(manifest_dir: &Path, path: &Path) -> Vec<GuardMatch> {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("failed to read source file {path:?}: {err}"));
    let relative_path = path
        .strip_prefix(manifest_dir)
        .unwrap_or_else(|err| panic!("failed to make {path:?} relative: {err}"))
        .to_string_lossy()
        .replace('\\', "/");

    let mut matches = Vec::new();
    let mut pending_cfg_test = false;
    let mut test_module_depth = None;
    let mut brace_depth = 0usize;

    for (index, line) in contents.lines().enumerate() {
        let trimmed = line.trim();
        let starts_cfg_test = trimmed == "#[cfg(test)]";
        let starts_test_module = pending_cfg_test && is_module_declaration(trimmed);
        let in_test_module = test_module_depth.is_some() || starts_test_module;

        if !in_test_module {
            let patterns = PATTERNS
                .iter()
                .copied()
                .filter(|pattern| line.contains(pattern))
                .collect::<Vec<_>>();

            if !patterns.is_empty() {
                matches.push(GuardMatch {
                    path: relative_path.clone(),
                    line: index + 1,
                    patterns: patterns.join("|"),
                    text: line.trim().to_owned(),
                });
            }
        }

        let previous_depth = brace_depth;
        brace_depth = brace_depth
            .saturating_add(line.matches('{').count())
            .saturating_sub(line.matches('}').count());

        if starts_test_module {
            test_module_depth = Some(previous_depth);
        }
        if test_module_depth.is_some_and(|depth| brace_depth <= depth) {
            test_module_depth = None;
        }

        if starts_cfg_test {
            pending_cfg_test = true;
        } else if pending_cfg_test
            && !trimmed.is_empty()
            && !trimmed.starts_with("#[")
            && !trimmed.starts_with("//")
        {
            pending_cfg_test = false;
        }
    }

    matches
}

fn is_module_declaration(line: &str) -> bool {
    (line.starts_with("mod test") || line.starts_with("mod tests")) && line.contains('{')
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
