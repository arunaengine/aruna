use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const BOUNDARIES: &[&str] = &[
    "authorize_tool",
    "enforce_policies",
    "ensure_permission",
    "request_authorization::authorize",
    "submit_execution",
];
const TOOL_COUNT: usize = 22;

#[test]
fn tools_reach_authorize() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/mcp");
    let mut paths = Vec::new();
    collect_sources(&root, &mut paths);
    let mut bodies: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut tools = BTreeSet::new();
    for path in paths {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {path:?}: {error}"));
        let source = strip_tests(&source);
        for (name, body) in fn_bodies(source) {
            bodies.entry(name).or_default().push(body.to_string());
        }
        tools.extend(tool_names(source));
    }
    assert_eq!(tools.len(), TOOL_COUNT, "MCP tool inventory changed");
    let unguarded = tools
        .iter()
        .filter(|tool| !is_guarded(tool, &bodies, &mut BTreeSet::new()))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        unguarded.is_empty(),
        "MCP tools must reach authorize_tool or the authorization boundary: {unguarded:?}"
    );
}

fn collect_sources(dir: &Path, paths: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|error| panic!("read {dir:?}: {error}")) {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry in {dir:?}: {error}"))
            .path();
        if path.is_dir() {
            collect_sources(&path, paths);
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            paths.push(path);
        }
    }
}

fn tool_names(source: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut rest = source;
    while let Some(attribute) = rest.find("#[tool(") {
        rest = &rest[attribute + 7..];
        let Some(function) = rest.find("fn ") else {
            break;
        };
        let name = rest[function + 3..]
            .chars()
            .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
            .collect::<String>();
        rest = &rest[function + 3 + name.len()..];
        if !name.is_empty() {
            names.insert(name);
        }
    }
    names
}

fn fn_bodies(source: &str) -> Vec<(String, &str)> {
    let mut functions = Vec::new();
    let mut offset = 0;
    while let Some(found) = source[offset..].find("fn ") {
        let start = offset + found;
        let name_start = start + 3;
        let name = source[name_start..]
            .chars()
            .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
            .collect::<String>();
        if name.is_empty() {
            offset = name_start;
            continue;
        }
        let Some(open) = source[name_start + name.len()..].find('{') else {
            break;
        };
        let open = name_start + name.len() + open;
        let Some(close) = block_end(source, open) else {
            break;
        };
        functions.push((name, &source[open + 1..close]));
        offset = close + 1;
    }
    functions
}

fn block_end(source: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, byte) in source.as_bytes()[open..].iter().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(open + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn is_guarded(
    name: &str,
    bodies: &BTreeMap<String, Vec<String>>,
    seen: &mut BTreeSet<String>,
) -> bool {
    if !seen.insert(name.to_string()) {
        return false;
    }
    bodies.get(name).is_some_and(|candidates| {
        candidates.iter().any(|body| {
            BOUNDARIES.iter().any(|boundary| body.contains(boundary))
                || called_names(body)
                    .iter()
                    .any(|called| is_guarded(called, bodies, seen))
        })
    })
}

fn called_names(body: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let bytes = body.as_bytes();
    let mut start = None;
    for (index, byte) in bytes.iter().enumerate() {
        if byte.is_ascii_alphanumeric() || *byte == b'_' {
            start.get_or_insert(index);
            continue;
        }
        if let Some(from) = start.take()
            && *byte == b'('
            && (from == 0 || bytes[from - 1] != b'.')
        {
            names.insert(body[from..index].to_string());
        }
    }
    names
}

fn strip_tests(source: &str) -> &str {
    source.split("#[cfg(test)]").next().unwrap_or(source)
}
