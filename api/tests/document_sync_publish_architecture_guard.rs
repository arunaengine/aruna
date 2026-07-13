use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const SCAN_DIRS: &[&str] = &[
    "api/src",
    "aruna/src",
    "aruna-doctor/src",
    "blob/src",
    "core/src",
    "net/src",
    "operations/src",
    "storage/src",
    "tasks/src",
];

const VARIANT_PATTERNS: &[&str] = &[
    "DocumentSyncPublish::Upsert",
    "DocumentSyncPublish::Delete",
    "DocumentSyncPublish::AdminOperation",
];

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PublishUse {
    path: String,
    line: usize,
    function: Option<String>,
    text: String,
}

#[test]
fn document_sync_publish_constructors_stay_outbox_owned() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("api crate has workspace parent");
    let actual = scan_sources(workspace_root);
    let unexpected = actual
        .iter()
        .filter(|publish_use| !allowed_publish_use(publish_use))
        .collect::<Vec<_>>();

    if !unexpected.is_empty() {
        panic!(
            "DocumentSyncPublish construction drifted. Production publish records must be built by operations from durable outbox records; net may only convert those records into transport events.\n\nUnexpected uses:\n{}",
            format_matches(&unexpected),
        );
    }
}

fn allowed_publish_use(publish_use: &PublishUse) -> bool {
    match publish_use.path.as_str() {
        "operations/src/task_incoming.rs" => {
            publish_use.function.as_deref() == Some("document_publish_from_outbox")
        }
        "net/src/irokle.rs" => {
            matches!(
                (publish_use.function.as_deref(), publish_use.text.as_str()),
                (
                    Some("publish_events_blocking"),
                    "DocumentSyncPublish::Upsert {"
                        | "DocumentSyncPublish::Delete {"
                        | "DocumentSyncPublish::AdminOperation { target, event, .. } => {"
                        | "DocumentSyncPublish::AdminOperation {",
                ) | (
                    Some("decode_eviction"),
                    "documents.push(DocumentSyncPublish::Upsert {"
                        | "documents.push(DocumentSyncPublish::Delete {"
                        | "documents.push(DocumentSyncPublish::AdminOperation {",
                )
            )
        }
        "operations/src/incoming.rs" => {
            publish_use.function.as_deref() == Some("reemit_evicted_documents")
                && matches!(
                    publish_use.text.as_str(),
                    "DocumentSyncPublish::Upsert {"
                        | "DocumentSyncPublish::Delete { target, change, .. } => {"
                        | "DocumentSyncPublish::AdminOperation { target, event, .. } => {"
                        | "DocumentSyncPublish::Delete {"
                        | "DocumentSyncPublish::AdminOperation {"
                )
        }
        _ => false,
    }
}

fn scan_sources(workspace_root: &Path) -> BTreeSet<PublishUse> {
    let mut files = Vec::new();

    for dir in SCAN_DIRS {
        collect_rs_files(&workspace_root.join(dir), &mut files);
    }

    files.sort();

    files
        .into_iter()
        .flat_map(|path| scan_file(workspace_root, &path))
        .collect()
}

fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
    if !dir.exists() {
        return;
    }

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

fn scan_file(workspace_root: &Path, path: &Path) -> Vec<PublishUse> {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("failed to read source file {path:?}: {err}"));
    let relative_path = path
        .strip_prefix(workspace_root)
        .unwrap_or_else(|err| panic!("failed to make {path:?} relative: {err}"))
        .to_string_lossy()
        .replace('\\', "/");

    let mut matches = Vec::new();
    let mut pending_cfg_test = false;
    let mut test_module_depth = None;
    let mut pending_function = None;
    let mut current_function = None;
    let mut function_depth = None;
    let mut brace_depth = 0usize;

    for (index, line) in contents.lines().enumerate() {
        let trimmed = line.trim();
        let previous_depth = brace_depth;
        let starts_cfg_test = trimmed == "#[cfg(test)]";
        let starts_test_module = pending_cfg_test && is_module_declaration(trimmed);
        let in_test_module = test_module_depth.is_some() || starts_test_module;

        if let Some(name) = function_name(trimmed) {
            if line.contains('{') {
                current_function = Some(name);
                function_depth = Some(previous_depth);
            } else {
                pending_function = Some(name);
            }
        } else if pending_function.is_some() && line.contains('{') {
            current_function = pending_function.take();
            function_depth = Some(previous_depth);
        }

        if !in_test_module
            && !trimmed.starts_with("//")
            && VARIANT_PATTERNS
                .iter()
                .any(|pattern| line.contains(pattern))
        {
            matches.push(PublishUse {
                path: relative_path.clone(),
                line: index + 1,
                function: current_function.clone(),
                text: trimmed.to_owned(),
            });
        }

        brace_depth = brace_depth
            .saturating_add(line.matches('{').count())
            .saturating_sub(line.matches('}').count());

        if starts_test_module {
            test_module_depth = Some(previous_depth);
        }
        if test_module_depth.is_some_and(|depth| brace_depth <= depth) {
            test_module_depth = None;
        }
        if function_depth.is_some_and(|depth| brace_depth <= depth) {
            current_function = None;
            function_depth = None;
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

fn function_name(line: &str) -> Option<String> {
    let fn_index = line.find("fn ")?;
    let mut prefix = line[..fn_index].trim();
    while !prefix.is_empty() {
        if let Some(rest) = prefix.strip_prefix("pub(") {
            let close = rest.find(')')?;
            prefix = rest[close + 1..].trim_start();
            continue;
        }
        let (token, rest) = prefix
            .split_once(char::is_whitespace)
            .unwrap_or((prefix, ""));
        if !matches!(token, "pub" | "async" | "const" | "unsafe") {
            return None;
        }
        prefix = rest.trim_start();
    }

    let name = line[fn_index + 3..]
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .next()
        .unwrap_or_default();
    (!name.is_empty()).then(|| name.to_owned())
}

fn is_module_declaration(line: &str) -> bool {
    (line.starts_with("mod test") || line.starts_with("mod tests")) && line.contains('{')
}

fn format_matches(matches: &[&PublishUse]) -> String {
    if matches.is_empty() {
        return "    none\n".to_owned();
    }

    matches
        .iter()
        .map(|publish_use| {
            format!(
                "    {}:{} in {}: {}\n",
                publish_use.path,
                publish_use.line,
                publish_use.function.as_deref().unwrap_or("<module>"),
                publish_use.text,
            )
        })
        .collect()
}
