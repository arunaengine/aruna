use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[path = "support/syntax.rs"]
mod syntax;
use syntax::{ident as ident_at, ident_byte as is_ident_byte, occurrences};

const LOCAL_BOUNDARIES: &[(&str, &str)] = &[
    ("src/mcp/mod.rs", "authorize_self"),
    ("src/mcp/mod.rs", "authorize_tool"),
];
const EXTERNAL_BOUNDARIES: &[&str] = &[
    "crate::routes::jobs::submit_execution",
    "crate::metadata::run_create_metadata",
];
const TOOL_COUNT: usize = 37;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ToolId {
    file: String,
    name: String,
}

#[derive(Default)]
struct Module {
    bodies: BTreeMap<String, Vec<String>>,
    imports: BTreeMap<String, Import>,
    tools: Vec<String>,
}

struct Import {
    file: String,
    name: String,
}

#[test]
fn tools_reach_authorize() {
    // Reachability supplements request permission tests; it cannot prove branch dominance.
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = read_sources(manifest_dir);
    let modules = load_modules(&files);
    let tools = tool_inventory(&modules);

    assert_eq!(
        tools.len(),
        TOOL_COUNT,
        "MCP tool inventory changed: found {} tools, expected {TOOL_COUNT}. \
         Update TOOL_COUNT and confirm every new tool reaches authorization.",
        tools.len(),
    );

    let unguarded = tools
        .iter()
        .filter(|tool| !is_guarded(&modules, &tool.file, &tool.name, &mut BTreeSet::new()))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        unguarded.is_empty(),
        "MCP tools must reach authorize_tool or the authorization boundary: {unguarded:?}"
    );
}

fn read_sources(manifest_dir: &Path) -> BTreeMap<String, String> {
    let mut files = BTreeMap::new();
    collect_sources(&manifest_dir.join("src/mcp"), manifest_dir, &mut files);
    files
}

fn collect_sources(dir: &Path, manifest_dir: &Path, files: &mut BTreeMap<String, String>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|error| panic!("read {dir:?}: {error}")) {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry in {dir:?}: {error}"))
            .path();

        if path.is_dir() {
            collect_sources(&path, manifest_dir, files);
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            let relative = path
                .strip_prefix(manifest_dir)
                .unwrap_or_else(|error| panic!("make {path:?} relative: {error}"))
                .to_string_lossy()
                .replace('\\', "/");
            let source =
                fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path:?}: {error}"));
            files.insert(relative, source);
        }
    }
}

fn load_modules(files: &BTreeMap<String, String>) -> BTreeMap<String, Module> {
    files
        .iter()
        .map(|(file, source)| {
            let shipped = syntax::production(source);
            (
                file.clone(),
                Module {
                    bodies: syntax::function_calls(source),
                    imports: import_map(&shipped, file),
                    tools: tool_names(&shipped),
                },
            )
        })
        .collect()
}

fn tool_inventory(modules: &BTreeMap<String, Module>) -> BTreeSet<ToolId> {
    modules
        .iter()
        .flat_map(|(file, module)| {
            module.tools.iter().map(|name| ToolId {
                file: file.clone(),
                name: name.clone(),
            })
        })
        .collect()
}

fn tool_names(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = source;

    while let Some(attribute) = rest.find("#[tool(") {
        rest = &rest[attribute + 7..];
        let Some(function) = rest.find("fn ") else {
            break;
        };
        let name = ident_at(rest, function + 3);
        rest = &rest[function + 3 + name.len()..];
        if !name.is_empty() {
            names.push(name);
        }
    }

    names
}

/// Maps each locally bound name to the function it actually resolves to.
/// Only `super`-relative imports inside the flat `src/mcp` tree are resolved;
/// anything else stays external and is matched by its boundary name.
fn import_map(source: &str, _file: &str) -> BTreeMap<String, Import> {
    let mut imports = BTreeMap::new();

    for start in occurrences(source, "use ") {
        if start > 0 && is_ident_byte(source.as_bytes()[start - 1]) {
            continue;
        }
        let tail = &source[start + 4..];
        let Some(end) = tail.find(';') else {
            continue;
        };
        let statement = tail[..end].trim();
        let Some(target) = import_target(statement) else {
            continue;
        };
        for (local, name) in import_items(statement) {
            imports.insert(
                local,
                Import {
                    file: target.clone(),
                    name,
                },
            );
        }
    }

    imports
}

fn import_target(statement: &str) -> Option<String> {
    let head = match statement.split_once('{') {
        Some((head, _)) => head.trim().trim_end_matches("::").trim(),
        None => statement.rsplit_once("::")?.0.trim(),
    };
    let mut segments = head.split("::");
    if segments.next()? != "super" {
        return None;
    }

    match segments.collect::<Vec<_>>().as_slice() {
        [] => Some("src/mcp/mod.rs".to_owned()),
        [name] => Some(format!("src/mcp/{name}.rs")),
        _ => None,
    }
}

fn import_items(statement: &str) -> Vec<(String, String)> {
    let items = match statement.split_once('{') {
        Some((_, tail)) => match tail.find('}') {
            Some(close) => &tail[..close],
            None => return Vec::new(),
        },
        None => statement,
    };

    items
        .split(',')
        .filter_map(|item| {
            let item = item.trim();
            if item.is_empty() || item.contains('{') {
                return None;
            }
            let (path, alias) = match item.split_once(" as ") {
                Some((path, alias)) => (path.trim(), Some(alias.trim())),
                None => (item, None),
            };
            let name = path.rsplit("::").next()?.trim();
            if name.is_empty() {
                return None;
            }
            let local = alias.unwrap_or(name);
            Some((local.to_owned(), name.to_owned()))
        })
        .collect()
}

fn is_guarded(
    modules: &BTreeMap<String, Module>,
    file: &str,
    name: &str,
    seen: &mut BTreeSet<(String, String)>,
) -> bool {
    let key = (file.to_owned(), name.to_owned());
    if !seen.insert(key.clone()) {
        return false;
    }

    if LOCAL_BOUNDARIES.contains(&(file, name)) {
        return modules
            .get(file)
            .is_some_and(|module| module.bodies.contains_key(name));
    }

    let guarded = modules.get(file).is_some_and(|module| {
        let Some(body) = module.bodies.get(name) else {
            return module
                .imports
                .get(name)
                .is_some_and(|import| is_guarded(modules, &import.file, &import.name, seen));
        };
        if body
            .iter()
            .any(|call| EXTERNAL_BOUNDARIES.contains(&call.as_str()))
        {
            return true;
        }

        body.iter().any(|call| {
            let Some((called_file, called_name)) = resolve_call(module, file, &call) else {
                return false;
            };
            is_guarded(modules, &called_file, &called_name, seen)
        })
    });

    seen.remove(&key);
    guarded
}

fn resolve_call(module: &Module, file: &str, call: &str) -> Option<(String, String)> {
    if let Some(import) = module.imports.get(call) {
        return Some((import.file.clone(), import.name.clone()));
    }
    if module.bodies.contains_key(call) {
        return Some((file.to_owned(), call.to_owned()));
    }
    if let Some(name) = call.strip_prefix("super::")
        && !name.contains("::")
        && file != "src/mcp/mod.rs"
    {
        return Some(("src/mcp/mod.rs".to_owned(), name.to_owned()));
    }
    None
}

#[cfg(test)]
mod fixtures {
    use super::*;

    fn fixture(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(path, source)| ((*path).to_owned(), (*source).to_owned()))
            .collect()
    }

    fn unguarded(files: &BTreeMap<String, String>) -> BTreeSet<ToolId> {
        let modules = load_modules(files);
        tool_inventory(&modules)
            .into_iter()
            .filter(|tool| !is_guarded(&modules, &tool.file, &tool.name, &mut BTreeSet::new()))
            .collect()
    }

    fn tool_id(file: &str, name: &str) -> ToolId {
        ToolId {
            file: file.to_owned(),
            name: name.to_owned(),
        }
    }

    #[test]
    fn direct_boundary_guarded() {
        let files = fixture(&[(
            "src/mcp/mod.rs",
            "fn authorize_tool() {}\n#[tool()]\nfn alpha(&self) { authorize_tool(); }\n",
        )]);

        assert!(unguarded(&files).is_empty());
    }

    #[test]
    fn shadowed_name_rejected() {
        let files = fixture(&[
            ("src/mcp/mod.rs", "#[tool()]\nfn alpha(&self) {}\n"),
            (
                "src/mcp/other.rs",
                "fn authorize_tool() {}\n#[tool()]\nfn alpha(&self) { authorize_tool(); }\n",
            ),
        ]);

        let unguarded = unguarded(&files);

        assert_eq!(
            unguarded,
            BTreeSet::from([tool_id("src/mcp/mod.rs", "alpha")])
        );
    }

    #[test]
    fn alias_boundary_resolved() {
        let files = fixture(&[
            ("src/mcp/mod.rs", "fn authorize_tool() {}\n"),
            (
                "src/mcp/tools.rs",
                "use super::helpers::check as verify;\n#[tool()]\nfn alpha(&self) { verify(); }\n",
            ),
            (
                "src/mcp/helpers.rs",
                "fn check() { super::authorize_tool(); }\n",
            ),
        ]);

        assert!(unguarded(&files).is_empty());
    }

    #[test]
    fn literal_not_boundary() {
        let files = fixture(&[(
            "src/mcp/mod.rs",
            "#[tool()]\nfn alpha(&self) { let _ = \"authorize_tool\"; }\n",
        )]);

        assert_eq!(
            unguarded(&files),
            BTreeSet::from([tool_id("src/mcp/mod.rs", "alpha")])
        );
    }

    #[test]
    fn comment_not_boundary() {
        let files = fixture(&[(
            "src/mcp/mod.rs",
            "// authorize_tool()\n#[tool()]\nfn alpha(&self) {}\n",
        )]);

        assert_eq!(
            unguarded(&files),
            BTreeSet::from([tool_id("src/mcp/mod.rs", "alpha")])
        );
    }

    #[test]
    fn extracted_handler_scanned() {
        let files = fixture(&[
            ("src/mcp/mod.rs", "fn authorize_tool() {}\n"),
            (
                "src/mcp/tools.rs",
                "use super::handlers::check;\n#[tool()]\nfn alpha(&self) { check(); }\n",
            ),
            (
                "src/mcp/handlers.rs",
                "pub fn check() { super::authorize_tool(); }\n",
            ),
        ]);

        assert!(unguarded(&files).is_empty());
    }

    #[test]
    fn unguarded_handler_flagged() {
        let files = fixture(&[("src/mcp/mod.rs", "#[tool()]\nfn alpha(&self) {}\n")]);

        assert_eq!(
            unguarded(&files),
            BTreeSet::from([tool_id("src/mcp/mod.rs", "alpha")])
        );
    }

    #[test]
    fn unrelated_path_rejected() {
        let files = fixture(&[(
            "src/mcp/tools.rs",
            "#[tool()]\nfn alpha(&self) { fake::authorize_tool(); }\n",
        )]);

        assert_eq!(
            unguarded(&files),
            BTreeSet::from([tool_id("src/mcp/tools.rs", "alpha")])
        );
    }

    #[test]
    fn local_shadow_rejected() {
        let files = fixture(&[(
            "src/mcp/tools.rs",
            "fn authorize_tool() {}\n#[tool()]\nfn alpha(&self) { authorize_tool(); }\n",
        )]);

        assert_eq!(
            unguarded(&files),
            BTreeSet::from([tool_id("src/mcp/tools.rs", "alpha")])
        );
    }

    #[test]
    #[should_panic(expected = "nested function authorize_tool")]
    fn nested_shadow_rejected() {
        let files = fixture(&[(
            "src/mcp/tools.rs",
            "#[tool()]\nfn alpha(&self) { fn authorize_tool() {} authorize_tool(); }\n",
        )]);

        let _ = unguarded(&files);
    }

    #[test]
    fn spaced_call_resolved() {
        let files = fixture(&[
            ("src/mcp/mod.rs", "fn authorize_tool() {}\n"),
            (
                "src/mcp/tools.rs",
                "#[tool()]\nfn alpha(&self) { super :: authorize_tool \n (); }\n",
            ),
        ]);

        assert!(unguarded(&files).is_empty());
    }

    #[test]
    fn validation_return_preserved() {
        let files = fixture(&[
            ("src/mcp/mod.rs", "fn authorize_tool() {}\n"),
            (
                "src/mcp/tools.rs",
                "#[tool()]\nfn alpha(&self) { if invalid() { return; } super::authorize_tool(); }\n",
            ),
        ]);

        assert!(unguarded(&files).is_empty());
    }
}
