//! Writes the language library a project builds against into its `.aruna` folder, again on every
//! build, so the project always uses the library and contract of the build that packages it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;

use crate::manifest::{Manifest, SettingKind};
use crate::trigger::TriggerKind;

/// The files of a library folder in `plugins/sdk`, by name.
macro_rules! library {
    ($folder:literal: $($name:literal),*) => {
        [$(($name, include_str!(concat!("../sdk/", $folder, "/", $name)))),*]
    };
}

/// The Python library's files, by name in its package `aruna`.
const PYTHON: [(&str, &str); 11] = library!("python/aruna":
    "__init__.py", "_component.py", "_host.py", "_run.py", "datasets.py", "errors.py", "jobs.py",
    "objects.py", "services.py", "state.py", "testing.py"
);

/// The JavaScript library's files: the module, its types, and the binding of the host's imports.
const JAVASCRIPT: [(&str, &str); 3] = library!("javascript": "aruna.js", "aruna.d.ts", "bind.js");

/// The Rust library's source files, as in `plugins/sdk/src`.
const RUST: [(&str, &str); 10] = library!("src":
    "datasets.rs", "form.rs", "host.rs", "jobs.rs", "lib.rs", "objects.rs", "outcome.rs", "run.rs",
    "service.rs", "state.rs"
);

/// The versions the Rust library depends on; a test checks them against the workspace's.
const SERDE: &str = "1.0.229";
const SERDE_JSON: &str = "1.0.152";
const WIT_BINDGEN: &str = "0.61.1";

/// Writes the crate `aruna-plugin` with a standalone manifest to `.aruna/rust/aruna-plugin/` and
/// the contract to `.aruna/rust/wit/`, the layout its bindings expect.
pub(crate) fn rust(project: &Path) -> std::io::Result<()> {
    let folder = project.join(".aruna/rust");
    let source = folder.join("aruna-plugin/src");
    std::fs::create_dir_all(&source)?;
    std::fs::create_dir_all(folder.join("wit"))?;
    for (name, text) in RUST {
        std::fs::write(source.join(name), text)?;
    }
    let manifest = format!(
        "# The Rust library for Aruna plugins, written by aruna-cli again on every build.\n\
         [package]\nname = \"aruna-plugin\"\nversion = \"{}\"\nedition = \"2024\"\n\
         license = \"MIT OR Apache-2.0\"\npublish = false\n\n[dependencies]\n\
         serde = {{ version = \"{SERDE}\", features = [\"derive\"] }}\n\
         serde_json = \"{SERDE_JSON}\"\nwit-bindgen = \"{WIT_BINDGEN}\"\n",
        env!("CARGO_PKG_VERSION")
    );
    std::fs::write(folder.join("aruna-plugin/Cargo.toml"), manifest)?;
    std::fs::write(folder.join("wit/plugin.wit"), crate::package::CONTRACT)
}

/// Writes the package `aruna` to `.aruna/python/aruna/`, with the types of the manifest's form
/// fields, so `dataset` and `object` fields load before the handler runs.
pub(crate) fn python(project: &Path, manifest: &Manifest) -> std::io::Result<()> {
    let folder = project.join(".aruna/python/aruna");
    std::fs::create_dir_all(&folder)?;
    for (name, text) in PYTHON {
        std::fs::write(folder.join(name), text)?;
    }
    let header = "\"\"\"The type of each manual form field, written by the build.\"\"\"\n";
    let fields = format!("{header}\nFIELDS = {}\n", form_kinds(manifest));
    std::fs::write(folder.join("_fields.py"), fields)
}

/// Writes the library to `.aruna/javascript/` with `entry.js`, which binds the host, imports the
/// project's `plugin.js` so it registers its handlers, and exports the handler calls.
pub(crate) fn javascript(project: &Path, manifest: &Manifest) -> std::io::Result<()> {
    let folder = project.join(".aruna/javascript");
    std::fs::create_dir_all(&folder)?;
    for (name, text) in JAVASCRIPT {
        std::fs::write(folder.join(name), text)?;
    }
    let entry = format!(
        "// The component's entry, written by the build.\n\
         import {{ use }} from \"./aruna.js\";\nimport host from \"./bind.js\";\n\
         import \"../../plugin.js\";\n\nuse(host, {});\n\n\
         export {{ handle, triggers }} from \"./aruna.js\";\n",
        form_kinds(manifest)
    );
    std::fs::write(folder.join("entry.js"), entry)
}

/// Each `[manual]` field's type by its name, as a JSON object.
fn form_kinds(manifest: &Manifest) -> serde_json::Value {
    let form = (manifest.triggers.get(&TriggerKind::Manual)).map(|trigger| &trigger.form);
    let kinds = (form.into_iter().flatten())
        .map(|(name, field)| (name.clone(), kind_name(field.kind).into()));
    serde_json::Value::Object(kinds.collect())
}

fn kind_name(kind: SettingKind) -> &'static str {
    match kind {
        SettingKind::String => "string",
        SettingKind::Integer => "integer",
        SettingKind::Boolean => "boolean",
        SettingKind::Url => "url",
        SettingKind::Choice => "choice",
        SettingKind::Dataset => "dataset",
        SettingKind::Object => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Manifest {
        let text = "name = \"p\"\nversion = \"0.1.0\"\non = [\"manual\"]\n[manual]\n\
                    dataset = { type = \"dataset\" }\nnote = \"Note\"\n";
        crate::authored::parse(text).unwrap()
    }

    #[test]
    fn rust_crate() {
        let project = tempfile::tempdir().unwrap();
        rust(project.path()).unwrap();
        let folder = project.path().join(".aruna/rust");
        let source = std::fs::read_dir(folder.join("aruna-plugin/src")).unwrap();
        assert_eq!(source.count(), RUST.len());
        assert!(folder.join("wit/plugin.wit").is_file());
        let text = std::fs::read_to_string(folder.join("aruna-plugin/Cargo.toml")).unwrap();
        let written: toml::Table = text.parse().unwrap();
        let workspace: toml::Table = include_str!("../../Cargo.toml").parse().unwrap();
        let version = |table: &toml::Value| {
            let version = table.get("version").unwrap_or(table);
            version.as_str().unwrap().to_string()
        };
        for name in ["serde", "serde_json", "wit-bindgen"] {
            let wanted = version(&workspace["workspace"]["dependencies"][name]);
            assert_eq!(version(&written["dependencies"][name]), wanted, "{name}");
        }
        let package = &workspace["workspace"]["package"]["version"];
        assert_eq!(&written["package"]["version"], package);
    }

    #[test]
    fn javascript_entry() {
        let project = tempfile::tempdir().unwrap();
        javascript(project.path(), &manifest()).unwrap();
        let folder = project.path().join(".aruna/javascript");
        let entry = std::fs::read_to_string(folder.join("entry.js")).unwrap();
        let bound = entry.find("import host from \"./bind.js\"").unwrap();
        assert!(bound < entry.find("import \"../../plugin.js\"").unwrap());
        assert!(entry.contains("use(host, {\"dataset\":\"dataset\",\"note\":\"string\"});"));
        assert!(entry.ends_with("export { handle, triggers } from \"./aruna.js\";\n"));
        let count = std::fs::read_dir(&folder).unwrap().count();
        assert_eq!(count, JAVASCRIPT.len() + 1);
    }

    #[test]
    fn python_package() {
        let manifest = manifest();
        let project = tempfile::tempdir().unwrap();
        python(project.path(), &manifest).unwrap();
        let folder = project.path().join(".aruna/python/aruna");
        let fields = std::fs::read_to_string(folder.join("_fields.py")).unwrap();
        assert!(
            fields.ends_with("FIELDS = {\"dataset\":\"dataset\",\"note\":\"string\"}\n"),
            "{fields}"
        );
        let entry = std::fs::read_to_string(folder.join("_component.py")).unwrap();
        assert!(
            entry.contains("import plugin"),
            "the entry imports the plugin's module"
        );
        assert_eq!(
            std::fs::read_dir(&folder).unwrap().count(),
            PYTHON.len() + 1
        );
    }
}
