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
