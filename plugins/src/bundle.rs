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

/// Writes the package `aruna` to `.aruna/python/aruna/`, with the types of the manifest's form
/// fields, so `dataset` and `object` fields load before the handler runs.
pub(crate) fn python(project: &Path, manifest: &Manifest) -> std::io::Result<()> {
    let folder = project.join(".aruna/python/aruna");
    std::fs::create_dir_all(&folder)?;
    for (name, text) in PYTHON {
        std::fs::write(folder.join(name), text)?;
    }
    std::fs::write(folder.join("_fields.py"), python_fields(manifest))
}

/// `FIELDS = {...}`: each `[manual]` field's type by its name.
fn python_fields(manifest: &Manifest) -> String {
    let form = (manifest.triggers.get(&TriggerKind::Manual)).map(|trigger| &trigger.form);
    let kinds: serde_json::Map<String, serde_json::Value> = (form.into_iter().flatten())
        .map(|(name, field)| (name.clone(), kind_name(field.kind).into()))
        .collect();
    let header = "\"\"\"The type of each manual form field, written by the build.\"\"\"\n";
    format!("{header}\nFIELDS = {}\n", serde_json::Value::Object(kinds))
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

    #[test]
    fn python_package() {
        let text = "name = \"p\"\nversion = \"0.1.0\"\non = [\"manual\"]\n[manual]\n\
                    dataset = { type = \"dataset\" }\nnote = \"Note\"\n";
        let manifest = crate::authored::parse(text).unwrap();
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
