//! Starts plugin projects and builds them with their toolchain into one checked artifact.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};

use aruna_plugins::artifact::ArtifactDigest;
use aruna_plugins::manifest::valid_plugin;
use aruna_plugins::package::build_project;
use serde::Serialize;

use super::Context;
use crate::args::PluginCommand;
use crate::error::CliError;

/// Template files of a Rust project as (path, content); `{{name}}` becomes the plugin name.
const TEMPLATE: [(&str, &str); 3] = [
    (
        "plugin.toml",
        include_str!("../../../plugins/templates/rust/plugin.toml"),
    ),
    (
        "Cargo.toml",
        include_str!("../../../plugins/templates/rust/Cargo.toml"),
    ),
    (
        "src/lib.rs",
        include_str!("../../../plugins/templates/rust/src/lib.rs"),
    ),
];

pub async fn plugin(context: &mut Context<'_>, command: PluginCommand) -> Result<(), CliError> {
    match command {
        PluginCommand::New { name, path } => {
            let path = path.unwrap_or_else(|| PathBuf::from(&name));
            create(&name, &path)?;
            let shown = path.display();
            context.out.line(format!(
                "created {shown}; next: aruna-cli plugin build {shown}"
            ))
        }
        PluginCommand::Build { path } => {
            let built = build_project(&path).map_err(|err| CliError::usage(err.to_string()))?;
            let summary = Built {
                digest: ArtifactDigest::of(&built.artifact).to_string(),
                artifact: built.path,
                handlers: built.manifest.handlers.into_iter().collect(),
            };
            match context.out.json {
                true => context.out.json(&summary),
                false => {
                    context
                        .out
                        .line(format!("{} {}", summary.artifact.display(), summary.digest))
                }
            }
        }
    }
}

/// Writes a starter project with one manual handler into an empty or new folder.
fn create(name: &str, path: &Path) -> Result<(), CliError> {
    if !valid_plugin(name) {
        return Err(CliError::usage(
            "a plugin name has 1 to 64 of a-z, 0-9 and -, starts with a letter and does not end with -",
        ));
    }
    if path.exists() && path.read_dir()?.next().is_some() {
        return Err(CliError::usage(format!(
            "{} exists and is not empty",
            path.display()
        )));
    }
    for (file, content) in TEMPLATE {
        let target = path.join(file);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(target, content.replace("{{name}}", name))?;
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct Built {
    artifact: PathBuf,
    digest: String,
    handlers: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_plugins::package::load_manifest;

    #[test]
    fn starter_project() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lab-sync");
        create("lab-sync", &path).unwrap();
        let mut manifest = load_manifest(&path).unwrap();
        assert_eq!(manifest.name, "lab-sync");
        assert_eq!(manifest.wire(None), Ok(()));
        let code = std::fs::read_to_string(path.join("src").join("lib.rs")).unwrap();
        assert!(code.contains("aruna_plugin::export!(run);"));
        assert!(
            create("lab-sync", &path).is_err(),
            "a folder with files is refused"
        );
        assert!(create("Lab", &dir.path().join("other")).is_err());
    }
}
