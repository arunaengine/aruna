//! Starts plugin projects in any supported language and builds them into one checked artifact.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};

use aruna_plugins::artifact::ArtifactDigest;
use aruna_plugins::manifest::valid_plugin;
use aruna_plugins::package::build_project;
use serde::{Deserialize, Serialize};

use super::Context;
use crate::args::{Language, PluginCommand};
use crate::client::read_json;
use crate::error::CliError;

/// The host contract every non-Rust project builds against; the Rust SDK carries its own copy.
const CONTRACT: &str = include_str!("../../../plugins/wit/plugin.wit");

/// Template files per language as (path, content); `{{name}}` becomes the plugin name.
fn template(language: Language) -> Vec<(&'static str, &'static str)> {
    match language {
        Language::Rust => vec![
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
        ],
        Language::Javascript => vec![
            (
                "plugin.toml",
                include_str!("../../../plugins/templates/javascript/plugin.toml"),
            ),
            (
                "plugin.js",
                include_str!("../../../plugins/templates/javascript/plugin.js"),
            ),
            ("wit/plugin.wit", CONTRACT),
        ],
        Language::Python => vec![
            (
                "plugin.toml",
                include_str!("../../../plugins/templates/python/plugin.toml"),
            ),
            (
                "app.py",
                include_str!("../../../plugins/templates/python/app.py"),
            ),
            (
                "aruna.py",
                include_str!("../../../plugins/templates/python/aruna.py"),
            ),
            ("wit/plugin.wit", CONTRACT),
        ],
    }
}

pub async fn plugin(context: &mut Context<'_>, command: PluginCommand) -> Result<(), CliError> {
    match command {
        PluginCommand::New {
            name,
            language,
            path,
        } => {
            let path = path.unwrap_or_else(|| PathBuf::from(&name));
            create(&name, language, &path)?;
            let shown = path.display();
            context.out.line(format!(
                "created {shown}; next: aruna-cli plugin build {shown}"
            ))
        }
        PluginCommand::Publish { artifact } => publish(context, &artifact).await,
        PluginCommand::List { name } => list(context, name).await,
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
fn create(name: &str, language: Language, path: &Path) -> Result<(), CliError> {
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
    for (file, content) in template(language) {
        let target = path.join(file);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(target, content.replace("{{name}}", name))?;
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct PluginView {
    name: String,
    version: String,
    title: Option<String>,
    digest: String,
    size: u64,
    published_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PluginPage {
    plugins: Vec<PluginView>,
    next: Option<String>,
}

/// Uploads one artifact; a management node answers once enough copies are stored.
async fn publish(context: &mut Context<'_>, artifact: &Path) -> Result<(), CliError> {
    let bytes = std::fs::read(artifact)?;
    let api = context.api()?;
    let request = api.request(reqwest::Method::POST, "plugins")?;
    let request = request.header(reqwest::header::CONTENT_TYPE, "application/wasm");
    let published: PluginView = read_json(request.body(bytes).send().await?).await?;
    match context.out.json {
        true => context.out.json(&published),
        false => context.out.line(format!(
            "published {} {} {}",
            published.name, published.version, published.digest
        )),
    }
}

/// Lists every published version, following the cursor across pages.
async fn list(context: &mut Context<'_>, name: Option<String>) -> Result<(), CliError> {
    let api = context.api()?;
    let mut plugins = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut query = vec![("limit", "1000".to_string())];
        query.extend(name.clone().map(|name| ("name", name)));
        query.extend(cursor.take().map(|cursor| ("cursor", cursor)));
        let page: PluginPage = api.get("plugins", &query).await?;
        plugins.extend(page.plugins);
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let rows = (plugins.iter())
        .map(|p| {
            vec![
                p.name.clone(),
                p.version.clone(),
                p.title.clone().unwrap_or_default(),
                p.digest.clone(),
            ]
        })
        .collect();
    context
        .out
        .listing(&plugins, &["NAME", "VERSION", "TITLE", "DIGEST"], rows)
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
    fn starter_projects() {
        let dir = tempfile::tempdir().unwrap();
        for language in [Language::Rust, Language::Javascript, Language::Python] {
            let path = dir.path().join(format!("{language:?}").to_lowercase());
            create("lab-sync", language, &path).unwrap();
            let mut manifest = load_manifest(&path).unwrap();
            assert_eq!(manifest.name, "lab-sync");
            assert_eq!(manifest.wire(None), Ok(()), "{language:?}");
            assert_eq!(manifest.build.is_some(), language != Language::Rust);
        }
        let rust = dir.path().join("rust");
        let code = std::fs::read_to_string(rust.join("src").join("lib.rs")).unwrap();
        assert!(code.contains("aruna_plugin::export!(run);"));
        let contract = std::fs::read_to_string(dir.path().join("python/wit/plugin.wit")).unwrap();
        assert_eq!(contract, CONTRACT);
        assert!(
            create("lab-sync", Language::Rust, &rust).is_err(),
            "a folder with files is refused"
        );
        assert!(create("Lab", Language::Rust, &dir.path().join("other")).is_err());
    }
}
