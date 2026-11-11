//! Starts plugin projects in any supported language and builds them into one checked artifact.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};

use aruna_plugins::artifact::ArtifactDigest;
use aruna_plugins::manifest::valid_plugin;
use aruna_plugins::package::build_project;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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
        PluginCommand::Install {
            name,
            group,
            version,
            settings,
            grants,
            preview,
            id,
        } => {
            let body = serde_json::json!({
                "group": group,
                "version": version,
                "settings": setting_values(&settings)?,
                "grants": grant_choices(&grants)?,
                "id": id,
            });
            install(context, &name, &body, preview).await
        }
        PluginCommand::Installs { group } => installs(context, &group).await,
        PluginCommand::Grants { id, grants } => {
            let body = serde_json::json!({ "grants": grant_choices(&grants)? });
            let path = format!("plugin-installs/{id}/grants");
            change(context, reqwest::Method::PUT, &path, &body).await
        }
        PluginCommand::Update { id, version } => {
            let body = serde_json::json!({ "version": version });
            let path = format!("plugin-installs/{id}/version");
            change(context, reqwest::Method::PUT, &path, &body).await
        }
        PluginCommand::Pause { id } => {
            let path = format!("plugin-installs/{id}/pause");
            change(
                context,
                reqwest::Method::POST,
                &path,
                &serde_json::json!({}),
            )
            .await
        }
        PluginCommand::Resume { id } => {
            let path = format!("plugin-installs/{id}/resume");
            change(
                context,
                reqwest::Method::POST,
                &path,
                &serde_json::json!({}),
            )
            .await
        }
        PluginCommand::Remove { id } => {
            context
                .api()?
                .delete(&format!("plugin-installs/{id}"))
                .await?;
            context.out.line(format!("removed {id}"))
        }
        PluginCommand::Run { id, input } => {
            let input: Value = serde_json::from_str(input.as_deref().unwrap_or("{}"))
                .map_err(|_| CliError::usage("--input is not JSON"))?;
            let path = format!("plugin-installs/{id}/runs");
            let body = serde_json::json!({ "input": input });
            let run: RunView = (context.api()?)
                .send(reqwest::Method::POST, &path, &body)
                .await?;
            show_run(context, &run)
        }
        PluginCommand::Runs { id, run: Some(run) } => {
            let path = format!("plugin-installs/{id}/runs/{run}");
            let run: RunView = context.api()?.get(&path, &[]).await?;
            show_run(context, &run)
        }
        PluginCommand::Runs { id, run: None } => runs(context, &id).await,
        PluginCommand::Delivery { id } => delivery(context, &id).await,
        PluginCommand::Resolve { id, run, done } => {
            let path = format!("plugin-installs/{id}/runs/{run}/resolve");
            let body = serde_json::json!({ "done": done });
            let run: RunView = (context.api()?)
                .send(reqwest::Method::POST, &path, &body)
                .await?;
            show_run(context, &run)
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

/// `name=value` pairs; a value that parses as JSON is JSON, anything else is text.
fn setting_values(pairs: &[String]) -> Result<serde_json::Map<String, Value>, CliError> {
    let mut values = serde_json::Map::new();
    for pair in pairs {
        let (name, value) = split_pair(pair)?;
        let value = serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.into()));
        values.insert(name.to_string(), value);
    }
    Ok(values)
}

/// `permission=pattern` pairs grouped by permission; `permission=` keeps an empty list.
fn grant_choices(pairs: &[String]) -> Result<BTreeMap<String, Vec<String>>, CliError> {
    let mut choices: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for pair in pairs {
        let (permission, pattern) = split_pair(pair)?;
        let patterns = choices.entry(permission.to_string()).or_default();
        if !pattern.is_empty() {
            patterns.push(pattern.to_string());
        }
    }
    Ok(choices)
}

fn split_pair(pair: &str) -> Result<(&str, &str), CliError> {
    pair.split_once('=')
        .filter(|(name, _)| !name.is_empty())
        .ok_or_else(|| CliError::usage(format!("`{pair}` is not of the form name=value")))
}

#[derive(Debug, Serialize, Deserialize)]
struct InstallView {
    id: String,
    plugin: String,
    version: String,
    status: String,
    #[serde(default)]
    failed: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct InstallPage {
    installs: Vec<InstallView>,
    next: Option<String>,
}

/// Installs, or with `preview` shows the summary and the id to install with.
async fn install(
    context: &mut Context<'_>,
    name: &str,
    body: &Value,
    preview: bool,
) -> Result<(), CliError> {
    let api = context.api()?;
    let request = api.request(reqwest::Method::POST, &format!("plugins/{name}/installs"))?;
    let request = request
        .query(&[("preview", preview.to_string())])
        .json(body);
    let answer: Value = read_json(request.send().await?).await?;
    if context.out.json {
        return context.out.json(&answer);
    }
    let summary = match preview {
        true => &answer,
        false => &answer["summary"],
    };
    let mut lines = vec![format!(
        "{} {} ({}) for group {}",
        summary["plugin"].as_str().unwrap_or_default(),
        summary["version"].as_str().unwrap_or_default(),
        summary["digest"].as_str().unwrap_or_default(),
        summary["group"].as_str().unwrap_or_default(),
    )];
    for line in summary["permissions"].as_array().into_iter().flatten() {
        let granted = line["granted"].as_array().map_or(0, Vec::len);
        lines.push(format!(
            "  {} granted on {granted} paths",
            line["permission"].as_str().unwrap_or_default()
        ));
    }
    for line in summary["connections"].as_array().into_iter().flatten() {
        let granted = line["granted"].as_bool().unwrap_or(false);
        lines.push(format!(
            "  connection {} granted: {granted}",
            line["url"].as_str().unwrap_or_default()
        ));
    }
    lines.push(match preview {
        true => format!(
            "install with --id {} --version {}",
            summary["id"].as_str().unwrap_or_default(),
            summary["digest"].as_str().unwrap_or_default()
        ),
        false => format!(
            "installed as {} ({})",
            answer["id"].as_str().unwrap_or_default(),
            answer["status"].as_str().unwrap_or_default()
        ),
    });
    lines.extend(warnings(summary));
    context.out.line(lines.join("\n"))
}

/// The summary's warnings, one line each.
fn warnings(summary: &Value) -> Vec<String> {
    (summary["warnings"].as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .map(|warning| format!("warning: {warning}"))
        .collect()
}

/// Shows the install's waiting events, its undelivered events and triggers not delivered yet.
async fn delivery(context: &mut Context<'_>, id: &str) -> Result<(), CliError> {
    let path = format!("plugin-installs/{id}/delivery");
    let view: Value = context.api()?.get(&path, &[]).await?;
    if context.out.json {
        return context.out.json(&view);
    }
    let mut lines = vec![format!(
        "waiting: {}",
        view["waiting"].as_u64().unwrap_or(0)
    )];
    if let Some(age) = view["oldest_age_ms"].as_u64() {
        lines.push(format!("oldest waiting: {} s", age / 1000));
    }
    for gap in view["gaps"].as_array().into_iter().flatten() {
        lines.push(format!(
            "not delivered: {} {} ({})",
            gap["count"].as_u64().unwrap_or(0),
            gap["kind"].as_str().unwrap_or_default(),
            gap["reason"].as_str().unwrap_or_default()
        ));
    }
    for kind in view["undelivered"].as_array().into_iter().flatten() {
        let kind = kind.as_str().unwrap_or_default();
        lines.push(format!("trigger not delivered yet: {kind}"));
    }
    lines.extend(view["note"].as_str().map(|note| format!("note: {note}")));
    context.out.line(lines.join("\n"))
}

/// Lists the installs of a group, following the cursor across pages.
async fn installs(context: &mut Context<'_>, group: &str) -> Result<(), CliError> {
    let api = context.api()?;
    let mut installs = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut query = vec![("group", group.to_string()), ("limit", "1000".to_string())];
        query.extend(cursor.take().map(|cursor| ("cursor", cursor)));
        let page: InstallPage = api.get("plugin-installs", &query).await?;
        installs.extend(page.installs);
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let rows = (installs.iter())
        .map(|i| {
            vec![
                i.id.clone(),
                i.plugin.clone(),
                i.version.clone(),
                i.status.clone(),
            ]
        })
        .collect();
    context
        .out
        .listing(&installs, &["ID", "PLUGIN", "VERSION", "STATUS"], rows)
}

#[derive(Debug, Serialize, Deserialize)]
struct RunLine {
    at_ms: u64,
    level: String,
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunView {
    id: String,
    state: String,
    trigger: String,
    attempts: u32,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    output: Option<String>,
    #[serde(default)]
    log: Vec<RunLine>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunPage {
    runs: Vec<RunView>,
    next: Option<String>,
}

/// One run: its state and outcome, then its log lines.
fn show_run(context: &mut Context<'_>, run: &RunView) -> Result<(), CliError> {
    if context.out.json {
        return context.out.json(run);
    }
    let outcome = run
        .message
        .as_deref()
        .or(run.output.as_deref())
        .unwrap_or_default();
    context.out.line(format!(
        "{} {} {} {outcome}",
        run.id, run.trigger, run.state
    ))?;
    for line in &run.log {
        context
            .out
            .line(format!("  {} {} {}", line.at_ms, line.level, line.message))?;
    }
    Ok(())
}

async fn runs(context: &mut Context<'_>, id: &str) -> Result<(), CliError> {
    let api = context.api()?;
    let path = format!("plugin-installs/{id}/runs");
    let mut runs = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut query = vec![("limit", "1000".to_string())];
        query.extend(cursor.take().map(|cursor| ("cursor", cursor)));
        let page: RunPage = api.get(&path, &query).await?;
        runs.extend(page.runs);
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let rows = (runs.iter())
        .map(|run| {
            let message = run.message.clone().unwrap_or_default();
            vec![
                run.id.clone(),
                run.trigger.clone(),
                run.state.clone(),
                message,
            ]
        })
        .collect();
    let header = ["ID", "TRIGGER", "STATE", "MESSAGE"];
    context.out.listing(&runs, &header, rows)
}

/// Sends one change of an install and shows its new state.
async fn change(
    context: &mut Context<'_>,
    method: reqwest::Method,
    path: &str,
    body: &Value,
) -> Result<(), CliError> {
    let answer: Value = context.api()?.send(method, path, body).await?;
    if context.out.json {
        return context.out.json(&answer);
    }
    let install = answer.get("install").unwrap_or(&answer);
    let view: InstallView = serde_json::from_value(install.clone())?;
    let mut line = format!(
        "{} {} {} {}",
        view.id, view.plugin, view.version, view.status
    );
    let missing: Vec<&str> = (answer["missing"].as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .collect();
    if !missing.is_empty() {
        line.push_str(&format!("; grant first: {}", missing.join(", ")));
    }
    let lines = [vec![line], warnings(&install["summary"])].concat();
    context.out.line(lines.join("\n"))
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
