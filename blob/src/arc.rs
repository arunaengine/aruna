//! Converts ISA metadata, records signed ARC snapshots and merges them into main.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::git::{command, exchange};
use aruna_core::git::{GitSnapshot, GitStatus, MAX_GIT_BYTES};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use tokio::process::Command;

pub async fn convert(request: Value) -> std::io::Result<Value> {
    let mut process = Command::new("python3");
    process.args(["-c", include_str!("arc.py")]);
    let bytes = serde_json::to_vec(&request).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_GIT_BYTES {
        return Err(std::io::Error::other("ARC conversion exceeds limit"));
    }
    let output = exchange(process, bytes.into(), false).await?;
    serde_json::from_slice(&output).map_err(std::io::Error::other)
}

pub async fn export(directory: &Path, revision: &str) -> std::io::Result<Value> {
    if revision.is_empty() || revision.starts_with('-') || revision.len() > 256 {
        return Err(std::io::Error::other("invalid Git revision"));
    }
    let oid = command(
        directory,
        &["rev-parse", "--verify", &format!("{revision}^{{commit}}")],
    )
    .await?;
    let oid = std::str::from_utf8(&oid)
        .map_err(std::io::Error::other)?
        .trim();
    let tree = command(directory, &["ls-tree", "-rlz", oid]).await?;
    let mut files = BTreeMap::new();
    let mut total = 0usize;
    for row in tree.split(|byte| *byte == 0).filter(|row| !row.is_empty()) {
        let row = std::str::from_utf8(row).map_err(std::io::Error::other)?;
        let (header, path) = row
            .split_once('\t')
            .ok_or_else(|| std::io::Error::other("invalid Git tree"))?;
        let fields: Vec<_> = header.split_whitespace().collect();
        if fields.len() != 4
            || fields[1] != "blob"
            || !matches!(fields[0], "100644" | "100755")
            || files.len() >= 10000
        {
            return Err(std::io::Error::other("unsupported ARC tree"));
        }
        let data = command(directory, &["cat-file", "blob", fields[2]]).await?;
        total = total.saturating_add(data.len());
        if total > MAX_GIT_BYTES / 2 {
            return Err(std::io::Error::other(
                "ARC Git files exceed limit; use LFS for large data",
            ));
        }
        files.insert(path.to_string(), STANDARD.encode(data));
    }
    let mut result = convert(json!({"mode":"inspect", "files":files})).await?;
    result["commit"] = json!(oid);
    Ok(result)
}

async fn reference(directory: &Path, name: &str) -> std::io::Result<Option<String>> {
    let refs = command(directory, &["for-each-ref", "--format=%(objectname)", name]).await?;
    let value = std::str::from_utf8(&refs)
        .map_err(std::io::Error::other)?
        .trim();
    Ok((!value.is_empty()).then(|| value.to_string()))
}

fn failed(error: &str) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

async fn revision(directory: &Path, commit: &str) -> std::io::Result<ulid::Ulid> {
    let format = "--format=%(trailers:key=Aruna-Revision,valueonly)";
    let value = command(directory, &["log", "-1", format, commit]).await?;
    std::str::from_utf8(&value)
        .map_err(std::io::Error::other)?
        .trim()
        .parse()
        .map_err(std::io::Error::other)
}

type Files = BTreeMap<String, (String, Vec<u8>)>;

/// Writes `base` (or an empty tree) with `files` replaced and `removed` paths dropped.
async fn write_tree(
    directory: &Path,
    base: Option<&str>,
    files: &Files,
    removed: &[String],
) -> std::io::Result<String> {
    let temporary = tempfile::tempdir_in(directory)?;
    let index = temporary.path().join("index");
    let git = |args: &[&str]| {
        let mut process = Command::new("git");
        process
            .current_dir(directory)
            .env("GIT_INDEX_FILE", &index)
            .args(args);
        process
    };
    if let Some(base) = base {
        exchange(git(&["read-tree", base]), Bytes::new(), false).await?;
    }
    for path in removed {
        exchange(
            git(&["update-index", "--force-remove", "--", path]),
            Bytes::new(),
            false,
        )
        .await?;
    }
    for (path, (mode, data)) in files {
        let oid = exchange(
            git(&["hash-object", "-w", "--stdin"]),
            data.clone().into(),
            false,
        )
        .await?;
        let oid = std::str::from_utf8(&oid)
            .map_err(std::io::Error::other)?
            .trim();
        let arguments = ["update-index", "--add", "--cacheinfo", mode, oid, path];
        exchange(git(&arguments), Bytes::new(), false).await?;
    }
    let tree = exchange(git(&["write-tree"]), Bytes::new(), false).await?;
    Ok(std::str::from_utf8(&tree)
        .map_err(std::io::Error::other)?
        .trim()
        .to_string())
}

async fn commit_tree(
    directory: &Path,
    tree: &str,
    parents: &[&str],
    message: String,
    occurred_at_ms: u64,
) -> std::io::Result<String> {
    let mut process = Command::new("git");
    let date = format!("@{} +0000", occurred_at_ms / 1000);
    process
        .current_dir(directory)
        .args(["commit-tree", "-S", tree])
        .env("GIT_AUTHOR_NAME", "Aruna")
        .env("GIT_COMMITTER_NAME", "Aruna")
        .env("GIT_AUTHOR_EMAIL", "git@aruna.local")
        .env("GIT_COMMITTER_EMAIL", "git@aruna.local")
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date);
    for parent in parents {
        process.args(["-p", parent]);
    }
    let commit = exchange(process, message.into(), false).await?;
    Ok(std::str::from_utf8(&commit)
        .map_err(std::io::Error::other)?
        .trim()
        .to_string())
}

fn workbook(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.starts_with("isa.") && name.ends_with(".xlsx")
}

fn entities(value: &Value) -> Vec<String> {
    let mut graph: Vec<_> = value["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .map(Value::to_string)
        .collect();
    graph.sort();
    graph
}

/// Brings graph changes onto a client-edited main without replacing equivalent client files.
/// Workbooks change only when their ISA meaning differs from the graph's generated ARC.
async fn reconcile(directory: &Path, main: &str, files: &Files) -> std::io::Result<Option<String>> {
    let (_, derived) = files
        .get("ro-crate-metadata.json")
        .ok_or_else(|| failed("ARC conversion omitted its RO-Crate"))?;
    let derived: Value = serde_json::from_slice(derived)?;
    let current = export(directory, main).await.ok();
    let same = current
        .as_ref()
        .and_then(|value| value.get("rocrate"))
        .is_some_and(|value| entities(value) == entities(&derived));
    let mut overlay = Files::new();
    for (path, entry) in files {
        let isa = workbook(path) || path == "LICENSE";
        if (isa && same)
            || !(isa
                || matches!(
                    path.as_str(),
                    "ro-crate-metadata.json" | "aruna-metadata.json"
                ))
        {
            continue;
        }
        let existing = command(directory, &["show", &format!("{main}:{path}")])
            .await
            .ok();
        if existing.as_deref() != Some(entry.1.as_slice()) {
            overlay.insert(path.clone(), entry.clone());
        }
    }
    let mut removed = Vec::new();
    if !same {
        let listing = command(directory, &["ls-tree", "-r", "-z", "--name-only", main]).await?;
        for path in listing.split(|byte| *byte == 0) {
            let path = std::str::from_utf8(path).map_err(std::io::Error::other)?;
            if workbook(path) && !files.contains_key(path) {
                removed.push(path.to_string());
            }
        }
    }
    if overlay.is_empty() && removed.is_empty() {
        return Ok(None);
    }
    write_tree(directory, Some(main), &overlay, &removed)
        .await
        .map(Some)
}

pub async fn snapshot(directory: &Path, source: GitSnapshot) -> std::io::Result<GitStatus> {
    let previous = reference(directory, "refs/heads/aruna").await?;
    if let Some(previous) = &previous {
        let event_id = revision(directory, previous).await?;
        let stored_json = command(
            directory,
            &["show", &format!("{previous}:aruna-metadata.json")],
        )
        .await?;
        if event_id > source.event_id
            || (event_id == source.event_id && stored_json.as_ref() == source.jsonld.as_bytes())
        {
            return Ok(GitStatus {
                event_id,
                commit: Some(previous.clone()),
                error: None,
            });
        }
    }
    let conversion = convert(json!({"mode":"generate", "document_id":source.document_id.to_string(), "jsonld":source.jsonld})).await?;
    let status = |error: &str| GitStatus {
        event_id: source.event_id,
        commit: previous.clone(),
        error: Some(error.into()),
    };
    if let Some(error) = conversion["error"].as_str() {
        return Ok(status(error));
    }
    let mut files = Files::new();
    for (path, content) in conversion["files"]
        .as_object()
        .ok_or_else(|| failed("ARC conversion omitted files"))?
    {
        let data = STANDARD
            .decode(content.as_str().ok_or_else(|| failed("invalid ARC file"))?)
            .map_err(std::io::Error::other)?;
        files.insert(path.clone(), ("100644".into(), data));
    }
    let main = reference(directory, "refs/heads/main").await?;
    let mut total: usize = files.values().map(|(_, data)| data.len()).sum();
    for path in conversion["required"].as_array().into_iter().flatten() {
        let path = path
            .as_str()
            .ok_or_else(|| failed("invalid ARC data path"))?;
        if files.contains_key(path) {
            continue;
        }
        let data = match &main {
            Some(main) => command(directory, &["show", &format!("{main}:{path}")])
                .await
                .ok(),
            None => None,
        };
        let (Some(main), Some(data)) = (&main, data) else {
            return Ok(status(
                "Referenced ARC data is missing; upload it through native Git/LFS",
            ));
        };
        total = total.saturating_add(data.len());
        if total > MAX_GIT_BYTES / 2 || files.len() >= 9999 {
            return Ok(status("ARC Git files exceed limit; use LFS for large data"));
        }
        let entry = command(
            directory,
            &["ls-tree", main, "--", &format!(":(literal){path}")],
        )
        .await?;
        let entry = std::str::from_utf8(&entry).map_err(std::io::Error::other)?;
        let mode = entry.split_whitespace().next().unwrap_or("");
        if !matches!(mode, "100644" | "100755") {
            return Err(failed("unsupported ARC data mode"));
        }
        files.insert(path.to_string(), (mode.to_string(), data.to_vec()));
    }
    let trailer = format!("\n\nAruna-Revision: {}\n", source.event_id);
    let tree = write_tree(directory, None, &files, &[]).await?;
    let parents: Vec<&str> = previous.iter().map(String::as_str).collect();
    let message = format!("feat: capture Aruna metadata{trailer}");
    let commit = commit_tree(directory, &tree, &parents, message, source.occurred_at_ms).await?;
    let zero = "0000000000000000000000000000000000000000";
    let mut transaction = format!(
        "start\nupdate refs/heads/aruna {commit} {}\n",
        previous.as_deref().unwrap_or(zero)
    );
    match main.as_deref() {
        None => transaction.push_str(&format!("update refs/heads/main {commit} {zero}\n")),
        Some(main) if previous.as_deref() == Some(main) => {
            transaction.push_str(&format!("update refs/heads/main {commit} {main}\n"))
        }
        Some(main) => {
            if let Some(tree) = reconcile(directory, main, &files).await? {
                let message = format!("Merge Aruna metadata into main{trailer}");
                let merged = commit_tree(
                    directory,
                    &tree,
                    &[main, &commit],
                    message,
                    source.occurred_at_ms,
                )
                .await?;
                transaction.push_str(&format!("update refs/heads/main {merged} {main}\n"));
            }
        }
    }
    transaction.push_str("prepare\ncommit\n");
    let mut process = Command::new("git");
    process
        .current_dir(directory)
        .args(["update-ref", "--stdin"]);
    exchange(process, transaction.into(), false).await?;
    Ok(GitStatus {
        event_id: source.event_id,
        commit: Some(commit),
        error: None,
    })
}
