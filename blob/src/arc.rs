//! Converts ISA metadata and records signed ARC snapshots through native Git plumbing.
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

pub async fn snapshot(directory: &Path, source: GitSnapshot) -> std::io::Result<GitStatus> {
    let previous = reference(directory, "refs/heads/aruna").await?;
    if let Some(previous) = &previous {
        let stored = command(directory, &["show", &format!("{previous}:.aruna/revision")]).await?;
        let event_id: ulid::Ulid = std::str::from_utf8(&stored)
            .map_err(std::io::Error::other)?
            .trim()
            .parse()
            .map_err(std::io::Error::other)?;
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
    if let Some(error) = conversion["error"].as_str() {
        return Ok(GitStatus {
            event_id: source.event_id,
            commit: previous,
            error: Some(error.into()),
        });
    }
    let mut files = conversion["files"]
        .as_object()
        .ok_or_else(|| std::io::Error::other("ARC conversion omitted files"))?
        .clone();
    let main = reference(directory, "refs/heads/main").await?;
    let mut modes = BTreeMap::new();
    let mut total: usize = files.values().filter_map(Value::as_str).map(str::len).sum();
    for path in conversion["required"].as_array().into_iter().flatten() {
        let path = path
            .as_str()
            .ok_or_else(|| std::io::Error::other("invalid ARC data path"))?;
        if files.contains_key(path) {
            continue;
        }
        let data = match &main {
            Some(main) => command(directory, &["show", &format!("{main}:{path}")])
                .await
                .ok(),
            None => None,
        };
        let Some(data) = data else {
            return Ok(GitStatus {
                event_id: source.event_id,
                commit: previous,
                error: Some(
                    "Referenced ARC data is missing; upload it through native Git/LFS".into(),
                ),
            });
        };
        let encoded = STANDARD.encode(data);
        total = total.saturating_add(encoded.len());
        if total > MAX_GIT_BYTES / 2 || files.len() >= 9999 {
            return Ok(GitStatus {
                event_id: source.event_id,
                commit: previous,
                error: Some("ARC Git files exceed limit; use LFS for large data".into()),
            });
        }
        files.insert(path.into(), json!(encoded));
        if let Some(main) = &main {
            let entry = command(
                directory,
                &["ls-tree", main, "--", &format!(":(literal){path}")],
            )
            .await?;
            let entry = std::str::from_utf8(&entry).map_err(std::io::Error::other)?;
            let mode = entry.split_whitespace().next().unwrap_or("");
            if !matches!(mode, "100644" | "100755") {
                return Err(std::io::Error::other("unsupported ARC data mode"));
            }
            modes.insert(path.to_string(), mode.to_string());
        }
    }
    files.insert(
        ".aruna/revision".into(),
        json!(STANDARD.encode(source.event_id.to_string())),
    );
    let temporary = tempfile::tempdir_in(directory)?;
    let index = temporary.path().join("index");
    for (path, content) in &files {
        let data = STANDARD
            .decode(
                content
                    .as_str()
                    .ok_or_else(|| std::io::Error::other("invalid ARC file"))?,
            )
            .map_err(std::io::Error::other)?;
        let mut process = Command::new("git");
        process
            .current_dir(directory)
            .args(["hash-object", "-w", "--stdin"]);
        let oid = exchange(process, data.into(), false).await?;
        let oid = std::str::from_utf8(&oid)
            .map_err(std::io::Error::other)?
            .trim();
        let mut process = Command::new("git");
        process
            .current_dir(directory)
            .env("GIT_INDEX_FILE", &index)
            .args([
                "update-index",
                "--add",
                "--cacheinfo",
                modes.get(path).map(String::as_str).unwrap_or("100644"),
                oid,
                path,
            ]);
        exchange(process, Bytes::new(), false).await?;
    }
    let mut process = Command::new("git");
    process
        .current_dir(directory)
        .env("GIT_INDEX_FILE", &index)
        .arg("write-tree");
    let tree = exchange(process, Bytes::new(), false).await?;
    let tree = std::str::from_utf8(&tree)
        .map_err(std::io::Error::other)?
        .trim();
    let mut process = Command::new("git");
    let date = format!("@{} +0000", source.occurred_at_ms / 1000);
    process
        .current_dir(directory)
        .args(["commit-tree", "-S", tree])
        .env("GIT_AUTHOR_NAME", "Aruna")
        .env("GIT_COMMITTER_NAME", "Aruna")
        .env("GIT_AUTHOR_EMAIL", "git@aruna.local")
        .env("GIT_COMMITTER_EMAIL", "git@aruna.local")
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date);
    if let Some(previous) = &previous {
        process.args(["-p", previous]);
    }
    let commit = exchange(
        process,
        Bytes::from_static(b"feat: capture Aruna metadata\n"),
        false,
    )
    .await?;
    let commit = std::str::from_utf8(&commit)
        .map_err(std::io::Error::other)?
        .trim()
        .to_string();
    let zero = "0000000000000000000000000000000000000000";
    let mut transaction = format!(
        "start\nupdate refs/heads/aruna {commit} {}\n",
        previous.as_deref().unwrap_or(zero)
    );
    if main.is_none() || main == previous {
        transaction.push_str(&format!(
            "update refs/heads/main {commit} {}\n",
            main.as_deref().unwrap_or(zero)
        ));
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
