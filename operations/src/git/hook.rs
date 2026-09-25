//! Validates received ARC and LFS content, then merges main into metadata before refs move.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::push::{PushRequest, encode};
use aruna_blob::git::command;
use aruna_core::git::{LfsObject, RefUpdate, ZERO_OID};
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashSet};
use std::path::Path;
use tokio::io::AsyncReadExt;

fn invalid() -> std::io::Error {
    std::io::Error::other("invalid ARC or missing LFS content")
}

pub async fn validate() -> std::io::Result<()> {
    let mut input = String::new();
    tokio::io::stdin()
        .take(65537)
        .read_to_string(&mut input)
        .await?;
    if input.len() > 65536 {
        return Err(invalid());
    }
    let directory = Path::new(".");
    let mut revisions = BTreeSet::new();
    let mut main = None;
    let mut updates = Vec::new();
    for update in input.lines() {
        let parts: Vec<_> = update.split_whitespace().collect();
        if parts.len() != 3
            || !(parts[2].starts_with("refs/heads/") || parts[2].starts_with("refs/tags/"))
            || parts[2] == "refs/heads/aruna"
            || parts[2].starts_with("refs/heads/aruna/")
        {
            return Err(invalid());
        }
        updates.push(RefUpdate {
            name: parts[2].to_string(),
            old: parts[0].to_string(),
            new: parts[1].to_string(),
        });
        if parts[1].bytes().all(|byte| byte == b'0') {
            if parts[2] == "refs/heads/main" {
                return Err(std::io::Error::other("the main branch cannot be deleted"));
            }
            continue;
        }
        if parts[2].starts_with("refs/tags/")
            && parts[0] != parts[1]
            && !parts[0].bytes().all(|byte| byte == b'0')
        {
            return Err(invalid());
        }
        let commit = command(
            directory,
            &["rev-parse", "--verify", &format!("{}^{{commit}}", parts[1])],
        )
        .await?;
        let commit = String::from_utf8(commit.to_vec())
            .map_err(|_| invalid())?
            .trim()
            .to_string();
        if parts[2] == "refs/heads/main" {
            main = Some((parts[0].to_string(), commit.clone()));
        }
        revisions.insert(commit);
        let commits = command(
            directory,
            &["rev-list", "--max-count=129", parts[1], "--not", "--all"],
        )
        .await?;
        revisions.extend(
            String::from_utf8(commits.to_vec())
                .map_err(|_| invalid())?
                .lines()
                .map(str::to_owned),
        );
        if revisions.len() > 128 {
            return Err(invalid());
        }
    }
    let arc = std::env::var("ARUNA_GIT_ARC").as_deref() == Ok("1");
    let mut objects = Vec::new();
    for revision in revisions {
        let entries = command(directory, &["ls-tree", "-rlz", &revision]).await?;
        let mut paths = HashSet::new();
        for entry in entries
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
        {
            if paths.len() >= 10_000 {
                return Err(invalid());
            }
            let entry = std::str::from_utf8(entry).map_err(|_| invalid())?;
            let (header, path) = entry.split_once('\t').ok_or_else(invalid)?;
            let fields: Vec<_> = header.split_whitespace().collect();
            if fields.len() != 4 || fields[1] != "blob" || !matches!(fields[0], "100644" | "100755")
            {
                return Err(invalid());
            }
            if path == ".lfsconfig" || path.split('/').any(|part| matches!(part, ".." | ".git")) {
                return Err(invalid());
            }
            paths.insert(path.to_string());
            let size: u64 = fields[3].parse().map_err(|_| invalid())?;
            if size <= 1024 {
                let bytes = command(directory, &["cat-file", "blob", fields[2]]).await?;
                if bytes.starts_with(b"version https://git-lfs.github.com/spec/") {
                    let text = std::str::from_utf8(&bytes).map_err(|_| invalid())?;
                    let lines: Vec<_> = text.lines().collect();
                    if lines.len() != 3 || lines[0] != "version https://git-lfs.github.com/spec/v1"
                    {
                        return Err(invalid());
                    }
                    let object = LfsObject {
                        oid: lines[1]
                            .strip_prefix("oid sha256:")
                            .ok_or_else(invalid)?
                            .to_string(),
                        size: lines[2]
                            .strip_prefix("size ")
                            .ok_or_else(invalid)?
                            .parse()
                            .map_err(|_| invalid())?,
                    };
                    if !object.valid() {
                        return Err(invalid());
                    }
                    objects.push(object);
                }
            }
        }
        if arc {
            let converted = aruna_blob::arc::export(directory, &revision).await?;
            if let Some(error) = converted["error"].as_str() {
                return Err(std::io::Error::other(error.to_string()));
            }
            if converted.get("rocrate").is_none() {
                return Err(invalid());
            }
            if !paths.contains("isa.investigation.xlsx") {
                return Err(invalid());
            }
            let workbook = command(
                directory,
                &["show", &format!("{revision}:isa.investigation.xlsx")],
            )
            .await?;
            if !workbook.starts_with(b"PK\x03\x04") {
                return Err(invalid());
            }
            for path in &paths {
                let parts: Vec<_> = path.split('/').collect();
                if parts.len() < 3 {
                    continue;
                }
                let metadata = match parts[0] {
                    "studies" => "isa.study.xlsx",
                    "assays" => "isa.assay.xlsx",
                    "workflows" => "workflow.cwl",
                    "runs" => "run.cwl",
                    _ => continue,
                };
                if !paths.contains(&format!("{}/{}/{metadata}", parts[0], parts[1])) {
                    return Err(invalid());
                }
            }
        }
    }
    let url = std::env::var("ARUNA_GIT_LFS_URL").map_err(|_| invalid())?;
    let token = std::env::var("ARUNA_GIT_TOKEN").map_err(|_| invalid())?;
    objects.sort_by(|left, right| left.oid.cmp(&right.oid));
    objects.dedup();
    for chunk in objects.chunks(100) {
        let body = serde_json::to_vec(&json!({"operation": "upload", "objects": chunk}))
            .map_err(std::io::Error::other)?;
        let bytes = aruna_blob::git::lfs_exchange(&url, &token, body).await?;
        let response: Value = serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
        let returned = response["objects"].as_array().ok_or_else(invalid)?;
        if returned.len() != chunk.len()
            || returned.iter().zip(chunk).any(|(value, expected)| {
                value.get("error").is_some()
                    || value.get("actions").is_some()
                    || value["oid"].as_str() != Some(&expected.oid)
                    || value["size"].as_u64() != Some(expected.size)
            })
        {
            return Err(invalid());
        }
    }
    current(directory, &updates).await?;
    let paths = changed(directory, &updates).await?;
    unlocked(&url, &paths, &token).await?;
    if arc && let Some((old, new)) = main {
        merge(directory, &old, &new, &token).await?;
    }
    publish(directory, updates, objects, paths, &token).await
}

/// Refuses updates whose old value no longer matches, before anything is published. A
/// replicated record can move a ref between the client's ref listing and its push.
async fn current(directory: &Path, updates: &[RefUpdate]) -> std::io::Result<()> {
    for update in updates {
        let arguments = ["rev-parse", "--verify", "--quiet", update.name.as_str()];
        let found = match command(directory, &arguments).await {
            Ok(oid) => String::from_utf8(oid.to_vec()).map_err(|_| invalid())?,
            Err(_) => ZERO_OID.to_string(),
        };
        if found.trim() != update.old {
            return Err(std::io::Error::other(format!(
                "{} moved on the server; fetch and push again",
                update.name
            )));
        }
    }
    Ok(())
}

/// Refuses early when another user locks a changed file, before metadata is merged.
async fn unlocked(lfs_url: &str, paths: &[String], token: &str) -> std::io::Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let url = lfs_url.replace("/objects/batch", "/locks/verify");
    let body = serde_json::to_vec(&json!({})).map_err(std::io::Error::other)?;
    let reply = aruna_blob::git::metadata_request(
        &url,
        token,
        Some(("application/vnd.git-lfs+json", body)),
    )
    .await?;
    let reply: Value = serde_json::from_slice(&reply)?;
    let paths: BTreeSet<&str> = paths.iter().map(String::as_str).collect();
    for lock in reply["theirs"].as_array().into_iter().flatten() {
        if let Some(path) = lock["path"].as_str()
            && paths.contains(path)
        {
            return Err(std::io::Error::other(format!(
                "{path} is locked by another user"
            )));
        }
    }
    Ok(())
}

/// Files that the updates change, for LFS lock checks.
async fn changed(directory: &Path, updates: &[RefUpdate]) -> std::io::Result<Vec<String>> {
    let mut paths = BTreeSet::new();
    for update in updates.iter().filter(|update| update.new != ZERO_OID) {
        let listing = if update.old == ZERO_OID {
            command(
                directory,
                &["ls-tree", "-r", "-z", "--name-only", &update.new],
            )
            .await?
        } else {
            let range = [update.old.as_str(), update.new.as_str()];
            let arguments = [
                "diff",
                "--name-only",
                "-z",
                "--no-renames",
                range[0],
                range[1],
            ];
            match command(directory, &arguments).await {
                Ok(listing) => listing,
                // Tags may name non-commit objects; they carry no file changes to check.
                Err(_) => continue,
            }
        };
        for path in listing
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
        {
            paths.insert(String::from_utf8(path.to_vec()).map_err(|_| invalid())?);
            if paths.len() > 100_000 {
                return Err(invalid());
            }
        }
    }
    Ok(paths.into_iter().collect())
}

/// Stores the pushed objects and publishes the ref updates as a replicated record. Git
/// moves the refs only after this succeeds; a refusal, such as a lock, rejects the push.
async fn publish(
    directory: &Path,
    updates: Vec<RefUpdate>,
    objects: Vec<LfsObject>,
    paths: Vec<String>,
    token: &str,
) -> std::io::Result<()> {
    let url = std::env::var("ARUNA_GIT_METADATA_URL").map_err(|_| invalid())?;
    let include: Vec<String> = updates
        .iter()
        .filter(|update| update.new != ZERO_OID)
        .map(|update| update.new.clone())
        .collect();
    let pack = if include.is_empty() {
        bytes::Bytes::new()
    } else {
        let existing: Vec<String> = aruna_blob::repo::refs(directory)
            .await?
            .into_values()
            .collect();
        aruna_blob::repo::pack(directory, &include, &existing).await?
    };
    let request = PushRequest {
        refs: updates,
        lfs: objects.into_iter().map(|object| object.oid).collect(),
        paths,
    };
    let body = encode(&request, &pack).map_err(std::io::Error::other)?;
    aruna_blob::git::metadata_request(
        &format!("{url}/git/push"),
        token,
        Some(("application/x-aruna-git-push", body)),
    )
    .await
    .map(|_| ())
}

/// Merges ISA and `aruna-metadata.json` edits on main into the document before refs move.
async fn merge(directory: &Path, old: &str, new: &str, token: &str) -> std::io::Result<()> {
    let url = std::env::var("ARUNA_GIT_METADATA_URL").map_err(|_| invalid())?;
    // Scaffolded documents have no raw revision until their first replacement.
    let current =
        match aruna_blob::git::metadata_request(&format!("{url}/rocrate?view=raw"), token, None)
            .await
        {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)?["raw"].take(),
            Err(_) => serde_json::from_slice::<Value>(
                &aruna_blob::git::metadata_request(&format!("{url}/rocrate"), token, None).await?,
            )?["rocrate"]
                .take(),
        };
    let old = (!old.bytes().all(|byte| byte == b'0')).then_some(old);
    let graph = serde_json::to_string(&current)?;
    let jsonld = match aruna_blob::arc::merge_metadata(directory, old, new, &graph).await? {
        Ok(Some(jsonld)) => jsonld,
        Ok(None) => return Ok(()),
        Err(error) => return Err(std::io::Error::other(error)),
    };
    let rocrate: Value = serde_json::from_str(&jsonld)?;
    let body = serde_json::to_vec(&json!({ "rocrate": rocrate }))?;
    aruna_blob::git::metadata_request(
        &format!("{url}/rocrate"),
        token,
        Some(("application/json", body)),
    )
    .await?;
    Ok(())
}
