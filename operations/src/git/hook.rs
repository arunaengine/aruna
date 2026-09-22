//! Validates received ARC structure and LFS availability before Git publishes refs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::git::command;
use aruna_core::git::LfsObject;
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
    for update in input.lines() {
        let parts: Vec<_> = update.split_whitespace().collect();
        if parts.len() != 3
            || !(parts[2].starts_with("refs/heads/") || parts[2].starts_with("refs/tags/"))
            || parts[2] == "refs/heads/aruna"
            || parts[2].starts_with("refs/heads/aruna/")
        {
            return Err(invalid());
        }
        if parts[1].bytes().all(|byte| byte == b'0') {
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
        revisions.insert(
            String::from_utf8(commit.to_vec())
                .map_err(|_| invalid())?
                .trim()
                .to_string(),
        );
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
            if converted.get("error").is_some() || converted.get("rocrate").is_none() {
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
    Ok(())
}
