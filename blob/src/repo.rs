//! Maintains a local repository cache from replicated packs and ref states.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::git::{command, exchange};
use aruna_core::git::{Refs, ZERO_OID};
use bytes::Bytes;
use std::collections::BTreeSet;
use std::path::Path;
use tokio::process::Command;

fn text(bytes: &[u8]) -> std::io::Result<&str> {
    std::str::from_utf8(bytes).map_err(std::io::Error::other)
}

/// SHA-256 digests of imported packs; the list lives in the cache and goes with it.
pub async fn imported(directory: &Path) -> std::io::Result<BTreeSet<String>> {
    match tokio::fs::read_to_string(directory.join("aruna-imported")).await {
        Ok(text) => Ok(text.lines().map(str::to_owned).collect()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
        Err(error) => Err(error),
    }
}

/// Imports a pack whose `digest` the caller verified, then remembers the digest.
pub async fn import(directory: &Path, digest: &str, pack: Bytes) -> std::io::Result<()> {
    let imported = directory.join("aruna-imported");
    let mut process = Command::new("git");
    process
        .current_dir(directory)
        .args(["index-pack", "--stdin", "--fix-thin"]);
    exchange(process, pack, false).await?;
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(imported)
        .await?;
    tokio::io::AsyncWriteExt::write_all(&mut file, format!("{digest}\n").as_bytes()).await?;
    file.sync_all().await
}

pub async fn refs(directory: &Path) -> std::io::Result<Refs> {
    let output = command(
        directory,
        &["for-each-ref", "--format=%(refname) %(objectname)"],
    )
    .await?;
    text(&output)?
        .lines()
        .map(|line| {
            line.split_once(' ')
                .map(|(name, oid)| (name.to_string(), oid.to_string()))
                .ok_or_else(|| std::io::Error::other("invalid ref listing"))
        })
        .collect()
}

pub async fn ancestry(directory: &Path, pairs: &[(String, String)]) -> Vec<bool> {
    let mut answers = Vec::with_capacity(pairs.len());
    for (ancestor, descendant) in pairs {
        let result = command(
            directory,
            &["merge-base", "--is-ancestor", ancestor, descendant],
        )
        .await;
        answers.push(result.is_ok());
    }
    answers
}

/// Moves refs from `expected` to `target` atomically; a concurrent change aborts all of them.
pub async fn set_refs(directory: &Path, expected: &Refs, target: &Refs) -> std::io::Result<()> {
    let mut transaction = String::from("start\n");
    let names: std::collections::BTreeSet<_> = expected.keys().chain(target.keys()).collect();
    for name in names {
        let (old, new) = (expected.get(name), target.get(name));
        if old == new {
            continue;
        }
        match new {
            Some(new) => transaction.push_str(&format!(
                "update {name} {new} {}\n",
                old.map_or(ZERO_OID, String::as_str)
            )),
            None => transaction.push_str(&format!(
                "delete {name} {}\n",
                old.map_or(ZERO_OID, String::as_str)
            )),
        }
    }
    if transaction == "start\n" {
        return Ok(());
    }
    transaction.push_str("prepare\ncommit\n");
    let mut process = Command::new("git");
    process
        .current_dir(directory)
        .args(["update-ref", "--stdin"]);
    exchange(process, transaction.into(), false)
        .await
        .map(|_| ())
}

/// Packs objects reachable from `include` and not from `exclude`, without thin deltas.
pub async fn pack(
    directory: &Path,
    include: &[String],
    exclude: &[String],
) -> std::io::Result<Bytes> {
    let mut input = include.join("\n");
    input.push_str("\n--not\n");
    input.push_str(&exclude.join("\n"));
    input.push('\n');
    let mut process = Command::new("git");
    process
        .current_dir(directory)
        .args(["pack-objects", "--revs", "--stdout", "-q"]);
    exchange(process, input.into(), false).await
}
