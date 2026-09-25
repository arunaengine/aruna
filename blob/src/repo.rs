//! Maintains a local repository cache from replicated packs and ref states.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::git::{command, exchange};
use aruna_core::git::{CommitInfo, FileChange, FileChangeKind, Refs, ZERO_OID};
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

/// Git's empty tree, the base of a root commit's changes.
const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

/// Refuses revisions Git could read as options or that exceed a sane length.
pub fn revision_valid(revision: &str) -> bool {
    !revision.is_empty() && !revision.starts_with('-') && revision.len() <= 256
}

pub async fn resolve(directory: &Path, revision: &str) -> Option<String> {
    if !revision_valid(revision) {
        return None;
    }
    let spec = format!("{revision}^{{commit}}");
    let output = command(directory, &["rev-parse", "--verify", "--quiet", &spec])
        .await
        .ok()?;
    Some(text(&output).ok()?.trim().to_string())
}

pub async fn merge_base(directory: &Path, first: &str, second: &str) -> Option<String> {
    if !revision_valid(first) || !revision_valid(second) {
        return None;
    }
    let output = command(directory, &["merge-base", first, second])
        .await
        .ok()?;
    Some(text(&output).ok()?.trim().to_string())
}

/// Reads raw commit headers, so signatures are reported present, not verified.
pub async fn log(
    directory: &Path,
    revision: &str,
    skip: usize,
    limit: usize,
) -> std::io::Result<Vec<CommitInfo>> {
    if !revision_valid(revision) {
        return Err(std::io::Error::other("invalid Git revision"));
    }
    let arguments = [
        "rev-list".to_string(),
        "--header".into(),
        format!("--skip={skip}"),
        format!("--max-count={limit}"),
        revision.into(),
        "--".into(),
    ];
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    let output = command(directory, &arguments).await?;
    text(&output)?
        .split('\0')
        .filter(|entry| !entry.trim().is_empty())
        .map(parse_commit)
        .collect()
}

fn parse_commit(entry: &str) -> std::io::Result<CommitInfo> {
    let invalid = || std::io::Error::other("invalid commit listing");
    let (headers, body) = entry.split_once("\n\n").unwrap_or((entry, ""));
    let mut lines = headers.lines();
    let commit = lines.next().ok_or_else(invalid)?.trim().to_string();
    let mut info = CommitInfo {
        commit,
        parents: Vec::new(),
        author_name: String::new(),
        author_email: String::new(),
        authored_at_s: 0,
        message: String::new(),
        signed: false,
    };
    for line in lines {
        if let Some(parent) = line.strip_prefix("parent ") {
            info.parents.push(parent.to_string());
        } else if let Some(author) = line.strip_prefix("author ") {
            // "Name <email> seconds zone"; names may contain spaces, never "<".
            let (identity, time) = author.rsplit_once("> ").ok_or_else(invalid)?;
            let (name, email) = identity.split_once(" <").unwrap_or(("", identity));
            info.author_name = name.to_string();
            info.author_email = email.trim_start_matches('<').to_string();
            let seconds = time.split_whitespace().next().ok_or_else(invalid)?;
            info.authored_at_s = seconds.parse().map_err(|_| invalid())?;
        } else if line.starts_with("gpgsig ") || line.starts_with("gpgsig-sha256 ") {
            info.signed = true;
        }
    }
    info.message = body
        .lines()
        .map(|line| line.strip_prefix("    ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string();
    Ok(info)
}

pub async fn diff(
    directory: &Path,
    from: Option<&str>,
    to: &str,
) -> std::io::Result<Vec<FileChange>> {
    let from = from.unwrap_or(EMPTY_TREE);
    if !revision_valid(from) || !revision_valid(to) {
        return Err(std::io::Error::other("invalid Git revision"));
    }
    let arguments = [
        "diff-tree",
        "-r",
        "-z",
        "--no-renames",
        "--name-status",
        from,
        to,
    ];
    let output = command(directory, &arguments).await?;
    let mut fields = output
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty());
    let mut changes = Vec::new();
    while let (Some(status), Some(path)) = (fields.next(), fields.next()) {
        let change = match status {
            b"A" => FileChangeKind::Added,
            b"D" => FileChangeKind::Deleted,
            _ => FileChangeKind::Modified,
        };
        changes.push(FileChange {
            path: text(path)?.to_string(),
            change,
        });
        if changes.len() > 100_000 {
            return Err(std::io::Error::other("too many changed files"));
        }
    }
    Ok(changes)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn git(directory: &Path, args: &[&str]) -> String {
        let mut process = Command::new("git");
        process
            .current_dir(directory)
            .args([
                "-c",
                "user.name=Ada Lovelace",
                "-c",
                "user.email=ada@example.org",
            ])
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            // The host's global hooks and signing settings must not apply here.
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_DATE", "@1700000000 +0200")
            .env("GIT_COMMITTER_DATE", "@1700000000 +0200");
        let output = exchange(process, Bytes::new(), false)
            .await
            .expect("git runs");
        String::from_utf8(output.to_vec())
            .expect("utf-8")
            .trim()
            .to_string()
    }

    #[tokio::test]
    async fn history_reads() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path();
        git(path, &["init", "-q", "--initial-branch=main"]).await;
        tokio::fs::write(path.join("a.txt"), "one\n")
            .await
            .expect("write");
        git(path, &["add", "a.txt"]).await;
        git(path, &["commit", "-q", "-m", "First"]).await;
        let first = git(path, &["rev-parse", "HEAD"]).await;
        tokio::fs::write(path.join("b.txt"), "two\n")
            .await
            .expect("write");
        tokio::fs::remove_file(path.join("a.txt"))
            .await
            .expect("remove");
        git(path, &["add", "-A"]).await;
        let body = "Second\n\nLonger text.\n\nAruna-User: someone";
        git(path, &["commit", "-q", "-m", body]).await;
        let second = git(path, &["rev-parse", "HEAD"]).await;

        let commits = log(path, "main", 0, 10).await.expect("log reads");
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].commit, second);
        assert_eq!(commits[0].parents, vec![first.clone()]);
        assert_eq!(commits[0].author_name, "Ada Lovelace");
        assert_eq!(commits[0].author_email, "ada@example.org");
        assert_eq!(commits[0].authored_at_s, 1_700_000_000);
        assert_eq!(commits[0].message, body);
        assert!(!commits[0].signed);
        assert!(commits[1].parents.is_empty());
        assert_eq!(log(path, "main", 1, 10).await.expect("skips").len(), 1);

        let changes = diff(path, Some(&first), &second).await.expect("diff reads");
        let summary: Vec<_> = changes
            .iter()
            .map(|change| (change.path.as_str(), change.change))
            .collect();
        assert_eq!(
            summary,
            vec![
                ("a.txt", FileChangeKind::Deleted),
                ("b.txt", FileChangeKind::Added)
            ]
        );
        let root = diff(path, None, &first).await.expect("root diff reads");
        assert_eq!(root.len(), 1);
        assert_eq!(resolve(path, "main").await, Some(second.clone()));
        assert_eq!(resolve(path, "missing").await, None);
        assert_eq!(resolve(path, "--all").await, None);
        assert_eq!(merge_base(path, &first, &second).await, Some(first));
    }
}
