//! Reads plain RO-Crate commits, checks their pushes and builds their snapshot files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::arc::{Files, Pointers};
use crate::git::{command, exchange};
use aruna_core::crate_merge;
use aruna_core::git::{LfsObject, LinkedObject};
use aruna_core::repo_layout::{
    self, CRATE_FILE, CrateError, DataFile, INVESTIGATION, Layout, control_file, data_path,
};
use aruna_core::structs::storage::dataset_location::DatasetLocation;
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use tokio::process::Command;

const POINTER: &[u8] = b"version https://git-lfs.github.com/spec/v1\n";

fn invalid() -> std::io::Error {
    std::io::Error::other("invalid Git tree")
}

/// The layout of a commit: an ARC when its root holds the ISA investigation.
pub async fn layout(directory: &Path, revision: &str) -> std::io::Result<Layout> {
    let arguments = [
        "ls-tree",
        "-z",
        "--name-only",
        revision,
        "--",
        INVESTIGATION,
    ];
    let listing = command(directory, &arguments).await?;
    let names = listing.split(|byte| *byte == 0);
    Ok(Layout::detect(
        names.filter_map(|name| std::str::from_utf8(name).ok()),
    ))
}

/// Contents of the given blobs, read in one `git cat-file --batch` call.
async fn blobs(directory: &Path, oids: &[&str]) -> std::io::Result<BTreeMap<String, Bytes>> {
    if oids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut process = Command::new("git");
    process.current_dir(directory).args(["cat-file", "--batch"]);
    let output = exchange(process, format!("{}\n", oids.join("\n")).into(), false).await?;
    let mut contents = BTreeMap::new();
    let mut rest = output;
    while let Some(end) = rest.iter().position(|byte| *byte == b'\n') {
        let header = std::str::from_utf8(&rest[..end]).map_err(|_| invalid())?;
        let fields: Vec<_> = header.split(' ').collect();
        let [oid, "blob", size] = fields[..] else {
            return Err(invalid());
        };
        let size: usize = size.parse().map_err(|_| invalid())?;
        let body = rest.get(end + 1..end + 1 + size).ok_or_else(invalid)?;
        contents.insert(oid.to_owned(), Bytes::copy_from_slice(body));
        rest = rest.slice((end + 2 + size).min(rest.len())..);
    }
    Ok(contents)
}

/// Every file of a commit with its size; an LFS pointer reports the size of its content.
pub async fn data_files(directory: &Path, revision: &str) -> std::io::Result<Vec<DataFile>> {
    Ok(tree_files(directory, revision)
        .await?
        .into_iter()
        .map(|(path, size, pointer)| DataFile {
            size: pointer.map_or(size, |pointer| pointer.size),
            path,
        })
        .collect())
}

/// The LFS pointer files of a commit with the object each names.
pub async fn pointer_files(
    directory: &Path,
    revision: &str,
) -> std::io::Result<BTreeMap<String, LfsObject>> {
    Ok(tree_files(directory, revision)
        .await?
        .into_iter()
        .filter_map(|(path, _, pointer)| Some((path, pointer?)))
        .collect())
}

/// Every file of a commit with its Git size and the LFS object it points to, if any.
async fn tree_files(
    directory: &Path,
    revision: &str,
) -> std::io::Result<Vec<(String, u64, Option<LfsObject>)>> {
    let tree = command(directory, &["ls-tree", "-rlz", revision]).await?;
    let mut rows = Vec::new();
    for row in tree.split(|byte| *byte == 0).filter(|row| !row.is_empty()) {
        let row = std::str::from_utf8(row).map_err(|_| invalid())?;
        let (header, path) = row.split_once('\t').ok_or_else(invalid)?;
        let fields: Vec<_> = header.split_whitespace().collect();
        if let [_, "blob", oid, size] = fields[..] {
            rows.push((path, oid, size.parse::<u64>().map_err(|_| invalid())?));
        }
        if rows.len() > 10_000 {
            return Err(invalid());
        }
    }
    let small: Vec<&str> = rows
        .iter()
        .filter(|(_, _, size)| *size <= 1024)
        .map(|(_, oid, _)| *oid)
        .collect();
    let contents = blobs(directory, &small).await?;
    let pointed = |oid: &str| LfsObject::from_pointer(contents.get(oid)?);
    Ok(rows
        .iter()
        .map(|(path, oid, size)| ((*path).to_owned(), *size, pointed(oid)))
        .collect())
}

async fn crate_bytes(directory: &Path, revision: &str) -> Option<Bytes> {
    let file = format!("{revision}:{CRATE_FILE}");
    command(directory, &["show", &file]).await.ok()
}

/// The commit's RO-Crate with a `File` entity added for every file it does not describe.
pub async fn read(directory: &Path, revision: &str) -> std::io::Result<Result<Value, CrateError>> {
    let Some(bytes) = crate_bytes(directory, revision).await else {
        return Ok(Err(CrateError::Missing));
    };
    let mut value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(error) => return Ok(Err(error.into())),
    };
    repo_layout::add_files(&mut value, &data_files(directory, revision).await?);
    Ok(Ok(value))
}

/// The metadata of a plain commit in the shape ARC exports use.
pub async fn export(directory: &Path, commit: &str) -> std::io::Result<Value> {
    Ok(match read(directory, commit).await? {
        Ok(rocrate) => json!({"rocrate": rocrate, "commit": commit}),
        Err(error) => json!({"error": error.to_string(), "commit": commit}),
    })
}

fn refusal(revision: &str, error: CrateError) -> std::io::Error {
    let short = revision.get(..12).unwrap_or(revision);
    std::io::Error::other(format!("commit {short}: {error}"))
}

/// Refuses a plain commit without a valid `ro-crate-metadata.json` at its root.
pub async fn check(directory: &Path, revision: &str) -> std::io::Result<()> {
    let bytes = crate_bytes(directory, revision).await;
    repo_layout::validate(bytes.as_deref())
        .map(|_| ())
        .map_err(|error| refusal(revision, error))
}

/// Refuses a branch update to a plain commit that dropped the entity of a file it still has.
pub async fn kept(directory: &Path, old: &str, new: &str) -> std::io::Result<()> {
    if layout(directory, new).await? == Layout::Arc {
        return Ok(());
    }
    let parse = async |revision: &str| -> Option<Value> {
        serde_json::from_slice(&crate_bytes(directory, revision).await?).ok()
    };
    let (Some(before), Some(after)) = (parse(old).await, parse(new).await) else {
        return Ok(());
    };
    let arguments = ["ls-tree", "-r", "-z", "--name-only", new];
    let listing = command(directory, &arguments).await?;
    let paths = listing
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8(path.to_vec()).map_err(|_| invalid()))
        .collect::<std::io::Result<_>>()?;
    repo_layout::keeps_entities(&before, &after, &paths).map_err(|error| refusal(new, error))
}

/// Applies the RO-Crate changes from `old` (or nothing) to `new` onto the `graph` JSON-LD.
pub async fn merge_metadata(
    directory: &Path,
    (old, new): (Option<&str>, &str),
    graph: &str,
    location: Option<&DatasetLocation>,
) -> std::io::Result<Result<Option<String>, String>> {
    let new = match read(directory, new).await? {
        Ok(new) => new,
        Err(error) => return Ok(Err(error.to_string())),
    };
    let base = match old {
        Some(old) => read(directory, old).await?.ok(),
        None => None,
    };
    let merged = crate_merge::merge(graph, base.as_ref(), &new, location);
    Ok(merged.map_err(|error| error.to_string()))
}

/// Snapshot files of a plain RO-Crate: its metadata and an LFS pointer for each linked
/// object with a repository path. Entities of such objects name that path in the Git copy.
pub fn files(jsonld: &str, objects: &[LinkedObject]) -> std::io::Result<(Files, Pointers)> {
    let mut value: Value = serde_json::from_str(jsonld)?;
    let mut placed = BTreeMap::new();
    let mut paths = std::collections::BTreeSet::new();
    for linked in objects {
        let Some(path) = linked.path.clone().or_else(|| data_path(&linked.entity)) else {
            continue;
        };
        if control_file(&path) || !paths.insert(path.clone()) {
            continue;
        }
        placed.insert(linked.entity.clone(), (path, linked));
    }
    let renamed = placed
        .iter()
        .map(|(entity, (path, _))| (entity.clone(), path.clone()))
        .collect();
    repo_layout::git_copy(&mut value, &renamed);
    let text = repo_layout::metadata_text(&value);
    let mut files = Files::new();
    files.insert(CRATE_FILE.into(), ("100644".into(), text));
    let (mut pointers, mut attributes) = (Pointers::new(), String::new());
    for (path, linked) in placed.into_values() {
        let object = &linked.object;
        let mut pointer = POINTER.to_vec();
        pointer.extend(format!("oid sha256:{}\nsize {}\n", object.sha256, object.size).bytes());
        files.insert(path.clone(), ("100644".into(), pointer));
        let pattern = path.replace(' ', "[[:space:]]");
        attributes.push_str(&format!("/{pattern} filter=lfs diff=lfs merge=lfs -text\n"));
        pointers.insert(path);
    }
    if !attributes.is_empty() {
        files.insert(
            ".gitattributes".into(),
            ("100644".into(), attributes.into_bytes()),
        );
    }
    Ok((files, pointers))
}

#[cfg(test)]
#[path = "rocrate_tests.rs"]
mod tests;
