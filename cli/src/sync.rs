//! Plans a one-shot sync: which files differ by presence, size or BLAKE3 between two sides.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};
use unicode_normalization::UnicodeNormalization;

/// Which side is copied onto the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Upload,
    Download,
}

/// A local file, by its path relative to the synced folder with `/` separators.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFile {
    pub path: String,
    pub size: u64,
}

/// A remote object, by its key relative to the synced prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub path: String,
    pub size: u64,
    pub blake3: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Reason {
    Missing,
    Size,
    Checksum,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Transfer {
    pub path: String,
    pub size: u64,
    pub reason: Reason,
}

/// The work a sync does; `unverified` are same-size files the node has no BLAKE3 for.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Plan {
    pub transfers: Vec<Transfer>,
    pub same: usize,
    pub unverified: Vec<String>,
    pub extra: Vec<String>,
}

impl Plan {
    pub fn bytes(&self) -> u64 {
        self.transfers.iter().map(|transfer| transfer.size).sum()
    }
}

/// Compares both sides; `digest` hashes a local file and is called only for equal sizes.
pub fn plan(
    direction: Direction,
    local: &[LocalFile],
    remote: &[RemoteFile],
    mut digest: impl FnMut(&str) -> Result<String, CliError>,
) -> Result<Plan, CliError> {
    let local_map: BTreeMap<&str, &LocalFile> = local
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    let remote_map: BTreeMap<&str, &RemoteFile> = remote
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect();
    let mut result = Plan::default();
    let pairs: Vec<(&str, u64, Option<&LocalFile>, Option<&RemoteFile>)> = match direction {
        Direction::Upload => local
            .iter()
            .map(|file| {
                let other = remote_map.get(file.path.as_str()).copied();
                (file.path.as_str(), file.size, Some(file), other)
            })
            .collect(),
        Direction::Download => remote
            .iter()
            .map(|file| {
                let other = local_map.get(file.path.as_str()).copied();
                (file.path.as_str(), file.size, other, Some(file))
            })
            .collect(),
    };
    for (path, size, local_file, remote_file) in pairs {
        let reason = match (local_file, remote_file) {
            (Some(local_file), Some(remote_file)) if local_file.size != remote_file.size => {
                Some(Reason::Size)
            }
            (Some(_), Some(remote_file)) => match &remote_file.blake3 {
                None => {
                    result.unverified.push(path.to_string());
                    None
                }
                Some(expected) => match digest(path)?.eq_ignore_ascii_case(expected) {
                    true => None,
                    false => Some(Reason::Checksum),
                },
            },
            _ => Some(Reason::Missing),
        };
        match reason {
            Some(reason) => result.transfers.push(Transfer {
                path: path.to_string(),
                size,
                reason,
            }),
            None if remote_file.is_some_and(|file| file.blake3.is_some()) => result.same += 1,
            None => {}
        }
    }
    result.extra = match direction {
        Direction::Upload => remote
            .iter()
            .filter(|file| !local_map.contains_key(file.path.as_str()))
            .map(|file| file.path.clone())
            .collect(),
        Direction::Download => local
            .iter()
            .filter(|file| !remote_map.contains_key(file.path.as_str()))
            .map(|file| file.path.clone())
            .collect(),
    };
    Ok(result)
}

/// Every regular file below `root`, skipping partial downloads; symbolic links are not followed.
pub fn walk_local(root: &Path) -> Result<Vec<LocalFile>, CliError> {
    let mut files = Vec::new();
    let mut folders = vec![root.to_path_buf()];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            let path = entry.path();
            if kind.is_dir() {
                folders.push(path);
                continue;
            }
            if !kind.is_file() || path.to_string_lossy().ends_with(".aruna-part") {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|error| CliError::Io(std::io::Error::other(error.to_string())))?;
            let parts: Option<Vec<&str>> = relative
                .components()
                .map(|part| part.as_os_str().to_str())
                .collect();
            let parts = parts
                .ok_or_else(|| CliError::usage(format!("{} is not valid UTF-8", path.display())))?;
            files.push(LocalFile {
                path: parts.join("/"),
                size: entry.metadata()?.len(),
            });
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

/// Whether a remote key relative to a folder stays below it on every platform: no empty, `.` or
/// `..` part, no part ending in a period or space (Windows drops them), no backslash, no absolute
/// path, and on Windows no colon (drive or stream).
pub fn is_safe_key(relative: &str) -> bool {
    !relative.contains('\\')
        && !(cfg!(windows) && relative.contains(':'))
        && relative
            .split('/')
            .all(|part| !part.is_empty() && !part.ends_with(['.', ' ']))
        && Path::new(relative)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}

/// The path of a remote key below `root`; refuses unsafe keys and linked folders below the root.
pub fn local_path(root: &Path, relative: &str) -> Result<PathBuf, CliError> {
    let refuse = |reason: &str| {
        CliError::usage(format!(
            "{relative} cannot be saved below {}: {reason}",
            root.display()
        ))
    };
    if !is_safe_key(relative) {
        return Err(refuse("the key is not a plain relative path"));
    }
    let mut path = root.to_path_buf();
    let mut checking = true;
    let parts: Vec<&str> = relative.split('/').collect();
    for (index, part) in parts.iter().enumerate() {
        path.push(part);
        if !checking || index + 1 == parts.len() {
            continue;
        }
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if is_link(&meta) => return Err(refuse("a folder on the way is a link")),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => checking = false,
            Err(error) => return Err(error.into()),
        }
    }
    match path.starts_with(root) {
        true => Ok(path),
        false => Err(refuse("the path leaves the folder")),
    }
}

/// Symbolic links, and on Windows also junctions and other reparse points.
fn is_link(meta: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        // The Windows file attribute FILE_ATTRIBUTE_REPARSE_POINT.
        const REPARSE_POINT: u32 = 0x400;
        if meta.file_attributes() & REPARSE_POINT != 0 {
            return true;
        }
    }
    meta.file_type().is_symlink()
}

/// Paths that differ only in case or Unicode normalization and so overwrite each other on
/// case-insensitive or normalizing volumes such as NTFS and APFS.
pub fn case_clashes<'a>(paths: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let mut groups: BTreeMap<String, BTreeSet<&str>> = BTreeMap::new();
    for path in paths {
        // NTFS compares names by their uppercase form, so `Σ` and `ς` share one name.
        let folded = path.nfc().collect::<String>().to_uppercase();
        groups.entry(folded).or_default().insert(path);
    }
    groups
        .into_values()
        .filter(|group| group.len() > 1)
        .flatten()
        .collect()
}

#[cfg(test)]
#[path = "sync_tests.rs"]
mod tests;
