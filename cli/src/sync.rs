//! Plans a one-shot sync: which files differ by presence, size or BLAKE3 between two sides.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

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

#[cfg(test)]
#[path = "sync_tests.rs"]
mod tests;
