//! Erases the storage roots of a wiped device once the node has shut down.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

use tracing::warn;

/// Erases the contents of each root, keeping the roots themselves so a mounted
/// volume stays mounted. Answers with the entries it could not remove.
pub fn purge(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut failed = Vec::new();
    for root in roots {
        purge_root(root, &mut failed);
    }
    failed
}

fn purge_root(root: &Path, failed: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            warn!(error = %error, root = %root.display(), "Failed to read a wipe root");
            failed.push(root.to_path_buf());
            return;
        }
    };
    for entry in entries {
        let Ok(entry) = entry else {
            failed.push(root.to_path_buf());
            continue;
        };
        let path = entry.path();
        let removed = if path.is_dir() {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        };
        if let Err(error) = removed {
            warn!(error = %error, path = %path.display(), "Failed to erase a wiped path");
            failed.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::purge;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn erases_root_contents() {
        // The root survives so a mounted volume stays mounted.
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join("blobstore/objects")).unwrap();
        fs::write(root.path().join("blobstore/objects/one"), b"data").unwrap();
        fs::write(root.path().join("identity"), b"key").unwrap();
        assert!(purge(&[root.path().to_path_buf()]).is_empty());
        assert!(root.path().is_dir());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn ignores_missing_root() {
        let root = tempdir().unwrap();
        let missing = root.path().join("gone");
        assert!(purge(&[missing]).is_empty());
    }
}
