//! Arms the device wipe and erases the storage roots once the node has shut down.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fs;
use std::path::{Path, PathBuf};

use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Exit status of a wiped node, so a supervisor tells an erased device apart
/// from a crash (1) and from an ordinary stop (0, 130, 143). It claims a
/// complete erasure and is used only when every root was emptied.
pub const WIPED_EXIT_CODE: i32 = 79;

/// Exit status when the wipe left paths behind. Data may still be on disk, so
/// this must never be reported as an erased device.
pub const INCOMPLETE_EXIT_CODE: i32 = 80;

/// The armed wipe and the roots it erases. The device surface arms it and the
/// process erases the roots after the ordinary shutdown sequence, because the
/// stores keep their files open until then.
#[derive(Debug)]
pub struct DeviceWipe {
    roots: Vec<PathBuf>,
    unsupported: Vec<String>,
    armed: CancellationToken,
}

impl DeviceWipe {
    /// `unsupported` names configured storage this process cannot erase, such
    /// as an object-storage backend a device was pointed at. A wipe that leaves
    /// any of it behind must never report a complete erasure.
    pub fn new(roots: Vec<PathBuf>, unsupported: Vec<String>) -> Self {
        Self {
            roots,
            unsupported,
            armed: CancellationToken::new(),
        }
    }

    pub fn arm(&self) {
        self.armed.cancel();
    }

    pub fn is_armed(&self) -> bool {
        self.armed.is_cancelled()
    }

    /// Resolves once the wipe is armed.
    pub async fn wait(&self) {
        self.armed.cancelled().await;
    }

    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    /// Configured storage this process cannot erase, by backend name.
    pub fn unsupported(&self) -> &[String] {
        &self.unsupported
    }
}

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
    use super::{DeviceWipe, purge};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn arms_once() {
        let wipe = DeviceWipe::new(
            vec![std::path::PathBuf::from("/tmp/aruna-wipe-test")],
            Vec::new(),
        );
        assert!(!wipe.is_armed());
        wipe.arm();
        assert!(wipe.is_armed());
        assert_eq!(wipe.roots().len(), 1);
        assert!(wipe.unsupported().is_empty());
    }

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
