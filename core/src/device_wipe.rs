//! Holds the armed device wipe: the roots a user node erases after shutdown and its exit codes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;

use tokio::sync::watch;

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
    armed: watch::Sender<bool>,
}

impl DeviceWipe {
    /// `unsupported` names configured storage this process cannot erase, such
    /// as an object-storage backend a device was pointed at. A wipe that leaves
    /// any of it behind must never report a complete erasure.
    pub fn new(roots: Vec<PathBuf>, unsupported: Vec<String>) -> Self {
        Self {
            roots,
            unsupported,
            armed: watch::Sender::new(false),
        }
    }

    pub fn arm(&self) {
        self.armed.send_replace(true);
    }

    pub fn is_armed(&self) -> bool {
        *self.armed.borrow()
    }

    /// Resolves once the wipe is armed.
    pub async fn wait(&self) {
        let mut armed = self.armed.subscribe();
        // The sender lives as long as `self`, so the wait ends only once the wipe is armed.
        let _ = armed.wait_for(|armed| *armed).await;
    }

    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    /// Configured storage this process cannot erase, by backend name.
    pub fn unsupported(&self) -> &[String] {
        &self.unsupported
    }
}

#[cfg(test)]
mod tests {
    use super::DeviceWipe;

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

    // An armed wipe releases a waiter that started before it was armed.
    #[tokio::test]
    async fn wait_ends_armed() {
        let wipe = std::sync::Arc::new(DeviceWipe::new(Vec::new(), Vec::new()));
        let waiter = tokio::spawn({
            let wipe = wipe.clone();
            async move { wipe.wait().await }
        });
        wipe.arm();
        waiter.await.unwrap();
        assert!(wipe.is_armed());
    }
}
