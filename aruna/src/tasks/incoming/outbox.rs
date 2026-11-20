//! Drains the document sync outbox through the timer table's rotation run, and keeps the test
//! barrier that pins the first drain of a launch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

#[cfg(debug_assertions)]
impl OutboxBarrier {
    pub(super) fn new() -> Option<Self> {
        let marker = std::env::var("ARUNA_TEST_OUTBOX_BARRIER")
            .ok()
            .map(std::path::PathBuf::from)?;
        Self::arm(marker)
    }

    /// Pins only the first drain of a launch. The test releases it once, so a
    /// retry or refire drain after that must run instead of waiting again.
    pub(super) fn arm(marker: std::path::PathBuf) -> Option<Self> {
        use std::io::Write;
        let armed = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)
            .and_then(|mut file| file.write_all(b"active"));
        match armed {
            Ok(()) => Some(Self { marker }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
            Err(error) => {
                warn!(error = %error, "Failed to arm outbox test barrier");
                None
            }
        }
    }

    /// Holds the drain open until the test writes "release" into the marker
    /// file, so the test alone decides when this drain may finish.
    pub(super) async fn wait_start(&self) {
        loop {
            let released =
                std::fs::read_to_string(&self.marker).is_ok_and(|value| value.trim() == "release");
            if released {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl OperationsTaskHandler {
    /// Runs one bounded invocation of the open rotation, then continues, yields
    /// through the timer, or closes it. No record is ever deleted, truncated, or
    /// overwritten to satisfy a bound.
    pub(super) async fn drain_sync_outbox(&self) {
        self.run_trigger(TaskKey::DrainSyncOutbox).await;
    }

    /// The open outbox rotation the drain keeps between its invocations.
    pub(super) fn kept_rotation(&self) -> OutboxRotation {
        self.kept_memory(&TaskKey::DrainSyncOutbox).rotation
    }
}

impl OutboxDrainer {
    pub fn new(context: Arc<DriverContext>) -> Self {
        Self {
            handler: Arc::new(OperationsTaskHandler::new(context, JobsRuntime::new())),
        }
    }

    /// Runs one bounded invocation of the open rotation.
    pub async fn run_once(&self) {
        self.handler.drain_sync_outbox().await;
    }

    /// Records examined so far, and whether the cursor is parked mid-rotation.
    pub fn rotation_progress(&self) -> (usize, bool) {
        let rotation = self.handler.kept_rotation();
        (rotation.totals.examined, rotation.cursor.is_some())
    }
}
