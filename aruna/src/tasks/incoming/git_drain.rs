//! Applies pending Git merges at startup and every minute, then removes old Git objects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_tasks::shutdown::Shutdown;
use tracing::warn;

use aruna_operations::driver::{DriverContext, drive, node_routing};
use aruna_operations::git::GitNode;
use aruna_operations::git::cleanup::remove_objects;
use aruna_operations::git::pending::drain_merges;

const DRAIN_INTERVAL: Duration = Duration::from_secs(60);

/// Runs the drain, so a merge left by a restart or a failed attempt lands without waiting for
/// Git traffic on its document.
pub fn spawn_drain(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            let node = GitNode {
                ids: context.node_and_realm(),
                routing: node_routing(&context),
            };
            if let Err(error) = drive(drain_merges(&node), &context).await {
                warn!(%error, "Pending Git merges not drained");
            }
            if let Err(error) = drive(remove_objects(node.ids), &context).await {
                warn!(%error, "Old Git objects not removed");
            }
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(DRAIN_INTERVAL) => {}
            }
        }
    });
}
