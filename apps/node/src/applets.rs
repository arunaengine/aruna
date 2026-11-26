//! Keeps the applets this node hosts: one upkeep pass every few minutes until shutdown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_clock::unix_timestamp_millis;
use aruna_ops_work::applets::upkeep::UpkeepOperation;
use aruna_ops_work::automations::upkeep::AutoUpkeepOperation;
use aruna_ops_work::workflows::runs::prune::PruneRunsOperation;
use aruna_runner::driver::{DriverContext, drive};
use aruna_scheduler::shutdown::Shutdown;
use tracing::{debug, warn};

use crate::plugin_host::serving_node;

/// How often a node keeps its applets.
const UPKEEP_INTERVAL: Duration = Duration::from_secs(300);

/// Runs an upkeep pass every few minutes; a node without a network keeps nothing.
pub fn spawn_upkeep(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(UPKEEP_INTERVAL) => {}
            }
            let Some((realm_id, node_id)) = serving_node(context.as_ref()) else {
                continue;
            };
            let now_ms = unix_timestamp_millis();
            let pass = UpkeepOperation::new((node_id, realm_id), now_ms);
            match drive(pass, context.as_ref()).await {
                Ok(report) => debug!(?report, "Kept the hosted applets"),
                Err(error) => warn!(%error, "Applet upkeep failed"),
            }
            match drive(PruneRunsOperation::new(now_ms), context.as_ref()).await {
                Ok(report) => debug!(?report, "Pruned expired workflow runs and cache"),
                Err(error) => warn!(%error, "Workflow run retention prune failed"),
            }
            match drive(
                AutoUpkeepOperation::new((node_id, realm_id), now_ms),
                context.as_ref(),
            )
            .await
            {
                Ok(report) => debug!(?report, "Pruned automation records and kept events"),
                Err(error) => warn!(%error, "Automation upkeep failed"),
            }
        }
    });
}
