//! Keeps plugin artifact copies and install accounts right: reclaims abandoned copies, fetches
//! the published artifacts a management node misses and repairs the settled installs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::time::unix_timestamp_millis;
use aruna_tasks::shutdown::Shutdown;
use tracing::warn;

use super::{ready_artifact, serving_node};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::plugins::artifacts::{ArtifactError, RepairCopies};
use aruna_operations::plugins::install::InstallError;
use aruna_operations::plugins::reconcile::ReconcileInstalls;

/// How often a node checks its copies and repairs the installs.
const REPAIR_INTERVAL: Duration = Duration::from_secs(600);

/// Reclaims abandoned copies and, on a management node, fetches every published artifact it
/// misses. Answers how many it fetched.
pub(crate) async fn repair_artifacts(ctx: &DriverContext) -> Result<usize, ArtifactError> {
    let unavailable = || ArtifactError::Unavailable("realm config unavailable".into());
    let node = serving_node(ctx).ok_or_else(unavailable)?;
    let plan = drive(RepairCopies::new(node, unix_timestamp_millis()), ctx).await?;
    let mut copied = 0;
    for digest in &plan.missing {
        match ready_artifact(ctx, &plan.config, digest, Some(node.1)).await {
            Ok(_) => copied += 1,
            Err(error) => warn!(%digest, %error, "A published plugin artifact is missing here"),
        }
    }
    Ok(copied)
}

/// Repairs the settled installs on a management node; a node without a network does nothing.
pub(crate) async fn reconcile_installs(ctx: &DriverContext) -> Result<usize, InstallError> {
    let Some(node) = serving_node(ctx) else {
        return Ok(0);
    };
    drive(ReconcileInstalls::new(node, unix_timestamp_millis()), ctx).await
}

/// Runs both repairs every few minutes until shutdown.
pub fn spawn_repair(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(REPAIR_INTERVAL) => {}
            }
            if let Err(error) = repair_artifacts(context.as_ref()).await {
                warn!(%error, "Plugin artifact repair failed");
            }
            if let Err(error) = reconcile_installs(context.as_ref()).await {
                warn!(%error, "Plugin install repair failed");
            }
        }
    });
}
