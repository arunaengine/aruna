//! Runs the device's folder passes on the task loop: each folder's pass, binding or cleanup
//! holds that folder's pass scope for its whole run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::synced_folder::SyncedFolder;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_scheduler::TaskHandle;
use tracing::warn;

use aruna_ops_data::device::publish_queue::DrainOutcome;
use aruna_ops_data::device::sync::reconcile::ReconcilePlan;
use aruna_ops_data::device::sync::{ReconcileFolderError, folder_pass, sync_timers};
use aruna_ops_data::metadata::device::drain::publish_pending;
use aruna_ops_work::node::folder_timer::ReconcileFoldersOperation;
use aruna_runner::driver::{DriverContext, drive};

/// Reconciles one folder under its pass scope.
pub async fn reconcile_folder(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
) -> Result<ReconcilePlan, ReconcileFolderError> {
    drive(
        folder_pass(context.node_and_realm(), folder.clone()),
        context,
    )
    .await
}

/// Reconciles every active folder once, and finishes interrupted binds and unbinds.
pub async fn reconcile_folders(context: &Arc<DriverContext>) -> DrainOutcome {
    drive(
        ReconcileFoldersOperation::new(context.node_and_realm()),
        context,
    )
    .await
    .unwrap_or(DrainOutcome::Deferred)
}

/// Arms the device timers a starting node needs; a realm node with no folders schedules nothing.
pub async fn restore_sync_timers(context: &Arc<DriverContext>, task_handle: &TaskHandle) {
    let timers = drive(sync_timers(context.node_and_realm()), context)
        .await
        .unwrap_or_default();
    if timers.folders
        && let TaskEvent::Error { message, .. } = task_handle
            .schedule_idle_timer(TaskKey::ReconcileSyncedFolders, Duration::ZERO)
            .await
    {
        warn!(%message, "Failed to restore the synced-folder timer");
    }
    if timers.uploads
        && let TaskEvent::Error { message, .. } = task_handle
            .schedule_idle_timer(TaskKey::DrainUploadOutbox, Duration::ZERO)
            .await
    {
        warn!(%message, "Failed to restore the synced-folder upload timer");
    }
}

/// Re-arms the publish drain when the queue still holds entries.
pub(super) async fn restore_publish_timer(context: &DriverContext, task_handle: &TaskHandle) {
    match drive(publish_pending(), context).await {
        Ok(false) => {}
        Ok(true) => {
            let key = TaskKey::DrainDeviceIntake;
            let armed = task_handle.schedule_idle_timer(key, Duration::ZERO).await;
            if let TaskEvent::Error { message, .. } = armed {
                warn!(%message, "Failed to restore the device publish timer");
            }
        }
        Err(error) => warn!(%error, "Failed to scan the device publish queue"),
    }
}
