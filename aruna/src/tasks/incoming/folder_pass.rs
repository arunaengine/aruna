//! Runs the device's folder passes on the task loop: each folder's pass, binding or cleanup
//! holds that folder's gate for its whole run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::{FolderState, SyncedFolder};
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_tasks::TaskHandle;
use tracing::warn;

use aruna_operations::device::drain::{DrainOutcome, publish_pending};
use aruna_operations::device::sync::folders::{finish_binding, finish_unbind, list_folders};
use aruna_operations::device::sync::reconcile::ReconcilePlan;
use aruna_operations::device::sync::{ReconcileFolderError, folder_gate, folder_pass, sync_timers};
use aruna_operations::driver::{DriverContext, drive};

/// Reconciles one folder under its gate.
pub async fn reconcile_folder(
    context: &Arc<DriverContext>,
    folder: &SyncedFolder,
) -> Result<ReconcilePlan, ReconcileFolderError> {
    let _pass = folder_gate(folder.folder_id).lock().await;
    drive(
        folder_pass(context.node_and_realm(), folder.clone()),
        context,
    )
    .await
}

/// Reconciles every active folder once, and finishes interrupted binds and unbinds.
pub async fn reconcile_folders(context: &Arc<DriverContext>) -> DrainOutcome {
    let Some(node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return DrainOutcome::Deferred;
    };
    let Ok(folders) = drive(list_folders(), context).await else {
        return DrainOutcome::Deferred;
    };
    let mut work = false;
    for folder in folders {
        let folder_id = folder.folder_id;
        match folder.state {
            // An unbind finishes here, also after a cancelled call or a restart.
            FolderState::Deleting => {
                let _pass = folder_gate(folder_id).lock().await;
                let unbind = finish_unbind(context.node_and_realm(), folder_id, node_id);
                if let Err(error) = drive(unbind, context).await {
                    warn!(folder = %folder_id, reason = %error, "Could not finish the unbind");
                }
            }
            // A bind cut short by a lost reply, a failed write or a restart finishes here.
            FolderState::Binding { .. } => {
                let _pass = folder_gate(folder_id).lock().await;
                match drive(finish_binding(folder_id), context).await {
                    Ok(_) => work = true,
                    Err(error) => {
                        warn!(folder = %folder_id, reason = %error, "Could not finish the folder binding");
                    }
                }
            }
            FolderState::Active => match reconcile_folder(context, &folder).await {
                Ok(plan) => work |= plan.uploads > 0 || plan.truncated,
                Err(error) => {
                    warn!(folder = %folder_id, reason = %error, "Could not reconcile the folder");
                }
            },
            _ => {}
        }
    }
    match work {
        true => DrainOutcome::Recheck,
        false => DrainOutcome::Idle,
    }
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
