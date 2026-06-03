use std::sync::Arc;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use tracing::{error, warn};

use crate::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation, REALM_PRESENCE_REFRESH_AFTER,
};
use crate::driver::{DriverContext, drive};
use crate::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use crate::sync_placement::SYNC_PLACEMENT_RETRY_AFTER;
use crate::task_persistence::{
    delete_persisted_timer, persist_task_effect, restore_persisted_task_timers,
};

#[derive(Debug)]
struct OperationsTaskHandler {
    context: Arc<DriverContext>,
}

impl OperationsTaskHandler {
    fn new(context: Arc<DriverContext>) -> Self {
        Self { context }
    }

    async fn reschedule_timer(&self, key: TaskKey, after: std::time::Duration) {
        let effect = TaskEffect::ResetTimer {
            key: key.clone(),
            after,
        };
        persist_task_effect(&self.context.storage_handle, &effect).await;
        let Some(task_handle) = self.context.task_handle.as_ref() else {
            warn!(key = ?key, "Cannot re-arm failed timer without task handle");
            return;
        };
        match task_handle.send_effect(Effect::Task(effect)).await {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {}
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(key = ?key, message = %message, "Failed to re-arm failed timer")
            }
            other => warn!(key = ?key, event = ?other, "Unexpected timer re-arm result"),
        }
    }
}

pub async fn initialize_task_incoming(context: Arc<DriverContext>, task_handle: TaskHandle) {
    let handler_context = context.clone();
    task_handle
        .set_inbound_handler(Arc::new(OperationsTaskHandler::new(handler_context)))
        .await;
    restore_persisted_task_timers(&context.storage_handle, &task_handle).await;
}

#[async_trait]
impl InboundTaskHandler for OperationsTaskHandler {
    async fn handle_timer(&self, key: TaskKey) {
        delete_persisted_timer(&self.context.storage_handle, &key).await;
        match key {
            TaskKey::RealmPresence { realm_id, node_id } => {
                let op = AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                    realm_id,
                    node_id,
                    schedule_refresh: true,
                });
                if let Err(err) = drive(op, self.context.as_ref()).await {
                    error!(error = ?err, "Failed to process realm presence timer event");
                    self.reschedule_timer(
                        TaskKey::RealmPresence { realm_id, node_id },
                        REALM_PRESENCE_REFRESH_AFTER,
                    )
                    .await;
                }
            }
            TaskKey::SyncPlacements { realm_id, node_id } => {
                let op = ProcessPlacementsOperation::new(PlacementConfig {
                    realm_id,
                    local_node_id: node_id,
                });
                if let Err(err) = drive(op, self.context.as_ref()).await {
                    error!(error = ?err, "Failed to process pending sync placements timer event");
                    self.reschedule_timer(
                        TaskKey::SyncPlacements { realm_id, node_id },
                        SYNC_PLACEMENT_RETRY_AFTER,
                    )
                    .await;
                }
            }
        }
    }
}
