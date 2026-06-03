use std::sync::Arc;

use aruna_core::task::TaskKey;
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use tracing::error;

use crate::announce_realm_presence::{AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation};
use crate::driver::{DriverContext, drive};
use crate::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use crate::task_persistence::{delete_persisted_timer, restore_persisted_task_timers};

#[derive(Debug)]
struct OperationsTaskHandler {
    context: Arc<DriverContext>,
}

impl OperationsTaskHandler {
    fn new(context: Arc<DriverContext>) -> Self {
        Self { context }
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
                }
            }
            TaskKey::SyncPlacements { realm_id, node_id } => {
                let op = ProcessPlacementsOperation::new(PlacementConfig {
                    realm_id,
                    local_node_id: node_id,
                });
                if let Err(err) = drive(op, self.context.as_ref()).await {
                    error!(error = ?err, "Failed to process pending sync placements timer event");
                }
            }
        }
    }
}
