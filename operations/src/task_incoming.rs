use std::sync::Arc;

use aruna_core::task::TaskKey;
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use tracing::error;

use crate::announce_realm_presence::{AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation};
use crate::driver::{DriverContext, drive};

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
    task_handle
        .set_inbound_handler(Arc::new(OperationsTaskHandler::new(context)))
        .await;
}

#[async_trait]
impl InboundTaskHandler for OperationsTaskHandler {
    async fn handle_timer(&self, key: TaskKey) {
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
        }
    }
}
