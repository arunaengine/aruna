use std::sync::Arc;

use aruna_core::document::DocumentSyncOutboxEvent;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::{IrokleEffect, IrokleEvent};
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use tracing::{error, warn};

use crate::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation, REALM_PRESENCE_REFRESH_AFTER,
};
use crate::document_sync_outbox::{
    delete_outbox_record, read_next_outbox_record, restore_document_sync_outbox_timers,
};
use crate::driver::{DriverContext, drive};
use crate::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use crate::sync_placement::{DOCUMENT_SYNC_RETRY_AFTER, SYNC_PLACEMENT_RETRY_AFTER};
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

    async fn reschedule_timer(&self, key: TaskKey, after: std::time::Duration) -> bool {
        let effect = TaskEffect::ResetTimer {
            key: key.clone(),
            after,
        };
        if let Err(message) = persist_task_effect(&self.context.storage_handle, &effect).await {
            warn!(key = ?key, message = %message, "Failed to persist timer re-arm");
            return false;
        }
        let Some(task_handle) = self.context.task_handle.as_ref() else {
            warn!(key = ?key, "Cannot re-arm failed timer without task handle");
            return false;
        };
        match task_handle.send_effect(Effect::Task(effect)).await {
            Event::Task(TaskEvent::TimerScheduled { .. }) => true,
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(key = ?key, message = %message, "Failed to re-arm failed timer");
                false
            }
            other => {
                warn!(key = ?key, event = ?other, "Unexpected timer re-arm result");
                false
            }
        }
    }

    async fn drain_document_sync_outbox(&self, prefix: Vec<u8>) {
        let retry_key = TaskKey::DrainDocumentSyncOutbox {
            prefix: prefix.clone(),
        };
        let (record_key, record, has_more) = match read_next_outbox_record(
            &self.context.storage_handle,
            &prefix,
        )
        .await
        {
            Ok(Some(record)) => record,
            Ok(None) => return,
            Err(error) => {
                warn!(prefix = ?prefix, error = %error, "Failed to read document sync outbox record");
                self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                    .await;
                return;
            }
        };

        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(key = ?retry_key, "Cannot drain document sync outbox without net handle");
            self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                .await;
            return;
        };

        let local_effect = match record.event.clone() {
            DocumentSyncOutboxEvent::Upsert { bytes } => IrokleEffect::PublishDocument {
                target: record.target.clone(),
                bytes,
                peers: record.peers.clone(),
            },
            DocumentSyncOutboxEvent::Delete => IrokleEffect::DeleteDocument {
                target: record.target.clone(),
                peers: record.peers.clone(),
            },
        };

        let event = net_handle
            .send_effect(Effect::Net(NetEffect::Irokle(local_effect)))
            .await;
        match event {
            Event::Net(NetEvent::Irokle(IrokleEvent::DocumentPublished { .. }))
            | Event::Net(NetEvent::Irokle(IrokleEvent::DocumentDeleted { .. })) => {}
            Event::Net(NetEvent::Irokle(IrokleEvent::Error { error, .. })) => {
                warn!(key = ?retry_key, error = %error, "Failed to create local document sync op");
                self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                    .await;
                return;
            }
            Event::Net(NetEvent::Error(error)) => {
                warn!(key = ?retry_key, error = ?error, "Failed to create local document sync op");
                self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                    .await;
                return;
            }
            other => {
                warn!(key = ?retry_key, event = ?other, "Unexpected local document sync op result");
                self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                    .await;
                return;
            }
        }

        let sync_key = TaskKey::SyncDocument {
            node_id: record.node_id,
            target: record.target,
            peers: record.peers,
        };
        if !self
            .reschedule_timer(sync_key, std::time::Duration::ZERO)
            .await
        {
            self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                .await;
            return;
        }

        if let Err(error) = delete_outbox_record(&self.context.storage_handle, &record_key).await {
            warn!(key = ?retry_key, error = %error, "Failed to delete document sync outbox record");
            self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                .await;
            return;
        }

        if has_more {
            self.reschedule_timer(retry_key, std::time::Duration::ZERO)
                .await;
        }
    }
}

pub async fn initialize_task_incoming(context: Arc<DriverContext>, task_handle: TaskHandle) {
    let handler_context = context.clone();
    task_handle
        .set_inbound_handler(Arc::new(OperationsTaskHandler::new(handler_context)))
        .await;
    restore_persisted_task_timers(&context.storage_handle, &task_handle).await;
    restore_document_sync_outbox_timers(&context.storage_handle, &task_handle).await;
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
            TaskKey::SyncDocument {
                node_id,
                target,
                peers,
            } => {
                let retry_key = TaskKey::SyncDocument {
                    node_id,
                    target: target.clone(),
                    peers: peers.clone(),
                };
                let Some(net_handle) = self.context.net_handle.as_ref() else {
                    warn!(key = ?retry_key, "Cannot sync document without net handle");
                    self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                        .await;
                    return;
                };
                let event = net_handle
                    .send_effect(Effect::Net(NetEffect::Irokle(IrokleEffect::SyncDocument {
                        target,
                        peers,
                    })))
                    .await;
                match event {
                    Event::Net(NetEvent::Irokle(IrokleEvent::DocumentsReconciled { .. })) => {}
                    Event::Net(NetEvent::Irokle(IrokleEvent::Error { error, .. })) => {
                        warn!(key = ?retry_key, error = %error, "Failed to process durable document sync timer event");
                        self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                            .await;
                    }
                    Event::Net(NetEvent::Error(error)) => {
                        warn!(key = ?retry_key, error = ?error, "Failed to process durable document sync timer event");
                        self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                            .await;
                    }
                    other => {
                        warn!(key = ?retry_key, event = ?other, "Unexpected durable document sync timer result");
                        self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                            .await;
                    }
                }
            }
            TaskKey::DrainDocumentSyncOutbox { prefix } => {
                self.drain_document_sync_outbox(prefix).await;
            }
        }
    }
}
