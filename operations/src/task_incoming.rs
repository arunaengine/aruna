use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncPublish, DocumentSyncTarget};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::util::unix_timestamp_millis;
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use tracing::{error, info, warn};

use crate::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation, REALM_PRESENCE_REFRESH_AFTER,
};
use crate::document_sync_outbox::{
    OUTBOX_DRAIN_BATCH_SIZE, delete_outbox_records, read_outbox_records,
    restore_document_sync_outbox_timers,
};
use crate::driver::{DriverContext, drive};
use crate::metadata::materialization_queue::{
    METADATA_MATERIALIZATION_POLL_AFTER, METADATA_MATERIALIZATION_RETRY_AFTER,
    metadata_materialization_jobs_exist, process_metadata_materialization_batch,
    restore_metadata_materialization_timer,
};
use crate::metadata::projector::{
    METADATA_PROJECTION_RETRY_AFTER, drain_pending_metadata_projection_queue,
    project_metadata_create_events, project_metadata_create_events_from_log,
    replay_metadata_event_log, restore_pending_metadata_projection_timer,
};
use crate::metadata::prune_queue::{
    METADATA_GRAPH_PRUNE_POLL_AFTER, METADATA_GRAPH_PRUNE_RETRY_AFTER,
    metadata_graph_prune_jobs_exist, process_metadata_graph_prune_batch,
    process_metadata_graph_tombstones, restore_metadata_graph_prune_timer,
};
use crate::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use crate::queue_backoff::timer_retry_after_secs;
use crate::replication::queue::{
    BLOB_REPLICATION_RETRY_AFTER, process_blob_replication_batch, restore_blob_replication_timer,
};
use crate::s3::refresh_reference_metadata::{
    REFERENCE_METADATA_REFRESH_RETRY_AFTER, process_reference_metadata_refresh_batch,
    restore_reference_metadata_refresh_timer,
};
use crate::sync_placement::DOCUMENT_SYNC_RETRY_AFTER;
use crate::task_persistence::{
    delete_persisted_timer, persist_task_effect, restore_persisted_task_timers,
};

const DRAIN_SUBBATCH_RECORDS: usize = 512;
const DURABLE_QUEUE_REARM_AFTER: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct OperationsTaskHandler {
    context: Arc<DriverContext>,
    // In-memory retry-attempt counters keyed by timer. Loss on restart is fine:
    // a restarted node simply retries from the base interval.
    retry_backoff: std::sync::Mutex<HashMap<TaskKey, u32>>,
}

struct DrainSubBatch {
    peers: Vec<aruna_core::NodeId>,
    documents: Vec<DocumentSyncPublish>,
    targets: Vec<DocumentSyncTarget>,
    record_keys: Vec<Vec<u8>>,
}

#[derive(Default)]
struct DrainSyncOutcome {
    sync_elapsed: Duration,
    project_elapsed: Duration,
    delete_elapsed: Duration,
    retry_needed: bool,
}

fn document_publish_from_outbox(
    event_id: ulid::Ulid,
    target: DocumentSyncTarget,
    event: DocumentSyncOutboxEvent,
) -> DocumentSyncPublish {
    match event {
        DocumentSyncOutboxEvent::Upsert { bytes, change } => DocumentSyncPublish::Upsert {
            event_id,
            target,
            bytes,
            change,
        },
        DocumentSyncOutboxEvent::Delete { change } => DocumentSyncPublish::Delete {
            event_id,
            target,
            change,
        },
        DocumentSyncOutboxEvent::AdminOperation { event } => {
            DocumentSyncPublish::AdminOperation { target, event }
        }
    }
}

impl DrainSyncOutcome {
    fn merge(&mut self, other: DrainSyncOutcome) {
        self.sync_elapsed += other.sync_elapsed;
        self.project_elapsed += other.project_elapsed;
        self.delete_elapsed += other.delete_elapsed;
        self.retry_needed |= other.retry_needed;
    }
}

impl OperationsTaskHandler {
    fn new(context: Arc<DriverContext>) -> Self {
        Self {
            context,
            retry_backoff: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Backoff interval for the next re-arm of `key`, derived from the in-memory
    /// attempt count without mutating it.
    fn backoff_after(&self, key: &TaskKey) -> Duration {
        let attempts = self
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .get(key)
            .copied()
            .unwrap_or(0);
        Duration::from_secs(timer_retry_after_secs(attempts))
    }

    fn note_retry_backoff(&self, key: &TaskKey) {
        let mut backoff = self
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned");
        let attempts = backoff.entry(key.clone()).or_insert(0);
        *attempts = attempts.saturating_add(1);
    }

    fn reset_backoff(&self, key: &TaskKey) {
        self.retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .remove(key);
    }

    /// Re-arms `key` at its current backoff interval and records the attempt.
    async fn reschedule_with_backoff(&self, key: TaskKey) -> bool {
        let after = self.backoff_after(&key);
        self.note_retry_backoff(&key);
        self.reschedule_timer(key, after).await
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

    async fn drain_document_sync_outbox(&self) {
        let retry_key = TaskKey::DrainDocumentSyncOutbox;
        let drain_started = Instant::now();
        let batch =
            match read_outbox_records(&self.context.storage_handle, &[], OUTBOX_DRAIN_BATCH_SIZE)
                .await
            {
                Ok(batch) if batch.records.is_empty() => {
                    if batch.has_more {
                        self.reschedule_timer(retry_key, Duration::ZERO).await;
                    } else {
                        self.reset_backoff(&retry_key);
                    }
                    return;
                }
                Ok(batch) => batch,
                Err(error) => {
                    warn!(error = %error, "Failed to read document sync outbox record");
                    self.reschedule_with_backoff(retry_key).await;
                    return;
                }
            };
        let scan_elapsed = drain_started.elapsed();
        let record_count = batch.records.len();
        let oldest_record_ms = batch
            .records
            .iter()
            .map(|(_, record)| record.outbox_id.timestamp_ms())
            .min();

        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(key = ?retry_key, "Cannot drain document sync outbox without net handle");
            self.reschedule_with_backoff(retry_key).await;
            return;
        };

        let mut publish_groups: BTreeMap<Vec<aruna_core::NodeId>, Vec<DrainSubBatch>> =
            BTreeMap::new();
        for (record_key, record) in batch.records {
            let document =
                document_publish_from_outbox(record.outbox_id, record.target.clone(), record.event);

            let subbatches = publish_groups.entry(record.peers.clone()).or_default();
            if subbatches
                .last()
                .is_none_or(|subbatch| subbatch.documents.len() >= DRAIN_SUBBATCH_RECORDS)
            {
                subbatches.push(DrainSubBatch {
                    peers: record.peers,
                    documents: Vec::new(),
                    targets: Vec::new(),
                    record_keys: Vec::new(),
                });
            }
            let subbatch = subbatches.last_mut().expect("sub-batch was just pushed");
            subbatch.documents.push(document);
            subbatch.targets.push(record.target);
            subbatch.record_keys.push(record_key);
        }

        let group_count = publish_groups.len();
        let subbatches: Vec<DrainSubBatch> = publish_groups.into_values().flatten().collect();
        let subbatch_count = subbatches.len();

        // Two-slot pipeline: publish sub-batch N+1 while sub-batch N syncs;
        // sub-batches enter the sync stage strictly in submission order.
        let mut publish_elapsed = Duration::ZERO;
        let mut totals = DrainSyncOutcome::default();
        let mut awaiting_sync: Option<DrainSubBatch> = None;
        for mut subbatch in subbatches {
            let documents = std::mem::take(&mut subbatch.documents);
            let peers = subbatch.peers.clone();
            let publish = async {
                let publish_started = Instant::now();
                let event = net_handle
                    .send_effect(Effect::Net(NetEffect::DocumentSync(
                        DocumentSyncEffect::PublishDocuments { documents, peers },
                    )))
                    .await;
                (event, publish_started.elapsed())
            };
            let ((publish_event, publish_time), sync_outcome) = tokio::join!(
                publish,
                self.sync_drain_subbatch(&retry_key, net_handle, awaiting_sync.take())
            );
            publish_elapsed += publish_time;
            totals.merge(sync_outcome);
            match publish_event {
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsPublished {
                    ..
                })) => {
                    awaiting_sync = Some(subbatch);
                }
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                    error, ..
                })) => {
                    warn!(key = ?retry_key, error = %error, "Failed to create local document sync batch");
                    totals.retry_needed = true;
                }
                Event::Net(NetEvent::Error(error)) => {
                    warn!(key = ?retry_key, error = ?error, "Failed to create local document sync batch");
                    totals.retry_needed = true;
                }
                other => {
                    warn!(key = ?retry_key, event = ?other, "Unexpected local document sync batch result");
                    totals.retry_needed = true;
                }
            }
        }
        let sync_outcome = self
            .sync_drain_subbatch(&retry_key, net_handle, awaiting_sync.take())
            .await;
        totals.merge(sync_outcome);

        let oldest_age_ms = oldest_record_ms
            .map(|record_ms| unix_timestamp_millis().saturating_sub(record_ms))
            .unwrap_or(0);
        info!(
            event = "pipeline.drain.summary",
            records = record_count,
            groups = group_count,
            subbatches = subbatch_count,
            scan_ms = duration_ms(scan_elapsed),
            publish_ms = duration_ms(publish_elapsed),
            sync_ms = duration_ms(totals.sync_elapsed),
            project_ms = duration_ms(totals.project_elapsed),
            delete_ms = duration_ms(totals.delete_elapsed),
            total_ms = duration_ms(drain_started.elapsed()),
            oldest_age_ms,
            retry = totals.retry_needed,
            has_more = batch.has_more,
            "Document sync outbox drain summary"
        );

        if totals.retry_needed {
            self.reschedule_with_backoff(retry_key).await;
        } else if batch.has_more {
            self.reschedule_timer(retry_key, std::time::Duration::ZERO)
                .await;
        } else {
            self.reset_backoff(&retry_key);
        }
    }

    async fn sync_drain_subbatch(
        &self,
        retry_key: &TaskKey,
        net_handle: &aruna_net::NetHandle,
        subbatch: Option<DrainSubBatch>,
    ) -> DrainSyncOutcome {
        let mut outcome = DrainSyncOutcome::default();
        let Some(subbatch) = subbatch else {
            return outcome;
        };
        let sync_started = Instant::now();
        let event = net_handle
            .send_effect(Effect::Net(NetEffect::DocumentSync(
                DocumentSyncEffect::SyncDocuments {
                    targets: subbatch.targets,
                    peers: subbatch.peers,
                },
            )))
            .await;
        outcome.sync_elapsed = sync_started.elapsed();
        self.finish_sync_drain_subbatch(retry_key, subbatch.record_keys, event, outcome)
            .await
    }

    async fn finish_sync_drain_subbatch(
        &self,
        retry_key: &TaskKey,
        record_keys: Vec<Vec<u8>>,
        event: Event,
        mut outcome: DrainSyncOutcome,
    ) -> DrainSyncOutcome {
        match event {
            Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
                ..
            })) => {
                process_metadata_graph_tombstones(self.context.as_ref(), metadata_graph_tombstones)
                    .await;
                let project_started = Instant::now();
                let projected = self
                    .project_reconciled_metadata_create_events(
                        retry_key,
                        targets,
                        metadata_create_events,
                    )
                    .await;
                outcome.project_elapsed = project_started.elapsed();
                if projected.is_err() {
                    outcome.retry_needed = true;
                    return outcome;
                }
                let delete_started = Instant::now();
                let deleted =
                    delete_outbox_records(&self.context.storage_handle, record_keys).await;
                outcome.delete_elapsed = delete_started.elapsed();
                if let Err(error) = deleted {
                    warn!(key = ?retry_key, error = %error, "Failed to delete document sync outbox records");
                    outcome.retry_needed = true;
                }
            }
            Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error { error, .. })) => {
                warn!(key = ?retry_key, error = %error, "Failed to sync document batch");
                outcome.retry_needed = true;
            }
            Event::Net(NetEvent::Error(error)) => {
                warn!(key = ?retry_key, error = ?error, "Failed to sync document batch");
                outcome.retry_needed = true;
            }
            other => {
                warn!(key = ?retry_key, event = ?other, "Unexpected document sync batch result");
                outcome.retry_needed = true;
            }
        }
        outcome
    }

    async fn project_reconciled_metadata_create_events(
        &self,
        retry_key: &TaskKey,
        targets: Vec<DocumentSyncTarget>,
        metadata_create_events: Vec<aruna_core::metadata::MetadataCreateEventRecord>,
    ) -> Result<(), ()> {
        if !metadata_create_events.is_empty() {
            let local_node_id = self.context.net_handle.as_ref().map(|net| net.node_id());
            if let Err(error) =
                project_metadata_create_events(&self.context, metadata_create_events, local_node_id)
                    .await
            {
                warn!(key = ?retry_key, error = ?error, "Failed to project metadata create event batch after document sync");
                return Err(());
            }
            return Ok(());
        }

        let mut create_event_targets = Vec::new();
        for target in targets {
            let DocumentSyncTarget::MetadataCreateEvent {
                document_id,
                event_id,
                ..
            } = target
            else {
                continue;
            };
            create_event_targets.push((document_id, event_id));
        }
        if let Err(error) =
            project_metadata_create_events_from_log(&self.context, create_event_targets).await
        {
            warn!(key = ?retry_key, error = ?error, "Failed to project metadata create event batch from log after document sync");
            return Err(());
        }
        Ok(())
    }

    async fn drain_metadata_materialization_queue(&self) {
        match process_metadata_materialization_batch(&self.context).await {
            Ok(result) if result.has_more_due => {
                self.reschedule_timer(
                    TaskKey::DrainMetadataMaterializationQueue,
                    std::time::Duration::ZERO,
                )
                .await;
            }
            Ok(result) if result.next_due_after.is_some() => {
                self.reschedule_timer(
                    TaskKey::DrainMetadataMaterializationQueue,
                    result
                        .next_due_after
                        .unwrap_or(METADATA_MATERIALIZATION_POLL_AFTER),
                )
                .await;
            }
            Ok(_) => {
                match metadata_materialization_jobs_exist(&self.context.storage_handle).await {
                    Ok(false) => {}
                    Ok(true) => {
                        self.reschedule_timer(
                            TaskKey::DrainMetadataMaterializationQueue,
                            METADATA_MATERIALIZATION_POLL_AFTER,
                        )
                        .await;
                    }
                    Err(error) => {
                        warn!(error = ?error, "Failed to probe metadata materialization jobs");
                        self.reschedule_timer(
                            TaskKey::DrainMetadataMaterializationQueue,
                            METADATA_MATERIALIZATION_RETRY_AFTER,
                        )
                        .await;
                    }
                }
            }
            Err(error) => {
                warn!(error = ?error, "Failed to drain metadata materialization queue");
                self.reschedule_timer(
                    TaskKey::DrainMetadataMaterializationQueue,
                    METADATA_MATERIALIZATION_RETRY_AFTER,
                )
                .await;
            }
        }
    }

    async fn drain_metadata_graph_prune_queue(&self) {
        match process_metadata_graph_prune_batch(&self.context).await {
            Ok(result) if result.has_more_due => {
                self.reschedule_timer(
                    TaskKey::DrainMetadataGraphPruneQueue,
                    std::time::Duration::ZERO,
                )
                .await;
            }
            Ok(result) if result.next_due_after.is_some() => {
                self.reschedule_timer(
                    TaskKey::DrainMetadataGraphPruneQueue,
                    result
                        .next_due_after
                        .unwrap_or(METADATA_GRAPH_PRUNE_POLL_AFTER),
                )
                .await;
            }
            Ok(_) => match metadata_graph_prune_jobs_exist(&self.context.storage_handle).await {
                Ok(false) => {}
                Ok(true) => {
                    self.reschedule_timer(
                        TaskKey::DrainMetadataGraphPruneQueue,
                        METADATA_GRAPH_PRUNE_POLL_AFTER,
                    )
                    .await;
                }
                Err(error) => {
                    warn!(error = ?error, "Failed to probe metadata graph prune jobs");
                    self.reschedule_timer(
                        TaskKey::DrainMetadataGraphPruneQueue,
                        METADATA_GRAPH_PRUNE_RETRY_AFTER,
                    )
                    .await;
                }
            },
            Err(error) => {
                warn!(error = ?error, "Failed to drain metadata graph prune queue");
                self.reschedule_timer(
                    TaskKey::DrainMetadataGraphPruneQueue,
                    METADATA_GRAPH_PRUNE_RETRY_AFTER,
                )
                .await;
            }
        }
    }

    async fn drain_metadata_projection_queue(&self) {
        match drain_pending_metadata_projection_queue(&self.context).await {
            Ok(result) if result.has_more => {
                self.reschedule_timer(
                    TaskKey::DrainMetadataProjectionQueue,
                    std::time::Duration::ZERO,
                )
                .await;
            }
            Ok(result) if result.markers_examined == 0 => {
                if let Err(error) = replay_metadata_event_log(&self.context).await {
                    warn!(error = ?error, "Failed to replay metadata event log fallback");
                    self.reschedule_timer(
                        TaskKey::DrainMetadataProjectionQueue,
                        METADATA_PROJECTION_RETRY_AFTER,
                    )
                    .await;
                }
            }
            Ok(_) => {}
            Err(error) => {
                warn!(error = ?error, "Failed to drain metadata projection queue");
                self.reschedule_timer(
                    TaskKey::DrainMetadataProjectionQueue,
                    METADATA_PROJECTION_RETRY_AFTER,
                )
                .await;
            }
        }
    }

    async fn drain_blob_replication_queue(&self) {
        match process_blob_replication_batch(&self.context).await {
            Ok(result) if result.has_more_due => {
                self.reschedule_timer(TaskKey::DrainBlobReplicationQueue, Duration::ZERO)
                    .await;
            }
            Ok(result) => {
                if let Some(after) = result.next_due_after {
                    self.reschedule_timer(TaskKey::DrainBlobReplicationQueue, after)
                        .await;
                }
            }
            Err(error) => {
                warn!(error = ?error, "Failed to drain blob replication queue");
                self.reschedule_timer(
                    TaskKey::DrainBlobReplicationQueue,
                    BLOB_REPLICATION_RETRY_AFTER,
                )
                .await;
            }
        }
    }

    async fn drain_reference_metadata_refresh_queue(&self) {
        match process_reference_metadata_refresh_batch(&self.context).await {
            Ok(result) if result.has_more_due => {
                self.reschedule_timer(TaskKey::DrainReferenceMetadataRefreshQueue, Duration::ZERO)
                    .await;
            }
            Ok(result) => {
                if let Some(after) = result.next_due_after {
                    self.reschedule_timer(TaskKey::DrainReferenceMetadataRefreshQueue, after)
                        .await;
                }
            }
            Err(error) => {
                warn!(error = ?error, "Failed to drain reference metadata refresh queue");
                self.reschedule_timer(
                    TaskKey::DrainReferenceMetadataRefreshQueue,
                    REFERENCE_METADATA_REFRESH_RETRY_AFTER,
                )
                .await;
            }
        }
    }
}

fn spawn_durable_queue_rearm(context: &Arc<DriverContext>, task_handle: &TaskHandle) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    runtime.spawn(durable_queue_rearm_loop(
        Arc::downgrade(context),
        task_handle.clone(),
    ));
}

async fn durable_queue_rearm_loop(context: Weak<DriverContext>, task_handle: TaskHandle) {
    loop {
        tokio::time::sleep(DURABLE_QUEUE_REARM_AFTER).await;
        let Some(context) = context.upgrade() else {
            return;
        };
        restore_blob_replication_timer(&context.storage_handle, &task_handle).await;
        restore_reference_metadata_refresh_timer(&context.storage_handle, &task_handle).await;
        restore_document_sync_outbox_timers(&context.storage_handle, &task_handle).await;
        restore_pending_metadata_projection_timer(&context.storage_handle, &task_handle).await;
        restore_metadata_materialization_timer(&context.storage_handle, &task_handle).await;
        restore_metadata_graph_prune_timer(&context.storage_handle, &task_handle).await;
    }
}

pub async fn initialize_task_incoming(context: Arc<DriverContext>, task_handle: TaskHandle) {
    let handler_context = context.clone();
    task_handle
        .set_inbound_handler(Arc::new(OperationsTaskHandler::new(handler_context)))
        .await;
    crate::queue_lag::spawn_queue_lag_monitor(&context);
    spawn_durable_queue_rearm(&context, &task_handle);
    restore_persisted_task_timers(&context.storage_handle, &task_handle).await;
    restore_document_sync_outbox_timers(&context.storage_handle, &task_handle).await;
    restore_pending_metadata_projection_timer(&context.storage_handle, &task_handle).await;
    restore_metadata_materialization_timer(&context.storage_handle, &task_handle).await;
    restore_metadata_graph_prune_timer(&context.storage_handle, &task_handle).await;
    restore_blob_replication_timer(&context.storage_handle, &task_handle).await;
    restore_reference_metadata_refresh_timer(&context.storage_handle, &task_handle).await;
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
                let retry_key = TaskKey::SyncPlacements { realm_id, node_id };
                let op = ProcessPlacementsOperation::new(PlacementConfig {
                    realm_id,
                    local_node_id: node_id,
                    retry_after: self.backoff_after(&retry_key),
                });
                match drive(op, self.context.as_ref()).await {
                    // The sweep re-armed itself for still-pending placements: grow the
                    // backoff so a persistently unsatisfiable placement stops hot-looping.
                    Ok(true) => self.note_retry_backoff(&retry_key),
                    // A clean sweep clears the backoff for the next unrelated re-arm.
                    Ok(false) => self.reset_backoff(&retry_key),
                    Err(err) => {
                        error!(error = ?err, "Failed to process pending sync placements timer event");
                        self.reschedule_with_backoff(retry_key).await;
                    }
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
                    .send_effect(Effect::Net(NetEffect::DocumentSync(
                        DocumentSyncEffect::SyncDocument { target, peers },
                    )))
                    .await;
                match event {
                    Event::Net(NetEvent::DocumentSync(
                        DocumentSyncNetEvent::DocumentsReconciled {
                            targets,
                            metadata_create_events,
                            metadata_graph_tombstones,
                            ..
                        },
                    )) => {
                        process_metadata_graph_tombstones(
                            self.context.as_ref(),
                            metadata_graph_tombstones,
                        )
                        .await;
                        if self
                            .project_reconciled_metadata_create_events(
                                &retry_key,
                                targets,
                                metadata_create_events,
                            )
                            .await
                            .is_err()
                        {
                            self.reschedule_timer(retry_key, DOCUMENT_SYNC_RETRY_AFTER)
                                .await;
                            return;
                        }
                    }
                    Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                        error,
                        ..
                    })) => {
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
            TaskKey::DrainDocumentSyncOutbox => {
                self.drain_document_sync_outbox().await;
            }
            TaskKey::DrainMetadataProjectionQueue => {
                self.drain_metadata_projection_queue().await;
            }
            TaskKey::DrainMetadataMaterializationQueue => {
                self.drain_metadata_materialization_queue().await;
            }
            TaskKey::DrainMetadataGraphPruneQueue => {
                self.drain_metadata_graph_prune_queue().await;
            }
            TaskKey::DrainBlobReplicationQueue => {
                self.drain_blob_replication_queue().await;
            }
            TaskKey::DrainReferenceMetadataRefreshQueue => {
                self.drain_reference_metadata_refresh_queue().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document_sync_outbox::{
        outbox_key, read_outbox_record, restore_document_sync_outbox_timers, write_outbox_effect,
    };
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent,
        DocumentSyncOutboxRecord, DocumentSyncRevision,
    };
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::METADATA_GRAPH_PRUNE_JOB_KEYSPACE;
    use aruna_core::metadata::{MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord};
    use aruna_core::structs::RealmId;
    use aruna_storage::FjallStorage;
    use aruna_tasks::{InboundTaskHandler, TaskHandle};
    use async_trait::async_trait;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    struct RecordingTaskHandler {
        seen: mpsc::Sender<TaskKey>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingTaskHandler {
        async fn handle_timer(&self, key: TaskKey) {
            let _ = self.seen.send(key).await;
        }
    }

    fn node(seed: u8) -> aruna_core::NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn target() -> DocumentSyncTarget {
        DocumentSyncTarget::Group {
            group_id: Ulid::from_parts(7, 1),
        }
    }

    fn change() -> DocumentSyncChange {
        DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(8, 1),
                actor: node(1),
                updated_at_ms: 9,
            },
            kind: DocumentSyncChangeKind::Upsert,
        }
    }

    async fn read_graph_prune_jobs(
        storage: &aruna_storage::StorageHandle,
    ) -> Vec<MetadataGraphPruneJobRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| postcard::from_bytes(&value).expect("prune job decodes"))
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn write_outbox_record(
        storage: &aruna_storage::StorageHandle,
        record: &DocumentSyncOutboxRecord,
    ) {
        match storage
            .send_effect(write_outbox_effect(record).expect("outbox effect"))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected outbox write event: {other:?}"),
        }
    }

    #[test]
    fn outbox_upsert_maps_to_publish_with_revision() {
        let event_id = Ulid::from_parts(10, 1);
        let target = target();
        let change = change();
        let publish = document_publish_from_outbox(
            event_id,
            target.clone(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: vec![1, 2, 3],
                change,
            },
        );

        assert_eq!(publish.target(), &target);
        assert_eq!(publish.event_id(), event_id);
        assert!(matches!(
            publish,
            DocumentSyncPublish::Upsert { bytes, change: actual, .. }
                if bytes == vec![1, 2, 3] && actual == change
        ));
    }

    async fn restore_document_sync_outbox_timer_and_receive_key(
        storage: &aruna_storage::StorageHandle,
    ) -> TaskKey {
        let task_handle = TaskHandle::new();
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        task_handle
            .set_inbound_handler(Arc::new(RecordingTaskHandler { seen: seen_tx }))
            .await;

        restore_document_sync_outbox_timers(storage, &task_handle).await;

        tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
            .await
            .expect("restored drain timer should fire")
            .expect("recording handler should receive timer key")
    }

    #[tokio::test]
    async fn restore_document_sync_outbox_timers_schedules_drain_when_outbox_has_records() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let record = crate::document_sync_outbox::new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"restore durable work".to_vec(),
                change: change(),
            },
            false,
        );
        write_outbox_record(&storage, &record).await;

        let restored_key = restore_document_sync_outbox_timer_and_receive_key(&storage).await;

        assert_eq!(restored_key, TaskKey::DrainDocumentSyncOutbox);
    }

    #[tokio::test]
    async fn outbox_sync_error_retains_record_for_retry() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let handler = OperationsTaskHandler::new(context);
        let record = crate::document_sync_outbox::new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"retained work".to_vec(),
                change: change(),
            },
            false,
        );
        let key = outbox_key(&record).to_vec();

        write_outbox_record(&storage, &record).await;

        let outcome = handler
            .finish_sync_drain_subbatch(
                &TaskKey::DrainDocumentSyncOutbox,
                vec![key.clone()],
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                    target: Some(record.target.clone()),
                    error: "only 1/2 peers synced".to_string(),
                })),
                DrainSyncOutcome::default(),
            )
            .await;

        assert!(outcome.retry_needed);
        let retained = read_outbox_record(&storage, &key)
            .await
            .expect("outbox record reads");
        assert_eq!(retained, Some(record));
    }

    #[tokio::test]
    async fn retained_outbox_record_after_sync_failure_restores_drain_timer() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let handler = OperationsTaskHandler::new(context);
        let record = crate::document_sync_outbox::new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"retry after restart".to_vec(),
                change: change(),
            },
            false,
        );
        let key = outbox_key(&record).to_vec();
        write_outbox_record(&storage, &record).await;

        let outcome = handler
            .finish_sync_drain_subbatch(
                &TaskKey::DrainDocumentSyncOutbox,
                vec![key.clone()],
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                    target: Some(record.target.clone()),
                    error: "sync failed before all peers acknowledged".to_string(),
                })),
                DrainSyncOutcome::default(),
            )
            .await;
        assert!(outcome.retry_needed);
        assert_eq!(
            read_outbox_record(&storage, &key)
                .await
                .expect("outbox record reads"),
            Some(record)
        );

        let restored_key = restore_document_sync_outbox_timer_and_receive_key(&storage).await;
        assert_eq!(restored_key, TaskKey::DrainDocumentSyncOutbox);
    }

    #[tokio::test]
    async fn tombstones_are_processed_before_projection_retry_return() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let handler = OperationsTaskHandler::new(context);
        let document_id = Ulid::from_parts(17, 1);
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            "urn:graph:tombstone-before-retry".to_string(),
            RealmId::from_bytes([3; 32]),
            Ulid::from_parts(18, 1),
            document_id,
            19,
        );

        let outcome = handler
            .finish_sync_drain_subbatch(
                &TaskKey::DrainDocumentSyncOutbox,
                Vec::new(),
                Event::Net(NetEvent::DocumentSync(
                    DocumentSyncNetEvent::DocumentsReconciled {
                        applied: 1,
                        targets: vec![DocumentSyncTarget::MetadataCreateEvent {
                            document_id,
                            event_id: Ulid::from_parts(20, 1),
                        }],
                        metadata_create_events: Vec::new(),
                        metadata_graph_tombstones: vec![tombstone.clone()],
                    },
                )),
                DrainSyncOutcome::default(),
            )
            .await;

        assert!(outcome.retry_needed);
        let jobs = read_graph_prune_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].graph_iri, tombstone.graph_iri);
    }
}
