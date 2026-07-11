use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncPublish, DocumentSyncTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{NotificationRecord, RealmConfigDocument, RealmId};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::util::unix_timestamp_millis;
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use byteview::ByteView;
use tracing::{debug, error, info, warn};

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
use crate::notifications::client::deliver_remote;
use crate::notifications::inbox::upsert_inbox_records_reporting;
use crate::notifications::outbox::{
    NOTIFICATION_DELIVERY_RETRY_AFTER, NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE,
    NOTIFICATION_OUTBOX_RETENTION_MS, delete_notification_outbox_records,
    read_notification_outbox_batch, restore_notification_outbox_timer,
    restore_notification_outbox_timer_if_idle,
};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::prune::{
    NOTIFICATION_PRUNE_POLL_AFTER, NOTIFICATION_PRUNE_RETRY_AFTER,
    process_notification_prune_batch, restore_notification_prune_timer,
};
use crate::notifications::watch::interest::{
    WATCH_INTEREST_PUBLISH_DEBOUNCE, rebuild_watch_interest_table,
    refresh_watch_interest_for_targets, restore_watch_interest_publish_timer,
};
use crate::process_placements::process_shard_placements;
use crate::queue_backoff::timer_retry_after_secs;
use crate::replication::queue::{
    BLOB_REPLICATION_RETRY_AFTER, process_blob_replication_batch, restore_blob_replication_timer,
};
use crate::s3::refresh_reference_metadata::{
    REFERENCE_METADATA_REFRESH_RETRY_AFTER, process_reference_metadata_refresh_batch,
    restore_reference_metadata_refresh_timer,
};
use crate::sync_placement::DOCUMENT_SYNC_DEFER_RETRY_AFTER;
use crate::task_persistence::{
    delete_persisted_timer, persist_task_effect, restore_persisted_task_timers,
};
use crate::usage_stats::{
    refresh_realm_usage_summary_for_targets, restore_usage_snapshot_publish_timer,
};

const DRAIN_SUBBATCH_RECORDS: usize = 512;
const DURABLE_QUEUE_REARM_AFTER: Duration = Duration::from_secs(5);

/// One drained outbox record with its resolved publish topic.
type DrainRecord = (
    Vec<u8>,
    aruna_core::document::DocumentSyncOutboxRecord,
    irokle::TopicId,
);

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
    topics: Vec<irokle::TopicId>,
    targets: Vec<DocumentSyncTarget>,
    record_keys: Vec<Vec<u8>>,
}

impl DrainSubBatch {
    fn sync_subset(&self, indices: &[usize]) -> Option<Self> {
        let mut topics = Vec::with_capacity(indices.len());
        let mut targets = Vec::with_capacity(indices.len());
        let mut record_keys = Vec::with_capacity(indices.len());
        for &index in indices {
            topics.push(*self.topics.get(index)?);
            targets.push(self.targets.get(index)?.clone());
            record_keys.push(self.record_keys.get(index)?.clone());
        }
        Some(Self {
            peers: self.peers.clone(),
            documents: Vec::new(),
            topics,
            targets,
            record_keys,
        })
    }
}

#[derive(Default)]
struct DrainSyncOutcome {
    sync_elapsed: Duration,
    project_elapsed: Duration,
    delete_elapsed: Duration,
    retry_needed: bool,
}

/// Resolves the shard placement a drained record publishes under. A record
/// that already carries a real ref keeps it; a NIL ref (admin-operation
/// emitters leave one) is resolved from the realm config for the target. Shared
/// realm targets ignore the placement, so resolving them is harmless.
fn resolve_publish_placement(
    config: Option<&aruna_core::structs::RealmConfigDocument>,
    target: &DocumentSyncTarget,
    current: aruna_core::structs::PlacementRef,
) -> aruna_core::structs::PlacementRef {
    if current != aruna_core::structs::PlacementRef::NIL {
        return current;
    }
    match config {
        Some(config) => {
            crate::placement::placement_ref_for_target(config, target, Default::default())
        }
        None => aruna_core::structs::PlacementRef::NIL,
    }
}

async fn load_realm_config_for_drain(
    context: &Arc<DriverContext>,
    realm_id: aruna_core::structs::RealmId,
) -> Option<aruna_core::structs::RealmConfigDocument> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    match context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .and_then(|bytes| aruna_core::structs::RealmConfigDocument::from_bytes(&bytes).ok()),
        _ => None,
    }
}

fn document_publish_from_outbox(
    event_id: ulid::Ulid,
    target: DocumentSyncTarget,
    event: DocumentSyncOutboxEvent,
    placement: aruna_core::structs::PlacementRef,
    allow_genesis: bool,
) -> DocumentSyncPublish {
    match event {
        DocumentSyncOutboxEvent::Upsert { bytes, change } => DocumentSyncPublish::Upsert {
            event_id,
            target,
            bytes,
            change,
            allow_genesis,
        },
        DocumentSyncOutboxEvent::Delete { change } => DocumentSyncPublish::Delete {
            event_id,
            target,
            change,
            allow_genesis,
        },
        DocumentSyncOutboxEvent::AdminOperation { event } => DocumentSyncPublish::AdminOperation {
            target,
            event,
            placement,
            allow_genesis,
        },
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

/// Per-run defer state, carried across the drain's pages so a topic deferred on
/// one page keeps deferring on later pages (preserving FIFO within a topic) and
/// each topic's genesis presence is probed at most once per run.
#[derive(Default)]
struct DrainDeferState {
    topic_exists: HashMap<irokle::TopicId, bool>,
    deferred_topics: HashSet<irokle::TopicId>,
}

/// Splits FIFO-ordered drain records into those to publish now and a count of
/// those deferred because their shard topic has no local genesis yet. Each
/// topic's availability is evaluated once per run (state persists in `defer`);
/// once a topic defers, every later record of that topic defers too. Splitting
/// FIFO-adjacent records of one topic across a defer/publish boundary would let
/// the newer op publish first, invert its origin sequence on receivers, and drop
/// the older op forever as StaleOriginSequence — so a topic never straddles that
/// boundary within a run, across pages included.
fn partition_drain_records(
    records: Vec<DrainRecord>,
    defer: &mut DrainDeferState,
    mut topic_available: impl FnMut(irokle::TopicId) -> bool,
) -> (Vec<DrainRecord>, usize) {
    let mut to_publish = Vec::with_capacity(records.len());
    let mut deferred = 0usize;
    for (record_key, record, topic) in records {
        if record.target.uses_shard_topic() {
            let available = !defer.deferred_topics.contains(&topic)
                && *defer
                    .topic_exists
                    .entry(topic)
                    .or_insert_with(|| topic_available(topic));
            if !available {
                defer.deferred_topics.insert(topic);
                debug!(
                    event = "pipeline.drain.deferred",
                    target = ?record.target,
                    %topic,
                    "Deferring outbox record: shard topic genesis not yet known"
                );
                deferred += 1;
                continue;
            }
        }
        to_publish.push((record_key, record, topic));
    }
    (to_publish, deferred)
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

    // Kicks the placement reconciler immediately (not persisted; it is re-derived
    // from the realm config at startup by `restore_shard_subscriptions`).
    async fn schedule_sync_placements(&self, realm_id: RealmId, node_id: aruna_core::NodeId) {
        let Some(task_handle) = self.context.task_handle.as_ref() else {
            warn!("Cannot schedule shard placement sync without task handle");
            return;
        };
        let effect = Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::SyncPlacements { realm_id, node_id },
            after: Duration::ZERO,
        });
        if let Event::Task(TaskEvent::Error { message, .. }) = task_handle.send_effect(effect).await
        {
            warn!(message = %message, "Failed to schedule shard placement sync after local realm config change");
        }
    }

    async fn drain_document_sync_outbox(&self) {
        let retry_key = TaskKey::DrainDocumentSyncOutbox;
        let drain_started = Instant::now();

        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(key = ?retry_key, "Cannot drain document sync outbox without net handle");
            self.reschedule_with_backoff(retry_key).await;
            return;
        };

        let realm_id = *net_handle.realm_id();
        // Admin-operation records (group/user document ops) are stamped NIL by
        // their emitters; resolve their shard placement here from the realm
        // config so they publish onto the right shard topic. Records that
        // already carry a real ref (metadata upserts/deletes) keep it.
        let realm_config = load_realm_config_for_drain(&self.context, realm_id).await;

        let mut totals = DrainSyncOutcome::default();
        let mut defer_state = DrainDeferState::default();
        let mut realm_config_drained = false;
        let mut start_after: Option<Vec<u8>> = None;
        let mut scan_elapsed = Duration::ZERO;
        let mut publish_elapsed = Duration::ZERO;
        let mut record_count = 0usize;
        let mut deferred_total = 0usize;
        let mut group_count = 0usize;
        let mut subbatch_count = 0usize;
        let mut pages = 0usize;
        let mut oldest_record_ms: Option<u64> = None;
        let mut read_failed = false;

        // Drain the whole outbox in pages every run rather than only the FIFO
        // head: a page of permanently-deferred records (a missing shard genesis)
        // at the head must never starve records for other topics sitting behind
        // it. Deferred records are left in place but paged past; published
        // records are deleted, so the next run reads from the head again. The
        // per-topic defer state carries across pages so a topic deferred on one
        // page keeps deferring on the next (FIFO within a topic).
        loop {
            let scan_started = Instant::now();
            let batch = match read_outbox_records(
                &self.context.storage_handle,
                &[],
                start_after.clone(),
                OUTBOX_DRAIN_BATCH_SIZE,
            )
            .await
            {
                Ok(batch) => batch,
                Err(error) => {
                    warn!(error = %error, "Failed to read document sync outbox record");
                    read_failed = true;
                    break;
                }
            };
            scan_elapsed += scan_started.elapsed();
            if batch.records.is_empty() {
                break;
            }
            pages += 1;
            let has_more = batch.has_more;
            start_after = batch.records.last().map(|(key, _)| key.clone());
            record_count += batch.records.len();
            if let Some(page_oldest) = batch
                .records
                .iter()
                .map(|(_, record)| record.outbox_id.timestamp_ms())
                .min()
            {
                oldest_record_ms =
                    Some(oldest_record_ms.map_or(page_oldest, |current| current.min(page_oldest)));
            }

            let records: Vec<DrainRecord> = batch
                .records
                .into_iter()
                .map(|(record_key, mut record)| {
                    realm_config_drained |=
                        matches!(record.target, DocumentSyncTarget::RealmConfig { .. });
                    record.placement = resolve_publish_placement(
                        realm_config.as_ref(),
                        &record.target,
                        record.placement,
                    );
                    let topic = record.target.sync_topic_id(realm_id, &record.placement);
                    (record_key, record, topic)
                })
                .collect();

            // Shard topics are join-only for this node unless it is the shard's
            // rank-0 holder: a record whose topic has no local genesis yet cannot
            // publish. Try one bootstrap pass against the record's peers (falling
            // back to the shard's resolved holders when the record carries none,
            // as admin operations do), then defer whatever is still missing to a
            // short retry — the genesis arrives via gossip or the next pass. A
            // topic already deferred earlier this run is skipped: its records
            // stay deferred regardless, and re-probing would only waste RPCs.
            let mut missing_topics: BTreeMap<Vec<aruna_core::NodeId>, BTreeSet<irokle::TopicId>> =
                BTreeMap::new();
            for (_, record, topic) in &records {
                if !record.target.uses_shard_topic()
                    || defer_state.deferred_topics.contains(topic)
                    || net_handle
                        .document_sync_topic_exists(*topic)
                        .unwrap_or(false)
                {
                    continue;
                }
                let mut bootstrap_peers = record.peers.clone();
                if bootstrap_peers.is_empty()
                    && let Some(config) = realm_config.as_ref()
                {
                    bootstrap_peers =
                        crate::placement::resolve_shard_holders(config, &record.placement);
                    bootstrap_peers.retain(|peer| *peer != net_handle.node_id());
                    crate::sync_placement::sort_node_ids(&mut bootstrap_peers);
                }
                if bootstrap_peers.is_empty() {
                    continue;
                }
                missing_topics
                    .entry(bootstrap_peers)
                    .or_default()
                    .insert(*topic);
            }
            for (peers, topics) in missing_topics {
                let event = net_handle
                    .sync_document_topics(topics.into_iter().collect(), peers)
                    .await;
                let outcome = self
                    .finish_sync_drain_subbatch(
                        &retry_key,
                        Vec::new(),
                        Vec::new(),
                        Event::Net(NetEvent::DocumentSync(event)),
                        Default::default(),
                    )
                    .await;
                totals.merge(outcome);
            }

            let (to_publish, deferred) =
                partition_drain_records(records, &mut defer_state, |topic| {
                    net_handle
                        .document_sync_topic_exists(topic)
                        .unwrap_or(false)
                });
            deferred_total += deferred;

            let mut publish_groups: BTreeMap<Vec<aruna_core::NodeId>, Vec<DrainSubBatch>> =
                BTreeMap::new();
            for (record_key, record, topic) in to_publish {
                let document = document_publish_from_outbox(
                    record.outbox_id,
                    record.target.clone(),
                    record.event,
                    record.placement,
                    record.allow_genesis,
                );

                let subbatches = publish_groups.entry(record.peers.clone()).or_default();
                if subbatches
                    .last()
                    .is_none_or(|subbatch| subbatch.documents.len() >= DRAIN_SUBBATCH_RECORDS)
                {
                    subbatches.push(DrainSubBatch {
                        peers: record.peers,
                        documents: Vec::new(),
                        topics: Vec::new(),
                        targets: Vec::new(),
                        record_keys: Vec::new(),
                    });
                }
                let subbatch = subbatches.last_mut().expect("sub-batch was just pushed");
                subbatch.documents.push(document);
                subbatch.topics.push(topic);
                subbatch.targets.push(record.target);
                subbatch.record_keys.push(record_key);
            }

            group_count += publish_groups.len();
            let subbatches: Vec<DrainSubBatch> = publish_groups.into_values().flatten().collect();
            subbatch_count += subbatches.len();

            // Two-slot pipeline: publish sub-batch N+1 while sub-batch N syncs;
            // sub-batches enter the sync stage strictly in submission order.
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
                    Event::Net(NetEvent::DocumentSync(
                        DocumentSyncNetEvent::DocumentsPublished { .. },
                    )) => {
                        awaiting_sync = Some(subbatch);
                    }
                    Event::Net(NetEvent::DocumentSync(
                        DocumentSyncNetEvent::DocumentsPartiallyPublished {
                            published_indices,
                            retry_indices,
                            error,
                        },
                    )) => {
                        warn!(
                            key = ?retry_key,
                            published = published_indices.len(),
                            retry = retry_indices.len(),
                            error = %error,
                            "Partially created local document sync batch"
                        );
                        totals.retry_needed = true;
                        match subbatch.sync_subset(&published_indices) {
                            Some(published_subbatch)
                                if !published_subbatch.record_keys.is_empty() =>
                            {
                                awaiting_sync = Some(published_subbatch);
                            }
                            Some(_) => {}
                            None => {
                                warn!(key = ?retry_key, "Invalid partial document publish indices");
                            }
                        }
                    }
                    Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                        error,
                        ..
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

            if !has_more {
                break;
            }
        }

        // A locally-originated realm-config change (strategy upsert, node
        // placement, quota) must re-run the placement reconciler on this origin
        // so its rank-0 shard topic geneses are created without a restart; the
        // inbound reconcile path does the same for remote changes.
        if realm_config_drained {
            self.schedule_sync_placements(realm_id, net_handle.node_id())
                .await;
        }

        if record_count == 0 {
            if read_failed {
                self.reschedule_with_backoff(retry_key).await;
            } else {
                self.reset_backoff(&retry_key);
            }
            return;
        }

        let oldest_age_ms = oldest_record_ms
            .map(|record_ms| unix_timestamp_millis().saturating_sub(record_ms))
            .unwrap_or(0);
        info!(
            event = "pipeline.drain.summary",
            records = record_count,
            deferred = deferred_total,
            groups = group_count,
            subbatches = subbatch_count,
            pages,
            scan_ms = duration_ms(scan_elapsed),
            publish_ms = duration_ms(publish_elapsed),
            sync_ms = duration_ms(totals.sync_elapsed),
            project_ms = duration_ms(totals.project_elapsed),
            delete_ms = duration_ms(totals.delete_elapsed),
            total_ms = duration_ms(drain_started.elapsed()),
            oldest_age_ms,
            retry = totals.retry_needed,
            "Document sync outbox drain summary"
        );

        if totals.retry_needed || read_failed {
            self.reschedule_with_backoff(retry_key).await;
        } else if deferred_total > 0 {
            // Deferred records wait only for a genesis to arrive from the
            // shard's rank-0 holder; retry quickly rather than on the failure
            // backoff.
            self.reschedule_timer(retry_key, DOCUMENT_SYNC_DEFER_RETRY_AFTER)
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
        let requested_targets = subbatch.targets.clone();
        let sync_started = Instant::now();
        let event = net_handle
            .send_effect(Effect::Net(NetEffect::DocumentSync(
                DocumentSyncEffect::SyncDocuments {
                    topics: subbatch.topics,
                    peers: subbatch.peers,
                },
            )))
            .await;
        outcome.sync_elapsed = sync_started.elapsed();
        self.finish_sync_drain_subbatch(
            retry_key,
            subbatch.record_keys,
            requested_targets,
            event,
            outcome,
        )
        .await
    }

    async fn finish_sync_drain_subbatch(
        &self,
        retry_key: &TaskKey,
        record_keys: Vec<Vec<u8>>,
        requested_targets: Vec<DocumentSyncTarget>,
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
                let mut refresh_targets = targets.clone();
                refresh_targets.extend(requested_targets);
                if let Some(net_handle) = self.context.net_handle.as_ref() {
                    refresh_realm_usage_summary_for_targets(
                        self.context.as_ref(),
                        net_handle.node_id(),
                        &refresh_targets,
                    )
                    .await;
                }
                refresh_watch_interest_for_targets(self.context.as_ref(), &refresh_targets).await;
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

    async fn publish_usage_snapshots(&self) {
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!("Cannot publish usage snapshots without net handle");
            return;
        };
        let node_id = net_handle.node_id();
        let realm_id = *net_handle.realm_id();
        match crate::usage_stats::publish_and_refresh_usage_snapshots(
            &self.context,
            node_id,
            realm_id,
            false,
        )
        .await
        {
            Ok(_) => {}
            Err(error) => {
                warn!(error = %error, "Failed to publish usage snapshots");
                self.reschedule_timer(
                    TaskKey::PublishUsageSnapshots,
                    crate::usage_stats::USAGE_SNAPSHOT_PUBLISH_DEBOUNCE,
                )
                .await;
            }
        }
    }

    async fn publish_node_info(&self) {
        if let Some(net_handle) = self.context.net_handle.as_ref() {
            let node_id = net_handle.node_id();
            let realm_id = *net_handle.realm_id();
            if let Err(error) =
                crate::node_info::refresh_node_info_heartbeat(&self.context, node_id, realm_id)
                    .await
            {
                warn!(error = %error, "Failed to publish node info heartbeat");
            }
        } else {
            warn!("Cannot publish node info without net handle");
        }
        // Periodic heartbeat: always re-arm for the next interval regardless of
        // outcome so a transient failure never stops the heartbeat.
        self.reschedule_timer(
            TaskKey::PublishNodeInfo,
            crate::node_info::NODE_INFO_PUBLISH_INTERVAL,
        )
        .await;
    }

    async fn publish_watch_interest(&self) {
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!("Cannot publish watch interest without net handle");
            return;
        };
        let node_id = net_handle.node_id();
        match crate::notifications::watch::interest::publish_watch_interest(&self.context, node_id)
            .await
        {
            // Fold this node's freshly written digest into the origin-side cache;
            // the local write bypasses the reconcile path that refreshes remotes.
            Ok(true) => {
                let table = crate::notifications::watch::interest::rebuild_watch_interest_table(
                    &self.context.storage_handle,
                )
                .await;
                net_handle.replace_watch_interest(table);
            }
            Ok(false) => {}
            Err(error) => {
                warn!(error = %error, "Failed to publish watch interest");
                self.reschedule_timer(
                    TaskKey::PublishWatchInterest,
                    WATCH_INTEREST_PUBLISH_DEBOUNCE,
                )
                .await;
            }
        }
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

    async fn read_realm_config(&self, realm_id: RealmId) -> Option<RealmConfigDocument> {
        match self
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => match RealmConfigDocument::from_bytes(&bytes) {
                Ok(document) => Some(document),
                Err(error) => {
                    warn!(realm_id = %realm_id, error = %error, "Failed to decode realm config for notification drain");
                    None
                }
            },
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => None,
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(realm_id = %realm_id, error = %error, "Failed to read realm config for notification drain");
                None
            }
            other => {
                warn!(realm_id = %realm_id, event = ?other, "Unexpected realm config read result for notification drain");
                None
            }
        }
    }

    async fn drain_notification_outbox(&self) {
        let retry_key = TaskKey::DrainNotificationOutbox;

        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(key = ?retry_key, "Cannot drain notification outbox without net handle");
            self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                .await;
            return;
        };
        let local_node_id = net_handle.node_id();

        let snapshot_txn_id = match self
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: true })
            .await
        {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(error = %error, "Failed to start notification outbox snapshot");
                self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                    .await;
                return;
            }
            other => {
                warn!(event = ?other, "Unexpected notification outbox snapshot start result");
                self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                    .await;
                return;
            }
        };

        let mut start_after: Option<Vec<u8>> = None;
        let mut retry_needed = false;
        let mut realm_configs: HashMap<RealmId, Option<RealmConfigDocument>> = HashMap::new();
        // One delivery attempt per remote holder per run: later records for a
        // holder already found unreachable are marked retry without another RPC.
        let mut failed_holders: HashSet<aruna_core::NodeId> = HashSet::new();

        // Scan the snapshot in full so a dead holder cannot hide healthy records
        // behind it, while rows appended during this run wait for the next run.
        loop {
            let batch = match read_notification_outbox_batch(
                &self.context.storage_handle,
                start_after.clone(),
                NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE,
                Some(snapshot_txn_id),
            )
            .await
            {
                Ok(batch) => batch,
                Err(error) => {
                    warn!(error = %error, "Failed to read notification outbox record");
                    retry_needed = true;
                    break;
                }
            };

            let has_more = batch.has_more;
            start_after = batch.next_start_after;
            if batch.records.is_empty() {
                if has_more && start_after.is_some() {
                    continue;
                }
                break;
            }

            let mut local_records: Vec<NotificationRecord> = Vec::new();
            let mut local_keys: Vec<Vec<u8>> = Vec::new();
            let mut remote_groups: HashMap<
                aruna_core::NodeId,
                (Vec<NotificationRecord>, Vec<Vec<u8>>),
            > = HashMap::new();

            for (record_key, outbox_record) in batch.records {
                let age_ms =
                    unix_timestamp_millis().saturating_sub(outbox_record.outbox_id.timestamp_ms());
                if age_ms > NOTIFICATION_OUTBOX_RETENTION_MS {
                    warn!(outbox_id = %outbox_record.outbox_id, age_ms, "Dropping expired notification outbox record");
                    if let Err(error) = delete_notification_outbox_records(
                        &self.context.storage_handle,
                        vec![record_key],
                    )
                    .await
                    {
                        warn!(error = %error, "Failed to delete expired notification outbox record");
                        retry_needed = true;
                    }
                    continue;
                }

                let record = outbox_record.record;
                let realm_id = record.recipient.realm_id;
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    realm_configs.entry(realm_id)
                {
                    let config = self.read_realm_config(realm_id).await;
                    entry.insert(config);
                }
                let Some(config) = realm_configs.get(&realm_id).and_then(Option::as_ref) else {
                    warn!(realm_id = %realm_id, "Notification realm config unavailable; retrying delivery");
                    retry_needed = true;
                    continue;
                };

                let holder = match resolve_inbox_holder(&record.recipient, config) {
                    Ok(holder) => holder,
                    Err(error) => {
                        warn!(recipient = %record.recipient, error = %error, "Failed to resolve notification inbox holder");
                        retry_needed = true;
                        continue;
                    }
                };
                let Some(holder) = holder else {
                    warn!(recipient = %record.recipient, "No eligible notification inbox holder; retrying delivery");
                    retry_needed = true;
                    continue;
                };

                if holder == local_node_id {
                    local_records.push(record);
                    local_keys.push(record_key);
                } else if failed_holders.contains(&holder) {
                    retry_needed = true;
                } else {
                    let group = remote_groups.entry(holder).or_default();
                    group.0.push(record);
                    group.1.push(record_key);
                }
            }

            if !local_records.is_empty() {
                match upsert_inbox_records_reporting(&self.context.storage_handle, &local_records)
                    .await
                {
                    Ok(outcome) => {
                        for recipient in &outcome.recipients {
                            net_handle.notify_inbox_activity(*recipient);
                        }
                        if let Err(error) = delete_notification_outbox_records(
                            &self.context.storage_handle,
                            local_keys,
                        )
                        .await
                        {
                            warn!(error = %error, "Failed to delete delivered notification outbox records");
                            retry_needed = true;
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "Failed to deliver notifications to local inbox");
                        retry_needed = true;
                    }
                }
            }

            for (holder, (records, keys)) in remote_groups {
                match deliver_remote(net_handle, holder, records).await {
                    Ok(_) => {
                        if let Err(error) =
                            delete_notification_outbox_records(&self.context.storage_handle, keys)
                                .await
                        {
                            warn!(error = %error, "Failed to delete delivered notification outbox records");
                            retry_needed = true;
                        }
                    }
                    Err(error) => {
                        warn!(holder = %holder, error = %error, "Failed to deliver notifications to remote holder");
                        failed_holders.insert(holder);
                        retry_needed = true;
                    }
                }
            }

            if !has_more {
                break;
            }
        }

        match self
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction {
                txn_id: snapshot_txn_id,
            })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(error = %error, "Failed to close notification outbox snapshot");
                retry_needed = true;
            }
            other => {
                warn!(event = ?other, "Unexpected notification outbox snapshot close result");
                retry_needed = true;
            }
        }

        if retry_needed {
            self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                .await;
        } else {
            match read_notification_outbox_batch(&self.context.storage_handle, None, 1, None).await
            {
                Ok(batch) if !batch.records.is_empty() || batch.has_more => {
                    self.reschedule_timer(retry_key, Duration::ZERO).await;
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(error = %error, "Failed to check for notification outbox records appended during drain");
                    self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                        .await;
                }
            }
        }
    }

    async fn prune_notifications(&self) {
        let after = match process_notification_prune_batch(&self.context).await {
            Ok(outcome) if outcome.has_more => Duration::ZERO,
            Ok(outcome) => outcome
                .next_due_after
                .unwrap_or(NOTIFICATION_PRUNE_POLL_AFTER)
                .min(NOTIFICATION_PRUNE_POLL_AFTER),
            Err(error) => {
                warn!(error = %error, "Failed to prune notifications");
                NOTIFICATION_PRUNE_RETRY_AFTER
            }
        };
        self.reschedule_timer(TaskKey::PruneNotifications, after)
            .await;
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
        restore_usage_snapshot_publish_timer(&context.storage_handle, &task_handle).await;
        restore_watch_interest_publish_timer(&context.storage_handle, &task_handle).await;
        crate::node_info::restore_node_info_publish_timer(&context.storage_handle, &task_handle)
            .await;
        restore_notification_outbox_timer_if_idle(
            &context.storage_handle,
            &task_handle,
            NOTIFICATION_DELIVERY_RETRY_AFTER,
        )
        .await;
        restore_pending_metadata_projection_timer(&context.storage_handle, &task_handle).await;
        restore_metadata_materialization_timer(&context.storage_handle, &task_handle).await;
        restore_metadata_graph_prune_timer(&context.storage_handle, &task_handle).await;
        restore_notification_prune_timer(&context.storage_handle, &task_handle).await;
    }
}

pub async fn initialize_task_incoming(context: Arc<DriverContext>, task_handle: TaskHandle) {
    let handler_context = context.clone();
    task_handle
        .set_inbound_handler(Arc::new(OperationsTaskHandler::new(handler_context)))
        .await;
    // Prime the origin-side watch interest cache from any digests already in
    // local storage so matching works before the first reconcile.
    if let Some(net_handle) = context.net_handle.as_ref() {
        let table = rebuild_watch_interest_table(&context.storage_handle).await;
        net_handle.replace_watch_interest(table);
    }
    spawn_durable_queue_rearm(&context, &task_handle);
    restore_persisted_task_timers(&context.storage_handle, &task_handle).await;
    restore_document_sync_outbox_timers(&context.storage_handle, &task_handle).await;
    restore_usage_snapshot_publish_timer(&context.storage_handle, &task_handle).await;
    restore_watch_interest_publish_timer(&context.storage_handle, &task_handle).await;
    crate::node_info::restore_node_info_publish_timer(&context.storage_handle, &task_handle).await;
    restore_notification_outbox_timer(&context.storage_handle, &task_handle, Duration::ZERO).await;
    restore_pending_metadata_projection_timer(&context.storage_handle, &task_handle).await;
    restore_metadata_materialization_timer(&context.storage_handle, &task_handle).await;
    restore_metadata_graph_prune_timer(&context.storage_handle, &task_handle).await;
    restore_notification_prune_timer(&context.storage_handle, &task_handle).await;
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
                process_shard_placements(&self.context, realm_id, node_id).await;
            }
            TaskKey::DrainDocumentSyncOutbox => {
                self.drain_document_sync_outbox().await;
            }
            TaskKey::PublishUsageSnapshots => {
                self.publish_usage_snapshots().await;
            }
            TaskKey::PublishNodeInfo => {
                self.publish_node_info().await;
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
            TaskKey::DrainNotificationOutbox => {
                self.drain_notification_outbox().await;
            }
            TaskKey::PruneNotifications => {
                self.prune_notifications().await;
            }
            TaskKey::PublishWatchInterest => {
                self.publish_watch_interest().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document_sync_outbox::{
        new_outbox_record_with_id, outbox_key, read_outbox_record,
        restore_document_sync_outbox_timers, write_outbox_effect,
    };
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent,
        DocumentSyncOutboxRecord, DocumentSyncRevision,
    };
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{METADATA_GRAPH_PRUNE_JOB_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE};
    use aruna_core::metadata::{MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord};
    use aruna_core::storage_entries::notification_outbox_write_entry;
    use aruna_core::structs::{
        Actor, NotificationClass, NotificationKind, NotificationOutboxRecord, RealmConfigDocument,
        RealmId, RealmNodeKind,
    };
    use aruna_core::types::UserId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use aruna_tasks::{InboundTaskHandler, TaskHandle};
    use async_trait::async_trait;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    use crate::notifications::outbox::new_notification_outbox_record;

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
            placement: aruna_core::structs::PlacementRef::NIL,
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
        let record = new_outbox_record_with_id(
            event_id,
            node(1),
            target.clone(),
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: vec![1, 2, 3],
                change,
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        let publish = document_publish_from_outbox(&record);

        assert_eq!(publish.target(), &target);
        assert_eq!(publish.event_id(), event_id);
        assert!(publish.allow_genesis());
        assert!(matches!(
            publish,
            DocumentSyncPublish::Upsert { bytes, change: actual, .. }
                if bytes == vec![1, 2, 3] && actual == change
        ));
    }

    #[test]
    fn partial_publish_indices_select_exact_outbox_records() {
        let duplicate_target = target();
        let other_target = DocumentSyncTarget::Group {
            group_id: Ulid::from_parts(7, 2),
        };
        let subbatch = DrainSubBatch {
            peers: vec![node(2)],
            documents: Vec::new(),
            topics: vec![
                irokle::TopicId::hash(b"first"),
                irokle::TopicId::hash(b"second"),
                irokle::TopicId::hash(b"third"),
            ],
            targets: vec![
                duplicate_target.clone(),
                other_target,
                duplicate_target.clone(),
            ],
            record_keys: vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()],
        };

        let selected = subbatch
            .sync_subset(&[2])
            .expect("published index selects a record");

        assert_eq!(selected.targets, vec![duplicate_target]);
        assert_eq!(selected.topics, vec![irokle::TopicId::hash(b"third")]);
        assert_eq!(selected.record_keys, vec![b"third".to_vec()]);
        assert!(selected.documents.is_empty());
        assert!(subbatch.sync_subset(&[3]).is_none());
    }

    fn shard_topic_record(origin_seq: u64) -> DocumentSyncOutboxRecord {
        crate::document_sync_outbox::new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            DocumentSyncOutboxEvent::Upsert {
                bytes: vec![origin_seq as u8],
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            false,
        )
    }

    // Two FIFO-adjacent records for one shard topic must never split across a
    // defer/publish boundary: if the genesis "arrives" (availability flips
    // false→true) between the two records, the older would defer and the newer
    // publish first, inverting their origin sequence on receivers. The fix
    // evaluates availability once per topic, so both defer together.
    #[test]
    fn drain_partition_never_splits_a_topic_when_availability_flips() {
        let topic = irokle::TopicId::hash(b"shard-genesis-race");
        let older = shard_topic_record(1);
        let newer = shard_topic_record(2);
        assert!(older.target.uses_shard_topic());
        let records = vec![
            (b"older".to_vec(), older, topic),
            (b"newer".to_vec(), newer, topic),
        ];

        let mut calls = 0usize;
        let mut defer = DrainDeferState::default();
        let (to_publish, deferred) = partition_drain_records(records, &mut defer, |_| {
            calls += 1;
            calls > 1
        });

        assert_eq!(calls, 1, "topic availability is evaluated once per run");
        assert!(
            to_publish.is_empty(),
            "no record of a deferred topic may publish"
        );
        assert_eq!(deferred, 2);
    }

    #[test]
    fn drain_partition_publishes_all_records_of_an_available_topic_in_fifo_order() {
        let topic = irokle::TopicId::hash(b"shard-genesis-present");
        let records = vec![
            (b"older".to_vec(), shard_topic_record(1), topic),
            (b"newer".to_vec(), shard_topic_record(2), topic),
        ];

        let mut defer = DrainDeferState::default();
        let (to_publish, deferred) = partition_drain_records(records, &mut defer, |_| true);

        assert_eq!(deferred, 0);
        let keys: Vec<Vec<u8>> = to_publish.into_iter().map(|(key, _, _)| key).collect();
        assert_eq!(keys, vec![b"older".to_vec(), b"newer".to_vec()]);
    }

    // A full first page of records for a genesis-less shard topic (all deferred)
    // must not starve records for other topics behind it in the FIFO: the drain
    // pages the whole outbox per run, so a later-page record still publishes.
    #[tokio::test]
    async fn drain_paginates_past_a_deferred_head_page_to_publish_later_records() {
        let realm_id = RealmId::from_bytes([44u8; 32]);
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
        });

        // Every head-page record targets one shard topic with no local genesis,
        // so all of them defer.
        let deferred_change = DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(8, 1),
                actor: node(1),
                updated_at_ms: 9,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: aruna_core::structs::PlacementRef {
                strategy_id: Ulid::from_parts(42, 1),
                epoch: 0,
                shard: 3,
            },
        };
        let deferred_target = DocumentSyncTarget::MetadataRegistry {
            group_id: Ulid::from_parts(1, 1),
            document_id: Ulid::from_parts(2, 2),
        };
        let mut writes = Vec::with_capacity(OUTBOX_DRAIN_BATCH_SIZE + 1);
        for index in 0..OUTBOX_DRAIN_BATCH_SIZE {
            let record = crate::document_sync_outbox::new_outbox_record_with_id(
                Ulid::from_parts(1, index as u128),
                node(1),
                deferred_target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::Upsert {
                    bytes: Vec::new(),
                    change: deferred_change,
                },
                aruna_core::structs::PlacementRef::NIL,
                false,
            );
            writes.push(
                crate::document_sync_outbox::outbox_write_entry(&record).expect("outbox entry"),
            );
        }

        // One later origin record for a shared (non-shard) topic, ordered
        // strictly after the head page, so only pagination reaches it.
        let publish_record = crate::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_parts(2, 0),
            node(1),
            DocumentSyncTarget::RealmAuthorization { realm_id },
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"realm-auth".to_vec(),
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        let publish_key = outbox_key(&publish_record).to_vec();
        writes
            .push(crate::document_sync_outbox::outbox_write_entry(&publish_record).expect("entry"));

        match storage
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected batch write event: {other:?}"),
        }

        let handler = OperationsTaskHandler::new(context);
        handler.drain_document_sync_outbox().await;

        assert_eq!(
            read_outbox_record(&storage, &publish_key)
                .await
                .expect("read publish record"),
            None,
            "the later-page record must publish despite an all-deferred first page"
        );
        let remaining = read_outbox_records(&storage, &[], None, OUTBOX_DRAIN_BATCH_SIZE + 8)
            .await
            .expect("read remaining");
        assert_eq!(
            remaining.records.len(),
            OUTBOX_DRAIN_BATCH_SIZE,
            "every deferred record is retained for the next run"
        );

        net.shutdown().await;
    }

    // A realm-config change originated locally lands only in the outbox; draining
    // it must kick the placement reconciler so this rank-0 node creates its shard
    // topic geneses without waiting for a restart.
    #[tokio::test]
    async fn draining_a_local_realm_config_change_creates_rank0_shard_topics() {
        let realm_id = RealmId::from_bytes([61u8; 32]);
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
        });

        // Install the config so this sole node is rank-0 of every shard, but do
        // not run the placement reconciler yet.
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        config.ensure_node(net.node_id(), RealmNodeKind::Management);
        let actor = Actor {
            node_id: net.node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        match storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: (*realm_id.as_bytes()).into(),
                value: config.to_bytes(&actor).expect("config bytes").into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write: {other:?}"),
        }
        net.refresh_realm_peers_from_document(&config)
            .await
            .expect("refresh peers");

        initialize_task_incoming(context.clone(), task_handle.clone()).await;

        let strategy_id = config.strategies.first().expect("a strategy").strategy_id;
        let topic = aruna_core::document::shard_topic_id(
            realm_id,
            &aruna_core::structs::PlacementRef {
                strategy_id,
                epoch: 0,
                shard: 0,
            },
        );
        assert!(
            !net.document_sync_topic_exists(topic).unwrap_or(false),
            "the rank-0 shard topic must not exist before the config change is drained"
        );

        let record = crate::document_sync_outbox::new_outbox_record(
            net.node_id(),
            DocumentSyncTarget::RealmConfig { realm_id },
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"config".to_vec(),
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        write_outbox_record(&storage, &record).await;
        task_handle
            .send_effect(crate::document_sync_outbox::schedule_outbox_drain_effect())
            .await;

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if net.document_sync_topic_exists(topic).unwrap_or(false) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "rank-0 shard topic was not created after the local config change drained"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        net.shutdown().await;
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
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        write_outbox_record(&storage, &record).await;

        let restored_key = restore_document_sync_outbox_timer_and_receive_key(&storage).await;

        assert_eq!(restored_key, TaskKey::DrainDocumentSyncOutbox);
    }

    #[tokio::test]
    async fn restore_document_sync_outbox_timers_keeps_existing_backoff_timer() {
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
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        write_outbox_record(&storage, &record).await;

        let task_handle = TaskHandle::new();
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        task_handle
            .set_inbound_handler(Arc::new(RecordingTaskHandler { seen: seen_tx }))
            .await;
        match task_handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainDocumentSyncOutbox,
                after: Duration::from_secs(3600),
            }))
            .await
        {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {}
            other => panic!("unexpected timer schedule event: {other:?}"),
        }

        restore_document_sync_outbox_timers(&storage, &task_handle).await;

        assert!(
            tokio::time::timeout(Duration::from_millis(50), seen_rx.recv())
                .await
                .is_err(),
            "durable rearm must not replace an active backoff timer"
        );
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
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        let key = outbox_key(&record).to_vec();

        write_outbox_record(&storage, &record).await;

        let outcome = handler
            .finish_sync_drain_subbatch(
                &TaskKey::DrainDocumentSyncOutbox,
                vec![key.clone()],
                Vec::new(),
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
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        let key = outbox_key(&record).to_vec();
        write_outbox_record(&storage, &record).await;

        let outcome = handler
            .finish_sync_drain_subbatch(
                &TaskKey::DrainDocumentSyncOutbox,
                vec![key.clone()],
                Vec::new(),
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

    #[tokio::test]
    async fn drain_reconcile_refreshes_realm_usage_summary() {
        use aruna_core::keyspaces::{USAGE_NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE};
        use aruna_core::structs::{
            NODE_USAGE_SUMMARY_GLOBAL_KEY, NodeUsageSnapshot, UsageCounters, node_usage_global_key,
            usage_global_shard_key,
        };
        use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};

        async fn write_stat(
            storage: &aruna_storage::StorageHandle,
            key_space: &str,
            key: Vec<u8>,
            value: Vec<u8>,
        ) {
            match storage
                .send_effect(Effect::Storage(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                }))
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => panic!("unexpected write event: {other:?}"),
            }
        }

        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let realm_id = RealmId::from_bytes([44u8; 32]);
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let remote = node(2);

        // Local global counters (10) plus a remote node's snapshot (5) should sum
        // to 15 in the refreshed realm summary cache.
        write_stat(
            &storage,
            USAGE_STATS_KEYSPACE,
            usage_global_shard_key(0),
            UsageCounters {
                logical_bytes: 10,
                ..Default::default()
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
        write_stat(
            &storage,
            USAGE_NODE_STATS_KEYSPACE,
            node_usage_global_key(remote),
            NodeUsageSnapshot {
                node_id: remote,
                counters: UsageCounters {
                    logical_bytes: 5,
                    ..Default::default()
                },
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let handler = OperationsTaskHandler::new(context);

        let outcome = handler
            .finish_sync_drain_subbatch(
                &TaskKey::DrainDocumentSyncOutbox,
                Vec::new(),
                Vec::new(),
                Event::Net(NetEvent::DocumentSync(
                    DocumentSyncNetEvent::DocumentsReconciled {
                        applied: 1,
                        targets: vec![DocumentSyncTarget::NodeUsage {
                            realm_id,
                            node_id: remote,
                            group_id: None,
                        }],
                        metadata_create_events: Vec::new(),
                        metadata_graph_tombstones: Vec::new(),
                    },
                )),
                DrainSyncOutcome::default(),
            )
            .await;

        assert!(!outcome.retry_needed);
        let summary = match storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
                key: NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec().into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected read event: {other:?}"),
        };
        let summary = summary.expect("realm usage summary refreshed by drain reconcile");
        assert_eq!(
            UsageCounters::from_bytes(summary.as_ref())
                .unwrap()
                .logical_bytes,
            15
        );

        net.shutdown().await;
    }

    #[tokio::test]
    async fn drain_reconcile_clears_realm_usage_summary_for_requested_realm_config() {
        use aruna_core::keyspaces::USAGE_NODE_STATS_KEYSPACE;
        use aruna_core::structs::{NODE_USAGE_SUMMARY_GLOBAL_KEY, UsageCounters};
        use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};

        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let realm_id = RealmId::from_bytes([45u8; 32]);
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        match storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
                key: NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec().into(),
                value: UsageCounters {
                    logical_bytes: 99,
                    ..Default::default()
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }

        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let handler = OperationsTaskHandler::new(context);

        let outcome = handler
            .finish_sync_drain_subbatch(
                &TaskKey::DrainDocumentSyncOutbox,
                Vec::new(),
                vec![DocumentSyncTarget::RealmConfig { realm_id }],
                Event::Net(NetEvent::DocumentSync(
                    DocumentSyncNetEvent::DocumentsReconciled {
                        applied: 0,
                        targets: Vec::new(),
                        metadata_create_events: Vec::new(),
                        metadata_graph_tombstones: Vec::new(),
                    },
                )),
                DrainSyncOutcome::default(),
            )
            .await;

        assert!(!outcome.retry_needed);
        match storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
                key: NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec().into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
            other => panic!("expected summary cache to be cleared, got {other:?}"),
        }

        net.shutdown().await;
    }

    async fn make_net_handle(
        realm_id: RealmId,
        storage: &aruna_storage::StorageHandle,
        secret: [u8; 32],
    ) -> NetHandle {
        NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle")
    }

    async fn write_realm_config(
        storage: &aruna_storage::StorageHandle,
        realm_id: RealmId,
        config: &RealmConfigDocument,
        node_id: aruna_core::NodeId,
    ) {
        let actor = Actor {
            node_id,
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let bytes = config.to_bytes(&actor).expect("config serializes");
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                value: ByteView::from(bytes),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write event: {other:?}"),
        }
    }

    async fn write_notification_outbox(
        storage: &aruna_storage::StorageHandle,
        record: &NotificationOutboxRecord,
    ) {
        let (key_space, key, value) =
            notification_outbox_write_entry(record).expect("outbox entry");
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected outbox write event: {other:?}"),
        }
    }

    async fn read_inbox_records(storage: &aruna_storage::StorageHandle) -> Vec<NotificationRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| NotificationRecord::from_bytes(&value).expect("record decodes"))
                .collect(),
            other => panic!("unexpected inbox iter event: {other:?}"),
        }
    }

    fn notification_recipient(realm_id: RealmId) -> UserId {
        UserId::new(Ulid::from_bytes([9u8; 16]), realm_id)
    }

    fn notification_record(realm_id: RealmId, created_at_ms: u64) -> NotificationRecord {
        let recipient = notification_recipient(realm_id);
        NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::from_bytes([1u8; 16]),
                actor_user_id: recipient,
            },
            created_at_ms,
        )
    }

    #[tokio::test]
    async fn notification_drain_delivers_locally_when_self_is_holder() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let net_handle = make_net_handle(realm_id, &storage, [21u8; 32]).await;

        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(net_handle.node_id(), RealmNodeKind::Server);
        write_realm_config(&storage, realm_id, &config, net_handle.node_id()).await;

        let record = notification_record(realm_id, 1_700_000_000_000);
        let outbox = new_notification_outbox_record(record.clone());
        write_notification_outbox(&storage, &outbox).await;

        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net_handle),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let handler = OperationsTaskHandler::new(context);
        handler.drain_notification_outbox().await;

        let inbox = read_inbox_records(&storage).await;
        assert_eq!(inbox, vec![record]);
        let remaining = read_notification_outbox_batch(&storage, None, 1024, None)
            .await
            .expect("outbox read");
        assert!(remaining.records.is_empty());
    }

    #[tokio::test]
    async fn notification_drain_retries_when_holder_unresolvable() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let net_handle = make_net_handle(realm_id, &storage, [22u8; 32]).await;

        let record = notification_record(realm_id, 1_700_000_000_000);
        let outbox = new_notification_outbox_record(record);
        write_notification_outbox(&storage, &outbox).await;

        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net_handle),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
        });
        let handler = OperationsTaskHandler::new(context);
        handler.drain_notification_outbox().await;

        let remaining = read_notification_outbox_batch(&storage, None, 1024, None)
            .await
            .expect("outbox read");
        assert_eq!(remaining.records.len(), 1);
        assert!(read_inbox_records(&storage).await.is_empty());

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::DrainNotificationOutbox,
                after: Duration::from_secs(10_000),
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };
        assert!(
            after <= NOTIFICATION_DELIVERY_RETRY_AFTER,
            "an unresolvable holder must re-arm the retry timer"
        );
    }

    #[tokio::test]
    async fn notification_drain_drops_expired_records_with_warn() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let net_handle = make_net_handle(realm_id, &storage, [23u8; 32]).await;

        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(net_handle.node_id(), RealmNodeKind::Server);
        write_realm_config(&storage, realm_id, &config, net_handle.node_id()).await;

        let outbox = NotificationOutboxRecord {
            outbox_id: Ulid::from_parts(1, 0),
            record: notification_record(realm_id, 1_000),
        };
        write_notification_outbox(&storage, &outbox).await;

        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net_handle),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let handler = OperationsTaskHandler::new(context);
        handler.drain_notification_outbox().await;

        assert!(read_inbox_records(&storage).await.is_empty());
        let remaining = read_notification_outbox_batch(&storage, None, 1024, None)
            .await
            .expect("outbox read");
        assert!(remaining.records.is_empty());
    }

    #[tokio::test]
    async fn notification_drain_delivers_to_remote_holder() {
        let realm_id = RealmId::from_bytes([8u8; 32]);

        let dir_a = tempdir().expect("temp dir");
        let storage_a =
            FjallStorage::open(dir_a.path().to_str().expect("temp path")).expect("storage opens");
        let net_a = make_net_handle(realm_id, &storage_a, [24u8; 32]).await;

        let dir_b = tempdir().expect("temp dir");
        let storage_b =
            FjallStorage::open(dir_b.path().to_str().expect("temp path")).expect("storage opens");
        let net_b = make_net_handle(realm_id, &storage_b, [25u8; 32]).await;

        net_a.add_peer_addr(net_b.endpoint_addr()).await;

        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(net_a.node_id(), RealmNodeKind::Server);
        config.ensure_node(net_b.node_id(), RealmNodeKind::Server);
        write_realm_config(&storage_a, realm_id, &config, net_a.node_id()).await;
        write_realm_config(&storage_b, realm_id, &config, net_b.node_id()).await;

        let b_id = net_b.node_id();
        let recipient = loop {
            let candidate = UserId::new(Ulid::r#gen(), realm_id);
            if resolve_inbox_holder(&candidate, &config).expect("resolve holder") == Some(b_id) {
                break candidate;
            }
        };

        let record = NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::from_bytes([1u8; 16]),
                actor_user_id: recipient,
            },
            1_700_000_000_000,
        );
        let outbox = new_notification_outbox_record(record.clone());
        write_notification_outbox(&storage_a, &outbox).await;

        let context_b = Arc::new(DriverContext {
            storage_handle: storage_b.clone(),
            net_handle: Some(net_b),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        crate::incoming::initialize_net_incoming(context_b.clone());

        let context_a = Arc::new(DriverContext {
            storage_handle: storage_a.clone(),
            net_handle: Some(net_a),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let handler = OperationsTaskHandler::new(context_a);
        handler.drain_notification_outbox().await;

        assert_eq!(read_inbox_records(&storage_b).await, vec![record]);
        let remaining = read_notification_outbox_batch(&storage_a, None, 1024, None)
            .await
            .expect("outbox read");
        assert!(remaining.records.is_empty());
    }
}
