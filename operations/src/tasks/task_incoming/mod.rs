use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use aruna_core::document::{
    DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncPublish, DocumentSyncTarget,
};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::{
    JobExecutionClass, NotificationRecord, RealmConfigDocument, RealmId, RoCrateLimits,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_millis;
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use byteview::ByteView;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[cfg(debug_assertions)]
struct OutboxBarrier {
    marker: std::path::PathBuf,
}

#[cfg(debug_assertions)]
impl Drop for OutboxBarrier {
    fn drop(&mut self) {
        info!(event = "test.outbox.joined", "Outbox drain joined");
        if let Err(error) = std::fs::write(&self.marker, b"joined") {
            warn!(error = %error, "Failed to record outbox join");
        }
    }
}

use crate::blob::blob_holders::RefreshBlobHoldersOperation;
use crate::blob::cleanup::{
    BLOB_CLEANUP_AFTER, BLOB_CLEANUP_RETRY, process_cleanup_batch, sweep_stale_uploads,
};
use crate::blob::hidden::{
    HIDDEN_SWEEP_AFTER, HIDDEN_SWEEP_RETRY, process_hidden_sweep, restore_hidden_sweep,
};
use crate::blob::reclaim::{
    RECLAIM_SWEEP_AFTER, RECLAIM_SWEEP_RETRY, process_reclaim_batch, restore_reclaim_sweep,
};
use crate::device::drain::{
    DrainOutcome, INTAKE_CONTINUE_AFTER, INTAKE_DEFER_RETRY_AFTER, restore_intake_timer,
};
use crate::device::sync::{
    RECONCILE_CONTINUE_AFTER, RECONCILE_IDLE_AFTER, RECONCILE_RETRY_AFTER, UPLOAD_CONTINUE_AFTER,
    UPLOAD_DEFER_RETRY_AFTER, restore_sync_timers,
};
use crate::driver::{DriverContext, drive};
use crate::groups::backends::remove::remove_drained_backends;
use crate::jobs::drain::{JobClassBudget, process_job_queue_batch, restore_job_queue_timer};
use crate::jobs::lifecycle::outbox::{OUTBOX_RETRY_AFTER, drain_family_outbox};
use crate::jobs::lifecycle::updates::{SETTLE_RETRY_AFTER, settle_terminals};
use crate::jobs::lifecycle::witness::{WITNESS_RETRY_AFTER, drain_witness_deadlines};
use crate::jobs::prune::{process_job_prune_batch, restore_job_prune_timer};
use crate::jobs::runtime::JobsRuntime;
use crate::jobs::store::release_job;
use crate::jobs::{JOB_DRAIN_RETRY_AFTER, JOB_PRUNE_POLL_AFTER, JOB_PRUNE_RETRY_AFTER};
use crate::metadata::materialization_queue::{
    METADATA_MATERIALIZATION_NEXT_BATCH_AFTER, METADATA_MATERIALIZATION_POLL_AFTER,
    METADATA_MATERIALIZATION_RETRY_AFTER, MetadataMaterializationDrainResult,
    metadata_materialization_jobs_exist, process_metadata_materialization_batch,
    requeue_dead_letters, restore_metadata_materialization_timer,
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
use crate::node::dashboard::{notify_dashboard_change, targets_change_dashboard};
use crate::node::usage_stats::{
    refresh_realm_usage_summary_for_targets, restore_usage_snapshot_publish_timer,
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
use crate::placement::policy::observe_placement;
use crate::placement::process_placements::{PlacementReconcileStatus, process_shard_placements};
use crate::realm::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation, REALM_PRESENCE_REFRESH_AFTER,
};
use crate::replication::queue::{
    BLOB_REPLICATION_RETRY_AFTER, process_blob_replication_batch, restore_blob_replication_timer,
};
use crate::s3::refresh_reference_metadata::{
    REFERENCE_METADATA_REFRESH_RETRY_AFTER, process_reference_metadata_refresh_batch,
    restore_reference_metadata_refresh_timer,
};
use crate::sync::document_sync_outbox::{
    OUTBOX_DRAIN_BATCH_SIZE, delete_outbox_records, read_outbox_records, read_outbox_tails,
    restore_document_sync_outbox_timers,
};
use crate::sync::shard_placement::{
    DOCUMENT_SYNC_DEFER_RETRY_AFTER, SHARD_TOPIC_PULL_RETRY_AFTER, SHARD_TOPIC_PULL_RETRY_MAX,
    SYNC_PLACEMENT_RETRY_AFTER,
};
use crate::sync::sync_mirror_repair::{
    MIRROR_REPAIR_RETRY_AFTER, process_mirror_repairs, restore_mirror_timer,
};
use crate::tasks::queue_backoff::{queue_retry_after_ms, retry_after_ms};
use crate::tasks::task_persistence::{
    delete_persisted_timer, persist_task_effect, restore_persisted_task_timers,
};

mod outbox;
mod restore;

pub use restore::{initialize_task_holder, initialize_task_incoming};

/// Process-wide tally of document sync outbox records ever classified
/// undeliverable. The drain already error-logs each one; this exposes the count
/// so a test can assert the draining-flush path never black-holes a record.
pub static UNDELIVERABLE_RECORD_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

const DRAIN_SUBBATCH_RECORDS: usize = 512;
/// Pages one drain invocation may examine, so a large or blocked queue cannot
/// monopolise a task invocation.
const OUTBOX_INVOCATION_PAGES: usize = 2;
const OUTBOX_INVOCATION_RECORDS: usize = OUTBOX_INVOCATION_PAGES * OUTBOX_DRAIN_BATCH_SIZE;
/// Consecutive continuations before a rotation yields through the timer, so an
/// append-heavy or wholly blocked queue cannot keep the task continuously hot.
const OUTBOX_CONTINUATION_STREAK: u32 = 8;
const _: () = assert!(OUTBOX_INVOCATION_PAGES > 0 && OUTBOX_CONTINUATION_STREAK > 0);
const OUTBOX_CONTINUATION_AFTER: Duration = Duration::from_millis(50);
const DURABLE_QUEUE_REARM_AFTER: Duration = Duration::from_secs(5);

/// How long a device keeps its copy of the realm documents before asking for
/// them again. A revocation reaches it within this window at the latest.
const REALM_DOCUMENTS_AFTER: Duration = Duration::from_secs(60);

/// How long one attempt may spend on the realm as a whole. The folder cadence
/// is what it shares that time with, so it is bounded once, not per node.
const REALM_DOCUMENTS_BUDGET: Duration = Duration::from_secs(20);
/// Rearm ticks between dead-letter sweeps, i.e. one sweep a minute.
const DEAD_LETTER_SWEEP_TICKS: usize = 12;
/// How long a record may wait for its shard topic's genesis before the drain
/// stops treating the wait as normal and says so at error level.
const OUTBOX_STUCK_AFTER: Duration = Duration::from_secs(300);

#[derive(Clone, Copy)]
struct OutboxLimits {
    pages: usize,
    records: usize,
    continuation_streak: u32,
}

impl Default for OutboxLimits {
    fn default() -> Self {
        Self {
            pages: OUTBOX_INVOCATION_PAGES,
            records: OUTBOX_INVOCATION_RECORDS,
            continuation_streak: OUTBOX_CONTINUATION_STREAK,
        }
    }
}

/// One drained outbox record with its resolved publish topic.
type DrainRecord = (
    Vec<u8>,
    aruna_core::document::DocumentSyncOutboxRecord,
    irokle::TopicId,
);

struct OperationsTaskHandler {
    context: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    rocrate_limits: RoCrateLimits,
    // In-memory retry-attempt counters keyed by timer. Loss on restart is fine:
    // a restarted node simply retries from the base interval.
    retry_backoff: std::sync::Mutex<HashMap<TaskKey, u32>>,
    // Where a capped reclaim sweep resumes. Loss on restart is fine: the next
    // sweep starts from the head and reaches the tail over the ticks after it.
    reclaim_cursor: std::sync::Mutex<Option<Key>>,
    // Rotation state of the bounded outbox drain. Loss on restart is fine: the
    // next rotation opens at the head.
    rotation: std::sync::Mutex<OutboxRotation>,
    drain_guard: tokio::sync::Mutex<()>,
    outbox_limits: OutboxLimits,
    // When a device may next fetch the realm documents, and how many attempts
    // have failed. Loss on restart is fine: a restart fetches them anyway.
    realm_documents: std::sync::Mutex<(u32, u64)>,
}

/// Outcome counts accumulated across the invocations of one rotation.
#[derive(Clone, Copy, Debug, Default)]
struct RotationTotals {
    examined: usize,
    deleted: usize,
    deferred: usize,
    undeliverable: usize,
    retry_invocations: usize,
    invocations: u32,
}

/// One rotation of the bounded document-sync drain: head to observed end across
/// as many bounded invocations as it needs. Ordering blocks and outcome counts
/// carry across them; holdership and publisher authority never do.
#[derive(Default)]
struct OutboxRotation {
    /// Last key observed when this rotation opened.
    boundary: Option<Vec<u8>>,
    /// Last key observed in each independently ordered event-kind stream.
    stream_boundaries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Where the next invocation resumes; `None` is the head.
    cursor: Option<Vec<u8>>,
    /// Topics whose FIFO is blocked for the rest of this rotation.
    blocked_topics: HashSet<irokle::TopicId>,
    /// Admin origin streams blocked for the rest of this rotation.
    blocked_origins: HashSet<aruna_core::NodeId>,
    /// Topics this node can never publish onto, for the rest of this rotation.
    undeliverable_topics: HashSet<irokle::TopicId>,
    totals: RotationTotals,
    /// Continuations already spent without yielding through the timer.
    continuations: u32,
}

struct DrainSubBatch {
    peers: Vec<aruna_core::NodeId>,
    documents: Vec<DocumentSyncPublish>,
    topics: Vec<irokle::TopicId>,
    /// Admin origin of each entry, so a blocked publish also blocks the rest of
    /// that origin's sequence.
    origins: Vec<Option<aruna_core::NodeId>>,
    targets: Vec<DocumentSyncTarget>,
    record_keys: Vec<Vec<u8>>,
}

#[derive(Default)]
struct DrainSyncOutcome {
    sync_elapsed: Duration,
    project_elapsed: Duration,
    delete_elapsed: Duration,
    retry_needed: bool,
    deleted: usize,
    /// Ordering domains to block for the rest of the rotation.
    blocked_topics: Vec<irokle::TopicId>,
    blocked_origins: Vec<aruna_core::NodeId>,
}

/// Per-invocation defer state. The block sets are seeded from the open rotation;
/// `topic_exists` and `topic_held` deliberately are not, so holdership and
/// genesis presence are re-read every invocation.
#[derive(Default)]
struct DrainDeferState {
    topic_exists: HashMap<irokle::TopicId, bool>,
    topic_held: HashMap<irokle::TopicId, bool>,
    deferred_topics: HashSet<irokle::TopicId>,
    blocked_origins: HashSet<aruna_core::NodeId>,
    undeliverable_topics: HashSet<irokle::TopicId>,
}

type StuckRecord = (
    u64,
    DocumentSyncTarget,
    irokle::TopicId,
    aruna_core::structs::PlacementRef,
);

struct DrainInvocation {
    outcome: DrainSyncOutcome,
    defer: DrainDeferState,
    cursor: Option<Vec<u8>>,
    reached_end: bool,
    scan_elapsed: Duration,
    publish_elapsed: Duration,
    records: usize,
    deferred: usize,
    stuck: usize,
    oldest_stuck: Option<StuckRecord>,
    undeliverable: usize,
    groups: usize,
    subbatches: usize,
    pages: usize,
    oldest_record_ms: Option<u64>,
    read_failed: bool,
    config_drained: bool,
}

enum DrainPage {
    Records {
        records: Vec<(Vec<u8>, DocumentSyncOutboxRecord)>,
        has_more: bool,
        boundary_reached: bool,
    },
    Skip,
    Stop,
}

/// Whether a shard-classed record can ever publish from this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeferOutcome {
    /// The local node holds the bucket, so it may publish onto the bucket's topic
    /// once the genesis is there (rank-0 creates it, every other holder pulls it).
    Retry,
    /// The local node holds none of the record's bucket. Topic membership is the
    /// holder set, so it may neither mint the genesis nor join the topic: this
    /// record can never publish from here, however long it waits.
    Undeliverable,
}

impl OperationsTaskHandler {
    fn new(context: Arc<DriverContext>, jobs_runtime: Arc<JobsRuntime>) -> Self {
        Self {
            context,
            jobs_runtime,
            rocrate_limits: RoCrateLimits::default(),
            retry_backoff: std::sync::Mutex::new(HashMap::new()),
            reclaim_cursor: std::sync::Mutex::new(None),
            rotation: std::sync::Mutex::new(OutboxRotation::default()),
            drain_guard: tokio::sync::Mutex::new(()),
            outbox_limits: OutboxLimits::default(),
            realm_documents: std::sync::Mutex::new((0, 0)),
        }
    }

    fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    #[cfg(test)]
    fn with_outbox_limits(
        mut self,
        pages: usize,
        records: usize,
        continuation_streak: u32,
    ) -> Self {
        assert!(pages > 0 && records > 0 && continuation_streak > 0);
        self.outbox_limits = OutboxLimits {
            pages,
            records,
            continuation_streak,
        };
        self
    }

    async fn refresh_blob_holders(&self) {
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(task_id = ?TaskKey::RefreshBlobHolders, "Cannot refresh blob holders without net handle");
            self.reschedule_timer(
                TaskKey::RefreshBlobHolders,
                Duration::from_millis(self.rocrate_limits.holder_refresh_ms),
            )
            .await;
            return;
        };
        // A User-kind device registers no holdership: the realm refuses its DHT
        // puts. An unreadable config is bootstrap, which registers.
        if matches!(
            crate::metadata::forward::is_user_origin(
                &self.context,
                *net_handle.realm_id(),
                net_handle.node_id()
            )
            .await,
            Ok(true)
        ) {
            return;
        }
        let operation =
            RefreshBlobHoldersOperation::new(*net_handle.realm_id(), self.rocrate_limits.clone());
        if let Err(error) = drive(operation, self.context.as_ref()).await {
            warn!(task_id = ?TaskKey::RefreshBlobHolders, error = %error, "Failed to refresh blob holders");
            self.reschedule_timer(
                TaskKey::RefreshBlobHolders,
                Duration::from_millis(self.rocrate_limits.holder_refresh_ms),
            )
            .await;
        }
    }

    /// Handles records this node cannot publish itself: admin records are relayed under
    /// this node's origin signature and deleted once a holder takes custody, while
    /// unsigned upserts/deletes stay in the outbox. Returns keys whose custody moved.
    async fn relay_undeliverable_records(
        &self,
        config: Option<&aruna_core::structs::RealmConfigDocument>,
        undeliverable: &[DrainRecord],
    ) -> Vec<Vec<u8>> {
        let mut relayed = Vec::new();
        let device = self.is_device(config);
        for (record_key, record, topic) in undeliverable {
            if let Some(config) = config
                && self.relay_admin_record(config, record).await
            {
                relayed.push(record_key.clone());
                continue;
            }
            // A device can never become a holder, so an unrelayable row of its
            // own would only be error-logged on every drain: drop it instead.
            if device && !matches!(record.event, DocumentSyncOutboxEvent::AdminOperation { .. }) {
                warn!(
                    event = "pipeline.drain.dropped",
                    target = ?record.target,
                    "Dropping a document sync outbox record no device can publish"
                );
                relayed.push(record_key.clone());
                continue;
            }
            error!(
                event = "pipeline.drain.undeliverable",
                target = ?record.target,
                %topic,
                strategy = %record.placement.strategy_id,
                shard = record.placement.shard,
                age_ms = unix_timestamp_millis().saturating_sub(record.outbox_id.timestamp_ms()),
                "Cannot publish a document sync outbox record from this node and it is not relayable; leaving it in the outbox"
            );
        }
        // A relayed record left this node, so it never counts as undeliverable.
        UNDELIVERABLE_RECORD_COUNT.fetch_add(
            undeliverable.len().saturating_sub(relayed.len()) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        relayed
    }

    /// Whether this node is a device: configured with a kind that holds no
    /// sync topic. Unknown kinds count as infrastructure.
    fn is_device(&self, config: Option<&aruna_core::structs::RealmConfigDocument>) -> bool {
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            return false;
        };
        config
            .and_then(|config| {
                crate::realm::mutate_realm_placement::node_kind(config, net_handle.node_id())
            })
            .is_some_and(|kind| !kind.is_sync_eligible())
    }

    /// Signs one locally originated administrative envelope and hands it to a
    /// holder. Returns whether a holder took custody.
    async fn relay_admin_record(
        &self,
        config: &aruna_core::structs::RealmConfigDocument,
        record: &DocumentSyncOutboxRecord,
    ) -> bool {
        let DocumentSyncOutboxEvent::AdminOperation {
            event,
            origin_signature,
        } = &record.event
        else {
            return false;
        };
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            return false;
        };
        let origin_signature = match origin_signature {
            Some(signature) => *signature,
            None if event.origin_node_id == net_handle.node_id() => {
                match event.signing_bytes(&record.placement) {
                    Ok(bytes) => net_handle.sign(&bytes),
                    Err(error) => {
                        warn!(%error, "Cannot sign an admin event for relay");
                        return false;
                    }
                }
            }
            None => return false,
        };
        let holders = crate::placement::resolve_shard_holders(config, &record.placement);
        if holders.is_empty() {
            return false;
        }
        match crate::metadata::forward::relay_admin_event(
            &self.context,
            &holders,
            record.target.clone(),
            event.clone(),
            record.placement,
            origin_signature,
        )
        .await
        {
            Ok(()) => {
                info!(
                    event = "pipeline.drain.relayed",
                    target = ?record.target,
                    origin = %event.origin_node_id,
                    "Relayed an admin outbox record to a holder"
                );
                true
            }
            Err(error) => {
                warn!(
                    event = "pipeline.drain.relay_failed",
                    target = ?record.target,
                    %error,
                    "No holder accepted a relayed admin outbox record"
                );
                false
            }
        }
    }

    /// Backoff interval for the next re-arm of `key`, from the in-memory attempt count
    /// without mutating it. The drain re-arm is the only retry path for an accepted
    /// write after a transient sync failure, so it uses the queue scale, not a 30s base.
    fn backoff_after(&self, key: &TaskKey) -> Duration {
        let attempts = self
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .get(key)
            .copied()
            .unwrap_or(0);
        Duration::from_millis(queue_retry_after_ms(attempts))
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

    fn reclaim_start(&self) -> Option<Key> {
        self.reclaim_cursor
            .lock()
            .expect("reclaim cursor mutex poisoned")
            .clone()
    }

    fn set_reclaim_start(&self, cursor: Option<Key>) {
        *self
            .reclaim_cursor
            .lock()
            .expect("reclaim cursor mutex poisoned") = cursor;
    }

    /// Keeps the first missing-topic pull retry prompt, then doubles each subsequent
    /// full placement scan up to the placement interval. New holders only need their
    /// co-holders to apply the same config, so the ladder must not cliff to 30s.
    fn placement_pull_retry_after(&self, key: &TaskKey) -> Duration {
        self.retry_ladder(
            key,
            SHARD_TOPIC_PULL_RETRY_AFTER,
            SHARD_TOPIC_PULL_RETRY_MAX,
        )
    }

    /// Keeps `base` for the first attempt, then doubles each further one up to
    /// `max`, counting attempts in the shared in-memory ladder.
    fn retry_ladder(&self, key: &TaskKey, base: Duration, max: Duration) -> Duration {
        let mut backoff = self
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned");
        match backoff.entry(key.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(0);
                base
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let attempts = entry.get().saturating_add(1);
                entry.insert(attempts);
                Duration::from_millis(retry_after_ms(
                    attempts,
                    base.as_millis() as u64,
                    max.as_millis() as u64,
                ))
            }
        }
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
            warn!(task_id = ?key, message = %message, "Failed to persist timer re-arm");
            return false;
        }
        let Some(task_handle) = self.context.task_handle.as_ref() else {
            warn!(task_id = ?key, "Cannot re-arm failed timer without task handle");
            return false;
        };
        match task_handle.send_effect(Effect::Task(effect)).await {
            Event::Task(TaskEvent::TimerScheduled { .. }) => true,
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(task_id = ?key, message = %message, "Failed to re-arm failed timer");
                false
            }
            other => {
                warn!(task_id = ?key, event = ?other, "Unexpected timer re-arm result");
                false
            }
        }
    }

    // Kicks the placement reconciler immediately (not persisted; it is re-derived
    // from the realm config at startup by `restore_shard_subscriptions`).
    async fn schedule_sync_placements(&self, realm_id: RealmId, node_id: aruna_core::NodeId) {
        let task_id = TaskKey::SyncPlacements { realm_id, node_id };
        let Some(task_handle) = self.context.task_handle.as_ref() else {
            warn!(task_id = ?task_id, "Cannot schedule shard placement sync without task handle");
            return;
        };
        let effect = Effect::Task(TaskEffect::ResetTimer {
            key: task_id.clone(),
            after: Duration::ZERO,
        });
        if let Event::Task(TaskEvent::Error { message, .. }) = task_handle.send_effect(effect).await
        {
            warn!(task_id = ?task_id, message = %message, "Failed to schedule shard placement sync after local realm config change");
        }
    }

    /// Fetches the realm documents on a device, at most once per backoff
    /// window. A device is judged by state it does not hold, so this is the one
    /// beat that keeps a revocation or an eviction reaching it.
    async fn fetch_realm_documents(&self) {
        let now = unix_timestamp_millis();
        {
            let due = self
                .realm_documents
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if due.1 > now {
                return;
            }
        }
        let fetched = crate::device::realm_documents::fetch_realm_documents(
            &self.context,
            REALM_DOCUMENTS_BUDGET,
        )
        .await;
        if fetched {
            crate::device::sync_status::note_contact(&self.context).await;
        }
        let mut state = self
            .realm_documents
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let attempts = match fetched {
            true => 0,
            false => state.0.saturating_add(1),
        };
        let after = match fetched {
            true => REALM_DOCUMENTS_AFTER.as_millis() as u64,
            false => queue_retry_after_ms(attempts),
        };
        *state = (attempts, now.saturating_add(after));
    }

    /// Refreshes the metadata replicas this device keeps, on the same beat the
    /// folders run on. A node that keeps none does nothing.
    async fn refresh_device_replicas(&self) {
        if crate::device::refresh::refresh_replicas(&self.context).await > 0 {
            crate::device::sync_status::note_sync(&self.context).await;
        }
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
                let delete_count = record_keys.len();
                let deleted =
                    delete_outbox_records(&self.context.storage_handle, record_keys).await;
                outcome.delete_elapsed = delete_started.elapsed();
                if deleted.is_ok() {
                    outcome.deleted += delete_count;
                }
                if let Err(error) = deleted {
                    warn!(task_id = ?retry_key, error = %error, "Failed to delete document sync outbox records");
                    outcome.retry_needed = true;
                } else if targets_change_dashboard(&refresh_targets) {
                    notify_dashboard_change(self.context.as_ref());
                }
            }
            Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error { error, .. })) => {
                warn!(task_id = ?retry_key, error = %error, "Failed to sync document batch");
                outcome.retry_needed = true;
            }
            Event::Net(NetEvent::Error(error)) => {
                warn!(task_id = ?retry_key, error = ?error, "Failed to sync document batch");
                outcome.retry_needed = true;
            }
            other => {
                warn!(task_id = ?retry_key, event = ?other, "Unexpected document sync batch result");
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
                warn!(task_id = ?retry_key, error = ?error, "Failed to project metadata create event batch after document sync");
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
            warn!(task_id = ?retry_key, error = ?error, "Failed to project metadata create event batch from log after document sync");
            return Err(());
        }
        Ok(())
    }
}

/// The durable queue work deferred until after the local serving gate.
pub struct TaskQueues {
    context: Arc<DriverContext>,
    task_handle: TaskHandle,
    handler: Arc<OperationsTaskHandler>,
    refresh_holders: bool,
}

/// Kicks the installed document-sync drain owner without replacing an existing
/// persisted retry deadline.
pub async fn drive_document_sync_outbox_drain(context: Arc<DriverContext>) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        warn!("Cannot kick document sync outbox drain without task handle");
        return;
    };
    restore_document_sync_outbox_timers(&context.storage_handle, task_handle).await;
}

/// A document sync drain that keeps its rotation across invocations like the
/// timer-driven handler. A fresh drainer starts at the head, which is the same
/// reset a process restart performs.
pub struct OutboxDrainer {
    handler: Arc<OperationsTaskHandler>,
}

#[doc(hidden)]
pub async fn drain_notification_outbox(context: Arc<DriverContext>) {
    OperationsTaskHandler::new(context, JobsRuntime::new())
        .drain_notification_outbox()
        .await;
}

impl OutboxDrainer {
    pub fn new(context: Arc<DriverContext>) -> Self {
        Self {
            handler: Arc::new(OperationsTaskHandler::new(context, JobsRuntime::new())),
        }
    }

    /// Runs one bounded invocation of the open rotation.
    pub async fn run_once(&self) {
        self.handler.drain_document_sync_outbox().await;
    }

    /// Records examined so far, and whether the cursor is parked mid-rotation.
    pub fn rotation_progress(&self) -> (usize, bool) {
        let rotation = self
            .handler
            .rotation
            .lock()
            .expect("outbox rotation mutex poisoned");
        (rotation.totals.examined, rotation.cursor.is_some())
    }
}

#[async_trait]
impl InboundTaskHandler for OperationsTaskHandler {
    async fn handle_timer(&self, key: TaskKey) {
        delete_persisted_timer(&self.context.storage_handle, &key).await;
        match key {
            TaskKey::RealmPresence { realm_id, node_id } => {
                // A User-kind device is DHT read-only and never publishes
                // presence. An unreadable config is bootstrap, which announces.
                if matches!(
                    crate::metadata::forward::is_user_origin(&self.context, realm_id, node_id)
                        .await,
                    Ok(true)
                ) {
                    return;
                }
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
                let key = TaskKey::SyncPlacements { realm_id, node_id };
                // The same observation that reconciles shards reconciles this
                // node's placement subject: a moved, draining or removed node
                // stops admitting governed data and revalidates its inventory.
                if let Err(error) =
                    observe_placement(&self.context, realm_id, node_id, unix_timestamp_millis())
                        .await
                {
                    warn!(error = %error, "Placement subject reconcile failed");
                }
                let outcome = process_shard_placements(&self.context, realm_id, node_id).await;
                match outcome.status {
                    PlacementReconcileStatus::Clean => self.reset_backoff(&key),
                    PlacementReconcileStatus::RetryScheduled if outcome.pull_pending => {
                        let after = self.placement_pull_retry_after(&key);
                        self.reschedule_timer(key, after).await;
                    }
                    PlacementReconcileStatus::RetryScheduled => {}
                    PlacementReconcileStatus::StorageFailure => {
                        self.reschedule_timer(key, SYNC_PLACEMENT_RETRY_AFTER).await;
                    }
                }
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
            TaskKey::DrainJobQueue => {
                self.drain_job_queue().await;
            }
            TaskKey::PruneJobs => {
                self.prune_jobs().await;
            }
            TaskKey::DrainSyncMirrorRepair => {
                self.drain_mirror_repair().await;
            }
            TaskKey::SweepHiddenBlobs => {
                self.sweep_hidden_blobs().await;
            }
            TaskKey::DrainBlobCleanupQueue => {
                self.drain_blob_cleanup().await;
            }
            TaskKey::DrainBlobReclaimQueue => {
                self.drain_blob_reclaim().await;
            }
            TaskKey::RefreshBlobHolders => {
                self.refresh_blob_holders().await;
            }
            TaskKey::DrainJobFamilyOutbox => {
                self.drain_job_family_outbox().await;
            }
            TaskKey::DrainJobWitnessQueue => {
                self.drain_job_witness_queue().await;
            }
            TaskKey::SettleJobTerminals => {
                self.settle_job_terminals().await;
            }
            TaskKey::ReconcileSyncedFolders => {
                let after = match crate::device::sync::reconcile_folders(&self.context).await {
                    DrainOutcome::Deferred => RECONCILE_RETRY_AFTER,
                    DrainOutcome::More => RECONCILE_CONTINUE_AFTER,
                    DrainOutcome::Idle => RECONCILE_IDLE_AFTER,
                };
                self.reschedule_timer(TaskKey::ReconcileSyncedFolders, after)
                    .await;
                // After the folders, never before them: an unreachable realm
                // must not hold up the owner's own files.
                self.fetch_realm_documents().await;
                self.refresh_device_replicas().await;
            }
            TaskKey::DrainSyncUploadOutbox => {
                let after = match crate::device::sync::drain_sync_outbox(&self.context).await {
                    DrainOutcome::Deferred => Some(UPLOAD_DEFER_RETRY_AFTER),
                    DrainOutcome::More => Some(UPLOAD_CONTINUE_AFTER),
                    DrainOutcome::Idle => None,
                };
                if let Some(after) = after {
                    self.reschedule_timer(TaskKey::DrainSyncUploadOutbox, after)
                        .await;
                }
            }
            TaskKey::DrainDeviceIntake => {
                let after = match crate::device::drain::drain_intake(&self.context).await {
                    DrainOutcome::Deferred => Some(INTAKE_DEFER_RETRY_AFTER),
                    DrainOutcome::More => Some(INTAKE_CONTINUE_AFTER),
                    DrainOutcome::Idle => None,
                };
                if let Some(after) = after {
                    self.reschedule_timer(TaskKey::DrainDeviceIntake, after)
                        .await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::outbox::document_publish_from_outbox;
    use super::outbox::partition_drain_records;
    use super::restore::drain_delay;
    use super::*;
    use crate::jobs::store::{ClaimOutcome, claim_job, insert_job, read_job_record};
    use crate::sync::document_sync_outbox::{
        outbox_key, read_outbox_record, restore_document_sync_outbox_timers, write_outbox_effect,
    };
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent,
        DocumentSyncOutboxRecord, DocumentSyncRevision,
    };
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{
        METADATA_GRAPH_PRUNE_JOB_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE, TASK_TIMER_KEYSPACE,
    };
    use aruna_core::metadata::{MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord};
    use aruna_core::storage_entries::notification_outbox_write_entry;
    use aruna_core::structs::{
        Actor, FIRST_GRANTABLE_HANDLE, JobId, JobPayload, JobRecord, JobState, NotificationClass,
        NotificationKind, NotificationOutboxRecord, RealmConfigDocument, RealmId, RealmNodeKind,
    };
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::types::UserId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use aruna_tasks::{InboundTaskHandler, TaskHandle};
    use async_trait::async_trait;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    use crate::notifications::outbox::new_notification_outbox_record;

    fn job_id() -> JobId {
        crate::jobs::submit::mint_job_id(
            PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            BucketId::new(0).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn blocked_batch_waits() {
        // Nothing processed means the due head is blocked, so the next scan
        // waits instead of respinning at the 25ms batch pace.
        let blocked = MetadataMaterializationDrainResult {
            processed: 0,
            has_more_due: true,
            next_due_after: None,
        };
        let progressing = MetadataMaterializationDrainResult {
            processed: 4,
            ..blocked
        };
        assert_eq!(drain_delay(&blocked), METADATA_MATERIALIZATION_RETRY_AFTER);
        assert_eq!(
            drain_delay(&progressing),
            METADATA_MATERIALIZATION_NEXT_BATCH_AFTER
        );
    }

    #[test]
    fn reclaim_retry_climbs() {
        // A failing sweep earns the fast retry, then doubles up to the normal
        // interval so a candidate that always fails cannot hot-loop a full
        // rescan every minute.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let handler = OperationsTaskHandler::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            JobsRuntime::new(),
        );
        let key = TaskKey::DrainBlobReclaimQueue;
        let ladder = |handler: &OperationsTaskHandler| {
            handler.retry_ladder(&key, RECLAIM_SWEEP_RETRY, RECLAIM_SWEEP_AFTER)
        };

        assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY);
        assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY * 2);
        for _ in 0..8 {
            ladder(&handler);
        }
        assert_eq!(ladder(&handler), RECLAIM_SWEEP_AFTER);

        handler.reset_backoff(&key);
        assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY);
    }

    struct RecordingTaskHandler {
        seen: mpsc::Sender<TaskKey>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingTaskHandler {
        async fn handle_timer(&self, key: TaskKey) {
            let _ = self.seen.send(key).await;
        }
    }

    struct InstalledDrainHandler {
        handler: Arc<OperationsTaskHandler>,
        completed: mpsc::Sender<()>,
    }

    #[async_trait]
    impl InboundTaskHandler for InstalledDrainHandler {
        async fn handle_timer(&self, key: TaskKey) {
            self.handler.handle_timer(key.clone()).await;
            if key == TaskKey::DrainDocumentSyncOutbox {
                let _ = self.completed.send(()).await;
            }
        }
    }

    /// Tokio inhibits paused-clock auto-advance while a blocking task is alive.
    /// Storage and net answer from their own threads, so without this the clock
    /// races ahead of every round trip. `tokio::time::advance` still applies.
    struct ClockGuard {
        _stop: std::sync::mpsc::Sender<()>,
    }

    fn freeze_clock() -> ClockGuard {
        let (stop, wait) = std::sync::mpsc::channel::<()>();
        tokio::task::spawn_blocking(move || {
            let _ = wait.recv();
        });
        ClockGuard { _stop: stop }
    }

    // Net shutdown drains under a timeout that a still clock never expires.
    async fn shutdown_net(net: &NetHandle) {
        tokio::time::resume();
        net.shutdown().await;
    }

    struct InstalledHarness {
        _dir: tempfile::TempDir,
        storage: aruna_storage::StorageHandle,
        net: NetHandle,
        task_handle: TaskHandle,
        context: Arc<DriverContext>,
        handler: Arc<OperationsTaskHandler>,
        completed: mpsc::Receiver<()>,
    }

    // The frozen clock only moves on an explicit advance, so waiting here cannot
    // outrun the drain; a poll bound would instead depend on machine speed.
    async fn recv_progress(receiver: &mut mpsc::Receiver<()>) -> bool {
        receiver.recv().await.is_some()
    }

    async fn installed_setup() -> InstalledHarness {
        let realm_id = RealmId::from_bytes([46u8; 32]);
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = make_net_handle(realm_id, &storage, [46u8; 32]).await;
        tokio::time::pause();
        let target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let topic = target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
        net.ensure_document_sync_topics(&[topic], Vec::new())
            .expect("shared topic genesis");
        for index in 1..=2u128 {
            let record = crate::sync::document_sync_outbox::new_outbox_record_with_id(
                Ulid::from_parts(1, index),
                node(1),
                target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::Upsert {
                    bytes: index.to_be_bytes().to_vec(),
                    change: change(),
                },
                aruna_core::structs::PlacementRef::NIL,
                true,
            );
            write_outbox_record(&storage, &record).await;
        }

        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        let handler = Arc::new(
            OperationsTaskHandler::new(context.clone(), JobsRuntime::new())
                .with_outbox_limits(1, 1, 2),
        );
        let (completed_tx, completed) = mpsc::channel(4);
        task_handle
            .set_inbound_handler(Arc::new(InstalledDrainHandler {
                handler: handler.clone(),
                completed: completed_tx,
            }))
            .await;
        InstalledHarness {
            _dir: dir,
            storage,
            net,
            task_handle,
            context,
            handler,
            completed,
        }
    }

    fn node(seed: u8) -> aruna_core::NodeId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        iroh::SecretKey::from_bytes(&bytes).public()
    }

    fn outbox_handler() -> (tempfile::TempDir, OperationsTaskHandler, TaskHandle) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let task_handle = TaskHandle::new();
        let handler = OperationsTaskHandler::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: Some(task_handle.clone()),
                compute_handle: None,
            }),
            JobsRuntime::new(),
        );
        (dir, handler, task_handle)
    }

    async fn scheduled_after(task_handle: &TaskHandle) -> Duration {
        let TaskEvent::TimerScheduled { after, .. } = task_handle
            .schedule_timer_if_idle(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
            .await
        else {
            panic!("expected timer schedule event");
        };
        after
    }

    #[tokio::test]
    async fn paused_runtime_waits() {
        // Startup recovery must wait until production has made S3 reachable.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        let job_id = job_id();
        let record = JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node(7),
            1,
            1,
            None,
        );
        insert_job(&storage, &record).await.expect("insert job");
        assert!(matches!(
            claim_job(&storage, job_id, node(7), 2).await,
            Ok(ClaimOutcome::Claimed(_))
        ));
        let runtime = JobsRuntime::new_paused();

        initialize_task_incoming(context, task_handle, runtime.clone()).await;

        let stored = read_job_record(&storage, job_id, None)
            .await
            .expect("read job")
            .expect("job exists");
        assert_eq!(stored.state, JobState::Claimed);
        assert_eq!(runtime.recover_stale_jobs(&storage).await.unwrap(), 1);
        runtime.start();
        let stored = read_job_record(&storage, job_id, None)
            .await
            .expect("read job")
            .expect("job exists");
        assert_eq!(stored.state, JobState::Queued);
    }

    #[tokio::test]
    async fn start_keeps_job() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        let job_id = job_id();
        let record = JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            UserId::new(Ulid::from_bytes([3u8; 16]), RealmId([2u8; 32])),
            node(7),
            1,
            1,
            None,
        );
        insert_job(&storage, &record).await.expect("insert job");
        assert!(matches!(
            claim_job(&storage, job_id, node(7), 2).await,
            Ok(ClaimOutcome::Claimed(_))
        ));
        let runtime = JobsRuntime::new_paused();

        let queues = initialize_task_holder(
            context,
            task_handle.clone(),
            runtime.clone(),
            RoCrateLimits::default(),
        )
        .await;
        let shutdown = Shutdown::new();
        queues.start(&shutdown).await;

        let stored = read_job_record(&storage, job_id, None)
            .await
            .expect("read job")
            .expect("job exists");
        assert_eq!(stored.state, JobState::Claimed);
        assert!(!runtime.is_started());

        let requested = Duration::from_secs(7200);
        let TaskEvent::TimerScheduled { after, .. } = task_handle
            .schedule_timer_if_idle(TaskKey::DrainJobQueue, requested)
            .await
        else {
            panic!("expected timer schedule event");
        };
        assert_eq!(after, requested, "job queue timer must remain unscheduled");
        assert!(shutdown.drain(Duration::from_secs(30)).await);
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

    #[tokio::test]
    async fn placement_storage_rearms() {
        let realm_id = RealmId::from_bytes([43u8; 32]);
        let node_id = node(7);
        let key = TaskKey::SyncPlacements { realm_id, node_id };
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        match storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                value: ByteView::from(b"malformed config".to_vec()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write: {other:?}"),
        }

        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle),
            compute_handle: None,
        });

        let before_ms = unix_timestamp_millis();
        OperationsTaskHandler::new(context, JobsRuntime::new())
            .handle_timer(key.clone())
            .await;
        let after_ms = unix_timestamp_millis();

        let persisted = match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 2,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            other => panic!("unexpected timer iter result: {other:?}"),
        };
        assert_eq!(persisted.len(), 1, "one retry timer must be persisted");
        let timer: aruna_core::task::PersistedTaskTimer =
            postcard::from_bytes(&persisted[0].1).expect("persisted timer decodes");
        assert_eq!(timer.key, key);
        let retry_ms = crate::sync::shard_placement::SYNC_PLACEMENT_RETRY_AFTER.as_millis() as u64;
        assert!(timer.due_at_unix_millis >= before_ms.saturating_add(retry_ms));
        assert!(timer.due_at_unix_millis <= after_ms.saturating_add(retry_ms));
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
            aruna_core::structs::PlacementRef::NIL,
            true,
        );

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
            origins: vec![None, None, Some(node(3))],
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
        assert_eq!(selected.origins, vec![Some(node(3))]);
        assert_eq!(selected.record_keys, vec![b"third".to_vec()]);
        assert!(selected.documents.is_empty());
        assert!(subbatch.sync_subset(&[3]).is_none());
    }

    fn shard_topic_record(origin_seq: u64) -> DocumentSyncOutboxRecord {
        crate::sync::document_sync_outbox::new_outbox_record(
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
    // defer/publish boundary, or a between-records availability flip would publish the
    // newer first and invert origin sequence. Availability is evaluated once per topic.
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
        let (to_publish, deferred, undeliverable) = partition_drain_records(
            records,
            &mut defer,
            true,
            |_| {
                calls += 1;
                calls > 1
            },
            |_| DeferOutcome::Retry,
        );

        assert_eq!(calls, 1, "topic availability is evaluated once per run");
        assert!(
            to_publish.is_empty(),
            "no record of a deferred topic may publish"
        );
        assert_eq!(deferred.len(), 2);
        assert!(undeliverable.is_empty());
    }

    fn admin_placement(shard: u32) -> aruna_core::structs::PlacementRef {
        aruna_core::structs::PlacementRef {
            strategy_id: Ulid::from_bytes([50; 16]),
            shard,
        }
    }

    fn admin_outbox(
        realm_id: RealmId,
        origin: aruna_core::NodeId,
        origin_seq: u64,
        target: DocumentSyncTarget,
        placement: aruna_core::structs::PlacementRef,
    ) -> DocumentSyncOutboxRecord {
        use aruna_core::admin_documents::{
            AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
        };
        let user_id = aruna_core::types::UserId::nil(realm_id);
        crate::sync::document_sync_outbox::new_outbox_record(
            node(1),
            target,
            Vec::new(),
            DocumentSyncOutboxEvent::admin(AdminDocumentEvent {
                event_id: ulid::Ulid::from_parts(9, u128::from(origin_seq)),
                target: AdminDocumentTarget::User { user_id },
                origin_node_id: origin,
                origin_seq,
                observed: AdminDocumentClock::default(),
                actor: aruna_core::structs::Actor {
                    node_id: node(1),
                    user_id,
                    realm_id,
                },
                op: AdminDocumentOperation::UserNameSet {
                    name: format!("user-{origin_seq}"),
                },
            }),
            placement,
            false,
        )
    }

    fn admin_record(origin: aruna_core::NodeId, origin_seq: u64) -> DocumentSyncOutboxRecord {
        admin_outbox(
            RealmId([3; 32]),
            origin,
            origin_seq,
            target(),
            admin_placement(1),
        )
    }

    fn shard_change(seed: u8) -> DocumentSyncChange {
        let mut value = change();
        value.placement = aruna_core::structs::PlacementRef {
            strategy_id: Ulid::from_bytes([seed; 16]),
            shard: 1,
        };
        value
    }

    // A blocked admin operation blocks the rest of its origin sequence for the
    // whole rotation, whatever topic the later records ride: publishing a later
    // origin_seq first would drop the earlier one as StaleOriginSequence.
    #[test]
    fn admin_origin_blocks() {
        let origin = node(4);
        let blocked_topic = irokle::TopicId::hash(b"blocked-admin-topic");
        let healthy_topic = irokle::TopicId::hash(b"healthy-admin-topic");
        let records = vec![
            (b"first".to_vec(), admin_record(origin, 1), blocked_topic),
            (b"second".to_vec(), admin_record(origin, 2), healthy_topic),
        ];

        let mut defer = DrainDeferState::default();
        let (to_publish, deferred, undeliverable) = partition_drain_records(
            records,
            &mut defer,
            true,
            |topic| topic != blocked_topic,
            |_| DeferOutcome::Retry,
        );

        assert!(
            to_publish.is_empty(),
            "a later origin_seq must not overtake"
        );
        assert_eq!(deferred.len(), 2);
        assert!(undeliverable.is_empty());
        assert!(defer.blocked_origins.contains(&origin));
    }

    #[test]
    fn topic_block_spans() {
        let topic = irokle::TopicId::hash(b"blocked-topic-pages");
        let healthy_topic = irokle::TopicId::hash(b"healthy-topic-pages");
        let mut defer = DrainDeferState::default();
        let (_, deferred, _) = partition_drain_records(
            vec![(b"first".to_vec(), shard_topic_record(1), topic)],
            &mut defer,
            true,
            |_| false,
            |_| DeferOutcome::Retry,
        );
        assert_eq!(deferred.len(), 1);

        let (published, deferred, undeliverable) = partition_drain_records(
            vec![
                (b"healthy".to_vec(), shard_topic_record(3), healthy_topic),
                (b"second".to_vec(), shard_topic_record(2), topic),
            ],
            &mut defer,
            true,
            |_| true,
            |_| DeferOutcome::Retry,
        );
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].0, b"healthy".to_vec());
        assert_eq!(deferred.len(), 1);
        assert!(undeliverable.is_empty());
    }

    #[test]
    fn admin_block_spans() {
        let origin = node(4);
        let blocked_topic = irokle::TopicId::hash(b"blocked-admin-page");
        let healthy_topic = irokle::TopicId::hash(b"healthy-admin-page");
        let mut defer = DrainDeferState::default();
        let (_, deferred, _) = partition_drain_records(
            vec![(b"first".to_vec(), admin_record(origin, 1), blocked_topic)],
            &mut defer,
            true,
            |_| false,
            |_| DeferOutcome::Retry,
        );
        assert_eq!(deferred.len(), 1);

        let (published, deferred, undeliverable) = partition_drain_records(
            vec![(b"second".to_vec(), admin_record(origin, 2), healthy_topic)],
            &mut defer,
            true,
            |_| true,
            |_| DeferOutcome::Retry,
        );
        assert!(published.is_empty());
        assert_eq!(deferred.len(), 1);
        assert!(undeliverable.is_empty());
    }

    #[tokio::test]
    async fn topic_page_blocks() {
        let _clock = freeze_clock();
        let realm_id = RealmId::from_bytes([49u8; 32]);
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = make_net_handle(realm_id, &storage, [49u8; 32]).await;
        tokio::time::pause();
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        let blocked_target = DocumentSyncTarget::Group {
            group_id: Ulid::from_parts(10, 1),
        };
        let healthy_target = DocumentSyncTarget::Group {
            group_id: Ulid::from_parts(10, 2),
        };
        // A shard topic keys on (strategy, shard) alone, so the two records need
        // different placements to ride a blocked and a healthy topic.
        let blocked_change = shard_change(49);
        let healthy_change = shard_change(51);
        let blocked_topic = blocked_target.sync_topic_id(realm_id, &blocked_change.placement);
        let healthy_topic = healthy_target.sync_topic_id(realm_id, &healthy_change.placement);
        net.ensure_document_sync_topics(&[healthy_topic], Vec::new())
            .expect("healthy topic genesis");
        let blocked = crate::sync::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, 1),
            node(1),
            blocked_target,
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"blocked".to_vec(),
                change: blocked_change,
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        let healthy = crate::sync::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, 2),
            node(1),
            healthy_target,
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"healthy".to_vec(),
                change: healthy_change,
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        let blocked_key = outbox_key(&blocked).to_vec();
        let healthy_key = outbox_key(&healthy).to_vec();
        write_outbox_record(&storage, &blocked).await;
        write_outbox_record(&storage, &healthy).await;
        let handler =
            OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 2);

        handler.drain_document_sync_outbox().await;
        assert_eq!(
            read_outbox_record(&storage, &blocked_key)
                .await
                .expect("read blocked record"),
            Some(blocked.clone())
        );
        assert_eq!(
            scheduled_after(&task_handle).await,
            OUTBOX_CONTINUATION_AFTER
        );
        handler.drain_document_sync_outbox().await;
        assert_eq!(
            read_outbox_record(&storage, &healthy_key)
                .await
                .expect("read healthy record"),
            None,
            "a later page may progress while the blocked topic is retained"
        );
        net.ensure_document_sync_topics(&[blocked_topic], Vec::new())
            .expect("blocked topic genesis");
        for _ in 0..3 {
            handler.drain_document_sync_outbox().await;
        }
        assert_eq!(
            read_outbox_record(&storage, &blocked_key)
                .await
                .expect("read retried record"),
            None
        );
        shutdown_net(&net).await;
    }

    #[tokio::test]
    async fn admin_page_blocks() {
        let _clock = freeze_clock();
        let realm_id = RealmId::from_bytes([50u8; 32]);
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = make_net_handle(realm_id, &storage, [50u8; 32]).await;
        tokio::time::pause();
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        // The local node is the origin: it can only publish envelopes it signs.
        let origin = net.node_id();
        let blocked_target = DocumentSyncTarget::Group {
            group_id: Ulid::from_parts(11, 1),
        };
        let healthy_target = DocumentSyncTarget::Group {
            group_id: Ulid::from_parts(11, 2),
        };
        // A shard topic keys on (strategy, shard) alone, so the two records need
        // different placements to ride a blocked and a healthy topic.
        let blocked_placement = admin_placement(1);
        let healthy_placement = admin_placement(2);
        let blocked_topic = blocked_target.sync_topic_id(realm_id, &blocked_placement);
        let healthy_topic = healthy_target.sync_topic_id(realm_id, &healthy_placement);
        net.ensure_document_sync_topics(&[healthy_topic], Vec::new())
            .expect("healthy topic genesis");
        let blocked = admin_outbox(realm_id, origin, 1, blocked_target, blocked_placement);
        let healthy = admin_outbox(realm_id, origin, 2, healthy_target, healthy_placement);
        let blocked_key = outbox_key(&blocked).to_vec();
        let healthy_key = outbox_key(&healthy).to_vec();
        write_outbox_record(&storage, &blocked).await;
        write_outbox_record(&storage, &healthy).await;
        let handler =
            OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 2);

        handler.drain_document_sync_outbox().await;
        assert_eq!(
            scheduled_after(&task_handle).await,
            OUTBOX_CONTINUATION_AFTER
        );
        handler.drain_document_sync_outbox().await;
        assert_eq!(
            read_outbox_record(&storage, &healthy_key)
                .await
                .expect("read blocked-origin record"),
            Some(healthy.clone()),
            "a later origin sequence must remain blocked across pages"
        );
        net.ensure_document_sync_topics(&[blocked_topic], Vec::new())
            .expect("blocked topic genesis");
        for _ in 0..3 {
            handler.drain_document_sync_outbox().await;
        }
        assert_eq!(
            read_outbox_record(&storage, &blocked_key)
                .await
                .expect("read admin retry"),
            None
        );
        assert_eq!(
            read_outbox_record(&storage, &healthy_key)
                .await
                .expect("read admin suffix"),
            None
        );
        shutdown_net(&net).await;
    }

    // Closing a rotation returns its accumulated totals and clears every
    // ordering block, so the next rotation starts clean at the head.
    #[test]
    fn rotation_close_clears() {
        let mut rotation = OutboxRotation {
            cursor: Some(b"somewhere".to_vec()),
            continuations: 3,
            ..OutboxRotation::default()
        };
        rotation
            .blocked_topics
            .insert(irokle::TopicId::hash(b"blocked"));
        rotation.blocked_origins.insert(node(4));
        rotation
            .undeliverable_topics
            .insert(irokle::TopicId::hash(b"undeliverable"));
        rotation.totals.examined = 12;
        rotation.totals.deleted = 5;
        rotation.totals.invocations = 2;

        let totals = rotation.close();

        assert_eq!(totals.examined, 12);
        assert_eq!(totals.deleted, 5);
        assert_eq!(totals.invocations, 2);
        assert!(rotation.cursor.is_none());
        assert!(rotation.blocked_topics.is_empty());
        assert!(rotation.blocked_origins.is_empty());
        assert!(rotation.undeliverable_topics.is_empty());
        assert_eq!(rotation.continuations, 0);
        assert_eq!(rotation.totals.examined, 0);
    }

    #[test]
    fn rotation_holds_boundary() {
        let mut rotation = OutboxRotation {
            boundary: Some(b"b".to_vec()),
            cursor: Some(b"a".to_vec()),
            ..OutboxRotation::default()
        };

        assert!(rotation.admits(b"a"));
        assert!(rotation.admits(b"b"));
        assert!(!rotation.admits(b"c"));
        assert!(!rotation.at_end(Some(b"a")));
        assert!(rotation.at_end(Some(b"b")));

        rotation.close();
        assert!(rotation.boundary.is_none());
        assert!(rotation.admits(b"appended"));
    }

    #[tokio::test(start_paused = true)]
    async fn close_routes_defer() {
        let _clock = freeze_clock();
        let (_dir, handler, task_handle) = outbox_handler();
        let key = TaskKey::DrainDocumentSyncOutbox;
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .insert(key.clone(), 3);
        let rotation = OutboxRotation {
            totals: RotationTotals {
                deferred: 2,
                deleted: 1,
                invocations: 2,
                ..RotationTotals::default()
            },
            ..OutboxRotation::default()
        };

        handler.close_rotation(key.clone(), rotation).await;

        assert_eq!(
            scheduled_after(&task_handle).await,
            DOCUMENT_SYNC_DEFER_RETRY_AFTER
        );
        assert!(
            !handler
                .retry_backoff
                .lock()
                .expect("retry backoff mutex poisoned")
                .contains_key(&key),
            "progress in the clean suffix resets failure backoff"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn close_keeps_retry() {
        let _clock = freeze_clock();
        let (_dir, handler, task_handle) = outbox_handler();
        let key = TaskKey::DrainDocumentSyncOutbox;
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .insert(key.clone(), 3);
        let rotation = OutboxRotation {
            totals: RotationTotals {
                retry_invocations: 1,
                invocations: 2,
                ..RotationTotals::default()
            },
            ..OutboxRotation::default()
        };

        handler.close_rotation(key.clone(), rotation).await;

        assert_eq!(scheduled_after(&task_handle).await, Duration::from_secs(2));
        assert_eq!(
            handler
                .retry_backoff
                .lock()
                .expect("retry backoff mutex poisoned")
                .get(&key),
            Some(&4)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn close_resets_progress() {
        let _clock = freeze_clock();
        let (_dir, handler, task_handle) = outbox_handler();
        let key = TaskKey::DrainDocumentSyncOutbox;
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .insert(key.clone(), 3);
        let rotation = OutboxRotation {
            totals: RotationTotals {
                // A retry on an early page followed by a clean suffix made
                // progress, so the next retry returns to the base interval.
                deleted: 1,
                retry_invocations: 1,
                invocations: 2,
                ..RotationTotals::default()
            },
            ..OutboxRotation::default()
        };

        handler.close_rotation(key.clone(), rotation).await;

        assert_eq!(
            scheduled_after(&task_handle).await,
            Duration::from_millis(250)
        );
        assert_eq!(
            handler
                .retry_backoff
                .lock()
                .expect("retry backoff mutex poisoned")
                .get(&key),
            Some(&1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_suffix_closes() {
        let _clock = freeze_clock();
        let (_dir, handler, task_handle) = outbox_handler();
        let key = TaskKey::DrainDocumentSyncOutbox;
        // The first page retried; a clean suffix made progress before the
        // rotation reached its observed high-water boundary.
        let rotation = OutboxRotation {
            boundary: Some(b"last".to_vec()),
            cursor: Some(b"first".to_vec()),
            totals: RotationTotals {
                deleted: 1,
                retry_invocations: 1,
                invocations: 2,
                ..RotationTotals::default()
            },
            ..OutboxRotation::default()
        };

        handler.finish_retry(key.clone(), rotation, true).await;

        assert_eq!(
            scheduled_after(&task_handle).await,
            Duration::from_millis(250)
        );
        let rotation = handler.rotation.lock().expect("rotation lock");
        assert!(rotation.boundary.is_none());
        assert!(rotation.cursor.is_none());
        assert_eq!(
            handler
                .retry_backoff
                .lock()
                .expect("retry backoff mutex poisoned")
                .get(&key),
            Some(&1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn midpoint_retry_keeps() {
        let _clock = freeze_clock();
        let (_dir, handler, task_handle) = outbox_handler();
        let key = TaskKey::DrainDocumentSyncOutbox;
        let topic = irokle::TopicId::hash(b"midpoint-retry-topic");
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .insert(key.clone(), 3);
        let mut rotation = OutboxRotation {
            boundary: Some(b"last".to_vec()),
            cursor: Some(b"middle".to_vec()),
            totals: RotationTotals {
                retry_invocations: 1,
                invocations: 1,
                ..RotationTotals::default()
            },
            ..OutboxRotation::default()
        };
        rotation.blocked_topics.insert(topic);

        handler.finish_retry(key.clone(), rotation, false).await;

        {
            let rotation = handler.rotation.lock().expect("rotation lock");
            assert_eq!(rotation.cursor.as_deref(), Some(b"middle".as_slice()));
            assert!(rotation.blocked_topics.contains(&topic));
            assert_eq!(rotation.totals.retry_invocations, 1);
        }
        assert_eq!(scheduled_after(&task_handle).await, Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn empty_resets_backoff() {
        let _clock = freeze_clock();
        let (_dir, handler, _task_handle) = outbox_handler();
        let key = TaskKey::DrainDocumentSyncOutbox;
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .insert(key.clone(), 3);

        assert!(
            handler
                .open_rotation(&key, OutboxRotation::default())
                .await
                .is_none()
        );
        assert!(
            !handler
                .retry_backoff
                .lock()
                .expect("retry backoff mutex poisoned")
                .contains_key(&key)
        );
    }

    #[tokio::test]
    async fn blocked_keeps_backoff() {
        let _clock = freeze_clock();
        let realm_id = RealmId::from_bytes([48u8; 32]);
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let net = make_net_handle(realm_id, &storage, [48u8; 32]).await;
        tokio::time::pause();
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        let handler =
            OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 1);
        let key = TaskKey::DrainDocumentSyncOutbox;
        handler
            .retry_backoff
            .lock()
            .expect("retry backoff mutex poisoned")
            .insert(key.clone(), 3);
        let mut blocked_change = change();
        blocked_change.placement = aruna_core::structs::PlacementRef {
            strategy_id: Ulid::from_bytes([48; 16]),
            shard: 1,
        };
        let record = crate::sync::document_sync_outbox::new_outbox_record(
            node(1),
            target(),
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"blocked".to_vec(),
                change: blocked_change,
            },
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        write_outbox_record(&storage, &record).await;

        handler.drain_document_sync_outbox().await;

        assert_eq!(
            scheduled_after(&task_handle).await,
            DOCUMENT_SYNC_DEFER_RETRY_AFTER
        );
        assert_eq!(
            handler
                .retry_backoff
                .lock()
                .expect("retry backoff mutex poisoned")
                .get(&key),
            Some(&3)
        );
        shutdown_net(&net).await;
    }

    // The invocation bound is a work-unit limit, never a latency assertion.
    #[test]
    fn outbox_bound_finite() {
        assert_eq!(
            OUTBOX_INVOCATION_RECORDS,
            OUTBOX_INVOCATION_PAGES * OUTBOX_DRAIN_BATCH_SIZE
        );
        assert_ne!(OUTBOX_INVOCATION_RECORDS, 0);
    }

    #[test]
    fn drain_partition_publishes_all_records_of_an_available_topic_in_fifo_order() {
        let topic = irokle::TopicId::hash(b"shard-genesis-present");
        let records = vec![
            (b"older".to_vec(), shard_topic_record(1), topic),
            (b"newer".to_vec(), shard_topic_record(2), topic),
        ];

        let mut defer = DrainDeferState::default();
        let (to_publish, deferred, undeliverable) =
            partition_drain_records(records, &mut defer, true, |_| true, |_| DeferOutcome::Retry);

        assert!(deferred.is_empty());
        assert!(undeliverable.is_empty());
        let keys: Vec<Vec<u8>> = to_publish.into_iter().map(|(key, _, _)| key).collect();
        assert_eq!(keys, vec![b"older".to_vec(), b"newer".to_vec()]);
    }

    // A record for a bucket this node does not hold can never publish: it may
    // neither mint the topic's genesis nor join the topic. Deferring it forever
    // would be silent data loss, so it is separated out to be dropped loudly.
    #[test]
    fn unheld_bucket_records_are_undeliverable() {
        let topic = irokle::TopicId::hash(b"unheld-bucket");
        let records = vec![
            (b"older".to_vec(), shard_topic_record(1), topic),
            (b"newer".to_vec(), shard_topic_record(2), topic),
        ];

        let mut classified = 0usize;
        let mut defer = DrainDeferState::default();
        let (to_publish, deferred, undeliverable) = partition_drain_records(
            records,
            &mut defer,
            true,
            |_| false,
            |_| {
                classified += 1;
                DeferOutcome::Undeliverable
            },
        );

        assert_eq!(classified, 1, "holdership is decided once per topic");
        assert!(to_publish.is_empty());
        assert!(deferred.is_empty());
        let keys: Vec<Vec<u8>> = undeliverable.into_iter().map(|(key, _, _)| key).collect();
        assert_eq!(keys, vec![b"older".to_vec(), b"newer".to_vec()]);
    }

    // A full first page of records for a genesis-less shard topic (all deferred)
    // must not starve records for other topics behind it in the FIFO: the drain
    // pages the whole outbox per run, so a later-page record still publishes.
    #[tokio::test(start_paused = true)]
    async fn deferred_head_paginates() {
        let _clock = freeze_clock();
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
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
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
                shard: 3,
            },
        };
        let deferred_target = DocumentSyncTarget::MetadataRegistry {
            group_id: Ulid::from_parts(1, 1),
            document_id: Ulid::from_parts(2, 2),
        };
        let mut writes = Vec::with_capacity(OUTBOX_DRAIN_BATCH_SIZE + 1);
        for index in 0..OUTBOX_DRAIN_BATCH_SIZE {
            let record = crate::sync::document_sync_outbox::new_outbox_record_with_id(
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
                crate::sync::document_sync_outbox::outbox_write_entry(&record)
                    .expect("outbox entry"),
            );
        }

        // One later origin record for a shared (non-shard) topic, ordered
        // strictly after the head page, so only pagination reaches it.
        let publish_record = crate::sync::document_sync_outbox::new_outbox_record_with_id(
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
        writes.push(
            crate::sync::document_sync_outbox::outbox_write_entry(&publish_record).expect("entry"),
        );

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

        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
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
        assert_eq!(
            scheduled_after(&task_handle).await,
            DOCUMENT_SYNC_DEFER_RETRY_AFTER,
            "an early defer followed by a clean suffix keeps the aggregate retry"
        );

        shutdown_net(&net).await;
    }

    struct BoundaryHarness {
        _dir: tempfile::TempDir,
        storage: aruna_storage::StorageHandle,
        net: NetHandle,
        task_handle: TaskHandle,
        handler: OperationsTaskHandler,
        realm_id: RealmId,
        appended: Vec<(Vec<u8>, DocumentSyncOutboxRecord)>,
    }

    impl BoundaryHarness {
        async fn new() -> Self {
            let realm_id = RealmId::from_bytes([45u8; 32]);
            let dir = tempdir().expect("temp dir");
            let storage =
                FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
            let net = make_net_handle(realm_id, &storage, [45u8; 32]).await;
            tokio::time::pause();
            let task_handle = TaskHandle::new();
            let context = Arc::new(DriverContext {
                storage_handle: storage.clone(),
                net_handle: Some(net.clone()),
                blob_handle: None,
                metadata_handle: None,
                task_handle: Some(task_handle.clone()),
                compute_handle: None,
            });
            let handler =
                OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 1);
            Self {
                _dir: dir,
                storage,
                net,
                task_handle,
                handler,
                realm_id,
                appended: Vec::new(),
            }
        }

        fn placed_change(&self) -> DocumentSyncChange {
            let mut value = change();
            value.placement = aruna_core::structs::PlacementRef {
                strategy_id: Ulid::from_bytes([45; 16]),
                shard: 1,
            };
            value
        }

        fn record(
            &self,
            id: u128,
            target: DocumentSyncTarget,
            event: DocumentSyncOutboxEvent,
        ) -> DocumentSyncOutboxRecord {
            crate::sync::document_sync_outbox::new_outbox_record_with_id(
                Ulid::from_parts(1, id),
                node(1),
                target,
                Vec::new(),
                event,
                aruna_core::structs::PlacementRef::NIL,
                true,
            )
        }

        async fn seed_records(&self) {
            for id in 1..=3 {
                let record = self.record(
                    id,
                    target(),
                    DocumentSyncOutboxEvent::Upsert {
                        bytes: id.to_be_bytes().to_vec(),
                        change: self.placed_change(),
                    },
                );
                write_outbox_record(&self.storage, &record).await;
            }
            let initial = self.record(
                0,
                DocumentSyncTarget::RealmAuthorization {
                    realm_id: self.realm_id,
                },
                DocumentSyncOutboxEvent::Delete { change: change() },
            );
            write_outbox_record(&self.storage, &initial).await;
        }

        async fn append_records(&mut self) {
            let shared = DocumentSyncTarget::RealmAuthorization {
                realm_id: self.realm_id,
            };
            let topic =
                shared.sync_topic_id(self.realm_id, &aruna_core::structs::PlacementRef::NIL);
            self.net
                .ensure_document_sync_topics(&[topic], Vec::new())
                .expect("appended topic genesis");
            let records = [
                self.record(
                    4,
                    shared,
                    DocumentSyncOutboxEvent::Delete { change: change() },
                ),
                self.record(
                    5,
                    target(),
                    DocumentSyncOutboxEvent::Upsert {
                        bytes: b"appended-upsert".to_vec(),
                        change: self.placed_change(),
                    },
                ),
            ];
            for record in records {
                let key = outbox_key(&record).to_vec();
                write_outbox_record(&self.storage, &record).await;
                self.appended.push((key, record));
            }
        }

        async fn append_later(&mut self) {
            let record = self.record(
                6,
                target(),
                DocumentSyncOutboxEvent::Upsert {
                    bytes: b"appended-later".to_vec(),
                    change: self.placed_change(),
                },
            );
            let key = outbox_key(&record).to_vec();
            write_outbox_record(&self.storage, &record).await;
            self.appended.push((key, record));
        }

        fn assert_rotation(&self, examined: usize, cursor: bool, continuations: u32) {
            let rotation = self.handler.rotation.lock().expect("rotation lock");
            assert_eq!(
                (rotation.totals.examined, rotation.cursor.is_some()),
                (examined, cursor)
            );
            assert_eq!(rotation.continuations, continuations);
        }

        async fn assert_appends(&self) {
            for (key, record) in &self.appended {
                assert_eq!(
                    read_outbox_record(&self.storage, key)
                        .await
                        .expect("read appended record"),
                    Some(record.clone())
                );
            }
        }

        async fn finish_retry(self) {
            let shard_topic =
                target().sync_topic_id(self.realm_id, &self.placed_change().placement);
            self.net
                .ensure_document_sync_topics(&[shard_topic], Vec::new())
                .expect("blocked head topic genesis");
            for _ in 0..6 {
                self.handler.drain_document_sync_outbox().await;
            }
            let remaining = read_outbox_records(&self.storage, &[], None, 8)
                .await
                .expect("read retried records");
            assert!(
                remaining.records.is_empty(),
                "records across all streams must retry after the rotation closes"
            );
            shutdown_net(&self.net).await;
        }
    }

    #[tokio::test]
    async fn boundary_appends_wait() {
        let _clock = freeze_clock();
        let mut harness = BoundaryHarness::new().await;
        harness.seed_records().await;

        harness.handler.drain_document_sync_outbox().await;
        harness.assert_rotation(1, true, 1);
        assert_eq!(
            scheduled_after(&harness.task_handle).await,
            OUTBOX_CONTINUATION_AFTER
        );

        harness.append_records().await;
        harness.handler.drain_document_sync_outbox().await;
        harness.assert_rotation(2, true, 0);
        assert_eq!(
            scheduled_after(&harness.task_handle).await,
            DOCUMENT_SYNC_DEFER_RETRY_AFTER
        );

        harness.append_later().await;
        harness.handler.drain_document_sync_outbox().await;
        harness.assert_rotation(3, true, 1);
        assert_eq!(
            scheduled_after(&harness.task_handle).await,
            OUTBOX_CONTINUATION_AFTER
        );

        harness.handler.drain_document_sync_outbox().await;
        harness.assert_rotation(0, false, 0);
        harness.assert_appends().await;
        harness.finish_retry().await;
    }

    #[tokio::test]
    async fn rotation_streak() {
        let _clock = freeze_clock();
        let realm_id = RealmId::from_bytes([51u8; 32]);
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = make_net_handle(realm_id, &storage, [51u8; 32]).await;
        tokio::time::pause();
        let target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let topic = target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
        net.ensure_document_sync_topics(&[topic], Vec::new())
            .expect("shared topic genesis");
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(
            1,
            1,
            OUTBOX_CONTINUATION_STREAK,
        );
        let total = u128::from(OUTBOX_CONTINUATION_STREAK) + 2;
        for index in 1..=total {
            let record = crate::sync::document_sync_outbox::new_outbox_record_with_id(
                Ulid::from_parts(1, index),
                node(1),
                target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::Upsert {
                    bytes: index.to_be_bytes().to_vec(),
                    change: change(),
                },
                aruna_core::structs::PlacementRef::NIL,
                true,
            );
            write_outbox_record(&storage, &record).await;
        }

        for expected in 1..=OUTBOX_CONTINUATION_STREAK {
            handler.drain_document_sync_outbox().await;
            assert_eq!(
                handler
                    .rotation
                    .lock()
                    .expect("rotation lock")
                    .continuations,
                expected
            );
            assert_eq!(
                scheduled_after(&task_handle).await,
                OUTBOX_CONTINUATION_AFTER
            );
        }
        handler.drain_document_sync_outbox().await;
        assert_eq!(
            handler
                .rotation
                .lock()
                .expect("rotation lock")
                .continuations,
            0
        );
        assert_eq!(
            scheduled_after(&task_handle).await,
            DOCUMENT_SYNC_DEFER_RETRY_AFTER
        );
        for _ in 0..4 {
            handler.drain_document_sync_outbox().await;
        }
        assert!(
            read_outbox_records(&storage, &[], None, 32)
                .await
                .expect("read streak records")
                .records
                .is_empty()
        );
        shutdown_net(&net).await;
    }

    struct ConfigHarness {
        _dir: tempfile::TempDir,
        storage: aruna_storage::StorageHandle,
        net: NetHandle,
        handler: OperationsTaskHandler,
        config: RealmConfigDocument,
        realm_id: RealmId,
        placement: aruna_core::structs::PlacementRef,
        shard_target: DocumentSyncTarget,
    }

    async fn config_setup() -> ConfigHarness {
        let realm_id = RealmId::from_bytes([47u8; 32]);
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let net = make_net_handle(realm_id, &storage, [47u8; 32]).await;
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        write_realm_config(&storage, realm_id, &config, net.node_id()).await;
        let placement = aruna_core::structs::PlacementRef {
            strategy_id: config.strategies[0].strategy_id,
            shard: 0,
        };
        let shared_target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let shard_target = DocumentSyncTarget::MetadataRegistry {
            group_id: Ulid::from_parts(7, 1),
            document_id: Ulid::from_parts(8, 1),
        };
        let shared_topic =
            shared_target.sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
        net.ensure_document_sync_topics(&[shared_topic], Vec::new())
            .expect("shared topic genesis");
        let mut shard_change = change();
        shard_change.placement = placement;
        let shared = crate::sync::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, 1),
            node(1),
            shared_target,
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"shared".to_vec(),
                change: change(),
            },
            aruna_core::structs::PlacementRef::NIL,
            true,
        );
        let shard = crate::sync::document_sync_outbox::new_outbox_record_with_id(
            Ulid::from_parts(1, 2),
            node(1),
            shard_target.clone(),
            Vec::new(),
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"shard".to_vec(),
                change: shard_change,
            },
            placement,
            true,
        );
        write_outbox_record(&storage, &shared).await;
        write_outbox_record(&storage, &shard).await;

        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let handler =
            OperationsTaskHandler::new(context, JobsRuntime::new()).with_outbox_limits(1, 1, 2);
        ConfigHarness {
            _dir: temp_dir,
            storage,
            net,
            handler,
            config,
            realm_id,
            placement,
            shard_target,
        }
    }

    #[tokio::test]
    async fn config_reloads_between() {
        let ConfigHarness {
            _dir,
            storage,
            net,
            handler,
            mut config,
            realm_id,
            placement,
            shard_target,
        } = config_setup().await;
        handler.drain_document_sync_outbox().await;
        {
            let rotation = handler.rotation.lock().expect("rotation lock");
            assert!(rotation.cursor.is_some());
            assert_eq!(rotation.totals.deleted, 1);
        }

        config.ensure_node(net.node_id(), RealmNodeKind::Server);
        write_realm_config(&storage, realm_id, &config, net.node_id()).await;
        let shard_topic = shard_target.sync_topic_id(realm_id, &placement);
        net.ensure_document_sync_topics(&[shard_topic], Vec::new())
            .expect("updated holder topic genesis");

        handler.drain_document_sync_outbox().await;
        assert!(
            read_outbox_records(&storage, &[], None, 4)
                .await
                .expect("read revalidated records")
                .records
                .is_empty(),
            "the continuation must use the updated holder config"
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
            compute_handle: None,
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

        initialize_task_incoming(context.clone(), task_handle.clone(), JobsRuntime::new()).await;

        let strategy_id = config.strategies.first().expect("a strategy").strategy_id;
        let topic = aruna_core::document::shard_topic_id(
            realm_id,
            &aruna_core::structs::PlacementRef {
                strategy_id,
                shard: 0,
            },
        );
        assert!(
            !net.document_sync_topic_exists(topic).unwrap_or(false),
            "the rank-0 shard topic must not exist before the config change is drained"
        );

        let record = crate::sync::document_sync_outbox::new_outbox_record(
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
            .send_effect(crate::sync::document_sync_outbox::schedule_outbox_drain_effect())
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

    // Post-rebalance genesis adoption: live holders no longer include the emit-time
    // stamped holder carrying the genesis, so the bootstrap pull must union stamp and
    // live holders; otherwise a fresh genesis could fork the topic and evict writes.
    #[tokio::test]
    async fn pull_reaches_ex_holder() {
        let realm_id = RealmId::from_bytes([53u8; 32]);
        let ex_dir = tempdir().expect("temp dir");
        let ex_storage =
            FjallStorage::open(ex_dir.path().to_str().expect("temp path")).expect("storage opens");
        let ex_holder = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            ex_storage.clone(),
        )
        .await
        .expect("net handle");
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
        net.add_peer_addr(ex_holder.endpoint_addr()).await;
        ex_holder.add_peer_addr(net.endpoint_addr()).await;
        // The ex-holder must serve inbound sync streams for the pull to reach
        // its genesis.
        crate::sync::incoming::initialize_net_incoming(Arc::new(DriverContext {
            storage_handle: ex_storage.clone(),
            net_handle: Some(ex_holder.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        }));

        // The live config resolves the shard's holders to this node only: the
        // stamped ex-holder has been rebalanced out.
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
        // The ex-holder keeps the realm config that rebalanced it out, so it
        // still admits inbound sync from the current holders.
        ex_holder
            .refresh_realm_peers_from_document(&config)
            .await
            .expect("ex-holder refreshes realm peers");

        let strategy_id = config.strategies.first().expect("a strategy").strategy_id;
        let placement = aruna_core::structs::PlacementRef {
            strategy_id,
            shard: 0,
        };
        let target = DocumentSyncTarget::MetadataRegistry {
            group_id: Ulid::from_parts(1, 1),
            document_id: Ulid::from_parts(2, 2),
        };
        let topic = target.sync_topic_id(realm_id, &placement);

        // Only the ex-holder carries the genesis (with this node as a member,
        // as the pre-rebalance membership reconciliation would have left it).
        ex_holder
            .ensure_document_sync_topics(&[topic], vec![net.node_id()])
            .expect("genesis on the ex-holder");
        assert!(!net.document_sync_topic_exists(topic).unwrap_or(true));

        let mut change = change();
        change.placement = placement;
        let record = crate::sync::document_sync_outbox::new_outbox_record(
            net.node_id(),
            target,
            vec![ex_holder.node_id()],
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"doc".to_vec(),
                change,
            },
            aruna_core::structs::PlacementRef::NIL,
            false,
        );
        let record_key = outbox_key(&record).to_vec();
        write_outbox_record(&storage, &record).await;

        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            handler.drain_document_sync_outbox().await;
            if read_outbox_record(&storage, &record_key)
                .await
                .expect("read outbox record")
                .is_none()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "record never published: the drain did not adopt the ex-holder's genesis"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            net.document_sync_topic_exists(topic).unwrap_or(false),
            "the genesis must be adopted from the stamped ex-holder"
        );

        ex_holder.shutdown().await;
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
        let record = crate::sync::document_sync_outbox::new_outbox_record(
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

    #[tokio::test(start_paused = true)]
    async fn restore_document_sync_outbox_timers_keeps_existing_backoff_timer() {
        let _clock = freeze_clock();
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let record = crate::sync::document_sync_outbox::new_outbox_record(
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

        let TaskEvent::TimerScheduled { after, .. } = task_handle
            .schedule_timer_if_idle(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
            .await
        else {
            panic!("expected timer schedule event");
        };
        assert_eq!(
            after,
            Duration::from_secs(3600),
            "durable rearm must preserve the active backoff deadline"
        );
    }

    #[tokio::test]
    async fn installed_fence() {
        let _clock = freeze_clock();
        let InstalledHarness {
            _dir,
            task_handle,
            context,
            handler,
            mut completed,
            net,
            ..
        } = installed_setup().await;

        drive_document_sync_outbox_drain(context.clone()).await;
        assert!(recv_progress(&mut completed).await);
        {
            let rotation = handler.rotation.lock().expect("rotation lock");
            assert_eq!(rotation.totals.examined, 1);
            assert!(rotation.cursor.is_some());
            assert_eq!(rotation.continuations, 1);
        }
        assert_eq!(
            scheduled_after(&task_handle).await,
            OUTBOX_CONTINUATION_AFTER
        );
        assert!(completed.try_recv().is_err());

        drive_document_sync_outbox_drain(context.clone()).await;
        assert_eq!(
            scheduled_after(&task_handle).await,
            OUTBOX_CONTINUATION_AFTER
        );
        assert!(completed.try_recv().is_err());

        drive_document_sync_outbox_drain(context).await;
        assert_eq!(
            scheduled_after(&task_handle).await,
            OUTBOX_CONTINUATION_AFTER
        );
        assert!(completed.try_recv().is_err());

        shutdown_net(&net).await;
    }

    #[tokio::test]
    async fn installed_continues() {
        let _clock = freeze_clock();
        let InstalledHarness {
            _dir,
            storage,
            net,
            context,
            mut completed,
            ..
        } = installed_setup().await;

        drive_document_sync_outbox_drain(context).await;
        assert!(recv_progress(&mut completed).await);
        // Tokio rounds a timer deadline up to the next millisecond, so the clock
        // has to pass the interval rather than land exactly on it.
        tokio::time::advance(OUTBOX_CONTINUATION_AFTER + Duration::from_millis(1)).await;
        assert!(recv_progress(&mut completed).await);
        assert!(completed.try_recv().is_err());
        assert!(
            read_outbox_records(&storage, &[], None, 4)
                .await
                .expect("read drained records")
                .records
                .is_empty()
        );

        shutdown_net(&net).await;
    }

    #[tokio::test]
    async fn drain_keeps_timer() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("storage opens"))
            .expect("storage opens");
        let record = crate::sync::document_sync_outbox::new_outbox_record(
            node(1),
            target(),
            vec![node(2)],
            DocumentSyncOutboxEvent::Upsert {
                bytes: b"direct fence".to_vec(),
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

        drive_document_sync_outbox_drain(Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
        }))
        .await;

        assert!(
            tokio::time::timeout(Duration::from_millis(50), seen_rx.recv())
                .await
                .is_err(),
            "the direct fence must not replace the active timer"
        );
        let TaskEvent::TimerScheduled { after, .. } = task_handle
            .schedule_timer_if_idle(TaskKey::DrainDocumentSyncOutbox, Duration::ZERO)
            .await
        else {
            panic!("expected timer schedule event");
        };
        assert!(after > Duration::from_secs(3000));
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
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
        let record = crate::sync::document_sync_outbox::new_outbox_record(
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
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
        let record = crate::sync::document_sync_outbox::new_outbox_record(
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
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
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
    async fn drain_reconcile_wakes() {
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
        let mut revisions = net.subscribe_dashboard_changes();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());

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
        tokio::time::timeout(Duration::from_secs(2), revisions.changed())
            .await
            .expect("dashboard revision arrives")
            .expect("channel open");
        assert_eq!(*revisions.borrow_and_update(), 1);
        net.shutdown().await;
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
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());

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
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());

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
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
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
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
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
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
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
            let candidate = UserId::new(Ulid::generate(), realm_id);
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
            compute_handle: None,
        });
        crate::sync::incoming::initialize_net_incoming(context_b.clone());

        let context_a = Arc::new(DriverContext {
            storage_handle: storage_a.clone(),
            net_handle: Some(net_a),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let handler = OperationsTaskHandler::new(context_a, JobsRuntime::new());
        handler.drain_notification_outbox().await;

        assert_eq!(read_inbox_records(&storage_b).await, vec![record]);
        let remaining = read_notification_outbox_batch(&storage_a, None, 1024, None)
            .await
            .expect("outbox read");
        assert!(remaining.records.is_empty());
    }

    #[tokio::test]
    async fn device_skips_holders() {
        // A device publishes no holder record, so the refresh does not even arm
        // its own timer.
        let realm_id = RealmId::from_bytes([57u8; 32]);
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
            compute_handle: None,
        });

        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(
            net.node_id(),
            RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        );
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

        OperationsTaskHandler::new(context, JobsRuntime::new())
            .refresh_blob_holders()
            .await;

        let timer = storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                key: ByteView::from(
                    postcard::to_allocvec(&TaskKey::RefreshBlobHolders).expect("timer key"),
                ),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            timer,
            Event::Storage(StorageEvent::ReadResult { value: None, .. })
        ));

        net.shutdown().await;
    }
}
