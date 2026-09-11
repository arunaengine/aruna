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

pub use outbox::drive_document_sync_outbox_drain;
pub use restore::{drain_notification_outbox, initialize_task_holder, initialize_task_incoming};

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
}

/// The durable queue work deferred until after the local serving gate.
pub struct TaskQueues {
    context: Arc<DriverContext>,
    task_handle: TaskHandle,
    handler: Arc<OperationsTaskHandler>,
    refresh_holders: bool,
}

/// A document sync drain that keeps its rotation across invocations like the
/// timer-driven handler. A fresh drainer starts at the head, which is the same
/// reset a process restart performs.
pub struct OutboxDrainer {
    handler: Arc<OperationsTaskHandler>,
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
    use harness::*;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    use crate::notifications::outbox::new_notification_outbox_record;

    mod harness;
    mod notification;
    mod outbox;
    mod restore;
}
