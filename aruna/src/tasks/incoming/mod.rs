//! Owns the background task handler and the queues that run its timers and drains.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use aruna_clock::unix_timestamp_millis;
use aruna_core::document::{
    DocumentEffect, DocumentNetEvent, DocumentOutboxEvent, DocumentOutboxRecord,
    DocumentSyncPublish, DocumentTarget,
};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::execution::job::identity::JobExecutionClass;
use aruna_core::structs::execution::job::input::RoCrateLimits;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::Key;
use aruna_tasks::shutdown::Shutdown;
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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

use aruna_drivers::driver::{DriverContext, drive, drive_retry, node_routing};
use aruna_drivers::jobs::runtime::JobsRuntime;
use aruna_operations::blob::reclaim_sweep::{ReclaimSweepOperation, reclaim_plan};
use aruna_operations::blob::sweep_timer::ArmSweepOperation;
use aruna_operations::device::publish_queue::DrainOutcome;
use aruna_operations::device::sync::{
    RECONCILE_CONTINUE_AFTER, RECONCILE_IDLE_AFTER, RECONCILE_RETRY_AFTER,
};
use aruna_operations::device_node::sync_note::{NoteSyncOperation, SyncNote};
use aruna_operations::forward::realm::RelayAdminOperation;
use aruna_operations::jobs::DRAIN_RETRY_AFTER;
use aruna_operations::jobs::drain::{DrainJobsOperation, DrainTimerOperation, JobClassBudget};
use aruna_operations::jobs::prune::PruneTimerOperation;
use aruna_operations::jobs::store::job_calls;
use aruna_operations::metadata::materialization_queue::{
    NextTimerOperation, RequeueParkedOperation, UNKNOWN_CORES,
};
use aruna_operations::metadata::projection::ProjectionTimerOperation;
use aruna_operations::metadata::prune_queue::PruneTimerOperation as GraphPruneTimerOperation;

/// The logical cores this host offers, as the operating system reports them.
fn host_cores() -> usize {
    std::thread::available_parallelism().map_or(UNKNOWN_CORES, |cores| cores.get())
}

/// Arms the graph prune drain for its next due job at start.
async fn restore_graph_prune(
    context: &aruna_drivers::driver::DriverContext,
    task_handle: &aruna_tasks::TaskHandle,
) {
    match aruna_drivers::driver::drive(GraphPruneTimerOperation::new(), context).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let wake =
                aruna_core::effects::Effect::Task(aruna_core::task::TaskEffect::ResetTimer {
                    key: TaskKey::DrainPruneQueue,
                    after,
                });
            if let aruna_core::events::Event::Task(aruna_core::task::TaskEvent::Error {
                message,
                ..
            }) = task_handle.send_effect(wake).await
            {
                warn!(message = %message, "Failed to restore metadata graph prune timer");
            }
        }
        Err(error) => warn!(error = ?error, "Failed to scan metadata graph prune jobs"),
    }
}
use crate::tasks::task_persistence::restore_task_timers;
use aruna_core::row_plan::PlanOperation;
use aruna_core::structs::storage::encryption::{BucketKeyRef, KeyTicket};
use aruna_drivers::driver::node_facts;
use aruna_operations::notifications::email::RefreshMailOperation;
use aruna_operations::notifications::outbox::{DELIVERY_RETRY_AFTER, RestoreOutboxOperation};
use aruna_operations::notifications::prune::RestorePruneOperation;
use aruna_operations::notifications::watch::digest::InterestOperation;
use aruna_operations::notifications::watch::interest::RefreshInterestOperation;
use aruna_operations::notifications::watch::table::RebuildTableOperation;
use aruna_operations::queue_backoff::retry_delay_ms;
use aruna_operations::replication::drain::RestoreDrainOperation;
use aruna_operations::replication::mirror_repair::drain::RestoreTimerOperation;
use aruna_operations::s3::bucket::key::lock::LockBucketOperation;
use aruna_operations::sync::document_outbox::{
    DeleteOutboxPlan, OutboxPagePlan, OutboxTailsOperation, OutboxTimerOperation,
};
use aruna_operations::sync::outbox::defer::{
    DrainDeferState, DrainRecord, admin_origin, classify_deferred_record, partition_drain_records,
};
use aruna_operations::sync::outbox::record::OUTBOX_DRAIN_SIZE;
use aruna_operations::sync::reconcile::SyncTopicsOperation;
use aruna_operations::sync::shard_placement::DEFER_RETRY_AFTER;
use aruna_operations::triggers::{
    TimerInput, TimerMemory, TimerPlan, timer_needs, timer_operation,
};
use aruna_storage::task_timers::{delete_persisted_timer, hold_change, persist_stamped};
use folder_pass::{restore_publish_timer, restore_sync_timers};

pub(crate) mod drained;
pub mod folder_pass;
pub mod git_drain;
pub mod outbox;
pub mod restore;

/// Process-wide tally of document sync outbox records ever classified
/// undeliverable. The drain already error-logs each one; this exposes the count
/// so a test can assert the draining-flush path never black-holes a record.
pub static UNDELIVERABLE_RECORD_COUNT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

const DRAIN_SUBBATCH_RECORDS: usize = 512;
/// Outbox records remembered as published; past it they are forgotten and published once
/// more, which only costs log space.
const MAX_PUBLISHED_RECORDS: usize = 65_536;
/// Pages one drain invocation may examine, so a large or blocked queue cannot
/// monopolise a task invocation.
const OUTBOX_INVOCATION_PAGES: usize = 2;
const OUTBOX_INVOCATION_RECORDS: usize = OUTBOX_INVOCATION_PAGES * OUTBOX_DRAIN_SIZE;
/// Consecutive continuations before a rotation yields through the timer, so an
/// append-heavy or wholly blocked queue cannot keep the task continuously hot.
const OUTBOX_CONTINUATION_STREAK: u32 = 8;
const _: () = assert!(OUTBOX_INVOCATION_PAGES > 0 && OUTBOX_CONTINUATION_STREAK > 0);
const OUTBOX_CONTINUATION_AFTER: Duration = Duration::from_millis(50);
const QUEUE_REARM_AFTER: Duration = Duration::from_secs(5);

/// How long a device keeps its copy of the realm documents before asking for
/// them again. A revocation reaches it within this window at the latest.
const REALM_DOCUMENTS_AFTER: Duration = Duration::from_secs(60);

/// How long one attempt may spend on the realm as a whole. The folder cadence
/// is what it shares that time with, so it is bounded once, not per node.
const REALM_DOCUMENTS_BUDGET: Duration = Duration::from_secs(20);
/// Rearm ticks between dead-letter sweeps, i.e. one sweep a minute.
const DEAD_LETTER_TICKS: usize = 12;
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
pub(crate) struct OperationsTaskHandler {
    context: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    rocrate_limits: RoCrateLimits,
    // In-memory retry-attempt counters keyed by timer. Loss on restart is fine:
    // a restarted node simply retries from the base interval.
    retry_backoff: std::sync::Mutex<HashMap<TaskKey, u32>>,
    // What each converted timer keeps between runs; the operations decide on it. Loss on
    // restart is fine: a restarted timer starts afresh.
    timer_memory: std::sync::Mutex<HashMap<TaskKey, TimerMemory>>,
    // Rotation state of the bounded outbox drain. Loss on restart is fine: the
    // next rotation opens at the head.
    rotation: std::sync::Mutex<OutboxRotation>,
    // Outbox records whose op this process already created, so a retry only syncs them.
    // Loss on restart is fine: such a record is published once more.
    published_records: std::sync::Mutex<BTreeSet<Vec<u8>>>,
    drain_guard: tokio::sync::Mutex<()>,
    outbox_limits: OutboxLimits,
    // When a device may next fetch the realm documents, and how many attempts
    // have failed. Loss on restart is fine: a restart fetches them anyway.
    realm_documents: std::sync::Mutex<(u32, u64)>,
    // Signs registry publications; `None` where no node identity was supplied.
    node_capabilities: Option<NodeCapabilities>,
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
    blocked_origins: HashSet<aruna_core::id::NodeId>,
    /// Topics this node can never publish onto, for the rest of this rotation.
    undeliverable_topics: HashSet<irokle::TopicId>,
    totals: RotationTotals,
    /// Continuations already spent without yielding through the timer.
    continuations: u32,
}

struct DrainSubBatch {
    peers: Vec<aruna_core::id::NodeId>,
    documents: Vec<DocumentSyncPublish>,
    topics: Vec<irokle::TopicId>,
    /// Admin origin of each entry, so a blocked publish also blocks the rest of
    /// that origin's sequence.
    origins: Vec<Option<aruna_core::id::NodeId>>,
    targets: Vec<DocumentTarget>,
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
    blocked_origins: Vec<aruna_core::id::NodeId>,
}

type StuckRecord = (
    u64,
    DocumentTarget,
    irokle::TopicId,
    aruna_core::structs::placement::record::PlacementRef,
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
        records: Vec<(Vec<u8>, DocumentOutboxRecord)>,
        has_more: bool,
        boundary_reached: bool,
    },
    Skip,
    Stop,
}

impl OperationsTaskHandler {
    pub(crate) fn new(context: Arc<DriverContext>, jobs_runtime: Arc<JobsRuntime>) -> Self {
        Self {
            context,
            jobs_runtime,
            rocrate_limits: RoCrateLimits::default(),
            retry_backoff: std::sync::Mutex::new(HashMap::new()),
            timer_memory: std::sync::Mutex::new(HashMap::new()),
            rotation: std::sync::Mutex::new(OutboxRotation::default()),
            published_records: std::sync::Mutex::new(BTreeSet::new()),
            drain_guard: tokio::sync::Mutex::new(()),
            outbox_limits: OutboxLimits::default(),
            realm_documents: std::sync::Mutex::new((0, 0)),
            node_capabilities: None,
        }
    }

    fn with_node_capabilities(mut self, capabilities: Option<NodeCapabilities>) -> Self {
        self.node_capabilities = capabilities;
        self
    }

    fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_outbox_limits(
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

    /// Handles records this node cannot publish itself: admin records are relayed under
    /// this node's origin signature and deleted once a holder takes custody, while
    /// unsigned upserts/deletes stay in the outbox. Returns keys whose custody moved.
    async fn relay_undeliverable_records(
        &self,
        config: Option<&aruna_core::structs::identity::realm::RealmConfigDocument>,
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
            if device && !matches!(record.event, DocumentOutboxEvent::AdminOperation { .. }) {
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
    fn is_device(
        &self,
        config: Option<&aruna_core::structs::identity::realm::RealmConfigDocument>,
    ) -> bool {
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            return false;
        };
        config
            .and_then(|config| {
                aruna_operations::realm::mutate_placement::node_kind(config, net_handle.node_id())
            })
            .is_some_and(|kind| !kind.is_sync_eligible())
    }

    /// Signs one locally originated administrative envelope and hands it to a
    /// holder. Returns whether a holder took custody.
    async fn relay_admin_record(
        &self,
        config: &aruna_core::structs::identity::realm::RealmConfigDocument,
        record: &DocumentOutboxRecord,
    ) -> bool {
        let DocumentOutboxEvent::AdminOperation {
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
        let holders = aruna_core::placement::resolve_shard_holders(config, &record.placement);
        if holders.is_empty() {
            return false;
        }
        match drive(
            RelayAdminOperation::new(
                self.context.node_and_realm().map(|(node_id, _)| node_id),
                &holders,
                record.target.clone(),
                event.clone(),
                record.placement,
                origin_signature,
            ),
            &self.context,
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
        Duration::from_millis(retry_delay_ms(attempts))
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
        let _held = hold_change(&effect).await;
        let stamp = match persist_stamped(&self.context.storage_handle, &effect).await {
            Ok(stamp) => stamp,
            Err(message) => {
                warn!(task_id = ?key, message = %message, "Failed to persist timer re-arm");
                return false;
            }
        };
        let Some(task_handle) = self.context.task_handle.as_ref() else {
            warn!(task_id = ?key, "Cannot re-arm failed timer without task handle");
            return false;
        };
        match task_handle.send_stamped(effect, stamp).await {
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

    /// Re-arms a timer in memory even when storage cannot persist it, so an outage does not end
    /// the retries; the persisted copy is best effort.
    async fn keep_retrying(&self, key: TaskKey, after: std::time::Duration) {
        let effect = TaskEffect::ResetTimer {
            key: key.clone(),
            after,
        };
        let _held = hold_change(&effect).await;
        let stamp = match persist_stamped(&self.context.storage_handle, &effect).await {
            Ok(stamp) => stamp,
            Err(message) => {
                warn!(task_id = ?key, message = %message, "Retry timer kept in memory only");
                None
            }
        };
        if let Some(task_handle) = self.context.task_handle.as_ref() {
            task_handle.send_stamped(effect, stamp).await;
        }
    }

    // Kicks the placement reconciler immediately (not persisted; it is re-derived
    // from the realm config at startup by `RestoreTopicsOperation`).
    async fn schedule_sync_placements(&self, realm_id: RealmId, node_id: aruna_core::id::NodeId) {
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
        let fetched = drive(
            aruna_operations::device_node::realm_documents::fetch_realm_documents(
                self.context.node_and_realm(),
                REALM_DOCUMENTS_BUDGET,
            ),
            &self.context,
        )
        .await
        .unwrap_or(false);
        let note = match fetched {
            true => SyncNote::Contact,
            false => SyncNote::Failure,
        };
        let note = NoteSyncOperation::new(note, unix_timestamp_millis());
        if let Err(error) = drive(note, &self.context).await {
            warn!(%error, "Failed to note the device sync state");
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
            false => retry_delay_ms(attempts),
        };
        *state = (attempts, now.saturating_add(after));
    }

    /// Refreshes the metadata replicas this device keeps, on the same beat the
    /// folders run on. A node that keeps none does nothing.
    async fn refresh_device_replicas(&self) {
        let pass = aruna_operations::metadata::device::refresh::refresh_replicas(
            self.context.node_and_realm(),
        );
        if drive(pass, &self.context).await.unwrap_or(0) > 0 {
            let note = NoteSyncOperation::new(SyncNote::Finished, unix_timestamp_millis());
            if let Err(error) = drive(note, &self.context).await {
                warn!(%error, "Failed to note the device sync state");
            }
        }
    }

    /// Runs the operation the trigger table names for `key` and arms the timer when it asks.
    async fn run_trigger(&self, key: TaskKey) {
        let memory = self.kept_memory(&key);
        let needs = timer_needs(&key);
        let context = self.context.as_ref();
        let input = TimerInput {
            now_ms: unix_timestamp_millis(),
            local_node: context.net_handle.as_ref().map(|net| net.node_id()),
            serving: context.node_and_realm(),
            memory: memory.clone(),
            facts: (needs.facts.then(|| node_facts(context))).unwrap_or_default(),
            routing: (needs.routing.then(|| node_routing(context))).unwrap_or_default(),
            has_blob: context.blob_handle.is_some(),
            running: needs.running.then(|| self.jobs_runtime.running_jobs()),
            has_tasks: context.task_handle.is_some(),
            host_cores: (needs.cores.then(host_cores)).unwrap_or(UNKNOWN_CORES),
            heartbeats: (needs.cluster.then(|| context.fresh_heartbeats())).unwrap_or_default(),
            limits: (needs.cluster.then(|| context.compute_limits())).unwrap_or_default(),
            rocrate_limits: self.rocrate_limits.clone(),
        };
        let Some(operation) = timer_operation(&key, &input) else {
            error!(task_id = ?key, "The trigger table names no operation for this timer");
            return;
        };
        // A lost commit conflict reruns a fresh copy, as the operation's conflict hook decides.
        let seed = key
            .storage_key()
            .map(|key| key.to_vec())
            .unwrap_or_default();
        let build = || operation.clone();
        let bulk = operation.bulk_lane().then(|| self.context.bulk());
        let context = bulk.as_ref().unwrap_or(self.context.as_ref());
        let plan = match drive_retry(build, context, &seed).await {
            Ok(plan) => plan,
            Err(failure) => {
                warn!(task_id = ?key, error = %failure, "Timer run failed");
                TimerPlan {
                    after: Some(failure.retry_after),
                    memory,
                }
            }
        };
        self.keep_memory(&key, plan.memory);
        let Some(after) = plan.after else {
            return;
        };
        if !self.reschedule_timer(key.clone(), after).await {
            // The fired row is gone, so a refused rearm keeps a live wake that stores it again.
            self.keep_retrying(key, after).await;
        }
    }

    /// What a timer kept from its last run, empty for its first run since the start.
    fn kept_memory(&self, key: &TaskKey) -> TimerMemory {
        (self.timer_memory.lock())
            .unwrap_or_else(|error| error.into_inner())
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    /// Stores what a timer keeps until its next run; an empty memory is dropped.
    fn keep_memory(&self, key: &TaskKey, memory: TimerMemory) {
        let mut kept = (self.timer_memory.lock()).unwrap_or_else(|error| error.into_inner());
        if memory == TimerMemory::default() {
            kept.remove(key);
        } else {
            kept.insert(key.clone(), memory);
        }
    }

    fn timer_work(&self, key: TaskKey) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        match key {
            TaskKey::RealmPresence { .. } => Box::pin(self.run_trigger(key)),
            TaskKey::SyncPlacements { .. } => Box::pin(self.run_trigger(key)),
            TaskKey::DrainSyncOutbox => Box::pin(async move {
                self.drain_sync_outbox().await;
            }),
            TaskKey::PublishUsageSnapshots => Box::pin(self.run_trigger(key)),
            TaskKey::PublishNodeInfo => Box::pin(self.run_trigger(key)),
            TaskKey::DrainProjectionQueue => Box::pin(self.run_trigger(key)),
            TaskKey::DrainMaterializationQueue => Box::pin(self.run_trigger(key)),
            TaskKey::DrainPruneQueue => Box::pin(self.run_trigger(key)),
            TaskKey::DrainReplicationQueue => Box::pin(self.run_trigger(key)),
            TaskKey::DrainRefreshQueue => Box::pin(self.run_trigger(key)),
            TaskKey::DrainNotificationOutbox => Box::pin(self.run_trigger(key)),
            TaskKey::PruneNotifications => Box::pin(self.run_trigger(key)),
            TaskKey::PublishWatchInterest => Box::pin(self.run_trigger(key)),
            TaskKey::DrainWatchEvents => Box::pin(self.run_trigger(key)),
            TaskKey::DrainAppliedSync => Box::pin(self.run_trigger(key)),
            TaskKey::DrainJobQueue => Box::pin(async move {
                self.drain_job_queue().await;
            }),
            TaskKey::PruneJobs => Box::pin(self.run_trigger(key)),
            TaskKey::DrainMirrorRepair => Box::pin(self.run_trigger(key)),
            TaskKey::SweepHiddenBlobs => Box::pin(self.run_trigger(key)),
            TaskKey::DrainCleanupQueue => Box::pin(self.run_trigger(key)),
            TaskKey::DrainReclaimQueue => Box::pin(async move {
                self.drain_blob_reclaim().await;
            }),
            TaskKey::MigrateCompression => Box::pin(self.run_trigger(key)),
            TaskKey::RefreshBlobHolders => Box::pin(self.run_trigger(key)),
            TaskKey::LockBucket {
                bucket_id,
                generation,
                session_id,
            } => Box::pin(async move {
                let key = BucketKeyRef::new(bucket_id, generation);
                let ticket = KeyTicket { key, session_id };
                let Some(net_handle) = self.context.net_handle.as_ref() else {
                    warn!("Cannot record a timed bucket lock without net handle");
                    return;
                };
                let now_ms = unix_timestamp_millis();
                let operation = LockBucketOperation::timed(ticket, net_handle.node_id(), now_ms);
                if let Err(error) = drive(operation, &self.context).await {
                    warn!(error = %error, "Timed bucket lock failed");
                }
            }),
            TaskKey::PromotePending {
                bucket_id,
                generation,
            } => Box::pin(async move {
                let Some(net_handle) = self.context.net_handle.as_ref() else {
                    warn!("Cannot promote pending archives without net handle");
                    return;
                };
                let key = BucketKeyRef::new(bucket_id, generation);
                let origin = (*net_handle.realm_id(), net_handle.node_id());
                let promoted = aruna_operations::blob::unlocked::PromoteUnlockedOperation::new(
                    key,
                    origin,
                    &self.rocrate_limits,
                );
                // A locked key leaves the archives pending; its next unlock promotes them.
                if let Err(error) = drive(promoted, &self.context).await {
                    warn!(bucket_id = %bucket_id, error = %error, "Pending promotion failed");
                }
            }),
            key @ TaskKey::RecordAudit { .. } => Box::pin(async move {
                use aruna_operations::s3::bucket::audit_retry::{AUDIT_RETRY, StoreAuditOperation};
                // The key change already applies; only its record waits for storage.
                let store = StoreAuditOperation::new(key.clone());
                if let Err(error) = drive(store, &self.context).await {
                    warn!(error = %error, "Bucket key audit record still not stored");
                    self.keep_retrying(key, AUDIT_RETRY).await;
                }
            }),
            TaskKey::DeliverKeyWakes => Box::pin(self.run_trigger(key)),
            TaskKey::PublishRegistration => Box::pin(async move {
                self.publish_registration().await;
            }),
            TaskKey::PruneConnectorReferences => Box::pin(self.run_trigger(key)),
            TaskKey::MaintainRanges => Box::pin(self.run_trigger(key)),
            TaskKey::RunBackendImports => Box::pin(self.run_trigger(key)),
            TaskKey::DrainFamilyOutbox => Box::pin(self.run_trigger(key)),
            TaskKey::DrainWitnessQueue => Box::pin(self.run_trigger(key)),
            TaskKey::SettleJobTerminals => Box::pin(self.run_trigger(key)),
            TaskKey::DrainLinkQueue => Box::pin(async move {
                self.drain_link_queue().await;
            }),
            TaskKey::DrainCaptures => Box::pin(self.run_trigger(key)),
            TaskKey::ReconcileSyncedFolders => Box::pin(async move {
                let after = match folder_pass::reconcile_folders(&self.context).await {
                    DrainOutcome::Deferred => RECONCILE_RETRY_AFTER,
                    DrainOutcome::Recheck => RECONCILE_CONTINUE_AFTER,
                    DrainOutcome::Idle => RECONCILE_IDLE_AFTER,
                };
                self.reschedule_timer(TaskKey::ReconcileSyncedFolders, after)
                    .await;
                // After the folders, never before them: an unreachable realm
                // must not hold up the owner's own files.
                self.fetch_realm_documents().await;
                self.refresh_device_replicas().await;
            }),
            TaskKey::DrainUploadOutbox => Box::pin(self.run_trigger(key)),
            TaskKey::DrainDeviceIntake => Box::pin(self.run_trigger(key)),
        }
    }
}

/// The durable queue work deferred until after the local serving gate.
#[derive(Clone)]
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
        self.handle_firing(key, None).await;
    }

    async fn handle_firing(&self, key: TaskKey, stamp: Option<u64>) {
        // An audit record retry keeps its row until the record is stored, in one transaction.
        if !matches!(key, TaskKey::RecordAudit { .. }) {
            delete_persisted_timer(&self.context.storage_handle, &key, stamp).await;
        }
        self.timer_work(key).await;
    }
}

#[cfg(test)]
mod tests;
