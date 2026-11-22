//! Owns the background task handler and the queues that run its timers and drains.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use aruna_clock::unix_timestamp_millis;
use aruna_core::effects::Effect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::execution::job::identity::JobExecutionClass;
use aruna_core::structs::execution::job::input::RoCrateLimits;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
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

use crate::tasks::task_persistence::restore_task_timers;
use aruna_core::structs::storage::encryption::{BucketKeyRef, KeyTicket};
use aruna_drivers::driver::node_facts;
use aruna_drivers::driver::{DriverContext, drive, drive_retry, node_routing};
use aruna_drivers::effect_adapters::task::{arm_unless_stored, change_timer};
use aruna_drivers::jobs::runtime::JobsRuntime;
use aruna_operations::blob::reclaim_sweep::{ReclaimSweepOperation, reclaim_plan};
use aruna_operations::blob::sweep_timer::ArmSweepOperation;
use aruna_operations::device::publish_queue::DrainOutcome;
use aruna_operations::device::sync::{
    RECONCILE_CONTINUE_AFTER, RECONCILE_IDLE_AFTER, RECONCILE_RETRY_AFTER,
};
use aruna_operations::device_node::sync_note::{NoteSyncOperation, SyncNote};
use aruna_operations::jobs::DRAIN_RETRY_AFTER;
use aruna_operations::jobs::drain::{DrainJobsOperation, DrainTimerOperation, JobClassBudget};
use aruna_operations::jobs::prune::PruneTimerOperation;
use aruna_operations::jobs::store::job_calls;
use aruna_operations::metadata::materialization_queue::{
    NextTimerOperation, RequeueParkedOperation, UNKNOWN_CORES,
};
use aruna_operations::metadata::projection::ProjectionTimerOperation;
use aruna_operations::metadata::prune_queue::PruneTimerOperation as GraphPruneTimerOperation;
use aruna_operations::notifications::outbox::{DELIVERY_RETRY_AFTER, RestoreOutboxOperation};
use aruna_operations::notifications::prune::RestorePruneOperation;
use aruna_operations::notifications::watch::digest::InterestOperation;
use aruna_operations::notifications::watch::table::RebuildTableOperation;
use aruna_operations::queue_backoff::retry_delay_ms;
use aruna_operations::replication::drain::RestoreDrainOperation;
use aruna_operations::replication::mirror_repair::drain::RestoreTimerOperation;
use aruna_operations::s3::bucket::key::lock::LockBucketOperation;
use aruna_operations::sync::document_outbox::OutboxTimerOperation;
use aruna_operations::sync::outbox::batches::OutboxLimits;
use aruna_operations::sync::outbox::rotation::OutboxRotation;
use aruna_operations::triggers::{
    TimerInput, TimerMemory, TimerPlan, keeps_earlier, timer_needs, timer_operation,
};
use aruna_storage::task_timers::delete_persisted_timer;
use folder_pass::{restore_publish_timer, restore_sync_timers};

pub(crate) mod drained;
pub mod folder_pass;
pub mod git_drain;
pub mod outbox;
pub mod restore;

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

const QUEUE_REARM_AFTER: Duration = Duration::from_secs(5);

/// How long a device keeps its copy of the realm documents before asking for
/// them again. A revocation reaches it within this window at the latest.
const REALM_DOCUMENTS_AFTER: Duration = Duration::from_secs(60);

/// How long one attempt may spend on the realm as a whole. The folder cadence
/// is what it shares that time with, so it is bounded once, not per node.
const REALM_DOCUMENTS_BUDGET: Duration = Duration::from_secs(20);
/// Rearm ticks between dead-letter sweeps, i.e. one sweep a minute.
const DEAD_LETTER_TICKS: usize = 12;

/// Dispatches scheduled tasks and background drains to their operations.
pub(crate) struct OperationsTaskHandler {
    context: Arc<DriverContext>,
    jobs_runtime: Arc<JobsRuntime>,
    rocrate_limits: RoCrateLimits,
    // What each converted timer keeps between runs; the operations decide on it. Loss on
    // restart is fine: a restarted timer starts afresh.
    timer_memory: std::sync::Mutex<HashMap<TaskKey, TimerMemory>>,
    outbox_limits: OutboxLimits,
    // When a device may next fetch the realm documents, and how many attempts
    // have failed. Loss on restart is fine: a restart fetches them anyway.
    realm_documents: std::sync::Mutex<(u32, u64)>,
}

impl OperationsTaskHandler {
    pub(crate) fn new(context: Arc<DriverContext>, jobs_runtime: Arc<JobsRuntime>) -> Self {
        Self {
            context,
            jobs_runtime,
            rocrate_limits: RoCrateLimits::default(),
            timer_memory: std::sync::Mutex::new(HashMap::new()),
            outbox_limits: OutboxLimits::default(),
            realm_documents: std::sync::Mutex::new((0, 0)),
        }
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

    async fn reschedule_timer(&self, key: TaskKey, after: std::time::Duration) -> bool {
        let effect = TaskEffect::ResetTimer {
            key: key.clone(),
            after,
        };
        let storage = &self.context.storage_handle;
        let task_handle = self.context.task_handle.as_ref();
        match change_timer(storage, task_handle, effect, false).await {
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
        self.rearm_live(TaskEffect::ResetTimer { key, after }).await;
    }

    /// Applies a timer arming in memory even when storage cannot persist it.
    async fn rearm_live(&self, effect: TaskEffect) {
        let storage = &self.context.storage_handle;
        let task_handle = self.context.task_handle.as_ref();
        if let Event::Task(TaskEvent::Error { key, message }) =
            change_timer(storage, task_handle, effect, true).await
        {
            warn!(task_id = ?key, message = %message, "Failed to arm the retry timer");
        }
    }

    /// Arms the backend import pass at start, live even when its row cannot be stored, since
    /// no restored wake runs the rules until a rule changes.
    pub(super) async fn arm_backend_imports(&self) {
        self.keep_retrying(TaskKey::RunBackendImports, Duration::ZERO)
            .await;
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
            facts: if needs.facts {
                node_facts(context)
            } else {
                Default::default()
            },
            routing: if needs.routing {
                node_routing(context)
            } else {
                Default::default()
            },
            has_blob: context.blob_handle.is_some(),
            running: needs.running.then(|| self.jobs_runtime.running_jobs()),
            has_tasks: context.task_handle.is_some(),
            host_cores: (needs.cores.then(host_cores)).unwrap_or(UNKNOWN_CORES),
            heartbeats: if needs.cluster {
                context.fresh_heartbeats()
            } else {
                Default::default()
            },
            limits: if needs.cluster {
                context.compute_limits()
            } else {
                Default::default()
            },
            rocrate_limits: self.rocrate_limits.clone(),
            outbox: self.outbox_limits,
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
        // The fired row is gone, so a refused rearm keeps a live wake that stores it again.
        if keeps_earlier(&key) {
            return self.rearm_stored(key, after).await;
        }
        self.rearm_live(TaskEffect::ResetTimer { key, after }).await;
    }

    /// Rearms `key` unless a row stored while it ran holds a newer wake, also one no longer live
    /// after a stop; live even when the row cannot be stored.
    async fn rearm_stored(&self, key: TaskKey, after: Duration) {
        let Some(task_handle) = self.context.task_handle.as_ref() else {
            warn!(task_id = ?key, "Cannot rearm a timer without task handle");
            return;
        };
        let storage = &self.context.storage_handle;
        let effect = TaskEffect::ShortenTimer { key, after };
        if let Event::Task(TaskEvent::Error { key, message }) =
            arm_unless_stored(storage, task_handle, effect).await
        {
            warn!(task_id = ?key, message = %message, "Failed to arm the retry timer");
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
            TaskKey::DrainSyncOutbox => Box::pin(self.run_trigger(key)),
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
            TaskKey::PruneFamilies => Box::pin(self.run_trigger(key)),
            TaskKey::ConfigCatchUp => Box::pin(self.run_trigger(key)),
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
            // The key change already applies; only its record waits for storage.
            TaskKey::RecordAudit { .. } => Box::pin(self.run_trigger(key)),
            TaskKey::DeliverKeyWakes => Box::pin(self.run_trigger(key)),
            TaskKey::PublishRegistration => Box::pin(self.run_trigger(key)),
            TaskKey::PruneConnectorReferences => Box::pin(self.run_trigger(key)),
            TaskKey::MaintainRanges => Box::pin(self.run_trigger(key)),
            TaskKey::RunBackendImports => Box::pin(self.run_trigger(key)),
            TaskKey::DrainFamilyOutbox => Box::pin(self.run_trigger(key)),
            TaskKey::DrainWitnessQueue => Box::pin(self.run_trigger(key)),
            TaskKey::SettleJobTerminals => Box::pin(self.run_trigger(key)),
            TaskKey::DrainLinkQueue => Box::pin(self.run_trigger(key)),
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
        // A test pins the first outbox drain of a launch until it releases the barrier.
        #[cfg(debug_assertions)]
        if matches!(key, TaskKey::DrainSyncOutbox)
            && let Some(barrier) = OutboxBarrier::new()
        {
            barrier.wait_start().await;
        }
        // An audit record retry keeps its row until the record is stored, in one transaction.
        if !matches!(key, TaskKey::RecordAudit { .. }) {
            delete_persisted_timer(&self.context.storage_handle, &key, stamp).await;
        }
        self.timer_work(key).await;
    }
}

#[cfg(test)]
mod tests;
