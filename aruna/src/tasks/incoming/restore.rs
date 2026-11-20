//! Installs the task queues and rearms durable timers so queued work resumes after a restart.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::drained::remove_drained_backends;
use super::*;
use crate::tasks::runner::{claim_producer, reconcile_lost_attempt, spawn_job};
use aruna_drivers::driver::node_routing;
use aruna_operations::federation::registration::PublishRegistrationOperation;
use std::time::SystemTime;

const LINK_RETRY_AFTER: Duration = Duration::from_secs(30);
/// The first and the longest wait before an incomplete timer restoration resumes.
const TIMER_RESUME_FIRST: Duration = Duration::from_secs(1);
const TIMER_RESUME_MOST: Duration = Duration::from_secs(60);

/// Resumes an incomplete timer restoration from where it stopped until every persisted timer is
/// armed, so a durable retry on a later page does not stay dormant for the process lifetime.
fn spawn_timer_resume(
    context: &Arc<DriverContext>,
    task_handle: &TaskHandle,
    shutdown: &Shutdown,
    mut resume_after: Option<byteview::ByteView>,
) {
    let (context, task_handle) = (Arc::downgrade(context), task_handle.clone());
    let cancelled = shutdown.token();
    shutdown.spawn(async move {
        let mut wait = TIMER_RESUME_FIRST;
        loop {
            tokio::select! {
                _ = cancelled.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
            let Some(context) = context.upgrade() else {
                return;
            };
            let storage = &context.storage_handle;
            match restore_task_timers(storage, &task_handle, resume_after.take()).await {
                Ok(()) => return,
                Err(stopped) => {
                    error!(error = %stopped.error, "Persisted task timers are still not restored");
                    resume_after = stopped.resume_after;
                    wait = wait.saturating_mul(2).min(TIMER_RESUME_MOST);
                }
            }
        }
    });
}

fn spawn_queue_rearm(context: &Arc<DriverContext>, task_handle: &TaskHandle, shutdown: &Shutdown) {
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }
    shutdown.spawn(durable_rearm_loop(
        Arc::downgrade(context),
        task_handle.clone(),
        shutdown.token(),
    ));
}

async fn durable_rearm_loop(
    context: Weak<DriverContext>,
    task_handle: TaskHandle,
    cancelled: CancellationToken,
) {
    let mut ticks = 0usize;
    loop {
        tokio::select! {
            _ = cancelled.cancelled() => return,
            _ = tokio::time::sleep(QUEUE_REARM_AFTER) => {}
        }
        let Some(context) = context.upgrade() else {
            return;
        };
        ticks = ticks.saturating_add(1);
        drive(RestoreDrainOperation::default(), &context).await.ok();
        let refresh = aruna_operations::s3::object::metadata_drain::RestoreRefreshTimer::new();
        if let Err(error) = drive(refresh, &context).await {
            warn!(?error, "Failed to scan reference metadata refresh jobs");
        }
        let restore = aruna_operations::connectors::reference_prune::RestorePruneOperation::new();
        if let Err(error) = drive(restore, &context).await {
            warn!(%error, "Failed to restore connector reference pruning");
        }
        restore_outbox_timer(&context).await;
        restore_publish_timer(&context, &task_handle).await;
        restore_sync_timers(&context, &task_handle).await;
        restore_usage_timer(&context.storage_handle, &task_handle).await;
        restore_watch_interest(&context, &task_handle).await;
        restore_info_timer(&task_handle).await;
        restore_notification_outbox(&context, &task_handle, None).await;
        if let Err(error) = drive(ProjectionTimerOperation::new(), &context).await {
            warn!(%error, "Failed to scan metadata pending projection markers");
        }
        // Dead letters retry on a minute-scale backoff, so sweeping every rearm
        // tick would scan the keyspace far more often than it can yield work.
        if ticks.is_multiple_of(DEAD_LETTER_TICKS) {
            sweep_dead_letters(&context).await;
        }
        restore_materialization_timer(&context, &task_handle).await;
        restore_graph_prune(&context, &task_handle).await;
        restore_notification_prune(&context, &task_handle).await;
        arm_drain_timer(&context).await;
        arm_prune_timer(&context).await;
        restore_mirror_timer(&context).await;
        if let Err(error) = drive(
            aruna_operations::jobs::repository::link_queue::LinkTimerOperation::new(),
            &context,
        )
        .await
        {
            warn!(%error, "Failed to arm the repository link queue");
        }
    }
}

/// Arms the notification drain when outbox rows wait: within `shorten`, or else at the retry
/// delay unless a drain runs already.
async fn restore_notification_outbox(
    context: &DriverContext,
    task_handle: &TaskHandle,
    shorten: Option<Duration>,
) {
    let context = DriverContext {
        task_handle: Some(task_handle.clone()),
        ..context.clone()
    };
    let restore = match shorten {
        Some(after) => RestoreOutboxOperation::shorten(after),
        None => RestoreOutboxOperation::probe(),
    };
    match drive(restore, &context).await {
        Ok(true) if shorten.is_none() => {
            let key = TaskKey::DrainNotificationOutbox;
            let event = task_handle
                .schedule_idle_timer(key, DELIVERY_RETRY_AFTER)
                .await;
            if let TaskEvent::Error { message, .. } = event {
                warn!(message = %message, "Failed to restore notification outbox timer");
            }
        }
        Ok(_) => {}
        Err(error) => warn!(%error, "Failed to restore notification outbox timer"),
    }
}

/// The watch interest table of the stored digests; empty when they cannot be read.
async fn interest_table(
    context: &DriverContext,
) -> aruna_core::structs::execution::notification_watch::WatchInterestTable {
    drive(RebuildTableOperation::default(), context)
        .await
        .unwrap_or_else(|error| {
            warn!(%error, "Failed to scan watch interest digests");
            Default::default()
        })
}

/// Requests the watch interest publish when a dirty marker is left over.
async fn restore_watch_interest(context: &DriverContext, task_handle: &TaskHandle) {
    let context = DriverContext {
        task_handle: Some(task_handle.clone()),
        ..context.clone()
    };
    if let Err(error) = drive(InterestOperation::restore(), &context).await {
        warn!(%error, "Failed to restore watch interest publish timer");
    }
}

/// Arms the notification prune timer on the task handle the queues run on.
async fn restore_notification_prune(context: &DriverContext, task_handle: &TaskHandle) {
    let context = DriverContext {
        task_handle: Some(task_handle.clone()),
        ..context.clone()
    };
    let restore = RestorePruneOperation::new(unix_timestamp_millis());
    if let Err(error) = drive(restore, &context).await {
        warn!(%error, "Failed to restore notification prune timer");
    }
}

// Parked materialization jobs come back on their own backoff, so a node that hit
// the failure cap during a storm converges again without an operator or restart.
async fn sweep_dead_letters(context: &DriverContext) {
    if let Err(error) = drive(RequeueParkedOperation::new(), &context.bulk()).await {
        warn!(error = ?error, "Failed to requeue metadata materialization dead letters");
    }
}

async fn restore_materialization_timer(context: &DriverContext, task_handle: &TaskHandle) {
    match drive(NextTimerOperation::new(), &context.bulk()).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let effect = Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainMaterializationQueue,
                after,
            });
            if let Event::Task(TaskEvent::Error { message, .. }) =
                task_handle.send_effect(effect).await
            {
                warn!(message = %message, "Failed to restore metadata materialization timer");
            }
        }
        Err(error) => warn!(error = ?error, "Failed to scan metadata materialization jobs"),
    }
}

/// Installs the inbound task handler without touching durable queues. Handler
/// installation stays in the serving gate; the expensive durable-queue
/// restoration behind it is [`TaskQueues::restore_and_start`].
pub async fn install_task_queues(
    context: Arc<DriverContext>,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
    rocrate_limits: RoCrateLimits,
    node_capabilities: Option<NodeCapabilities>,
) -> TaskQueues {
    install_task_handler(
        context,
        task_handle,
        jobs_runtime,
        rocrate_limits,
        node_capabilities,
        true,
    )
    .await
}

/// Test convenience: installs the inbound handler, then restores and starts
/// durable queues under the caller's shutdown owner. Production uses
/// [`install_task_queues`] and [`TaskQueues::restore_and_start`].
#[doc(hidden)]
pub async fn start_task_queues(
    context: Arc<DriverContext>,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
    shutdown: &Shutdown,
) {
    install_task_queues(
        context,
        task_handle,
        jobs_runtime,
        RoCrateLimits::default(),
        None,
    )
    .await
    .restore_and_start(shutdown)
    .await;
}

async fn install_task_handler(
    context: Arc<DriverContext>,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
    rocrate_limits: RoCrateLimits,
    node_capabilities: Option<NodeCapabilities>,
    refresh_holders: bool,
) -> TaskQueues {
    let handler_context = context.clone();
    let handler = Arc::new(
        OperationsTaskHandler::new(handler_context, jobs_runtime.clone())
            .with_rocrate_limits(rocrate_limits)
            .with_node_capabilities(node_capabilities),
    );
    task_handle.set_inbound_handler(handler.clone()).await;
    // Prime the origin-side watch interest cache from any digests already in
    // local storage so matching works before the first reconcile.
    if let Some(net_handle) = context.net_handle.as_ref() {
        net_handle.replace_watch_interest(interest_table(&context).await);
    }
    TaskQueues {
        context,
        task_handle,
        handler,
        refresh_holders,
    }
}

impl TaskQueues {
    /// Restores persisted timers with their stored due time and starts the
    /// recurring re-arm loop, once the node is already serving.
    pub async fn restore_and_start(self, shutdown: &Shutdown) {
        let stop = shutdown.token();
        self.start_until_stopped(shutdown, &stop, || false).await;
    }

    /// [`Self::restore_and_start`] with an explicit startup stop and a
    /// required-service failure probe, checked before every restore unit so an
    /// accepted stop finishes the current unit and admits no later one.
    pub async fn start_until_stopped(
        self,
        shutdown: &Shutdown,
        stop: &CancellationToken,
        should_stop: impl Fn() -> bool,
    ) {
        let Self {
            context,
            task_handle,
            handler,
            refresh_holders,
        } = self;
        let stopped = || stop.is_cancelled() || should_stop();
        if stopped() {
            return;
        }
        spawn_queue_rearm(&context, &task_handle, shutdown);
        if stopped() {
            return;
        }
        let restored = restore_task_timers(&context.storage_handle, &task_handle, None).await;
        if let Err(incomplete) = restored {
            warn!(error = %incomplete.error, "Persisted task timers not fully restored; resuming");
            spawn_timer_resume(&context, &task_handle, shutdown, incomplete.resume_after);
        }
        if stopped() {
            return;
        }
        restore_outbox_timer(&context).await;
        if stopped() {
            return;
        }
        restore_publish_timer(&context, &task_handle).await;
        if stopped() {
            return;
        }
        // Before the first refresh: a queued edit is the one local change no
        // holder would hand back.
        if context.graph_handle.is_some() {
            drive(
                aruna_operations::metadata::device::edit::replay_queued_edits(),
                &context,
            )
            .await
            .ok();
        }
        if stopped() {
            return;
        }
        restore_sync_timers(&context, &task_handle).await;
        if stopped() {
            return;
        }
        restore_usage_timer(&context.storage_handle, &task_handle).await;
        if stopped() {
            return;
        }
        restore_watch_interest(&context, &task_handle).await;
        if stopped() {
            return;
        }
        restore_info_timer(&task_handle).await;
        if stopped() {
            return;
        }
        restore_registration_timer(&context.storage_handle, &task_handle).await;
        if stopped() {
            return;
        }
        restore_notification_outbox(&context, &task_handle, Some(Duration::ZERO)).await;
        if stopped() {
            return;
        }
        if let Err(error) = drive(ProjectionTimerOperation::new(), &context).await {
            warn!(%error, "Failed to scan metadata pending projection markers");
        }
        if stopped() {
            return;
        }
        sweep_dead_letters(&context).await;
        if stopped() {
            return;
        }
        restore_materialization_timer(&context, &task_handle).await;
        if stopped() {
            return;
        }
        restore_graph_prune(&context, &task_handle).await;
        if stopped() {
            return;
        }
        restore_notification_prune(&context, &task_handle).await;
        if stopped() {
            return;
        }
        drive(RestoreDrainOperation::default(), &context).await.ok();
        if stopped() {
            return;
        }
        let refresh = aruna_operations::s3::object::metadata_drain::RestoreRefreshTimer::new();
        if let Err(error) = drive(refresh, &context).await {
            warn!(?error, "Failed to scan reference metadata refresh jobs");
        }
        if stopped() {
            return;
        }
        arm_prune_timer(&context).await;
        if stopped() {
            return;
        }
        restore_mirror_timer(&context).await;
        if stopped() {
            return;
        }
        // Watch events queued before a restart leave once the drain runs.
        handler
            .reschedule_timer(TaskKey::DrainWatchEvents, Duration::ZERO)
            .await;
        // Follow-ups of shard reconciles applied before a restart run once the drain does.
        handler
            .reschedule_timer(TaskKey::DrainAppliedSync, Duration::ZERO)
            .await;
        // Git snapshots whose capture marks outlived a restart are taken once the drain runs.
        handler
            .reschedule_timer(TaskKey::DrainCaptures, Duration::ZERO)
            .await;
        if context.blob_handle.is_some() {
            let arm = ArmSweepOperation::new(TaskKey::SweepHiddenBlobs);
            if let Err(error) = drive(arm, &context).await {
                warn!(error = %error, "Failed to schedule hidden blob sweep");
            }
            let arm = ArmSweepOperation::new(TaskKey::DrainReclaimQueue);
            if let Err(error) = drive(arm, &context).await {
                warn!(error = %error, "Failed to schedule blob reclaim sweep");
            }
            handler
                .reschedule_timer(TaskKey::DrainCleanupQueue, Duration::ZERO)
                .await;
            // Wakes owed before a restart are delivered again; a run with none is cheap.
            handler
                .reschedule_timer(TaskKey::DeliverKeyWakes, Duration::ZERO)
                .await;
            // A run deletes its timer before the page it works on, so a crash in between
            // leaves an unfinished migration without one. A run with nothing left is cheap.
            handler
                .reschedule_timer(TaskKey::MigrateCompression, Duration::ZERO)
                .await;
            // The run computes every rule's next due time and re-arms itself.
            handler
                .reschedule_timer(TaskKey::RunBackendImports, Duration::ZERO)
                .await;
            // Each pass re-arms itself; a node without distributed buckets finds no accounts.
            handler
                .reschedule_timer(TaskKey::MaintainRanges, Duration::ZERO)
                .await;
        }
        if refresh_holders {
            handler
                .reschedule_timer(TaskKey::RefreshBlobHolders, Duration::ZERO)
                .await;
        }
    }
}

impl OperationsTaskHandler {
    pub(super) async fn publish_registration(&self) {
        let key = TaskKey::PublishRegistration;
        match (self.context.net_handle.as_ref(), &self.node_capabilities) {
            (Some(net_handle), Some(capabilities)) => {
                let publish = PublishRegistrationOperation::new(
                    *net_handle.realm_id(),
                    net_handle.node_id(),
                    capabilities.clone(),
                );
                if let Err(error) = drive(publish, &self.context).await {
                    warn!(task_id = ?key, error = %error, "Failed to publish registry registration");
                }
            }
            _ => warn!(task_id = ?key, "Cannot publish registration without net handle or keys"),
        }
        self.reschedule_timer(key, aruna_core::federation::PUBLISH_INTERVAL)
            .await;
    }
}

impl OperationsTaskHandler {
    /// Starts the repository link pushes whose debounce elapsed and re-arms for the next one.
    pub(super) async fn drain_link_queue(&self) {
        let drain = aruna_operations::jobs::repository::link_queue::DrainLinksOperation::new(
            self.context.node_scope(),
        );
        let after = match drive(drain, &self.context).await {
            Ok(after) => after,
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainLinkQueue, %error, "Failed to drain repository link queue");
                Some(LINK_RETRY_AFTER)
            }
        };
        if let Some(after) = after {
            self.reschedule_timer(TaskKey::DrainLinkQueue, after).await;
        }
    }
}

impl OperationsTaskHandler {
    pub(super) async fn drain_job_queue(&self) {
        if !self.jobs_runtime.is_started() {
            return;
        }
        let Some(owner_node_id) = self.context.net_handle.as_ref().map(|net| net.node_id()) else {
            warn!(task_id = ?TaskKey::DrainJobQueue, "Cannot drain job queue without net handle");
            self.reschedule_timer(TaskKey::DrainJobQueue, DRAIN_RETRY_AFTER)
                .await;
            return;
        };
        let Some(claim_producer) = claim_producer(&self.jobs_runtime).await else {
            return;
        };
        // Per class: one aggregate would claim rows of a saturated class only to
        // release them again on every pass.
        let budget = JobClassBudget {
            in_process: self
                .jobs_runtime
                .available_slots_for(JobExecutionClass::InProcess),
            external: self
                .jobs_runtime
                .available_slots_for(JobExecutionClass::ExternalAttempt),
        };

        let drain = DrainJobsOperation::new(owner_node_id, budget, unix_timestamp_millis());
        let mut result = match drive(drain, self.context.as_ref()).await {
            Ok(result) => result,
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainJobQueue, error = %error, "Failed to drain job queue");
                self.reschedule_timer(TaskKey::DrainJobQueue, DRAIN_RETRY_AFTER)
                    .await;
                return;
            }
        };

        for record in std::mem::take(&mut result.lost_attempts) {
            reconcile_lost_attempt(&self.jobs_runtime, self.context.as_ref(), record).await;
        }

        for record in result.claimed {
            if self
                .jobs_runtime
                .available_slots_for(record.execution_class)
                == 0
            {
                let Some(token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
                    warn!(job_id = %record.job_id, "Claimed job has no claim token; cannot release");
                    continue;
                };
                if let Err(error) = drive(
                    job_calls::release_job(record.job_id, token, unix_timestamp_millis()),
                    self.context.as_ref(),
                )
                .await
                {
                    warn!(job_id = %record.job_id, error = %error, "Failed to release excess job claim");
                }
                continue;
            }
            spawn_job(&self.jobs_runtime, self.context.clone(), record);
        }
        drop(claim_producer);

        // A per-job error stopped the batch after handing off what was claimed; back off
        // and re-drive the remainder rather than hot-looping on the failure.
        if result.retry_after_error {
            self.reschedule_timer(TaskKey::DrainJobQueue, DRAIN_RETRY_AFTER)
                .await;
            return;
        }

        // Due work left behind by a saturated class: wait for a completion kick, not
        // a ZERO hot-loop.
        match result.next_due_after {
            Some(after) if after.is_zero() && result.deferred_saturated => {
                self.reschedule_timer(TaskKey::DrainJobQueue, DRAIN_RETRY_AFTER)
                    .await;
            }
            Some(after) => {
                self.reschedule_timer(TaskKey::DrainJobQueue, after).await;
            }
            None => {}
        }
    }

    pub(super) async fn drain_blob_reclaim(&self) {
        let key = TaskKey::DrainReclaimQueue;
        let memory = self.kept_memory(&key);
        let catalog = node_routing(&self.context).catalog;
        let start = memory.cursor.clone().map(Key::from);
        let sweep = ReclaimSweepOperation::new(catalog, SystemTime::now(), start);
        let outcome = drive(sweep, &self.context).await;
        if let Err(error) = &outcome {
            warn!(task_id = ?key, error = %error, "Failed to drain blob reclaim");
        }
        let (plan, drained) = reclaim_plan(outcome.as_ref().ok(), &memory);
        self.keep_memory(&key, plan.memory);
        // Removal walks whole keyspaces too, so it only rides a sweep that
        // reached the end of the queue, never the fast retries behind a backlog.
        if drained && let Err(error) = remove_drained_backends(&self.context).await {
            warn!(error = %error, "Failed to remove drained storage backends");
        }
        if let Some(after) = plan.after {
            self.reschedule_timer(key, after).await;
        }
    }
}

#[doc(hidden)]
pub async fn drain_notification_outbox(context: Arc<DriverContext>) {
    OperationsTaskHandler::new(context, JobsRuntime::new())
        .run_trigger(TaskKey::DrainNotificationOutbox)
        .await;
}

/// Re-arms the sync mirror repair timer; a failure waits for the next rearm tick.
async fn restore_mirror_timer(context: &DriverContext) {
    if let Err(error) = drive(RestoreTimerOperation::new(), context).await {
        warn!(%error, "Failed to restore sync mirror repair timer");
    }
}

/// Arms the outbox drain for waiting records; a failure waits for the next rearm tick.
async fn restore_outbox_timer(context: &DriverContext) {
    if let Err(error) = drive(OutboxTimerOperation::new(), context).await {
        warn!(%error, "Failed to restore document sync outbox timer");
    }
}

/// Arms the periodic node-info heartbeat at startup. `ShortenTimer` (never `ResetTimer`) so the
/// durable-queue re-arm loop cannot push the deadline forward past the handler's own re-arm.
async fn restore_info_timer(task_handle: &TaskHandle) {
    let effect = Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PublishNodeInfo,
        after: aruna_core::heartbeat::INFO_PUBLISH_INTERVAL,
    });
    if let Event::Task(TaskEvent::Error { message, .. }) = task_handle.send_effect(effect).await {
        warn!(message = %message, "Failed to arm node info heartbeat timer");
    }
}

/// Arms the registration timer at startup without postponing a restored due time. The due
/// time is persisted, so repeated restarts do not keep moving the first publication.
async fn restore_registration_timer(
    storage: &aruna_storage::storage::handle::StorageHandle,
    task_handle: &TaskHandle,
) {
    let effect = TaskEffect::ShortenTimer {
        key: TaskKey::PublishRegistration,
        after: aruna_core::federation::PUBLISH_INTERVAL,
    };
    let _held = hold_change(&effect).await;
    if let Err(message) = persist_task_effect(storage, &effect).await {
        warn!(message = %message, "Failed to persist registry publication timer");
    }
    if let Event::Task(TaskEvent::Error { message, .. }) =
        task_handle.send_effect(Effect::Task(effect)).await
    {
        warn!(message = %message, "Failed to arm registry publication timer");
    }
}

/// Re-arms the debounced publish task when dirty markers survived a restart.
async fn restore_usage_timer(
    storage: &aruna_storage::storage::handle::StorageHandle,
    task_handle: &TaskHandle,
) {
    let has_markers = match storage
        .send_storage_effect(aruna_core::effects::StorageEffect::Iter {
            key_space: aruna_core::keyspaces::NODE_STATS_KEYSPACE.to_string(),
            prefix: Some(Key::from(
                aruna_core::structs::storage::usage::DIRTY_PREFIX.to_vec(),
            )),
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => !values.is_empty(),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan node usage dirty markers");
            return;
        }
        other => {
            warn!(
                event = other.label(),
                "Unexpected event while scanning node usage dirty markers"
            );
            return;
        }
    };
    if has_markers
        && let Event::Task(TaskEvent::Error { message, .. }) = task_handle
            .send_effect(aruna_operations::s3::usage::schedule_snapshot_publish())
            .await
    {
        warn!(message = %message, "Failed to restore node usage publish timer");
    }
}

async fn arm_drain_timer(context: &DriverContext) {
    let timer = DrainTimerOperation::new(unix_timestamp_millis());
    if let Err(error) = drive(timer, context).await {
        warn!(error = %error, "Failed to restore job drain timer");
    }
}

async fn arm_prune_timer(context: &DriverContext) {
    let timer = PruneTimerOperation::new(unix_timestamp_millis());
    if let Err(error) = drive(timer, context).await {
        warn!(error = %error, "Failed to restore job prune timer");
    }
}

#[cfg(test)]
mod stop_tests {
    use super::*;
    use aruna_storage::storage::worker::FjallStorage;
    use tempfile::tempdir;

    async fn restore_queues() -> (tempfile::TempDir, TaskHandle, TaskQueues, Shutdown) {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            graph_handle: None,
            task_handle: Some(task_handle.clone()),
            compute_handle: None,
            outbound_handle: None,
        });
        let queues = install_task_queues(
            context,
            task_handle.clone(),
            JobsRuntime::new_paused(),
            RoCrateLimits::default(),
            None,
        )
        .await;
        (temp_dir, task_handle, queues, Shutdown::new())
    }

    // A stop accepted before restoration admits no restore unit.
    #[tokio::test]
    async fn cancel_skips_restore() {
        let (temp, task_handle, queues, shutdown) = restore_queues().await;
        persist_task_effect(
            &queues.context.storage_handle,
            &TaskEffect::ResetTimer {
                key: TaskKey::RefreshBlobHolders,
                after: Duration::from_secs(7200),
            },
        )
        .await
        .expect("timer persists");
        let stop = CancellationToken::new();
        stop.cancel();

        queues.start_until_stopped(&shutdown, &stop, || false).await;

        let TaskEvent::TimerScheduled { after, .. } = task_handle
            .schedule_idle_timer(TaskKey::RefreshBlobHolders, Duration::from_secs(3600))
            .await
        else {
            panic!("expected timer schedule event");
        };
        assert_eq!(
            after,
            Duration::from_secs(3600),
            "a cancelled restore must not restore the persisted timer"
        );
        drop(temp);
    }

    // A stop observed after the first unit finishes the current unit and skips
    // the next one instead of racing and dropping it mid-flight.
    #[tokio::test]
    async fn stop_between_restores() {
        let (temp, task_handle, queues, shutdown) = restore_queues().await;
        persist_task_effect(
            &queues.context.storage_handle,
            &TaskEffect::ResetTimer {
                key: TaskKey::RefreshBlobHolders,
                after: Duration::from_secs(7200),
            },
        )
        .await
        .expect("timer persists");
        let checks = std::cell::Cell::new(0u32);
        // The re-arm loop is not part of this unit sequence; stop it up front
        // so only `start_until_stopped` can restore the timer.
        shutdown.trigger();

        queues
            .start_until_stopped(&shutdown, &CancellationToken::new(), || {
                checks.set(checks.get() + 1);
                checks.get() > 1
            })
            .await;

        assert!(checks.get() >= 2, "each unit boundary must be checked");
        let TaskEvent::TimerScheduled { after, .. } = task_handle
            .schedule_idle_timer(TaskKey::RefreshBlobHolders, Duration::from_secs(3600))
            .await
        else {
            panic!("expected timer schedule event");
        };
        assert_eq!(
            after,
            Duration::from_secs(3600),
            "the later restore unit must not run after the stop"
        );
        drop(temp);
    }
}
