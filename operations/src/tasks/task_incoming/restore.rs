use super::*;

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
            _ = tokio::time::sleep(DURABLE_QUEUE_REARM_AFTER) => {}
        }
        let Some(context) = context.upgrade() else {
            return;
        };
        ticks = ticks.saturating_add(1);
        restore_blob_replication_timer(&context.storage_handle, &task_handle).await;
        restore_reference_metadata_refresh_timer(&context.storage_handle, &task_handle).await;
        restore_document_sync_outbox_timers(&context.storage_handle, &task_handle).await;
        restore_intake_timer(&context.storage_handle, &task_handle).await;
        restore_sync_timers(&context, &task_handle).await;
        restore_usage_snapshot_publish_timer(&context.storage_handle, &task_handle).await;
        restore_watch_interest_publish_timer(&context.storage_handle, &task_handle).await;
        crate::node::node_info::restore_node_info_publish_timer(
            &context.storage_handle,
            &task_handle,
        )
        .await;
        restore_notification_outbox_timer_if_idle(
            &context.storage_handle,
            &task_handle,
            NOTIFICATION_DELIVERY_RETRY_AFTER,
        )
        .await;
        restore_pending_metadata_projection_timer(&context.storage_handle, &task_handle).await;
        // Dead letters retry on a minute-scale backoff, so sweeping every rearm
        // tick would scan the keyspace far more often than it can yield work.
        if ticks.is_multiple_of(DEAD_LETTER_SWEEP_TICKS) {
            sweep_dead_letters(&context.storage_handle).await;
        }
        restore_metadata_materialization_timer(&context.storage_handle.bulk(), &task_handle).await;
        restore_metadata_graph_prune_timer(&context.storage_handle, &task_handle).await;
        restore_notification_prune_timer(&context.storage_handle, &task_handle).await;
        restore_job_queue_timer(&context.storage_handle, &task_handle).await;
        restore_job_prune_timer(&context.storage_handle, &task_handle).await;
        restore_mirror_timer(&context.storage_handle, &task_handle).await;
    }
}

// A batch that processed nothing is blocked behind jobs that are not due yet, so
// it backs off instead of rescanning the same head at batch pace.
pub(super) fn drain_delay(result: &MetadataMaterializationDrainResult) -> Duration {
    if result.processed == 0 {
        METADATA_MATERIALIZATION_RETRY_AFTER
    } else {
        METADATA_MATERIALIZATION_NEXT_BATCH_AFTER
    }
}

// Parked materialization jobs come back on their own backoff, so a node that hit
// the failure cap during a storm converges again without an operator or restart.
async fn sweep_dead_letters(storage: &aruna_storage::StorageHandle) {
    if let Err(error) = requeue_dead_letters(&storage.bulk()).await {
        warn!(error = ?error, "Failed to requeue metadata materialization dead letters");
    }
}

pub async fn initialize_task_incoming(
    context: Arc<DriverContext>,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
) {
    install_task_handler(
        context,
        task_handle,
        jobs_runtime,
        RoCrateLimits::default(),
        false,
    )
    .await
    .start(&Shutdown::new())
    .await;
}

/// Installs the inbound task handler without touching durable queues. Handler
/// installation stays in the serving gate; the expensive durable-queue
/// restoration behind it is [`TaskQueues::start`].
pub async fn initialize_task_holder(
    context: Arc<DriverContext>,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
    rocrate_limits: RoCrateLimits,
) -> TaskQueues {
    install_task_handler(context, task_handle, jobs_runtime, rocrate_limits, true).await
}

async fn install_task_handler(
    context: Arc<DriverContext>,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
    rocrate_limits: RoCrateLimits,
    refresh_holders: bool,
) -> TaskQueues {
    let handler_context = context.clone();
    if context.compute_handle.is_some() {
        jobs_runtime.set_reconciler(crate::jobs::workflow::reconcile::ComputeReconciler::new(
            context.clone(),
            Arc::downgrade(&jobs_runtime),
        ));
    }
    let handler = Arc::new(
        OperationsTaskHandler::new(handler_context, jobs_runtime.clone())
            .with_rocrate_limits(rocrate_limits),
    );
    task_handle.set_inbound_handler(handler.clone()).await;
    // Prime the origin-side watch interest cache from any digests already in
    // local storage so matching works before the first reconcile.
    if let Some(net_handle) = context.net_handle.as_ref() {
        let table = rebuild_watch_interest_table(&context.storage_handle).await;
        net_handle.replace_watch_interest(table);
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
    pub async fn start(self, shutdown: &Shutdown) {
        let Self {
            context,
            task_handle,
            handler,
            refresh_holders,
        } = self;
        spawn_queue_rearm(&context, &task_handle, shutdown);
        restore_persisted_task_timers(&context.storage_handle, &task_handle).await;
        restore_document_sync_outbox_timers(&context.storage_handle, &task_handle).await;
        restore_intake_timer(&context.storage_handle, &task_handle).await;
        // Before the first refresh: a queued edit is the one local change no
        // holder would hand back.
        crate::device::edit::replay_queued_edits(&context).await;
        restore_sync_timers(&context, &task_handle).await;
        restore_usage_snapshot_publish_timer(&context.storage_handle, &task_handle).await;
        restore_watch_interest_publish_timer(&context.storage_handle, &task_handle).await;
        crate::node::node_info::restore_node_info_publish_timer(
            &context.storage_handle,
            &task_handle,
        )
        .await;
        restore_notification_outbox_timer(&context.storage_handle, &task_handle, Duration::ZERO)
            .await;
        restore_pending_metadata_projection_timer(&context.storage_handle, &task_handle).await;
        sweep_dead_letters(&context.storage_handle).await;
        restore_metadata_materialization_timer(&context.storage_handle.bulk(), &task_handle).await;
        restore_metadata_graph_prune_timer(&context.storage_handle, &task_handle).await;
        restore_notification_prune_timer(&context.storage_handle, &task_handle).await;
        restore_blob_replication_timer(&context.storage_handle, &task_handle).await;
        restore_reference_metadata_refresh_timer(&context.storage_handle, &task_handle).await;
        restore_job_prune_timer(&context.storage_handle, &task_handle).await;
        restore_mirror_timer(&context.storage_handle, &task_handle).await;
        if context.blob_handle.is_some() {
            restore_hidden_sweep(&context.storage_handle, &task_handle).await;
            restore_reclaim_sweep(&context.storage_handle, &task_handle).await;
            handler
                .reschedule_timer(TaskKey::DrainBlobCleanupQueue, Duration::ZERO)
                .await;
        }
        if refresh_holders {
            handler
                .reschedule_timer(TaskKey::RefreshBlobHolders, Duration::ZERO)
                .await;
        }
    }
}
