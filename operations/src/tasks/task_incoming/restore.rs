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

impl OperationsTaskHandler {
    pub(super) async fn publish_usage_snapshots(&self) {
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(task_id = ?TaskKey::PublishUsageSnapshots, "Cannot publish usage snapshots without net handle");
            return;
        };
        let node_id = net_handle.node_id();
        let realm_id = *net_handle.realm_id();
        match crate::node::usage_stats::publish_and_refresh_usage_snapshots(
            &self.context,
            node_id,
            realm_id,
            false,
        )
        .await
        {
            Ok(_) => {}
            Err(error) => {
                warn!(task_id = ?TaskKey::PublishUsageSnapshots, error = %error, "Failed to publish usage snapshots");
                self.reschedule_timer(
                    TaskKey::PublishUsageSnapshots,
                    crate::node::usage_stats::USAGE_SNAPSHOT_PUBLISH_DEBOUNCE,
                )
                .await;
            }
        }
    }

    pub(super) async fn publish_node_info(&self) {
        if let Some(net_handle) = self.context.net_handle.as_ref() {
            let node_id = net_handle.node_id();
            let realm_id = *net_handle.realm_id();
            if let Err(error) = crate::node::node_info::refresh_node_info_heartbeat(
                &self.context,
                node_id,
                realm_id,
            )
            .await
            {
                warn!(task_id = ?TaskKey::PublishNodeInfo, error = %error, "Failed to publish node info heartbeat");
            }
        } else {
            warn!(task_id = ?TaskKey::PublishNodeInfo, "Cannot publish node info without net handle");
        }
        // Periodic heartbeat: always re-arm for the next interval regardless of
        // outcome so a transient failure never stops the heartbeat.
        self.reschedule_timer(
            TaskKey::PublishNodeInfo,
            crate::node::node_info::NODE_INFO_PUBLISH_INTERVAL,
        )
        .await;
    }

    pub(super) async fn publish_watch_interest(&self) {
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(task_id = ?TaskKey::PublishWatchInterest, "Cannot publish watch interest without net handle");
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
                warn!(task_id = ?TaskKey::PublishWatchInterest, error = %error, "Failed to publish watch interest");
                self.reschedule_timer(
                    TaskKey::PublishWatchInterest,
                    WATCH_INTEREST_PUBLISH_DEBOUNCE,
                )
                .await;
            }
        }
    }

    /// A context whose storage effects dispatch on the bulk lane, so background
    /// queue draining never starves foreground sync traffic.
    fn bulk_context(&self) -> DriverContext {
        let mut context = self.context.as_ref().clone();
        context.storage_handle = context.storage_handle.bulk();
        context.metadata_handle = context
            .metadata_handle
            .as_ref()
            .map(|metadata_handle| metadata_handle.bulk());
        context
    }

    pub(super) async fn drain_metadata_materialization_queue(&self) {
        let bulk = self.bulk_context();
        match process_metadata_materialization_batch(&bulk).await {
            Ok(result) if result.has_more_due => {
                self.reschedule_timer(
                    TaskKey::DrainMetadataMaterializationQueue,
                    drain_delay(&result),
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
            Ok(_) => match metadata_materialization_jobs_exist(&bulk.storage_handle).await {
                Ok(false) => {}
                Ok(true) => {
                    self.reschedule_timer(
                        TaskKey::DrainMetadataMaterializationQueue,
                        METADATA_MATERIALIZATION_POLL_AFTER,
                    )
                    .await;
                }
                Err(error) => {
                    warn!(task_id = ?TaskKey::DrainMetadataMaterializationQueue, error = ?error, "Failed to probe metadata materialization jobs");
                    self.reschedule_timer(
                        TaskKey::DrainMetadataMaterializationQueue,
                        METADATA_MATERIALIZATION_RETRY_AFTER,
                    )
                    .await;
                }
            },
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainMetadataMaterializationQueue, error = ?error, "Failed to drain metadata materialization queue");
                self.reschedule_timer(
                    TaskKey::DrainMetadataMaterializationQueue,
                    METADATA_MATERIALIZATION_RETRY_AFTER,
                )
                .await;
            }
        }
    }

    pub(super) async fn drain_metadata_graph_prune_queue(&self) {
        let bulk = self.bulk_context();
        match process_metadata_graph_prune_batch(&bulk).await {
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
            Ok(_) => match metadata_graph_prune_jobs_exist(&bulk.storage_handle).await {
                Ok(false) => {}
                Ok(true) => {
                    self.reschedule_timer(
                        TaskKey::DrainMetadataGraphPruneQueue,
                        METADATA_GRAPH_PRUNE_POLL_AFTER,
                    )
                    .await;
                }
                Err(error) => {
                    warn!(task_id = ?TaskKey::DrainMetadataGraphPruneQueue, error = ?error, "Failed to probe metadata graph prune jobs");
                    self.reschedule_timer(
                        TaskKey::DrainMetadataGraphPruneQueue,
                        METADATA_GRAPH_PRUNE_RETRY_AFTER,
                    )
                    .await;
                }
            },
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainMetadataGraphPruneQueue, error = ?error, "Failed to drain metadata graph prune queue");
                self.reschedule_timer(
                    TaskKey::DrainMetadataGraphPruneQueue,
                    METADATA_GRAPH_PRUNE_RETRY_AFTER,
                )
                .await;
            }
        }
    }
}

impl OperationsTaskHandler {
    pub(super) async fn drain_metadata_projection_queue(&self) {
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
                    warn!(task_id = ?TaskKey::DrainMetadataProjectionQueue, error = ?error, "Failed to replay metadata event log fallback");
                    self.reschedule_timer(
                        TaskKey::DrainMetadataProjectionQueue,
                        METADATA_PROJECTION_RETRY_AFTER,
                    )
                    .await;
                }
            }
            Ok(_) => {}
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainMetadataProjectionQueue, error = ?error, "Failed to drain metadata projection queue");
                self.reschedule_timer(
                    TaskKey::DrainMetadataProjectionQueue,
                    METADATA_PROJECTION_RETRY_AFTER,
                )
                .await;
            }
        }
    }

    pub(super) async fn drain_blob_replication_queue(&self) {
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
                warn!(task_id = ?TaskKey::DrainBlobReplicationQueue, error = ?error, "Failed to drain blob replication queue");
                self.reschedule_timer(
                    TaskKey::DrainBlobReplicationQueue,
                    BLOB_REPLICATION_RETRY_AFTER,
                )
                .await;
            }
        }
    }

    pub(super) async fn drain_mirror_repair(&self) {
        let Some(net_handle) = self.context.net_handle.as_ref() else {
            self.reschedule_timer(TaskKey::DrainSyncMirrorRepair, MIRROR_REPAIR_RETRY_AFTER)
                .await;
            return;
        };
        match process_mirror_repairs(&self.context, net_handle.node_id()).await {
            Ok(result) if result.has_more_due => {
                self.reschedule_timer(TaskKey::DrainSyncMirrorRepair, Duration::ZERO)
                    .await;
            }
            Ok(result) => {
                if let Some(after) = result.next_due_after {
                    self.reschedule_timer(TaskKey::DrainSyncMirrorRepair, after)
                        .await;
                }
            }
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainSyncMirrorRepair, %error, "Failed to drain sync mirror repair queue");
                self.reschedule_timer(TaskKey::DrainSyncMirrorRepair, MIRROR_REPAIR_RETRY_AFTER)
                    .await;
            }
        }
    }

    pub(super) async fn drain_reference_metadata_refresh_queue(&self) {
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
                warn!(task_id = ?TaskKey::DrainReferenceMetadataRefreshQueue, error = ?error, "Failed to drain reference metadata refresh queue");
                self.reschedule_timer(
                    TaskKey::DrainReferenceMetadataRefreshQueue,
                    REFERENCE_METADATA_REFRESH_RETRY_AFTER,
                )
                .await;
            }
        }
    }
}

impl OperationsTaskHandler {
    /// Runs every witness round whose persisted deadline has elapsed.
    pub(super) async fn drain_job_witness_queue(&self) {
        let now_ms = unix_timestamp_millis();
        if drain_witness_deadlines(self.context.as_ref(), now_ms).await {
            self.reschedule_timer(TaskKey::DrainJobWitnessQueue, WITNESS_RETRY_AFTER)
                .await;
        }
    }
}

impl OperationsTaskHandler {
    /// Retries the terminal publications receipted executions still owe. The
    /// reservation row keeps the obligation durable, so the retry re-arms until
    /// every one of them is published and its capacity released.
    pub(super) async fn settle_job_terminals(&self) {
        let pending = match settle_terminals(self.context.as_ref()).await {
            Ok(pending) => pending,
            Err(error) => {
                warn!(task_id = ?TaskKey::SettleJobTerminals, error = %error, "Failed to settle terminal job publications");
                true
            }
        };
        if pending {
            self.reschedule_timer(TaskKey::SettleJobTerminals, SETTLE_RETRY_AFTER)
                .await;
        }
    }
}

impl OperationsTaskHandler {
    /// Replicates locally published job-family records to the other holders.
    /// The pass is bounded, so a large backlog re-arms instead of blocking.
    pub(super) async fn drain_job_family_outbox(&self) {
        if drain_family_outbox(self.context.as_ref()).await {
            self.reschedule_timer(TaskKey::DrainJobFamilyOutbox, OUTBOX_RETRY_AFTER)
                .await;
        }
    }
}

impl OperationsTaskHandler {
    pub(super) async fn prune_notifications(&self) {
        let after = match process_notification_prune_batch(&self.context).await {
            Ok(outcome) if outcome.has_more => Duration::ZERO,
            Ok(outcome) => outcome
                .next_due_after
                .unwrap_or(NOTIFICATION_PRUNE_POLL_AFTER)
                .min(NOTIFICATION_PRUNE_POLL_AFTER),
            Err(error) => {
                warn!(task_id = ?TaskKey::PruneNotifications, error = %error, "Failed to prune notifications");
                NOTIFICATION_PRUNE_RETRY_AFTER
            }
        };
        self.reschedule_timer(TaskKey::PruneNotifications, after)
            .await;
    }
}

impl OperationsTaskHandler {
    pub(super) async fn read_realm_config(&self, realm_id: RealmId) -> Option<RealmConfigDocument> {
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
                    warn!(task_id = ?TaskKey::DrainNotificationOutbox, realm_id = %realm_id, error = %error, "Failed to decode realm config for notification drain");
                    None
                }
            },
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => None,
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(task_id = ?TaskKey::DrainNotificationOutbox, realm_id = %realm_id, error = %error, "Failed to read realm config for notification drain");
                None
            }
            other => {
                warn!(task_id = ?TaskKey::DrainNotificationOutbox, realm_id = %realm_id, event = ?other, "Unexpected realm config read result for notification drain");
                None
            }
        }
    }
}
