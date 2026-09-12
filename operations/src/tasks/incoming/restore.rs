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
        restore_blob_timer(&context.storage_handle, &task_handle).await;
        crate::s3::refresh_metadata::restore_timer(&context.storage_handle, &task_handle).await;
        restore_outbox_timers(&context.storage_handle, &task_handle).await;
        restore_publish_timer(&context.storage_handle, &task_handle).await;
        restore_sync_timers(&context, &task_handle).await;
        restore_usage_timer(&context.storage_handle, &task_handle).await;
        crate::notifications::watch::interest::restore_publish_timer(
            &context.storage_handle,
            &task_handle,
        )
        .await;
        crate::node::node_info::restore_info_timer(&context.storage_handle, &task_handle).await;
        restore_idle_timer(
            &context.storage_handle,
            &task_handle,
            NOTIFICATION_DELIVERY_RETRY_AFTER,
        )
        .await;
        restore_projection_timer(&context.storage_handle, &task_handle).await;
        // Dead letters retry on a minute-scale backoff, so sweeping every rearm
        // tick would scan the keyspace far more often than it can yield work.
        if ticks.is_multiple_of(DEAD_LETTER_SWEEP_TICKS) {
            sweep_dead_letters(&context.storage_handle).await;
        }
        restore_materialization_timer(&context.storage_handle.bulk(), &task_handle).await;
        crate::metadata::prune_queue::restore_prune_timer(&context.storage_handle, &task_handle)
            .await;
        crate::notifications::prune::restore_prune_timer(&context.storage_handle, &task_handle)
            .await;
        restore_drain_timer(&context.storage_handle, &task_handle).await;
        restore_prune_timer(&context.storage_handle, &task_handle).await;
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
        let table = rebuild_interest_table(&context.storage_handle).await;
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
        restore_task_timers(&context.storage_handle, &task_handle).await;
        restore_outbox_timers(&context.storage_handle, &task_handle).await;
        restore_publish_timer(&context.storage_handle, &task_handle).await;
        // Before the first refresh: a queued edit is the one local change no
        // holder would hand back.
        crate::device::edit::replay_queued_edits(&context).await;
        restore_sync_timers(&context, &task_handle).await;
        restore_usage_timer(&context.storage_handle, &task_handle).await;
        crate::notifications::watch::interest::restore_publish_timer(
            &context.storage_handle,
            &task_handle,
        )
        .await;
        crate::node::node_info::restore_info_timer(&context.storage_handle, &task_handle).await;
        restore_outbox_timer(&context.storage_handle, &task_handle, Duration::ZERO).await;
        restore_projection_timer(&context.storage_handle, &task_handle).await;
        sweep_dead_letters(&context.storage_handle).await;
        restore_materialization_timer(&context.storage_handle.bulk(), &task_handle).await;
        crate::metadata::prune_queue::restore_prune_timer(&context.storage_handle, &task_handle)
            .await;
        crate::notifications::prune::restore_prune_timer(&context.storage_handle, &task_handle)
            .await;
        restore_blob_timer(&context.storage_handle, &task_handle).await;
        crate::s3::refresh_metadata::restore_timer(&context.storage_handle, &task_handle).await;
        restore_prune_timer(&context.storage_handle, &task_handle).await;
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
        match crate::node::usage_stats::publish_refresh_snapshots(
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
            if let Err(error) =
                crate::node::node_info::refresh_info_heartbeat(&self.context, node_id, realm_id)
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
                let table = crate::notifications::watch::interest::rebuild_interest_table(
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

    pub(super) async fn drain_materialization_queue(&self) {
        let bulk = self.bulk_context();
        match process_materialization_batch(&bulk).await {
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
            Ok(_) => match materialization_jobs_exist(&bulk.storage_handle).await {
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

    pub(super) async fn drain_graph_queue(&self) {
        let bulk = self.bulk_context();
        match crate::metadata::prune_queue::process_prune_batch(&bulk).await {
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
            Ok(_) => match prune_jobs_exist(&bulk.storage_handle).await {
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
    pub(super) async fn drain_projection_queue(&self) {
        match drain_projection_queue(&self.context).await {
            Ok(result) if result.has_more => {
                self.reschedule_timer(
                    TaskKey::DrainMetadataProjectionQueue,
                    std::time::Duration::ZERO,
                )
                .await;
            }
            Ok(result) if result.markers_examined == 0 => {
                if let Err(error) = replay_event_log(&self.context).await {
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

    pub(super) async fn drain_replication_queue(&self) {
        match process_blob_batch(&self.context).await {
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

    pub(super) async fn drain_refresh_queue(&self) {
        match crate::s3::refresh_metadata::process_batch(&self.context).await {
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
    pub(super) async fn drain_witness_queue(&self) {
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
    pub(super) async fn drain_family_outbox(&self) {
        if drain_family_outbox(self.context.as_ref()).await {
            self.reschedule_timer(TaskKey::DrainJobFamilyOutbox, OUTBOX_RETRY_AFTER)
                .await;
        }
    }
}

impl OperationsTaskHandler {
    pub(super) async fn prune_notifications(&self) {
        let after = match process_prune_batch(&self.context).await {
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

impl OperationsTaskHandler {
    pub(super) async fn drain_notification_outbox(&self) {
        let retry_key = TaskKey::DrainNotificationOutbox;

        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(task_id = ?retry_key, "Cannot drain notification outbox without net handle");
            self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                .await;
            return;
        };
        let local_node_id = net_handle.node_id();

        let mut snapshot_owner = match self.context.storage_handle.start_transaction(true).await {
            Ok(owner) => owner,
            Err(error) => {
                warn!(task_id = ?retry_key, error = %error, "Failed to start notification outbox snapshot");
                self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                    .await;
                return;
            }
        };
        let Some(snapshot_txn_id) = snapshot_owner.id() else {
            warn!(task_id = ?retry_key, "Notification outbox snapshot owner missing transaction");
            self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                .await;
            return;
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
            let batch = match read_outbox_batch(
                &self.context.storage_handle,
                start_after.clone(),
                NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE,
                Some(snapshot_txn_id),
            )
            .await
            {
                Ok(batch) => batch,
                Err(error) => {
                    warn!(task_id = ?retry_key, error = %error, "Failed to read notification outbox record");
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
                    warn!(task_id = ?retry_key, outbox_id = %outbox_record.outbox_id, age_ms, "Dropping expired notification outbox record");
                    if let Err(error) =
                        delete_outbox_records(&self.context.storage_handle, vec![record_key]).await
                    {
                        warn!(task_id = ?retry_key, error = %error, "Failed to delete expired notification outbox record");
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
                    warn!(task_id = ?retry_key, realm_id = %realm_id, "Notification realm config unavailable; retrying delivery");
                    retry_needed = true;
                    continue;
                };

                let holder = match resolve_inbox_holder(&record.recipient, config) {
                    Ok(holder) => holder,
                    Err(error) => {
                        warn!(task_id = ?retry_key, recipient = %record.recipient, error = %error, "Failed to resolve notification inbox holder");
                        retry_needed = true;
                        continue;
                    }
                };
                let Some(holder) = holder else {
                    warn!(task_id = ?retry_key, recipient = %record.recipient, "No eligible notification inbox holder; retrying delivery");
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
                match upsert_with_report(&self.context.storage_handle, &local_records).await {
                    Ok(outcome) => {
                        for recipient in &outcome.recipients {
                            net_handle.notify_inbox_activity(*recipient);
                        }
                        if let Err(error) =
                            delete_outbox_records(&self.context.storage_handle, local_keys).await
                        {
                            warn!(task_id = ?retry_key, error = %error, "Failed to delete delivered notification outbox records");
                            retry_needed = true;
                        }
                    }
                    Err(error) => {
                        warn!(task_id = ?retry_key, error = %error, "Failed to deliver notifications to local inbox");
                        retry_needed = true;
                    }
                }
            }

            for (holder, (records, keys)) in remote_groups {
                match deliver_remote(net_handle, holder, records).await {
                    Ok(_) => {
                        if let Err(error) =
                            delete_outbox_records(&self.context.storage_handle, keys).await
                        {
                            warn!(task_id = ?retry_key, error = %error, "Failed to delete delivered notification outbox records");
                            retry_needed = true;
                        }
                    }
                    Err(error) => {
                        warn!(task_id = ?retry_key, holder = %holder, error = %error, "Failed to deliver notifications to remote holder");
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
            Event::Storage(StorageEvent::TransactionCommitted { txn_id })
                if txn_id == snapshot_txn_id =>
            {
                snapshot_owner.finish()
            }
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(task_id = ?retry_key, error = %error, "Failed to close notification outbox snapshot");
                match error {
                    aruna_core::errors::StorageError::TransactionConflict
                    | aruna_core::errors::StorageError::TransactionNotFound => {
                        snapshot_owner.finish();
                    }
                    aruna_core::errors::StorageError::QueueFull => {}
                    _ => snapshot_owner.unknown(),
                }
                retry_needed = true;
            }
            other => {
                warn!(task_id = ?retry_key, event = ?other, "Unexpected notification outbox snapshot close result");
                snapshot_owner.unknown();
                retry_needed = true;
            }
        }

        if retry_needed {
            self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                .await;
        } else {
            match read_outbox_batch(&self.context.storage_handle, None, 1, None).await {
                Ok(batch) if !batch.records.is_empty() || batch.has_more => {
                    self.reschedule_timer(retry_key, Duration::ZERO).await;
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(task_id = ?retry_key, error = %error, "Failed to check for notification outbox records appended during drain");
                    self.reschedule_timer(retry_key, NOTIFICATION_DELIVERY_RETRY_AFTER)
                        .await;
                }
            }
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
            self.reschedule_timer(TaskKey::DrainJobQueue, JOB_DRAIN_RETRY_AFTER)
                .await;
            return;
        };
        let Some(claim_producer) = self.jobs_runtime.claim_producer().await else {
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

        let reconciler = self.jobs_runtime.reconciler();
        let result = match drain_job_batch(
            &self.context.storage_handle,
            owner_node_id,
            budget,
            reconciler.as_ref(),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainJobQueue, error = %error, "Failed to drain job queue");
                self.reschedule_timer(TaskKey::DrainJobQueue, JOB_DRAIN_RETRY_AFTER)
                    .await;
                return;
            }
        };

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
                if let Err(error) = release_job(
                    &self.context.storage_handle,
                    record.job_id,
                    token,
                    unix_timestamp_millis(),
                )
                .await
                {
                    warn!(job_id = %record.job_id, error = %error, "Failed to release excess job claim");
                }
                continue;
            }
            self.jobs_runtime.spawn(self.context.clone(), record);
        }
        drop(claim_producer);

        // A per-job error stopped the batch after handing off what was claimed; back off
        // and re-drive the remainder rather than hot-looping on the failure.
        if result.retry_after_error {
            self.reschedule_timer(TaskKey::DrainJobQueue, JOB_DRAIN_RETRY_AFTER)
                .await;
            return;
        }

        // Due work left behind by a saturated class: wait for a completion kick, not
        // a ZERO hot-loop.
        match result.next_due_after {
            Some(after) if after.is_zero() && result.deferred_saturated => {
                self.reschedule_timer(TaskKey::DrainJobQueue, JOB_DRAIN_RETRY_AFTER)
                    .await;
            }
            Some(after) => {
                self.reschedule_timer(TaskKey::DrainJobQueue, after).await;
            }
            None => {}
        }
    }

    pub(super) async fn prune_jobs(&self) {
        let after = match prune_job_batch(&self.context).await {
            Ok(outcome) if outcome.has_more => Duration::ZERO,
            Ok(outcome) => outcome
                .next_due_after
                .unwrap_or(JOB_PRUNE_POLL_AFTER)
                .min(JOB_PRUNE_POLL_AFTER),
            Err(error) => {
                warn!(task_id = ?TaskKey::PruneJobs, error = %error, "Failed to prune jobs");
                JOB_PRUNE_RETRY_AFTER
            }
        };
        self.reschedule_timer(TaskKey::PruneJobs, after).await;
    }

    pub(super) async fn drain_blob_cleanup(&self) {
        let after = match process_cleanup_batch(&self.context).await {
            Ok(outcome) if outcome.failed > 0 => BLOB_CLEANUP_RETRY,
            Ok(_) => BLOB_CLEANUP_AFTER,
            Err(error) => {
                warn!(task_id = ?TaskKey::DrainBlobCleanupQueue, error = %error, "Failed to drain blob cleanup");
                BLOB_CLEANUP_RETRY
            }
        };
        // The parts a reclaimed upload frees become cleanup rows, so the sweep
        // rides the same timer that drains them.
        if let Err(error) = sweep_stale_uploads(&self.context, unix_timestamp_millis()).await {
            warn!(task_id = ?TaskKey::DrainBlobCleanupQueue, error = %error, "Failed to sweep stale multipart uploads");
        }
        self.reschedule_timer(TaskKey::DrainBlobCleanupQueue, after)
            .await;
    }

    pub(super) async fn drain_blob_reclaim(&self) {
        let key = TaskKey::DrainBlobReclaimQueue;
        // A failed candidate earns the fast retry, then doubles up to the normal interval, so a permanently
        // failing one cannot hold a one-minute rescan of the whole queue forever.
        let (after, drained) =
            match process_reclaim_batch(&self.context, self.reclaim_start()).await {
                Ok(outcome) => {
                    self.set_reclaim_start(outcome.next_start_after);
                    match (outcome.capped, outcome.failed) {
                        (true, _) => {
                            self.reset_backoff(&key);
                            (RECLAIM_SWEEP_RETRY, false)
                        }
                        (false, 0) => {
                            self.reset_backoff(&key);
                            (RECLAIM_SWEEP_AFTER, true)
                        }
                        (false, _) => (
                            self.retry_ladder(&key, RECLAIM_SWEEP_RETRY, RECLAIM_SWEEP_AFTER),
                            true,
                        ),
                    }
                }
                Err(error) => {
                    warn!(task_id = ?key, error = %error, "Failed to drain blob reclaim");
                    (
                        self.retry_ladder(&key, RECLAIM_SWEEP_RETRY, RECLAIM_SWEEP_AFTER),
                        false,
                    )
                }
            };
        // Removal walks whole keyspaces too, so it only rides a sweep that
        // reached the end of the queue, never the fast retries behind a backlog.
        if drained && let Err(error) = remove_drained_backends(&self.context).await {
            warn!(error = %error, "Failed to remove drained storage backends");
        }
        self.reschedule_timer(key, after).await;
    }

    pub(super) async fn sweep_hidden_blobs(&self) {
        let after = match process_hidden_sweep(&self.context).await {
            Ok(outcome) if outcome.cleanup_pending => HIDDEN_SWEEP_RETRY,
            Ok(_) => HIDDEN_SWEEP_AFTER,
            Err(error) => {
                warn!(task_id = ?TaskKey::SweepHiddenBlobs, error = %error, "Failed to sweep hidden blobs");
                HIDDEN_SWEEP_RETRY
            }
        };
        self.reschedule_timer(TaskKey::SweepHiddenBlobs, after)
            .await;
    }
}

#[doc(hidden)]
pub async fn drain_notification_outbox(context: Arc<DriverContext>) {
    OperationsTaskHandler::new(context, JobsRuntime::new())
        .drain_notification_outbox()
        .await;
}
