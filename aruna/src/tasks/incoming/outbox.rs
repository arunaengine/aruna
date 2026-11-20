//! Drains the sync outbox in pages and publishes batched document records to peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_operations::node::finish_batch::FinishBatchOperation;
use aruna_operations::node::publish_batches::PublishBatchesOperation;

/// The realm configuration the drain decides with: `None` on a node that has none yet, where the
/// bootstrap rules apply, and an error when it cannot be read or decoded, which postpones the drain
/// rather than let an unknown membership publish.
pub(super) async fn load_drain_config(
    context: &Arc<DriverContext>,
    realm_id: aruna_core::structs::identity::realm::RealmId,
) -> Result<Option<aruna_core::structs::identity::realm::RealmConfigDocument>, String> {
    let target = DocumentTarget::RealmConfig { realm_id };
    match context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
            None => Ok(None),
            Some(bytes) => {
                let config =
                    aruna_core::structs::identity::realm::RealmConfigDocument::from_bytes(&bytes);
                config.map(Some).map_err(|error| error.to_string())
            }
        },
        other => Err(format!(
            "the realm configuration read answered {}",
            other.label()
        )),
    }
}

#[cfg(debug_assertions)]
impl OutboxBarrier {
    pub(super) fn new() -> Option<Self> {
        let marker = std::env::var("ARUNA_TEST_OUTBOX_BARRIER")
            .ok()
            .map(std::path::PathBuf::from)?;
        Self::arm(marker)
    }

    /// Pins only the first drain of a launch. The test releases it once, so a
    /// retry or refire drain after that must run instead of waiting again.
    pub(super) fn arm(marker: std::path::PathBuf) -> Option<Self> {
        use std::io::Write;
        let armed = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)
            .and_then(|mut file| file.write_all(b"active"));
        match armed {
            Ok(()) => Some(Self { marker }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
            Err(error) => {
                warn!(error = %error, "Failed to arm outbox test barrier");
                None
            }
        }
    }

    /// Holds the drain open until the test writes "release" into the marker
    /// file, so the test alone decides when this drain may finish.
    pub(super) async fn wait_start(&self) {
        loop {
            let released =
                std::fs::read_to_string(&self.marker).is_ok_and(|value| value.trim() == "release");
            if released {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl OperationsTaskHandler {
    async fn read_drain_page(
        &self,
        retry_key: &TaskKey,
        rotation: &OutboxRotation,
        invocation: &mut DrainInvocation,
    ) -> DrainPage {
        let page_limit = OUTBOX_DRAIN_SIZE.min(
            self.outbox_limits
                .records
                .saturating_sub(invocation.records),
        );
        let scan_started = Instant::now();
        let read = OutboxPagePlan {
            prefix: Vec::new(),
            start_after: invocation.cursor.clone(),
            limit: page_limit,
        };
        let mut batch = match drive(PlanOperation::new(read), self.context.as_ref()).await {
            Ok(batch) => batch,
            Err(error) => {
                warn!(task_id = ?retry_key, error = %error, "Failed to read document sync outbox record");
                invocation.read_failed = true;
                return DrainPage::Stop;
            }
        };
        if batch.records.iter().any(|(_, record)| matches!(&record.event,
            DocumentOutboxEvent::AdminOperation { event, .. }
                if matches!(event.op, aruna_core::admin_documents::AdminDocumentOperation::GroupDeleted { .. })))
            && let Err(error) = aruna_drivers::driver::drive(
                aruna_operations::groups::deletion::coordinator::SyncDecisionOperation,
                self.context.as_ref(),
            )
            .await
        {
            warn!(error = %error, "Group deletion decision is not durable; retaining outbox");
            invocation.read_failed = true;
            invocation.outcome.retry_needed = true;
            return DrainPage::Stop;
        }
        invocation.scan_elapsed += scan_started.elapsed();
        let has_more = batch.has_more;
        invocation.cursor = batch.next_start_after;
        let boundary_reached = rotation.at_end(invocation.cursor.as_deref());
        batch.records.retain(|(key, _)| rotation.admits(key));
        if !batch.records.is_empty() {
            return DrainPage::Records {
                records: batch.records,
                has_more,
                boundary_reached,
            };
        }
        if boundary_reached || !has_more {
            invocation.reached_end = true;
            DrainPage::Stop
        } else if invocation.cursor.is_some() {
            DrainPage::Skip
        } else {
            invocation.reached_end = true;
            DrainPage::Stop
        }
    }
}

impl OperationsTaskHandler {
    async fn run_drain(&self) {
        let retry_key = TaskKey::DrainSyncOutbox;
        let drain_started = Instant::now();

        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(task_id = ?retry_key, "Cannot drain document sync outbox without net handle");
            self.retry_drain(retry_key, self.take_rotation()).await;
            return;
        };

        let realm_id = *net_handle.realm_id();
        let realm_config = match load_drain_config(&self.context, realm_id).await {
            Ok(config) => config,
            Err(error) => {
                // No row is decided while membership is unknown, so all keep their order.
                warn!(task_id = ?retry_key, %error, "Outbox drain waits for the realm config");
                self.retry_drain(retry_key, self.take_rotation()).await;
                return;
            }
        };

        let rotation = self.take_rotation();
        let Some(rotation) = self.open_rotation(&retry_key, rotation).await else {
            return;
        };
        let mut invocation = DrainInvocation::new(&rotation);
        loop {
            if invocation.records >= self.outbox_limits.records
                || invocation.pages >= self.outbox_limits.pages
            {
                break;
            }
            match self
                .read_drain_page(&retry_key, &rotation, &mut invocation)
                .await
            {
                DrainPage::Records {
                    records,
                    has_more,
                    boundary_reached,
                } => {
                    self.process_drain_page(
                        &retry_key,
                        net_handle,
                        realm_config.as_ref(),
                        realm_id,
                        records,
                        &mut invocation,
                    )
                    .await;
                    if boundary_reached || !has_more {
                        invocation.reached_end = true;
                        break;
                    }
                }
                DrainPage::Skip => continue,
                DrainPage::Stop => break,
            }
        }

        self.finish_drain_invocation(
            retry_key,
            net_handle,
            realm_id,
            rotation,
            invocation,
            drain_started,
        )
        .await;
    }
}

impl OperationsTaskHandler {
    /// Runs one bounded invocation of the open rotation, then continues, yields
    /// through the timer, or closes it. No record is ever deleted, truncated, or
    /// overwritten to satisfy a bound.
    pub(super) async fn drain_sync_outbox(&self) {
        let _drain = self.drain_guard.lock().await;
        #[cfg(debug_assertions)]
        if let Some(barrier) = OutboxBarrier::new() {
            barrier.wait_start().await;
        }

        self.run_drain().await;
    }
}

impl OperationsTaskHandler {
    pub(super) async fn close_rotation(&self, retry_key: TaskKey, mut rotation: OutboxRotation) {
        let (closed, after) = rotation.close_plan();
        self.store_rotation(rotation);
        if closed.examined > 0 {
            info!(
                event = "pipeline.drain.rotation",
                examined = closed.examined,
                deleted = closed.deleted,
                deferred = closed.deferred,
                undeliverable = closed.undeliverable,
                retry_invocations = closed.retry_invocations,
                invocations = closed.invocations,
                "Document sync outbox rotation complete"
            );
        }

        if let Some(after) = after {
            self.reschedule_timer(retry_key, after).await;
        }
    }
}

impl OperationsTaskHandler {
    pub(super) async fn open_rotation(
        &self,
        retry_key: &TaskKey,
        mut rotation: OutboxRotation,
    ) -> Option<OutboxRotation> {
        if rotation.boundary.is_none() {
            match drive(OutboxTailsOperation::new(), self.context.as_ref()).await {
                // An empty outbox drops the rotation, and with it the retry ladder.
                Ok(boundaries) if boundaries.is_empty() => return None,
                Ok(boundaries) => {
                    rotation.boundary = boundaries.iter().map(|(_, key)| key).max().cloned();
                    rotation.stream_boundaries = boundaries;
                }
                Err(error) => {
                    warn!(task_id = ?retry_key, %error, "Failed to open document sync outbox rotation");
                    self.retry_drain(retry_key.clone(), rotation).await;
                    return None;
                }
            }
        }
        debug_assert!(rotation.boundary.is_some());
        Some(rotation)
    }
}

impl OperationsTaskHandler {
    pub(super) fn store_rotation(&self, rotation: OutboxRotation) {
        *self
            .rotation
            .lock()
            .expect("outbox rotation mutex poisoned") = rotation;
    }
}

impl OperationsTaskHandler {
    /// Takes the open rotation, leaving a fresh one for a concurrent
    /// invocation.
    fn take_rotation(&self) -> OutboxRotation {
        std::mem::take(
            &mut *self
                .rotation
                .lock()
                .expect("outbox rotation mutex poisoned"),
        )
    }
}

impl OperationsTaskHandler {
    async fn process_drain_page(
        &self,
        retry_key: &TaskKey,
        net_handle: &aruna_net::NetHandle,
        config: Option<&aruna_core::structs::identity::realm::RealmConfigDocument>,
        realm_id: RealmId,
        records: Vec<(Vec<u8>, DocumentOutboxRecord)>,
        invocation: &mut DrainInvocation,
    ) {
        invocation.pages += 1;
        invocation.records += records.len();
        if let Some(page_oldest) = records
            .iter()
            .map(|(_, record)| record.outbox_id.timestamp_ms())
            .min()
        {
            invocation.oldest_record_ms = Some(
                invocation
                    .oldest_record_ms
                    .map_or(page_oldest, |current| current.min(page_oldest)),
            );
        }
        let records = self
            .prepare_drain_records(retry_key, net_handle, config, realm_id, records, invocation)
            .await;
        let (to_publish, deferred, undeliverable) = partition_drain_records(
            records,
            &mut invocation.defer,
            !self.is_device(config),
            |topic| net_handle.sync_topic_exists(topic).unwrap_or(false),
            |record| classify_deferred_record(config, net_handle.node_id(), record),
        );
        invocation.deferred += deferred.len();
        let now_ms = unix_timestamp_millis();
        for (_, record, topic) in &deferred {
            let record_ms = record.outbox_id.timestamp_ms();
            if now_ms.saturating_sub(record_ms) < OUTBOX_STUCK_AFTER.as_millis() as u64 {
                continue;
            }
            invocation.stuck += 1;
            if invocation
                .oldest_stuck
                .as_ref()
                .is_none_or(|(oldest_ms, ..)| record_ms < *oldest_ms)
            {
                invocation.oldest_stuck =
                    Some((record_ms, record.target.clone(), *topic, record.placement));
            }
        }
        let relayed = self
            .relay_undeliverable_records(config, &undeliverable)
            .await;
        invocation.undeliverable += undeliverable.len().saturating_sub(relayed.len());
        if !relayed.is_empty()
            && let Err(error) = drive(
                PlanOperation::new(DeleteOutboxPlan { keys: relayed }),
                self.context.as_ref(),
            )
            .await
        {
            warn!(%error, "Failed to delete relayed admin outbox records");
        }

        let (groups, subbatches) = {
            let published = self
                .published_records
                .lock()
                .expect("published records poisoned");
            build_drain_batches(to_publish, &published)
        };
        invocation.groups += groups;
        invocation.subbatches += subbatches.len();
        let (publish_elapsed, outcome) = self
            .publish_drain_batches(retry_key, net_handle, subbatches)
            .await;
        invocation.publish_elapsed += publish_elapsed;
        invocation.outcome.merge(outcome);
    }
}

impl OperationsTaskHandler {
    async fn prepare_drain_records(
        &self,
        retry_key: &TaskKey,
        net_handle: &aruna_net::NetHandle,
        config: Option<&aruna_core::structs::identity::realm::RealmConfigDocument>,
        realm_id: RealmId,
        records: Vec<(Vec<u8>, DocumentOutboxRecord)>,
        invocation: &mut DrainInvocation,
    ) -> Vec<DrainRecord> {
        let mut records: Vec<DrainRecord> = records
            .into_iter()
            .map(|(record_key, mut record)| {
                invocation.config_drained |=
                    matches!(record.target, DocumentTarget::RealmConfig { .. });
                record.placement =
                    resolve_publish_placement(config, &record.target, record.placement);
                let topic = record.target.sync_topic_id(realm_id, &record.placement);
                (record_key, record, topic)
            })
            .collect();

        // Pull a missing shard genesis from stamped ex-holders and live holders.
        // Only current or draining former holders may adopt it for publication.
        let mut missing_topics: BTreeMap<
            Vec<aruna_core::id::NodeId>,
            (Vec<aruna_core::id::NodeId>, BTreeSet<irokle::TopicId>),
        > = BTreeMap::new();
        for (_, record, topic) in &records {
            if !record.target.uses_shard_topic()
                || invocation.defer.deferred_topics.contains(topic)
                || !config.is_none_or(|config| {
                    aruna_core::placement::holds_placement(
                        config,
                        &record.placement,
                        net_handle.node_id(),
                    ) || aruna_core::placement::is_draining_holder(
                        config,
                        &record.placement,
                        net_handle.node_id(),
                    )
                })
                || net_handle.sync_topic_exists(*topic).unwrap_or(false)
            {
                continue;
            }
            let mut bootstrap_peers = record.peers.clone();
            if let Some(config) = config {
                for holder in
                    aruna_core::placement::resolve_shard_holders(config, &record.placement)
                {
                    if !bootstrap_peers.contains(&holder) {
                        bootstrap_peers.push(holder);
                    }
                }
            }
            bootstrap_peers.retain(|peer| *peer != net_handle.node_id());
            if bootstrap_peers.is_empty() {
                continue;
            }
            let mut peer_key = bootstrap_peers.clone();
            aruna_operations::sync::shard_placement::sort_node_ids(&mut peer_key);
            missing_topics
                .entry(peer_key)
                .or_insert_with(|| (bootstrap_peers, BTreeSet::new()))
                .1
                .insert(*topic);
        }
        for (_, (peers, topics)) in missing_topics {
            let sync = SyncTopicsOperation::new(
                *net_handle.realm_id(),
                topics.into_iter().collect(),
                peers,
            )
            .queuing_metadata(net_handle.node_id());
            let event = Event::Net(NetEvent::DocumentSync(DocumentNetEvent::reconciled(
                drive(sync, self.context.as_ref()).await,
            )));
            let outcome = self
                .finish_sync_batch(retry_key, Vec::new(), Vec::new(), event, Default::default())
                .await;
            invocation.outcome.merge(outcome);
        }

        // Publish to the bucket's sync members (admitted targets and retained departing holders included), but
        // keep stamped peers above as genesis sources: a target must see writes made during the window.
        if let Some(config) = config {
            let now_ms = aruna_clock::unix_timestamp_millis();
            for (_, record, _) in &mut records {
                if !record.target.uses_shard_topic() {
                    continue;
                }
                let members =
                    aruna_core::placement::bucket_membership(config, &record.placement, now_ms)
                        .members;
                if !members.is_empty() {
                    record.peers = members;
                }
            }
        }
        records
    }
}

impl OperationsTaskHandler {
    /// Publishes and syncs the sub-batches through their operation and keeps which records this
    /// process published; a run that broke off retries every sub-batch and blocks its domains.
    async fn publish_drain_batches(
        &self,
        retry_key: &TaskKey,
        net_handle: &aruna_net::NetHandle,
        subbatches: Vec<DrainSubBatch>,
    ) -> (Duration, DrainSyncOutcome) {
        let mut blocked = DrainSyncOutcome {
            retry_needed: true,
            ..DrainSyncOutcome::default()
        };
        for subbatch in &subbatches {
            let (topics, origins) = subbatch.ordering_domains();
            blocked.blocked_topics.extend(topics);
            blocked.blocked_origins.extend(origins);
        }
        let serving = (net_handle.node_id(), *net_handle.realm_id());
        let publish = PublishBatchesOperation::new(serving, subbatches);
        match drive(publish, self.context.as_ref()).await {
            Ok(done) => {
                self.note_published(&done.published);
                self.forget_published(&done.deleted);
                (done.publish_elapsed, done.outcome)
            }
            Err(error) => {
                warn!(task_id = ?retry_key, %error, "Publishing document sync batches broke off");
                (Duration::ZERO, blocked)
            }
        }
    }
}

impl OperationsTaskHandler {
    pub(super) async fn finish_retry(
        &self,
        retry_key: TaskKey,
        mut rotation: OutboxRotation,
        reached_end: bool,
    ) {
        rotation.continuations = 0;
        if reached_end {
            self.close_rotation(retry_key, rotation).await;
        } else {
            self.retry_drain(retry_key, rotation).await;
        }
    }
}

impl OperationsTaskHandler {
    async fn finish_drain_invocation(
        &self,
        retry_key: TaskKey,
        net_handle: &aruna_net::NetHandle,
        realm_id: RealmId,
        mut rotation: OutboxRotation,
        mut invocation: DrainInvocation,
        drain_started: Instant,
    ) {
        invocation
            .defer
            .deferred_topics
            .extend(invocation.outcome.blocked_topics.iter().copied());
        invocation
            .defer
            .blocked_origins
            .extend(invocation.outcome.blocked_origins.iter().copied());
        if let Some((oldest_ms, target, topic, placement)) = &invocation.oldest_stuck {
            error!(
                event = "pipeline.drain.stuck",
                count = invocation.stuck,
                oldest_age_ms = unix_timestamp_millis().saturating_sub(*oldest_ms),
                representative_target = ?target,
                representative_topic = %topic,
                representative_strategy = %placement.strategy_id,
                representative_shard = placement.shard,
                "Document sync outbox records are stuck: this node holds their buckets but their shard topic geneses have never arrived"
            );
        }
        if invocation.config_drained {
            self.schedule_sync_placements(realm_id, net_handle.node_id())
                .await;
        }

        let unvisited = invocation.has_unvisited();
        rotation.cursor = if invocation.reached_end {
            None
        } else {
            invocation.cursor.clone()
        };
        rotation.blocked_topics = invocation.defer.deferred_topics;
        rotation.blocked_origins = invocation.defer.blocked_origins;
        rotation.undeliverable_topics = invocation.defer.undeliverable_topics;
        rotation.totals.examined += invocation.records;
        rotation.totals.deleted += invocation.outcome.deleted;
        rotation.totals.deferred += invocation.deferred;
        rotation.totals.undeliverable += invocation.undeliverable;
        rotation.totals.retry_invocations +=
            usize::from(invocation.outcome.retry_needed || invocation.read_failed);
        rotation.totals.invocations = rotation.totals.invocations.saturating_add(1);

        let oldest_age_ms = invocation
            .oldest_record_ms
            .map(|record_ms| unix_timestamp_millis().saturating_sub(record_ms))
            .unwrap_or(0);
        if invocation.records > 0 {
            info!(
                event = "pipeline.drain.summary",
                records = invocation.records,
                examined = invocation.records,
                deleted = invocation.outcome.deleted,
                deferred = invocation.deferred,
                undeliverable = invocation.undeliverable,
                retry_scheduled = invocation.outcome.retry_needed || invocation.read_failed,
                has_unvisited = unvisited,
                continuation = rotation.continuations,
                rotation_complete = invocation.reached_end,
                groups = invocation.groups,
                subbatches = invocation.subbatches,
                pages = invocation.pages,
                scan_ms = duration_ms(invocation.scan_elapsed),
                publish_ms = duration_ms(invocation.publish_elapsed),
                sync_ms = duration_ms(invocation.outcome.sync_elapsed),
                project_ms = duration_ms(invocation.outcome.project_elapsed),
                delete_ms = duration_ms(invocation.outcome.delete_elapsed),
                total_ms = duration_ms(drain_started.elapsed()),
                oldest_age_ms,
                retry = invocation.outcome.retry_needed || invocation.read_failed,
                "Document sync outbox drain summary"
            );
        }

        if invocation.outcome.retry_needed || invocation.read_failed {
            self.finish_retry(retry_key, rotation, invocation.reached_end)
                .await;
        } else if unvisited {
            if rotation.continuations < self.outbox_limits.continuation_streak {
                rotation.continuations = rotation.continuations.saturating_add(1);
                self.store_rotation(rotation);
                self.reschedule_timer(retry_key, OUTBOX_CONTINUATION_AFTER)
                    .await;
            } else {
                rotation.continuations = 0;
                self.store_rotation(rotation);
                self.reschedule_timer(retry_key, DEFER_RETRY_AFTER).await;
            }
        } else {
            self.close_rotation(retry_key, rotation).await;
        }
    }
}

impl OutboxDrainer {
    pub fn new(context: Arc<DriverContext>) -> Self {
        Self {
            handler: Arc::new(OperationsTaskHandler::new(context, JobsRuntime::new())),
        }
    }

    /// Runs one bounded invocation of the open rotation.
    pub async fn run_once(&self) {
        self.handler.drain_sync_outbox().await;
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

/// Kicks the installed document-sync drain owner without replacing an existing
/// persisted retry deadline.
pub async fn drive_sync_drain(context: Arc<DriverContext>) {
    if context.task_handle.is_none() {
        warn!("Cannot kick document sync outbox drain without task handle");
        return;
    }
    if let Err(error) = drive(OutboxTimerOperation::new(), context.as_ref()).await {
        warn!(%error, "Failed to restore document sync outbox timer");
    }
}

impl OperationsTaskHandler {
    /// Finishes one synced sub-batch through its operation, and forgets the records it deleted.
    pub(super) async fn finish_sync_batch(
        &self,
        retry_key: &TaskKey,
        record_keys: Vec<Vec<u8>>,
        requested_targets: Vec<DocumentTarget>,
        event: Event,
        outcome: DrainSyncOutcome,
    ) -> DrainSyncOutcome {
        let (serving, generation) = (self.context.node_and_realm(), ulid::Ulid::generate());
        let batch = (record_keys.clone(), requested_targets);
        let finish = FinishBatchOperation::new(serving, generation, batch, event, outcome);
        match drive(finish, self.context.as_ref()).await {
            Ok(finished) => {
                if finished.deleted {
                    self.forget_published(&record_keys);
                }
                finished.outcome
            }
            Err(error) => {
                warn!(task_id = ?retry_key, %error, "Finishing a document sync batch broke off");
                DrainSyncOutcome {
                    retry_needed: true,
                    ..DrainSyncOutcome::default()
                }
            }
        }
    }
}

impl OperationsTaskHandler {
    fn note_published(&self, record_keys: &[Vec<u8>]) {
        let mut published = self
            .published_records
            .lock()
            .expect("published records poisoned");
        if published.len() + record_keys.len() > MAX_PUBLISHED_RECORDS {
            published.clear();
        }
        published.extend(record_keys.iter().cloned());
    }

    fn forget_published(&self, record_keys: &[Vec<u8>]) {
        let mut published = self
            .published_records
            .lock()
            .expect("published records poisoned");
        for record_key in record_keys {
            published.remove(record_key);
        }
    }
}
