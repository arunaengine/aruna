use std::collections::{BTreeMap, BTreeSet, VecDeque};

use ::irokle::{Event as _, Storage as _};
use aruna_core::document::{DocumentSyncEvent, DocumentSyncReconcileResult, DocumentSyncTarget};
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{
    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE, METADATA_CREATE_ACCEPTANCE_KEYSPACE,
};
use aruna_core::metadata::{MetadataCreateEventRecord, MetadataGraphLifecycleRecord};
use aruna_core::storage_entries::{
    create_acceptance_entry, create_acceptance_key, create_projection_entries,
    shard_manifest_entry, sync_revision_entry,
};
use aruna_core::structs::SyncQuarantineIdentity;
use aruna_core::types::Value;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync::storage::{
    replace_batch_in, start_storage_transaction, transaction_read,
};
use crate::document_sync::{
    DOCUMENT_SYNC_FRAME_LEN_LIMIT, DeferredTopicRegistrationOutcome, DocumentEventBatch,
    DocumentSyncDependency, DocumentSyncService, MetadataPlacementFence, MetadataPlacementOutcome,
    PendingMetadataCreateApply, SyncRejection,
};
use crate::error::{NetError, Result};

use super::batch::{BatchOutcome, BatchState, apply_batch_event, is_config_candidate, retry_admin};
use super::cursor::{
    applied_cursor_clock, applied_cursor_value, deferred_topics_key, topic_cursor_key,
};
use super::registry::{
    create_fence_txn, event_is_create, lifecycle_stale_txn, registry_identity_matches,
    same_create_event, transaction_placement_fence, validate_metadata_event,
};
use super::{dependency_available, register_deferred_topic, remove_deferred_topic};

/// One bounded causal batch with the topic identity and cursor it was read
/// under. The cursor is already advanced past the batch.
struct ReconcileBatch {
    genesis: ::irokle::OpId,
    cursor_key: ByteView,
    cursor: ::irokle::ActorClock,
    rejections: Vec<SyncRejection>,
    events: Vec<(DocumentSyncEvent, ::irokle::ActorId, u64)>,
}

/// Topics still to reconcile plus the persisted dependency registry that gates
/// them. Already-satisfied dependencies are resolved once per pass, so a
/// restart does not wait for a new event to re-seed its queue.
struct ReconcileWork {
    deferred_topics: BTreeMap<DocumentSyncDependency, BTreeSet<::irokle::TopicId>>,
    queued_topics: BTreeSet<::irokle::TopicId>,
    queue: VecDeque<::irokle::TopicId>,
}

impl DocumentSyncService {
    /// Reconciles the requested topics in causal batches. One pass holds the
    /// reconcile lock so cursor self-healing, admin settlement and the
    /// deferred-topic registry observe a consistent view.
    pub(in crate::document_sync) async fn reconcile_document_topics(
        &self,
        topic_ids: impl IntoIterator<Item = ::irokle::TopicId>,
    ) -> Result<DocumentSyncReconcileResult> {
        let _reconcile_guard = self.reconcile_lock.lock().await;
        let mut work = self.pending_reconcile_work(topic_ids).await?;
        let mut applied_targets = Vec::new();
        let mut metadata_create_events = Vec::new();
        let mut metadata_graph_tombstones = Vec::new();
        let mut pending_metadata_creates: Vec<PendingMetadataCreateApply> = Vec::new();
        let mut deferred_cursor_writes: Vec<(::irokle::TopicId, (String, ByteView, Value))> =
            Vec::new();
        let mut deferred_rejections: Vec<SyncRejection> = Vec::new();
        while let Some(topic_id) = work.queue.pop_front() {
            work.queued_topics.remove(&topic_id);
            pending_metadata_creates.retain(|pending| pending.identity.topic != topic_id);
            deferred_cursor_writes.retain(|(pending_topic_id, _)| *pending_topic_id != topic_id);
            deferred_rejections.retain(|rejection| rejection.topic_id() != topic_id);
            let Some(batch) = self.load_reconcile_batch(topic_id).await? else {
                continue;
            };
            let genesis = batch.genesis;
            let cursor = batch.cursor.clone();
            let cursor_key = batch.cursor_key.clone();
            let mut outcome = self.apply_reconcile_batch(topic_id, batch).await?;
            applied_targets.append(&mut outcome.applied_targets);
            metadata_graph_tombstones.append(&mut outcome.graph_tombstones);
            pending_metadata_creates.append(&mut outcome.pending_creates);
            if !outcome.cross_topic.is_empty() {
                remove_deferred_topic(&mut work.deferred_topics, topic_id);
                for dependency in outcome.cross_topic {
                    if matches!(
                        register_deferred_topic(&mut work.deferred_topics, dependency, topic_id),
                        DeferredTopicRegistrationOutcome::CapacityExceeded
                    ) {
                        warn!(
                            %topic_id,
                            ?dependency,
                            "Dropping document dependency registration because the deferred-topic registry is full"
                        );
                    }
                }
                // Registry capacity limits retry discovery, not whether an
                // unresolved topic is safe to mark as applied.
                continue;
            }
            let value = applied_cursor_value(self.node.storage(), topic_id, genesis, &cursor)?;
            if outcome.deferred_creates {
                deferred_rejections.append(&mut outcome.rejections);
                deferred_cursor_writes.push((
                    topic_id,
                    (
                        DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                        cursor_key,
                        value,
                    ),
                ));
            } else if outcome.rejections.is_empty() {
                self.storage_write(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key,
                    value,
                )
                .await?;
            } else if !self
                .commit_cursor_evidence(
                    &outcome.rejections,
                    (
                        DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                        cursor_key,
                        value,
                    ),
                )
                .await?
            {
                // Fail closed: the events redeliver once evidence fits again.
                continue;
            }
            let retry_topics = {
                remove_deferred_topic(&mut work.deferred_topics, topic_id);
                outcome
                    .satisfied
                    .into_iter()
                    .filter_map(|dependency| work.deferred_topics.remove(&dependency))
                    .flatten()
                    .collect::<Vec<_>>()
            };
            for retry_topic in retry_topics {
                if work.queued_topics.insert(retry_topic) {
                    work.queue.push_back(retry_topic);
                }
            }
        }
        let persisted_deferred_topics = postcard::to_allocvec(&work.deferred_topics)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.storage_write(
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
            deferred_topics_key(),
            persisted_deferred_topics.clone().into(),
        )
        .await?;
        self.apply_create_batch(
            pending_metadata_creates,
            deferred_cursor_writes,
            deferred_rejections,
            &mut work.deferred_topics,
            &mut applied_targets,
            &mut metadata_create_events,
        )
        .await?;
        let updated_deferred_topics = postcard::to_allocvec(&work.deferred_topics)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if updated_deferred_topics != persisted_deferred_topics {
            self.storage_write(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                deferred_topics_key(),
                updated_deferred_topics.into(),
            )
            .await?;
        }
        Ok(DocumentSyncReconcileResult {
            targets: applied_targets,
            metadata_create_events,
            metadata_graph_tombstones,
        })
    }

    async fn pending_reconcile_work(
        &self,
        topic_ids: impl IntoIterator<Item = ::irokle::TopicId>,
    ) -> Result<ReconcileWork> {
        let deferred_topics: BTreeMap<DocumentSyncDependency, BTreeSet<::irokle::TopicId>> = self
            .storage_read(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                deferred_topics_key(),
            )
            .await?
            .map(|bytes| postcard::from_bytes(&bytes))
            .transpose()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .unwrap_or_default();
        let mut work = ReconcileWork {
            deferred_topics,
            queued_topics: BTreeSet::new(),
            queue: VecDeque::new(),
        };
        for topic_id in topic_ids {
            if work.queued_topics.insert(topic_id) {
                work.queue.push_back(topic_id);
            }
        }
        let mut satisfied = Vec::new();
        for dependency in work.deferred_topics.keys().copied().collect::<Vec<_>>() {
            if dependency_available(&self.storage, dependency).await? {
                satisfied.push(dependency);
            }
        }
        for dependency in satisfied {
            if let Some(topics) = work.deferred_topics.remove(&dependency) {
                for topic_id in topics {
                    if work.queued_topics.insert(topic_id) {
                        work.queue.push_back(topic_id);
                    }
                }
            }
        }
        Ok(work)
    }

    async fn load_reconcile_batch(
        &self,
        topic_id: ::irokle::TopicId,
    ) -> Result<Option<ReconcileBatch>> {
        let Some(topic) = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
        else {
            return Ok(None);
        };
        if topic.event_type_id != DocumentSyncEvent::TYPE_ID {
            return Ok(None);
        }
        // The cursor self-heals here: a lost or failed eviction callback cannot
        // make a rebuilt chain's early ops look applied; each position is rechecked.
        let genesis = topic.genesis;
        let cursor_key = topic_cursor_key(topic_id);
        let cursor = applied_cursor_clock(
            self.node.storage(),
            topic_id,
            genesis,
            self.storage_read(
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                cursor_key.clone(),
            )
            .await?,
        )?;
        let batch = self.document_event_batch(topic_id, &cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)?;
        if batch.cursor == cursor {
            return Ok(None);
        }
        Ok(Some(ReconcileBatch {
            genesis,
            cursor_key,
            cursor: batch.cursor,
            rejections: batch.rejections,
            events: batch.events,
        }))
    }

    async fn apply_reconcile_batch(
        &self,
        topic_id: ::irokle::TopicId,
        batch: ReconcileBatch,
    ) -> Result<BatchOutcome> {
        let mut state = BatchState::new(batch.rejections);
        for (event, actor_id, actor_seq) in batch.events {
            let identity = SyncQuarantineIdentity {
                topic: topic_id,
                actor: actor_id,
                actor_seq,
            };
            // Any event outside the run must observe the run's state, so
            // the buffer flushes before anything else applies.
            if !is_config_candidate(&event) {
                state.flush_config(&self.storage).await?;
            }
            if self
                .shard_publishers
                .read()
                .get(&topic_id)
                .is_some_and(|policy| !policy.allows(&actor_id, actor_seq))
            {
                warn!(
                    %topic_id,
                    %actor_id,
                    "Rejecting shard event from a publisher outside the current holder set"
                );
                state.add_rejection(SyncRejection::new(
                    identity,
                    event,
                    "shard publisher is outside the current holder set",
                ));
                continue;
            }
            let target_topic_id = event
                .target()
                .sync_topic_id(self.realm_id, &event.placement());
            if target_topic_id != topic_id {
                warn!(
                    %topic_id,
                    %target_topic_id,
                    "Skipping document sync event whose target does not match its topic"
                );
                state.add_rejection(SyncRejection::new(
                    identity,
                    event,
                    "event target does not match its topic",
                ));
                continue;
            }
            apply_batch_event(self, topic_id, actor_id, identity, event, &mut state).await?;
        }
        state.flush_config(&self.storage).await?;
        retry_admin(self, topic_id, &mut state).await?;
        Ok(state.finish())
    }

    async fn apply_create_batch(
        &self,
        pending: Vec<PendingMetadataCreateApply>,
        cursor_writes: Vec<(::irokle::TopicId, (String, ByteView, Value))>,
        mut rejections: Vec<SyncRejection>,
        deferred_topics: &mut BTreeMap<DocumentSyncDependency, BTreeSet<::irokle::TopicId>>,
        applied_targets: &mut Vec<DocumentSyncTarget>,
        metadata_create_events: &mut Vec<MetadataCreateEventRecord>,
    ) -> Result<()> {
        if pending.is_empty() && cursor_writes.is_empty() && rejections.is_empty() {
            return Ok(());
        }
        let mut candidates = Vec::with_capacity(pending.len());
        for apply in pending {
            if let Err(error) = validate_metadata_event(&apply.record) {
                warn!(
                    topic_id = %apply.identity.topic,
                    document_id = %apply.record.record.document_id,
                    %error,
                    "Rejecting replicated metadata event with inconsistent identity"
                );
                rejections.push(SyncRejection::new(
                    apply.identity,
                    apply.event,
                    format!("replicated metadata event has an inconsistent identity: {error}"),
                ));
                continue;
            }
            let mut entries = Vec::new();
            if let Some(revision) = &apply.lifecycle_revision {
                entries.push(
                    sync_revision_entry(&apply.target, revision)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                );
                if let Some(manifest) = shard_manifest_entry(&apply.target, revision)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?
                {
                    entries.push(manifest);
                }
            }
            let mut event_entries = create_projection_entries(&apply.record)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if let Some((_, _, value)) = event_entries.first_mut() {
                *value = ByteView::from(apply.bytes.clone());
            }
            entries.extend(event_entries);
            if event_is_create(&apply.record) {
                entries.push(
                    create_acceptance_entry(&apply.record)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                );
            }
            candidates.push((apply, entries));
        }

        let txn_id = start_storage_transaction(&self.storage).await?;
        let mut writes = Vec::with_capacity(candidates.len() * 3 + cursor_writes.len());
        let mut accepted = Vec::with_capacity(candidates.len());
        let mut accepted_candidates = Vec::with_capacity(candidates.len());
        let mut create_acceptances: BTreeMap<Ulid, MetadataCreateEventRecord> = BTreeMap::new();
        let mut deferred_cursor_topics = BTreeSet::new();
        for (apply, entries) in candidates {
            let fenced = match create_fence_txn(&self.storage, &apply.record, txn_id).await {
                Ok(fenced) => fenced,
                Err(error) => {
                    let _ = self
                        .storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
            if fenced {
                continue;
            }
            if let Some(revision) = &apply.lifecycle_revision {
                let stale =
                    match lifecycle_stale_txn(&self.storage, &apply.target, revision, txn_id).await
                    {
                        Ok(stale) => stale,
                        Err(error) => {
                            let _ = self
                                .storage
                                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                                .await;
                            return Err(error);
                        }
                    };
                if stale {
                    continue;
                }
            }
            match transaction_placement_fence(&self.storage, &apply.record.record, txn_id).await {
                Ok(MetadataPlacementOutcome::Accepted(MetadataPlacementFence)) => {}
                Ok(MetadataPlacementOutcome::Deferred(dependency)) => {
                    warn!(
                        topic_id = %apply.identity.topic,
                        realm_id = %apply.record.record.realm_id,
                        document_id = %apply.record.record.document_id,
                        strategy_id = %apply.record.record.placement.strategy_id,
                        "Deferring replicated metadata create until its placement strategy is available"
                    );
                    deferred_cursor_topics.insert(apply.identity.topic);
                    if matches!(
                        register_deferred_topic(deferred_topics, dependency, apply.identity.topic),
                        DeferredTopicRegistrationOutcome::CapacityExceeded
                    ) {
                        warn!(
                            topic_id = %apply.identity.topic,
                            ?dependency,
                            "Dropping metadata placement dependency because the deferred-topic registry is full"
                        );
                    }
                    continue;
                }
                Ok(MetadataPlacementOutcome::Rejected) => {
                    warn!(
                        topic_id = %apply.identity.topic,
                        realm_id = %apply.record.record.realm_id,
                        document_id = %apply.record.record.document_id,
                        strategy_id = %apply.record.record.placement.strategy_id,
                        "Rejecting replicated metadata create with mismatched placement configuration"
                    );
                    rejections.push(SyncRejection::new(
                        apply.identity,
                        apply.event,
                        "replicated metadata create has a mismatched placement configuration",
                    ));
                    continue;
                }
                Err(error) => {
                    let _ = self
                        .storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
            let document_id = apply.record.record.document_id;
            let accepted_create = if let Some(event) = create_acceptances.get(&document_id) {
                Some(event.clone())
            } else {
                let value = match transaction_read(
                    &self.storage,
                    METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                    create_acceptance_key(document_id),
                    Some(txn_id),
                )
                .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = self
                            .storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(error);
                    }
                };
                let event = match value
                    .as_deref()
                    .map(postcard::from_bytes::<MetadataCreateEventRecord>)
                    .transpose()
                {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = self
                            .storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(NetError::Bootstrap(error.to_string()));
                    }
                };
                if let Some(event) = &event {
                    if !event_is_create(event) {
                        let _ = self
                            .storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(NetError::Bootstrap(
                            "metadata create acceptance contains a non-create event".to_string(),
                        ));
                    }
                    if let Err(error) = validate_metadata_event(event) {
                        let _ = self
                            .storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(error);
                    }
                    create_acceptances.insert(document_id, event.clone());
                }
                event
            };
            if event_is_create(&apply.record) {
                if accepted_create
                    .as_ref()
                    .is_some_and(|accepted| !same_create_event(accepted, &apply.record))
                {
                    warn!(
                        topic_id = %apply.identity.topic,
                        %document_id,
                        "Rejecting divergent replicated metadata create"
                    );
                    rejections.push(SyncRejection::new(
                        apply.identity,
                        apply.event,
                        "divergent replicated metadata create",
                    ));
                    continue;
                }
                create_acceptances
                    .entry(document_id)
                    .or_insert_with(|| apply.record.clone());
            } else if accepted_create.is_none() {
                warn!(
                    topic_id = %apply.identity.topic,
                    %document_id,
                    "Deferring replicated metadata update until its create is accepted"
                );
                deferred_cursor_topics.insert(apply.identity.topic);
                continue;
            } else if accepted_create.as_ref().is_some_and(|accepted| {
                !registry_identity_matches(&accepted.record, &apply.record.record)
            }) {
                warn!(
                    topic_id = %apply.identity.topic,
                    %document_id,
                    "Rejecting replicated metadata update with mismatched accepted create"
                );
                rejections.push(SyncRejection::new(
                    apply.identity,
                    apply.event,
                    "replicated metadata update has a mismatched accepted create",
                ));
                continue;
            }
            accepted_candidates.push((apply, entries));
        }
        for (apply, entries) in accepted_candidates {
            writes.extend(entries);
            accepted.push(apply);
        }
        match self.quarantine_entries(&rejections, txn_id).await {
            Ok(Some(entries)) => writes.extend(entries),
            Ok(None) => {
                // Fail closed: no topic may advance past evidence that does not fit.
                deferred_cursor_topics.extend(rejections.iter().map(|reject| reject.topic_id()));
            }
            Err(error) => {
                self.abort_transaction(txn_id).await;
                return Err(error);
            }
        }
        writes.extend(cursor_writes.into_iter().filter_map(|(topic_id, write)| {
            (!deferred_cursor_topics.contains(&topic_id)).then_some(write)
        }));
        if let Err(error) = replace_batch_in(&self.storage, txn_id, Vec::new(), writes).await {
            let _ = self
                .storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(error);
        }
        for apply in accepted {
            applied_targets.push(apply.target);
            metadata_create_events.push(apply.record);
        }
        Ok(())
    }
}
