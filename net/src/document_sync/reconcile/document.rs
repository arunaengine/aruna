//! Reads bounded event batches from a sync topic, applies them and advances the cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use ::irokle::{Event as _, Storage as _};
use aruna_core::document::{
    BatchEvent, DocumentEvent, DocumentReconcileResult, DocumentTarget, ReconcileBatch,
};
use aruna_core::document_apply::create::{CreateBatchPlan, PendingCreateApply};
use aruna_core::document_apply::quarantine::CursorEvidencePlan;
use aruna_core::keyspaces::APPLIED_OPS_KEYSPACE;
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::structs::SyncQuarantineIdentity;
use aruna_core::sync_checks::{EventScreen, screen_event};
use aruna_core::types::Value;
use byteview::ByteView;
use tracing::warn;

use crate::document_sync::plan_run::run_plan;
use crate::document_sync::{
    DeferredRegistrationOutcome, DocumentSyncDependency, DocumentSyncService, FRAME_LEN_LIMIT,
    SyncRejection,
};
use crate::error::{NetError, Result};

use super::batch::{BatchOutcome, BatchState, apply_batch_event, is_config_candidate, retry_admin};
use super::cursor::{
    applied_cursor_clock, applied_cursor_value, deferred_topics_key, topic_cursor_key,
};
use super::{dependency_available, register_deferred_topic, remove_deferred_topic};

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
    ) -> Result<DocumentReconcileResult> {
        let _reconcile_guard = self.reconcile_lock.lock().await;
        let mut work = self.pending_reconcile_work(topic_ids).await?;
        let mut applied_targets = Vec::new();
        let mut metadata_create_events = Vec::new();
        let mut metadata_graph_tombstones = Vec::new();
        let mut pending_metadata_creates: Vec<PendingCreateApply> = Vec::new();
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
            let cursor_key = batch.cursor_key.clone();
            let value = batch.cursor_value.clone();
            let start = batch.start.clone();
            let mut outcome = self.apply_reconcile_batch(topic_id, batch).await?;
            if outcome.overflowed {
                self.record_overflow(topic_id, genesis, &start).await?;
            }
            applied_targets.append(&mut outcome.applied_targets);
            metadata_graph_tombstones.append(&mut outcome.graph_tombstones);
            pending_metadata_creates.append(&mut outcome.pending_creates);
            if !outcome.cross_topic.is_empty() {
                remove_deferred_topic(&mut work.deferred_topics, topic_id);
                for dependency in outcome.cross_topic {
                    if matches!(
                        register_deferred_topic(&mut work.deferred_topics, dependency, topic_id),
                        DeferredRegistrationOutcome::CapacityExceeded
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
            if outcome.deferred_creates {
                deferred_rejections.append(&mut outcome.rejections);
                deferred_cursor_writes.push((
                    topic_id,
                    (APPLIED_OPS_KEYSPACE.to_string(), cursor_key, value),
                ));
            } else if outcome.rejections.is_empty() {
                self.storage_write(APPLIED_OPS_KEYSPACE.to_string(), cursor_key, value)
                    .await?;
            } else if !run_plan(
                &self.storage,
                CursorEvidencePlan {
                    rejections: outcome.rejections,
                    cursor: (APPLIED_OPS_KEYSPACE.to_string(), cursor_key, value),
                },
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
            APPLIED_OPS_KEYSPACE.to_string(),
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
                APPLIED_OPS_KEYSPACE.to_string(),
                deferred_topics_key(),
                updated_deferred_topics.into(),
            )
            .await?;
        }
        // Every pass, with or without new events, gives parked snapshots another chance:
        // a local membership change never passes through the replicated apply path.
        super::retry_parked(&self.storage, self.realm_id).await?;
        self.replay_overflow().await?;
        Ok(DocumentReconcileResult {
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
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), deferred_topics_key())
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
        if topic.event_type_id != DocumentEvent::TYPE_ID {
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
            self.storage_read(APPLIED_OPS_KEYSPACE.to_string(), cursor_key.clone())
                .await?,
        )?;
        let batch = self.document_event_batch(topic_id, &cursor, FRAME_LEN_LIMIT)?;
        if batch.cursor == cursor {
            return Ok(None);
        }
        let cursor_value =
            applied_cursor_value(self.node.storage(), topic_id, genesis, &batch.cursor)?;
        let policies = self.shard_publishers.read();
        let policy = policies.get(&topic_id);
        let events = (batch.events.into_iter())
            .map(|(event, actor, actor_seq)| BatchEvent {
                publisher_allowed: policy.is_none_or(|policy| policy.allows(&actor, actor_seq)),
                event,
                actor,
                actor_seq,
            })
            .collect();
        drop(policies);
        Ok(Some(ReconcileBatch {
            genesis,
            start: cursor,
            cursor_key,
            cursor_value,
            rejections: batch.rejections,
            events,
        }))
    }

    async fn apply_reconcile_batch(
        &self,
        topic_id: ::irokle::TopicId,
        batch: ReconcileBatch,
    ) -> Result<BatchOutcome> {
        let mut state = BatchState::new(batch.rejections);
        for BatchEvent {
            event,
            actor: actor_id,
            actor_seq,
            publisher_allowed,
        } in batch.events
        {
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
            let target_topic_id = event
                .target()
                .sync_topic_id(self.realm_id, &event.placement());
            match screen_event(topic_id, target_topic_id, event.target(), publisher_allowed) {
                EventScreen::Apply => {}
                EventScreen::Drop => continue,
                EventScreen::Reject(reason) => {
                    warn!(%topic_id, %actor_id, %target_topic_id, reason, "Rejecting a document sync event");
                    state.add_rejection(SyncRejection::new(identity, event, reason));
                    continue;
                }
            }
            apply_batch_event(self, topic_id, actor_id, identity, event, &mut state).await?;
        }
        state.flush_config(&self.storage).await?;
        retry_admin(self, topic_id, &mut state).await?;
        Ok(state.finish())
    }

    async fn apply_create_batch(
        &self,
        pending: Vec<PendingCreateApply>,
        cursor_writes: Vec<(::irokle::TopicId, (String, ByteView, Value))>,
        rejections: Vec<SyncRejection>,
        deferred_topics: &mut BTreeMap<DocumentSyncDependency, BTreeSet<::irokle::TopicId>>,
        applied_targets: &mut Vec<DocumentTarget>,
        metadata_create_events: &mut Vec<MetadataEventRecord>,
    ) -> Result<()> {
        let plan = CreateBatchPlan::new(pending, cursor_writes, rejections)?;
        let outcome = run_plan(&self.storage, plan).await?;
        for note in outcome.notes {
            warn!(
                topic_id = %note.topic,
                document_id = %note.document_id,
                "{}",
                note.message
            );
        }
        for (dependency, topic_id) in outcome.deferred {
            if matches!(
                register_deferred_topic(deferred_topics, dependency, topic_id),
                DeferredRegistrationOutcome::CapacityExceeded
            ) {
                warn!(
                    %topic_id,
                    ?dependency,
                    "Dropping metadata placement dependency because the deferred-topic registry is full"
                );
            }
        }
        for (target, record) in outcome.accepted {
            applied_targets.push(target);
            metadata_create_events.push(record);
        }
        Ok(())
    }
}
