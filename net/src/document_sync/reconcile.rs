//! Owns document reconcile: shard topic membership plus the apply, cursor and check parts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ::irokle::Event as _;
use aruna_core::document::{BatchEvent, OverflowPage, ReconcileBatch};

use super::*;

impl DocumentSyncService {
    /// Reconciles shard topic membership to the current holder set: the publisher
    /// cutover installs before controls. `history_cutoff` freezes only for
    /// `verified_topics`; retained draining peers stay members until flushed.
    pub async fn reconcile_shard_membership(
        &self,
        topics: &[::irokle::TopicId],
        members: Vec<NodeId>,
        publishers: Vec<NodeId>,
        retained: &BTreeSet<NodeId>,
        verified_topics: &BTreeSet<::irokle::TopicId>,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }

        let reconcile_guard = Arc::clone(&self.reconcile_lock).lock_owned().await;
        let local_peer = self.node.peer_id();
        // Membership and publish authority are separate sets; the adapter
        // installs exactly the sets it is handed and policy stays in operations.
        let member_peers: BTreeSet<PeerId> = members
            .into_iter()
            .map(|node_id| node_to_peer(&node_id))
            .chain(retained.iter().map(node_to_peer))
            .collect();
        if !member_peers.contains(&local_peer) {
            return Err(NetError::Bootstrap(
                "local node is not a shard member".to_string(),
            ));
        }
        let publisher_peers: BTreeSet<PeerId> = publishers
            .into_iter()
            .map(|node_id| node_to_peer(&node_id))
            .chain(retained.iter().map(node_to_peer))
            .collect();

        let mut seen_topics = BTreeSet::new();
        let topics: Vec<::irokle::TopicId> = topics
            .iter()
            .copied()
            .filter(|topic_id| seen_topics.insert(*topic_id))
            .collect();
        let mut states = Vec::with_capacity(topics.len());
        let mut policies = Vec::with_capacity(topics.len());
        let mut missing_topic = None;
        for topic_id in topics {
            let state = self
                .node
                .storage()
                .topic_state(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let current = publisher_peers
                .iter()
                .copied()
                .map(|peer| ::irokle::actor_id_for(topic_id, peer))
                .collect();
            match state {
                Some(state) => {
                    if state.event_type_id != DocumentEvent::TYPE_ID {
                        return Err(NetError::Bootstrap(format!(
                            "Document sync topic {topic_id} has event type {}, expected {}",
                            state.event_type_id,
                            DocumentEvent::TYPE_ID
                        )));
                    }
                    let history_cutoff = if verified_topics.contains(&topic_id) {
                        Some(
                            self.node
                                .storage()
                                .actor_clock(&topic_id)
                                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                        )
                    } else {
                        None
                    };
                    policies.push((
                        topic_id,
                        ShardPublisherPolicy {
                            current,
                            history_cutoff,
                        },
                    ));
                    states.push((topic_id, state));
                }
                None => {
                    missing_topic.get_or_insert(topic_id);
                    policies.push((
                        topic_id,
                        ShardPublisherPolicy {
                            current,
                            history_cutoff: None,
                        },
                    ));
                }
            }
        }
        self.shard_publishers.write().extend(policies);
        let sync_peers: BTreeSet<PeerId> = member_peers
            .iter()
            .copied()
            .filter(|peer| *peer != local_peer)
            .collect();
        self.allow_sync_peers(&sync_peers)?;
        if let Some(topic_id) = missing_topic {
            return Err(NetError::Bootstrap(format!(
                "document sync topic {topic_id} is missing"
            )));
        }

        // Control writes and the flush block, so they run off the async runtime. The task
        // owns the guard: a cancelled caller cannot let a newer reconcile in before it ends.
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let _reconcile_guard = reconcile_guard;
            service.write_shard_controls(states, &member_peers, local_peer)
        })
        .await
        .map_err(|error| NetError::Bootstrap(error.to_string()))?
    }

    fn write_shard_controls(
        &self,
        states: Vec<(::irokle::TopicId, ::irokle::storage::TopicState)>,
        member_peers: &BTreeSet<PeerId>,
        local_peer: PeerId,
    ) -> Result<()> {
        let oplog = self.node.oplog();
        for (topic_id, state) in states {
            let missing_peers = member_peers
                .iter()
                .copied()
                .filter(|peer| *peer != local_peer && !state.members.contains(peer))
                .collect::<Vec<_>>();
            let stale_peers = state
                .members
                .iter()
                .copied()
                .filter(|peer| !member_peers.contains(peer))
                .collect::<Vec<_>>();
            if missing_peers.is_empty() && stale_peers.is_empty() {
                continue;
            }

            let actor_id = ::irokle::actor_id_for(topic_id, local_peer);
            for peer in missing_peers {
                oplog
                    .create_control_op(
                        topic_id,
                        actor_id,
                        TopicControl::AddPeer { peer },
                        self.node.signer(),
                    )
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            }
            for peer in stale_peers {
                oplog
                    .create_control_op(
                        topic_id,
                        actor_id,
                        TopicControl::RemovePeer { peer },
                        self.node.signer(),
                    )
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            }
            self.net.schedule_topic_recheck(topic_id)?;
        }

        self.flush_database()
    }
}

impl DocumentSyncService {
    /// Answers a reconcile operation with the topic's next batch.
    pub fn reconcile_batch_event(
        &self,
        topic_id: ::irokle::TopicId,
        stored_cursor: Option<Value>,
    ) -> DocumentNetEvent {
        match self.reconcile_batch(topic_id, stored_cursor) {
            Ok(batch) => DocumentNetEvent::ReconcileBatch(batch.map(Box::new)),
            Err(error) => DocumentNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    /// Answers an overflow replay with its next page.
    pub fn overflow_page_event(
        &self,
        topic_id: ::irokle::TopicId,
        start: Value,
        applied: Option<Value>,
    ) -> DocumentNetEvent {
        match self.overflow_page(topic_id, start, applied) {
            Ok(page) => DocumentNetEvent::OverflowPage(Box::new(page)),
            Err(error) => DocumentNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    /// The next batch above the stored cursor; the cursor repairs itself here: a lost or failed
    /// eviction callback cannot make a rebuilt chain's early ops look applied.
    fn reconcile_batch(
        &self,
        topic_id: ::irokle::TopicId,
        stored_cursor: Option<Value>,
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
        let storage = self.node.storage();
        let genesis = topic.genesis;
        let cursor = applied_cursor_clock(storage, topic_id, genesis, stored_cursor)?;
        let batch = self.document_event_batch(topic_id, &cursor, FRAME_LEN_LIMIT)?;
        if batch.cursor == cursor {
            return Ok(None);
        }
        let start_value = applied_cursor_value(storage, topic_id, genesis, &cursor)?;
        let cursor_value = applied_cursor_value(storage, topic_id, genesis, &batch.cursor)?;
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
            start_value,
            cursor_key: topic_cursor_key(topic_id),
            cursor_value,
            rejections: batch.rejections,
            events,
        }))
    }

    /// The events after the encoded position `start`, until the topic's applied cursor.
    fn overflow_page(
        &self,
        topic_id: ::irokle::TopicId,
        start: Value,
        applied: Option<Value>,
    ) -> Result<OverflowPage> {
        let storage = self.node.storage();
        let Some(topic) = storage
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
        else {
            return Ok(OverflowPage::TopicGone);
        };
        let genesis = topic.genesis;
        let clock = applied_cursor_clock(storage, topic_id, genesis, Some(start))?;
        let main = applied_cursor_clock(storage, topic_id, genesis, applied)?;
        if clock.dominates(&main) {
            return Ok(OverflowPage::Finished);
        }
        let batch = self.document_event_batch(topic_id, &clock, FRAME_LEN_LIMIT)?;
        if batch.cursor == clock {
            return Ok(OverflowPage::Finished);
        }
        Ok(OverflowPage::Page {
            at: applied_cursor_value(storage, topic_id, genesis, &clock)?,
            next: applied_cursor_value(storage, topic_id, genesis, &batch.cursor)?,
            events: (batch.events.into_iter())
                .map(|(event, actor, _)| (event, actor))
                .collect(),
        })
    }
}

impl DocumentSyncService {
    /// Returns one bounded causal batch above a component-wise cursor.
    pub(in crate::document_sync) fn document_event_batch(
        &self,
        topic_id: ::irokle::TopicId,
        cursor: &::irokle::ActorClock,
        byte_limit: usize,
    ) -> Result<DocumentEventBatch> {
        let storage = self.node.storage();
        let topic_clock = storage
            .actor_clock(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let mut working_cursor = cursor.clone();
        let mut queued = BTreeSet::new();
        for (actor_id, actor_tip) in topic_clock.iter() {
            if working_cursor.get(actor_id) < *actor_tip {
                queued.insert(*actor_id);
            }
        }

        // A candidate can wait on a dependency actor's next contiguous op. Wake
        // it only after that actor reaches the required sequence.
        let mut blocked_by: BTreeMap<
            ::irokle::ActorId,
            BTreeMap<u64, BTreeSet<::irokle::ActorId>>,
        > = BTreeMap::new();
        let mut events = Vec::with_capacity(REPLAY_BATCH_LIMIT);
        let mut rejections = Vec::new();
        let mut processed = 0usize;
        let mut batch_bytes = 0usize;
        while processed < REPLAY_BATCH_LIMIT {
            let Some(actor_id) = queued.pop_first() else {
                break;
            };
            let actor_seq = working_cursor
                .get(&actor_id)
                .checked_add(1)
                .ok_or_else(|| {
                    NetError::Bootstrap("document sync actor sequence overflow".into())
                })?;
            if actor_seq > topic_clock.get(&actor_id) {
                continue;
            }
            let op_id = storage
                .actor_index(&topic_id, &actor_id, actor_seq)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| {
                    NetError::Bootstrap(format!(
                        "missing document sync op for actor {actor_id} sequence {actor_seq}"
                    ))
                })?;
            let meta = storage
                .get_meta(&op_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| {
                    NetError::Bootstrap(format!("missing document sync op meta {op_id}"))
                })?;
            if meta.topic_id != topic_id || meta.actor_id != actor_id || meta.actor_seq != actor_seq
            {
                return Err(NetError::Bootstrap(format!(
                    "document sync actor index mismatch for actor {actor_id} sequence {actor_seq}"
                )));
            }

            let mut missing = BTreeMap::new();
            for dependency in &meta.deps {
                let dependency_meta = storage
                    .get_meta(dependency)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?
                    .ok_or_else(|| {
                        NetError::Bootstrap(format!(
                            "missing document sync dependency meta {dependency}"
                        ))
                    })?;
                if dependency_meta.topic_id != topic_id {
                    return Err(NetError::Bootstrap(
                        "document sync dependency belongs to another topic".into(),
                    ));
                }
                if dependency_meta.actor_seq > working_cursor.get(&dependency_meta.actor_id) {
                    missing
                        .entry(dependency_meta.actor_id)
                        .and_modify(|sequence: &mut u64| {
                            *sequence = (*sequence).max(dependency_meta.actor_seq)
                        })
                        .or_insert(dependency_meta.actor_seq);
                }
            }
            if !missing.is_empty() {
                for (dependency_actor, dependency_seq) in missing {
                    blocked_by
                        .entry(dependency_actor)
                        .or_default()
                        .entry(dependency_seq)
                        .or_default()
                        .insert(actor_id);
                }
                continue;
            }

            let op = storage
                .get_op(&op_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| NetError::Bootstrap(format!("missing document sync op {op_id}")))?;
            let op_bytes = postcard::experimental::serialized_size(&op)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if op_bytes > FRAME_LEN_LIMIT {
                return Err(NetError::Bootstrap(
                    "document sync operation exceeds replay frame limit".into(),
                ));
            }
            if processed > 0 && batch_bytes.saturating_add(op_bytes) > byte_limit {
                break;
            }
            let actor_id = op.signed.body.actor_id;
            let actor_seq = op.signed.body.actor_seq;
            match op.signed.body.payload {
                // An undecodable payload is permanent: it becomes raw evidence
                // and the cursor advances instead of replaying forever.
                TopicPayload::Event(envelope) => match envelope.decode_event::<DocumentEvent>() {
                    Ok(event) => events.push((event, actor_id, actor_seq)),
                    Err(error) => rejections.push(SyncRejection::raw(
                        SyncQuarantineIdentity {
                            topic: topic_id,
                            actor: actor_id,
                            actor_seq,
                        },
                        envelope.payload.to_vec(),
                        format!(
                            "undecodable sync payload of type `{}`: {error}",
                            envelope.type_id
                        ),
                    )),
                },
                TopicPayload::Genesis(_) | TopicPayload::Control(_) => {}
            }
            working_cursor.observe(actor_id, actor_seq);
            processed += 1;
            batch_bytes = batch_bytes.saturating_add(op_bytes);

            let mut wake = BTreeSet::new();
            if let Some(waiters) = blocked_by.get_mut(&actor_id) {
                let ready = waiters
                    .range(..=actor_seq)
                    .map(|(sequence, _)| *sequence)
                    .collect::<Vec<_>>();
                for sequence in ready {
                    if let Some(waiters) = waiters.remove(&sequence) {
                        wake.extend(waiters);
                    }
                }
            }
            if blocked_by
                .get(&actor_id)
                .is_some_and(|waiters| waiters.is_empty())
            {
                blocked_by.remove(&actor_id);
            }
            queued.extend(wake);
            if working_cursor.get(&actor_id) < topic_clock.get(&actor_id) {
                queued.insert(actor_id);
            }
        }

        if processed == 0 {
            if !cursor.dominates(&topic_clock) {
                return Err(NetError::Bootstrap(
                    "document sync causal replay made no progress".into(),
                ));
            }
            return Ok(DocumentEventBatch {
                cursor: working_cursor,
                events: Vec::new(),
                rejections,
            });
        }

        Ok(DocumentEventBatch {
            cursor: working_cursor,
            events,
            rejections,
        })
    }
}
