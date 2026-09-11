use super::*;

mod admin;
mod apply;
mod cursor;
mod materialize;
mod registry;
mod validate;

pub(in crate::document_sync) use self::admin::*;
#[cfg(test)]
pub(in crate::document_sync) use self::apply::*;
pub(in crate::document_sync) use self::cursor::*;
pub(in crate::document_sync) use self::materialize::*;
pub(in crate::document_sync) use self::registry::*;
pub(in crate::document_sync) use self::validate::*;

impl DocumentSyncService {
    /// Reconciles shard-only topic membership to the authoritative current
    /// holder set. The publisher cutover is installed before controls are
    /// emitted, so later events from a removed holder cannot apply while
    /// pre-cutover replacement history remains usable. Shared topics and the
    /// default peer set are intentionally untouched.
    ///
    /// A `history_cutoff` is only frozen for topics in `verified_topics`: until a
    /// node has durably verified a shard, its local clock is not a trustworthy
    /// cutover boundary, so former-holder history is left admissible ([BR-030]).
    ///
    /// `retained` peers are draining former-holders that must keep publishing onto
    /// the shard until they have flushed (flush-then-leave): they stay members and
    /// accepted publishers even though they are not canonical holders, so a
    /// removal never cuts off an in-flight flush. They rejoin neither the missing
    /// nor the local-holder computation — only a canonical holder may mint or top
    /// up membership — so a true non-holder is never added (DECISIONS D11).
    pub async fn reconcile_shard_membership(
        &self,
        topics: &[irokle_crate::TopicId],
        members: Vec<NodeId>,
        publishers: Vec<NodeId>,
        retained: &BTreeSet<NodeId>,
        verified_topics: &BTreeSet<irokle_crate::TopicId>,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }

        let _reconcile_guard = self.reconcile_lock.lock().await;
        let local_peer = self.node.peer_id();
        // Membership (delivery) and publish authority are separate sets: an
        // unactivated transition target is a member without authority. The
        // adapter installs exactly the sets it is handed; policy stays in
        // operations.
        let member_peers: BTreeSet<PeerId> = members
            .into_iter()
            .map(|node_id| node_id_to_peer_id(&node_id))
            .chain(retained.iter().map(node_id_to_peer_id))
            .collect();
        if !member_peers.contains(&local_peer) {
            return Err(NetError::Bootstrap(
                "local node is not a shard member".to_string(),
            ));
        }
        let publisher_peers: BTreeSet<PeerId> = publishers
            .into_iter()
            .map(|node_id| node_id_to_peer_id(&node_id))
            .chain(retained.iter().map(node_id_to_peer_id))
            .collect();

        let mut seen_topics = BTreeSet::new();
        let topics: Vec<irokle_crate::TopicId> = topics
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
                .map(|peer| irokle_crate::actor_id_for(topic_id, peer))
                .collect();
            match state {
                Some(state) => {
                    if state.event_type_id != DocumentSyncEvent::TYPE_ID {
                        return Err(NetError::Bootstrap(format!(
                            "Document sync topic {topic_id} has event type {}, expected {}",
                            state.event_type_id,
                            DocumentSyncEvent::TYPE_ID
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

        let oplog = Oplog::with_storage(self.node.storage().clone());
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

            let actor_id = irokle_crate::actor_id_for(topic_id, local_peer);
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
    pub async fn reconcile_document_sync_topics(
        &self,
        topic_ids: Vec<irokle_crate::TopicId>,
    ) -> Result<DocumentSyncReconcileResult> {
        self.reconcile_document_topics(topic_ids).await
    }

    pub(in crate::document_sync) async fn reconcile_documents(
        &self,
    ) -> Result<DocumentSyncReconcileResult> {
        let topics = self.document_topic_ids()?;
        self.reconcile_document_topics(topics).await
    }

    pub(in crate::document_sync) fn document_topic_ids(
        &self,
    ) -> Result<Vec<irokle_crate::TopicId>> {
        let topics = self
            .node
            .list_topics()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        Ok(topics
            .into_iter()
            .filter(|topic| topic.event_type_id == DocumentSyncEvent::TYPE_ID)
            .map(|topic| topic.topic_id)
            .collect())
    }
}

impl DocumentSyncService {
    /// Returns one bounded causal batch above a component-wise cursor.
    pub(in crate::document_sync) fn document_event_batch(
        &self,
        topic_id: irokle_crate::TopicId,
        cursor: &irokle_crate::ActorClock,
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
            irokle_crate::ActorId,
            BTreeMap<u64, BTreeSet<irokle_crate::ActorId>>,
        > = BTreeMap::new();
        let mut events = Vec::with_capacity(DOCUMENT_SYNC_REPLAY_BATCH_LIMIT);
        let mut rejections = Vec::new();
        let mut processed = 0usize;
        let mut batch_bytes = 0usize;
        while processed < DOCUMENT_SYNC_REPLAY_BATCH_LIMIT {
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
            if op_bytes > DOCUMENT_SYNC_FRAME_LEN_LIMIT {
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
                // An undecodable payload is permanent: no redelivery can make
                // the same signed bytes valid, so it becomes raw evidence and
                // the cursor advances past it instead of replaying forever.
                TopicPayload::Event(envelope) => {
                    match envelope.decode_event::<DocumentSyncEvent>() {
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
                    }
                }
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
