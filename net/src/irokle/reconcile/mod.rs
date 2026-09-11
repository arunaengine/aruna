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
