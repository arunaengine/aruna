use super::*;

impl DocumentSyncService {
    pub fn ensure_sync_topics(
        &self,
        topics: &[::irokle::TopicId],
        peers: Vec<NodeId>,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }

        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;

        let mut seen_topics = BTreeSet::new();
        for topic_id in topics.iter().copied() {
            if seen_topics.insert(topic_id) {
                self.ensure_topic(topic_id, &sync_peers, true)?;
            }
        }

        self.flush_database()
    }

    /// Ensures topics this node holds alone: it mints what is missing and no
    /// peer joins their membership, so nothing about them is ever exchanged.
    pub fn ensure_local_topics(&self, topics: &[::irokle::TopicId]) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        let mut seen_topics = BTreeSet::new();
        for topic_id in topics.iter().copied() {
            if seen_topics.insert(topic_id) {
                self.ensure_topic(topic_id, &BTreeSet::new(), true)?;
            }
        }
        self.flush_database()
    }

    /// Whether the topic's genesis is known locally. The outbox drain uses this
    /// to defer shard-topic records until the rank-0 holder's genesis arrives.
    pub fn topic_exists(&self, topic_id: ::irokle::TopicId) -> Result<bool> {
        self.has_topic(topic_id)
    }

    pub(in crate::document_sync) fn has_topic(&self, topic_id: ::irokle::TopicId) -> Result<bool> {
        Ok(self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .is_some())
    }

    /// Closes a topic before a departing holder scans its journal and outbox.
    /// The result reports whether a journal entry still exists afterwards.
    pub fn close_topic(&self, topic_id: ::irokle::TopicId) -> Result<bool> {
        self.node
            .seal_topic(topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.flush_database()?;
        Ok(self
            .node
            .pending_evictions()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .into_iter()
            .any(|eviction| eviction.topic_id == topic_id))
    }

    pub fn reopen_topic(&self, topic_id: ::irokle::TopicId) -> Result<()> {
        let removed = self
            .node
            .unseal_topic(topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if removed {
            self.flush_database()?;
        }
        Ok(())
    }
}
