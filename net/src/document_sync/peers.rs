use super::*;

impl DocumentSyncService {
    /// Shares the realm-config kind table so topic membership and sync fan-out
    /// apply the same node-kind boundary as the accept loop.
    pub(crate) fn set_peer_kinds(&mut self, peer_kinds: PeerKinds) {
        self.peer_kinds = peer_kinds;
    }

    /// Whether a peer may hold sync state: a user device never joins a topic and
    /// is never dialed for one. A peer whose kind is unknown stays eligible; the
    /// table is empty until realm config materializes.
    fn peer_is_eligible(&self, peer: &PeerId) -> bool {
        let Ok(node_id) = NodeId::from_bytes(peer.as_bytes()) else {
            return true;
        };
        self.peer_kinds
            .read()
            .get(&node_id)
            .is_none_or(RealmNodeKind::is_sync_eligible)
    }

    /// Dial-side mirror of the accept matrix: drops peers whose configured kind
    /// carries no sync responsibility.
    pub(in crate::document_sync) fn eligible_peers(
        &self,
        peers: impl IntoIterator<Item = PeerId>,
        topic_id: Option<::irokle::TopicId>,
    ) -> BTreeSet<PeerId> {
        peers
            .into_iter()
            .filter(|peer| {
                let eligible = self.peer_is_eligible(peer);
                if !eligible {
                    debug!(node_id = %peer, ?topic_id, "Skipping a document sync peer that is not sync eligible");
                }
                eligible
            })
            .collect()
    }

    pub fn allow_peer_node(&self, node_id: NodeId) -> Result<()> {
        let peer_id = node_to_peer(&node_id);
        if peer_id == self.node.peer_id() {
            return Ok(());
        }
        self.node
            .add_peer_to_whitelist(peer_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.flush_database()
    }

    pub fn add_peer_candidate(&self, node_id: NodeId) -> Result<()> {
        let peer_id = node_to_peer(&node_id);
        if peer_id == self.node.peer_id() {
            return Ok(());
        }
        self.allow_peer_node(node_id)?;
        self.default_peers.write().insert(peer_id);
        Ok(())
    }

    pub fn add_peer_candidates(&self, nodes: impl IntoIterator<Item = NodeId>) -> Result<()> {
        for node_id in nodes {
            self.add_peer_candidate(node_id)?;
        }
        Ok(())
    }

    pub fn refresh_peer_candidates(&self, nodes: impl IntoIterator<Item = NodeId>) -> Result<()> {
        let mut peers = BTreeSet::new();
        for node_id in nodes {
            let peer_id = node_to_peer(&node_id);
            if peer_id == self.node.peer_id() {
                continue;
            }
            peers.insert(peer_id);
        }
        self.node
            .add_peers_to_whitelist(peers.iter().copied())
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        // Realm config is now authoritative: replace the fan-out/admission set
        // and stop honoring the bootstrap `configured_peers`.
        *self.default_peers.write() = peers;
        self.realm_config_materialized
            .store(true, Ordering::Release);
        self.flush_database()?;
        Ok(())
    }
}
