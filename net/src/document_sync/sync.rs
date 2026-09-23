//! Picks sync peers and runs topic sync, fan-out and bootstrap rounds for document sync.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl DocumentSyncService {
    pub async fn sync_with_peers(
        &self,
        topic_id: ::irokle::TopicId,
        peers: Vec<NodeId>,
    ) -> Result<()> {
        let selection = self.sync_peer_selection(&peers, &topic_id)?;
        self.log_peer_selection(topic_id, &selection);
        self.allow_sync_peers(&selection.peers)?;
        let round = selection.round;
        let result = self.sync_topic(topic_id, selection).await;
        self.advance_cursor(topic_id, round)?;
        self.flush_database()?;
        result
    }

    pub fn allow_topic_peers(
        &self,
        topics: &[::irokle::TopicId],
        peers: Vec<NodeId>,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }

        let sync_peers = self.sync_peers(peers);
        if sync_peers.is_empty() {
            return Ok(());
        }
        self.allow_sync_peers(&sync_peers)?;

        let mut seen_topics = BTreeSet::new();
        for topic_id in topics.iter().copied() {
            if !seen_topics.insert(topic_id) {
                continue;
            }

            let state = self
                .node
                .storage()
                .topic_state(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .ok_or_else(|| {
                    NetError::Bootstrap(format!("document sync topic {topic_id} is missing"))
                })?;

            if state.event_type_id != DocumentEvent::TYPE_ID {
                return Err(NetError::Bootstrap(format!(
                    "Document sync topic {topic_id} has event type {}, expected {}",
                    state.event_type_id,
                    DocumentEvent::TYPE_ID
                )));
            }

            let missing_peers = sync_peers
                .iter()
                .copied()
                .filter(|peer| !state.members.contains(peer))
                .collect::<Vec<_>>();
            if missing_peers.is_empty() {
                continue;
            }

            let actor_id = ::irokle::actor_id_for(topic_id, self.node.peer_id());
            let oplog = Oplog::with_storage(self.node.storage().clone());
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
            self.net.schedule_topic_recheck(topic_id)?;
        }

        self.flush_database()
    }

    pub(in crate::document_sync) fn sync_peers(&self, peers: Vec<NodeId>) -> BTreeSet<PeerId> {
        let candidates = if peers.is_empty() {
            self.default_peers.read().clone()
        } else {
            peers
                .into_iter()
                .map(|node_id| node_to_peer(&node_id))
                .collect()
        };
        let mut sync_peers = self.eligible_peers(candidates, None);
        sync_peers.remove(&self.node.peer_id());
        sync_peers
    }

    pub(in crate::document_sync) fn topic_genesis(
        &self,
        topic_id: ::irokle::TopicId,
    ) -> Result<Option<::irokle::OpId>> {
        Ok(self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .map(|state| state.genesis))
    }

    fn next_sync_round(&self, topic_id: ::irokle::TopicId) -> Result<u64> {
        let Some(genesis) = self.topic_genesis(topic_id)? else {
            return Ok(0);
        };
        current_cursor(&self.fanout_cursors, topic_id, genesis)
    }

    pub(in crate::document_sync) fn advance_cursor(
        &self,
        topic_id: ::irokle::TopicId,
        round: u64,
    ) -> Result<()> {
        let Some(genesis) = self.topic_genesis(topic_id)? else {
            return Ok(());
        };
        advance_cursor(&self.fanout_cursors, topic_id, genesis, round)
    }

    pub(in crate::document_sync) fn clear_cursor(&self, topic_id: ::irokle::TopicId) {
        if let Err(error) = remove_cursor(&self.fanout_cursors, topic_id) {
            warn!(%error, %topic_id, "Failed to clear document sync fan-out cursor");
        }
    }

    pub(in crate::document_sync) fn sync_peer_selection(
        &self,
        peers: &[NodeId],
        topic_id: &::irokle::TopicId,
    ) -> Result<PeerSelection> {
        let round = self.next_sync_round(*topic_id)?;
        let mut subject = [0u8; 64];
        subject[..32].copy_from_slice(topic_id.as_ref());
        subject[32..].copy_from_slice(self.node.peer_id().as_bytes());
        let candidates = if peers.is_empty() {
            self.default_peers.read().clone()
        } else {
            peers
                .iter()
                .copied()
                .map(|node_id| node_to_peer(&node_id))
                .collect()
        };
        Ok(select_sync_peers(
            self.eligible_peers(candidates, Some(*topic_id)),
            self.node.peer_id(),
            &subject,
            round,
        ))
    }

    pub(in crate::document_sync) fn log_peer_selection(
        &self,
        topic_id: ::irokle::TopicId,
        selection: &PeerSelection,
    ) {
        if selection.truncated {
            debug!(
                %topic_id,
                selected = selection.peers.len(),
                "Document sync fan-out bounded; omitted peers remain anti-entropy work"
            );
        }
    }

    pub(in crate::document_sync) fn allow_sync_peers(
        &self,
        peers: &BTreeSet<PeerId>,
    ) -> Result<()> {
        self.node
            .add_peers_to_whitelist(peers.iter().copied())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    pub(in crate::document_sync) async fn sync_peer_fanout<F, Fut>(
        selection: PeerSelection,
        context: String,
        run: F,
    ) -> Result<()>
    where
        F: Fn(PeerId) -> Fut,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let omitted = selection.truncated;
        let attempted = selection.peers.len();
        if attempted == 0 {
            return Ok(());
        }

        let fanout_started = Instant::now();
        let mut syncs = JoinSet::new();
        for peer in selection.peers {
            let future = run(peer);
            syncs.spawn(async move {
                let peer_started = Instant::now();
                let result = future.await;
                (peer, result, peer_started.elapsed())
            });
        }
        let mut successes = 0usize;
        let mut first_error = None;
        let mut per_peer = Vec::with_capacity(attempted);
        while let Some(result) = syncs.join_next().await {
            match result {
                Ok((peer, Ok(()), elapsed)) => {
                    successes += 1;
                    per_peer.push(format!(
                        "{}={}ms",
                        short_display_id(peer),
                        duration_ms(elapsed)
                    ));
                    debug!(%peer, context = %context, "Synced document peer")
                }
                Ok((peer, Err(error), elapsed)) => {
                    per_peer.push(format!(
                        "{}={}ms(err)",
                        short_display_id(peer),
                        duration_ms(elapsed)
                    ));
                    warn!(%peer, context = %context, error = %error, "Document sync peer sync failed; deferring to resync scheduler");
                    if first_error.is_none() {
                        first_error = Some(error.to_string());
                    }
                }
                Err(error) => {
                    warn!(context = %context, error = %error, "Document sync peer sync task failed");
                    if first_error.is_none() {
                        first_error = Some(error.to_string());
                    }
                }
            }
        }
        info!(
            event = "pipeline.fanout.summary",
            context = %context,
            peers = attempted,
            omitted,
            ok = successes,
            failed = attempted - successes,
            total_ms = duration_ms(fanout_started.elapsed()),
            per_peer = %per_peer.join(","),
            "Document sync peer fan-out summary"
        );
        if successes != attempted {
            let detail = first_error.unwrap_or_else(|| "unknown sync error".to_string());
            return Err(NetError::Bootstrap(format!(
                "{context}: only {successes}/{attempted} peers synced; {detail}"
            )));
        }
        Ok(())
    }

    pub(in crate::document_sync) async fn sync_topic(
        &self,
        topic_id: ::irokle::TopicId,
        selection: PeerSelection,
    ) -> Result<()> {
        let net = self.net.clone();
        Self::sync_peer_fanout(
            selection,
            format!("document sync topic {topic_id}"),
            move |peer| {
                let net = net.clone();
                async move {
                    match timeout(PEER_SYNC_TIMEOUT, net.sync_peer_now(peer, topic_id)).await {
                        Ok(Ok(())) => Ok(()),
                        // Irokle schedules the pages left after its budget.
                        Ok(Err(error)) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
                        Ok(Err(error)) => Err(NetError::Bootstrap(error.to_string())),
                        Err(_) => Err(NetError::Timeout(PEER_SYNC_TIMEOUT)),
                    }
                }
            },
        )
        .await
    }

    pub(in crate::document_sync) async fn sync_topics(
        &self,
        topic_ids: Vec<::irokle::TopicId>,
        peers: &[NodeId],
    ) -> Result<()> {
        if topic_ids.is_empty() {
            return Ok(());
        }
        type SyncGroups =
            BTreeMap<BTreeSet<PeerId>, (PeerSelection, Vec<(::irokle::TopicId, u64)>)>;
        for chunk in topic_ids.chunks(BATCH_TOPIC_LIMIT) {
            let mut groups: SyncGroups = BTreeMap::new();
            for topic_id in chunk.iter().copied() {
                let selection = self.sync_peer_selection(peers, &topic_id)?;
                let round = selection.round;
                let selected = selection.peers.clone();
                if let Some((group, topics)) = groups.get_mut(&selected) {
                    group.truncated |= selection.truncated;
                    topics.push((topic_id, round));
                } else {
                    groups.insert(selected, (selection, vec![(topic_id, round)]));
                }
            }
            for (_, (selection, topics)) in groups {
                let Some((topic_id, _)) = topics.first() else {
                    continue;
                };
                self.log_peer_selection(*topic_id, &selection);
                self.allow_sync_peers(&selection.peers)?;
                let topic_ids = topics
                    .iter()
                    .map(|(topic_id, _)| *topic_id)
                    .collect::<Vec<_>>();
                let result = self.sync_topic_batch(&topic_ids, selection).await;
                for (topic_id, round) in topics {
                    self.advance_cursor(topic_id, round)?;
                }
                self.flush_database()?;
                result?;
            }
        }
        Ok(())
    }

    async fn sync_topic_batch(
        &self,
        topic_ids: &[::irokle::TopicId],
        selection: PeerSelection,
    ) -> Result<()> {
        if topic_ids.is_empty() {
            return Ok(());
        }
        let service = self.clone();
        let topic_ids = topic_ids.to_vec();
        Self::sync_peer_fanout(
            selection,
            format!("document sync topic batch of {} topics", topic_ids.len()),
            move |peer| {
                let service = service.clone();
                let topic_ids = topic_ids.clone();
                async move { service.sync_batch_with(peer, topic_ids).await }
            },
        )
        .await
    }

    async fn sync_batch_with(&self, peer: PeerId, topic_ids: Vec<::irokle::TopicId>) -> Result<()> {
        let peer_addr = peer_endpoint_addr(peer)?;
        let results = self.net.sync_topics_now(peer_addr, &topic_ids).await;
        finish_batch_sync(peer, &results)
    }

    pub(in crate::document_sync) async fn bootstrap_from_peers(
        &self,
        topic_id: ::irokle::TopicId,
        peers: &[NodeId],
    ) -> Result<()> {
        let selection = self.sync_peer_selection(peers, &topic_id)?;
        self.log_peer_selection(topic_id, &selection);
        self.allow_sync_peers(&selection.peers)?;
        let mut first_error = None;
        for peer in selection.peers {
            match self.bootstrap_from_peer(topic_id, peer).await {
                Ok(()) => match self.has_topic(topic_id) {
                    Ok(true) => {
                        self.advance_cursor(topic_id, selection.round)?;
                        return Ok(());
                    }
                    Ok(false) => {
                        let error = NetError::TopicNotReady(topic_id.to_string());
                        warn!(%peer, %topic_id, "Document sync bootstrap peer has no topic");
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    Err(error) => {
                        warn!(%peer, %topic_id, error = %error, "Document sync bootstrap topic check failed");
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                },
                Err(error) => {
                    warn!(%peer, %topic_id, error = %error, "Document sync bootstrap attempt failed");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        // Advance after attempted peers so omitted candidates rotate into the next retry.
        self.advance_cursor(topic_id, selection.round)?;
        Err(first_error.unwrap_or_else(|| {
            NetError::Bootstrap(format!(
                "no peers available to bootstrap document sync topic {topic_id}"
            ))
        }))
    }

    /// Probes each co-holder for an existing genesis of `topics` without adopting
    /// anything, so a rank-0 holder can decide whether creation is safe (see
    /// [`ShardGenesisProbe`]); unreachable co-holders are recorded.
    pub async fn probe_shard_geneses(
        &self,
        topics: Vec<::irokle::TopicId>,
        co_holders: Vec<NodeId>,
    ) -> ShardGenesisProbe {
        use futures::StreamExt as _;

        let mut probe = ShardGenesisProbe::default();
        if topics.is_empty() {
            return probe;
        }
        let wanted: BTreeSet<::irokle::TopicId> = topics.iter().copied().collect();
        // Callers pass co-holders with the local node already excluded.
        let topic_ids = &topics;
        let probes = futures::stream::iter(co_holders.iter().copied().map(|node_id| async move {
            let peer = node_to_peer(&node_id);
            (node_id, self.probe_peer_topics(topic_ids, peer).await)
        }))
        .buffer_unordered(SHARD_PROBE_CONCURRENCY);
        // Poll a bounded number of peer probes together; aggregation is order
        // independent (set unions plus an unreachable list).
        let probe_results: Vec<_> = probes.collect().await;
        for (node_id, result) in probe_results {
            match result {
                Ok(peer_probe) => {
                    probe
                        .known_co_holder
                        .extend(peer_probe.known.iter().copied());
                    // A reached co-holder that neither advertised nor confirmed
                    // unknown refused it; withhold, never fork with a fresh genesis.
                    for topic in &wanted {
                        if !peer_probe.known.contains(topic)
                            && !peer_probe.confirmed_unknown.contains(topic)
                        {
                            probe.unconfirmed.insert(*topic);
                        }
                    }
                }
                Err(error) => {
                    debug!(%node_id, error = %error, "co-holder unreachable while probing shard genesis");
                    probe.unreachable.push(node_id);
                }
            }
        }
        probe
    }

    async fn probe_peer_topics(
        &self,
        topics: &[::irokle::TopicId],
        peer: PeerId,
    ) -> Result<PeerTopicProbe> {
        let peer_addr = peer_endpoint_addr(peer)?;
        let wanted: BTreeSet<::irokle::TopicId> = topics.iter().copied().collect();
        let mut probe = PeerTopicProbe::default();
        for chunk in topics.chunks(BATCH_TOPIC_LIMIT) {
            let opens: Vec<SyncMessage> = chunk
                .iter()
                .map(|topic| SyncMessage::Open(self.node.sync_open(*topic)))
                .collect();
            let responses = timeout(
                PEER_SYNC_TIMEOUT,
                self.net.sync_with(peer_addr.clone(), &opens),
            )
            .await
            .map_err(|_| NetError::Timeout(PEER_SYNC_TIMEOUT))?
            .map_err(NetError::from)?;
            probe.merge(classify_probe_responses(&wanted, responses.messages()));
        }
        Ok(probe)
    }

    async fn bootstrap_from_peer(&self, topic_id: ::irokle::TopicId, peer: PeerId) -> Result<()> {
        let peer_addr = peer_endpoint_addr(peer)?;
        let responses = timeout(
            PEER_SYNC_TIMEOUT,
            self.net.sync_with(
                peer_addr.clone(),
                &[SyncMessage::Open(self.node.sync_open(topic_id))],
            ),
        )
        .await
        .map_err(|_| NetError::Timeout(PEER_SYNC_TIMEOUT))?
        .map_err(NetError::from)?;
        let summary = responses
            .messages()
            .iter()
            .find_map(|response| match response {
                SyncMessage::Summary(summary) if summary.topic_id == topic_id => Some(summary),
                _ => None,
            })
            .ok_or_else(|| {
                NetError::Bootstrap(format!(
                    "peer {peer} did not return a document sync summary for topic {topic_id}"
                ))
            })?;
        if summary_is_empty(summary) {
            return Ok(());
        }
        if summary.event_type_id.as_deref() != Some(DocumentEvent::TYPE_ID) {
            return Err(NetError::Bootstrap(format!(
                "peer {peer} advertised document sync topic {topic_id} with unexpected event type {:?}",
                summary.event_type_id
            )));
        }

        let results = self.net.sync_topics_now(peer_addr, &[topic_id]).await;
        finish_batch_sync(peer, &results)
    }
}
