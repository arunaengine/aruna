use super::*;

impl DocumentSyncService {
    /// Notes a live inbound sync connection so the resync scheduler retries the
    /// peer immediately. It is not pooled: streams over it toward the dialer are
    /// never accepted by the accept loop.
    pub fn register_inbound_connection(&self, connection: &iroh::endpoint::Connection) {
        self.net
            .note_peer_reachable(node_to_peer(&connection.remote_id()));
    }

    /// Admission for one inbound sync stream, decided before any payload byte
    /// is read: the pusher must be a configured realm peer and within the
    /// per-peer and global stream budgets.
    pub(in crate::document_sync) fn admit_inbound(
        &self,
        peer: NodeId,
    ) -> Result<InboundSyncPermit> {
        let peer_id = node_to_peer(&peer);
        // Bootstrap peers admit only until realm config materializes; after that
        // the sync-eligible set is sole authority, so a removed peer fails here.
        let bootstrap_window = !self.realm_config_materialized.load(Ordering::Acquire);
        let admitted = self.default_peers.read().contains(&peer_id)
            || (bootstrap_window && self.configured_peers.contains(&peer_id));
        if !admitted {
            return Err(NetError::AdmissionRejected(format!(
                "document sync peer {peer_id} is not a current realm peer"
            )));
        }
        self.inbound_budget.acquire(peer_id).ok_or_else(|| {
            NetError::AdmissionRejected(format!(
                "document sync stream budget exhausted for peer {peer_id}"
            ))
        })
    }

    pub async fn handle_inbound_stream(
        &self,
        stream: BiStream,
        peer: NodeId,
    ) -> Result<Vec<::irokle::TopicId>> {
        let stream_started = Instant::now();
        let _permit = self.admit_inbound(peer)?;
        self.net.note_peer_reachable(node_to_peer(&peer));
        let BiStream(mut send, mut recv, _) = stream;
        let mut byte_reservation =
            InboundByteReservation::new(self.inbound_budget.clone(), node_to_peer(&peer));
        let (messages, touched_topics) = timeout(
            DOCUMENT_SYNC_INBOUND_STREAM_TIMEOUT,
            read_sync_messages(&mut recv, &mut byte_reservation),
        )
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_INBOUND_STREAM_TIMEOUT))??;
        let read_elapsed = stream_started.elapsed();
        let message_count = messages.len();
        let handle_started = Instant::now();
        let net = self.net.clone();
        let responses = tokio::task::spawn_blocking(move || net.handle_messages(peer, messages))
            .await
            .map_err(|error| NetError::Stream(error.to_string()))?
            .map_err(|error| NetError::Stream(error.to_string()))?;
        let handle_elapsed = handle_started.elapsed();
        let write_started = Instant::now();
        write_sync_messages(&mut send, &responses).await?;
        let write_elapsed = write_started.elapsed();
        let flush_started = Instant::now();
        self.flush_database()?;
        info!(
            event = "pipeline.inbound_sync.summary",
            peer = %node_to_peer(&peer),
            messages = message_count,
            responses = responses.len(),
            topics = touched_topics.len(),
            read_ms = duration_ms(read_elapsed),
            handle_ms = duration_ms(handle_elapsed),
            write_ms = duration_ms(write_elapsed),
            flush_ms = duration_ms(flush_started.elapsed()),
            total_ms = duration_ms(stream_started.elapsed()),
            "Inbound document sync stream summary"
        );
        Ok(touched_topics)
    }
}
