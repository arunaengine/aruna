use super::*;

pub(crate) fn topic_cursor_key(topic_id: ::irokle::TopicId) -> ByteView {
    let mut key = b"topic-cursor/".to_vec();
    key.extend_from_slice(topic_id.as_bytes());
    ByteView::from(key)
}

/// Decodes a stored cursor, discarding one written under another genesis, one
/// whose recorded ops no longer occupy their positions, and one in an
/// unreadable shape. A discarded cursor restarts replay at actor sequence one.
pub(in crate::document_sync) fn applied_cursor_clock(
    storage: &impl ::irokle::storage::Storage,
    topic_id: ::irokle::TopicId,
    genesis: ::irokle::OpId,
    stored: Option<Value>,
) -> Result<::irokle::ActorClock> {
    let Some(cursor) = stored
        .and_then(|value| postcard::from_bytes::<AppliedCursor>(value.as_ref()).ok())
        .filter(|cursor| cursor.lineage == genesis)
    else {
        return Ok(::irokle::ActorClock::default());
    };
    for (actor_id, actor_seq) in cursor.clock.iter() {
        if *actor_seq == 0 {
            continue;
        }
        let current = storage
            .actor_index(&topic_id, actor_id, *actor_seq)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if current.is_none() || current.as_ref() != cursor.marks.get(actor_id) {
            debug!(%topic_id, %actor_id, "Discarding a document sync cursor whose history was rebuilt");
            return Ok(::irokle::ActorClock::default());
        }
    }
    Ok(cursor.clock)
}

/// Encodes a cursor with the ops that currently occupy its positions. A position
/// with no op is left unmarked, which the read side treats as untrusted.
pub(in crate::document_sync) fn applied_cursor_value(
    storage: &impl ::irokle::storage::Storage,
    topic_id: ::irokle::TopicId,
    genesis: ::irokle::OpId,
    clock: &::irokle::ActorClock,
) -> Result<ByteView> {
    let mut marks = BTreeMap::new();
    for (actor_id, actor_seq) in clock.iter() {
        if *actor_seq == 0 {
            continue;
        }
        if let Some(op_id) = storage
            .actor_index(&topic_id, actor_id, *actor_seq)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
        {
            marks.insert(*actor_id, op_id);
        }
    }
    postcard::to_allocvec(&AppliedCursor {
        lineage: genesis,
        clock: clock.clone(),
        marks,
    })
    .map(ByteView::from)
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

pub(in crate::document_sync) fn fanout_cursor_value(
    genesis: ::irokle::OpId,
    round: u64,
) -> [u8; FANOUT_CURSOR_LEN] {
    let mut value = [0u8; FANOUT_CURSOR_LEN];
    value[..::irokle::OpId::LEN].copy_from_slice(genesis.as_bytes());
    value[::irokle::OpId::LEN..].copy_from_slice(&round.to_be_bytes());
    value
}

pub(in crate::document_sync) fn fanout_cursor_round(
    value: &[u8],
    genesis: ::irokle::OpId,
) -> Option<u64> {
    let (lineage, round) = value.split_at_checked(::irokle::OpId::LEN)?;
    if lineage != genesis.as_bytes() {
        return None;
    }
    Some(u64::from_be_bytes(round.try_into().ok()?))
}

pub(in crate::document_sync) fn current_cursor(
    cursors: &fjall::OptimisticTxKeyspace,
    topic_id: ::irokle::TopicId,
    genesis: ::irokle::OpId,
) -> Result<u64> {
    Ok(cursors
        .get(topic_cursor_key(topic_id))
        .map_err(|error| NetError::Bootstrap(error.to_string()))?
        .and_then(|value| fanout_cursor_round(value.as_ref(), genesis))
        .unwrap_or_default())
}

pub(in crate::document_sync) fn advance_cursor(
    cursors: &fjall::OptimisticTxKeyspace,
    topic_id: ::irokle::TopicId,
    genesis: ::irokle::OpId,
    round: u64,
) -> Result<()> {
    cursors
        .update_fetch(topic_cursor_key(topic_id), |value| {
            // A cursor from a replaced genesis reads as no round at all, so the
            // first advance after a tie-break rebinds it to the new lineage.
            match value.and_then(|value| fanout_cursor_round(value.as_ref(), genesis)) {
                Some(stored) if stored != round => value.cloned(),
                _ => Some(fjall::Slice::from(
                    fanout_cursor_value(genesis, round.wrapping_add(1)).as_slice(),
                )),
            }
        })
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    // Advance only after network attempts; a crash before persistence safely repeats them.
    Ok(())
}

pub(in crate::document_sync) fn remove_cursor(
    cursors: &fjall::OptimisticTxKeyspace,
    topic_id: ::irokle::TopicId,
) -> Result<()> {
    cursors
        .remove(topic_cursor_key(topic_id))
        .map_err(|error| NetError::Bootstrap(error.to_string()))
}

pub(in crate::document_sync) fn deferred_topics_key() -> ByteView {
    // Retain the original key so previously persisted admin dependencies decode.
    ByteView::from(b"deferred-admin-topics".to_vec())
}

pub(in crate::document_sync) async fn read_sync_messages(
    recv: &mut iroh::endpoint::RecvStream,
    reservation: &mut InboundByteReservation,
) -> Result<(Vec<SyncMessage>, Vec<::irokle::TopicId>)> {
    let mut messages = Vec::new();
    let mut topics = BTreeSet::new();
    let mut bytes_read = 0usize;
    let mut frame_index = 0usize;
    while let Some(frame) = timeout(
        DOCUMENT_SYNC_INBOUND_FRAME_TIMEOUT,
        read_sync_frame(recv, &mut bytes_read, reservation),
    )
    .await
    .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_INBOUND_FRAME_TIMEOUT))??
    {
        frame_index = frame_index.saturating_add(1);
        if messages.len() >= DOCUMENT_SYNC_INBOUND_SYNC_MESSAGE_LIMIT {
            return Err(NetError::Stream(format!(
                "document sync stream exceeded {DOCUMENT_SYNC_INBOUND_SYNC_MESSAGE_LIMIT} messages"
            )));
        }
        let message = decode_sync_message(&frame).map_err(|error| {
            NetError::Stream(format!(
                "invalid document sync message frame {frame_index} ({} bytes): {error}",
                frame.len()
            ))
        })?;
        topics.insert(message_topic_id(&message));
        messages.push(message);
    }
    Ok((messages, topics.into_iter().collect()))
}

pub(in crate::document_sync) async fn read_sync_frame(
    recv: &mut iroh::endpoint::RecvStream,
    bytes_read: &mut usize,
    reservation: &mut InboundByteReservation,
) -> Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    let Some(first_read) = read_sync_chunk(recv, &mut len_buf[..1]).await? else {
        return Ok(None);
    };
    if first_read == 0 {
        return Ok(None);
    }

    let mut read = first_read;
    while read < len_buf.len() {
        let Some(n) = read_sync_chunk(recv, &mut len_buf[read..]).await? else {
            return Err(NetError::Stream(
                "incomplete document sync frame length".to_string(),
            ));
        };
        if n == 0 {
            return Err(NetError::Stream(
                "incomplete document sync frame length".to_string(),
            ));
        }
        read += n;
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > DOCUMENT_SYNC_FRAME_LEN_LIMIT {
        return Err(NetError::Stream(
            "document sync frame exceeds maximum length".to_string(),
        ));
    }
    *bytes_read = bytes_read.saturating_add(4).saturating_add(len);
    if *bytes_read > DOCUMENT_SYNC_INBOUND_SYNC_STREAM_BYTES {
        return Err(NetError::Stream(format!(
            "document sync stream exceeded {DOCUMENT_SYNC_INBOUND_SYNC_STREAM_BYTES} bytes"
        )));
    }
    reservation.reserve(len)?;

    let mut payload = vec![0u8; len];
    let mut payload_read = 0usize;
    while payload_read < payload.len() {
        let Some(n) = read_sync_chunk(recv, &mut payload[payload_read..]).await? else {
            return Err(NetError::Stream(
                "incomplete document sync frame payload".to_string(),
            ));
        };
        if n == 0 {
            return Err(NetError::Stream(
                "incomplete document sync frame payload".to_string(),
            ));
        }
        payload_read += n;
    }
    Ok(Some(payload))
}

pub(in crate::document_sync) async fn read_sync_chunk(
    recv: &mut iroh::endpoint::RecvStream,
    buf: &mut [u8],
) -> Result<Option<usize>> {
    timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT, recv.read(buf))
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
        .map_err(|error| NetError::Stream(error.to_string()))
}

pub(in crate::document_sync) async fn write_sync_messages(
    send: &mut iroh::endpoint::SendStream,
    messages: &[SyncMessage],
) -> Result<()> {
    for message in messages {
        let payload =
            encode_sync_message(message).map_err(|error| NetError::Stream(error.to_string()))?;
        let frame = encode_frame(&payload).map_err(|error| NetError::Stream(error.to_string()))?;
        timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT, send.write_all(&frame))
            .await
            .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
            .map_err(|error| NetError::Stream(error.to_string()))?;
    }
    send.finish()
        .map_err(|error| NetError::Stream(error.to_string()))
}

type BatchSummaryOutcome = (
    BTreeSet<::irokle::TopicId>,
    BTreeSet<::irokle::TopicId>,
    Vec<SyncMessage>,
);

pub(in crate::document_sync) fn process_summary_responses(
    node: &::irokle::Irokle<::irokle::FjallStorage>,
    peer: PeerId,
    known_topics: &BTreeSet<::irokle::TopicId>,
    local_fingerprints: &BTreeMap<::irokle::TopicId, [u8; 32]>,
    responses: Vec<SyncMessage>,
) -> Result<BatchSummaryOutcome> {
    let mut responded_topics = BTreeSet::new();
    let mut failed_topics = BTreeSet::new();
    let mut sync_messages = Vec::new();
    for response in responses {
        match response {
            // A terminal failure still answers for this topic: only it stays
            // dirty; the rest of the batch keeps summaries, data and acks.
            SyncMessage::Failure(failure) if known_topics.contains(&failure.topic_id) => {
                responded_topics.insert(failure.topic_id);
                failed_topics.insert(failure.topic_id);
                warn!(
                    %peer,
                    topic_id = %failure.topic_id,
                    code = ?failure.code,
                    "Skipping document sync batch topic: peer reported a sync failure"
                );
            }
            SyncMessage::Fingerprint(remote) if known_topics.contains(&remote.topic_id) => {
                responded_topics.insert(remote.topic_id);
                if local_fingerprints.get(&remote.topic_id) != Some(&remote.fingerprint) {
                    warn!(
                        %peer,
                        topic_id = %remote.topic_id,
                        "Skipping document sync batch topic: peer returned mismatched fingerprint"
                    );
                    failed_topics.insert(remote.topic_id);
                }
            }
            SyncMessage::Summary(summary) if known_topics.contains(&summary.topic_id) => {
                responded_topics.insert(summary.topic_id);
                if let Some(event_type_id) = summary.event_type_id.as_deref()
                    && event_type_id != DocumentSyncEvent::TYPE_ID
                {
                    warn!(
                        %peer,
                        topic_id = %summary.topic_id,
                        event_type_id,
                        "Skipping document sync batch topic: peer advertised unexpected event type"
                    );
                    failed_topics.insert(summary.topic_id);
                    continue;
                }
                let plan = match node.negotiate_sync(peer, &summary) {
                    Ok(plan) => plan,
                    Err(error) => {
                        warn!(
                            %peer,
                            topic_id = %summary.topic_id,
                            error = %error,
                            "Skipping document sync batch topic: sync negotiation failed"
                        );
                        failed_topics.insert(summary.topic_id);
                        continue;
                    }
                };
                let wants_remote_data = !plan.need.is_empty() || !plan.actor_range_hints.is_empty();
                if !plan.send.is_empty() || wants_remote_data {
                    sync_messages.push(SyncMessage::Open(node.sync_open(plan.topic_id)));
                    if !plan.send.is_empty() {
                        sync_messages.push(SyncMessage::Data(SyncData {
                            topic_id: plan.topic_id,
                            ops: plan.send,
                        }));
                    }
                    if wants_remote_data {
                        sync_messages.push(SyncMessage::Request(SyncRequest {
                            topic_id: plan.topic_id,
                            known: plan.common,
                            wants: plan.need,
                            actor_range_hints: plan.actor_range_hints,
                        }));
                    }
                }
            }
            other => {
                return Err(NetError::Bootstrap(format!(
                    "unexpected document sync batch response from {peer}: {other:?}"
                )));
            }
        }
    }
    Ok((responded_topics, failed_topics, sync_messages))
}

/// Names the bounded-journal refusal. Past Irokle's cap on unreleased records
/// every genesis tie-break reset is refused, which otherwise reaches operators
/// only as an opaque admission failure.
pub(in crate::document_sync) fn report_journal_full(
    topic_id: ::irokle::TopicId,
    error: &::irokle::Error,
) {
    if matches!(error, ::irokle::Error::EvictionJournalFull) {
        error!(
            %topic_id,
            "Eviction journal is full; genesis tie-break resets stay refused until the eviction consumer drains it"
        );
    }
}

pub(in crate::document_sync) fn forward_evictions_to(
    sink: &tokio::sync::mpsc::UnboundedSender<TopicEviction>,
    evictions: Vec<TopicEviction>,
) {
    for eviction in evictions {
        if sink.send(eviction).is_err() {
            warn!("Document sync eviction consumer closed; dropping re-emitted payloads");
        }
    }
}

pub(in crate::document_sync) fn process_data_responses(
    node: &::irokle::Irokle<::irokle::FjallStorage>,
    net: &::irokle::net::IrohNet<::irokle::FjallStorage>,
    peer: PeerId,
    known_topics: &BTreeSet<::irokle::TopicId>,
    mut failed_topics: BTreeSet<::irokle::TopicId>,
    responses: Vec<SyncMessage>,
    eviction_tx: &tokio::sync::mpsc::UnboundedSender<TopicEviction>,
) -> Result<(BTreeSet<::irokle::TopicId>, Vec<SyncMessage>)> {
    let mut followup = Vec::new();
    let mut acks = Vec::new();
    for response in responses {
        match response {
            SyncMessage::Ack(ack)
                if ack.peer_id == peer && known_topics.contains(&ack.topic_id) =>
            {
                acks.push(ack);
            }
            SyncMessage::Failure(failure) if known_topics.contains(&failure.topic_id) => {
                failed_topics.insert(failure.topic_id);
                warn!(
                    %peer,
                    topic_id = %failure.topic_id,
                    code = ?failure.code,
                    "Skipping document sync batch topic: peer reported a sync failure"
                );
            }
            SyncMessage::Summary(summary) if known_topics.contains(&summary.topic_id) => {}
            SyncMessage::Data(data) if known_topics.contains(&data.topic_id) => {
                let topic_id = data.topic_id;
                let ack = match node.receive_sync_data_from_evicting(peer, data) {
                    Ok((ack, evictions)) => {
                        forward_evictions_to(eviction_tx, evictions);
                        ack
                    }
                    Err(error) => {
                        report_journal_full(topic_id, &error);
                        warn!(
                            %peer,
                            topic_id = %topic_id,
                            error = %error,
                            "Skipping document sync batch topic: receiving sync data failed"
                        );
                        failed_topics.insert(topic_id);
                        continue;
                    }
                };
                net.schedule_topic_recheck(topic_id)?;
                followup.push(SyncMessage::Open(node.sync_open(topic_id)));
                followup.push(SyncMessage::Ack(ack));
            }
            other => {
                return Err(NetError::Bootstrap(format!(
                    "unexpected document sync batch data response from {peer}: {other:?}"
                )));
            }
        }
    }
    for (ack, result) in acks.iter().zip(node.apply_sync_acks(&acks)) {
        if let Err(error) = result {
            warn!(
                %peer,
                topic_id = %ack.topic_id,
                error = %error,
                "Skipping document sync batch topic: applying sync ack failed"
            );
            failed_topics.insert(ack.topic_id);
        }
    }
    Ok((failed_topics, followup))
}

#[allow(clippy::too_many_arguments)]
pub(in crate::document_sync) fn log_batch_summary(
    peer: PeerId,
    topics: usize,
    r1_build: Duration,
    r1_io: Duration,
    r1_process: Duration,
    r2_io: Duration,
    r2_process: Duration,
    fu_io: Duration,
    r2_messages: usize,
    total: Duration,
) {
    info!(
        event = "pipeline.peer_batch.summary",
        peer = %peer,
        topics,
        r1_build_ms = duration_ms(r1_build),
        r1_io_ms = duration_ms(r1_io),
        r1_process_ms = duration_ms(r1_process),
        r2_io_ms = duration_ms(r2_io),
        r2_process_ms = duration_ms(r2_process),
        fu_io_ms = duration_ms(fu_io),
        r2_messages,
        total_ms = duration_ms(total),
        "Document sync peer batch sync round breakdown"
    );
}

pub(in crate::document_sync) fn finish_batch_sync(
    peer: PeerId,
    known_topics: &BTreeSet<::irokle::TopicId>,
    failed_topics: &BTreeSet<::irokle::TopicId>,
) -> Result<()> {
    if !failed_topics.is_empty() {
        warn!(
            %peer,
            failed = failed_topics.len(),
            total = known_topics.len(),
            "Document sync batch sync failed for one or more topics"
        );
        return Err(NetError::Bootstrap(format!(
            "peer {peer}: {}/{} document sync batch topics failed to sync",
            failed_topics.len(),
            known_topics.len()
        )));
    }
    Ok(())
}

pub(in crate::document_sync) fn message_topic_id(message: &SyncMessage) -> ::irokle::TopicId {
    match message {
        SyncMessage::Open(open) => open.topic_id,
        SyncMessage::Fingerprint(fingerprint) => fingerprint.topic_id,
        SyncMessage::Summary(summary) => summary.topic_id,
        SyncMessage::Request(request) => request.topic_id,
        SyncMessage::Data(data) => data.topic_id,
        SyncMessage::Ack(ack) => ack.topic_id,
        SyncMessage::Failure(failure) => failure.topic_id,
    }
}

pub(in crate::document_sync) fn summary_is_empty(summary: &::irokle::sync::SyncSummary) -> bool {
    summary.event_type_id.is_none() && summary.heads.is_empty()
}

impl PeerTopicProbe {
    pub(in crate::document_sync) fn merge(&mut self, other: PeerTopicProbe) {
        self.known.extend(other.known);
        self.confirmed_unknown.extend(other.confirmed_unknown);
    }
}

/// Buckets a peer's Open responses for `wanted`: non-empty summary ⇒ the peer
/// holds a genesis; empty summary ⇒ it has none; no summary ⇒ refused (holds it
/// but the prober may not open it yet, so it is not unknown).
pub(in crate::document_sync) fn classify_probe_responses(
    wanted: &BTreeSet<::irokle::TopicId>,
    responses: Vec<SyncMessage>,
) -> PeerTopicProbe {
    let mut probe = PeerTopicProbe::default();
    for response in responses {
        if let SyncMessage::Summary(summary) = response
            && wanted.contains(&summary.topic_id)
        {
            if summary_is_empty(&summary) {
                probe.confirmed_unknown.insert(summary.topic_id);
            } else {
                probe.known.insert(summary.topic_id);
            }
        }
    }
    probe
}

pub(in crate::document_sync) fn peer_endpoint_addr(peer_id: PeerId) -> Result<iroh::EndpointAddr> {
    let endpoint_id = iroh::EndpointId::from_bytes(peer_id.as_bytes())
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(iroh::EndpointAddr::from(endpoint_id))
}

/// An applied-ops cursor with the history it describes: the genesis it was built
/// from and the op at each actor position. A chain replacement or orphan rebuild
/// renumbers sequences, so a position alone is never evidence its op stands.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct AppliedCursor {
    pub(in crate::document_sync) lineage: ::irokle::OpId,
    pub(crate) clock: ::irokle::ActorClock,
    pub(in crate::document_sync) marks: BTreeMap<::irokle::ActorId, ::irokle::OpId>,
}

const FANOUT_CURSOR_LEN: usize = ::irokle::OpId::LEN + std::mem::size_of::<u64>();

/// Per-peer probe outcome: topics the peer holds a genesis for (`known`) and
/// those it confirmed it has none of (`confirmed_unknown`). A topic in neither
/// was refused and must not be treated as unknown.
#[derive(Debug, Default, PartialEq, Eq)]
pub(in crate::document_sync) struct PeerTopicProbe {
    pub(in crate::document_sync) known: BTreeSet<::irokle::TopicId>,
    pub(in crate::document_sync) confirmed_unknown: BTreeSet<::irokle::TopicId>,
}
