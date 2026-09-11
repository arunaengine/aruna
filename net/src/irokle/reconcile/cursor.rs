use super::*;

pub(in crate::document_sync) fn topic_cursor_key(topic_id: irokle_crate::TopicId) -> ByteView {
    let mut key = b"topic-cursor/".to_vec();
    key.extend_from_slice(topic_id.as_bytes());
    ByteView::from(key)
}

/// Decodes a stored cursor, discarding one written under another genesis, one
/// whose recorded ops no longer occupy their positions, and one in an
/// unreadable shape. A discarded cursor restarts replay at actor sequence one.
pub(in crate::document_sync) fn applied_cursor_clock(
    storage: &impl irokle_crate::storage::Storage,
    topic_id: irokle_crate::TopicId,
    genesis: irokle_crate::OpId,
    stored: Option<Value>,
) -> Result<irokle_crate::ActorClock> {
    let Some(cursor) = stored
        .and_then(|value| postcard::from_bytes::<AppliedCursor>(value.as_ref()).ok())
        .filter(|cursor| cursor.lineage == genesis)
    else {
        return Ok(irokle_crate::ActorClock::default());
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
            return Ok(irokle_crate::ActorClock::default());
        }
    }
    Ok(cursor.clock)
}

/// Encodes a cursor with the ops that currently occupy its positions. A position
/// with no op is left unmarked, which the read side treats as untrusted.
pub(in crate::document_sync) fn applied_cursor_value(
    storage: &impl irokle_crate::storage::Storage,
    topic_id: irokle_crate::TopicId,
    genesis: irokle_crate::OpId,
    clock: &irokle_crate::ActorClock,
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
    genesis: irokle_crate::OpId,
    round: u64,
) -> [u8; FANOUT_CURSOR_LEN] {
    let mut value = [0u8; FANOUT_CURSOR_LEN];
    value[..irokle_crate::OpId::LEN].copy_from_slice(genesis.as_bytes());
    value[irokle_crate::OpId::LEN..].copy_from_slice(&round.to_be_bytes());
    value
}

pub(in crate::document_sync) fn fanout_cursor_round(
    value: &[u8],
    genesis: irokle_crate::OpId,
) -> Option<u64> {
    let (lineage, round) = value.split_at_checked(irokle_crate::OpId::LEN)?;
    if lineage != genesis.as_bytes() {
        return None;
    }
    Some(u64::from_be_bytes(round.try_into().ok()?))
}

pub(in crate::document_sync) fn current_cursor(
    cursors: &fjall::OptimisticTxKeyspace,
    topic_id: irokle_crate::TopicId,
    genesis: irokle_crate::OpId,
) -> Result<u64> {
    Ok(cursors
        .get(topic_cursor_key(topic_id))
        .map_err(|error| NetError::Bootstrap(error.to_string()))?
        .and_then(|value| fanout_cursor_round(value.as_ref(), genesis))
        .unwrap_or_default())
}

pub(in crate::document_sync) fn advance_cursor(
    cursors: &fjall::OptimisticTxKeyspace,
    topic_id: irokle_crate::TopicId,
    genesis: irokle_crate::OpId,
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
    topic_id: irokle_crate::TopicId,
) -> Result<()> {
    cursors
        .remove(topic_cursor_key(topic_id))
        .map_err(|error| NetError::Bootstrap(error.to_string()))
}

pub(in crate::document_sync) fn deferred_topics_key() -> ByteView {
    // Retain the original key so previously persisted admin dependencies decode.
    ByteView::from(b"deferred-admin-topics".to_vec())
}

pub(in crate::document_sync) async fn read_inbound_sync_messages(
    recv: &mut iroh::endpoint::RecvStream,
    reservation: &mut InboundByteReservation,
) -> Result<(Vec<SyncMessage>, Vec<irokle_crate::TopicId>)> {
    let mut messages = Vec::new();
    let mut topics = BTreeSet::new();
    let mut bytes_read = 0usize;
    let mut frame_index = 0usize;
    while let Some(frame) = timeout(
        DOCUMENT_SYNC_INBOUND_FRAME_TIMEOUT,
        read_next_inbound_sync_frame(recv, &mut bytes_read, reservation),
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
        topics.insert(sync_message_topic_id(&message));
        messages.push(message);
    }
    Ok((messages, topics.into_iter().collect()))
}

pub(in crate::document_sync) async fn read_next_inbound_sync_frame(
    recv: &mut iroh::endpoint::RecvStream,
    bytes_read: &mut usize,
    reservation: &mut InboundByteReservation,
) -> Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    let Some(first_read) = read_some_inbound_sync(recv, &mut len_buf[..1]).await? else {
        return Ok(None);
    };
    if first_read == 0 {
        return Ok(None);
    }

    let mut read = first_read;
    while read < len_buf.len() {
        let Some(n) = read_some_inbound_sync(recv, &mut len_buf[read..]).await? else {
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
        let Some(n) = read_some_inbound_sync(recv, &mut payload[payload_read..]).await? else {
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

pub(in crate::document_sync) async fn read_some_inbound_sync(
    recv: &mut iroh::endpoint::RecvStream,
    buf: &mut [u8],
) -> Result<Option<usize>> {
    timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT, recv.read(buf))
        .await
        .map_err(|_| NetError::Timeout(DOCUMENT_SYNC_PEER_SYNC_TIMEOUT))?
        .map_err(|error| NetError::Stream(error.to_string()))
}

pub(in crate::document_sync) async fn write_inbound_sync_messages(
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
