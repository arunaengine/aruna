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
