//! Remembers the oplog op each local publish became, so a publish repeated after a restart finds
//! that op instead of appending the event again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

/// Event keys (topic, then a hash of event id and target) to the op id they became; a topic id
/// alone keys the mark: the sequence and id of this node's last own op the keyspace covers.
pub(in crate::document_sync) const PUBLISHED_KEYSPACE: &str = "document-sync-published";
/// Locks for the check and the append of a topic; a topic takes the one its first byte names.
pub(in crate::document_sync) const PUBLISH_LOCKS: usize = 64;
/// Own ops one read of the unrecorded tail takes.
const TAIL_PAGE: usize = 256;

pub(in crate::document_sync) fn event_key(
    topic_id: ::irokle::TopicId,
    event: &DocumentEvent,
) -> Result<Vec<u8>> {
    let identity = (event.event_id(), event.target());
    let identity = postcard::to_allocvec(&identity).map_err(failed)?;
    let mut key = topic_id.as_bytes().to_vec();
    key.extend_from_slice(blake3::hash(&identity).as_bytes());
    Ok(key)
}

fn failed(error: impl std::fmt::Display) -> NetError {
    NetError::Bootstrap(error.to_string())
}

fn stored_id(value: &[u8]) -> Option<::irokle::OpId> {
    let bytes: [u8; 32] = value.try_into().ok()?;
    Some(::irokle::OpId::from_bytes(bytes))
}

fn read_mark(value: &[u8]) -> Option<(u64, ::irokle::OpId)> {
    let (seq, op_id) = value.split_first_chunk::<8>()?;
    Some((u64::from_be_bytes(*seq), stored_id(op_id)?))
}

impl DocumentSyncService {
    /// Holds the publish lock of `topic_id` while the guard lives.
    pub(in crate::document_sync) fn publish_lock(
        &self,
        topic_id: ::irokle::TopicId,
    ) -> parking_lot::MutexGuard<'_, ()> {
        let lock = usize::from(topic_id.as_bytes()[0]) % self.publish_locks.len();
        self.publish_locks[lock].lock()
    }

    /// The op on the current lineage this node published the event under `key` as. A recorded op
    /// a genesis tie-break removed is gone, so the event is published again.
    pub(in crate::document_sync) fn published_op(
        &self,
        topic_id: ::irokle::TopicId,
        actor_id: ::irokle::ActorId,
        key: &[u8],
    ) -> Result<Option<::irokle::Op>> {
        self.record_tail(topic_id, actor_id)?;
        let stored = self.published.get(key).map_err(failed)?;
        let Some(op_id) = stored.and_then(|value| stored_id(&value)) else {
            return Ok(None);
        };
        self.node.storage().get_op(&op_id).map_err(failed)
    }

    /// Records the event of `op` under `key` and moves the mark of its topic to it.
    pub(in crate::document_sync) fn keep_published(
        &self,
        key: &[u8],
        op: &::irokle::Op,
    ) -> Result<()> {
        let body = &op.signed.body;
        self.store_row(key, op.id.as_bytes())?;
        self.move_mark(body.topic_id, body.actor_seq, op.id)
    }

    fn move_mark(
        &self,
        topic_id: ::irokle::TopicId,
        seq: u64,
        op_id: ::irokle::OpId,
    ) -> Result<()> {
        let mut mark = seq.to_be_bytes().to_vec();
        mark.extend_from_slice(op_id.as_bytes());
        self.store_row(topic_id.as_bytes(), &mark)
    }

    pub(in crate::document_sync) fn store_row(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.published.insert(key, value).map_err(failed)
    }

    /// Records the own ops after the mark, appended but not recorded when the process stopped in
    /// between. A mark the lineage lost reads from this node's first op again.
    fn record_tail(&self, topic_id: ::irokle::TopicId, actor_id: ::irokle::ActorId) -> Result<()> {
        let storage = self.node.storage();
        let mark = self.published.get(topic_id.as_bytes()).map_err(failed)?;
        let mut after = 0;
        if let Some((seq, op_id)) = mark.and_then(|value| read_mark(&value)) {
            let held = storage.actor_index(&topic_id, &actor_id, seq);
            if held.map_err(failed)? == Some(op_id) {
                after = seq;
            }
        }
        loop {
            let page = storage.actor_range(&topic_id, &actor_id, after, TAIL_PAGE);
            let page = page.map_err(failed)?;
            for (_, op_id) in &page {
                let Some(op) = storage.get_op(op_id).map_err(failed)? else {
                    continue;
                };
                if let TopicPayload::Event(envelope) = &op.signed.body.payload
                    && let Ok(event) = envelope.decode_event::<DocumentEvent>()
                {
                    self.store_row(&event_key(topic_id, &event)?, op.id.as_bytes())?;
                }
            }
            let Some(&(seq, op_id)) = page.last() else {
                return Ok(());
            };
            self.move_mark(topic_id, seq, op_id)?;
            after = seq;
            if page.len() < TAIL_PAGE {
                return Ok(());
            }
        }
    }
}
