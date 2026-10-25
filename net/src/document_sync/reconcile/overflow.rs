//! Revisits topic operations whose distribution snapshots could not be parked because the
//! parking bound was reached, in bounded pages, so no snapshot is skipped for good.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::cursor::{applied_cursor_clock, applied_cursor_value, topic_cursor_key};
use super::*;

const OVERFLOW_PREFIX: &[u8] = b"overflow-cursor/";
/// Where the current sweep over the overflowed operations continues.
const SCAN_PREFIX: &[u8] = b"overflow-scan/";
/// The first operation of the current sweep that still found parking full.
const FULL_PREFIX: &[u8] = b"overflow-full/";
/// Topics one pass revisits; snapshots ride the realm config topic, so one is typical.
const MAX_OVERFLOW_TOPICS: usize = 16;
/// Pages one pass replays per topic.
const REPLAY_PAGES: usize = 4;

fn overflow_key(topic_id: ::irokle::TopicId) -> ByteView {
    topic_key(OVERFLOW_PREFIX, topic_id)
}

fn topic_key(prefix: &[u8], topic_id: ::irokle::TopicId) -> ByteView {
    let mut key = prefix.to_vec();
    key.extend_from_slice(topic_id.as_bytes());
    ByteView::from(key)
}

impl DocumentSyncService {
    /// Remembers where a batch that overflowed started; an earlier checkpoint is kept.
    pub(in crate::document_sync) async fn record_overflow(
        &self,
        topic_id: ::irokle::TopicId,
        genesis: ::irokle::OpId,
        start: &::irokle::ActorClock,
    ) -> Result<()> {
        let key = overflow_key(topic_id);
        if self
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), key.clone())
            .await?
            .is_some()
        {
            return Ok(());
        }
        let value = applied_cursor_value(self.node.storage(), topic_id, genesis, start)?;
        self.storage_write(APPLIED_OPS_KEYSPACE.to_string(), key, value)
            .await
    }

    /// Replays distribution snapshots after each overflow checkpoint, a few pages per pass. A
    /// snapshot that still finds parking full is passed over, so later ones that can install
    /// or park are not starved; the next sweep restarts at the first one passed over.
    pub(in crate::document_sync) async fn replay_overflow(&self) -> Result<()> {
        let rows = match self
            .storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: APPLIED_OPS_KEYSPACE.to_string(),
                prefix: Some(ByteView::from(OVERFLOW_PREFIX.to_vec())),
                start: None,
                limit: MAX_OVERFLOW_TOPICS,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(NetError::Dht(format!(
                    "unexpected storage event while listing overflow checkpoints: {other:?}"
                )));
            }
        };
        for (key, value) in rows {
            let topic = key
                .get(OVERFLOW_PREFIX.len()..)
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                .map(::irokle::TopicId::from_bytes);
            match topic {
                Some(topic_id) => self.replay_topic(topic_id, key, value).await?,
                None => self.storage_delete(key).await?,
            }
        }
        Ok(())
    }

    async fn replay_topic(
        &self,
        topic_id: ::irokle::TopicId,
        key: ByteView,
        value: Value,
    ) -> Result<()> {
        let storage = self.node.storage();
        let Some(topic) = storage
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
        else {
            // A topic that is gone takes its sweep rows along; the checkpoint goes last.
            for prefix in [SCAN_PREFIX, FULL_PREFIX] {
                self.storage_delete(topic_key(prefix, topic_id)).await?;
            }
            return self.storage_delete(key).await;
        };
        let genesis = topic.genesis;
        let (scan_key, full_key) = (
            topic_key(SCAN_PREFIX, topic_id),
            topic_key(FULL_PREFIX, topic_id),
        );
        let scan = self
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), scan_key.clone())
            .await?;
        let mut full = self
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), full_key.clone())
            .await?;
        let mut clock = applied_cursor_clock(storage, topic_id, genesis, scan.or(Some(value)))?;
        let applied = self
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), topic_cursor_key(topic_id))
            .await?;
        let main = applied_cursor_clock(storage, topic_id, genesis, applied)?;
        let mut cache = ConfigValidationCache::default();
        for _ in 0..REPLAY_PAGES {
            let batch = match clock.dominates(&main) {
                true => None,
                false => Some(self.document_event_batch(topic_id, &clock, FRAME_LEN_LIMIT)?),
            };
            let Some(batch) = batch.filter(|batch| batch.cursor != clock) else {
                return self.end_sweep(key, scan_key, full_key, full).await;
            };
            for (event, actor_id, _) in batch.events {
                let DocumentEvent::AdminOperation {
                    target,
                    event,
                    placement,
                    origin_signature,
                } = event
                else {
                    continue;
                };
                if !matches!(event.target, AdminDocumentTarget::Distribution { .. }) {
                    continue;
                }
                let validation = validate_admin_event(
                    &self.storage,
                    topic_id,
                    actor_id,
                    &target,
                    &event,
                    self.realm_id,
                    &placement,
                    &origin_signature,
                    &mut cache,
                )
                .await?;
                if validation != AdminEventValidation::Accepted {
                    continue;
                }
                let handled = apply_distribution_operation(&self.storage, target, *event).await?;
                if handled == Handled::Full && full.is_none() {
                    // The next sweep restarts at this page once parked snapshots have drained.
                    let value = applied_cursor_value(storage, topic_id, genesis, &clock)?;
                    self.storage_write(
                        APPLIED_OPS_KEYSPACE.to_string(),
                        full_key.clone(),
                        value.clone(),
                    )
                    .await?;
                    full = Some(value);
                }
            }
            clock = batch.cursor;
        }
        let value = applied_cursor_value(storage, topic_id, genesis, &clock)?;
        self.storage_write(APPLIED_OPS_KEYSPACE.to_string(), scan_key, value)
            .await
    }

    /// Ends a sweep at the topic cursor: the checkpoint moves to the first snapshot that found
    /// parking full, or goes once nothing was passed over.
    async fn end_sweep(
        &self,
        key: ByteView,
        scan_key: ByteView,
        full_key: ByteView,
        full: Option<Value>,
    ) -> Result<()> {
        self.storage_delete(scan_key).await?;
        match full {
            Some(value) => {
                self.storage_write(APPLIED_OPS_KEYSPACE.to_string(), key, value)
                    .await?;
                self.storage_delete(full_key).await
            }
            None => self.storage_delete(key).await,
        }
    }

    async fn storage_delete(&self, key: ByteView) -> Result<()> {
        match self
            .storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: APPLIED_OPS_KEYSPACE.to_string(),
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(NetError::Dht(format!(
                "unexpected storage event while deleting an overflow checkpoint: {other:?}"
            ))),
        }
    }
}
