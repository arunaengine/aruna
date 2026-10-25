//! Storage reads and writes of the range index feeder: range lookup, bounded walks, state
//! rows and the guarded deletion of applied feed rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{RANGE_INDEX_KEYSPACE, SEARCH_FEED_KEYSPACE};
use aruna_core::structs::storage::distribution::accounting::{RangeSlot, slot_seek};
use aruna_core::types::{Key, TxnId, Value};
use aruna_storage::storage::StorageHandle;
use ulid::Ulid;

use super::index_feed::FeedError;

/// The open end of a range in its range index key.
const OPEN_END: [u8; 1] = [0xFF];

/// A located range: set, bounds and id, read from the range index.
pub(super) struct Located {
    pub(super) set_id: Ulid,
    pub(super) lower: String,
    pub(super) upper: Option<String>,
    pub(super) range_id: Ulid,
}

impl Located {
    pub(super) fn contains(&self, set_id: Ulid, key: &str) -> bool {
        self.set_id == set_id
            && self.lower.as_str() <= key
            && self.upper.as_deref().is_none_or(|upper| key < upper)
    }
}

/// The installed range of the set that contains `key`, as the inventory update finds it.
pub(super) async fn locate(
    storage: &StorageHandle,
    set_id: Ulid,
    key: &str,
) -> Result<Option<Located>, FeedError> {
    let effect = StorageEffect::Iter {
        key_space: RANGE_INDEX_KEYSPACE.to_string(),
        prefix: Some(set_id.to_bytes().to_vec().into()),
        start: Some(IterStart::After(slot_seek(set_id, key).into())),
        limit: 1,
        txn_id: None,
    };
    let values = match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        event => return Err(storage_error(event)),
    };
    let Some((slot_key, value)) = values.first() else {
        return Ok(None);
    };
    let slot = RangeSlot::from_bytes(value)?;
    if slot.lower.as_str() > key {
        return Ok(None);
    }
    let upper = match &slot_key[16..] {
        bytes if bytes == OPEN_END => None,
        bytes => Some(String::from_utf8(bytes.to_vec()).map_err(ConversionError::from)?),
    };
    Ok(Some(Located {
        set_id,
        lower: slot.lower,
        upper,
        range_id: slot.range_id,
    }))
}

/// Deletes the feed rows whose value is still the one applied, in one transaction; a
/// conflicting writer leaves them for the next pass.
pub(super) async fn drop_settled(
    storage: &StorageHandle,
    settled: Vec<(Key, Value)>,
) -> Result<usize, FeedError> {
    if settled.is_empty() {
        return Ok(0);
    }
    let txn_id = match (storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false }))
    .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        event => return Err(storage_error(event)),
    };
    let reads = (settled.iter())
        .map(|(key, _)| (SEARCH_FEED_KEYSPACE.to_string(), key.clone()))
        .collect();
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: Some(txn_id),
    };
    let current = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        event => {
            abort(storage, txn_id).await;
            return Err(storage_error(event));
        }
    };
    let deletes: Vec<(String, Key)> = (settled.into_iter().zip(current))
        .filter(|((_, seen), (_, now))| now.as_ref().is_some_and(|now| now == seen))
        .map(|((key, _), _)| (SEARCH_FEED_KEYSPACE.to_string(), key))
        .collect();
    let dropped = deletes.len();
    if dropped > 0 {
        let delete = StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        };
        if let event @ Event::Storage(StorageEvent::Error { .. }) =
            storage.send_storage_effect(delete).await
        {
            abort(storage, txn_id).await;
            return Err(storage_error(event));
        }
    }
    match (storage.send_storage_effect(StorageEffect::CommitTransaction { txn_id })).await {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(dropped),
        _ => Ok(0),
    }
}

pub(super) async fn abort(storage: &StorageHandle, txn_id: TxnId) {
    storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

pub(super) async fn iter(
    storage: &StorageHandle,
    key_space: &str,
    after: &Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(Key, Value)>, Option<Vec<u8>>), FeedError> {
    let effect = StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: None,
        start: after.clone().map(|key| IterStart::After(key.into())),
        limit,
        txn_id: None,
    };
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after.map(|key| key.to_vec()))),
        event => Err(storage_error(event)),
    }
}

pub(super) async fn read_many(
    storage: &StorageHandle,
    key_space: &str,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Value>>, FeedError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let count = keys.len();
    let reads = (keys.into_iter())
        .map(|key| (key_space.to_string(), Key::from(key)))
        .collect();
    let effect = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == count => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        event => Err(storage_error(event)),
    }
}

pub(super) async fn put_row(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), FeedError> {
    let effect = StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        event => Err(storage_error(event)),
    }
}

pub(super) fn storage_error(event: Event) -> FeedError {
    match event {
        Event::Storage(StorageEvent::Error { error }) => FeedError::Storage(error.to_string()),
        event => FeedError::Storage(format!("unexpected storage event: {event:?}")),
    }
}
