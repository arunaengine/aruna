//! Parses storage read and iterate events and reads a keyspace range page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError, StorageReadError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::{Key, TxnId, Value};
use aruna_storage::StorageHandle;

/// Decodes a read row, if any.
pub(crate) fn decode_row<T>(
    value: Option<Value>,
    parse: impl FnOnce(&[u8]) -> Result<T, ConversionError>,
) -> Result<Option<T>, StorageReadError> {
    (value.as_deref().map(parse).transpose()).map_err(StorageReadError::Conversion)
}

pub(crate) type StorageScan = (Vec<(Key, Value)>, Option<Key>);

pub(crate) fn parse_storage_read<T>(
    event: Event,
    parse: impl FnOnce(&[u8]) -> Result<T, ConversionError>,
) -> Result<Option<T>, StorageReadError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| parse(bytes.as_ref()).map_err(StorageReadError::Conversion))
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::ReadError(
            "unexpected event".to_string(),
        ))),
    }
}

pub(crate) fn parse_storage_iter<T>(
    event: Event,
    parse: impl Fn(&[u8]) -> Result<T, ConversionError>,
) -> Result<(Vec<T>, Option<Key>), StorageReadError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            let records = values
                .into_iter()
                .map(|(_, value)| parse(value.as_ref()).map_err(StorageReadError::Conversion))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((records, next_start_after))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::ReadError(
            "unexpected event".to_string(),
        ))),
    }
}

pub(crate) fn parse_storage_scan(event: Event) -> Result<StorageScan, StorageReadError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::ReadError(
            "unexpected event".to_string(),
        ))),
    }
}

/// One page of `key_space` under `prefix` after `start_after`, and the key the next page
/// starts after.
pub async fn iter_prefix_page(
    storage: &StorageHandle,
    key_space: &str,
    prefix: Option<Key>,
    start_after: Option<Key>,
    limit: usize,
    txn_id: Option<TxnId>,
) -> Result<(Vec<(Key, Value)>, Option<Key>), String> {
    let iter = StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix,
        start: start_after.map(IterStart::After),
        limit,
        txn_id,
    };
    parse_storage_scan(storage.send_storage_effect(iter).await).map_err(|error| error.to_string())
}
