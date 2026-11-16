//! Parses storage read and iterate events and reads a keyspace range page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::{Key, Value};
use aruna_storage::StorageHandle;
use futures_util::{Stream, stream};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum StorageReadError {
    #[error(transparent)]
    Storage(StorageError),
    #[error(transparent)]
    Conversion(ConversionError),
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

/// Reads a keyspace range page by page, so a caller handles each page before the next read.
pub(crate) fn scan_pages<'a>(
    storage: &'a StorageHandle,
    key_space: &'a str,
    prefix: Option<Key>,
) -> impl Stream<Item = Result<Vec<(Key, Value)>, String>> + 'a {
    stream::try_unfold(Some(None), move |start: Option<Option<Key>>| {
        let prefix = prefix.clone();
        async move {
            let Some(start) = start else {
                return Ok(None);
            };
            let iter = StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix,
                start: start.map(IterStart::After),
                limit: 1_000,
                txn_id: None,
            };
            match storage.send_storage_effect(iter).await {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => Ok(Some((values, next_start_after.map(Some)))),
                Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
                other => Err(format!("unexpected iter event: {other:?}")),
            }
        }
    })
}
