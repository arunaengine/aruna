pub mod create;
pub mod delete;
pub mod query;
pub mod replace;
pub mod validation;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::GROUP_STORAGE_BACKEND_KEYSPACE;
use aruna_core::structs::{BackendRef, GroupStorageBackend};
use aruna_core::types::{Key, TxnId};
use thiserror::Error;
use ulid::Ulid;

/// Records are keyed by backend id alone so the blob adapter can resolve a
/// stored `BackendRef::Group` without knowing the owning group.
pub fn backend_key(backend_id: Ulid) -> Key {
    backend_id.to_bytes().to_vec().into()
}

#[derive(Debug, Error, PartialEq)]
pub enum BackendFenceError {
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("storage backend is not available for writes")]
    Unavailable,
}

/// Joins a tenant backend's record to the transaction that commits a reference
/// to it. Deletion retires the record before it scans, so a writer that already
/// resolved the backend either reads the retirement or loses its commit.
pub fn fence_backend(backend: &BackendRef, txn_id: Option<TxnId>) -> Option<Effect> {
    let BackendRef::Group(backend_id) = backend else {
        return None;
    };
    Some(Effect::Storage(StorageEffect::Read {
        key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
        key: backend_key(*backend_id),
        txn_id,
    }))
}

pub fn check_fence(event: Event) -> Result<(), BackendFenceError> {
    match parse_read(event, GroupStorageBackend::from_bytes)? {
        Some(record) if !record.retiring => Ok(()),
        _ => Err(BackendFenceError::Unavailable),
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum RecordReadError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected storage event")]
    Unexpected,
}

pub fn parse_read<T>(
    event: Event,
    decode: impl Fn(&[u8]) -> Result<T, ConversionError>,
) -> Result<Option<T>, RecordReadError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| decode(value.as_ref()))
            .transpose()
            .map_err(RecordReadError::Conversion),
        Event::Storage(StorageEvent::Error { error }) => Err(RecordReadError::Storage(error)),
        _ => Err(RecordReadError::Unexpected),
    }
}

/// Like [`parse_iter`], but hands the stored key to the decoder as well, so a
/// scan can answer from the key alone.
pub fn parse_pairs<T>(
    event: Event,
    decode: impl Fn(&Key, &[u8]) -> Result<T, ConversionError>,
) -> Result<(Vec<T>, Option<Key>), RecordReadError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            let records = values
                .iter()
                .map(|(key, value)| {
                    decode(key, value.as_ref()).map_err(RecordReadError::Conversion)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((records, next_start_after))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(RecordReadError::Storage(error)),
        _ => Err(RecordReadError::Unexpected),
    }
}

pub fn parse_iter<T>(
    event: Event,
    decode: impl Fn(&[u8]) -> Result<T, ConversionError>,
) -> Result<(Vec<T>, Option<Key>), RecordReadError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            let records = values
                .into_iter()
                .map(|(_, value)| decode(value.as_ref()).map_err(RecordReadError::Conversion))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((records, next_start_after))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(RecordReadError::Storage(error)),
        _ => Err(RecordReadError::Unexpected),
    }
}
