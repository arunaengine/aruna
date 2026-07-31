pub mod create;
pub mod disable;
pub mod query;
pub mod replace;
pub mod validation;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{GROUP_STORAGE_BACKEND_INDEX_KEYSPACE, GROUP_STORAGE_BACKEND_KEYSPACE};
use aruna_core::structs::{BackendRef, GroupStorageBackend};
use aruna_core::types::{GroupId, Key, TxnId};
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

/// Records are keyed by backend id alone so the blob adapter can resolve a
/// stored `BackendRef::Group` without knowing the owning group.
pub fn backend_key(backend_id: Ulid) -> Key {
    backend_id.to_bytes().to_vec().into()
}

/// Index key of the same record. The group prefix is what lets routing scan one
/// group instead of the whole keyspace.
pub fn index_key(group_id: GroupId, backend_id: Ulid) -> Key {
    let mut key = group_id.to_bytes().to_vec();
    key.extend_from_slice(&backend_id.to_bytes());
    key.into()
}

pub fn index_prefix(group_id: GroupId) -> Key {
    group_id.to_bytes().to_vec().into()
}

/// Both copies of one record. Callers must apply them in a single batch or
/// transaction so the index can never disagree with the id-keyed record.
pub fn record_writes(
    record: &GroupStorageBackend,
) -> Result<Vec<(String, Key, ByteView)>, ConversionError> {
    let value: ByteView = record.to_bytes()?.into();
    Ok(vec![
        (
            GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
            backend_key(record.backend_id),
            value.clone(),
        ),
        (
            GROUP_STORAGE_BACKEND_INDEX_KEYSPACE.to_string(),
            index_key(record.group_id, record.backend_id),
            value,
        ),
    ])
}

#[derive(Debug, Error, PartialEq)]
pub enum BackendFenceError {
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("storage backend is not available for writes")]
    Unavailable,
}

/// Joins a tenant backend's record to the transaction that commits a reference
/// to it, so a writer that already resolved a backend the tenant has since
/// disabled either reads the flag or loses its commit.
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
        Some(record) if !record.disabled => Ok(()),
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
