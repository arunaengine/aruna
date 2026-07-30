pub mod create;
pub mod delete;
pub mod query;
pub mod replace;
pub mod validation;

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::Key;
use thiserror::Error;
use ulid::Ulid;

/// Records are keyed by backend id alone so the blob adapter can resolve a
/// stored `BackendRef::Group` without knowing the owning group.
pub fn backend_key(backend_id: Ulid) -> Key {
    backend_id.to_bytes().to_vec().into()
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
