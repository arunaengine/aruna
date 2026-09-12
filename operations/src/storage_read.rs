use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::types::{Key, Value};
use aruna_storage::StorageHandle;
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

pub(crate) async fn scan_all(
    storage: &StorageHandle,
    key_space: &str,
    prefix: Option<Key>,
) -> Result<Vec<(Key, Value)>, String> {
    let mut collected = Vec::new();
    let mut start = None;
    loop {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: prefix.clone(),
                start: start.map(IterStart::After),
                limit: 1_000,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                collected.extend(values);
                match next_start_after {
                    Some(next) => start = Some(next),
                    None => break,
                }
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected iter event: {other:?}")),
        }
    }
    Ok(collected)
}
