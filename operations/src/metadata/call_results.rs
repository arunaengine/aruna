//! Unpacks the call results metadata operations expect, refusing any other result kind.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::{Key, Value};

/// The values of a read call, one per requested key. A batch read effect, used inside a
/// caller's transaction, answers the same way.
pub(crate) fn read_values(result: IoResult) -> Result<Vec<Option<Value>>, IoError> {
    match result {
        IoResult::Values(values) => Ok(values),
        IoResult::Event(event) => match *event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => {
                Ok(values.into_iter().map(|(_, value)| value).collect())
            }
            other => Err(storage_failure("StorageEvent::BatchReadResult", other)),
        },
        other => Err(wrong_result("IoResult::Values", &other)),
    }
}

/// The value of a read call for exactly one key.
pub(crate) fn read_value(result: IoResult) -> Result<Option<Value>, IoError> {
    let [value] = <[_; 1]>::try_from(read_values(result)?).map_err(|_| IoError::Unexpected {
        expected: "one value",
        got: "another count of values",
    })?;
    Ok(value)
}

/// The rows of a page call and the key to continue after. An iteration effect, used to start
/// at a key or inside a caller's transaction, answers the same way.
pub(crate) fn page_rows(result: IoResult) -> Result<(Vec<(Key, Value)>, Option<Key>), IoError> {
    match result {
        IoResult::Page { rows, next } => Ok((rows, next)),
        IoResult::Event(event) => match *event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => Ok((values, next_start_after)),
            other => Err(storage_failure("StorageEvent::IterResult", other)),
        },
        other => Err(wrong_result("IoResult::Page", &other)),
    }
}

/// The event that answered an effect call.
pub(crate) fn effect_event(result: IoResult) -> Result<Event, IoError> {
    match result {
        IoResult::Event(event) => Ok(*event),
        other => Err(wrong_result("IoResult::Event", &other)),
    }
}

/// The error a storage effect answered with, as the runner reports it for its own calls.
fn storage_failure(expected: &'static str, event: Event) -> IoError {
    match event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => IoError::Conflict,
        Event::Storage(StorageEvent::Error { error }) => IoError::Storage(error),
        other => IoError::Unexpected {
            expected,
            got: other.label(),
        },
    }
}

fn wrong_result(expected: &'static str, got: &IoResult) -> IoError {
    IoError::Unexpected {
        expected,
        got: got.label(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpacks_expected_results() {
        let value: Value = vec![1].into();
        assert_eq!(
            read_value(IoResult::Values(vec![Some(value.clone())])),
            Ok(Some(value))
        );
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(page_rows(page), Ok((Vec::new(), None)));
    }

    #[test]
    fn refuses_other_results() {
        assert!(matches!(
            read_values(IoResult::Done),
            Err(IoError::Unexpected {
                got: "IoResult::Done",
                ..
            })
        ));
        assert!(read_value(IoResult::Values(Vec::new())).is_err());
        assert!(read_value(IoResult::Values(vec![None, None])).is_err());
        assert!(page_rows(IoResult::Woke).is_err());
        assert!(effect_event(IoResult::Done).is_err());
    }

    fn storage(event: StorageEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Storage(event)))
    }

    #[test]
    fn unpacks_effect_answers() {
        let key: Key = vec![1].into();
        let value: Value = vec![2].into();
        let batch = StorageEvent::BatchReadResult {
            values: vec![(key.clone(), Some(value.clone()))],
        };
        assert_eq!(read_value(storage(batch)), Ok(Some(value.clone())));
        let iter = StorageEvent::IterResult {
            values: vec![(key.clone(), value.clone())],
            next_start_after: Some(key.clone()),
        };
        assert_eq!(
            page_rows(storage(iter)),
            Ok((vec![(key.clone(), value)], Some(key)))
        );
    }

    #[test]
    fn storage_errors_map() {
        let conflict = StorageEvent::Error {
            error: StorageError::TransactionConflict,
        };
        assert_eq!(page_rows(storage(conflict)), Err(IoError::Conflict));
        let timeout = StorageEvent::Error {
            error: StorageError::Timeout,
        };
        assert_eq!(
            read_values(storage(timeout)),
            Err(IoError::Storage(StorageError::Timeout))
        );
        assert!(matches!(
            page_rows(storage(StorageEvent::SyncAllFinished)),
            Err(IoError::Unexpected { .. })
        ));
    }
}
