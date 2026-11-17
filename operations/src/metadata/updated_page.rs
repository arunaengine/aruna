//! Pages registry records by update time through the timestamp index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::effects::IterStart;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{parse_updated_key, updated_index_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::call_results::read_values;
use crate::metadata::timestamp_sweep::{records_call, scan_answer, scan_call};

/// One page of registry records ordered by `updated_at_ms`, plus stale index
/// keys the caller may sweep.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdatedRecordsPage {
    pub records: Vec<MetadataRegistryRecord>,
    /// Exclusive cursor to resume after; `None` when the window is exhausted.
    pub next_after: Option<Key>,
    /// Index keys whose record moved to a newer datestamp or was deleted. Safe to
    /// delete in a sweep; skipping them keeps this read path read-only.
    pub stale_keys: Vec<Key>,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdatedPageError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for UpdatedPageError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
}

type PageTurn = Result<Next<UpdatedRecordsPage>, UpdatedPageError>;

/// Enumerates registry records whose `updated_at_ms` is in `[from_ms, until_ms]`,
/// ascending, up to `limit` records. Keys are validated against the current record;
/// stale keys are skipped, never under-listing.
#[derive(Debug, PartialEq)]
pub struct UpdatedPageOperation {
    start: IterStart,
    until_ms: u64,
    limit: usize,
    records: Vec<MetadataRegistryRecord>,
    stale_keys: Vec<Key>,
    /// The candidate keys of the current page with their datestamp, in scan order;
    /// empty while an index page is read.
    candidates: Vec<(Key, u64)>,
    /// Where the scan continues after this page; `None` ends the window.
    after_page: Option<IterStart>,
}

impl UpdatedPageOperation {
    pub fn new(from_ms: u64, until_ms: u64, after: Option<Key>, limit: usize) -> Self {
        let start = match after {
            Some(cursor) => IterStart::After(cursor),
            None => IterStart::At(updated_index_key(from_ms, Ulid::nil())),
        };
        Self {
            start,
            until_ms,
            limit,
            records: Vec::new(),
            stale_keys: Vec::new(),
            candidates: Vec::new(),
            after_page: None,
        }
    }

    fn page(&mut self, next_after: Option<Key>) -> PageTurn {
        Ok(Next::Done(UpdatedRecordsPage {
            records: std::mem::take(&mut self.records),
            next_after,
            stale_keys: std::mem::take(&mut self.stale_keys),
        }))
    }

    fn scanned_page(&mut self, entries: Vec<(Key, Value)>, next: Option<Key>) -> PageTurn {
        let mut documents = Vec::new();
        let mut past_window = false;
        for (key, _) in entries {
            let (updated_at_ms, document_id) = parse_updated_key(key.as_ref())?;
            if updated_at_ms > self.until_ms {
                past_window = true;
                break;
            }
            self.candidates.push((key, updated_at_ms));
            documents.push(document_id);
        }
        self.after_page = match (past_window, next) {
            (false, Some(next)) => Some(IterStart::After(next)),
            _ => None,
        };
        if documents.is_empty() {
            return self.page(None);
        }
        Ok(Next::Call(records_call(documents)))
    }

    fn read_records(&mut self, values: Vec<Option<Value>>) -> PageTurn {
        let candidates = std::mem::take(&mut self.candidates);
        if values.len() != candidates.len() {
            return Err(IoError::Unexpected {
                expected: "one record per index key",
                got: "another count of records",
            }
            .into());
        }
        for ((key, updated_at_ms), value) in candidates.into_iter().zip(values) {
            let record = value
                .map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes))
                .transpose()
                .map_err(ConversionError::from)?;
            match record {
                Some(record) if record.updated_at_ms == updated_at_ms => {
                    self.records.push(record);
                    if self.records.len() >= self.limit {
                        return self.page(Some(key));
                    }
                }
                _ => self.stale_keys.push(key),
            }
        }
        match self.after_page.take() {
            Some(next) => Ok(Next::Call(scan_call(next))),
            None => self.page(None),
        }
    }
}

impl Operation for UpdatedPageOperation {
    type Output = UpdatedRecordsPage;
    type Error = UpdatedPageError;

    fn first(&mut self) -> PageTurn {
        Ok(Next::Call(scan_call(self.start.clone())))
    }

    fn next(&mut self, result: IoResult) -> PageTurn {
        if self.candidates.is_empty() {
            let (rows, next) = scan_answer(result)?;
            return self.scanned_page(rows, next);
        }
        self.read_records(read_values(result)?)
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::metadata::timestamp_sweep::state_machine_tests::{
        document, first_start, page, record_bytes,
    };

    fn record(document_id: Ulid, updated_at_ms: u64) -> MetadataRegistryRecord {
        postcard::from_bytes(&record_bytes(document_id, updated_at_ms)).expect("record decodes")
    }

    fn scanning(operation: UpdatedPageOperation) -> UpdatedPageOperation {
        let mut operation = operation;
        operation.first().unwrap();
        operation
    }

    fn whole_window() -> UpdatedPageOperation {
        scanning(UpdatedPageOperation::new(0, u64::MAX, None, 10))
    }

    #[test]
    fn pages_in_window() {
        // A moved record leaves a stale key, and a key past the window ends the page.
        let (current, moved, late) = (document(1), document(2), document(3));
        let keys = [
            updated_index_key(10, current),
            updated_index_key(20, moved),
            updated_index_key(30, late),
        ];
        let mut operation = UpdatedPageOperation::new(0, 25, None, 10);
        assert_eq!(operation.first(), Ok(Next::Call(scan_call(first_start()))));
        assert_eq!(
            operation.next(page(&keys, None)),
            Ok(Next::Call(records_call([current, moved])))
        );
        let values = vec![
            Some(record_bytes(current, 10)),
            Some(record_bytes(moved, 99)),
        ];
        assert_eq!(
            operation.next(IoResult::Values(values)),
            Ok(Next::Done(UpdatedRecordsPage {
                records: vec![record(current, 10)],
                next_after: None,
                stale_keys: vec![keys[1].clone()],
            }))
        );
    }

    #[test]
    fn limit_keeps_cursor() {
        // A full page stops at the last listed key, after the given cursor.
        let keys = [
            updated_index_key(10, document(1)),
            updated_index_key(11, document(2)),
            updated_index_key(12, document(3)),
        ];
        let mut operation = UpdatedPageOperation::new(0, u64::MAX, Some(keys[0].clone()), 1);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(scan_call(IterStart::After(keys[0].clone()))))
        );
        assert_eq!(
            operation.next(page(&keys[1..], Some(keys[2].clone()))),
            Ok(Next::Call(records_call([document(2), document(3)])))
        );
        let values = vec![
            Some(record_bytes(document(2), 11)),
            Some(record_bytes(document(3), 12)),
        ];
        assert_eq!(
            operation.next(IoResult::Values(values)),
            Ok(Next::Done(UpdatedRecordsPage {
                records: vec![record(document(2), 11)],
                next_after: Some(keys[1].clone()),
                stale_keys: Vec::new(),
            }))
        );
    }

    #[test]
    fn scans_next_page() {
        // A short page continues the scan until the index ends.
        let key = updated_index_key(10, document(1));
        let mut operation = whole_window();
        assert_eq!(
            operation.next(page(std::slice::from_ref(&key), Some(key.clone()))),
            Ok(Next::Call(records_call([document(1)])))
        );
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Call(scan_call(IterStart::After(key.clone()))))
        );
        assert_eq!(
            operation.next(page(&[], None)),
            Ok(Next::Done(UpdatedRecordsPage {
                records: Vec::new(),
                next_after: None,
                stale_keys: vec![key],
            }))
        );
    }

    #[test]
    fn broken_rows_fail() {
        assert!(matches!(
            whole_window().next(page(&[vec![1u8].into()], None)),
            Err(UpdatedPageError::Conversion(_))
        ));
        let key = updated_index_key(10, document(1));
        let mut bad_record = whole_window();
        bad_record
            .next(page(std::slice::from_ref(&key), None))
            .unwrap();
        assert!(matches!(
            bad_record.next(IoResult::Values(vec![Some(vec![0xff].into())])),
            Err(UpdatedPageError::Conversion(_))
        ));
        let mut short = whole_window();
        short.next(page(std::slice::from_ref(&key), None)).unwrap();
        assert!(matches!(
            short.next(IoResult::Values(Vec::new())),
            Err(UpdatedPageError::Io(IoError::Unexpected { .. }))
        ));
    }

    #[test]
    fn errors_map() {
        let error = IoResult::Event(aruna_core::events::Event::Storage(
            aruna_core::events::StorageEvent::Error {
                error: StorageError::Timeout,
            },
        ));
        assert_eq!(
            whole_window().next(error),
            Err(UpdatedPageError::Storage(StorageError::Timeout))
        );
        assert!(matches!(
            whole_window().next(IoResult::Done),
            Err(UpdatedPageError::Io(IoError::Unexpected { .. }))
        ));
    }
}
