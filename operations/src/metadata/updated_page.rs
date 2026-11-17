//! Pages registry records by update time through the timestamp index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_INDEX_KEYSPACE, UPDATED_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{metadata_document_key, parse_updated_key, updated_index_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::timestamp_sweep::INDEX_SCAN_BATCH;

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
    #[error("unexpected event while {state}: got {got}")]
    UnexpectedEvent {
        state: &'static str,
        got: &'static str,
    },
    #[error("the updated records page did not finish")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum PageState {
    Init,
    Scan,
    Records,
    Done,
}

impl PageState {
    fn name(&self) -> &'static str {
        match self {
            Self::Init => "starting",
            Self::Scan => "reading an index page",
            Self::Records => "reading registry records",
            Self::Done => "finished",
        }
    }
}

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
    /// The candidate keys of the current page with their datestamp, in scan order.
    candidates: Vec<(Key, u64)>,
    /// Where the scan continues after this page; `None` ends the window.
    after_page: Option<IterStart>,
    state: PageState,
    output: Option<Result<UpdatedRecordsPage, UpdatedPageError>>,
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
            state: PageState::Init,
            output: None,
        }
    }

    fn scan(&mut self, start: IterStart) -> Effects {
        self.state = PageState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: UPDATED_INDEX_KEYSPACE.to_string(),
            prefix: None,
            start: Some(start),
            limit: INDEX_SCAN_BATCH,
            txn_id: None,
        })]
    }

    fn finish(&mut self, output: Result<UpdatedRecordsPage, UpdatedPageError>) -> Effects {
        self.state = PageState::Done;
        self.output = Some(output);
        smallvec![]
    }

    fn page(&mut self, next_after: Option<Key>) -> Effects {
        let page = UpdatedRecordsPage {
            records: std::mem::take(&mut self.records),
            next_after,
            stale_keys: std::mem::take(&mut self.stale_keys),
        };
        self.finish(Ok(page))
    }

    fn scanned_page(&mut self, entries: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        if entries.is_empty() {
            return self.page(None);
        }
        let mut documents = Vec::new();
        let mut past_window = false;
        for (key, _) in entries {
            let (updated_at_ms, document_id) = match parse_updated_key(key.as_ref()) {
                Ok(parsed) => parsed,
                Err(error) => return self.finish(Err(error.into())),
            };
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
        let reads = documents
            .into_iter()
            .map(|document_id| {
                (
                    DOCUMENT_INDEX_KEYSPACE.to_string(),
                    metadata_document_key(document_id),
                )
            })
            .collect();
        self.state = PageState::Records;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn read_records(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let candidates = std::mem::take(&mut self.candidates);
        if values.len() != candidates.len() {
            return self.finish(Err(UpdatedPageError::UnexpectedEvent {
                state: self.state.name(),
                got: "StorageEvent::BatchReadResult with a different length",
            }));
        }
        for ((key, updated_at_ms), (_, value)) in candidates.into_iter().zip(values) {
            let record = match value
                .map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes))
            {
                Some(Ok(record)) => Some(record),
                Some(Err(error)) => return self.finish(Err(ConversionError::from(error).into())),
                None => None,
            };
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
            Some(next) => self.scan(next),
            None => self.page(None),
        }
    }
}

impl Operation for UpdatedPageOperation {
    type Output = UpdatedRecordsPage;
    type Error = UpdatedPageError;

    fn start(&mut self) -> Effects {
        let start = self.start.clone();
        self.scan(start)
    }

    fn step(&mut self, event: Event) -> Effects {
        let reading = matches!(self.state, PageState::Scan | PageState::Records);
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.state == PageState::Scan => self.scanned_page(values, next_start_after),
            Event::Storage(StorageEvent::BatchReadResult { values })
                if self.state == PageState::Records =>
            {
                self.read_records(values)
            }
            Event::Storage(StorageEvent::Error { error }) if reading => {
                self.finish(Err(error.into()))
            }
            event => {
                let error = UpdatedPageError::UnexpectedEvent {
                    state: self.state.name(),
                    got: event.label(),
                };
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == PageState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(UpdatedPageError::Unfinished))
    }

    // Every read runs outside a transaction, so nothing is left to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::metadata::timestamp_sweep::state_machine_tests::{
        document, first_start, page, reads_effect, record_bytes, records, scan_effect,
    };
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn record(document_id: Ulid, updated_at_ms: u64) -> MetadataRegistryRecord {
        postcard::from_bytes(&record_bytes(document_id, updated_at_ms)).expect("record decodes")
    }

    fn whole_window() -> UpdatedPageOperation {
        UpdatedPageOperation::new(0, u64::MAX, None, 10)
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
        check_trace(
            &mut operation,
            &[scan_effect(first_start())],
            vec![
                (page(&keys, None), vec![reads_effect(&[current, moved])]),
                (
                    records(vec![
                        Some(record_bytes(current, 10)),
                        Some(record_bytes(moved, 99)),
                    ]),
                    vec![],
                ),
            ],
        );
        assert_eq!(
            operation.finalize(),
            Ok(UpdatedRecordsPage {
                records: vec![record(current, 10)],
                next_after: None,
                stale_keys: vec![keys[1].clone()],
            })
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
        check_trace(
            &mut operation,
            &[scan_effect(IterStart::After(keys[0].clone()))],
            vec![
                (
                    page(&keys[1..], Some(keys[2].clone())),
                    vec![reads_effect(&[document(2), document(3)])],
                ),
                (
                    records(vec![
                        Some(record_bytes(document(2), 11)),
                        Some(record_bytes(document(3), 12)),
                    ]),
                    vec![],
                ),
            ],
        );
        assert_eq!(
            operation.finalize(),
            Ok(UpdatedRecordsPage {
                records: vec![record(document(2), 11)],
                next_after: Some(keys[1].clone()),
                stale_keys: Vec::new(),
            })
        );
    }

    #[test]
    fn scans_next_page() {
        // A short page continues the scan until the index ends.
        let key = updated_index_key(10, document(1));
        let mut operation = whole_window();
        check_trace(
            &mut operation,
            &[scan_effect(first_start())],
            vec![
                (
                    page(std::slice::from_ref(&key), Some(key.clone())),
                    vec![reads_effect(&[document(1)])],
                ),
                (
                    records(vec![None]),
                    vec![scan_effect(IterStart::After(key.clone()))],
                ),
                (page(&[], None), vec![]),
            ],
        );
        assert_eq!(
            operation.finalize(),
            Ok(UpdatedRecordsPage {
                records: Vec::new(),
                next_after: None,
                stale_keys: vec![key],
            })
        );
    }

    #[test]
    fn storage_errors_fail() {
        let key = updated_index_key(10, document(1));
        let error = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        let mut at_scan = whole_window();
        at_scan.start();
        let mut at_reads = whole_window();
        at_reads.start();
        at_reads.step(page(std::slice::from_ref(&key), None));
        for mut operation in [at_scan, at_reads] {
            assert!(operation.step(error()).is_empty());
            assert_eq!(
                operation.finalize(),
                Err(UpdatedPageError::Storage(StorageError::Timeout))
            );
        }
    }

    #[test]
    fn broken_rows_fail() {
        let mut bad_key = whole_window();
        bad_key.start();
        bad_key.step(page(&[vec![1u8].into()], None));
        assert!(matches!(
            bad_key.finalize(),
            Err(UpdatedPageError::Conversion(_))
        ));

        let key = updated_index_key(10, document(1));
        let mut bad_record = whole_window();
        bad_record.start();
        bad_record.step(page(std::slice::from_ref(&key), None));
        bad_record.step(records(vec![Some(vec![0xff].into())]));
        assert!(matches!(
            bad_record.finalize(),
            Err(UpdatedPageError::Conversion(_))
        ));

        let mut short = whole_window();
        short.start();
        short.step(page(std::slice::from_ref(&key), None));
        short.step(records(Vec::new()));
        assert!(matches!(
            short.finalize(),
            Err(UpdatedPageError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn rejects_other_events() {
        let scanning = || {
            let mut operation = whole_window();
            operation.start();
            operation
        };
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (_, effects, error) in rejects_events(scanning, sample_events, accepted) {
            assert!(effects.is_empty());
            assert!(matches!(error, UpdatedPageError::UnexpectedEvent { .. }));
        }
        let mut idle = whole_window();
        idle.step(page(&[], None));
        assert!(matches!(
            idle.finalize(),
            Err(UpdatedPageError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = whole_window();
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(whole_window());
        let mut scanning = whole_window();
        scanning.start();
        rejects_finalize(scanning);
    }
}
