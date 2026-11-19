//! Sweeps timestamp index keys whose record moved to a newer datestamp or was deleted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{DOCUMENT_INDEX_KEYSPACE, UPDATED_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::done;
use aruna_core::storage_entries::{metadata_document_key, parse_updated_key, updated_index_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::call_results::{page_rows, read_values};

/// Storage rows read per index page.
pub(crate) const INDEX_SCAN_BATCH: usize = 256;
/// Index keys validated by one default sweep pass.
pub const SWEEP_SCAN_LIMIT: usize = 4_096;
/// Stale keys per delete batch.
const SWEEP_DELETE_BATCH: usize = 128;

/// One page of the timestamp index from `start`. The scan may start at a key, which a
/// page call cannot express, so it goes out as an effect.
pub(crate) fn scan_call(start: IterStart) -> IoCall {
    IoCall::Effect(Box::new(Effect::Storage(StorageEffect::Iter {
        key_space: UPDATED_INDEX_KEYSPACE.to_string(),
        prefix: None,
        start: Some(start),
        limit: INDEX_SCAN_BATCH,
        txn_id: None,
    })))
}

/// The registry record reads of `documents`, in order.
pub(crate) fn records_call(documents: impl IntoIterator<Item = Ulid>) -> IoCall {
    let reads = documents
        .into_iter()
        .map(|document_id| {
            (
                DOCUMENT_INDEX_KEYSPACE.to_string(),
                metadata_document_key(document_id),
            )
        })
        .collect();
    IoCall::Read(reads)
}

/// One bounded sweep pass: how many stale keys it deleted and where to resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepPass {
    pub deleted: usize,
    pub next_after: Option<Key>,
}

#[derive(Debug, Error, PartialEq)]
pub enum SweepError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("a timestamp index key or registry record does not decode: {0}")]
    Decode(String),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for SweepError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
}

#[derive(Debug, PartialEq)]
enum SweepState {
    /// Reading one index page.
    Scan,
    /// Reading the records of the scanned keys, in scan order.
    Records,
    /// Deleting one batch of stale keys.
    Delete,
}

type SweepTurn = Result<Next<SweepPass>, SweepError>;

/// Deletes index keys whose record moved to a newer datestamp or was deleted, at most
/// `scan_limit` keys per pass. Racing a concurrent write is benign: the writer re-adds the
/// current key, and readers validate every key against the record regardless.
#[derive(Debug, PartialEq)]
pub struct TimestampSweepOperation {
    start: IterStart,
    scan_limit: usize,
    scanned: usize,
    /// The last scanned key, where the next pass resumes.
    resume: Option<Key>,
    /// The scanned keys of the current page with their datestamp, in scan order.
    page: Vec<(Key, u64)>,
    /// Where the scan continues after this page; `None` ends the pass.
    after_page: Option<IterStart>,
    stale: Vec<Key>,
    /// Full delete batches still to send, then whether the pass ends after them.
    batches: VecDeque<Vec<Key>>,
    finishing: bool,
    deleted: usize,
    state: SweepState,
}

impl TimestampSweepOperation {
    /// A pass that resumes after `after`, or starts at the oldest key.
    pub fn new(after: Option<Key>, scan_limit: usize) -> Self {
        let start = match after {
            Some(cursor) => IterStart::After(cursor),
            None => IterStart::At(updated_index_key(0, Ulid::nil())),
        };
        Self {
            start,
            scan_limit,
            scanned: 0,
            resume: None,
            page: Vec::new(),
            after_page: None,
            stale: Vec::new(),
            batches: VecDeque::new(),
            finishing: false,
            deleted: 0,
            state: SweepState::Scan,
        }
    }

    fn scan(&mut self, start: IterStart) -> SweepTurn {
        self.state = SweepState::Scan;
        Ok(Next::Call(scan_call(start)))
    }

    fn scanned_page(&mut self, entries: Vec<(Key, Value)>, next: Option<Key>) -> SweepTurn {
        // An empty page ends the scan and keeps the last scanned key as the resume point.
        if entries.is_empty() {
            return self.end_pass();
        }
        let mut page = Vec::new();
        let mut budget_spent = false;
        for (key, _) in entries {
            let (updated_at_ms, document_id) = parse_updated_key(key.as_ref())
                .map_err(|error| SweepError::Decode(error.to_string()))?;
            self.scanned += 1;
            self.resume = Some(key.clone());
            page.push((key, updated_at_ms, document_id));
            if self.scanned >= self.scan_limit {
                budget_spent = true;
                break;
            }
        }
        self.after_page = match (budget_spent, next) {
            (true, _) => None,
            (false, Some(next)) => Some(IterStart::After(next)),
            (false, None) => {
                self.resume = None;
                None
            }
        };
        let call = records_call(page.iter().map(|(_, _, document_id)| *document_id));
        self.page = page
            .into_iter()
            .map(|(key, updated_at_ms, _)| (key, updated_at_ms))
            .collect();
        self.state = SweepState::Records;
        Ok(Next::Call(call))
    }

    fn read_records(&mut self, values: Vec<Option<Value>>) -> SweepTurn {
        if values.len() != self.page.len() {
            return Err(SweepError::Decode(format!(
                "{} records answered for {} index keys",
                values.len(),
                self.page.len()
            )));
        }
        for ((key, updated_at_ms), value) in std::mem::take(&mut self.page).into_iter().zip(values)
        {
            let record = value
                .map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes))
                .transpose()
                .map_err(|error| SweepError::Decode(error.to_string()))?;
            if !record.is_some_and(|record| record.updated_at_ms == updated_at_ms) {
                self.stale.push(key);
                if self.stale.len() >= SWEEP_DELETE_BATCH {
                    self.batches.push_back(std::mem::take(&mut self.stale));
                }
            }
        }
        match self.after_page.take() {
            Some(next) => {
                self.finishing = false;
                self.next_batch_or(next)
            }
            None => self.end_pass(),
        }
    }

    /// Sends the next full delete batch, or continues the scan at `next`.
    fn next_batch_or(&mut self, next: IterStart) -> SweepTurn {
        match self.batches.pop_front() {
            Some(batch) => {
                self.after_page = Some(next);
                self.delete(batch)
            }
            None => self.scan(next),
        }
    }

    fn end_pass(&mut self) -> SweepTurn {
        self.finishing = true;
        let rest = std::mem::take(&mut self.stale);
        if !rest.is_empty() {
            self.batches.push_back(rest);
        }
        match self.batches.pop_front() {
            Some(batch) => self.delete(batch),
            None => self.finish(),
        }
    }

    fn delete(&mut self, batch: Vec<Key>) -> SweepTurn {
        self.state = SweepState::Delete;
        self.deleted += batch.len();
        let deletes = batch
            .into_iter()
            .map(|key| (UPDATED_INDEX_KEYSPACE.to_string(), key))
            .collect();
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn deleted_batch(&mut self) -> SweepTurn {
        if let Some(batch) = self.batches.pop_front() {
            return self.delete(batch);
        }
        match (self.finishing, self.after_page.take()) {
            (false, Some(next)) => self.scan(next),
            _ => self.finish(),
        }
    }

    fn finish(&mut self) -> SweepTurn {
        Ok(Next::Done(SweepPass {
            deleted: self.deleted,
            next_after: self.resume.take(),
        }))
    }
}

impl Operation for TimestampSweepOperation {
    type Output = SweepPass;
    type Error = SweepError;

    fn first(&mut self) -> SweepTurn {
        let start = self.start.clone();
        self.scan(start)
    }

    fn next(&mut self, result: IoResult) -> SweepTurn {
        match self.state {
            SweepState::Scan => {
                let (rows, next) = page_rows(result)?;
                self.scanned_page(rows, next)
            }
            SweepState::Records => self.read_records(read_values(result)?),
            SweepState::Delete => {
                done(result)?;
                self.deleted_batch()
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod state_machine_tests {
    use super::*;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;

    pub(crate) fn document(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    pub(crate) fn record_bytes(document_id: Ulid, updated_at_ms: u64) -> Value {
        let record = MetadataRegistryRecord {
            realm_id: RealmId([1; 32]),
            group_id: Ulid::from_bytes([2; 16]),
            document_id,
            document_path: format!("doc/{document_id}"),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: "/p".to_string(),
            placement: PlacementRef {
                strategy_id: Ulid::nil(),
                shard: 0,
            },
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms,
            establishing_event_id: Ulid::from_bytes([8; 16]),
            last_event_id: Ulid::from_bytes([9; 16]),
        };
        postcard::to_allocvec(&record)
            .expect("record encodes")
            .into()
    }

    pub(crate) fn first_start() -> IterStart {
        IterStart::At(updated_index_key(0, Ulid::nil()))
    }

    /// An index page answer with `keys`, values unused.
    pub(crate) fn page(keys: &[Key], next: Option<Key>) -> IoResult {
        IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
            values: keys
                .iter()
                .map(|key| (key.clone(), Vec::<u8>::new().into()))
                .collect(),
            next_start_after: next,
        })))
    }

    fn delete_call(keys: &[Key]) -> SweepTurn {
        let deletes = keys
            .iter()
            .map(|key| (UPDATED_INDEX_KEYSPACE.to_string(), key.clone()))
            .collect();
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn scanning(after: Option<Key>, limit: usize) -> TimestampSweepOperation {
        let mut operation = TimestampSweepOperation::new(after, limit);
        operation.first().unwrap();
        operation
    }

    #[test]
    fn sweeps_stale_keys() {
        // A current key stays; a moved and a deleted record lose their keys.
        let (current, moved, gone) = (document(1), document(2), document(3));
        let keys = [
            updated_index_key(10, current),
            updated_index_key(20, moved),
            updated_index_key(30, gone),
        ];
        let mut operation = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        assert_eq!(operation.first(), Ok(Next::Call(scan_call(first_start()))));
        assert_eq!(
            operation.next(page(&keys, None)),
            Ok(Next::Call(records_call([current, moved, gone])))
        );
        let values = vec![
            Some(record_bytes(current, 10)),
            Some(record_bytes(moved, 99)),
            None,
        ];
        assert_eq!(
            operation.next(IoResult::Values(values)),
            delete_call(&keys[1..])
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done(SweepPass {
                deleted: 2,
                next_after: None,
            }))
        );
    }

    #[test]
    fn budget_keeps_cursor() {
        // A spent scan budget ends the pass at the last scanned key, without a delete.
        let keys = [
            updated_index_key(10, document(1)),
            updated_index_key(11, document(2)),
        ];
        let after = Some(keys[1].clone());
        let mut operation = TimestampSweepOperation::new(Some(keys[0].clone()), 1);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(scan_call(IterStart::After(keys[0].clone()))))
        );
        assert_eq!(
            operation.next(page(&keys[1..], after.clone())),
            Ok(Next::Call(records_call([document(2)])))
        );
        assert_eq!(
            operation.next(IoResult::Values(vec![Some(record_bytes(document(2), 11))])),
            Ok(Next::Done(SweepPass {
                deleted: 0,
                next_after: after,
            }))
        );
    }

    #[test]
    fn full_batch_deletes() {
        // A full batch is deleted before the scan continues with the next page.
        let documents = (0..SWEEP_DELETE_BATCH as u8)
            .map(|seed| document(seed.wrapping_add(10)))
            .collect::<Vec<_>>();
        let keys = documents
            .iter()
            .map(|document_id| updated_index_key(5, *document_id))
            .collect::<Vec<_>>();
        let next = keys.last().cloned();
        let mut operation = scanning(None, SWEEP_SCAN_LIMIT);
        assert_eq!(
            operation.next(page(&keys, next.clone())),
            Ok(Next::Call(records_call(documents.iter().copied())))
        );
        assert_eq!(
            operation.next(IoResult::Values(vec![None; documents.len()])),
            delete_call(&keys)
        );
        let after = IterStart::After(next.clone().expect("cursor"));
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(scan_call(after)))
        );
        assert_eq!(
            operation.next(page(&[], None)),
            Ok(Next::Done(SweepPass {
                deleted: SWEEP_DELETE_BATCH,
                next_after: next,
            }))
        );
    }

    #[test]
    fn storage_errors_fail() {
        let error = IoResult::Event(Box::new(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })));
        assert_eq!(
            scanning(None, SWEEP_SCAN_LIMIT).next(error),
            Err(SweepError::Storage(StorageError::Timeout))
        );
        assert_eq!(
            SweepError::from(IoError::Conflict),
            SweepError::Storage(StorageError::TransactionConflict)
        );
    }

    #[test]
    fn broken_rows_fail() {
        let bad_key = scanning(None, SWEEP_SCAN_LIMIT).next(page(&[vec![1u8].into()], None));
        assert!(matches!(bad_key, Err(SweepError::Decode(_))));

        let key = updated_index_key(10, document(1));
        let mut bad_record = scanning(None, SWEEP_SCAN_LIMIT);
        bad_record
            .next(page(std::slice::from_ref(&key), None))
            .unwrap();
        assert!(matches!(
            bad_record.next(IoResult::Values(vec![Some(vec![0xff].into())])),
            Err(SweepError::Decode(_))
        ));

        let mut short = scanning(None, SWEEP_SCAN_LIMIT);
        short.next(page(std::slice::from_ref(&key), None)).unwrap();
        assert!(matches!(
            short.next(IoResult::Values(Vec::new())),
            Err(SweepError::Decode(_))
        ));
    }

    #[test]
    fn wrong_result_fails() {
        assert!(matches!(
            scanning(None, SWEEP_SCAN_LIMIT).next(IoResult::Done),
            Err(SweepError::Io(IoError::Unexpected { .. }))
        ));
    }
}

#[cfg(test)]
#[path = "timestamp_index_tests.rs"]
mod index_tests;
