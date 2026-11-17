//! Sweeps timestamp index keys whose record moved to a newer datestamp or was deleted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_INDEX_KEYSPACE, UPDATED_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{metadata_document_key, parse_updated_key, updated_index_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// Storage rows read per index page.
const INDEX_SCAN_BATCH: usize = 256;
/// Index keys validated by one default sweep pass.
pub const SWEEP_SCAN_LIMIT: usize = 4_096;
/// Stale keys per delete batch.
const SWEEP_DELETE_BATCH: usize = 128;

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
    #[error("unexpected event while {state}: got {got}")]
    UnexpectedEvent {
        state: &'static str,
        got: &'static str,
    },
    #[error("the timestamp index sweep did not finish")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum SweepState {
    Init,
    /// Reading one index page.
    Scan,
    /// Reading the records of the scanned keys, in scan order.
    Records,
    /// Deleting one batch of stale keys.
    Delete,
    Done,
}

impl SweepState {
    fn name(&self) -> &'static str {
        match self {
            Self::Init => "starting",
            Self::Scan => "reading an index page",
            Self::Records => "reading registry records",
            Self::Delete => "deleting stale keys",
            Self::Done => "finished",
        }
    }
}

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
    output: Option<Result<SweepPass, SweepError>>,
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
            state: SweepState::Init,
            output: None,
        }
    }

    fn scan(&mut self, start: IterStart) -> Effects {
        self.state = SweepState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: UPDATED_INDEX_KEYSPACE.to_string(),
            prefix: None,
            start: Some(start),
            limit: INDEX_SCAN_BATCH,
            txn_id: None,
        })]
    }

    fn fail(&mut self, error: SweepError) -> Effects {
        self.state = SweepState::Done;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn scanned_page(&mut self, entries: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        // An empty page ends the scan and keeps the last scanned key as the resume point.
        if entries.is_empty() {
            return self.end_pass();
        }
        let mut page = Vec::new();
        let mut budget_spent = false;
        for (key, _) in entries {
            let (updated_at_ms, document_id) = match parse_updated_key(key.as_ref()) {
                Ok(parsed) => parsed,
                Err(error) => return self.fail(SweepError::Decode(error.to_string())),
            };
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
        let reads = page
            .iter()
            .map(|(_, _, document_id)| {
                (
                    DOCUMENT_INDEX_KEYSPACE.to_string(),
                    metadata_document_key(*document_id),
                )
            })
            .collect();
        self.page = page
            .into_iter()
            .map(|(key, updated_at_ms, _)| (key, updated_at_ms))
            .collect();
        self.state = SweepState::Records;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn read_records(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        if values.len() != self.page.len() {
            return self.fail(SweepError::Decode(format!(
                "{} records answered for {} index keys",
                values.len(),
                self.page.len()
            )));
        }
        for ((key, updated_at_ms), (_, value)) in
            std::mem::take(&mut self.page).into_iter().zip(values)
        {
            let record =
                match value.map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes)) {
                    Some(Ok(record)) => Some(record),
                    Some(Err(error)) => return self.fail(SweepError::Decode(error.to_string())),
                    None => None,
                };
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
    fn next_batch_or(&mut self, next: IterStart) -> Effects {
        match self.batches.pop_front() {
            Some(batch) => {
                self.after_page = Some(next);
                self.delete(batch)
            }
            None => self.scan(next),
        }
    }

    fn end_pass(&mut self) -> Effects {
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

    fn delete(&mut self, batch: Vec<Key>) -> Effects {
        self.state = SweepState::Delete;
        self.deleted += batch.len();
        let deletes = batch
            .into_iter()
            .map(|key| (UPDATED_INDEX_KEYSPACE.to_string(), key))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })]
    }

    fn deleted_batch(&mut self) -> Effects {
        if let Some(batch) = self.batches.pop_front() {
            return self.delete(batch);
        }
        match (self.finishing, self.after_page.take()) {
            (false, Some(next)) => self.scan(next),
            _ => self.finish(),
        }
    }

    fn finish(&mut self) -> Effects {
        self.state = SweepState::Done;
        self.output = Some(Ok(SweepPass {
            deleted: self.deleted,
            next_after: self.resume.take(),
        }));
        smallvec![]
    }
}

impl Operation for TimestampSweepOperation {
    type Output = SweepPass;
    type Error = SweepError;

    fn start(&mut self) -> Effects {
        let start = self.start.clone();
        self.scan(start)
    }

    fn step(&mut self, event: Event) -> Effects {
        let running = !matches!(self.state, SweepState::Init | SweepState::Done);
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.state == SweepState::Scan => self.scanned_page(values, next_start_after),
            Event::Storage(StorageEvent::BatchReadResult { values })
                if self.state == SweepState::Records =>
            {
                self.read_records(values)
            }
            Event::Storage(StorageEvent::BatchDeleteResult { .. })
                if self.state == SweepState::Delete =>
            {
                self.deleted_batch()
            }
            Event::Storage(StorageEvent::Error { error }) if running => {
                self.fail(SweepError::Storage(error))
            }
            event => {
                let error = SweepError::UnexpectedEvent {
                    state: self.state.name(),
                    got: event.label(),
                };
                self.fail(error)
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == SweepState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SweepError::Unfinished))
    }

    // Every read and delete runs outside a transaction, so nothing is left to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;

    fn document(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    fn record_bytes(document_id: Ulid, updated_at_ms: u64) -> Value {
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

    fn scan_effect(start: IterStart) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: UPDATED_INDEX_KEYSPACE.to_string(),
            prefix: None,
            start: Some(start),
            limit: INDEX_SCAN_BATCH,
            txn_id: None,
        })
    }

    fn reads_effect(documents: &[Ulid]) -> Effect {
        Effect::Storage(StorageEffect::BatchRead {
            reads: documents
                .iter()
                .map(|document_id| {
                    (
                        DOCUMENT_INDEX_KEYSPACE.to_string(),
                        metadata_document_key(*document_id),
                    )
                })
                .collect(),
            txn_id: None,
        })
    }

    fn delete_effect(keys: &[Key]) -> Effect {
        Effect::Storage(StorageEffect::BatchDelete {
            deletes: keys
                .iter()
                .map(|key| (UPDATED_INDEX_KEYSPACE.to_string(), key.clone()))
                .collect(),
            txn_id: None,
        })
    }

    fn page(keys: &[Key], next: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: keys
                .iter()
                .map(|key| (key.clone(), Vec::<u8>::new().into()))
                .collect(),
            next_start_after: next,
        })
    }

    fn records(values: Vec<Option<Value>>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: values
                .into_iter()
                .map(|value| (Vec::<u8>::new().into(), value))
                .collect(),
        })
    }

    fn deleted() -> Event {
        Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        })
    }

    fn first_start() -> IterStart {
        IterStart::At(updated_index_key(0, Ulid::nil()))
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
        check_trace(
            &mut operation,
            &[scan_effect(first_start())],
            vec![
                (
                    page(&keys, None),
                    vec![reads_effect(&[current, moved, gone])],
                ),
                (
                    records(vec![
                        Some(record_bytes(current, 10)),
                        Some(record_bytes(moved, 99)),
                        None,
                    ]),
                    vec![delete_effect(&keys[1..])],
                ),
                (deleted(), vec![]),
            ],
        );
        assert_eq!(
            operation.finalize(),
            Ok(SweepPass {
                deleted: 2,
                next_after: None,
            })
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
        check_trace(
            &mut operation,
            &[scan_effect(IterStart::After(keys[0].clone()))],
            vec![
                (
                    page(&keys[1..], after.clone()),
                    vec![reads_effect(&[document(2)])],
                ),
                (records(vec![Some(record_bytes(document(2), 11))]), vec![]),
            ],
        );
        assert_eq!(
            operation.finalize(),
            Ok(SweepPass {
                deleted: 0,
                next_after: after,
            })
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
        let mut operation = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        check_trace(
            &mut operation,
            &[scan_effect(first_start())],
            vec![
                (page(&keys, next.clone()), vec![reads_effect(&documents)]),
                (
                    records(vec![None; documents.len()]),
                    vec![delete_effect(&keys)],
                ),
                (
                    deleted(),
                    vec![scan_effect(IterStart::After(next.clone().expect("cursor")))],
                ),
                (page(&[], None), vec![]),
            ],
        );
        assert_eq!(
            operation.finalize(),
            Ok(SweepPass {
                deleted: SWEEP_DELETE_BATCH,
                next_after: next,
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
        let at_scan = || TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        let at_reads = || {
            let mut operation = at_scan();
            operation.start();
            operation.step(page(std::slice::from_ref(&key), None));
            operation
        };
        let at_delete = || {
            let mut operation = at_reads();
            operation.step(records(vec![None]));
            operation
        };
        for mut operation in [
            {
                let mut operation = at_scan();
                operation.start();
                operation
            },
            at_reads(),
            at_delete(),
        ] {
            assert!(operation.step(error()).is_empty());
            assert_eq!(
                operation.finalize(),
                Err(SweepError::Storage(StorageError::Timeout))
            );
        }
    }

    #[test]
    fn broken_rows_fail() {
        let mut bad_key = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        bad_key.start();
        bad_key.step(page(&[vec![1u8].into()], None));
        assert!(matches!(bad_key.finalize(), Err(SweepError::Decode(_))));

        let key = updated_index_key(10, document(1));
        let mut bad_record = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        bad_record.start();
        bad_record.step(page(std::slice::from_ref(&key), None));
        bad_record.step(records(vec![Some(vec![0xff].into())]));
        assert!(matches!(bad_record.finalize(), Err(SweepError::Decode(_))));

        let mut short = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        short.start();
        short.step(page(std::slice::from_ref(&key), None));
        short.step(records(Vec::new()));
        assert!(matches!(short.finalize(), Err(SweepError::Decode(_))));
    }

    #[test]
    fn rejects_other_events() {
        let scanning = || {
            let mut operation = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
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
            assert!(matches!(error, SweepError::UnexpectedEvent { .. }));
        }
        let mut idle = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        idle.step(page(&[], None));
        assert!(matches!(
            idle.finalize(),
            Err(SweepError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT));
        let mut scanning = TimestampSweepOperation::new(None, SWEEP_SCAN_LIMIT);
        scanning.start();
        rejects_finalize(scanning);
    }
}
