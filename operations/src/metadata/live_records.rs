//! Keeps the registry records whose graph and document are not deleted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{DOCUMENT_LIFECYCLE_KEYSPACE, GRAPH_LIFECYCLE_KEYSPACE};
use aruna_core::metadata::{
    GraphLifecycleRecord, MetadataLifecycleRecord, REGISTRY_CANDIDATE_LIMIT,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{document_lifecycle_key, graph_lifecycle_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Key, TxnId, Value};
use thiserror::Error;

use crate::metadata::call_results::read_values;

#[derive(Debug, Error, PartialEq)]
pub enum LiveRecordsError {
    #[error("metadata candidate limit exceeded")]
    TooMany,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for LiveRecordsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
}

/// Reads the graph and document lifecycle of every record in one batch, in
/// `txn_id` when given, and keeps the records neither marks as deleted.
#[derive(Debug, PartialEq)]
pub struct LiveRecordsOperation {
    records: Vec<MetadataRegistryRecord>,
    txn_id: Option<TxnId>,
}

impl LiveRecordsOperation {
    pub fn new(records: Vec<MetadataRegistryRecord>, txn_id: Option<TxnId>) -> Self {
        Self { records, txn_id }
    }

    fn live(
        &self,
        values: Vec<Option<Value>>,
    ) -> Result<Vec<MetadataRegistryRecord>, LiveRecordsError> {
        if values.len() != self.records.len().saturating_mul(2) {
            return Err(LiveRecordsError::Invalid(format!(
                "metadata lifecycle batch returned {} values for {} records",
                values.len(),
                self.records.len()
            )));
        }
        let mut live = Vec::with_capacity(self.records.len());
        for (record, pair) in self.records.iter().zip(values.as_chunks::<2>().0) {
            let graph_deleted = pair[0]
                .as_ref()
                .map(|value| graph_lifecycle_deleted(record, value))
                .transpose()?
                .unwrap_or(false);
            let document_deleted = pair[1]
                .as_ref()
                .map(|value| document_lifecycle_deleted(record, value))
                .transpose()?
                .unwrap_or(false);
            if !graph_deleted && !document_deleted {
                live.push(record.clone());
            }
        }
        Ok(live)
    }
}

impl Operation for LiveRecordsOperation {
    type Output = Vec<MetadataRegistryRecord>;
    type Error = LiveRecordsError;

    fn first(&mut self) -> Result<Next<Self::Output>, LiveRecordsError> {
        if self.records.len() > REGISTRY_CANDIDATE_LIMIT {
            return Err(LiveRecordsError::TooMany);
        }
        if self.records.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        let reads = lifecycle_reads(&self.records);
        Ok(Next::Call(match self.txn_id {
            // The caller's transaction is not the runner's, so the read goes out as an effect.
            Some(txn_id) => IoCall::Effect(Box::new(Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: Some(txn_id),
            }))),
            None => IoCall::Read(reads),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, LiveRecordsError> {
        let values = read_values(result)?;
        Ok(Next::Done(self.live(values)?))
    }
}

/// The graph and the document lifecycle key of every record, in record order.
fn lifecycle_reads(records: &[MetadataRegistryRecord]) -> Vec<(String, Key)> {
    records
        .iter()
        .flat_map(|record| {
            [
                (
                    GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                    graph_lifecycle_key(&record.graph_iri),
                ),
                (
                    DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
                    document_lifecycle_key(record.document_id),
                ),
            ]
        })
        .collect()
}

fn graph_lifecycle_deleted(
    record: &MetadataRegistryRecord,
    value: &[u8],
) -> Result<bool, LiveRecordsError> {
    let lifecycle: GraphLifecycleRecord = postcard::from_bytes(value)
        .map_err(|error| LiveRecordsError::Invalid(error.to_string()))?;
    if lifecycle.graph_iri != record.graph_iri
        || lifecycle.realm_id != record.realm_id
        || lifecycle.group_id != record.group_id
        || lifecycle.document_id != record.document_id
    {
        return Err(LiveRecordsError::Invalid(
            "metadata graph lifecycle record mismatch".to_string(),
        ));
    }
    Ok(lifecycle.is_deleted())
}

fn document_lifecycle_deleted(
    record: &MetadataRegistryRecord,
    value: &[u8],
) -> Result<bool, LiveRecordsError> {
    let lifecycle: MetadataLifecycleRecord = postcard::from_bytes(value)
        .map_err(|error| LiveRecordsError::Invalid(error.to_string()))?;
    let matches = match &lifecycle {
        MetadataLifecycleRecord::Upsert { event }
        | MetadataLifecycleRecord::UpsertWithMessage { event, .. } => {
            event.record.document_id == record.document_id
                && event.record.graph_iri == record.graph_iri
                && event.record.realm_id == record.realm_id
                && event.record.group_id == record.group_id
        }
        MetadataLifecycleRecord::Delete { event } => {
            event.tombstone.document_id == record.document_id
                && event.tombstone.graph_iri == record.graph_iri
                && event.tombstone.realm_id == record.realm_id
                && event.tombstone.group_id == record.group_id
        }
    };
    if !matches {
        return Err(LiveRecordsError::Invalid(
            "metadata document lifecycle record mismatch".to_string(),
        ));
    }
    Ok(matches!(lifecycle, MetadataLifecycleRecord::Delete { .. }))
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::metadata::timestamp_sweep::state_machine_tests::{document, record_bytes};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::metadata::MetadataDeleteRecord;
    use ulid::Ulid;

    fn record(seed: u8) -> MetadataRegistryRecord {
        postcard::from_bytes(&record_bytes(document(seed), 10)).expect("record decodes")
    }

    fn tombstone(record: &MetadataRegistryRecord) -> GraphLifecycleRecord {
        GraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            2,
        )
    }

    fn graph_deleted(record: &MetadataRegistryRecord) -> Option<Value> {
        Some(postcard::to_allocvec(&tombstone(record)).unwrap().into())
    }

    fn document_deleted(record: &MetadataRegistryRecord) -> Option<Value> {
        let lifecycle = MetadataLifecycleRecord::Delete {
            event: MetadataDeleteRecord {
                event_id: Ulid::from_bytes([3; 16]),
                tombstone: tombstone(record),
                deleted_after_id: record.last_event_id,
            },
        };
        Some(postcard::to_allocvec(&lifecycle).unwrap().into())
    }

    #[test]
    fn keeps_live_records() {
        // A graph tombstone or a document delete drops a record; the rest stay in order.
        let (live, gone, removed) = (record(1), record(2), record(3));
        let records = vec![live.clone(), gone.clone(), removed.clone()];
        let mut operation = LiveRecordsOperation::new(records.clone(), None);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(lifecycle_reads(&records))))
        );
        let values = vec![
            None,
            None,
            graph_deleted(&gone),
            None,
            None,
            document_deleted(&removed),
        ];
        assert_eq!(
            operation.next(IoResult::Values(values)),
            Ok(Next::Done(vec![live]))
        );
    }

    #[test]
    fn reads_in_transaction() {
        let txn_id = Ulid::from_bytes([4; 16]);
        let live = record(1);
        let mut operation = LiveRecordsOperation::new(vec![live.clone()], Some(txn_id));
        let read = StorageEffect::BatchRead {
            reads: lifecycle_reads(std::slice::from_ref(&live)),
            txn_id: Some(txn_id),
        };
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(read)))))
        );
        let answer = StorageEvent::BatchReadResult {
            values: vec![
                (Vec::<u8>::new().into(), None),
                (Vec::<u8>::new().into(), None),
            ],
        };
        assert_eq!(
            operation.next(IoResult::Event(Box::new(Event::Storage(answer)))),
            Ok(Next::Done(vec![live]))
        );
        let mut failed = LiveRecordsOperation::new(vec![record(1)], Some(txn_id));
        failed.first().unwrap();
        let error = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert_eq!(
            failed.next(IoResult::Event(Box::new(error))),
            Err(LiveRecordsError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn empty_and_capped() {
        // No records need no read; more than the candidate limit refuse before reading.
        let mut empty = LiveRecordsOperation::new(Vec::new(), None);
        assert_eq!(empty.first(), Ok(Next::Done(Vec::new())));
        let mut capped =
            LiveRecordsOperation::new(vec![record(1); REGISTRY_CANDIDATE_LIMIT + 1], None);
        assert_eq!(capped.first(), Err(LiveRecordsError::TooMany));
    }

    #[test]
    fn broken_rows_fail() {
        let (own, stranger) = (record(1), record(2));
        let cases = [
            vec![Some(vec![1u8].into()), None],
            vec![graph_deleted(&stranger), None],
            vec![None, document_deleted(&stranger)],
            Vec::new(),
        ];
        for values in cases {
            let mut operation = LiveRecordsOperation::new(vec![own.clone()], None);
            operation.first().unwrap();
            assert!(matches!(
                operation.next(IoResult::Values(values)),
                Err(LiveRecordsError::Invalid(_))
            ));
        }
    }

    #[test]
    fn wrong_result_fails() {
        let mut operation = LiveRecordsOperation::new(vec![record(1)], None);
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(LiveRecordsError::Io(IoError::Unexpected { .. }))
        ));
    }

    #[test]
    fn runner_errors_map() {
        assert_eq!(
            LiveRecordsError::from(IoError::Storage(StorageError::Timeout)),
            LiveRecordsError::Storage(StorageError::Timeout)
        );
        assert_eq!(
            LiveRecordsError::from(IoError::Aborted),
            LiveRecordsError::Io(IoError::Aborted)
        );
    }
}
