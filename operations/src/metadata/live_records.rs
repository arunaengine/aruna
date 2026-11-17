//! Keeps the registry records whose graph and document are not deleted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DOCUMENT_LIFECYCLE_KEYSPACE, GRAPH_LIFECYCLE_KEYSPACE};
use aruna_core::metadata::{
    GraphLifecycleRecord, MetadataLifecycleRecord, REGISTRY_CANDIDATE_LIMIT,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{document_lifecycle_key, graph_lifecycle_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum LiveRecordsError {
    #[error("metadata candidate limit exceeded")]
    TooMany,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("{0}")]
    Invalid(String),
    #[error("unexpected event while {state}: got {got}")]
    UnexpectedEvent {
        state: &'static str,
        got: &'static str,
    },
    #[error("the live record check did not finish")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum LiveState {
    Init,
    Reading,
    Done,
}

/// Reads the graph and document lifecycle of every record in one batch, in
/// `txn_id` when given, and keeps the records neither marks as deleted.
#[derive(Debug, PartialEq)]
pub struct LiveRecordsOperation {
    records: Vec<MetadataRegistryRecord>,
    txn_id: Option<TxnId>,
    state: LiveState,
    output: Option<Result<Vec<MetadataRegistryRecord>, LiveRecordsError>>,
}

impl LiveRecordsOperation {
    pub fn new(records: Vec<MetadataRegistryRecord>, txn_id: Option<TxnId>) -> Self {
        Self {
            records,
            txn_id,
            state: LiveState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<Vec<MetadataRegistryRecord>, LiveRecordsError>) -> Effects {
        self.state = LiveState::Done;
        self.output = Some(output);
        smallvec![]
    }

    fn live(
        &self,
        values: Vec<(Key, Option<Value>)>,
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
            let (graph_key, graph_value) = &pair[0];
            if graph_key != &graph_lifecycle_key(&record.graph_iri) {
                return Err(LiveRecordsError::Invalid(
                    "metadata graph lifecycle batch key mismatch".to_string(),
                ));
            }
            let graph_deleted = graph_value
                .as_ref()
                .map(|value| graph_lifecycle_deleted(record, value))
                .transpose()?
                .unwrap_or(false);

            let (document_key, document_value) = &pair[1];
            if document_key != &document_lifecycle_key(record.document_id) {
                return Err(LiveRecordsError::Invalid(
                    "metadata document lifecycle batch key mismatch".to_string(),
                ));
            }
            let document_deleted = document_value
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

    fn start(&mut self) -> Effects {
        if self.records.len() > REGISTRY_CANDIDATE_LIMIT {
            return self.finish(Err(LiveRecordsError::TooMany));
        }
        if self.records.is_empty() {
            return self.finish(Ok(Vec::new()));
        }
        let mut reads = Vec::with_capacity(self.records.len().saturating_mul(2));
        for record in &self.records {
            reads.push((
                GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                graph_lifecycle_key(&record.graph_iri),
            ));
            reads.push((
                DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
                document_lifecycle_key(record.document_id),
            ));
        }
        self.state = LiveState::Reading;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let reading = self.state == LiveState::Reading;
        match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) if reading => {
                let output = self.live(values);
                self.finish(output)
            }
            Event::Storage(StorageEvent::Error { error }) if reading => {
                self.finish(Err(error.into()))
            }
            event => {
                let error = LiveRecordsError::UnexpectedEvent {
                    state: if reading {
                        "reading lifecycles"
                    } else {
                        "idle"
                    },
                    got: event.label(),
                };
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == LiveState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LiveRecordsError::Unfinished))
    }

    // The batch read changes nothing, and a caller's transaction stays the caller's.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
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
mod state_machine_tests {
    use super::*;
    use crate::metadata::timestamp_sweep::state_machine_tests::{document, record_bytes};
    use aruna_core::metadata::MetadataDeleteRecord;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
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

    fn reads_effect(records: &[MetadataRegistryRecord], txn_id: Option<TxnId>) -> Effect {
        let reads = records
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
            .collect();
        Effect::Storage(StorageEffect::BatchRead { reads, txn_id })
    }

    /// Answers the batch read with one graph and one document value per record.
    fn answer(rows: &[(&MetadataRegistryRecord, Option<Value>, Option<Value>)]) -> Event {
        let values = rows
            .iter()
            .flat_map(|(record, graph, document)| {
                [
                    (graph_lifecycle_key(&record.graph_iri), graph.clone()),
                    (document_lifecycle_key(record.document_id), document.clone()),
                ]
            })
            .collect();
        Event::Storage(StorageEvent::BatchReadResult { values })
    }

    fn reading(records: Vec<MetadataRegistryRecord>) -> LiveRecordsOperation {
        let mut operation = LiveRecordsOperation::new(records, None);
        operation.start();
        operation
    }

    #[test]
    fn keeps_live_records() {
        // A graph tombstone or a document delete drops a record; the rest stay in order.
        let (live, gone, removed) = (record(1), record(2), record(3));
        let records = vec![live.clone(), gone.clone(), removed.clone()];
        let mut operation = LiveRecordsOperation::new(records.clone(), None);
        check_trace(
            &mut operation,
            &[reads_effect(&records, None)],
            vec![(
                answer(&[
                    (&live, None, None),
                    (&gone, graph_deleted(&gone), None),
                    (&removed, None, document_deleted(&removed)),
                ]),
                vec![],
            )],
        );
        assert_eq!(operation.finalize(), Ok(vec![live]));
    }

    #[test]
    fn reads_in_transaction() {
        let txn_id = Ulid::from_bytes([4; 16]);
        let live = record(1);
        let mut operation = LiveRecordsOperation::new(vec![live.clone()], Some(txn_id));
        check_trace(
            &mut operation,
            &[reads_effect(std::slice::from_ref(&live), Some(txn_id))],
            vec![(answer(&[(&live, None, None)]), vec![])],
        );
        assert_eq!(operation.finalize(), Ok(vec![live]));
    }

    #[test]
    fn empty_and_capped() {
        // No records need no read; more than the candidate limit refuse before reading.
        let mut empty = LiveRecordsOperation::new(Vec::new(), None);
        assert!(empty.start().is_empty());
        assert!(empty.is_complete());
        assert_eq!(empty.finalize(), Ok(Vec::new()));

        let mut capped =
            LiveRecordsOperation::new(vec![record(1); REGISTRY_CANDIDATE_LIMIT + 1], None);
        assert!(capped.start().is_empty());
        assert_eq!(capped.finalize(), Err(LiveRecordsError::TooMany));
    }

    #[test]
    fn storage_errors_fail() {
        let mut operation = reading(vec![record(1)]);
        let error = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert!(operation.step(error).is_empty());
        assert_eq!(
            operation.finalize(),
            Err(LiveRecordsError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn broken_rows_fail() {
        let (own, stranger) = (record(1), record(2));
        let cases = [
            answer(&[(&own, Some(vec![1u8].into()), None)]),
            answer(&[(&own, graph_deleted(&stranger), None)]),
            answer(&[(&own, None, document_deleted(&stranger))]),
            answer(&[(&stranger, None, None)]),
            Event::Storage(StorageEvent::BatchReadResult { values: Vec::new() }),
        ];
        for event in cases {
            let mut operation = reading(vec![own.clone()]);
            assert!(operation.step(event).is_empty());
            assert!(matches!(
                operation.finalize(),
                Err(LiveRecordsError::Invalid(_))
            ));
        }
    }

    #[test]
    fn rejects_other_events() {
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::BatchReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (_, effects, error) in
            rejects_events(|| reading(vec![record(1)]), sample_events, accepted)
        {
            assert!(effects.is_empty());
            assert!(matches!(error, LiveRecordsError::UnexpectedEvent { .. }));
        }
        let mut idle = LiveRecordsOperation::new(vec![record(1)], None);
        idle.step(answer(&[]));
        assert!(matches!(
            idle.finalize(),
            Err(LiveRecordsError::UnexpectedEvent { .. })
        ));
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = LiveRecordsOperation::new(vec![record(1)], None);
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(LiveRecordsOperation::new(vec![record(1)], None));
        rejects_finalize(reading(vec![record(1)]));
    }
}
