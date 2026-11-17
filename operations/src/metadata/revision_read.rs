//! Loads a document's raw revision and its stored raw state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    DOCUMENT_LIFECYCLE_KEYSPACE, GRAPH_LIFECYCLE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE,
    RAW_REVISION_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MaterializationState, MaterializationStatusRecord, MetadataEventRecord,
    MetadataLifecycleRecord, MetadataRawRevision, resolve_raw_revision,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    document_lifecycle_key, graph_lifecycle_key, materialization_status_key, raw_revision_key,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Key, TxnId, Value};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::metadata::call_results::read_value;
use crate::metadata::raw_events::{
    RawEventsOperation, RawLoadBudget, RawReadError, RawRevisionState,
};

/// The materialization status, when asked for, and the raw revision of a live document.
pub type RawViewOutput = (
    Option<MaterializationStatusRecord>,
    Option<MetadataRawRevision>,
);

/// A document's raw revision with how far its projection has caught up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRawView {
    pub revision: MetadataRawRevision,
    pub projection_state: MaterializationState,
    pub projected_event_id: Option<Ulid>,
}

/// One row read, in the caller's transaction when there is one.
fn row_call(key_space: &str, key: Key, txn_id: Option<TxnId>) -> IoCall {
    let reads = vec![(key_space.to_string(), key)];
    match txn_id {
        // The caller's transaction is not the runner's, so the read goes out as an effect.
        Some(txn_id) => IoCall::Effect(Box::new(Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        }))),
        None => IoCall::Read(reads),
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: Option<Value>) -> Result<Option<T>, RawReadError> {
    value
        .map(|value| postcard::from_bytes::<T>(&value))
        .transpose()
        .map_err(|error| ConversionError::from(error).into())
}

#[derive(Debug, PartialEq)]
enum ViewStep {
    Status,
    Document,
    Graph,
    Raw,
    Events,
}

/// A deleted document or graph has no revision. A merged document answers with its
/// stored revision; otherwise the event log is replayed within the raw budget.
#[derive(Debug, PartialEq)]
pub struct RawViewOperation {
    document_id: Ulid,
    txn_id: Option<TxnId>,
    with_status: bool,
    status: Option<MaterializationStatusRecord>,
    events: Option<RawEventsOperation>,
    step: ViewStep,
}

type ViewTurn = Result<Next<RawViewOutput>, RawReadError>;

impl RawViewOperation {
    pub(crate) fn new(document_id: Ulid, txn_id: Option<TxnId>, with_status: bool) -> Self {
        Self {
            document_id,
            txn_id,
            with_status,
            status: None,
            events: None,
            step: ViewStep::Document,
        }
    }

    fn read(&mut self, step: ViewStep, key_space: &str, key: Key) -> ViewTurn {
        self.step = step;
        Ok(Next::Call(row_call(key_space, key, self.txn_id)))
    }

    fn done(&mut self, revision: Option<MetadataRawRevision>) -> ViewTurn {
        Ok(Next::Done((self.status.take(), revision)))
    }

    fn read_document(&mut self) -> ViewTurn {
        let key = document_lifecycle_key(self.document_id);
        self.read(ViewStep::Document, DOCUMENT_LIFECYCLE_KEYSPACE, key)
    }

    fn document_read(&mut self, value: Option<Value>) -> ViewTurn {
        let record = decode::<MetadataLifecycleRecord>(value)?;
        if matches!(record, Some(MetadataLifecycleRecord::Delete { .. })) {
            return self.done(None);
        }
        let graph_iri = MetadataRegistryRecord::graph_iri_for(self.document_id);
        self.read(
            ViewStep::Graph,
            GRAPH_LIFECYCLE_KEYSPACE,
            graph_lifecycle_key(&graph_iri),
        )
    }

    fn graph_read(&mut self, value: Option<Value>) -> ViewTurn {
        let record = decode::<GraphLifecycleRecord>(value)?;
        if record.is_some_and(|record| record.is_deleted()) {
            return self.done(None);
        }
        let key = raw_revision_key(self.document_id);
        self.read(ViewStep::Raw, RAW_REVISION_KEYSPACE, key)
    }

    // Once a batch has merged, the rendered graph is the document's revision;
    // the authored-text replay only serves a document until then.
    fn raw_read(&mut self, value: Option<Value>) -> ViewTurn {
        if let Some(state) = decode::<RawRevisionState>(value)?
            && state.merged
        {
            return self.done(state.revision);
        }
        let budget = RawLoadBudget::default();
        let mut events = RawEventsOperation::new(self.document_id, None, None, self.txn_id, budget);
        let turn = events.first()?;
        self.events = Some(events);
        self.step = ViewStep::Events;
        self.events_turn(turn)
    }

    /// Passes the event read's calls on until it finishes, then resolves the revision.
    fn events_turn(&mut self, turn: Next<Vec<MetadataEventRecord>>) -> ViewTurn {
        match turn {
            Next::Call(call) => Ok(Next::Call(call)),
            Next::Wait => Ok(Next::Wait),
            // The event read is a plain read, so it has no calls after a commit.
            Next::Done(events) | Next::Then { output: events, .. } => {
                self.events = None;
                let revision = resolve_raw_revision(&events)?;
                self.done(revision)
            }
        }
    }
}

impl Operation for RawViewOperation {
    type Output = RawViewOutput;
    type Error = RawReadError;

    fn first(&mut self) -> ViewTurn {
        if !self.with_status {
            return self.read_document();
        }
        let key = materialization_status_key(self.document_id);
        self.read(ViewStep::Status, MATERIALIZATION_STATUS_KEYSPACE, key)
    }

    fn next(&mut self, result: IoResult) -> ViewTurn {
        if self.step == ViewStep::Events {
            let turn = match self.events.as_mut() {
                Some(events) => events.next(result)?,
                None => return Err(RawReadError::UnexpectedEvent("no event read".into())),
            };
            return self.events_turn(turn);
        }
        let value = read_value(result)?;
        match self.step {
            ViewStep::Status => {
                self.status = decode(value)?;
                self.read_document()
            }
            ViewStep::Document => self.document_read(value),
            ViewStep::Graph => self.graph_read(value),
            _ => self.raw_read(value),
        }
    }
}

/// Reads a document's stored raw state outside any transaction.
#[derive(Debug, PartialEq)]
pub struct RawStateOperation {
    document_id: Ulid,
}

impl RawStateOperation {
    pub(crate) fn new(document_id: Ulid) -> Self {
        Self { document_id }
    }
}

impl Operation for RawStateOperation {
    type Output = Option<RawRevisionState>;
    type Error = RawReadError;

    fn first(&mut self) -> Result<Next<Self::Output>, RawReadError> {
        let key = raw_revision_key(self.document_id);
        Ok(Next::Call(row_call(RAW_REVISION_KEYSPACE, key, None)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, RawReadError> {
        Ok(Next::Done(decode(read_value(result)?)?))
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::metadata::raw_events::call_tests::{event, page, page_call, row};
    use aruna_core::calls::IoError;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::metadata::MetadataDeleteRecord;

    fn document() -> Ulid {
        event(10).record.document_id
    }

    fn read_call<T>(key_space: &str, key: Key) -> Next<T> {
        Next::Call(IoCall::Read(vec![(key_space.to_string(), key)]))
    }

    fn document_call<T>() -> Next<T> {
        read_call(
            DOCUMENT_LIFECYCLE_KEYSPACE,
            document_lifecycle_key(document()),
        )
    }

    fn graph_call<T>() -> Next<T> {
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document());
        read_call(GRAPH_LIFECYCLE_KEYSPACE, graph_lifecycle_key(&graph_iri))
    }

    fn raw_call<T>() -> Next<T> {
        read_call(RAW_REVISION_KEYSPACE, raw_revision_key(document()))
    }

    fn value(bytes: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![bytes.map(Value::from)])
    }

    fn bytes<T: serde::Serialize>(value: &T) -> Option<Vec<u8>> {
        Some(postcard::to_allocvec(value).unwrap())
    }

    fn revision(event: &MetadataEventRecord) -> MetadataRawRevision {
        MetadataRawRevision {
            jsonld: "{}".to_string(),
            winning_event_id: event.event_id,
            context_digest: [0; 32],
            dataset_digest: None,
            merged: None,
        }
    }

    fn reading(with_status: bool) -> RawViewOperation {
        let mut operation = RawViewOperation::new(document(), None, with_status);
        operation.first().unwrap();
        operation
    }

    #[test]
    fn merged_state_answers() {
        // A merged document answers with its stored revision, without the event log.
        let merged = RawRevisionState {
            base: None,
            last_event_id: event(10).event_id,
            revision: Some(revision(&event(10))),
            merged: true,
        };
        let mut operation = RawViewOperation::new(document(), None, false);
        assert_eq!(operation.first(), Ok(document_call()));
        assert_eq!(operation.next(value(None)), Ok(graph_call()));
        assert_eq!(operation.next(value(None)), Ok(raw_call()));
        assert_eq!(
            operation.next(value(bytes(&merged))),
            Ok(Next::Done((None, Some(revision(&event(10))))))
        );
    }

    #[test]
    fn replays_event_log() {
        let first = event(10);
        let status = MaterializationStatusRecord::pending(&first, 1);
        let mut operation = RawViewOperation::new(document(), None, true);
        assert_eq!(
            operation.first(),
            Ok(read_call(
                MATERIALIZATION_STATUS_KEYSPACE,
                materialization_status_key(document())
            ))
        );
        assert_eq!(operation.next(value(bytes(&status))), Ok(document_call()));
        assert_eq!(operation.next(value(None)), Ok(graph_call()));
        assert_eq!(operation.next(value(None)), Ok(raw_call()));
        assert_eq!(operation.next(value(None)), Ok(page_call(None)));
        let expected = resolve_raw_revision(std::slice::from_ref(&first)).unwrap();
        assert_eq!(
            operation.next(page(vec![row(&first)], None)),
            Ok(Next::Done((Some(status), expected)))
        );
    }

    #[test]
    fn reads_in_transaction() {
        let txn_id = Ulid::from_bytes([6; 16]);
        let mut operation = RawViewOperation::new(document(), Some(txn_id), false);
        let read = StorageEffect::BatchRead {
            reads: vec![(
                DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
                document_lifecycle_key(document()),
            )],
            txn_id: Some(txn_id),
        };
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(read)))))
        );
        let error = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert_eq!(
            operation.next(IoResult::Event(Box::new(error))),
            Err(RawReadError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn deleted_has_none() {
        let first = event(10);
        let tombstone = GraphLifecycleRecord::deleted(
            first.record.graph_iri.clone(),
            first.record.realm_id,
            first.record.group_id,
            first.record.document_id,
            2,
        );
        let removed = MetadataLifecycleRecord::Delete {
            event: MetadataDeleteRecord {
                event_id: Ulid::from_parts(1, 20),
                tombstone: tombstone.clone(),
                deleted_after_id: first.event_id,
            },
        };
        let mut by_document = reading(false);
        assert_eq!(
            by_document.next(value(bytes(&removed))),
            Ok(Next::Done((None, None)))
        );

        let mut by_graph = reading(false);
        by_graph.next(value(None)).unwrap();
        assert_eq!(
            by_graph.next(value(bytes(&tombstone))),
            Ok(Next::Done((None, None)))
        );
    }

    #[test]
    fn broken_rows_fail() {
        for answered in 0..4 {
            let mut operation = reading(true);
            for _ in 0..answered {
                operation.next(value(None)).unwrap();
            }
            assert!(
                matches!(
                    operation.next(value(Some(vec![0xff, 0xff]))),
                    Err(RawReadError::Conversion(_))
                ),
                "after {answered} answers"
            );
        }
    }

    #[test]
    fn wrong_result_fails() {
        assert!(matches!(
            reading(false).next(IoResult::Done),
            Err(RawReadError::UnexpectedEvent(_))
        ));
        assert_eq!(
            RawReadError::from(IoError::Conflict),
            RawReadError::Storage(StorageError::TransactionConflict)
        );
    }

    #[test]
    fn reads_stored_state() {
        let stored = RawRevisionState {
            base: None,
            last_event_id: event(10).event_id,
            revision: None,
            merged: false,
        };
        let mut operation = RawStateOperation::new(document());
        assert_eq!(operation.first(), Ok(raw_call()));
        assert_eq!(
            operation.next(value(bytes(&stored))),
            Ok(Next::Done(Some(stored)))
        );
        let mut missing = RawStateOperation::new(document());
        missing.first().unwrap();
        assert_eq!(missing.next(value(None)), Ok(Next::Done(None)));
    }

    #[test]
    fn state_read_fails() {
        let mut broken = RawStateOperation::new(document());
        broken.first().unwrap();
        assert!(matches!(
            broken.next(value(Some(vec![0xff, 0xff]))),
            Err(RawReadError::Conversion(_))
        ));
        let mut wrong = RawStateOperation::new(document());
        wrong.first().unwrap();
        assert!(wrong.next(IoResult::Woke).is_err());
    }
}
