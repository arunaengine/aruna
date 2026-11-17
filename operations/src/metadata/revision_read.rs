//! Loads a document's raw revision and its stored raw state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DOCUMENT_LIFECYCLE_KEYSPACE, GRAPH_LIFECYCLE_KEYSPACE, RAW_REVISION_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MaterializationStatusRecord, MetadataLifecycleRecord,
    MetadataRawRevision, resolve_raw_revision,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{document_lifecycle_key, graph_lifecycle_key, raw_revision_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use ulid::Ulid;

use crate::metadata::raw_events::{
    RawEventsOperation, RawLoadBudget, RawReadError, RawRevisionState,
};
use crate::metadata::repository::{StorageReadError, parse_status_read, read_status_effect};

/// The materialization status, when asked for, and the raw revision of a live document.
pub type RawViewOutput = (
    Option<MaterializationStatusRecord>,
    Option<MetadataRawRevision>,
);

#[derive(Debug, PartialEq)]
enum ViewState {
    Init,
    Status,
    Document,
    Graph,
    Raw,
    Events,
    Done,
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
    state: ViewState,
    output: Option<Result<RawViewOutput, RawReadError>>,
}

impl RawViewOperation {
    pub(crate) fn new(document_id: Ulid, txn_id: Option<TxnId>, with_status: bool) -> Self {
        Self {
            document_id,
            txn_id,
            with_status,
            status: None,
            events: None,
            state: ViewState::Init,
            output: None,
        }
    }

    fn finish(&mut self, revision: Result<Option<MetadataRawRevision>, RawReadError>) -> Effects {
        self.state = ViewState::Done;
        self.output = Some(revision.map(|revision| (self.status.take(), revision)));
        smallvec![]
    }

    fn read(&mut self, state: ViewState, key_space: &str, key: Key) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn read_document(&mut self) -> Effects {
        let key = document_lifecycle_key(self.document_id);
        self.read(ViewState::Document, DOCUMENT_LIFECYCLE_KEYSPACE, key)
    }

    fn status_read(&mut self, event: Event) -> Effects {
        match parse_status_read(event) {
            Ok(status) => {
                self.status = status;
                self.read_document()
            }
            Err(StorageReadError::Storage(error)) => self.finish(Err(error.into())),
            Err(StorageReadError::Conversion(error)) => self.finish(Err(error.into())),
        }
    }

    fn document_read(&mut self, value: Option<Value>) -> Effects {
        let deleted = value
            .map(|value| postcard::from_bytes::<MetadataLifecycleRecord>(&value))
            .transpose()
            .map(|record| matches!(record, Some(MetadataLifecycleRecord::Delete { .. })));
        match deleted {
            Ok(true) => self.finish(Ok(None)),
            Ok(false) => {
                let graph_iri = MetadataRegistryRecord::graph_iri_for(self.document_id);
                let key = graph_lifecycle_key(&graph_iri);
                self.read(ViewState::Graph, GRAPH_LIFECYCLE_KEYSPACE, key)
            }
            Err(error) => self.finish(Err(ConversionError::from(error).into())),
        }
    }

    fn graph_read(&mut self, value: Option<Value>) -> Effects {
        let deleted = value
            .map(|value| postcard::from_bytes::<GraphLifecycleRecord>(&value))
            .transpose()
            .map(|record| record.is_some_and(|record| record.is_deleted()));
        match deleted {
            Ok(true) => self.finish(Ok(None)),
            Ok(false) => {
                let key = raw_revision_key(self.document_id);
                self.read(ViewState::Raw, RAW_REVISION_KEYSPACE, key)
            }
            Err(error) => self.finish(Err(ConversionError::from(error).into())),
        }
    }

    // Once a batch has merged, the rendered graph is the document's revision;
    // the authored-text replay only serves a document until then.
    fn raw_read(&mut self, value: Option<Value>) -> Effects {
        let state = value
            .map(|value| postcard::from_bytes::<RawRevisionState>(&value))
            .transpose();
        match state {
            Ok(Some(state)) if state.merged => self.finish(Ok(state.revision)),
            Ok(_) => {
                let budget = RawLoadBudget::default();
                let mut events =
                    RawEventsOperation::new(self.document_id, None, None, self.txn_id, budget);
                let effects = events.start();
                self.events = Some(events);
                self.state = ViewState::Events;
                self.events_progress(effects)
            }
            Err(error) => self.finish(Err(ConversionError::from(error).into())),
        }
    }

    /// Passes the event read's effects on until it completes, then resolves the revision.
    fn events_progress(&mut self, effects: Effects) -> Effects {
        if !self
            .events
            .as_ref()
            .is_some_and(|events| events.is_complete())
        {
            return effects;
        }
        let Some(events) = self.events.take() else {
            return self.finish(Err(RawReadError::UnexpectedEvent("no event read".into())));
        };
        let revision = events
            .finalize()
            .and_then(|events| resolve_raw_revision(&events).map_err(Into::into));
        self.finish(revision)
    }
}

impl Operation for RawViewOperation {
    type Output = RawViewOutput;
    type Error = RawReadError;

    fn start(&mut self) -> Effects {
        if !self.with_status {
            return self.read_document();
        }
        self.state = ViewState::Status;
        smallvec![read_status_effect(self.document_id, self.txn_id)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == ViewState::Events {
            let effects = match self.events.as_mut() {
                Some(events) => events.step(event),
                None => smallvec![],
            };
            return self.events_progress(effects);
        }
        if self.state == ViewState::Status {
            return self.status_read(event);
        }
        let reading = matches!(
            self.state,
            ViewState::Document | ViewState::Graph | ViewState::Raw
        );
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) if reading => match self.state {
                ViewState::Document => self.document_read(value),
                ViewState::Graph => self.graph_read(value),
                _ => self.raw_read(value),
            },
            Event::Storage(StorageEvent::Error { error }) if reading => {
                self.finish(Err(error.into()))
            }
            other => {
                let error = RawReadError::UnexpectedEvent(other.label().to_string());
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ViewState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RawReadError::UnexpectedEvent(
            "the raw view read did not finish".to_string(),
        )))
    }

    // Every read is untransacted or runs in the caller's transaction.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Reads a document's stored raw state outside any transaction.
#[derive(Debug, PartialEq)]
pub struct RawStateOperation {
    document_id: Ulid,
    reading: bool,
    output: Option<Result<Option<RawRevisionState>, RawReadError>>,
}

impl RawStateOperation {
    pub(crate) fn new(document_id: Ulid) -> Self {
        Self {
            document_id,
            reading: false,
            output: None,
        }
    }
}

impl Operation for RawStateOperation {
    type Output = Option<RawRevisionState>;
    type Error = RawReadError;

    fn start(&mut self) -> Effects {
        self.reading = true;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: RAW_REVISION_KEYSPACE.to_string(),
            key: raw_revision_key(self.document_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let output = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) if self.reading => value
                .map(|value| postcard::from_bytes::<RawRevisionState>(&value))
                .transpose()
                .map_err(|error| ConversionError::from(error).into()),
            Event::Storage(StorageEvent::Error { error }) if self.reading => Err(error.into()),
            other => Err(RawReadError::UnexpectedEvent(other.label().to_string())),
        };
        self.reading = false;
        self.output = Some(output);
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RawReadError::UnexpectedEvent(
            "the raw state read did not finish".to_string(),
        )))
    }

    // One untransacted read leaves nothing to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::metadata::raw_events::state_machine_tests::{event, page, page_effect, row};
    use aruna_core::errors::StorageError;
    use aruna_core::metadata::{MetadataDeleteRecord, MetadataEventRecord};
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn document() -> Ulid {
        event(10).record.document_id
    }

    fn read_effect(key_space: &str, key: Key) -> Effect {
        Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
    }

    fn document_effect() -> Effect {
        read_effect(
            DOCUMENT_LIFECYCLE_KEYSPACE,
            document_lifecycle_key(document()),
        )
    }

    fn graph_effect() -> Effect {
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document());
        read_effect(GRAPH_LIFECYCLE_KEYSPACE, graph_lifecycle_key(&graph_iri))
    }

    fn raw_effect() -> Effect {
        read_effect(RAW_REVISION_KEYSPACE, raw_revision_key(document()))
    }

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: value.map(Into::into),
        })
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
        operation.start();
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
        check_trace(
            &mut operation,
            &[document_effect()],
            vec![
                (read(None), vec![graph_effect()]),
                (read(None), vec![raw_effect()]),
                (read(bytes(&merged)), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok((None, Some(revision(&event(10))))));
    }

    #[test]
    fn replays_event_log() {
        let first = event(10);
        let status = MaterializationStatusRecord::pending(&first, 1);
        let mut operation = RawViewOperation::new(document(), None, true);
        check_trace(
            &mut operation,
            &[read_status_effect(document(), None)],
            vec![
                (read(bytes(&status)), vec![document_effect()]),
                (read(None), vec![graph_effect()]),
                (read(None), vec![raw_effect()]),
                (read(None), vec![page_effect(None)]),
                (page(vec![row(&first)], None), vec![]),
            ],
        );
        let expected = resolve_raw_revision(std::slice::from_ref(&first)).unwrap();
        assert_eq!(operation.finalize(), Ok((Some(status), expected)));
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
        by_document.step(read(bytes(&removed)));
        assert_eq!(by_document.finalize(), Ok((None, None)));

        let mut by_graph = reading(false);
        by_graph.step(read(None));
        by_graph.step(read(bytes(&tombstone)));
        assert_eq!(by_graph.finalize(), Ok((None, None)));
    }

    #[test]
    fn storage_errors_fail() {
        let error = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        // Steps already taken before the error: status, document, graph, raw, event page.
        for answered in 0..5 {
            let mut operation = reading(true);
            for _ in 0..answered {
                operation.step(read(None));
            }
            assert!(operation.step(error()).is_empty());
            assert_eq!(
                operation.finalize(),
                Err(RawReadError::Storage(StorageError::Timeout)),
                "after {answered} answers"
            );
        }
    }

    #[test]
    fn broken_rows_fail() {
        for answered in 0..4 {
            let mut operation = reading(true);
            for _ in 0..answered {
                operation.step(read(None));
            }
            operation.step(read(Some(vec![0xff, 0xff])));
            assert!(
                matches!(operation.finalize(), Err(RawReadError::Conversion(_))),
                "after {answered} answers"
            );
        }
    }

    #[test]
    fn rejects_other_events() {
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (_, effects, error) in rejects_events(|| reading(false), sample_events, accepted) {
            assert!(effects.is_empty());
            assert!(matches!(error, RawReadError::UnexpectedEvent(_)));
        }
        let mut idle = RawViewOperation::new(document(), None, false);
        idle.step(read(None));
        assert!(matches!(
            idle.finalize(),
            Err(RawReadError::UnexpectedEvent(_))
        ));
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = RawViewOperation::new(document(), None, true);
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(RawViewOperation::new(document(), None, true));
        rejects_finalize(reading(true));
    }
}
