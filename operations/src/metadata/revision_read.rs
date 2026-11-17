//! Loads a document's raw revision: lifecycle checks, the merged state, then the event log.
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
