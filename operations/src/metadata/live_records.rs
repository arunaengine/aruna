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
