//! Runs a metadata document update under this node's per-document hold, after its revision
//! and Profile checks, and keeps the registry cache in step.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    DOCUMENT_JOB_KEYSPACE, EVENT_LOG_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE,
    PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    MaterializationState, MaterializationStatusRecord, MetadataEffect, MetadataError,
    MetadataEvent, MetadataEventPayload, MetadataEventRecord,
};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::rows::{read_row, value};
use aruna_core::storage_entries::{
    document_job_prefix, event_log_prefix, materialization_status_key,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::Key;
use ulid::Ulid;

use crate::metadata::call_results::{effect_event, page_rows};
use crate::metadata::profile::rules::stale_status;
use crate::metadata::profile::validation::{
    MetadataProfilePreview, ProfileAssessOperation, submission_verdict,
};
use crate::metadata::update_document::{
    UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
};

/// Pending projection markers read per page while looking for an unapplied event.
const MARKER_PAGE: usize = 100;

/// Whether a mutation is planned against this holder's graph, so its expected revision must be
/// applied here first.
fn plans_on_graph(mutation: &UpdateDocumentMutation) -> bool {
    matches!(
        mutation,
        UpdateDocumentMutation::ReplaceRoCrate { .. }
            | UpdateDocumentMutation::UpsertDataEntity { .. }
            | UpdateDocumentMutation::UpsertContextualEntity { .. }
            | UpdateDocumentMutation::ReplaceDataEntity { .. }
            | UpdateDocumentMutation::RemoveDataEntity { .. }
    )
}

#[derive(Debug, Clone, PartialEq)]
enum BaseStage {
    /// Reading the newest event of the document's log.
    Newest,
    /// Reading the materialization status.
    Status,
    /// Paging the pending projection markers, after the given key.
    Markers,
    /// Reading the logged event of one marker; the rest of the page waits.
    Marker(Vec<Key>, Option<Key>),
    /// Asking the graph whether one marker's batch is applied.
    Dot(Vec<Key>, Option<Key>),
    /// Looking for a queued materialization job.
    Jobs,
}

/// Refuses a revision-checked change this holder cannot plan yet: a base other than its newest
/// event is a conflict, and one its graph has not applied (a status short of it, a received
/// event not handed on yet, or a queued materialization job) is retryable. The transactional
/// revision check still runs later.
#[derive(Debug, PartialEq)]
pub struct ReadyBaseOperation {
    document_id: Ulid,
    expected: Ulid,
    stage: BaseStage,
}

type BaseTurn = Result<Next<()>, UpdateDocumentError>;

impl ReadyBaseOperation {
    pub fn new(document_id: Ulid, expected: Ulid) -> Self {
        Self {
            document_id,
            expected,
            stage: BaseStage::Newest,
        }
    }

    fn pending(&self) -> UpdateDocumentError {
        UpdateDocumentError::GraphPending {
            expected: self.expected,
        }
    }

    fn newest_read(&mut self, result: IoResult) -> BaseTurn {
        let (rows, _) = page_rows(result)?;
        let current = (rows.first())
            .and_then(|(key, _)| <[u8; 16]>::try_from(key.get(16..32)?).ok())
            .map_or(Ulid::nil(), Ulid::from_bytes);
        if current != self.expected {
            return Err(UpdateDocumentError::RevisionConflict {
                expected: self.expected,
                current,
            });
        }
        self.stage = BaseStage::Status;
        let key = materialization_status_key(self.document_id);
        Ok(Next::Call(read_row(MATERIALIZATION_STATUS_KEYSPACE, key)))
    }

    fn status_read(&mut self, result: IoResult) -> BaseTurn {
        let status = value(result)?
            .and_then(|bytes| postcard::from_bytes::<MaterializationStatusRecord>(&bytes).ok());
        let applied = status.is_some_and(|status| {
            status.state == MaterializationState::Materialized && status.event_id >= self.expected
        });
        if !applied {
            return Err(self.pending());
        }
        self.markers(None)
    }

    /// A received event keeps its projection marker until the projector has written its
    /// materialization job, so markers read before jobs miss no event moving between them.
    fn markers(&mut self, after: Option<Key>) -> BaseTurn {
        self.stage = BaseStage::Markers;
        Ok(Next::Call(IoCall::Page {
            space: PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: event_log_prefix(self.document_id),
            after,
            limit: MARKER_PAGE,
        }))
    }

    fn markers_read(&mut self, result: IoResult) -> BaseTurn {
        let (rows, next) = page_rows(result)?;
        let keys = rows.into_iter().map(|(key, _)| key).collect();
        self.next_marker(keys, next)
    }

    fn next_marker(&mut self, mut keys: Vec<Key>, next: Option<Key>) -> BaseTurn {
        if keys.is_empty() {
            return match next {
                Some(after) => self.markers(Some(after)),
                None => self.jobs(),
            };
        }
        let key = keys.remove(0);
        self.stage = BaseStage::Marker(keys, next);
        Ok(Next::Call(read_row(EVENT_LOG_KEYSPACE, key)))
    }

    /// A marker of an applied batch is only a deferred lifecycle mint; any other one is a
    /// received change the graph has not applied.
    fn marker_read(&mut self, result: IoResult, keys: Vec<Key>, next: Option<Key>) -> BaseTurn {
        let event = value(result)?
            .and_then(|bytes| postcard::from_bytes::<MetadataEventRecord>(&bytes).ok());
        let Some(MetadataEventPayload::ApplyBatch { batch, .. }) = event.map(|event| event.payload)
        else {
            return Err(self.pending());
        };
        self.stage = BaseStage::Dot(keys, next);
        let contains = MetadataEffect::ContainsDot {
            graph_iri: batch.graph_iri.clone(),
            actor: batch.actor,
            counter: batch.counter,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            contains,
        )))))
    }

    fn dot_read(&mut self, result: IoResult, keys: Vec<Key>, next: Option<Key>) -> BaseTurn {
        match effect_event(result)? {
            Event::Metadata(MetadataEvent::ContainsDotResult { contains: true, .. })
            // A node without the graph store has nothing to wait for.
            | Event::Metadata(MetadataEvent::Error {
                error: MetadataError::HandleMissing,
                ..
            }) => self.next_marker(keys, next),
            Event::Metadata(MetadataEvent::ContainsDotResult { contains: false, .. }) => {
                Err(self.pending())
            }
            Event::Metadata(MetadataEvent::Error { error, .. }) => {
                Err(MetadataError::Backend(error.to_string()).into())
            }
            other => Err(MetadataError::Backend(format!(
                "unexpected dot check answer: {}",
                other.label()
            ))
            .into()),
        }
    }

    fn jobs(&mut self) -> BaseTurn {
        self.stage = BaseStage::Jobs;
        Ok(Next::Call(IoCall::Page {
            space: DOCUMENT_JOB_KEYSPACE.to_string(),
            prefix: document_job_prefix(self.document_id),
            after: None,
            limit: 1,
        }))
    }
}

impl Operation for ReadyBaseOperation {
    type Output = ();
    type Error = UpdateDocumentError;

    fn first(&mut self) -> BaseTurn {
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
            StorageEffect::Last {
                key_space: EVENT_LOG_KEYSPACE.to_string(),
                prefix: Some(event_log_prefix(self.document_id)),
                txn_id: None,
            },
        )))))
    }

    fn next(&mut self, result: IoResult) -> BaseTurn {
        match std::mem::replace(&mut self.stage, BaseStage::Newest) {
            BaseStage::Newest => self.newest_read(result),
            BaseStage::Status => self.status_read(result),
            BaseStage::Markers => self.markers_read(result),
            BaseStage::Marker(keys, next) => self.marker_read(result, keys, next),
            BaseStage::Dot(keys, next) => self.dot_read(result, keys, next),
            BaseStage::Jobs => {
                let (queued, _) = page_rows(result)?;
                match queued.is_empty() {
                    true => Ok(Next::Done(())),
                    false => Err(self.pending()),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum EntryStage {
    Ready,
    Validate,
    Generation,
    Update,
    Cache,
}

/// A metadata document update from its entry. It holds this node's writes to the document, so
/// its actor's dots stay in order, checks a revision-checked base and the Profile verdict, and
/// caches the updated row unless the registry cache changed meanwhile.
#[derive(Debug, PartialEq)]
pub struct UpdateEntryOperation {
    operation: Option<UpdateDocumentOperation>,
    local_node: Option<NodeId>,
    stage: EntryStage,
    generation: Option<u64>,
    updated: Option<MetadataRegistryRecord>,
}

type EntryTurn = Result<Next<MetadataRegistryRecord>, UpdateDocumentError>;

impl UpdateEntryOperation {
    /// `local_node` is this node on the network, which a Profile fetch needs.
    pub fn new(operation: UpdateDocumentOperation, local_node: Option<NodeId>) -> Self {
        Self {
            operation: Some(operation),
            local_node,
            stage: EntryStage::Ready,
            generation: None,
            updated: None,
        }
    }

    fn update(&mut self) -> Result<&mut UpdateDocumentOperation, UpdateDocumentError> {
        self.operation
            .as_mut()
            .ok_or_else(|| MetadataError::Backend("the update was already started".into()).into())
    }

    fn validate(&mut self) -> EntryTurn {
        let local_node = self.local_node;
        let operation = self.update()?;
        let document_id = operation.config.document_id;
        match &operation.config.mutation {
            UpdateDocumentMutation::ReplaceRoCrate { jsonld } => {
                let assess = ProfileAssessOperation::write(
                    local_node,
                    document_id,
                    operation.config.group_id,
                    jsonld.clone(),
                );
                self.stage = EntryStage::Validate;
                Ok(Next::Call(IoCall::child(assess)))
            }
            UpdateDocumentMutation::UpsertDataEntity { .. }
            | UpdateDocumentMutation::UpsertContextualEntity { .. }
            | UpdateDocumentMutation::ReplaceDataEntity { .. }
            | UpdateDocumentMutation::RemoveDataEntity { .. }
            | UpdateDocumentMutation::ApplyBatch { .. }
            | UpdateDocumentMutation::Checkpoint => {
                operation.route_profile_status =
                    Some(stale_status(document_id, "dataset_revision_changed"));
                self.read_generation()
            }
        }
    }

    fn validated(&mut self, result: IoResult) -> EntryTurn {
        let assessed = result.child::<MetadataProfilePreview, MetadataError>()?;
        let operation = self.update()?;
        let jsonld = match &operation.config.mutation {
            UpdateDocumentMutation::ReplaceRoCrate { jsonld } => jsonld.as_str(),
            _ => "",
        };
        let status = submission_verdict(assessed, operation.config.document_id, jsonld)?;
        operation.route_profile_status = Some(status);
        self.read_generation()
    }

    /// The cache generation before the update, so a fill that started meanwhile wins.
    fn read_generation(&mut self) -> EntryTurn {
        self.stage = EntryStage::Generation;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            MetadataEffect::CacheGeneration,
        )))))
    }

    fn generation_read(&mut self, result: IoResult) -> EntryTurn {
        // A node without the metadata handle keeps no cache.
        if let Event::Metadata(MetadataEvent::CacheGeneration { generation }) =
            effect_event(result)?
        {
            self.generation = Some(generation);
        }
        let operation = self
            .operation
            .take()
            .ok_or_else(|| MetadataError::Backend("the update was already started".into()))?;
        self.stage = EntryStage::Update;
        Ok(Next::Call(IoCall::child(operation)))
    }

    fn updated(&mut self, result: IoResult) -> EntryTurn {
        let updated = result.child::<MetadataRegistryRecord, UpdateDocumentError>()??;
        let Some(generation) = self.generation else {
            return Ok(Next::Done(updated));
        };
        let cache = MetadataEffect::CacheRecords {
            records: vec![updated.clone()],
            generation: Some(generation),
        };
        self.updated = Some(updated);
        self.stage = EntryStage::Cache;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            cache,
        )))))
    }
}

impl Operation for UpdateEntryOperation {
    type Output = MetadataRegistryRecord;
    type Error = UpdateDocumentError;

    fn hold_scope(&self) -> Option<HoldScope> {
        let operation = self.operation.as_ref()?;
        Some(HoldScope::DocumentWrite(operation.config.document_id))
    }

    fn first(&mut self) -> EntryTurn {
        let operation = self.update()?;
        let ready = (operation.config.expected_revision)
            .filter(|_| plans_on_graph(&operation.config.mutation))
            .map(|expected| ReadyBaseOperation::new(operation.config.document_id, expected));
        match ready {
            Some(ready) => Ok(Next::Call(IoCall::child(ready))),
            None => self.validate(),
        }
    }

    fn next(&mut self, result: IoResult) -> EntryTurn {
        match self.stage {
            EntryStage::Ready => {
                result.child::<(), UpdateDocumentError>()??;
                self.validate()
            }
            EntryStage::Validate => self.validated(result),
            EntryStage::Generation => self.generation_read(result),
            EntryStage::Update => self.updated(result),
            EntryStage::Cache => {
                // The row is committed; a cache that missed it refills from storage.
                let _ = effect_event(result);
                let updated = self
                    .updated
                    .take()
                    .ok_or_else(|| MetadataError::Backend("the updated row is missing".into()))?;
                Ok(Next::Done(updated))
            }
        }
    }
}

#[cfg(test)]
#[path = "update_entry_tests.rs"]
mod tests;
