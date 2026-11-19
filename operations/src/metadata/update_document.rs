//! Runs the update operation that plans a document change and logs its event.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    CREATE_ACCEPTANCE_KEYSPACE, EVENT_LOG_KEYSPACE, EVENT_SIZE_KEYSPACE, METADATA_ACTOR_KEYSPACE,
    METADATA_CHECKPOINT_KEYSPACE, METADATA_INDEX_KEYSPACE, RAW_BUDGET_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
};
use aruna_core::metadata::{
    EVENT_LIMIT, MaterializationStatusRecord, MetadataActor, MetadataBatch, MetadataBatchSource,
    MetadataEffect, MetadataError, MetadataEvent, MetadataEventPayload, MetadataEventRecord,
    MetadataLifecycleRecord, MetadataMaterializationRecord, ProfileValidationStatus,
    RAW_BYTES_LIMIT, RawOriginBudget, raw_quotas,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_runs::{SourceEvent, SourceKind, journal_entries};
use aruna_core::storage_entries::{
    commit_message_entry, create_acceptance_key, document_lifecycle_entry, event_log_key,
    event_log_prefix, metadata_actor_entry, metadata_actor_key, metadata_registry_key,
    profile_validation_entry, raw_budget_entry, raw_budget_key, sync_revision_entry,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::{
    MetadataAuditRecord, MetadataRegistryRecord,
};
use aruna_core::types::{GroupId, Key, Value};
use byteview::ByteView;
use craqle::GraphReplicaSnapshot;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::MetadataPhaseSource;
use crate::metadata::call_results::{effect_event, page_rows, read_values};
use crate::metadata::profile::rules::{
    batch_adds_profile, not_profiled_status, profile_path_rule, stale_status, stamp_unprofiled,
    submission_profile_tag,
};
use crate::metadata::repository::{
    create_outbox_record, event_projection_entries, registry_outbox_record,
    schedule_materialization,
};
use crate::sync::outbox_record::{outbox_write_entry, schedule_drain_effect};
use crate::sync::shard_placement::sort_node_ids;

const RAW_EVENT_LIMIT: usize = EVENT_LIMIT as usize;

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateDocumentConfig {
    pub actor: aruna_core::structs::identity::auth::Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub public: bool,
    pub mutation: UpdateDocumentMutation,
    /// The newest event the caller based the change on; any later event fails the update.
    pub expected_revision: Option<Ulid>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UpdateDocumentMutation {
    ReplaceRoCrate {
        jsonld: String,
    },
    UpsertDataEntity {
        jsonld: String,
    },
    UpsertContextualEntity {
        jsonld: String,
    },
    /// Replaces one data entity by `@id`, so the properties it omits go.
    ReplaceDataEntity {
        jsonld: String,
    },
    /// Removes one root-linked `File` entity by `@id` and the root's link to it.
    RemoveDataEntity {
        id: String,
    },
    /// A batch a device planned against its own replica. It is appended
    /// verbatim, under the device's actor, so both replicas converge on it.
    ApplyBatch {
        batch: Box<MetadataBatch>,
        authored: MetadataBatchSource,
    },
    /// Opens a new history window with this holder's current graph state.
    Checkpoint,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateDocumentError {
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("metadata raw update budget exceeded")]
    RawLimit,
    #[error("the document's bucket cut over to a new holder set; retry the update")]
    PlacementFenced,
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("the document changed since revision {expected}; it is now at {current}")]
    RevisionConflict { expected: Ulid, current: Ulid },
    #[error("the graph here has not applied revision {expected} yet; retry")]
    GraphPending { expected: Ulid },
    #[error("the metadata graph changed since the version the update was based on")]
    GraphChanged,
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for UpdateDocumentError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Conflict => {
                Self::StorageError(aruna_core::errors::StorageError::TransactionConflict)
            }
            other => Self::Io(other),
        }
    }
}

/// The batch source a mutation plans from; a checkpoint plans none.
fn batch_source(mutation: &UpdateDocumentMutation) -> Option<MetadataBatchSource> {
    match mutation {
        UpdateDocumentMutation::ReplaceRoCrate { jsonld } => {
            Some(MetadataBatchSource::ReplaceRoCrate {
                jsonld: jsonld.clone(),
            })
        }
        UpdateDocumentMutation::UpsertDataEntity { jsonld } => {
            Some(MetadataBatchSource::UpsertDataEntity {
                jsonld: jsonld.clone(),
            })
        }
        UpdateDocumentMutation::UpsertContextualEntity { jsonld } => {
            Some(MetadataBatchSource::UpsertContextualEntity {
                jsonld: jsonld.clone(),
            })
        }
        UpdateDocumentMutation::ReplaceDataEntity { jsonld } => {
            Some(MetadataBatchSource::ReplaceDataEntity {
                jsonld: jsonld.clone(),
            })
        }
        UpdateDocumentMutation::RemoveDataEntity { id } => {
            Some(MetadataBatchSource::RemoveDataEntity { id: id.clone() })
        }
        UpdateDocumentMutation::ApplyBatch { authored, .. } => Some(authored.clone()),
        UpdateDocumentMutation::Checkpoint => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum UpdateStage {
    /// Reading the registry row outside any transaction.
    Record,
    /// Reading the realm config and, for a planned change, this node's actor.
    Config,
    /// Taking a checkpoint's graph state.
    Snapshot,
    /// Planning the batch against the local graph.
    Plan,
    /// Running the transactional commit as a child.
    Commit,
}

type UpdateTurn = Result<Next<MetadataRegistryRecord>, UpdateDocumentError>;

/// Validates a metadata update outside a transaction, then commits it as a write child.
/// Success means the event and its projection work are durable, not that the graph converged.
#[derive(Debug, PartialEq)]
pub struct UpdateDocumentOperation {
    pub(super) config: UpdateDocumentConfig,
    /// Minted before the batch is planned: it is the batch actor as well as the
    /// event id, so a plan can never be attributed to another event.
    event_id: Ulid,
    record: Option<MetadataRegistryRecord>,
    planned_batch: Option<MetadataBatch>,
    /// This node's actor after this update's dot, written with the event.
    actor: Option<MetadataActor>,
    /// A checkpoint's graph state; `Some(None)` when too large to carry.
    snapshot: Option<Option<Box<GraphReplicaSnapshot>>>,
    realm_config: Option<RealmConfigDocument>,
    pub(super) route_profile_status: Option<ProfileValidationStatus>,
    /// The graph version the caller based its change on; any other refuses the update.
    expected_graph: Option<String>,
    /// The author's commit message for the ARC snapshot of this update.
    message: Option<String>,
    /// Phase-time and identity sampling; production keeps the defaults.
    phase_source: MetadataPhaseSource,
    stage: UpdateStage,
}

impl UpdateDocumentOperation {
    pub fn new(config: UpdateDocumentConfig) -> Self {
        let route_profile_status = match &config.mutation {
            UpdateDocumentMutation::ReplaceRoCrate { jsonld }
                if !submission_profile_tag(jsonld) =>
            {
                Some(not_profiled_status(config.document_id))
            }
            UpdateDocumentMutation::ReplaceRoCrate { .. } => None,
            UpdateDocumentMutation::UpsertDataEntity { .. }
            | UpdateDocumentMutation::UpsertContextualEntity { .. }
            | UpdateDocumentMutation::ReplaceDataEntity { .. }
            | UpdateDocumentMutation::RemoveDataEntity { .. }
            | UpdateDocumentMutation::ApplyBatch { .. }
            | UpdateDocumentMutation::Checkpoint => {
                Some(stale_status(config.document_id, "dataset_revision_changed"))
            }
        };
        let planned_batch = match &config.mutation {
            UpdateDocumentMutation::ApplyBatch { batch, .. } => Some((**batch).clone()),
            _ => None,
        };
        Self {
            config,
            // The event identity is minted from the phase source on the first
            // real transition, so replacing the source before `first` is not
            // shadowed by an incidental constructor sample.
            event_id: Ulid::nil(),
            record: None,
            planned_batch,
            actor: None,
            snapshot: None,
            realm_config: None,
            route_profile_status,
            expected_graph: None,
            message: None,
            phase_source: MetadataPhaseSource::default(),
            stage: UpdateStage::Record,
        }
    }

    /// Refuses the update unless it is planned against the graph at `version`, so a change
    /// computed from an older graph never removes values that arrived since.
    pub(crate) fn with_expected_graph(mut self, version: String) -> Self {
        self.expected_graph = Some(version);
        self
    }

    /// Records `message` as the commit message of this update's ARC snapshot.
    pub fn with_message(mut self, message: Option<String>) -> Self {
        self.message = message;
        self
    }

    /// Replaces the phase-time and identity source and re-mints the event
    /// identity from it. Set before `first`; production never calls this.
    #[cfg(test)]
    pub(crate) fn with_phase_source(mut self, phase_source: MetadataPhaseSource) -> Self {
        self.phase_source = phase_source;
        self.event_id = phase_source.next_id();
        self
    }

    /// Plans the change set against the local graph. The planner runs the same
    /// structural validation the applying call would, so this replaces it.
    fn plan_batch_effect(
        &self,
        record: &MetadataRegistryRecord,
    ) -> Result<Option<Effect>, MetadataError> {
        match &self.config.mutation {
            // A device already planned its batch, so there is nothing to plan.
            UpdateDocumentMutation::ApplyBatch { .. } | UpdateDocumentMutation::Checkpoint => {
                return Ok(None);
            }
            UpdateDocumentMutation::ReplaceRoCrate { .. } => {}
            UpdateDocumentMutation::UpsertDataEntity { jsonld }
            | UpdateDocumentMutation::UpsertContextualEntity { jsonld }
            | UpdateDocumentMutation::ReplaceDataEntity { jsonld } => {
                validate_entity_jsonld(jsonld)?;
            }
            UpdateDocumentMutation::RemoveDataEntity { id } if id.trim().is_empty() => {
                return Err(MetadataError::InvalidInput(
                    "a removal names the entity's @id".to_string(),
                ));
            }
            UpdateDocumentMutation::RemoveDataEntity { .. } => {}
        }
        let Some(actor) = self.actor.as_ref() else {
            return Err(MetadataError::Backend(
                "metadata actor is missing before planning".to_string(),
            ));
        };
        Ok(Some(Effect::Metadata(MetadataEffect::PlanBatch {
            graph_iri: record.graph_iri.clone(),
            actor: actor.actor,
            counter: actor.counter,
            source: batch_source(&self.config.mutation)
                .ok_or_else(|| MetadataError::Backend("a checkpoint plans no batch".to_string()))?,
        })))
    }

    fn record(&self) -> Result<&MetadataRegistryRecord, UpdateDocumentError> {
        self.record
            .as_ref()
            .ok_or(UpdateDocumentError::DocumentNotFound)
    }

    /// Whether the mutation plans against this node's actor; a device batch and a checkpoint
    /// do not.
    fn plans_with_actor(&self) -> bool {
        !matches!(
            self.config.mutation,
            UpdateDocumentMutation::ApplyBatch { .. } | UpdateDocumentMutation::Checkpoint
        )
    }

    fn record_read(&mut self, result: IoResult) -> UpdateTurn {
        let bytes = result
            .value()?
            .ok_or(UpdateDocumentError::DocumentNotFound)?;
        let record: MetadataRegistryRecord =
            postcard::from_bytes(&bytes).map_err(ConversionError::from)?;
        let mut reads = vec![(
            REALM_CONFIG_KEYSPACE.to_string(),
            ByteView::from(*record.realm_id.as_bytes()),
        )];
        if self.plans_with_actor() {
            reads.push((
                METADATA_ACTOR_KEYSPACE.to_string(),
                metadata_actor_key(self.config.document_id),
            ));
        }
        self.record = Some(record);
        self.stage = UpdateStage::Config;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn config_read(&mut self, result: IoResult) -> UpdateTurn {
        let mut values = result.values()?.into_iter();
        if let Some(bytes) = values.next().flatten() {
            self.realm_config = Some(RealmConfigDocument::from_bytes(&bytes)?);
        }
        if let UpdateDocumentMutation::Checkpoint = self.config.mutation {
            let graph_iri = self.record()?.graph_iri.clone();
            self.stage = UpdateStage::Snapshot;
            let snapshot = Effect::Metadata(MetadataEffect::GraphSnapshot { graph_iri });
            return Ok(Next::Call(IoCall::Effect(Box::new(snapshot))));
        }
        if !self.plans_with_actor() {
            return self.commit();
        }
        self.take_actor(values.next().flatten())
    }

    /// Takes this node's next dot for the document. The event id is raised above the
    /// node's previous one, so materialization applies the actor's dots in order.
    fn take_actor(&mut self, value: Option<Value>) -> UpdateTurn {
        let current = value
            .as_deref()
            .map(postcard::from_bytes::<MetadataActor>)
            .transpose()
            .map_err(ConversionError::from)?;
        // Materialization runs a document's events in id order, so a new event
        // must sort after every event its plan may depend on.
        let floor = self
            .record
            .as_ref()
            .map(|record| record.last_event_id)
            .into_iter()
            .chain(current.as_ref().map(|current| current.last_event_id))
            .max();
        if let Some(floor) = floor
            && self.event_id <= floor
        {
            self.event_id = floor
                .increment()
                .map_err(|_| UpdateDocumentError::RawLimit)?;
        }
        self.actor = MetadataActor::next(
            current.as_ref(),
            self.config.document_id,
            self.config.actor.node_id,
            self.event_id,
        );
        if self.actor.is_none() {
            return Err(UpdateDocumentError::RawLimit);
        }
        let record = self.record()?.clone();
        match self.plan_batch_effect(&record)? {
            Some(effect) => {
                self.stage = UpdateStage::Plan;
                Ok(Next::Call(IoCall::Effect(Box::new(effect))))
            }
            None => self.commit(),
        }
    }

    /// Keeps graph state small enough that the checkpoint fits a fresh window.
    fn snapshot_taken(&mut self, result: IoResult) -> UpdateTurn {
        match effect_event(result)? {
            Event::Metadata(MetadataEvent::GraphSnapshotResult { snapshot, .. }) => {
                let fits = postcard::experimental::serialized_size(&snapshot)
                    .is_ok_and(|size| size as u64 <= RAW_BYTES_LIMIT / 4);
                self.snapshot = Some(fits.then_some(snapshot));
                self.commit()
            }
            Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
            other => Err(unexpected("the checkpoint graph state", &other)),
        }
    }

    fn batch_planned(&mut self, result: IoResult) -> UpdateTurn {
        match effect_event(result)? {
            Event::Metadata(MetadataEvent::BatchPlanned { batch, .. }) => {
                if let Some(expected) = &self.expected_graph
                    && aruna_core::metadata::graph_version(&batch.base_clock) != *expected
                {
                    return Err(UpdateDocumentError::GraphChanged);
                }
                self.planned_batch = Some(batch);
                self.commit()
            }
            Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
            other => Err(unexpected("the metadata batch plan", &other)),
        }
    }

    /// Hands the planned change to the transactional commit.
    fn commit(&mut self) -> UpdateTurn {
        let commit = self.commit_operation()?;
        self.stage = UpdateStage::Commit;
        Ok(Next::Call(IoCall::child(commit)))
    }

    /// The transactional commit of the planned change, after the Profile path rule.
    fn commit_operation(&mut self) -> Result<UpdateCommitOperation, UpdateDocumentError> {
        if let (Some(record), Some(batch)) = (&self.record, &self.planned_batch) {
            profile_path_rule(&record.document_path, || batch_adds_profile(batch))?;
        }
        Ok(UpdateCommitOperation {
            config: self.config.clone(),
            event_id: self.event_id,
            update_event: None,
            planned_batch: self.planned_batch.take(),
            actor: self.actor.take(),
            snapshot: self.snapshot.take(),
            realm_config: self.realm_config.take(),
            route_profile_status: self.route_profile_status.take(),
            message: self.message.clone(),
            phase_source: self.phase_source,
            raw_budget: None,
            accepted_create: None,
            window: None,
            opening: None,
            fenced: Vec::new(),
            stage: CommitStage::Registry,
            now_ms: 0,
        })
    }

    /// After the commit: wake the materialization queue and the sync outbox drain. Their
    /// durable rows cover a failed wake.
    fn committed(&mut self, result: IoResult) -> UpdateTurn {
        let record = result.child::<MetadataRegistryRecord, UpdateDocumentError>()??;
        Ok(Next::Then {
            output: record,
            calls: vec![
                IoCall::Effect(Box::new(schedule_materialization())),
                IoCall::Effect(Box::new(schedule_drain_effect())),
            ],
        })
    }
}

impl Operation for UpdateDocumentOperation {
    type Output = MetadataRegistryRecord;
    type Error = UpdateDocumentError;

    fn first(&mut self) -> UpdateTurn {
        if self.event_id.is_nil() {
            self.event_id = self.phase_source.next_id();
        }
        let key = metadata_registry_key(self.config.group_id, self.config.document_id);
        let read = vec![(METADATA_INDEX_KEYSPACE.to_string(), key)];
        Ok(Next::Call(IoCall::Read(read)))
    }

    fn next(&mut self, result: IoResult) -> UpdateTurn {
        match self.stage {
            UpdateStage::Record => self.record_read(result),
            UpdateStage::Config => self.config_read(result),
            UpdateStage::Snapshot => self.snapshot_taken(result),
            UpdateStage::Plan => self.batch_planned(result),
            UpdateStage::Commit => self.committed(result),
        }
    }
}

fn unexpected(expected: &'static str, event: &Event) -> UpdateDocumentError {
    UpdateDocumentError::Io(IoError::Unexpected {
        expected,
        got: event.label(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CommitStage {
    /// Re-reading the registry row inside the transaction.
    Registry,
    /// Reading the latest checkpoint, which opens the current history window.
    Window,
    /// Reading this node's budget, the accepted create, the fences and the window's opening.
    Sidecars,
    /// Reading the history window's event sizes.
    History,
    /// Writing the event, its projection work and the sidecars.
    Write,
}

type CommitTurn = Result<Next<MetadataRegistryRecord>, UpdateDocumentError>;

/// Writes one planned update in a single write transaction, behind the bucket fences and this
/// node's raw budget, and answers with the updated registry row.
#[derive(Debug, PartialEq)]
pub struct UpdateCommitOperation {
    config: UpdateDocumentConfig,
    event_id: Ulid,
    update_event: Option<MetadataEventRecord>,
    planned_batch: Option<MetadataBatch>,
    actor: Option<MetadataActor>,
    snapshot: Option<Option<Box<GraphReplicaSnapshot>>>,
    realm_config: Option<RealmConfigDocument>,
    route_profile_status: Option<ProfileValidationStatus>,
    message: Option<String>,
    phase_source: MetadataPhaseSource,
    raw_budget: Option<RawOriginBudget>,
    accepted_create: Option<MetadataEventRecord>,
    /// The latest checkpoint; raw budgets count events from it on.
    window: Option<Ulid>,
    /// That checkpoint's event, which sets the window's quota.
    opening: Option<MetadataEventRecord>,
    /// Buckets this update publishes onto and the activation generation each
    /// resolved at, read as a fence inside the write transaction.
    fenced: Vec<(PlacementRef, u64)>,
    stage: CommitStage,
    now_ms: u64,
}

impl UpdateCommitOperation {
    fn updated_record(&self, mut record: MetadataRegistryRecord) -> MetadataRegistryRecord {
        record.public = self.config.public;
        record.updated_at_ms = self.phase_source.now_ms();
        record
    }

    fn update_event_payload(&self) -> Result<MetadataEventPayload, UpdateDocumentError> {
        if let UpdateDocumentMutation::Checkpoint = self.config.mutation {
            let Some(snapshot) = self.snapshot.clone() else {
                return Err(MetadataError::Backend(
                    "checkpoint graph state is missing before commit".to_string(),
                )
                .into());
            };
            return Ok(MetadataEventPayload::Checkpoint { snapshot });
        }
        let (Some(batch), Some(authored)) = (
            self.planned_batch.clone(),
            batch_source(&self.config.mutation),
        ) else {
            return Err(MetadataError::Backend(
                "metadata batch is missing before update commit".to_string(),
            )
            .into());
        };
        Ok(MetadataEventPayload::ApplyBatch { batch, authored })
    }

    fn update_event_record(
        &self,
        record: &MetadataRegistryRecord,
    ) -> Result<MetadataEventRecord, UpdateDocumentError> {
        let mut record = record.clone();
        record.last_event_id = self.event_id;
        let occurred_at_ms = record.updated_at_ms;
        Ok(MetadataEventRecord {
            event_id: self.event_id,
            record,
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            payload: self.update_event_payload()?,
            occurred_at_ms,
        })
    }

    fn update_event(&self) -> Result<&MetadataEventRecord, UpdateDocumentError> {
        self.update_event.as_ref().ok_or_else(|| {
            MetadataError::Backend("the update event is missing before commit".to_string()).into()
        })
    }

    fn audit_record(&self, event: &MetadataEventRecord) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id: event.record.realm_id,
            group_id: event.record.group_id,
            document_id: event.record.document_id,
            graph_iri: event.record.graph_iri.clone(),
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            operation: event.payload.audit_operation(),
            occurred_at_ms: event.occurred_at_ms,
            details: Some(audit_details(
                event.payload.materialization_kind(),
                self.message.as_deref(),
            )),
        }
    }

    /// The buckets this update publishes onto and the generation each resolves
    /// at. Empty before the realm's first candidate map, where no activation
    /// exists and no transition can be in flight.
    fn fenced_buckets(&self, record: &MetadataRegistryRecord) -> Vec<(PlacementRef, u64)> {
        let Some(config) = self.realm_config.as_ref() else {
            return Vec::new();
        };
        [
            record.placement,
            aruna_core::placement::registry_placement(config, record),
        ]
        .into_iter()
        .filter_map(|placement| {
            let generation = aruna_core::placement::fence::write_generation(config, &placement)?;
            Some((placement, generation))
        })
        .collect()
    }

    fn generation_of(&self, placement: &PlacementRef) -> u64 {
        self.fenced
            .iter()
            .find(|(bucket, _)| bucket == placement)
            .map_or(0, |(_, generation)| *generation)
    }

    fn update_rows(
        &self,
        next_raw_budget: &RawOriginBudget,
    ) -> Result<Vec<(String, Key, Value)>, UpdateDocumentError> {
        let event = self.update_event()?;
        let now = self.phase_source.now_ms();
        let audit = self.audit_record(event);
        // Updating an existing document is a mutation, not an origin write, so it
        // never mints the lifecycle sync topic genesis.
        let message = self.message.as_deref();
        let lifecycle_outbox =
            create_outbox_record(event, message, self.realm_config.as_ref(), false)
                .fenced_at(self.generation_of(&event.record.placement));
        let outbox = (!event.record.holder_node_ids.is_empty()).then_some(&lifecycle_outbox);
        let status = MaterializationStatusRecord::pending(event, now);
        let job = MetadataMaterializationRecord::new(event, now);
        let mut writes = event_projection_entries(event, &audit, outbox, &status, &job)?;
        // Refresh the everywhere-bound registry row so non-holders see the new
        // revision, not just the bucket's holders.
        if let Some(registry_outbox) =
            registry_outbox_record(event, self.realm_config.as_ref(), false).map(|record| {
                let generation = self.generation_of(&record.placement);
                record.fenced_at(generation)
            })
        {
            writes.push(outbox_write_entry(&registry_outbox).map_err(ConversionError::from)?);
        }
        if let Some(message) = message {
            writes.push(commit_message_entry(event, message)?);
        }
        let lifecycle = MetadataLifecycleRecord::for_event(event.clone(), self.message.clone());
        writes.push(document_lifecycle_entry(&lifecycle)?);
        // The accepting node journals a content change for plugins in its commit batch.
        if let Some(updated) = SourceEvent::from_metadata(event, now)
            && updated.kind == SourceKind::DatasetUpdated
        {
            writes.extend(journal_entries(&updated)?);
        }
        if outbox.is_none() {
            let aruna_core::document::DocumentOutboxEvent::Upsert { change, .. } =
                lifecycle_outbox.event
            else {
                unreachable!("metadata lifecycle update outbox must be an upsert");
            };
            writes.push(sync_revision_entry(&lifecycle_outbox.target, &change)?);
        }
        writes.push(raw_budget_entry(next_raw_budget)?);
        if let Some(actor) = &self.actor {
            writes.push(metadata_actor_entry(actor)?);
        }
        let Some(mut profile_status) = self.route_profile_status.clone() else {
            return Err(MetadataError::Backend(
                "profile validation status is missing before update commit".to_string(),
            )
            .into());
        };
        profile_status.document_id = event.record.document_id;
        profile_status.dataset_revision = event.event_id;
        stamp_unprofiled(&mut profile_status, self.now_ms);
        // The merged render is only known once the batch materializes, so the
        // accepted status carries no digest to be fresh against yet.
        profile_status.dataset_digest = None;
        writes.push(profile_validation_entry(&profile_status)?);
        Ok(writes)
    }

    fn origin_quota(
        &self,
        event: &MetadataEventRecord,
    ) -> Result<RawOriginBudget, UpdateDocumentError> {
        if event.record.document_id != self.config.document_id
            || event.record.establishing_event_id != event.event_id
            || event.record.last_event_id != event.event_id
            || !matches!(
                event.payload,
                MetadataEventPayload::Scaffold { .. } | MetadataEventPayload::RoCrate { .. }
            )
        {
            return Err(UpdateDocumentError::RawLimit);
        }
        let mut origins = event.record.holder_node_ids.clone();
        if origins.is_empty() {
            return Err(UpdateDocumentError::RawLimit);
        }
        let original = origins.clone();
        sort_node_ids(&mut origins);
        if origins != original {
            return Err(UpdateDocumentError::RawLimit);
        }
        let encoded_bytes = postcard::experimental::serialized_size(event)
            .map_err(|_| UpdateDocumentError::RawLimit)
            .and_then(|size| u64::try_from(size).map_err(|_| UpdateDocumentError::RawLimit))?;
        raw_quotas(
            event.record.document_id,
            &origins,
            event.node_id,
            encoded_bytes,
        )
        .and_then(|budgets| {
            budgets
                .into_iter()
                .find(|budget| budget.node_id == self.config.actor.node_id)
        })
        .ok_or(UpdateDocumentError::RawLimit)
    }

    /// A checkpoint splits the create's origins' quotas anew, like a create does.
    fn window_quota(
        &self,
        checkpoint: &MetadataEventRecord,
    ) -> Result<RawOriginBudget, UpdateDocumentError> {
        let Some(create) = self.accepted_create.as_ref() else {
            return Err(UpdateDocumentError::RawLimit);
        };
        if checkpoint.record.document_id != self.config.document_id
            || !matches!(checkpoint.payload, MetadataEventPayload::Checkpoint { .. })
        {
            return Err(UpdateDocumentError::RawLimit);
        }
        let encoded_bytes = postcard::experimental::serialized_size(checkpoint)
            .map_err(|_| UpdateDocumentError::RawLimit)
            .and_then(|size| u64::try_from(size).map_err(|_| UpdateDocumentError::RawLimit))?;
        raw_quotas(
            checkpoint.record.document_id,
            &create.record.holder_node_ids,
            checkpoint.node_id,
            encoded_bytes,
        )
        .and_then(|budgets| {
            budgets
                .into_iter()
                .find(|budget| budget.node_id == self.config.actor.node_id)
        })
        .ok_or(UpdateDocumentError::RawLimit)
    }

    fn valid_budget(&self, budget: &RawOriginBudget, quota: &RawOriginBudget) -> bool {
        budget.document_id == self.config.document_id
            && budget.node_id == self.config.actor.node_id
            && budget.event_limit == quota.event_limit
            && budget.byte_limit == quota.byte_limit
            && budget.events >= quota.events
            && budget.encoded_bytes >= quota.encoded_bytes
            && budget.events <= budget.event_limit
            && budget.encoded_bytes <= budget.byte_limit
    }

    fn history_budget(
        &self,
        values: &[(ByteView, ByteView)],
        next_start_after: Option<&ByteView>,
    ) -> Result<(RawOriginBudget, usize, u64), UpdateDocumentError> {
        if values.len() > RAW_EVENT_LIMIT || next_start_after.is_some() {
            return Err(UpdateDocumentError::RawLimit);
        }
        let Some(create) = self.accepted_create.as_ref() else {
            return Err(UpdateDocumentError::RawLimit);
        };
        // A window opens at its checkpoint, which then stands in for the create.
        let opening = match self.window {
            Some(checkpoint) => {
                let opening = self.opening.as_ref().ok_or(UpdateDocumentError::RawLimit)?;
                let first = values
                    .first()
                    .and_then(|(key, _)| key.get(16..32))
                    .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                    .map(Ulid::from_bytes);
                if opening.event_id != checkpoint || first != Some(checkpoint) {
                    return Err(UpdateDocumentError::RawLimit);
                }
                opening
            }
            None => create,
        };
        let quota = match self.window {
            Some(_) => self.window_quota(opening)?,
            None => self.origin_quota(create)?,
        };
        let prefix = event_log_prefix(self.config.document_id);
        let mut events = 0u32;
        let mut encoded_bytes = 0u64;
        let mut total_bytes = 0u64;
        let mut saw_create = false;
        for (key, value) in values {
            let size: aruna_core::metadata::EventSize =
                postcard::from_bytes(value).map_err(|_| UpdateDocumentError::RawLimit)?;
            if key.len() != 32 || !key.starts_with(&prefix) {
                return Err(UpdateDocumentError::RawLimit);
            }
            total_bytes = total_bytes
                .checked_add(size.bytes)
                .ok_or(UpdateDocumentError::RawLimit)?;
            if total_bytes > RAW_BYTES_LIMIT {
                return Err(UpdateDocumentError::RawLimit);
            }
            if key.get(16..32) == Some(opening.event_id.to_bytes().as_slice()) {
                saw_create = true;
            }
            if size.node_id == self.config.actor.node_id {
                events = events.checked_add(1).ok_or(UpdateDocumentError::RawLimit)?;
                encoded_bytes = encoded_bytes
                    .checked_add(size.bytes)
                    .ok_or(UpdateDocumentError::RawLimit)?;
            }
        }
        if !saw_create || events > quota.event_limit || encoded_bytes > quota.byte_limit {
            return Err(UpdateDocumentError::RawLimit);
        }
        Ok((
            RawOriginBudget {
                document_id: quota.document_id,
                node_id: quota.node_id,
                event_limit: quota.event_limit,
                byte_limit: quota.byte_limit,
                events,
                encoded_bytes,
            },
            values.len(),
            total_bytes,
        ))
    }

    fn check_raw_budget(
        &self,
        history_events: usize,
        history_bytes: u64,
        quota: &RawOriginBudget,
    ) -> Result<RawOriginBudget, UpdateDocumentError> {
        let event = self.update_event()?;
        if history_events >= RAW_EVENT_LIMIT {
            return Err(UpdateDocumentError::RawLimit);
        }
        let Some(budget) = self.raw_budget.as_ref() else {
            return Err(UpdateDocumentError::RawLimit);
        };
        if !self.valid_budget(budget, quota) || budget.events >= budget.event_limit {
            return Err(UpdateDocumentError::RawLimit);
        }
        let event_bytes =
            postcard::experimental::serialized_size(event).map_err(ConversionError::from)?;
        let event_bytes = u64::try_from(event_bytes).map_err(|_| UpdateDocumentError::RawLimit)?;
        if history_bytes
            .checked_add(event_bytes)
            .is_none_or(|bytes| bytes > RAW_BYTES_LIMIT)
        {
            return Err(UpdateDocumentError::RawLimit);
        }
        let encoded_bytes = budget
            .encoded_bytes
            .checked_add(event_bytes)
            .ok_or(UpdateDocumentError::RawLimit)?;
        if encoded_bytes > budget.byte_limit {
            return Err(UpdateDocumentError::RawLimit);
        }
        Ok(RawOriginBudget {
            document_id: budget.document_id,
            node_id: budget.node_id,
            event_limit: budget.event_limit,
            byte_limit: budget.byte_limit,
            events: budget
                .events
                .checked_add(1)
                .ok_or(UpdateDocumentError::RawLimit)?,
            encoded_bytes,
        })
    }
    fn registry_read(&mut self, result: IoResult) -> CommitTurn {
        let bytes = result
            .value()?
            .ok_or(UpdateDocumentError::DocumentNotFound)?;
        let record: MetadataRegistryRecord =
            postcard::from_bytes(&bytes).map_err(ConversionError::from)?;
        let record = self.updated_record(record);
        self.update_event = Some(self.update_event_record(&record)?);
        self.fenced = self.fenced_buckets(&record);
        self.stage = CommitStage::Window;
        let last = StorageEffect::Last {
            key_space: METADATA_CHECKPOINT_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(self.config.document_id)),
            txn_id: None,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(last)))))
    }

    fn window_read(&mut self, result: IoResult) -> CommitTurn {
        let (values, _) = page_rows(result)?;
        self.window = match values.first() {
            Some((key, _)) => Some(
                key.get(16..32)
                    .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                    .map(Ulid::from_bytes)
                    .ok_or(UpdateDocumentError::RawLimit)?,
            ),
            None => None,
        };
        let realm_id = self.update_event()?.record.realm_id;
        let mut reads = vec![
            (
                RAW_BUDGET_KEYSPACE.to_string(),
                raw_budget_key(self.config.document_id, self.config.actor.node_id),
            ),
            (
                CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                create_acceptance_key(self.config.document_id),
            ),
        ];
        // The fence joins this transaction's read set, so a
        // departing holder's close conflicts an uncommitted write.
        reads.extend(
            self.fenced.iter().map(|(placement, _)| {
                aruna_core::placement::fence::fence_read(&realm_id, placement)
            }),
        );
        reads.extend(self.window.map(|checkpoint| {
            (
                EVENT_LOG_KEYSPACE.to_string(),
                event_log_key(self.config.document_id, checkpoint),
            )
        }));
        self.stage = CommitStage::Sidecars;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Checks the fences, the accepted create and this node's budget row.
    fn sidecars_read(&mut self, result: IoResult) -> CommitTurn {
        let values = read_values(result)?;
        let [raw_budget, accepted_create, rest @ ..] = values.as_slice() else {
            return Err(UpdateDocumentError::Io(IoError::Unexpected {
                expected: "the raw sidecar values",
                got: "fewer values",
            }));
        };
        let (fences, opening) = match self.window {
            Some(_) => match rest.split_last() {
                Some((opening, fences)) => (fences, opening.clone()),
                None => return Err(UpdateDocumentError::RawLimit),
            },
            None => (rest, None),
        };
        if self.window.is_some() {
            self.opening = match opening
                .as_deref()
                .map(postcard::from_bytes::<MetadataEventRecord>)
            {
                Some(Ok(checkpoint)) => Some(checkpoint),
                _ => return Err(UpdateDocumentError::RawLimit),
            };
        }
        if fences.len() != self.fenced.len() {
            return Err(UpdateDocumentError::Io(IoError::Unexpected {
                expected: "one fence value per fenced bucket",
                got: "another count of fence values",
            }));
        }
        let admitted = self
            .fenced
            .iter()
            .zip(fences)
            .all(|((_, generation), value)| {
                aruna_core::placement::fence::admits(value.as_ref(), *generation)
            });
        if !admitted {
            return Err(UpdateDocumentError::PlacementFenced);
        }
        let create: MetadataEventRecord = accepted_create
            .as_deref()
            .and_then(|value| postcard::from_bytes(value).ok())
            .ok_or(UpdateDocumentError::RawLimit)?;
        let quota = self.origin_quota(&create)?;
        self.accepted_create = Some(create);
        self.raw_budget = raw_budget
            .as_deref()
            .map(postcard::from_bytes::<RawOriginBudget>)
            .transpose()
            .map_err(|_| UpdateDocumentError::RawLimit)?;
        // Within a checkpoint window the log recount replaces the stored row.
        if self.window.is_some() {
            self.raw_budget = None;
        } else if self
            .raw_budget
            .as_ref()
            .is_some_and(|budget| !self.valid_budget(budget, &quota))
        {
            return Err(UpdateDocumentError::RawLimit);
        }
        let start = self
            .window
            .map(|checkpoint| IterStart::At(event_log_key(self.config.document_id, checkpoint)));
        self.stage = CommitStage::History;
        // Size rows stand in for the events, so the budget never decodes the history.
        let sizes = StorageEffect::Iter {
            key_space: EVENT_SIZE_KEYSPACE.to_string(),
            prefix: Some(event_log_prefix(self.config.document_id)),
            start,
            limit: RAW_EVENT_LIMIT,
            txn_id: None,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(sizes)))))
    }

    /// Counts the window's history against the budget and writes the update.
    fn history_read(&mut self, result: IoResult) -> CommitTurn {
        let (values, next_start_after) = page_rows(result)?;
        let (reconstructed, history_events, history_bytes) =
            self.history_budget(&values, next_start_after.as_ref())?;
        // Log keys end in the event id, so the last one is the newest event.
        let current = values
            .last()
            .and_then(|(key, _)| key.get(16..32))
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .map_or(Ulid::nil(), Ulid::from_bytes);
        if let Some(expected) = self.config.expected_revision
            && expected != current
        {
            return Err(UpdateDocumentError::RevisionConflict { expected, current });
        }
        let quota = RawOriginBudget {
            events: 0,
            encoded_bytes: 0,
            ..reconstructed.clone()
        };
        if self
            .raw_budget
            .as_ref()
            .is_some_and(|stored| stored != &reconstructed)
        {
            return Err(UpdateDocumentError::RawLimit);
        }
        self.raw_budget = Some(reconstructed);
        let next_raw_budget = match self.config.mutation {
            // The checkpoint is the first event of the window it opens.
            UpdateDocumentMutation::Checkpoint => {
                let checkpoint = self.update_event()?;
                let bytes = postcard::experimental::serialized_size(checkpoint)
                    .map_err(|_| UpdateDocumentError::RawLimit)? as u64;
                RawOriginBudget {
                    events: 1,
                    encoded_bytes: bytes,
                    ..self.window_quota(checkpoint)?
                }
            }
            _ => self.check_raw_budget(history_events, history_bytes, &quota)?,
        };
        let writes = self.update_rows(&next_raw_budget)?;
        self.stage = CommitStage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for UpdateCommitOperation {
    type Output = MetadataRegistryRecord;
    type Error = UpdateDocumentError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> CommitTurn {
        let key = metadata_registry_key(self.config.group_id, self.config.document_id);
        let read = vec![(METADATA_INDEX_KEYSPACE.to_string(), key)];
        Ok(Next::Call(IoCall::Read(read)))
    }

    fn next(&mut self, result: IoResult) -> CommitTurn {
        match self.stage {
            CommitStage::Registry => self.registry_read(result),
            CommitStage::Window => self.window_read(result),
            CommitStage::Sidecars => self.sidecars_read(result),
            CommitStage::History => self.history_read(result),
            CommitStage::Write => {
                result.done()?;
                Ok(Next::Done(self.update_event()?.record.clone()))
            }
        }
    }
}

fn validate_entity_jsonld(jsonld: &str) -> Result<(), MetadataError> {
    let value: serde_json::Value = serde_json::from_str(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let object = value.as_object().ok_or_else(|| {
        MetadataError::InvalidInput("entity payload must be a JSON object".to_string())
    })?;
    if object.contains_key("@graph") || object.contains_key("graph") {
        return Err(MetadataError::InvalidInput(
            "entity payload must not contain `@graph`; send a single JSON-LD entity object"
                .to_string(),
        ));
    }
    let has_id = object
        .get("@id")
        .or_else(|| object.get("id"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if !has_id {
        return Err(MetadataError::InvalidInput(
            "entity payload must define string `@id`".to_string(),
        ));
    }
    let entity_type = object
        .get("@type")
        .or_else(|| object.get("type"))
        .ok_or_else(|| {
            MetadataError::InvalidInput("entity payload must define `@type`".to_string())
        })?;
    let has_type = match entity_type {
        serde_json::Value::String(value) => !value.trim().is_empty(),
        serde_json::Value::Array(values) => {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| value.as_str().is_some_and(|value| !value.trim().is_empty()))
        }
        _ => false,
    };
    if !has_type {
        return Err(MetadataError::InvalidInput(
            "entity `@type` must be a string or non-empty string array".to_string(),
        ));
    }
    let has_name = object
        .get("name")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if !has_name {
        return Err(MetadataError::InvalidInput(
            "entity payload must define string `name`".to_string(),
        ));
    }
    Ok(())
}

/// The audit details of an update: what it changed, and the commit message that names who
/// made it, such as a plugin with its install and run.
fn audit_details(kind: &str, message: Option<&str>) -> String {
    match message {
        Some(message) => format!("{kind}; {message}"),
        None => kind.to_string(),
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentOutboxRecord,
        DocumentTarget,
    };
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{
        COMMIT_MESSAGE_KEYSPACE, DOCUMENT_INDEX_KEYSPACE, DOCUMENT_JOB_KEYSPACE,
        DOCUMENT_LIFECYCLE_KEYSPACE, MATERIALIZATION_JOB_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE,
        METADATA_AUDIT_KEYSPACE, SYNC_OUTBOX_KEYSPACE, SYNC_REVISION_KEYSPACE,
    };
    use aruna_core::storage_entries::sync_revision_key;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;

    type Rows = Vec<(String, Key, Value)>;

    fn actor() -> Actor {
        let realm_id = RealmId::from_bytes([9u8; 32]);
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 1), realm_id),
            realm_id,
        }
    }

    fn record(actor: &Actor) -> MetadataRegistryRecord {
        let group_id = Ulid::from_parts(2, 2);
        let document_id = Ulid::from_parts(3, 3);
        let document_path = "datasets/update-atomicity";
        MetadataRegistryRecord {
            realm_id: actor.realm_id,
            group_id,
            document_id,
            document_path: document_path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &actor.realm_id,
                group_id,
                document_path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: vec![actor.node_id],
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(1, 1),
            last_event_id: Ulid::from_parts(1, 1),
        }
    }

    fn replace_jsonld(document_id: Ulid, name: &str) -> String {
        format!(
            r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "https://w3id.org/aruna/{document_id}"}}
    }},
    {{
      "@id": "https://w3id.org/aruna/{document_id}",
      "@type": "Dataset",
      "name": "{name}",
      "description": "Updated atomically",
      "datePublished": "2026-01-01",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
        )
    }

    fn upsert() -> UpdateDocumentMutation {
        UpdateDocumentMutation::UpsertDataEntity {
            jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
        }
    }

    fn replace(record: &MetadataRegistryRecord, name: &str) -> UpdateDocumentMutation {
        UpdateDocumentMutation::ReplaceRoCrate {
            jsonld: replace_jsonld(record.document_id, name),
        }
    }

    fn config(
        actor: Actor,
        record: &MetadataRegistryRecord,
        mutation: UpdateDocumentMutation,
    ) -> UpdateDocumentConfig {
        UpdateDocumentConfig {
            actor,
            group_id: record.group_id,
            document_id: record.document_id,
            public: true,
            mutation,
            expected_revision: None,
        }
    }

    fn encoded<T: serde::Serialize>(value: &T) -> Option<Value> {
        Some(postcard::to_allocvec(value).unwrap().into())
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn create_event(record: &MetadataRegistryRecord) -> MetadataEventRecord {
        let node_id = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        MetadataEventRecord {
            event_id: record.establishing_event_id,
            record: record.clone(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(2, 2), record.realm_id),
            node_id,
            payload: MetadataEventPayload::Scaffold {
                name: "name".to_string(),
                description: "description".to_string(),
                date_published: "date".to_string(),
                license: None,
            },
            occurred_at_ms: record.created_at_ms,
        }
    }

    fn create_size(record: &MetadataRegistryRecord) -> u64 {
        postcard::experimental::serialized_size(&create_event(record)).unwrap() as u64
    }

    fn budget(record: &MetadataRegistryRecord, events: u32, encoded_bytes: u64) -> RawOriginBudget {
        let mut budget = raw_quotas(
            record.document_id,
            &record.holder_node_ids,
            actor().node_id,
            create_size(record),
        )
        .unwrap()
        .into_iter()
        .find(|budget| budget.node_id == actor().node_id)
        .unwrap();
        budget.events = events;
        budget.encoded_bytes = encoded_bytes;
        budget
    }

    fn batch(record: &MetadataRegistryRecord) -> MetadataBatch {
        MetadataBatch {
            graph_iri: record.graph_iri.clone(),
            actor: [7u8; 32],
            counter: 1,
            base_clock: craqle::VectorClock::default(),
            ops: Vec::new(),
            timestamp_millis: 1,
        }
    }

    fn planned_answer(batch: MetadataBatch) -> IoResult {
        event(Event::Metadata(MetadataEvent::BatchPlanned {
            graph_iri: batch.graph_iri.clone(),
            batch,
        }))
    }

    /// Drives an update through its reads up to the plan call.
    fn to_plan(
        operation: &mut UpdateDocumentOperation,
        record: &MetadataRegistryRecord,
    ) -> UpdateTurn {
        operation.first().unwrap();
        operation
            .next(IoResult::Values(vec![encoded(record)]))
            .unwrap();
        operation.next(IoResult::Values(vec![None, None]))
    }

    /// The transactional commit of a planned update of `record`.
    fn commit_of(
        operation: UpdateDocumentOperation,
        record: &MetadataRegistryRecord,
    ) -> UpdateCommitOperation {
        let mut operation = operation;
        to_plan(&mut operation, record).unwrap();
        operation.planned_batch = Some(batch(record));
        operation.commit_operation().unwrap()
    }

    fn commit(
        record: &MetadataRegistryRecord,
        mutation: UpdateDocumentMutation,
    ) -> UpdateCommitOperation {
        commit_of(
            UpdateDocumentOperation::new(config(actor(), record, mutation)),
            record,
        )
    }

    fn page(values: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        event(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: next,
        }))
    }

    /// The size row of the create, as the budget scan reads it.
    fn sizes(record: &MetadataRegistryRecord) -> IoResult {
        let entries =
            aruna_core::storage_entries::logged_event_entries(&create_event(record)).unwrap();
        let (_, key, size) = entries[1].clone();
        page(vec![(key, size)], None)
    }

    /// Steps a commit through the registry and window reads to its sidecar read.
    fn to_sidecars(
        commit: &mut UpdateCommitOperation,
        stored: &MetadataRegistryRecord,
    ) -> CommitTurn {
        commit.first().unwrap();
        commit
            .next(IoResult::Values(vec![encoded(stored)]))
            .unwrap();
        commit.next(page(Vec::new(), None))
    }

    fn sidecars(
        record: &MetadataRegistryRecord,
        budget: Option<RawOriginBudget>,
    ) -> Vec<Option<Value>> {
        vec![
            budget.as_ref().and_then(encoded),
            encoded(&create_event(record)),
        ]
    }

    /// Runs a commit with `budget` and the create's history to its writes.
    fn commit_rows(
        commit: &mut UpdateCommitOperation,
        record: &MetadataRegistryRecord,
        budget: Option<RawOriginBudget>,
    ) -> CommitTurn {
        to_sidecars(commit, record).unwrap();
        commit
            .next(IoResult::Values(sidecars(record, budget)))
            .unwrap();
        commit.next(sizes(record))
    }

    fn written(turn: CommitTurn) -> Rows {
        match turn {
            Ok(Next::Call(IoCall::Write(rows))) => rows,
            other => panic!("expected the update's writes, got {other:?}"),
        }
    }

    fn first_budget(record: &MetadataRegistryRecord) -> Option<RawOriginBudget> {
        Some(budget(record, 1, create_size(record)))
    }

    fn values_in<'a>(rows: &'a Rows, key_space: &str) -> Vec<&'a Value> {
        rows.iter()
            .filter(|(space, _, _)| space == key_space)
            .map(|(_, _, value)| value)
            .collect()
    }

    fn outboxes(rows: &Rows) -> Vec<DocumentOutboxRecord> {
        values_in(rows, SYNC_OUTBOX_KEYSPACE)
            .into_iter()
            .map(|value| postcard::from_bytes(value).expect("outbox row decodes"))
            .collect()
    }

    /// Checks the rows of an accepted update and returns its event.
    fn update_event(
        rows: &Rows,
        payload: impl FnOnce(&MetadataEventPayload) -> bool,
    ) -> MetadataEventRecord {
        for key_space in [
            EVENT_LOG_KEYSPACE,
            METADATA_INDEX_KEYSPACE,
            DOCUMENT_INDEX_KEYSPACE,
            METADATA_AUDIT_KEYSPACE,
            SYNC_OUTBOX_KEYSPACE,
            SYNC_REVISION_KEYSPACE,
            DOCUMENT_LIFECYCLE_KEYSPACE,
            MATERIALIZATION_STATUS_KEYSPACE,
            MATERIALIZATION_JOB_KEYSPACE,
            DOCUMENT_JOB_KEYSPACE,
            RAW_BUDGET_KEYSPACE,
        ] {
            assert!(
                !values_in(rows, key_space).is_empty(),
                "missing {key_space}"
            );
        }
        let event: MetadataEventRecord =
            postcard::from_bytes(values_in(rows, EVENT_LOG_KEYSPACE)[0]).expect("event decodes");
        assert!(payload(&event.payload));
        let outbox = &outboxes(rows)[0];
        assert_eq!(outbox.outbox_id, event.event_id);
        let (revision_key, revision): (_, DocumentChange) = rows
            .iter()
            .find(|(space, _, _)| space == SYNC_REVISION_KEYSPACE)
            .map(|(_, key, value)| (key, postcard::from_bytes(value).unwrap()))
            .unwrap();
        assert_eq!(revision_key, &sync_revision_key(&outbox.target));
        assert_eq!(revision.current.event_id, event.event_id);
        assert_eq!(revision.current.generation, event.record.updated_at_ms);
        assert_eq!(revision.kind, DocumentChangeKind::Upsert);
        let lifecycle: MetadataLifecycleRecord =
            postcard::from_bytes(values_in(rows, DOCUMENT_LIFECYCLE_KEYSPACE)[0]).unwrap();
        assert_eq!(
            lifecycle,
            MetadataLifecycleRecord::Upsert {
                event: Box::new(event.clone())
            }
        );
        event
    }

    fn is_replace(payload: &MetadataEventPayload) -> bool {
        matches!(
            payload,
            MetadataEventPayload::ApplyBatch {
                authored: MetadataBatchSource::ReplaceRoCrate { .. },
                ..
            }
        )
    }

    fn is_data_upsert(payload: &MetadataEventPayload) -> bool {
        matches!(
            payload,
            MetadataEventPayload::ApplyBatch {
                authored: MetadataBatchSource::UpsertDataEntity { .. },
                ..
            }
        )
    }

    #[test]
    fn update_plans_outside() {
        // The reads and the plan run outside a transaction; the commit is a child.
        let actor = actor();
        let record = record(&actor);
        let mut operation =
            UpdateDocumentOperation::new(config(actor, &record, replace(&record, "Plan")));
        assert_eq!(operation.transaction(), TransactionMode::None);
        let key = metadata_registry_key(record.group_id, record.document_id);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(vec![(
                METADATA_INDEX_KEYSPACE.to_string(),
                key
            )])))
        );
        let reads = vec![
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                ByteView::from(*record.realm_id.as_bytes()),
            ),
            (
                METADATA_ACTOR_KEYSPACE.to_string(),
                metadata_actor_key(record.document_id),
            ),
        ];
        assert_eq!(
            operation.next(IoResult::Values(vec![encoded(&record)])),
            Ok(Next::Call(IoCall::Read(reads)))
        );
        let Ok(Next::Call(IoCall::Effect(plan))) =
            operation.next(IoResult::Values(vec![None, None]))
        else {
            panic!("expected the batch plan");
        };
        assert!(matches!(
            *plan,
            Effect::Metadata(MetadataEffect::PlanBatch { ref graph_iri, .. }) if *graph_iri == record.graph_iri
        ));
        assert!(matches!(
            operation.next(planned_answer(batch(&record))),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let stored = record.clone();
        let outcome = IoResult::Child(ChildOutcome::new(Ok::<_, UpdateDocumentError>(
            stored.clone(),
        )));
        assert_eq!(
            operation.next(outcome),
            Ok(Next::Then {
                output: stored,
                calls: vec![
                    IoCall::Effect(Box::new(schedule_materialization())),
                    IoCall::Effect(Box::new(schedule_drain_effect())),
                ],
            })
        );
    }

    #[test]
    fn commit_runs_ordered() {
        // The commit re-reads the row, finds the window, checks the sidecars and history,
        // writes once and answers with the updated row.
        let record = record(&actor());
        let mut commit = commit(&record, replace(&record, "Commit"));
        assert_eq!(commit.transaction(), TransactionMode::Write);
        let Ok(Next::Call(IoCall::Effect(last))) = ({
            commit.first().unwrap();
            commit.next(IoResult::Values(vec![encoded(&record)]))
        }) else {
            panic!("expected the checkpoint lookup");
        };
        assert!(matches!(
            *last,
            Effect::Storage(StorageEffect::Last { ref key_space, txn_id: None, .. })
                if key_space == METADATA_CHECKPOINT_KEYSPACE
        ));
        let Ok(Next::Call(IoCall::Read(reads))) = commit.next(page(Vec::new(), None)) else {
            panic!("expected the sidecar reads");
        };
        assert_eq!(reads.len(), 2);
        let Ok(Next::Call(IoCall::Effect(scan))) =
            commit.next(IoResult::Values(sidecars(&record, first_budget(&record))))
        else {
            panic!("expected the history scan");
        };
        assert!(matches!(
            *scan,
            Effect::Storage(StorageEffect::Iter {
                limit: RAW_EVENT_LIMIT,
                start: None,
                txn_id: None,
                ..
            })
        ));
        let rows = written(commit.next(sizes(&record)));
        let event = update_event(&rows, is_replace);
        assert_eq!(event.record.last_event_id, event.event_id);
        assert_eq!(commit.next(IoResult::Done), Ok(Next::Done(event.record)));
    }

    // The bucket is chosen once, by the create-receiving node; an update keeps the stored one.
    #[test]
    fn update_keeps_placement() {
        let mut record = record(&actor());
        record.placement = PlacementRef {
            strategy_id: Ulid::from_bytes([5u8; 16]),
            shard: 11,
        };
        let mut commit = commit(&record, replace(&record, "Placement Preserved"));
        let rows = written(commit_rows(&mut commit, &record, first_budget(&record)));
        assert_eq!(
            update_event(&rows, is_replace).record.placement,
            record.placement
        );
    }

    #[test]
    fn update_uses_fence() {
        // The row read inside the transaction decides, not the one read before it.
        let record = record(&actor());
        let mut fenced = record.clone();
        fenced.placement = PlacementRef {
            strategy_id: Ulid::from_bytes([6u8; 16]),
            shard: 12,
        };
        let mut commit = commit(&record, upsert());
        commit.first().unwrap();
        commit
            .next(IoResult::Values(vec![encoded(&fenced)]))
            .unwrap();
        commit.next(page(Vec::new(), None)).unwrap();
        commit
            .next(IoResult::Values(sidecars(&record, first_budget(&record))))
            .unwrap();
        let rows = written(commit.next(sizes(&record)));
        assert_eq!(
            update_event(&rows, is_data_upsert).record.placement,
            fenced.placement
        );
    }

    /// A realm whose buckets are activated at generation one, so an update takes their fences.
    fn activated_config(record: &mut MetadataRegistryRecord) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(record.realm_id, Vec::new(), 3);
        config.ensure_node(
            actor().node_id,
            aruna_core::structs::identity::realm::RealmNodeKind::Server,
        );
        let strategy_id = Ulid::from_bytes([5u8; 16]);
        config
            .strategies
            .push(aruna_core::structs::placement::record::PlacementStrategy {
                strategy_id,
                name: "default".to_string(),
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
                shard_count: 16,
            });
        config.default_strategy_id = Some(strategy_id);
        config.snapshot_candidate_map();
        record.placement = PlacementRef {
            strategy_id,
            shard: 11,
        };
        config
    }

    /// A commit in an activated realm, answered up to its fence read with `closed`.
    fn fenced_commit(
        closed: &[Option<u64>],
    ) -> (UpdateCommitOperation, MetadataRegistryRecord, CommitTurn) {
        let mut record = record(&actor());
        let realm = activated_config(&mut record);
        let mut commit = commit(&record, upsert());
        commit.realm_config = Some(realm);
        let Ok(Next::Call(IoCall::Read(reads))) = to_sidecars(&mut commit, &record) else {
            panic!("expected the sidecar and fence reads");
        };
        assert_eq!(reads.len(), 2 + closed.len(), "one read per fenced bucket");
        let mut values = sidecars(&record, first_budget(&record));
        values.extend(
            closed
                .iter()
                .map(|generation| generation.map(|g| Value::from(g.to_be_bytes().to_vec()))),
        );
        let turn = commit.next(IoResult::Values(values));
        (commit, record, turn)
    }

    #[test]
    fn update_takes_bucket() {
        // An admitted update stamps the generation it resolved at onto every outbox row.
        let (mut commit, record, turn) = fenced_commit(&[None, None]);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
        let rows = written(commit.next(sizes(&record)));
        let outboxes = outboxes(&rows);
        assert!(!outboxes.is_empty(), "an admitted update publishes");
        for row in outboxes {
            assert_eq!(row.generation, 1, "row for {:?}", row.placement);
        }
    }

    #[test]
    fn closed_fence_rejects() {
        // The departing holder closed generation one: nothing may commit after that.
        let (_, _, turn) = fenced_commit(&[Some(1), None]);
        assert_eq!(turn, Err(UpdateDocumentError::PlacementFenced));
    }

    #[test]
    fn rejects_raw_limit() {
        let record = record(&actor());
        let mut commit = commit(&record, upsert());
        to_sidecars(&mut commit, &record).unwrap();
        commit
            .next(IoResult::Values(sidecars(&record, first_budget(&record))))
            .unwrap();
        let full = (0..RAW_EVENT_LIMIT)
            .map(|index| (Key::from(vec![index as u8]), Value::from(vec![0])))
            .collect();
        assert_eq!(
            commit.next(page(full, Some(Key::from(vec![1])))),
            Err(UpdateDocumentError::RawLimit)
        );
    }

    #[test]
    fn update_carries_message() {
        let rows = |message: Option<&str>| {
            let record = record(&actor());
            let operation = UpdateDocumentOperation::new(config(actor(), &record, upsert()))
                .with_message(message.map(str::to_owned));
            let mut commit = commit_of(operation, &record);
            written(commit_rows(&mut commit, &record, first_budget(&record)))
        };
        let lifecycles = |rows: &Rows| {
            let stored =
                postcard::from_bytes(values_in(rows, DOCUMENT_LIFECYCLE_KEYSPACE)[0]).unwrap();
            let sent = outboxes(rows)
                .into_iter()
                .find_map(|outbox| match (outbox.target, outbox.event) {
                    (
                        DocumentTarget::MetadataDocumentLifecycle { .. },
                        DocumentOutboxEvent::Upsert { bytes, .. },
                    ) => Some(postcard::from_bytes(&bytes).unwrap()),
                    _ => None,
                })
                .expect("lifecycle outbox is written");
            [stored, sent] as [MetadataLifecycleRecord; 2]
        };
        let audit = |rows: &Rows| {
            postcard::from_bytes::<MetadataAuditRecord>(values_in(rows, METADATA_AUDIT_KEYSPACE)[0])
                .unwrap()
        };

        let plain = rows(None);
        assert!(values_in(&plain, COMMIT_MESSAGE_KEYSPACE).is_empty());
        for lifecycle in lifecycles(&plain) {
            assert!(matches!(lifecycle, MetadataLifecycleRecord::Upsert { .. }));
        }
        assert!(!audit(&plain).details.unwrap_or_default().contains(';'));

        let noted = rows(Some("Add the raw file"));
        let (_, key, value) = noted
            .iter()
            .find(|(space, _, _)| space == COMMIT_MESSAGE_KEYSPACE)
            .expect("message row is written");
        let event = lifecycles(&noted)[0].upsert().cloned().expect("upsert");
        assert_eq!(
            *key,
            event_log_key(event.record.document_id, event.event_id)
        );
        assert_eq!(
            postcard::from_bytes::<String>(value).unwrap(),
            "Add the raw file"
        );
        for lifecycle in lifecycles(&noted) {
            assert_eq!(lifecycle.message(), Some("Add the raw file"));
        }
        let details = audit(&noted).details.unwrap_or_default();
        assert!(details.ends_with("; Add the raw file"), "{details}");
    }

    // The accepted batch journals the change once for plugins, with its author.
    #[test]
    fn journals_accepted_update() {
        let actor = actor();
        let record = record(&actor);
        let mut commit = commit(&record, upsert());
        let rows = written(commit_rows(&mut commit, &record, first_budget(&record)));
        let journal: Vec<SourceEvent> = rows
            .iter()
            .filter(|(space, key, _)| {
                space == aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE
                    && key.first() == Some(&aruna_core::plugin_runs::JOURNAL_ROW)
            })
            .map(|(_, _, value)| SourceEvent::from_bytes(value).unwrap())
            .collect();
        let [row] = journal.as_slice() else {
            panic!("expected one journal row, got {journal:?}");
        };
        assert_eq!(row.kind, SourceKind::DatasetUpdated);
        assert_eq!(row.resource, record.document_id.to_string());
        assert_eq!(row.actor, Some(actor.user_id));
    }

    #[test]
    fn refuses_changed_graph() {
        let planned = |version: String| {
            let record = record(&actor());
            let mut operation =
                UpdateDocumentOperation::new(config(actor(), &record, replace(&record, "merged")))
                    .with_expected_graph(version);
            to_plan(&mut operation, &record).unwrap();
            operation.next(planned_answer(batch(&record)))
        };
        let current = aruna_core::metadata::graph_version(&craqle::VectorClock::default());
        assert!(matches!(planned(current), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(
            planned("older".into()),
            Err(UpdateDocumentError::GraphChanged)
        );
    }

    fn guarded(expected: Ulid) -> CommitTurn {
        let record = record(&actor());
        let mut config = config(actor(), &record, replace(&record, "pulled"));
        config.expected_revision = Some(expected);
        let mut commit = commit_of(UpdateDocumentOperation::new(config), &record);
        commit_rows(&mut commit, &record, None)
    }

    #[test]
    fn checks_expected_revision() {
        let stale = Ulid::from_parts(0, 7);
        assert_eq!(
            guarded(stale),
            Err(UpdateDocumentError::RevisionConflict {
                expected: stale,
                current: Ulid::from_parts(1, 1),
            })
        );
        assert!(matches!(
            guarded(Ulid::from_parts(1, 1)),
            Ok(Next::Call(IoCall::Write(_)))
        ));
    }

    #[test]
    fn rebuilds_missing_budget() {
        let record = record(&actor());
        let mut commit = commit(&record, upsert());
        let rows = written(commit_rows(&mut commit, &record, None));
        let event = update_event(&rows, is_data_upsert);
        let budget: RawOriginBudget =
            postcard::from_bytes(values_in(&rows, RAW_BUDGET_KEYSPACE)[0]).unwrap();
        assert_eq!(budget.events, 2);
        assert_eq!(
            budget.encoded_bytes,
            create_size(&record) + postcard::experimental::serialized_size(&event).unwrap() as u64
        );
        assert_eq!(budget.node_id, actor().node_id);
    }

    #[test]
    fn rejects_new_origin() {
        // A node outside the create's origins has no quota.
        let original = record(&actor());
        let mut current = original.clone();
        let mut outsider = actor();
        outsider.node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        current.holder_node_ids = vec![outsider.node_id];
        let operation = UpdateDocumentOperation::new(config(outsider, &current, upsert()));
        let mut commit = commit_of(operation, &current);
        to_sidecars(&mut commit, &current).unwrap();
        assert_eq!(
            commit.next(IoResult::Values(sidecars(&original, None))),
            Err(UpdateDocumentError::RawLimit)
        );
    }

    #[test]
    fn rejects_origin_budget() {
        // The exhausted budget is rejected at the sidecar read itself.
        let record = record(&actor());
        let mut commit = commit(&record, upsert());
        to_sidecars(&mut commit, &record).unwrap();
        let spent = Some(budget(&record, EVENT_LIMIT, 0));
        assert_eq!(
            commit.next(IoResult::Values(sidecars(&record, spent))),
            Err(UpdateDocumentError::RawLimit)
        );
    }

    #[test]
    fn rejects_no_holders() {
        let mut record = record(&actor());
        record.holder_node_ids.clear();
        let mut commit = commit(&record, upsert());
        to_sidecars(&mut commit, &record).unwrap();
        assert_eq!(
            commit.next(IoResult::Values(sidecars(&record, None))),
            Err(UpdateDocumentError::RawLimit)
        );
    }

    #[test]
    fn update_fence_missing() {
        let record = record(&actor());
        let mut commit = commit(&record, upsert());
        commit.first().unwrap();
        assert_eq!(
            commit.next(IoResult::Values(vec![None])),
            Err(UpdateDocumentError::DocumentNotFound)
        );
    }

    fn plan_profile_type(document_path: &str) -> UpdateTurn {
        let mut record = record(&actor());
        record.document_path = document_path.to_string();
        let mut operation = UpdateDocumentOperation::new(config(
            actor(),
            &record,
            replace(&record, "Profile Type"),
        ));
        to_plan(&mut operation, &record).unwrap();
        let mut batch = batch(&record);
        batch.ops.push(aruna_core::metadata::MetadataQuadOp::Add {
            subject: format!("<{}>", record.graph_iri),
            predicate: "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".to_string(),
            object: "<http://www.w3.org/ns/dx/prof/Profile>".to_string(),
            dot: aruna_core::metadata::MetadataDot {
                actor: [7u8; 32],
                counter: 1,
            },
        });
        operation.next(planned_answer(batch))
    }

    #[test]
    fn profiles_stay_inside() {
        let Err(UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message))) =
            plan_profile_type("resolver-check/profile")
        else {
            panic!("expected the Profile path refusal");
        };
        assert!(message.contains("Profiles must be stored under profiles/"));
        assert!(matches!(
            plan_profile_type("profiles/example"),
            Ok(Next::Call(IoCall::Child(_)))
        ));
    }

    #[test]
    fn denial_before_plan() {
        // An invalid entity payload is denied before any plan or transaction.
        let record = record(&actor());
        let mutation = UpdateDocumentMutation::UpsertDataEntity {
            jsonld: r#"{"name":"missing identity"}"#.to_string(),
        };
        let mut operation = UpdateDocumentOperation::new(config(actor(), &record, mutation));
        assert_eq!(
            to_plan(&mut operation, &record),
            Err(UpdateDocumentError::MetadataError(
                MetadataError::InvalidInput("entity payload must define string `@id`".to_string())
            ))
        );
    }

    #[test]
    fn checkpoint_takes_snapshot() {
        // A checkpoint reads no actor and plans nothing: it carries the graph state.
        let record = record(&actor());
        let mut operation = UpdateDocumentOperation::new(config(
            actor(),
            &record,
            UpdateDocumentMutation::Checkpoint,
        ));
        operation.first().unwrap();
        let reads = vec![(
            REALM_CONFIG_KEYSPACE.to_string(),
            ByteView::from(*record.realm_id.as_bytes()),
        )];
        assert_eq!(
            operation.next(IoResult::Values(vec![encoded(&record)])),
            Ok(Next::Call(IoCall::Read(reads)))
        );
        let snapshot = Effect::Metadata(MetadataEffect::GraphSnapshot {
            graph_iri: record.graph_iri.clone(),
        });
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Call(IoCall::Effect(Box::new(snapshot))))
        );
        let failed = event(Event::Metadata(MetadataEvent::Error {
            graph_iri: Some(record.graph_iri.clone()),
            error: MetadataError::GraphNotFound,
        }));
        assert_eq!(
            operation.next(failed),
            Err(UpdateDocumentError::MetadataError(
                MetadataError::GraphNotFound
            ))
        );
    }

    #[test]
    fn checkpoint_opens_window() {
        let record = record(&actor());
        let checkpoint = MetadataEventRecord {
            event_id: Ulid::from_parts(30, 30),
            payload: MetadataEventPayload::Checkpoint { snapshot: None },
            ..create_event(&record)
        };
        let mut commit = commit(&record, replace(&record, "After Checkpoint"));
        commit.first().unwrap();
        commit
            .next(IoResult::Values(vec![encoded(&record)]))
            .unwrap();
        let window = vec![(
            event_log_key(record.document_id, checkpoint.event_id),
            Value::from(Vec::new()),
        )];
        commit.next(page(window, None)).unwrap();
        // The earlier window exhausted its quota; the new window starts over. The read also
        // returns the checkpoint event, which sets the window's quota.
        let mut values = sidecars(&record, Some(budget(&record, EVENT_LIMIT, 0)));
        values.push(encoded(&checkpoint));
        let Ok(Next::Call(IoCall::Effect(scan))) = commit.next(IoResult::Values(values)) else {
            panic!("expected the window scan");
        };
        let at = IterStart::At(event_log_key(record.document_id, checkpoint.event_id));
        assert!(
            matches!(*scan, Effect::Storage(StorageEffect::Iter { start: Some(ref start), .. }) if *start == at)
        );
        let entries = aruna_core::storage_entries::logged_event_entries(&checkpoint).unwrap();
        let (_, key, size) = entries[1].clone();
        let rows = written(commit.next(page(vec![(key, size)], None)));
        let budget: RawOriginBudget =
            postcard::from_bytes(values_in(&rows, RAW_BUDGET_KEYSPACE)[0]).unwrap();
        assert_eq!(budget.events, 2, "the checkpoint and this update count");
    }

    #[test]
    fn reuses_node_actor() {
        let record = record(&actor());
        let mut operation =
            UpdateDocumentOperation::new(config(actor(), &record, replace(&record, "Actor Reuse")));
        let previous = MetadataActor {
            document_id: record.document_id,
            actor: [5u8; 32],
            counter: 41,
            last_event_id: Ulid::from_parts(u64::MAX >> 16, 7),
            rejected: Vec::new(),
        };
        operation.first().unwrap();
        operation
            .next(IoResult::Values(vec![encoded(&record)]))
            .unwrap();
        let Ok(Next::Call(IoCall::Effect(plan))) =
            operation.next(IoResult::Values(vec![None, encoded(&previous)]))
        else {
            panic!("expected the batch plan");
        };
        let Effect::Metadata(MetadataEffect::PlanBatch {
            actor: planned,
            counter,
            ..
        }) = *plan
        else {
            panic!("expected batch planning with the node actor");
        };
        assert_eq!((planned, counter), (previous.actor, 42));
        assert!(operation.event_id > previous.last_event_id);
        let next = operation.actor.clone().expect("actor advanced");
        assert_eq!(next.last_event_id, operation.event_id);
        assert_eq!(next.counter, 42);
    }

    fn fixed_ms() -> u64 {
        1_700_000_000_000
    }

    fn fixed_id() -> Ulid {
        Ulid::from_bytes([0x7b; 16])
    }

    /// The update takes its event identity and phase time from the injected source, so
    /// identical inputs produce identical rows.
    #[test]
    fn update_source_pinned() {
        let record = record(&actor());
        let rows = || {
            let operation = UpdateDocumentOperation::new(config(actor(), &record, upsert()))
                .with_phase_source(MetadataPhaseSource::fixed(fixed_ms, fixed_id));
            assert_eq!(operation.event_id, fixed_id());
            let mut commit = commit_of(operation, &record);
            let mut rows = written(commit_rows(&mut commit, &record, first_budget(&record)));
            // The plugin journal keys its rows with its own unique id.
            rows.retain(|(space, _, _)| space != aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE);
            rows
        };
        let first = rows();
        let event: MetadataEventRecord =
            postcard::from_bytes(values_in(&first, EVENT_LOG_KEYSPACE)[0]).unwrap();
        assert_eq!(event.record.updated_at_ms, fixed_ms());
        assert_eq!(first, rows());
    }

    #[test]
    fn errors_map() {
        assert_eq!(
            UpdateDocumentError::from(IoError::Conflict),
            UpdateDocumentError::StorageError(
                aruna_core::errors::StorageError::TransactionConflict
            )
        );
        assert!(UpdateDocumentOperation::is_conflict(
            &UpdateDocumentError::from(IoError::Conflict)
        ));
        assert_eq!(
            UpdateDocumentError::from(IoError::Aborted),
            UpdateDocumentError::Io(IoError::Aborted)
        );
        let record = record(&actor());
        let mut commit = commit(&record, upsert());
        commit.first().unwrap();
        assert!(matches!(
            commit.next(IoResult::Done),
            Err(UpdateDocumentError::Io(IoError::Unexpected { .. }))
        ));
    }
}
