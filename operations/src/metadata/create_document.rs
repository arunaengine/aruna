//! Creates a metadata document: mints its id, picks its placement and writes the registry row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Mutex, OnceLock};

use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::Effect;
use aruna_core::errors::StorageError;
use aruna_core::events::Event;
use aruna_core::keyspaces::{CREATE_ACCEPTANCE_KEYSPACE, DOCUMENT_INDEX_KEYSPACE};
use aruna_core::metadata::{
    MetadataCrateRequest, MetadataEffect, MetadataError, MetadataEvent, MetadataEventPayload,
    MetadataEventRecord, MetadataGraphPolicy, MetadataRequestDurability, ProfileValidationStatus,
    RAW_BYTES_LIMIT, raw_quotas,
};
use aruna_core::operation::{AfterConflict, Operation, TransactionMode};
use aruna_core::storage_entries::{
    commit_message_entry, create_acceptance_entry, create_acceptance_key, metadata_document_key,
    profile_validation_entry, raw_budget_entry,
};
use aruna_core::structs::execution::job::{
    JobPayload, JobRecord, RETENTION_MS, WorkspaceMode, pid_dedup_key,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::binding_directory::BindingError;
use aruna_core::structs::placement::record::{
    DocumentClass, PlacementRef, PlacementScope, PlacementStrategy, shard_for_subject,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::{MintPersistentSpec, PersistentIdMapping};
use aruna_core::structured_id::{BucketId, PlacementHandle, StructuredIdGenerator};
use aruna_core::types::{GroupId, Key, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::call_results::{effect_event, read_value, read_values};
use crate::metadata::id_mapping::{mapping_revision, mapping_route_for, transition_entries};
use crate::metadata::profile::rules::{
    crate_is_profile, not_profiled_status, profile_path_rule, submission_profile_tag,
};
use crate::metadata::repository::create_projection_entries;
use crate::sync::shard_placement::sort_node_ids;
use aruna_core::placement::{
    PlacementResolutionContext, choose_origin_bucket, holds_placement, meta_bucket_subject,
    resolve_shard_holders, strategy_for_target,
};
use aruna_core::plugin_runs::{SourceEvent, journal_entries};

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDocumentConfig {
    pub actor: Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub document_path: String,
    pub public: bool,
    pub payload: CreateDocumentPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreateDocumentPayload {
    Scaffold {
        name: String,
        description: String,
        date_published: String,
        license: Option<String>,
    },
    RoCrate {
        jsonld: String,
    },
}

/// Result returned after a metadata create is durably accepted.
///
/// Graph materialization and replica convergence may still be pending.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDocumentResult {
    /// Registry state accepted for the create event.
    pub record: MetadataRegistryRecord,
    /// Durable event id used by projection and replication workers.
    pub event_id: Ulid,
}

/// Validates metadata create input outside a transaction, then appends the durable create
/// event in a write child. Success means pipeline acceptance, not materialization or replication.
#[derive(Debug, PartialEq)]
pub struct CreateDocumentOperation {
    pub config: CreateDocumentConfig,
    skip_existing_check: bool,
    /// Set when a non-holder forwarded this create; the id was already minted at
    /// the origin with the blind-hash bucket, so this node keeps it unchanged.
    pub forwarded: bool,
    /// Set on a rerun after a lost conflict: it goes straight to the commit, whose fence read
    /// finds a concurrent identical create.
    conflict_recheck: bool,
    stage: CreateStage,
    pub route_profile_status: Option<ProfileValidationStatus>,
    /// The author's commit message for the ARC snapshot of this create.
    message: Option<String>,
    /// The phase-time and identity source. Production uses the default source;
    /// tests replace it before the first record is built.
    phase_source: crate::metadata::MetadataPhaseSource,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CreateStage {
    /// Validating the crate against the graph.
    Validate,
    /// Checking that no registry row holds the id yet.
    Existing,
    /// Waiting for the write child.
    Commit,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateDocumentError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] aruna_core::errors::ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document already exists")]
    DocumentAlreadyExists,
    /// The receiving node holds no bucket of the governing strategy, so it can
    /// never publish this document. The caller forwards the create to a holder.
    #[error("create-receiving node holds no bucket of the governing strategy")]
    HoldsNoBucket,
    /// The id's placement handle resolves to no binding, or no strategy/binding
    /// governs the create target. Fails closed rather than guessing a placement.
    #[error("placement_binding_unavailable: {0}")]
    PlacementBindingUnavailable(String),
    #[error(transparent)]
    PlacementBinding(#[from] BindingError),
    /// The structured-id generator refused to mint under a clock-health fault.
    #[error(transparent)]
    ClockHealth(#[from] aruna_core::structured_id::ClockHealthError),
    #[error("metadata raw update budget exceeded")]
    RawLimit,
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("persistent id mapping bucket is closing; retry the create")]
    PlacementFenced,
    #[error(transparent)]
    Io(IoError),
}

// A storage failure keeps its own transience; a lost conflict is the conflict the retry reruns.
impl From<IoError> for CreateDocumentError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
}

type CreateTurn = Result<Next<CreateDocumentResult>, CreateDocumentError>;

impl CreateDocumentOperation {
    pub fn new(config: CreateDocumentConfig) -> Self {
        let route_profile_status = match &config.payload {
            CreateDocumentPayload::Scaffold { .. } => Some(not_profiled_status(config.document_id)),
            CreateDocumentPayload::RoCrate { jsonld } if !submission_profile_tag(jsonld) => {
                Some(not_profiled_status(config.document_id))
            }
            CreateDocumentPayload::RoCrate { .. } => None,
        };
        Self {
            config,
            skip_existing_check: false,
            forwarded: false,
            conflict_recheck: false,
            stage: CreateStage::Validate,
            route_profile_status,
            message: None,
            phase_source: crate::metadata::MetadataPhaseSource::default(),
        }
    }

    /// Replaces the phase-time and identity source before the first record is
    /// built. Production never calls this; tests call the same implementation
    /// with fixed samplers.
    #[cfg(test)]
    pub fn with_phase_source(mut self, phase_source: crate::metadata::MetadataPhaseSource) -> Self {
        self.phase_source = phase_source;
        self
    }

    pub fn new_generated_id(config: CreateDocumentConfig) -> Self {
        let mut operation = Self::new(config);
        operation.skip_existing_check = true;
        operation
    }

    /// A create a non-holder forwarded here. The document's bucket is its blind
    /// hash rather than this node's pick, so every holder the forwarder may try
    /// stamps the same bucket.
    pub fn new_forwarded(config: CreateDocumentConfig) -> Self {
        let mut operation = Self::new(config);
        operation.forwarded = true;
        operation
    }

    pub fn config(&self) -> &CreateDocumentConfig {
        &self.config
    }

    /// Records `message` as the commit message of this create's ARC snapshot.
    pub fn with_message(mut self, message: Option<String>) -> Self {
        self.message = message;
        self
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// A pristine operation with the same inputs, for a conflict retry.
    pub fn fresh_copy(&self) -> Self {
        Self {
            config: self.config.clone(),
            skip_existing_check: self.skip_existing_check,
            forwarded: self.forwarded,
            conflict_recheck: false,
            stage: CreateStage::Validate,
            route_profile_status: self.route_profile_status.clone(),
            message: self.message.clone(),
            phase_source: self.phase_source,
        }
    }

    pub fn create_event_payload(config: &CreateDocumentConfig) -> MetadataEventPayload {
        match &config.payload {
            CreateDocumentPayload::Scaffold {
                name,
                description,
                date_published,
                license,
            } => MetadataEventPayload::Scaffold {
                name: name.clone(),
                description: description.clone(),
                date_published: date_published.clone(),
                license: license.clone(),
            },
            CreateDocumentPayload::RoCrate { jsonld } => MetadataEventPayload::RoCrate {
                jsonld: jsonld.clone(),
            },
        }
    }

    fn graph_policy(&self) -> MetadataGraphPolicy {
        MetadataGraphPolicy {
            public: self.config.public,
            permission_paths: vec![permission_path(&self.config)],
        }
        .normalized()
    }

    fn graph_validation_effect(&self) -> Effect {
        let graph_iri = graph_iri(&self.config);
        let policy = self.graph_policy();
        match &self.config.payload {
            CreateDocumentPayload::Scaffold {
                name,
                description,
                date_published,
                license,
            } => Effect::Metadata(MetadataEffect::ValidateCreateCrate {
                request: MetadataCrateRequest {
                    graph_iri,
                    name: name.clone(),
                    description: description.clone(),
                    date_published: date_published.clone(),
                    license: license.clone(),
                    policy,
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor: None,
                },
            }),
            CreateDocumentPayload::RoCrate { jsonld } => {
                Effect::Metadata(MetadataEffect::ValidateRoCrate {
                    request: aruna_core::metadata::ApplyRoCrateRequest {
                        graph_iri,
                        jsonld: jsonld.clone(),
                        policy,
                        durability: MetadataRequestDurability::WalAlreadyDurable,
                        deterministic_actor: None,
                    },
                })
            }
        }
    }

    fn validated(&mut self, result: IoResult) -> CreateTurn {
        match effect_event(result)? {
            Event::Metadata(MetadataEvent::ValidationResult { .. }) if self.skip_existing_check => {
                self.commit()
            }
            Event::Metadata(MetadataEvent::ValidationResult { .. }) => {
                self.stage = CreateStage::Existing;
                let key = metadata_document_key(self.config.document_id);
                Ok(Next::Call(IoCall::Read(vec![(
                    DOCUMENT_INDEX_KEYSPACE.to_string(),
                    key,
                )])))
            }
            Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
            other => Err(unexpected("metadata validation result", &other)),
        }
    }

    fn existing_read(&mut self, result: IoResult) -> CreateTurn {
        match read_value(result)? {
            Some(bytes) => {
                postcard::from_bytes::<MetadataRegistryRecord>(&bytes)
                    .map_err(aruna_core::errors::ConversionError::from)?;
                Err(CreateDocumentError::DocumentAlreadyExists)
            }
            None => self.commit(),
        }
    }

    /// Hands the validated create to the transactional commit.
    fn commit(&mut self) -> CreateTurn {
        self.stage = CreateStage::Commit;
        Ok(Next::Call(IoCall::child(self.commit_operation())))
    }

    fn commit_operation(&self) -> CreateCommitOperation {
        CreateCommitOperation {
            config: self.config.clone(),
            route_profile_status: self.route_profile_status.clone(),
            message: self.message.clone(),
            phase_source: self.phase_source,
            stage: CommitStage::Fence,
            pending: None,
            created: None,
        }
    }
}

impl Operation for CreateDocumentOperation {
    type Output = CreateDocumentResult;
    type Error = CreateDocumentError;

    fn first(&mut self) -> CreateTurn {
        if self.conflict_recheck {
            return self.commit();
        }
        if let CreateDocumentPayload::RoCrate { jsonld } = &self.config.payload {
            profile_path_rule(&self.config.document_path, || crate_is_profile(jsonld))?;
        }
        let validation = self.graph_validation_effect();
        Ok(Next::Call(IoCall::Effect(Box::new(validation))))
    }

    fn next(&mut self, result: IoResult) -> CreateTurn {
        match self.stage {
            CreateStage::Validate => self.validated(result),
            CreateStage::Existing => self.existing_read(result),
            CreateStage::Commit => Ok(Next::Done(
                result.child::<CreateDocumentResult, CreateDocumentError>()??,
            )),
        }
    }

    fn on_conflict(&mut self, attempt: u32) -> AfterConflict<CreateDocumentResult> {
        self.conflict_recheck = true;
        AfterConflict::rerun_within(attempt, Self::conflict_retries())
    }
}

fn unexpected(expected: &'static str, event: &Event) -> CreateDocumentError {
    CreateDocumentError::Io(IoError::Unexpected {
        expected,
        got: event.label(),
    })
}

fn graph_iri(config: &CreateDocumentConfig) -> String {
    MetadataRegistryRecord::graph_iri_for(config.document_id)
}

fn permission_path(config: &CreateDocumentConfig) -> String {
    MetadataRegistryRecord::permission_path_for(
        &config.actor.realm_id,
        config.group_id,
        &config.document_path,
        config.document_id,
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CommitStage {
    /// Reading the accepted create, the realm config and the group fence.
    Fence,
    /// Reading the persistent id mapping's bucket fence.
    PidFence,
    /// Writing the create event and its rows.
    Write,
}

/// The placement a create resolved, kept while its persistent id fence is read.
type Resolved = (RealmConfigDocument, PlacementRef, Vec<NodeId>);

/// The write transaction of a metadata create: reads its fences, resolves the placement from
/// the id and appends the create event with all its rows. A matching accepted create is replayed.
#[derive(Debug, PartialEq)]
pub struct CreateCommitOperation {
    config: CreateDocumentConfig,
    route_profile_status: Option<ProfileValidationStatus>,
    message: Option<String>,
    phase_source: crate::metadata::MetadataPhaseSource,
    stage: CommitStage,
    pending: Option<Resolved>,
    created: Option<CreateDocumentResult>,
}

impl CreateCommitOperation {
    fn holder_node_ids(
        &self,
        config: Option<&RealmConfigDocument>,
        placement: &PlacementRef,
    ) -> Result<Vec<NodeId>, CreateDocumentError> {
        let Some(config) = config else {
            return Err(CreateDocumentError::PlacementBindingUnavailable(
                "realm config unavailable".to_string(),
            ));
        };
        let mut holders = resolve_shard_holders(config, placement);
        sort_node_ids(&mut holders);
        if !holders.contains(&self.config.actor.node_id) {
            return Err(CreateDocumentError::HoldsNoBucket);
        }
        Ok(holders)
    }

    /// Resolves placement from the minted id, never from current path policy.
    /// Non-holders and unresolved bindings fail closed for routed handling.
    fn placement_from_id(
        &self,
        config: Option<&RealmConfigDocument>,
    ) -> Result<PlacementRef, CreateDocumentError> {
        let Some(config) = config else {
            return Err(CreateDocumentError::PlacementBindingUnavailable(
                "realm config unavailable".to_string(),
            ));
        };
        let placement = resolve_metadata_id(
            config,
            self.config.actor.realm_id,
            Some(self.config.group_id),
            self.config.document_id,
        )?;
        if !holds_placement(config, &placement, self.config.actor.node_id) {
            return Err(CreateDocumentError::HoldsNoBucket);
        }
        Ok(placement)
    }

    fn build_record(
        &self,
        holder_node_ids: Vec<NodeId>,
        placement: PlacementRef,
    ) -> MetadataRegistryRecord {
        let now = self.phase_source.now_ms();
        MetadataRegistryRecord {
            realm_id: self.config.actor.realm_id,
            group_id: self.config.group_id,
            document_id: self.config.document_id,
            document_path: MetadataRegistryRecord::normalize_document_path(
                &self.config.document_path,
            ),
            graph_iri: graph_iri(&self.config),
            public: self.config.public,
            permission_path: permission_path(&self.config),
            placement,
            holder_node_ids,
            created_at_ms: now,
            updated_at_ms: now,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }

    fn create_event_record(&self, record: &MetadataRegistryRecord) -> MetadataEventRecord {
        let event_id = self.phase_source.next_id();
        let mut record = record.clone();
        record.establishing_event_id = event_id;
        record.last_event_id = event_id;
        let occurred_at_ms = record.created_at_ms;
        MetadataEventRecord {
            event_id,
            record,
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            payload: CreateDocumentOperation::create_event_payload(&self.config),
            occurred_at_ms,
        }
    }

    /// The realm config joins the fence read set read-only: it is never written
    /// back, so concurrent creates stay conflict-free, while a real config change
    /// conflicts the commit and makes the retry re-choose its placement.
    fn fence_reads(&self) -> Vec<(String, Key)> {
        let realm_target = DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        };
        vec![
            (
                CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                create_acceptance_key(self.config.document_id),
            ),
            (
                realm_target.storage_keyspace().to_string(),
                realm_target.storage_key(),
            ),
            crate::groups::fence::group_fence_key(self.config.group_id),
        ]
    }

    fn fence_read(&mut self, result: IoResult) -> CommitTurn {
        let values = read_values(result)?;
        let [acceptance, realm_config, group_fence] =
            <[_; 3]>::try_from(values).map_err(|_| IoError::Unexpected {
                expected: "three create fence values",
                got: "another count of values",
            })?;
        aruna_core::structs::identity::group_delete::check_group_write(group_fence.as_deref())?;
        if let Some(bytes) = acceptance {
            let event: MetadataEventRecord =
                postcard::from_bytes(&bytes).map_err(aruna_core::errors::ConversionError::from)?;
            if !accepted_create_matches(&self.config, &event) {
                return Err(CreateDocumentError::DocumentAlreadyExists);
            }
            return Ok(Next::Done(CreateDocumentResult {
                record: event.record,
                event_id: event.event_id,
            }));
        }
        let Some(config_bytes) = realm_config else {
            return Err(CreateDocumentError::PlacementBindingUnavailable(
                "realm config unavailable".to_string(),
            ));
        };
        let config = RealmConfigDocument::from_bytes(&config_bytes)?;
        let placement = self.placement_from_id(Some(&config))?;
        let holders = self.holder_node_ids(Some(&config), &placement)?;
        self.ensure_pid_fence(config, placement, holders)
    }

    fn ensure_pid_fence(
        &mut self,
        realm_config: RealmConfigDocument,
        placement: PlacementRef,
        holders: Vec<NodeId>,
    ) -> CommitTurn {
        let route = mapping_route_for(
            &realm_config,
            self.config.actor.realm_id,
            self.config.document_id,
            self.config.actor.node_id,
        );
        let Some(route) = route else {
            return self.append_create_event(&realm_config, placement, holders);
        };
        if route.peers.first().copied() != Some(self.config.actor.node_id) {
            return Err(CreateDocumentError::HoldsNoBucket);
        }
        if route.generation == 0 {
            return self.append_create_event(&realm_config, placement, holders);
        }
        let read = aruna_core::placement::fence::fence_read(&route.realm_id, &route.placement);
        self.pending = Some((realm_config, placement, holders));
        self.stage = CommitStage::PidFence;
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn pid_fence_read(&mut self, result: IoResult) -> CommitTurn {
        let value = read_value(result)?;
        let Some((config, placement, holders)) = self.pending.take() else {
            return Err(CreateDocumentError::PlacementFenced);
        };
        let Some(route) = mapping_route_for(
            &config,
            self.config.actor.realm_id,
            self.config.document_id,
            self.config.actor.node_id,
        ) else {
            return Err(CreateDocumentError::PlacementFenced);
        };
        if !aruna_core::placement::fence::admits(value.as_ref(), route.generation) {
            return Err(CreateDocumentError::PlacementFenced);
        }
        self.append_create_event(&config, placement, holders)
    }

    fn append_create_event(
        &mut self,
        realm_config: &RealmConfigDocument,
        placement: PlacementRef,
        holders: Vec<NodeId>,
    ) -> CommitTurn {
        let record = self.build_record(holders, placement);
        let create_event = self.create_event_record(&record);
        let encoded_bytes = postcard::experimental::serialized_size(&create_event)
            .map_err(aruna_core::errors::ConversionError::from)?;
        let encoded_bytes =
            u64::try_from(encoded_bytes).map_err(|_| CreateDocumentError::RawLimit)?;
        if encoded_bytes > RAW_BYTES_LIMIT {
            return Err(CreateDocumentError::RawLimit);
        }
        let Some(raw_budget) = raw_quotas(
            create_event.record.document_id,
            &create_event.record.holder_node_ids,
            create_event.node_id,
            encoded_bytes,
        )
        .and_then(|budgets| {
            budgets
                .into_iter()
                .find(|budget| budget.node_id == create_event.node_id)
        }) else {
            return Err(CreateDocumentError::RawLimit);
        };
        let Some(mut status) = self.route_profile_status.clone() else {
            return Err(MetadataError::Backend(
                "profile validation status is missing before create commit".to_string(),
            )
            .into());
        };
        status.document_id = create_event.record.document_id;
        status.dataset_revision = create_event.event_id;
        let mut writes = create_projection_entries(&create_event)?;
        writes.push(raw_budget_entry(&raw_budget)?);
        writes.push(create_acceptance_entry(&create_event)?);
        writes.push(profile_validation_entry(&status)?);
        // The accepting node journals the creation for plugins in its commit batch.
        let now = aruna_core::time::unix_timestamp_millis();
        let created = SourceEvent::dataset_created(&create_event, now);
        writes.extend(journal_entries(&created)?);
        if let Some(message) = &self.message {
            writes.push(commit_message_entry(&create_event, message)?);
        }
        writes.extend(self.pid_entries(realm_config, &create_event)?);
        self.created = Some(CreateDocumentResult {
            record: create_event.record,
            event_id: create_event.event_id,
        });
        self.stage = CommitStage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// The persistent id mint job and the requested mapping of the new document.
    fn pid_entries(
        &self,
        realm_config: &RealmConfigDocument,
        create_event: &MetadataEventRecord,
    ) -> Result<Vec<(String, Key, Value)>, aruna_core::errors::ConversionError> {
        let profile = match &self.config.payload {
            CreateDocumentPayload::Scaffold { .. } => false,
            CreateDocumentPayload::RoCrate { jsonld } => {
                crate::metadata::profile::rules::rocrate_is_profile(
                    jsonld,
                    &create_event.record.graph_iri,
                )
                .map_err(aruna_core::errors::ConversionError::FromStrError)?
            }
        };
        let dedup_key = pid_dedup_key(create_event.record.document_id);
        let job_id = crate::jobs::submit::mint_configured_job(
            realm_config,
            create_event.node_id,
            &dedup_key,
        )
        .map_err(|error| aruna_core::errors::ConversionError::FromStrError(error.to_string()))?;
        let mut job = JobRecord::new(
            job_id,
            JobPayload::MintPersistentId(MintPersistentSpec {
                document_id: create_event.record.document_id,
                minted_by: create_event.user_id,
            }),
            create_event.user_id,
            create_event.node_id,
            create_event.occurred_at_ms,
            create_event.occurred_at_ms,
            Some(dedup_key),
        );
        job.workspace_mode = WorkspaceMode::None;
        job.retention_ms = RETENTION_MS;
        let mut writes = aruna_core::structs::execution::job::job_insert_entries(&job)?;
        let route = mapping_route_for(
            realm_config,
            create_event.record.realm_id,
            create_event.record.document_id,
            create_event.node_id,
        );
        let mapping = PersistentIdMapping::requested(
            create_event.record.document_id,
            profile,
            create_event.user_id,
            job_id,
            create_event.record.public,
            create_event.record.permission_path.clone(),
            mapping_revision(&route, create_event.occurred_at_ms),
        );
        writes.extend(transition_entries(&route, &mapping).map_err(|error| {
            aruna_core::errors::ConversionError::FromStrError(error.to_string())
        })?);
        Ok(writes)
    }
}

type CommitTurn = Result<Next<CreateDocumentResult>, CreateDocumentError>;

impl Operation for CreateCommitOperation {
    type Output = CreateDocumentResult;
    type Error = CreateDocumentError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> CommitTurn {
        Ok(Next::Call(IoCall::Read(self.fence_reads())))
    }

    fn next(&mut self, result: IoResult) -> CommitTurn {
        match self.stage {
            CommitStage::Fence => self.fence_read(result),
            CommitStage::PidFence => self.pid_fence_read(result),
            CommitStage::Write => {
                result.done()?;
                Ok(Next::Done(self.created.clone().ok_or(IoError::Unfinished)?))
            }
        }
    }
}

pub(crate) fn accepted_create_matches(
    config: &CreateDocumentConfig,
    event: &MetadataEventRecord,
) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(&config.document_path);
    event.event_id != Ulid::nil()
        && event.record.document_id == config.document_id
        && event.record.realm_id == config.actor.realm_id
        && event.record.group_id == config.group_id
        && event.record.document_path == normalized_path
        && event.record.graph_iri == MetadataRegistryRecord::graph_iri_for(config.document_id)
        && event.record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &config.actor.realm_id,
                config.group_id,
                &normalized_path,
                config.document_id,
            )
        && event.record.public == config.public
        && event.record.created_at_ms == event.record.updated_at_ms
        && event.record.created_at_ms == event.occurred_at_ms
        && event.record.establishing_event_id == event.event_id
        && event.record.last_event_id == event.event_id
        && event.user_id == config.actor.user_id
        && event.payload == CreateDocumentOperation::create_event_payload(config)
}

pub fn resolve_metadata_id(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: Option<GroupId>,
    document_id: Ulid,
) -> Result<PlacementRef, CreateDocumentError> {
    let id = MetaResourceId::from_bytes(document_id.to_bytes()).map_err(|error| {
        CreateDocumentError::PlacementBindingUnavailable(format!(
            "document id is not a structured id: {error}"
        ))
    })?;
    let resolved = config.binding_directory().resolve_id(&id, |strategy_id| {
        config
            .strategy(&strategy_id)
            .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
    })?;
    if resolved.document_class != DocumentClass::Metadata {
        return Err(CreateDocumentError::PlacementBindingUnavailable(
            "document id does not name metadata placement".to_string(),
        ));
    }
    let scope_matches = match resolved.scope {
        PlacementScope::Realm(id) => id == realm_id,
        PlacementScope::Group(id) => group_id.is_none_or(|group_id| id == group_id),
    };
    if !scope_matches {
        return Err(CreateDocumentError::PlacementBindingUnavailable(
            "document id placement scope does not match create target".to_string(),
        ));
    }
    Ok(PlacementRef {
        strategy_id: resolved.strategy_id,
        shard: u32::from(resolved.bucket.get()),
    })
}

/// Mints a local held-bucket id from an already-loaded config.
pub fn mint_local_document(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
) -> Result<MetaResourceId, CreateDocumentError> {
    mint_document_for(config, actor, group_id, document_path, false)
}

/// Mints a forwarded blind-bucket id from an already-loaded config.
pub fn mint_forward_document(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
) -> Result<MetaResourceId, CreateDocumentError> {
    mint_document_for(config, actor, group_id, document_path, true)
}

pub(crate) fn mint_document_for(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
    forward_blind: bool,
) -> Result<MetaResourceId, CreateDocumentError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(document_path);
    let (handle, placement) =
        resolve_create_placement(config, actor, group_id, &normalized, forward_blind)?;
    mint_document_id(handle, &placement)
}

/// Resolves the `(handle, placement)` a create's id must embed. The handle comes
/// from the pre-provisioned binding for `(scope, Metadata, strategy)` (group
/// scope preferred, realm scope as fallback), and is never allocated here.
fn resolve_create_placement(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    normalized_path: &str,
    forward_blind: bool,
) -> Result<(PlacementHandle, PlacementRef), CreateDocumentError> {
    // A create has no minted id yet, so placement resolves by class/path/group
    // rather than a per-document override (there is no document to key on).
    let target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: Ulid::nil(),
    };
    let context = PlacementResolutionContext {
        group_id: Some(group_id),
        metadata_path: Some(normalized_path),
    };
    let Some((strategy, _)) = strategy_for_target(config, &target, context) else {
        return Err(CreateDocumentError::PlacementBindingUnavailable(
            "no strategy governs the metadata create target".to_string(),
        ));
    };
    let handle = create_handle(config, actor.realm_id, group_id, strategy)?;
    let subject = meta_bucket_subject(actor.realm_id, group_id, normalized_path);
    let placement = if forward_blind {
        PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: shard_for_subject(&subject, strategy.shard_count),
        }
    } else {
        choose_origin_bucket(config, strategy, actor.node_id, &subject)
            .ok_or(CreateDocumentError::HoldsNoBucket)?
    };
    Ok((handle, placement))
}

/// The pre-provisioned handle for `(scope, Metadata, strategy)`, group scope
/// preferred over realm. Fail loud when neither is provisioned.
fn create_handle(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    strategy: &PlacementStrategy,
) -> Result<PlacementHandle, CreateDocumentError> {
    let directory = config.binding_directory();
    directory
        .handle_for(
            PlacementScope::Group(group_id),
            DocumentClass::Metadata,
            strategy.strategy_id,
        )
        .or_else(|| {
            directory.handle_for(
                PlacementScope::Realm(realm_id),
                DocumentClass::Metadata,
                strategy.strategy_id,
            )
        })
        .ok_or_else(|| {
            CreateDocumentError::PlacementBindingUnavailable(format!(
                "no metadata binding for strategy {} in group {group_id} or realm {realm_id}",
                strategy.strategy_id
            ))
        })
}

fn bucket_from_placement(placement: &PlacementRef) -> Result<BucketId, CreateDocumentError> {
    let shard = u16::try_from(placement.shard).map_err(|_| {
        CreateDocumentError::PlacementBindingUnavailable(format!(
            "bucket {} exceeds the 12-bit id field",
            placement.shard
        ))
    })?;
    BucketId::new(shard)
        .map_err(|error| CreateDocumentError::PlacementBindingUnavailable(error.to_string()))
}

fn mint_document_id(
    handle: PlacementHandle,
    placement: &PlacementRef,
) -> Result<MetaResourceId, CreateDocumentError> {
    let bucket = bucket_from_placement(placement)?;
    let mut generator = id_generator()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Ok(generator.mint(handle, bucket)?)
}

fn id_generator() -> &'static Mutex<StructuredIdGenerator> {
    static GENERATOR: OnceLock<Mutex<StructuredIdGenerator>> = OnceLock::new();
    GENERATOR.get_or_init(|| Mutex::new(StructuredIdGenerator::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::create_entry::CreateEntryOperation;

    use std::sync::Arc;

    use aruna_core::StructuredId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{
        DEDUP_INDEX_KEYSPACE, EVENT_LOG_KEYSPACE, ID_MAPPING_KEYSPACE, JOB_KEYSPACE,
        PENDING_PROJECTION_KEYSPACE, RAW_BUDGET_KEYSPACE, REALM_CONFIG_KEYSPACE,
        SCHEDULE_INDEX_KEYSPACE,
    };
    use aruna_core::metadata::RawOriginBudget;
    use aruna_core::placement::resolve_shard_holders;
    use aruna_core::storage_entries::{event_log_prefix, pending_projection_key};
    use aruna_core::structs::PersistentIdStatus;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use aruna_core::structs::placement::record::{
        FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, HandleRange, PlacementBinding,
    };
    use aruna_storage::storage::EffectReceiver;
    use aruna_storage::{FjallStorage, StorageHandle};

    use crate::driver::{DriverContext, drive};
    use crate::metadata::MetadataHandle;

    type Rows = Vec<(String, Key, Value)>;

    fn actor(realm_id: RealmId, key_byte: u8) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[key_byte; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            realm_id,
        }
    }

    // Four servers at replica one: no node holds every bucket, and the holder
    // accepting this low-level create is also its PID authority.
    fn realm_config(actor: &Actor) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(actor.realm_id, Vec::new());
        config.seed_default_placement();
        config.strategies[0].replica_count = Some(1);
        config.ensure_node(actor.node_id, RealmNodeKind::Server);
        for seed in 20..23u8 {
            config.ensure_node(
                iroh::SecretKey::from_bytes(&[seed; 32]).public(),
                RealmNodeKind::Server,
            );
        }
        let range_id = Ulid::from_bytes([19; 16]);
        config.placement_handle_ranges.push(HandleRange {
            range_id,
            owner: actor.node_id,
            start: FIRST_GRANTABLE_HANDLE,
            end: FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE,
        });
        config.placement_bindings.push(PlacementBinding {
            handle: PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            scope: PlacementScope::Realm(actor.realm_id),
            document_class: DocumentClass::JobControl,
            strategy_id: config.default_strategy_id.unwrap(),
            allocator_range_id: Some(range_id),
            allocated_by: Some(actor.node_id),
            allocated_at_ms: Some(1),
        });
        config
    }

    /// Mints the held-bucket structured id the driver would produce for a local
    /// create against `config`, so a step-scripted create resolves a real binding
    /// and a bucket the actor holds (the id the fence and placement now require).
    fn held_doc_id(
        config: &RealmConfigDocument,
        actor: &Actor,
        group_id: GroupId,
        path: &str,
    ) -> Ulid {
        let normalized = MetadataRegistryRecord::normalize_document_path(path);
        let (handle, placement) =
            super::resolve_create_placement(config, actor, group_id, &normalized, false)
                .expect("test config provisions a metadata binding and a held bucket");
        super::mint_document_id(handle, &placement)
            .expect("mint")
            .as_ulid()
    }

    fn config(actor: Actor, group_id: GroupId, document_id: Ulid) -> CreateDocumentConfig {
        CreateDocumentConfig {
            actor,
            group_id,
            document_id,
            document_path: "datasets/fast-create".to_string(),
            public: true,
            payload: CreateDocumentPayload::Scaffold {
                name: "Fast Create".to_string(),
                description: "Validate then append only".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
        }
    }

    fn validation_result(document_id: Ulid) -> IoResult {
        IoResult::Event(Box::new(Event::Metadata(MetadataEvent::ValidationResult {
            graph_iri: format!("https://w3id.org/aruna/{document_id}"),
        })))
    }

    /// A create in a realm where `actor` holds the bucket its minted id names.
    fn held_create(seed: u8, path: &str) -> (Actor, RealmConfigDocument, CreateDocumentConfig) {
        let actor = actor(RealmId([seed; 32]), seed);
        let realm_config = realm_config(&actor);
        let group_id = GroupId::generate();
        let document_id = held_doc_id(&realm_config, &actor, group_id, path);
        let mut config = config(actor.clone(), group_id, document_id);
        config.document_path = path.to_string();
        (actor, realm_config, config)
    }

    fn fence_values(actor: &Actor, realm_config: &RealmConfigDocument) -> IoResult {
        let config = realm_config.to_bytes(actor).expect("realm config encodes");
        IoResult::Values(vec![None, Some(config.to_vec().into()), None])
    }

    /// Answers the commit's fence reads, and its persistent id fence read if it has one.
    fn commit_turn(
        commit: &mut CreateCommitOperation,
        actor: &Actor,
        realm_config: &RealmConfigDocument,
    ) -> CommitTurn {
        let reads = commit.first().unwrap();
        let Next::Call(IoCall::Read(reads)) = reads else {
            panic!("expected the create fence read");
        };
        assert_eq!(
            reads
                .iter()
                .map(|(space, _)| space.as_str())
                .collect::<Vec<_>>(),
            [
                CREATE_ACCEPTANCE_KEYSPACE,
                REALM_CONFIG_KEYSPACE,
                aruna_core::keyspaces::GROUP_DELETE_KEYSPACE,
            ]
        );
        match commit.next(fence_values(actor, realm_config))? {
            Next::Call(IoCall::Read(pid)) if pid.len() == 1 => {
                commit.next(IoResult::Values(vec![None]))
            }
            turn => Ok(turn),
        }
    }

    fn create_rows(
        config: CreateDocumentConfig,
        actor: &Actor,
        realm_config: &RealmConfigDocument,
    ) -> (CreateCommitOperation, Rows) {
        let mut commit = CreateDocumentOperation::new_generated_id(config).commit_operation();
        match commit_turn(&mut commit, actor, realm_config) {
            Ok(Next::Call(IoCall::Write(rows))) => (commit, rows),
            other => panic!("expected the create writes, got {other:?}"),
        }
    }

    fn row<'a>(rows: &'a Rows, key_space: &str) -> &'a (String, Key, Value) {
        rows.iter()
            .find(|(space, _, _)| space == key_space)
            .unwrap_or_else(|| panic!("missing {key_space}"))
    }

    /// Checks the rows of an accepted create and returns its event.
    fn assert_create_rows(rows: &Rows, document_id: Ulid, actor: &Actor) -> MetadataEventRecord {
        for required in [
            JOB_KEYSPACE,
            SCHEDULE_INDEX_KEYSPACE,
            DEDUP_INDEX_KEYSPACE,
            ID_MAPPING_KEYSPACE,
        ] {
            row(rows, required);
        }
        assert!(
            !rows
                .iter()
                .any(|(space, _, _)| space == REALM_CONFIG_KEYSPACE)
        );
        let (_, key, value) = row(rows, EVENT_LOG_KEYSPACE);
        assert!(
            key.as_ref()
                .starts_with(event_log_prefix(document_id).as_ref())
        );
        let event: MetadataEventRecord = postcard::from_bytes(value).expect("event decodes");
        assert_eq!(event.record.document_id, document_id);
        assert!(event.record.holder_node_ids.contains(&actor.node_id));
        assert!(
            event
                .record
                .holder_node_ids
                .windows(2)
                .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
        );
        assert_eq!(event.user_id, actor.user_id);
        assert_eq!(event.node_id, actor.node_id);
        assert!(matches!(
            &event.payload,
            MetadataEventPayload::Scaffold { .. }
        ));

        let job = JobRecord::from_bytes(&row(rows, JOB_KEYSPACE).2).expect("PID job decodes");
        let mapping = PersistentIdMapping::from_bytes(&row(rows, ID_MAPPING_KEYSPACE).2)
            .expect("PID intent decodes");
        assert!(matches!(
            job.payload,
            JobPayload::MintPersistentId(ref spec) if spec.document_id == document_id
        ));
        assert_eq!(
            rows.iter()
                .filter(|(space, _, _)| space == JOB_KEYSPACE)
                .count(),
            1,
            "one create attempt carries one PID job"
        );
        assert_eq!(mapping.job_id, Some(job.job_id));
        assert_eq!(mapping.status, PersistentIdStatus::Requested);

        let (_, acceptance_key, acceptance) = row(rows, CREATE_ACCEPTANCE_KEYSPACE);
        assert_eq!(acceptance_key, &create_acceptance_key(document_id));
        let accepted: MetadataEventRecord = postcard::from_bytes(acceptance).unwrap();
        assert_eq!(accepted, event);

        let (_, marker_key, marker) = row(rows, PENDING_PROJECTION_KEYSPACE);
        assert_eq!(
            marker_key,
            &pending_projection_key(document_id, event.event_id)
        );
        assert!(marker.as_ref().is_empty());

        let budget: RawOriginBudget = postcard::from_bytes(&row(rows, RAW_BUDGET_KEYSPACE).2)
            .expect("raw origin budget decodes");
        assert_eq!(budget.document_id, document_id);
        assert_eq!(budget.node_id, actor.node_id);
        assert_eq!(budget.events, 1);
        assert!(budget.event_limit >= budget.events);
        assert!(budget.byte_limit >= budget.encoded_bytes);
        event
    }

    #[test]
    fn create_runs_ordered() {
        // Validation and the registry check run outside a transaction; the commit is a child.
        let (_, _, config) = held_create(8, "datasets/fast-create");
        let document_id = config.document_id;
        let mut operation = CreateDocumentOperation::new(config);
        assert_eq!(operation.transaction(), TransactionMode::None);
        let Ok(Next::Call(IoCall::Effect(validation))) = operation.first() else {
            panic!("expected the crate validation");
        };
        let Effect::Metadata(MetadataEffect::ValidateCreateCrate { request }) = *validation else {
            panic!("expected the create crate validation");
        };
        assert_eq!(
            request.graph_iri,
            format!("https://w3id.org/aruna/{document_id}")
        );
        assert_eq!(
            request.durability,
            MetadataRequestDurability::WalAlreadyDurable
        );
        let existing = vec![(
            DOCUMENT_INDEX_KEYSPACE.to_string(),
            metadata_document_key(document_id),
        )];
        assert_eq!(
            operation.next(validation_result(document_id)),
            Ok(Next::Call(IoCall::Read(existing)))
        );
        assert!(matches!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let created = CreateDocumentResult {
            record: MetadataRegistryRecord {
                document_id,
                ..commit_record()
            },
            event_id: Ulid::from_parts(5, 5),
        };
        let outcome = ChildOutcome::new(Ok::<_, CreateDocumentError>(created.clone()));
        assert_eq!(
            operation.next(IoResult::Child(outcome)),
            Ok(Next::Done(created))
        );
    }

    fn commit_record() -> MetadataRegistryRecord {
        CreateDocumentOperation::new(config(
            actor(RealmId([1; 32]), 1),
            GroupId::from_bytes([1; 16]),
            Ulid::nil(),
        ))
        .commit_operation()
        .build_record(Vec::new(), PlacementRef::NIL)
    }

    #[test]
    fn generated_skips_check() {
        let (_, _, config) = held_create(6, "datasets/fast-create");
        let document_id = config.document_id;
        let mut operation = CreateDocumentOperation::new_generated_id(config);
        operation.first().unwrap();
        assert!(matches!(
            operation.next(validation_result(document_id)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
    }

    #[test]
    fn existing_row_refuses() {
        let (_, _, config) = held_create(9, "datasets/fast-create");
        let document_id = config.document_id;
        let mut operation = CreateDocumentOperation::new(config);
        operation.first().unwrap();
        operation.next(validation_result(document_id)).unwrap();
        let stored = postcard::to_allocvec(&commit_record()).unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![Some(stored.into())])),
            Err(CreateDocumentError::DocumentAlreadyExists)
        );
    }

    #[test]
    fn commit_writes_create() {
        let (actor, realm_config, config) = held_create(12, "datasets/fast-create");
        let document_id = config.document_id;
        let (mut commit, rows) = create_rows(config, &actor, &realm_config);
        assert_eq!(commit.transaction(), TransactionMode::Write);
        let event = assert_create_rows(&rows, document_id, &actor);
        assert_eq!(
            commit.next(IoResult::Done),
            Ok(Next::Done(CreateDocumentResult {
                record: event.record,
                event_id: event.event_id,
            }))
        );
    }

    #[test]
    fn create_conflict_fence() {
        // A rerun after a lost conflict goes straight to the commit; its fence read replays an
        // identical accepted create and refuses a different one.
        let (actor, realm_config, config) = held_create(31, "datasets/fast-create");
        let mut winner: MetadataEventRecord = {
            let (_, rows) = create_rows(config.clone(), &actor, &realm_config);
            postcard::from_bytes(&row(&rows, CREATE_ACCEPTANCE_KEYSPACE).2).unwrap()
        };
        winner.event_id = Ulid::from_bytes([33; 16]);
        winner.record.establishing_event_id = winner.event_id;
        winner.record.last_event_id = winner.event_id;
        assert!(accepted_create_matches(&config, &winner));
        let mut mismatched = winner.clone();
        mismatched.payload = MetadataEventPayload::RoCrate {
            jsonld: "{}".to_string(),
        };
        assert!(!accepted_create_matches(&config, &mismatched));

        let mut rerun = CreateDocumentOperation::new_generated_id(config.clone());
        assert_eq!(rerun.on_conflict(1), AfterConflict::Rerun);
        assert!(matches!(rerun.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(
            CreateDocumentOperation::new_generated_id(config.clone()).on_conflict(4),
            AfterConflict::Fail
        );

        let accepted = |event: &MetadataEventRecord| {
            let mut commit =
                CreateDocumentOperation::new_generated_id(config.clone()).commit_operation();
            commit.first().unwrap();
            let acceptance = postcard::to_allocvec(event).unwrap();
            commit.next(IoResult::Values(vec![Some(acceptance.into()), None, None]))
        };
        assert_eq!(
            accepted(&winner),
            Ok(Next::Done(CreateDocumentResult {
                record: winner.record.clone(),
                event_id: winner.event_id,
            }))
        );
        assert_eq!(
            accepted(&mismatched),
            Err(CreateDocumentError::DocumentAlreadyExists)
        );
    }

    // The phase source is sampled when the record and event are built, not in
    // the constructor, so a fixed source pins the whole create trace.
    #[test]
    fn create_source_pinned() {
        let realm_id = RealmId([43u8; 32]);
        let actor = actor(realm_id, 5);
        let document_id = Ulid::from_bytes([43; 16]);
        let commit = CreateDocumentOperation::new(config(
            actor,
            GroupId::from_bytes([0x43; 16]),
            document_id,
        ))
        .with_phase_source(crate::metadata::MetadataPhaseSource::fixed(
            || 1_700_000_000_000,
            || Ulid::from_bytes([0x77; 16]),
        ))
        .commit_operation();

        let record = commit.build_record(Vec::new(), PlacementRef::NIL);
        assert_eq!(record.created_at_ms, 1_700_000_000_000);
        assert_eq!(record.updated_at_ms, 1_700_000_000_000);
        let event = commit.create_event_record(&record);
        assert_eq!(event.event_id, Ulid::from_bytes([0x77; 16]));
        assert_eq!(event.occurred_at_ms, 1_700_000_000_000);
    }

    #[test]
    fn fresh_copy_resets() {
        let (_, _, config) = held_create(41, "datasets/fast-create");
        let document_id = config.document_id;
        let mut operation = CreateDocumentOperation::new_generated_id(config.clone());
        operation.first().unwrap();
        operation.next(validation_result(document_id)).unwrap();
        operation.conflict_recheck = true;

        let mut expected = CreateDocumentOperation::new_generated_id(config);
        // The status carries its construction time; the copy keeps the original's.
        expected.route_profile_status = operation.route_profile_status.clone();
        assert_eq!(operation.fresh_copy(), expected);
    }

    #[test]
    fn profile_create_records() {
        let (actor, realm_config, mut config) = held_create(7, "profiles/example");
        let document_id = config.document_id;
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        config.payload = CreateDocumentPayload::RoCrate {
            jsonld: serde_json::json!({
                "@context": "https://w3id.org/ro/crate/1.2/context",
                "@graph": [
                    {
                        "@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork",
                        "about": {"@id": graph_iri.clone()}
                    },
                    {
                        "@id": graph_iri,
                        "@type": ["Dataset", "http://www.w3.org/ns/dx/prof/Profile"]
                    }
                ]
            })
            .to_string(),
        };
        let (_, rows) = create_rows(config, &actor, &realm_config);
        let mappings = rows
            .iter()
            .filter(|(space, _, _)| space == ID_MAPPING_KEYSPACE)
            .map(|(_, _, value)| PersistentIdMapping::from_bytes(value).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(mappings.len(), 1);
        assert_eq!(
            mappings[0].pid,
            PersistentIdMapping::profile_pid(document_id)
        );
        assert_ne!(
            mappings[0].pid,
            MetadataRegistryRecord::graph_iri_for(document_id)
        );
    }

    fn profile_config(document_path: &str) -> CreateDocumentConfig {
        let realm_id = RealmId([13u8; 32]);
        let document_id = Ulid::generate();
        let mut config = config(actor(realm_id, 8), GroupId::generate(), document_id);
        config.document_path = document_path.to_string();
        config.payload = CreateDocumentPayload::RoCrate {
            jsonld: serde_json::json!({
                "@context": "https://w3id.org/ro/crate/1.2/context",
                "@graph": [
                    {
                        "@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork",
                        "about": {"@id": "./"}
                    },
                    {
                        "@id": "./",
                        "@type": ["Dataset", "http://www.w3.org/ns/dx/prof/Profile"]
                    }
                ]
            })
            .to_string(),
        };
        config
    }

    #[test]
    fn profiles_stay_inside() {
        let mut outside =
            CreateDocumentOperation::new_generated_id(profile_config("resolver-check/profile"));
        let Err(CreateDocumentError::MetadataError(MetadataError::InvalidInput(message))) =
            outside.first()
        else {
            panic!("expected the Profile path refusal");
        };
        assert!(message.contains("Profiles must be stored under profiles/"));

        let mut inside =
            CreateDocumentOperation::new_generated_id(profile_config("/profiles/example/"));
        let Ok(Next::Call(IoCall::Effect(validation))) = inside.first() else {
            panic!("expected crate validation");
        };
        assert!(matches!(
            *validation,
            Effect::Metadata(MetadataEffect::ValidateRoCrate { .. })
        ));
    }

    #[test]
    fn stamped_bucket_held() {
        // Topic membership is the holder set, so the origin can only publish a
        // create onto a bucket it holds: the stamp must land in its held set.
        let (actor, realm_config, config) = held_create(21, "datasets/fast-create");
        let document_id = config.document_id;
        let (mut commit, _) = create_rows(config, &actor, &realm_config);
        let Ok(Next::Done(created)) = commit.next(IoResult::Done) else {
            panic!("expected the created document");
        };
        let placement = created.record.placement;
        assert_ne!(placement, PlacementRef::NIL);
        assert!(resolve_shard_holders(&realm_config, &placement).contains(&actor.node_id));
        // The stamped placement bucket is exactly the bucket carried in the id.
        let id = MetaResourceId::from_bytes(document_id.to_bytes()).expect("structured id");
        assert_eq!(u32::from(id.bucket().get()), placement.shard);
    }

    #[test]
    fn rejects_unstructured_id() {
        // An unstructured document id (reserved handle 0) must hard-error at the
        // placement/routing boundary, never be silently treated as absent.
        let realm_id = RealmId([44u8; 32]);
        let actor = actor(realm_id, 3);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        // random == 1 packs handle 0 (bits 60..79): a plain, unstructured ULID.
        let document_id = Ulid::from_parts(1_700_000_000_000, 1);
        assert!(MetaResourceId::from_bytes(document_id.to_bytes()).is_err());
        let commit =
            CreateDocumentOperation::new_generated_id(config(actor, group_id, document_id))
                .commit_operation();
        assert!(matches!(
            commit.placement_from_id(Some(&realm_config)),
            Err(CreateDocumentError::PlacementBindingUnavailable(_))
        ));
    }

    #[test]
    fn validation_rejects_append() {
        let document_id = Ulid::generate();
        let mut operation = CreateDocumentOperation::new(config(
            actor(RealmId([13u8; 32]), 8),
            GroupId::generate(),
            document_id,
        ));
        operation.first().unwrap();
        let rejected = Event::Metadata(MetadataEvent::Error {
            graph_iri: Some(format!("https://w3id.org/aruna/{document_id}")),
            error: MetadataError::InvalidInput("invalid RO-Crate".to_string()),
        });
        assert_eq!(
            operation.next(IoResult::Event(Box::new(rejected))),
            Err(CreateDocumentError::MetadataError(
                MetadataError::InvalidInput("invalid RO-Crate".to_string())
            ))
        );
    }

    #[test]
    fn errors_map() {
        assert_eq!(
            CreateDocumentError::from(IoError::Storage(StorageError::WriteError("boom".into()))),
            CreateDocumentError::StorageError(StorageError::WriteError("boom".into()))
        );
        assert!(CreateDocumentOperation::is_conflict(
            &CreateDocumentError::from(IoError::Conflict)
        ));
        assert_eq!(
            CreateDocumentError::from(IoError::Aborted),
            CreateDocumentError::Io(IoError::Aborted)
        );
        let (actor, realm_config, config) = held_create(10, "datasets/fast-create");
        let (mut commit, _) = create_rows(config, &actor, &realm_config);
        assert!(matches!(
            commit.next(IoResult::Values(Vec::new())),
            Err(CreateDocumentError::Io(IoError::Unexpected { .. }))
        ));
    }

    // Answers each storage effect of the create flow; the first `conflict_commits` commits
    // fail with a conflict and the rest succeed. Only the realm config read finds a row.
    fn scripted_conflict_actor(
        receiver: EffectReceiver,
        conflict_commits: u32,
        realm_config: Vec<u8>,
    ) -> std::thread::JoinHandle<u32> {
        std::thread::spawn(move || {
            let mut starts = 0u32;
            let mut commits = 0u32;
            while let Ok((effect, response_tx, _span, _at, _guard)) = receiver.recv() {
                let event = match effect {
                    StorageEffect::StartTransaction { .. } => {
                        starts += 1;
                        StorageEvent::TransactionStarted {
                            txn_id: Ulid::from_parts(u64::from(starts), 1),
                        }
                    }
                    StorageEffect::Read { key, .. } => {
                        StorageEvent::ReadResult { key, value: None }
                    }
                    StorageEffect::BatchRead { reads, .. } => StorageEvent::BatchReadResult {
                        values: reads
                            .into_iter()
                            .map(|(key_space, key)| {
                                let found = (key_space == REALM_CONFIG_KEYSPACE)
                                    .then(|| realm_config.clone().into());
                                (key, found)
                            })
                            .collect(),
                    },
                    StorageEffect::BatchWrite { .. } => StorageEvent::BatchWriteResult {
                        entries: Vec::new(),
                    },
                    StorageEffect::CommitTransaction { txn_id } => {
                        commits += 1;
                        if commits <= conflict_commits {
                            StorageEvent::Error {
                                error: StorageError::TransactionConflict,
                            }
                        } else {
                            StorageEvent::TransactionCommitted { txn_id }
                        }
                    }
                    StorageEffect::AbortTransaction { txn_id } => {
                        StorageEvent::TransactionAborted { txn_id }
                    }
                    StorageEffect::Write { key, .. } => StorageEvent::WriteResult { key },
                    other => panic!("unexpected storage effect: {other:?}"),
                };
                response_tx.send(event);
            }
            starts
        })
    }

    fn conflict_test_context(storage: StorageHandle, temp: &std::path::Path) -> Arc<DriverContext> {
        let node_id = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let metadata_storage =
            FjallStorage::open(temp.join("meta-store").to_str().unwrap()).unwrap();
        let graph_handle = aruna_graph::GraphHandle::open(
            temp.join("meta"),
            node_id,
            None,
            None,
            aruna_graph::GraphOptions::default(),
        )
        .unwrap();
        let metadata_handle = MetadataHandle::new(
            temp.join("meta"),
            metadata_storage,
            None,
            Default::default(),
        );
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            graph_handle: Some(graph_handle),
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        })
    }

    // Real timers: the scripted storage runs on an OS thread, so a paused clock
    // would auto-advance the storage request timeout before it can respond.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_recovers_conflict() {
        // A commit conflict on the first drive is retried; the fresh transaction
        // then commits, so the wrapper returns Ok.
        let temp = tempfile::tempdir().unwrap();
        let (storage, receivers) = StorageHandle::new();

        let realm_id = RealmId([61u8; 32]);
        let actor = actor(realm_id, 9);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let document_id = held_doc_id(&realm_config, &actor, group_id, "datasets/fast-create");
        let config_bytes = realm_config.to_bytes(&actor).expect("realm config encodes");
        let actor_thread = scripted_conflict_actor(receivers.foreground, 1, config_bytes);
        let context = conflict_test_context(storage, temp.path());

        let operation =
            CreateDocumentOperation::new_generated_id(config(actor, group_id, document_id));
        let result = drive(
            CreateEntryOperation::new(
                operation,
                context.net_handle.as_ref().map(|net| net.node_id()),
            ),
            &context,
        )
        .await;
        assert!(result.is_ok(), "retry recovers conflict: {result:?}");

        drop(context);
        let starts = actor_thread.join().unwrap();
        assert_eq!(starts, 2, "the lost commit and the rerun each open one txn");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_exhausts_conflict() {
        // Every commit conflicts, so the wrapper exhausts its retries and returns
        // the conflict; each attempt opens one txn.
        let temp = tempfile::tempdir().unwrap();
        let (storage, receivers) = StorageHandle::new();

        let realm_id = RealmId([62u8; 32]);
        let actor = actor(realm_id, 9);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let document_id = held_doc_id(&realm_config, &actor, group_id, "datasets/fast-create");
        let config_bytes = realm_config.to_bytes(&actor).expect("realm config encodes");
        let actor_thread = scripted_conflict_actor(receivers.foreground, u32::MAX, config_bytes);
        let context = conflict_test_context(storage, temp.path());

        let operation =
            CreateDocumentOperation::new_generated_id(config(actor, group_id, document_id));
        let result = drive(
            CreateEntryOperation::new(
                operation,
                context.net_handle.as_ref().map(|net| net.node_id()),
            ),
            &context,
        )
        .await;
        assert!(matches!(
            result,
            Err(CreateDocumentError::StorageError(
                StorageError::TransactionConflict
            ))
        ));

        drop(context);
        let starts = actor_thread.join().unwrap();
        assert_eq!(starts, 4, "four attempts each open one txn");
    }
}
