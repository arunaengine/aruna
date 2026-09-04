use std::sync::{Arc, Mutex, OnceLock};

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_CREATE_ACCEPTANCE_KEYSPACE;
use aruna_core::metadata::{
    METADATA_RAW_BYTES_LIMIT, MetadataCreateCrateRequest, MetadataCreateEventPayload,
    MetadataCreateEventRecord, MetadataEffect, MetadataError, MetadataEvent, MetadataGraphPolicy,
    MetadataProfileValidationStatus, MetadataRequestDurability, raw_quotas,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    metadata_create_acceptance_key, metadata_create_acceptance_write_entry,
    metadata_profile_validation_status_write_entry, raw_budget_entry,
};
use aruna_core::structs::{
    Actor, BindingError, DEFAULT_JOB_RETENTION_MS, DocumentClass, JobPayload, JobRecord,
    MetadataRegistryRecord, MintPersistentIdSpec, PersistentIdMapping, PlacementRef,
    PlacementScope, PlacementStrategy, RealmConfigDocument, RealmId, WorkspaceMode, pid_dedup_key,
    shard_for_subject,
};
use aruna_core::structured_id::{BucketId, PlacementHandle, StructuredIdGenerator};
use aruna_core::types::{Effects, GroupId, TxnId, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_core::{MetaResourceId, StructuredId};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::profile_validation::{
    not_profiled_status, submission_has_profile_tag, validate_submission,
};
use crate::metadata::projector::schedule_pending_metadata_projection_drain;
use crate::metadata::repository::{
    metadata_create_event_and_pending_projection_write_entries, read_registry_by_document_effect,
};
use crate::persistent_id::{mapping_revision, mapping_route_for, transition_entries};
use crate::placement::{
    PlacementResolutionContext, choose_origin_bucket, holds_placement, meta_bucket_subject,
    resolve_shard_holders, strategy_for_target,
};
use crate::queue_backoff::conflict_backoff;
use crate::sync_placement::sort_node_ids;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMetadataDocumentConfig {
    pub actor: Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub document_path: String,
    pub public: bool,
    pub payload: CreateMetadataDocumentPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreateMetadataDocumentPayload {
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
/// Completion means the create event was appended for projection. Graph
/// materialization and replica convergence may still be pending.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateMetadataDocumentResult {
    /// Registry state accepted for the create event.
    pub record: MetadataRegistryRecord,
    /// Durable event id used by projection and replication workers.
    pub event_id: Ulid,
}

/// Validates metadata create input and appends a durable create event.
///
/// A successful operation does not mean the graph has been fully materialized
/// or replicated; callers should treat completion as acceptance into the
/// event/projection pipeline.
#[derive(Debug, PartialEq)]
pub struct CreateMetadataDocumentOperation {
    config: CreateMetadataDocumentConfig,
    skip_existing_check: bool,
    /// Set when a non-holder forwarded this create; the id was already minted at
    /// the origin with the blind-hash bucket, so this node keeps it unchanged.
    forwarded: bool,
    conflict_recheck: bool,
    txn_id: Option<TxnId>,
    state: CreateMetadataDocumentState,
    record: Option<MetadataRegistryRecord>,
    create_event: Option<MetadataCreateEventRecord>,
    profile_validation_status: Option<MetadataProfileValidationStatus>,
    pending_realm_config: Option<RealmConfigDocument>,
    pending_placement: Option<PlacementRef>,
    pending_holders: Vec<NodeId>,
    output: Option<Result<CreateMetadataDocumentResult, CreateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CreateMetadataDocumentState {
    Init,
    ValidateGraph,
    CheckExisting,
    StartTransaction,
    ReadCreateFence,
    ReadPidFence,
    AppendCreateEvent,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateMetadataDocumentError {
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
    OriginHoldsNoBucket,
    /// The id's placement handle resolves to no binding, or no strategy/binding
    /// governs the create target. Fails closed rather than guessing a placement.
    #[error("placement_binding_unavailable: {0}")]
    PlacementBindingUnavailable(String),
    #[error(transparent)]
    PlacementBinding(#[from] BindingError),
    /// The structured-id generator refused to mint under a clock-health fault.
    #[error(transparent)]
    ClockHealth(#[from] aruna_core::structured_id::ClockHealthError),
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("metadata raw update budget exceeded")]
    RawLimit,
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("persistent id mapping bucket is closing; retry the create")]
    PlacementFenced,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl CreateMetadataDocumentOperation {
    pub fn new(config: CreateMetadataDocumentConfig) -> Self {
        let profile_validation_status = match &config.payload {
            CreateMetadataDocumentPayload::Scaffold { .. } => {
                Some(not_profiled_status(config.document_id))
            }
            CreateMetadataDocumentPayload::RoCrate { jsonld }
                if !submission_has_profile_tag(jsonld) =>
            {
                Some(not_profiled_status(config.document_id))
            }
            CreateMetadataDocumentPayload::RoCrate { .. } => None,
        };
        Self {
            config,
            skip_existing_check: false,
            forwarded: false,
            conflict_recheck: false,
            txn_id: None,
            state: CreateMetadataDocumentState::Init,
            record: None,
            create_event: None,
            profile_validation_status,
            pending_realm_config: None,
            pending_placement: None,
            pending_holders: Vec::new(),
            output: None,
        }
    }

    pub fn new_for_generated_document_id(config: CreateMetadataDocumentConfig) -> Self {
        let mut operation = Self::new(config);
        operation.skip_existing_check = true;
        operation
    }

    /// A create a non-holder forwarded here. The document's bucket is its blind
    /// hash rather than this node's pick, so every holder the forwarder may try
    /// stamps the same bucket.
    pub fn new_forwarded(config: CreateMetadataDocumentConfig) -> Self {
        let mut operation = Self::new(config);
        operation.forwarded = true;
        operation
    }

    pub fn config(&self) -> &CreateMetadataDocumentConfig {
        &self.config
    }

    /// A pristine operation with the same inputs, for a conflict retry.
    fn fresh_copy(&self) -> Self {
        Self {
            config: self.config.clone(),
            skip_existing_check: self.skip_existing_check,
            forwarded: self.forwarded,
            conflict_recheck: false,
            txn_id: None,
            state: CreateMetadataDocumentState::Init,
            record: None,
            create_event: None,
            profile_validation_status: self.profile_validation_status.clone(),
            pending_realm_config: None,
            pending_placement: None,
            pending_holders: Vec::new(),
            output: None,
        }
    }

    fn graph_iri(&self) -> String {
        MetadataRegistryRecord::graph_iri_for(self.config.document_id)
    }

    fn permission_path(&self) -> String {
        MetadataRegistryRecord::permission_path_for(
            &self.config.actor.realm_id,
            self.config.group_id,
            &self.config.document_path,
            self.config.document_id,
        )
    }

    fn holder_node_ids(
        &self,
        config: Option<&RealmConfigDocument>,
        placement: &PlacementRef,
    ) -> Result<Vec<NodeId>, CreateMetadataDocumentError> {
        let Some(config) = config else {
            return Err(CreateMetadataDocumentError::PlacementBindingUnavailable(
                "realm config unavailable".to_string(),
            ));
        };
        let mut holders = resolve_shard_holders(config, placement);
        sort_node_ids(&mut holders);
        if !holders.contains(&self.config.actor.node_id) {
            return Err(CreateMetadataDocumentError::OriginHoldsNoBucket);
        }
        Ok(holders)
    }

    /// Resolves placement from the minted id, never from current path policy.
    /// Non-holders and unresolved bindings fail closed for routed handling.
    fn placement_from_id(
        &self,
        config: Option<&RealmConfigDocument>,
    ) -> Result<PlacementRef, CreateMetadataDocumentError> {
        let Some(config) = config else {
            return Err(CreateMetadataDocumentError::PlacementBindingUnavailable(
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
            return Err(CreateMetadataDocumentError::OriginHoldsNoBucket);
        }
        Ok(placement)
    }

    fn build_record(
        &self,
        holder_node_ids: Vec<NodeId>,
        placement: PlacementRef,
    ) -> MetadataRegistryRecord {
        let now = unix_timestamp_millis();
        MetadataRegistryRecord {
            realm_id: self.config.actor.realm_id,
            group_id: self.config.group_id,
            document_id: self.config.document_id,
            document_path: MetadataRegistryRecord::normalize_document_path(
                &self.config.document_path,
            ),
            graph_iri: self.graph_iri(),
            public: self.config.public,
            permission_path: self.permission_path(),
            placement,
            holder_node_ids,
            created_at_ms: now,
            updated_at_ms: now,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }

    fn create_event_payload(config: &CreateMetadataDocumentConfig) -> MetadataCreateEventPayload {
        match &config.payload {
            CreateMetadataDocumentPayload::Scaffold {
                name,
                description,
                date_published,
                license,
            } => MetadataCreateEventPayload::Scaffold {
                name: name.clone(),
                description: description.clone(),
                date_published: date_published.clone(),
                license: license.clone(),
            },
            CreateMetadataDocumentPayload::RoCrate { jsonld } => {
                MetadataCreateEventPayload::RoCrate {
                    jsonld: jsonld.clone(),
                }
            }
        }
    }

    fn create_event_record(&self, record: &MetadataRegistryRecord) -> MetadataCreateEventRecord {
        let event_id = Ulid::generate();
        let mut record = record.clone();
        record.establishing_event_id = event_id;
        record.last_event_id = event_id;
        let occurred_at_ms = record.created_at_ms;
        MetadataCreateEventRecord {
            event_id,
            record,
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            payload: Self::create_event_payload(&self.config),
            occurred_at_ms,
        }
    }

    fn graph_policy(&self) -> MetadataGraphPolicy {
        MetadataGraphPolicy {
            public: self.config.public,
            permission_paths: vec![self.permission_path()],
        }
        .normalized()
    }

    fn graph_validation_effect(&self) -> Effect {
        let graph_iri = self.graph_iri();
        let policy = self.graph_policy();
        match &self.config.payload {
            CreateMetadataDocumentPayload::Scaffold {
                name,
                description,
                date_published,
                license,
            } => Effect::Metadata(MetadataEffect::ValidateCreateCrate {
                request: MetadataCreateCrateRequest {
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
            CreateMetadataDocumentPayload::RoCrate { jsonld } => {
                Effect::Metadata(MetadataEffect::ValidateRoCrate {
                    request: aruna_core::metadata::MetadataApplyRoCrateRequest {
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

    fn validation_effect(&mut self) -> Effects {
        self.state = CreateMetadataDocumentState::ValidateGraph;
        smallvec![self.graph_validation_effect()]
    }

    fn start_transaction_effect(&mut self) -> Effects {
        self.state = CreateMetadataDocumentState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    /// The realm config joins the fence read set read-only: it is never written
    /// back, so concurrent creates stay conflict-free, while a real config change
    /// conflicts the commit and makes the retry re-choose its placement.
    fn read_create_fence_effect(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = CreateMetadataDocumentState::ReadCreateFence;
        let realm_target = DocumentSyncTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                    metadata_create_acceptance_key(self.config.document_id),
                ),
                (
                    realm_target.storage_keyspace().to_string(),
                    realm_target.storage_key(),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn finish_accepted_create(&mut self, event: MetadataCreateEventRecord) -> Effects {
        let Some(txn_id) = self.txn_id.take() else {
            return self.fail(CreateMetadataDocumentError::MissingTransaction);
        };
        self.record = Some(event.record.clone());
        self.create_event = Some(event.clone());
        self.state = CreateMetadataDocumentState::Finish;
        self.output = Some(Ok(CreateMetadataDocumentResult {
            record: event.record,
            event_id: event.event_id,
        }));
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn apply_create_fence(
        &mut self,
        acceptance_value: Option<Value>,
        realm_config_value: Option<Value>,
    ) -> Effects {
        if let Some(bytes) = acceptance_value {
            let event: MetadataCreateEventRecord = match postcard::from_bytes(&bytes) {
                Ok(event) => event,
                Err(error) => {
                    return self.fail(CreateMetadataDocumentError::ConversionError(error.into()));
                }
            };
            return if accepted_create_matches(&self.config, &event) {
                self.finish_accepted_create(event)
            } else {
                self.fail(CreateMetadataDocumentError::DocumentAlreadyExists)
            };
        }
        if self.conflict_recheck {
            return self.fail(StorageError::TransactionConflict.into());
        }

        let Some(config_bytes) = realm_config_value.as_ref() else {
            return self.fail(CreateMetadataDocumentError::PlacementBindingUnavailable(
                "realm config unavailable".to_string(),
            ));
        };
        let config = match RealmConfigDocument::from_bytes(config_bytes) {
            Ok(config) => config,
            Err(error) => return self.fail(error.into()),
        };
        match self.placement_from_id(Some(&config)) {
            Ok(placement) => match self.holder_node_ids(Some(&config), &placement) {
                Ok(holders) => self.read_pid_fence_or_append(config, placement, holders),
                Err(error) => self.fail(error),
            },
            Err(error) => self.fail(error),
        }
    }

    fn read_pid_fence_or_append(
        &mut self,
        realm_config: RealmConfigDocument,
        placement: PlacementRef,
        holders: Vec<NodeId>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(CreateMetadataDocumentError::MissingTransaction);
        };
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
            return self.fail(CreateMetadataDocumentError::OriginHoldsNoBucket);
        }
        if route.generation == 0 {
            return self.append_create_event(&realm_config, placement, holders);
        }
        let (key_space, key) =
            crate::placement::fence::fence_read(&route.realm_id, &route.placement);
        self.pending_realm_config = Some(realm_config);
        self.pending_placement = Some(placement);
        self.pending_holders = holders;
        self.state = CreateMetadataDocumentState::ReadPidFence;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space,
            key,
            txn_id: Some(txn_id),
        })]
    }

    fn append_create_event(
        &mut self,
        realm_config: &RealmConfigDocument,
        placement: PlacementRef,
        holders: Vec<NodeId>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(CreateMetadataDocumentError::MissingTransaction);
        };
        let record = self.build_record(holders, placement);
        let create_event = self.create_event_record(&record);
        self.create_event = Some(create_event.clone());
        self.record = Some(create_event.record.clone());
        self.state = CreateMetadataDocumentState::AppendCreateEvent;
        let encoded_bytes = match postcard::experimental::serialized_size(&create_event) {
            Ok(size) => size,
            Err(error) => {
                return self.fail(CreateMetadataDocumentError::ConversionError(error.into()));
            }
        };
        let encoded_bytes = match u64::try_from(encoded_bytes) {
            Ok(size) => size,
            Err(_) => return self.fail(CreateMetadataDocumentError::RawLimit),
        };
        if encoded_bytes > METADATA_RAW_BYTES_LIMIT {
            return self.fail(CreateMetadataDocumentError::RawLimit);
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
            return self.fail(CreateMetadataDocumentError::RawLimit);
        };
        let Some(mut status) = self.profile_validation_status.clone() else {
            return self.fail(
                MetadataError::Backend(
                    "profile validation status is missing before create commit".to_string(),
                )
                .into(),
            );
        };
        status.document_id = create_event.record.document_id;
        status.dataset_revision = create_event.event_id;
        let writes = metadata_create_event_and_pending_projection_write_entries(&create_event)
            .and_then(|mut writes| {
                writes.push(raw_budget_entry(&raw_budget)?);
                writes.push(metadata_create_acceptance_write_entry(&create_event)?);
                writes.push(metadata_profile_validation_status_write_entry(&status)?);
                Ok(writes)
            });
        match writes.and_then(|mut writes| {
            let profile = match &self.config.payload {
                CreateMetadataDocumentPayload::Scaffold { .. } => false,
                CreateMetadataDocumentPayload::RoCrate { jsonld } => {
                    crate::metadata::stats::rocrate_is_profile(
                        jsonld,
                        &create_event.record.graph_iri,
                    )
                    .map_err(aruna_core::errors::ConversionError::FromStrError)?
                }
            };
            let dedup_key = pid_dedup_key(create_event.record.document_id);
            let job_id = crate::jobs::service::mint_local_job_from_config(
                realm_config,
                create_event.node_id,
                &dedup_key,
            )
            .map_err(|error| {
                aruna_core::errors::ConversionError::FromStrError(error.to_string())
            })?;
            let mut job = JobRecord::new(
                job_id,
                JobPayload::MintPersistentId(MintPersistentIdSpec {
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
            job.retention_ms = DEFAULT_JOB_RETENTION_MS;
            writes.extend(crate::jobs::store::job_insert_entries(&job)?);

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
        }) {
            Ok(writes) => {
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: Some(txn_id),
                })]
            }
            Err(error) => self.fail(CreateMetadataDocumentError::ConversionError(error)),
        }
    }

    fn commit_transaction_effect(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(CreateMetadataDocumentError::MissingTransaction);
        };
        self.state = CreateMetadataDocumentState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: CreateMetadataDocumentError) -> Effects {
        let cleanup = self.abort();
        self.state = CreateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn fail_without_cleanup(&mut self, error: CreateMetadataDocumentError) -> Effects {
        self.fail(error)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(CreateMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

pub(crate) fn accepted_create_matches(
    config: &CreateMetadataDocumentConfig,
    event: &MetadataCreateEventRecord,
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
        && event.payload == CreateMetadataDocumentOperation::create_event_payload(config)
}

pub fn resolve_metadata_id(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: Option<GroupId>,
    document_id: Ulid,
) -> Result<PlacementRef, CreateMetadataDocumentError> {
    let id = MetaResourceId::from_bytes(document_id.to_bytes()).map_err(|error| {
        CreateMetadataDocumentError::PlacementBindingUnavailable(format!(
            "document id is not a structured id: {error}"
        ))
    })?;
    let resolved = config.binding_directory().resolve_id(&id, |strategy_id| {
        config
            .strategy(&strategy_id)
            .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
    })?;
    if resolved.document_class != DocumentClass::Metadata {
        return Err(CreateMetadataDocumentError::PlacementBindingUnavailable(
            "document id does not name metadata placement".to_string(),
        ));
    }
    let scope_matches = match resolved.scope {
        PlacementScope::Realm(id) => id == realm_id,
        PlacementScope::Group(id) => group_id.is_none_or(|group_id| id == group_id),
    };
    if !scope_matches {
        return Err(CreateMetadataDocumentError::PlacementBindingUnavailable(
            "document id placement scope does not match create target".to_string(),
        ));
    }
    Ok(PlacementRef {
        strategy_id: resolved.strategy_id,
        shard: u32::from(resolved.bucket.get()),
    })
}

const CREATE_CONFLICT_RETRIES: usize = 3;

pub async fn create_metadata_document(
    mut template: CreateMetadataDocumentOperation,
    context: Arc<DriverContext>,
) -> Result<CreateMetadataDocumentResult, CreateMetadataDocumentError> {
    // Only local nil-sentinel creates mint here; supplied and forwarded ids persist.
    if !template.forwarded && template.config.document_id.is_nil() {
        let document_id = mint_local_id(context.as_ref(), &template.config).await?;
        template.config.document_id = document_id.as_ulid();
    }
    template.profile_validation_status = Some(match &template.config.payload {
        CreateMetadataDocumentPayload::RoCrate { jsonld } => {
            validate_submission(
                context.as_ref(),
                template.config.document_id,
                template.config.group_id,
                jsonld,
            )
            .await?
        }
        CreateMetadataDocumentPayload::Scaffold { .. } => {
            not_profiled_status(template.config.document_id)
        }
    });
    let mut attempt = 0usize;
    let created = loop {
        match drive(template.fresh_copy(), context.as_ref()).await {
            Ok(created) => break created,
            Err(CreateMetadataDocumentError::StorageError(StorageError::TransactionConflict))
                if attempt < CREATE_CONFLICT_RETRIES =>
            {
                tokio::time::sleep(conflict_backoff(
                    attempt,
                    &template.config.document_id.to_bytes(),
                ))
                .await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    };
    schedule_pending_metadata_projection_drain(context.as_ref(), std::time::Duration::ZERO)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    if let Some(task_handle) = context.task_handle.as_ref() {
        task_handle
            .send_effect(crate::jobs::submit::schedule_job_drain_effect())
            .await;
    }
    Ok(created)
}

/// Mints from a local held bucket and a pre-provisioned binding.
/// Missing holdership or bindings fail closed for routed handling.
pub async fn mint_local_id(
    context: &DriverContext,
    config: &CreateMetadataDocumentConfig,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    let realm_config = load_create_config(context, config.actor.realm_id).await?;
    mint_document_for(
        &realm_config,
        &config.actor,
        config.group_id,
        &config.document_path,
        false,
    )
}

/// Mints a job-produced metadata document id with the blind placement.
pub async fn mint_job_document(
    context: &DriverContext,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    let realm_config = load_create_config(context, actor.realm_id).await?;
    mint_document_for(&realm_config, actor, group_id, document_path, true)
}

/// Mints a local held-bucket id from an already-loaded config.
pub fn mint_local_document(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    mint_document_for(config, actor, group_id, document_path, false)
}

/// Mints a forwarded blind-bucket id from an already-loaded config.
pub fn mint_forward_document(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    mint_document_for(config, actor, group_id, document_path, true)
}

fn mint_document_for(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
    forward_blind: bool,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(document_path);
    let (handle, placement) =
        resolve_create_placement(config, actor, group_id, &normalized, forward_blind)?;
    mint_document_id(handle, &placement)
}

/// Resolves the `(handle, placement)` a create's id must embed. The handle comes
/// from the pre-provisioned binding for `(scope, Metadata, strategy)` — group
/// scope preferred, realm scope as fallback — and is never allocated here.
fn resolve_create_placement(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    normalized_path: &str,
    forward_blind: bool,
) -> Result<(PlacementHandle, PlacementRef), CreateMetadataDocumentError> {
    // A create has no minted id yet, so placement resolves by class/path/group
    // rather than a per-document override (there is no document to key on).
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: Ulid::nil(),
    };
    let context = PlacementResolutionContext {
        group_id: Some(group_id),
        metadata_path: Some(normalized_path),
    };
    let Some((strategy, _)) = strategy_for_target(config, &target, context) else {
        return Err(CreateMetadataDocumentError::PlacementBindingUnavailable(
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
            .ok_or(CreateMetadataDocumentError::OriginHoldsNoBucket)?
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
) -> Result<PlacementHandle, CreateMetadataDocumentError> {
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
            CreateMetadataDocumentError::PlacementBindingUnavailable(format!(
                "no metadata binding for strategy {} in group {group_id} or realm {realm_id}",
                strategy.strategy_id
            ))
        })
}

fn bucket_from_placement(
    placement: &PlacementRef,
) -> Result<BucketId, CreateMetadataDocumentError> {
    let shard = u16::try_from(placement.shard).map_err(|_| {
        CreateMetadataDocumentError::PlacementBindingUnavailable(format!(
            "bucket {} exceeds the 12-bit id field",
            placement.shard
        ))
    })?;
    BucketId::new(shard).map_err(|error| {
        CreateMetadataDocumentError::PlacementBindingUnavailable(error.to_string())
    })
}

fn mint_document_id(
    handle: PlacementHandle,
    placement: &PlacementRef,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
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

async fn load_create_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, CreateMetadataDocumentError> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => Ok(RealmConfigDocument::from_bytes(&bytes)?),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Err(CreateMetadataDocumentError::PlacementBindingUnavailable(
                "realm config document missing".to_string(),
            ))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(CreateMetadataDocumentError::PlacementBindingUnavailable(
            format!("unexpected storage event reading realm config: {other:?}"),
        )),
    }
}

impl Operation for CreateMetadataDocumentOperation {
    type Output = CreateMetadataDocumentResult;
    type Error = CreateMetadataDocumentError;

    fn start(&mut self) -> Effects {
        self.validation_effect()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateMetadataDocumentState::ValidateGraph => match event {
                Event::Metadata(MetadataEvent::ValidationResult { .. }) => {
                    if self.skip_existing_check {
                        return self.start_transaction_effect();
                    }
                    self.state = CreateMetadataDocumentState::CheckExisting;
                    smallvec![read_registry_by_document_effect(
                        self.config.document_id,
                        None
                    )]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("metadata validation result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CheckExisting => {
                match crate::metadata::repository::parse_registry_read(event) {
                    Ok(Some(_)) => self
                        .fail_without_cleanup(CreateMetadataDocumentError::DocumentAlreadyExists),
                    Ok(None) => self.start_transaction_effect(),
                    Err(crate::metadata::repository::StorageReadError::Storage(error)) => {
                        self.fail_without_cleanup(error.into())
                    }
                    Err(crate::metadata::repository::StorageReadError::Conversion(error)) => {
                        self.fail_without_cleanup(error.into())
                    }
                }
            }
            CreateMetadataDocumentState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.read_create_fence_effect(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::ReadCreateFence => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, acceptance_value), (_, realm_config_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "metadata create fence read",
                            format!("batch read with {} values", values.len()),
                        );
                    };
                    self.apply_create_fence(acceptance_value.clone(), realm_config_value.clone())
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("create fence read result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::ReadPidFence => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(config) = self.pending_realm_config.take() else {
                        return self.fail(CreateMetadataDocumentError::PlacementFenced);
                    };
                    let Some(placement) = self.pending_placement.take() else {
                        return self.fail(CreateMetadataDocumentError::PlacementFenced);
                    };
                    let holders = std::mem::take(&mut self.pending_holders);
                    let Some(route) = mapping_route_for(
                        &config,
                        self.config.actor.realm_id,
                        self.config.document_id,
                        self.config.actor.node_id,
                    ) else {
                        return self.fail(CreateMetadataDocumentError::PlacementFenced);
                    };
                    if !crate::placement::fence::admits(value.as_ref(), route.generation) {
                        return self.fail(CreateMetadataDocumentError::PlacementFenced);
                    }
                    self.append_create_event(&config, placement, holders)
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("persistent id fence read", format!("{other:?}")),
            },
            CreateMetadataDocumentState::AppendCreateEvent => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    self.commit_transaction_effect()
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => {
                    self.unexpected_event("metadata create event append", format!("{other:?}"))
                }
            },
            CreateMetadataDocumentState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    let Some(record) = self.record.clone() else {
                        return self
                            .fail_without_cleanup(CreateMetadataDocumentError::MissingTransaction);
                    };
                    let Some(create_event) = self.create_event.as_ref() else {
                        return self
                            .fail_without_cleanup(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::Finish;
                    self.output = Some(Ok(CreateMetadataDocumentResult {
                        record,
                        event_id: create_event.event_id,
                    }));
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }) => {
                    self.txn_id = None;
                    self.record = None;
                    self.create_event = None;
                    self.pending_realm_config = None;
                    self.pending_placement = None;
                    self.pending_holders.clear();
                    self.conflict_recheck = true;
                    self.start_transaction_effect()
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::Finish
            | CreateMetadataDocumentState::Error
            | CreateMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateMetadataDocumentState::Finish | CreateMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(CreateMetadataDocumentError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
        CreateMetadataDocumentPayload, accepted_create_matches, create_metadata_document,
    };

    use std::sync::Arc;

    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        JOB_DEDUP_INDEX_KEYSPACE, JOB_KEYSPACE, JOB_SCHEDULE_INDEX_KEYSPACE,
        METADATA_CREATE_ACCEPTANCE_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
        METADATA_EVENT_LOG_KEYSPACE, METADATA_PENDING_PROJECTION_KEYSPACE,
        METADATA_RAW_BUDGET_KEYSPACE, PERSISTENT_ID_MAPPING_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::metadata::{
        MetadataCreateEventPayload, MetadataCreateEventRecord, MetadataEffect, MetadataError,
        MetadataEvent, MetadataRawOriginBudget, MetadataRequestDurability,
    };
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::{
        metadata_create_acceptance_key, metadata_event_log_prefix, metadata_pending_projection_key,
    };
    use aruna_core::structs::{
        Actor, DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, HandleRange, JobPayload,
        JobRecord, MetadataRegistryRecord, PersistentIdMapping, PersistentIdStatus,
        PlacementBinding, PlacementRef, PlacementScope, RealmConfigDocument, RealmId,
        RealmNodeKind,
    };
    use aruna_core::types::{Effects, GroupId, Key};
    use aruna_core::{MetaResourceId, PlacementHandle, StructuredId};
    use aruna_storage::storage::EffectReceiver;
    use aruna_storage::{FjallStorage, StorageHandle};
    use ulid::Ulid;

    use crate::driver::DriverContext;
    use crate::metadata::MetadataHandle;
    use crate::placement::resolve_shard_holders;

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

    fn create_fence_read(
        actor: &Actor,
        document_id: Ulid,
        config: Option<&RealmConfigDocument>,
    ) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (metadata_create_acceptance_key(document_id), None),
                (
                    actor.realm_id.as_bytes().to_vec().into(),
                    config.map(|config| {
                        config
                            .to_bytes(actor)
                            .expect("realm config encodes")
                            .to_vec()
                            .into()
                    }),
                ),
            ],
        })
    }

    fn apply_create_and_pid_fences(
        operation: &mut CreateMetadataDocumentOperation,
        actor: &Actor,
        document_id: Ulid,
        config: &RealmConfigDocument,
    ) -> Effects {
        let effects = operation.step(create_fence_read(actor, document_id, Some(config)));
        let [Effect::Storage(StorageEffect::Read { key, .. })] = effects.as_slice() else {
            return effects;
        };
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: key.clone(),
            value: None,
        }))
    }

    fn assert_fence_read(effects: &[Effect]) {
        let [
            Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: Some(_),
                ..
            }),
        ] = effects
        else {
            panic!("expected create fence read");
        };
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0].0, METADATA_CREATE_ACCEPTANCE_KEYSPACE);
        assert_eq!(reads[1].0, REALM_CONFIG_KEYSPACE);
    }

    fn begin_transaction(
        operation: &mut CreateMetadataDocumentOperation,
        effects: &[Effect],
    ) -> Effects {
        let [Effect::Storage(StorageEffect::StartTransaction { read: false })] = effects else {
            panic!("expected create transaction start");
        };
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([30; 16]),
        }))
    }

    #[test]
    fn create_conflict_fence() {
        let realm_id = RealmId([31u8; 32]);
        let actor = actor(realm_id, 9);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let document_id = held_doc_id(&realm_config, &actor, group_id, "datasets/fast-create");
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        operation.start();
        let effects = operation.step(validation_result(document_id));
        let [Effect::Storage(StorageEffect::StartTransaction { read: false })] = effects.as_slice()
        else {
            panic!("expected create transaction start");
        };

        let txn_id = Ulid::from_bytes([32; 16]);
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_fence_read(effects.as_slice());
        let [
            Effect::Storage(StorageEffect::BatchRead {
                txn_id: Some(read_txn),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected transactional create fence read");
        };
        assert_eq!(*read_txn, txn_id);

        let effects =
            apply_create_and_pid_fences(&mut operation, &actor, document_id, &realm_config);
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(write_txn),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected transactional create append");
        };
        assert_eq!(*write_txn, txn_id);
        assert!(
            !writes
                .iter()
                .any(|(key_space, _, _)| { key_space == REALM_CONFIG_KEYSPACE })
        );
        assert!(
            writes
                .iter()
                .any(|(key_space, _, _)| key_space == METADATA_EVENT_LOG_KEYSPACE)
        );
        assert!(
            writes
                .iter()
                .any(|(key_space, _, _)| key_space == METADATA_PENDING_PROJECTION_KEYSPACE)
        );
        assert!(
            writes
                .iter()
                .any(|(key_space, _, _)| key_space == METADATA_CREATE_ACCEPTANCE_KEYSPACE)
        );
        assert_eq!(
            writes
                .iter()
                .filter(|(key_space, _, _)| key_space == JOB_KEYSPACE)
                .count(),
            1,
            "one create attempt carries one PID job"
        );

        let mut winner: MetadataCreateEventRecord = writes
            .iter()
            .find(|(key_space, _, _)| key_space == METADATA_CREATE_ACCEPTANCE_KEYSPACE)
            .and_then(|(_, _, value)| postcard::from_bytes(value.as_ref()).ok())
            .expect("create acceptance decodes");
        winner.event_id = Ulid::from_bytes([33; 16]);
        winner.record.establishing_event_id = winner.event_id;
        winner.record.last_event_id = winner.event_id;
        assert!(accepted_create_matches(&operation.config, &winner));
        let mut mismatched = winner.clone();
        mismatched.payload = MetadataCreateEventPayload::RoCrate {
            jsonld: "{}".to_string(),
        };
        assert!(!accepted_create_matches(&operation.config, &mismatched));

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: committed })]
                if *committed == txn_id
        ));
        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::TransactionConflict,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));

        let retry_txn = Ulid::from_bytes([34; 16]);
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: retry_txn,
        }));
        assert_fence_read(effects.as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    metadata_create_acceptance_key(document_id),
                    Some(postcard::to_allocvec(&winner).unwrap().into()),
                ),
                (actor.realm_id.as_bytes().to_vec().into(), None),
            ],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
                if *aborted == retry_txn
        ));
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize().expect("winner is replayed"),
            super::CreateMetadataDocumentResult {
                record: winner.record,
                event_id: winner.event_id,
            }
        );
    }

    #[test]
    fn fresh_copy_resets() {
        let realm_id = RealmId([41u8; 32]);
        let actor = actor(realm_id, 11);
        let group_id = GroupId::generate();
        let document_id = Ulid::from_bytes([41; 16]);
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        operation.start();
        let effects = operation.step(validation_result(document_id));
        begin_transaction(&mut operation, effects.as_slice());
        operation.conflict_recheck = true;

        let mut expected = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor,
            group_id,
            document_id,
        ));
        // The status carries its construction time; the copy keeps the original's.
        expected.profile_validation_status = operation.profile_validation_status.clone();
        assert_eq!(operation.fresh_copy(), expected);
    }

    fn config(actor: Actor, group_id: GroupId, document_id: Ulid) -> CreateMetadataDocumentConfig {
        CreateMetadataDocumentConfig {
            actor,
            group_id,
            document_id,
            document_path: "datasets/fast-create".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Fast Create".to_string(),
                description: "Validate then append only".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
        }
    }

    fn validation_result(document_id: Ulid) -> Event {
        Event::Metadata(MetadataEvent::ValidationResult {
            graph_iri: format!("https://w3id.org/aruna/{document_id}"),
        })
    }

    fn assert_validation_effect(effects: &[Effect], document_id: Ulid) {
        let [Effect::Metadata(MetadataEffect::ValidateCreateCrate { request })] = effects else {
            panic!("expected metadata validation effect");
        };
        assert_eq!(
            request.graph_iri,
            format!("https://w3id.org/aruna/{document_id}")
        );
        assert_eq!(
            request.durability,
            MetadataRequestDurability::WalAlreadyDurable
        );
    }

    fn assert_existing_read(effects: &[Effect]) {
        let [
            Effect::Storage(StorageEffect::Read {
                key_space, txn_id, ..
            }),
        ] = effects
        else {
            panic!("expected metadata document index read");
        };
        assert_eq!(key_space, METADATA_DOCUMENT_INDEX_KEYSPACE);
        assert_eq!(txn_id, &None);
    }

    fn assert_create_event_append(effects: &[Effect], document_id: Ulid, actor: &Actor) -> Key {
        let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = effects else {
            panic!("expected metadata create event append");
        };
        assert!(txn_id.is_some());
        for required in [
            JOB_KEYSPACE,
            JOB_SCHEDULE_INDEX_KEYSPACE,
            JOB_DEDUP_INDEX_KEYSPACE,
            PERSISTENT_ID_MAPPING_KEYSPACE,
        ] {
            assert!(
                writes.iter().any(|(key_space, _, _)| key_space == required),
                "atomic create write is missing {required}"
            );
        }
        let (_, key, value) = writes
            .iter()
            .find(|(key_space, _, _)| key_space == METADATA_EVENT_LOG_KEYSPACE)
            .expect("event log write exists");
        assert!(
            key.as_ref()
                .starts_with(metadata_event_log_prefix(document_id).as_ref())
        );

        let event: MetadataCreateEventRecord =
            postcard::from_bytes(value.as_ref()).expect("create event decodes");
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
            MetadataCreateEventPayload::Scaffold { .. }
        ));

        let job: JobRecord = writes
            .iter()
            .find(|(key_space, _, _)| key_space == JOB_KEYSPACE)
            .and_then(|(_, _, value)| JobRecord::from_bytes(value).ok())
            .expect("atomic PID job decodes");
        let mapping = writes
            .iter()
            .find(|(key_space, _, _)| key_space == PERSISTENT_ID_MAPPING_KEYSPACE)
            .and_then(|(_, _, value)| PersistentIdMapping::from_bytes(value).ok())
            .expect("atomic PID intent decodes");
        assert!(matches!(
            job.payload,
            JobPayload::MintPersistentId(ref spec) if spec.document_id == document_id
        ));
        assert_eq!(mapping.job_id, Some(job.job_id));
        assert_eq!(mapping.status, PersistentIdStatus::Requested);

        let (_, acceptance_key, acceptance_value) = writes
            .iter()
            .find(|(key_space, key, _)| {
                key_space == METADATA_CREATE_ACCEPTANCE_KEYSPACE
                    && key == &metadata_create_acceptance_key(document_id)
            })
            .expect("create acceptance write exists");
        assert_eq!(acceptance_key, &metadata_create_acceptance_key(document_id));
        let accepted: MetadataCreateEventRecord =
            postcard::from_bytes(acceptance_value.as_ref()).expect("create acceptance decodes");
        assert_eq!(accepted, event);

        let (_, marker_key, marker_value) = writes
            .iter()
            .find(|(key_space, _, _)| key_space == METADATA_PENDING_PROJECTION_KEYSPACE)
            .expect("pending projection marker write exists");
        assert_eq!(
            marker_key,
            &metadata_pending_projection_key(document_id, event.event_id)
        );
        assert!(marker_value.as_ref().is_empty());

        assert!(
            writes
                .iter()
                .any(|(key_space, _, _)| key_space == METADATA_RAW_BUDGET_KEYSPACE)
        );
        let (_, _, budget_value) = writes
            .iter()
            .find(|(key_space, _, _)| key_space == METADATA_RAW_BUDGET_KEYSPACE)
            .expect("raw origin budget write exists");
        let budget: MetadataRawOriginBudget =
            postcard::from_bytes(budget_value).expect("raw origin budget decodes");
        assert_eq!(budget.document_id, document_id);
        assert_eq!(budget.node_id, actor.node_id);
        assert_eq!(budget.events, 1);
        assert!(budget.event_limit >= budget.events);
        assert!(budget.byte_limit >= budget.encoded_bytes);

        key.clone()
    }

    fn commit_create(operation: &mut CreateMetadataDocumentOperation, effects: &[Effect]) {
        let [Effect::Storage(StorageEffect::CommitTransaction { txn_id })] = effects else {
            panic!("expected create transaction commit");
        };
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: *txn_id,
        }));
        assert!(effects.is_empty());
    }

    #[test]
    fn generated_document_id_validates_then_appends_without_existing_read() {
        let realm_id = RealmId([11u8; 32]);
        let actor = actor(realm_id, 6);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let document_id = held_doc_id(&realm_config, &actor, group_id, "datasets/fast-create");
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        let effects = operation.start();
        assert_validation_effect(effects.as_slice(), document_id);
        let effects = operation.step(validation_result(document_id));
        let effects = begin_transaction(&mut operation, effects.as_slice());
        assert_fence_read(effects.as_slice());
        let effects =
            apply_create_and_pid_fences(&mut operation, &actor, document_id, &realm_config);
        assert_create_event_append(effects.as_slice(), document_id, &actor);
    }

    #[test]
    fn profile_create_records_only_profile_pid() {
        let realm_id = RealmId([12u8; 32]);
        let actor = actor(realm_id, 7);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let document_id = held_doc_id(&realm_config, &actor, group_id, "profiles/example");
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        let mut config = config(actor.clone(), group_id, document_id);
        config.document_path = "profiles/example".to_string();
        config.payload = CreateMetadataDocumentPayload::RoCrate {
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
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config);

        operation.start();
        let effects = operation.step(validation_result(document_id));
        let _ = begin_transaction(&mut operation, effects.as_slice());
        let effects =
            apply_create_and_pid_fences(&mut operation, &actor, document_id, &realm_config);
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected atomic create writes");
        };
        let mappings = writes
            .iter()
            .filter(|(key_space, _, _)| key_space == PERSISTENT_ID_MAPPING_KEYSPACE)
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

    #[test]
    fn config_joins_fence() {
        // The stamped bucket decides the document's sync topic, so the config it
        // came from must be in the commit's read set: no read precedes the
        // transaction, and the fence read carries the realm config.
        let realm_id = RealmId([22u8; 32]);
        let actor = actor(realm_id, 5);
        let group_id = GroupId::generate();
        let document_id = Ulid::generate();
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor,
            group_id,
            document_id,
        ));

        operation.start();
        let effects = operation.step(validation_result(document_id));
        let effects = begin_transaction(&mut operation, effects.as_slice());
        assert_fence_read(effects.as_slice());
    }

    #[test]
    fn stamped_bucket_is_held() {
        // Topic membership is the holder set, so the origin can only publish a
        // create onto a bucket it holds: the stamp must land in its held set.
        let realm_id = RealmId([21u8; 32]);
        let actor = actor(realm_id, 4);
        let realm_config = realm_config(&actor);
        let group_id = GroupId::generate();
        let document_id = held_doc_id(&realm_config, &actor, group_id, "datasets/fast-create");
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        operation.start();
        let effects = operation.step(validation_result(document_id));
        begin_transaction(&mut operation, effects.as_slice());
        let effects =
            apply_create_and_pid_fences(&mut operation, &actor, document_id, &realm_config);
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected transactional create append");
        };
        assert!(
            !writes
                .iter()
                .any(|(key_space, _, _)| key_space == REALM_CONFIG_KEYSPACE)
        );

        let placement = operation
            .record
            .as_ref()
            .expect("create record is built")
            .placement;
        assert_ne!(placement, PlacementRef::NIL);
        assert!(resolve_shard_holders(&realm_config, &placement).contains(&actor.node_id));
        // Guardrail: the stamped placement bucket is exactly the bucket carried
        // in the id (D4/D7 single source), which the RF fixtures cannot expose.
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
        let operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor,
            group_id,
            document_id,
        ));
        assert!(matches!(
            operation.placement_from_id(Some(&realm_config)),
            Err(CreateMetadataDocumentError::PlacementBindingUnavailable(_))
        ));
    }

    #[test]
    fn create_checks_existing_after_validation_and_uses_local_holder() {
        let realm_id = RealmId([8u8; 32]);
        let actor = actor(realm_id, 1);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let document_id = held_doc_id(&realm_config, &actor, group_id, "datasets/fast-create");
        let mut operation =
            CreateMetadataDocumentOperation::new(config(actor.clone(), group_id, document_id));

        let effects = operation.start();
        assert_validation_effect(effects.as_slice(), document_id);
        let effects = operation.step(validation_result(document_id));
        assert_existing_read(effects.as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: document_id.to_bytes().to_vec().into(),
            value: None,
        }));
        let effects = begin_transaction(&mut operation, effects.as_slice());
        assert_fence_read(effects.as_slice());
        let effects =
            apply_create_and_pid_fences(&mut operation, &actor, document_id, &realm_config);
        let create_event_key = assert_create_event_append(effects.as_slice(), document_id, &actor);
        assert!(operation.record.as_ref().is_some_and(|record| {
            record.holder_node_ids.contains(&actor.node_id)
                && record
                    .holder_node_ids
                    .windows(2)
                    .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
        }));

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![(METADATA_EVENT_LOG_KEYSPACE.to_string(), create_event_key)],
        }));
        commit_create(&mut operation, effects.as_slice());
        assert!(operation.is_complete());
        assert_eq!(
            operation
                .finalize()
                .expect("operation succeeds")
                .record
                .document_id,
            document_id
        );
    }

    #[test]
    fn create_returns_after_event_append_without_persistent_effects() {
        let realm_id = RealmId([12u8; 32]);
        let actor = actor(realm_id, 7);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let document_id = held_doc_id(&realm_config, &actor, group_id, "datasets/fast-create");
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        assert_validation_effect(operation.start().as_slice(), document_id);
        let effects = operation.step(validation_result(document_id));
        let effects = begin_transaction(&mut operation, effects.as_slice());
        assert_fence_read(effects.as_slice());
        let effects =
            apply_create_and_pid_fences(&mut operation, &actor, document_id, &realm_config);
        let create_event_key = assert_create_event_append(effects.as_slice(), document_id, &actor);
        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![(METADATA_EVENT_LOG_KEYSPACE.to_string(), create_event_key)],
        }));

        commit_create(&mut operation, effects.as_slice());
        assert!(operation.is_complete());
        assert_eq!(
            operation
                .finalize()
                .expect("operation succeeds")
                .record
                .document_id,
            document_id
        );
    }

    #[test]
    fn validation_failure_does_not_append_event() {
        let realm_id = RealmId([13u8; 32]);
        let actor = actor(realm_id, 8);
        let group_id = GroupId::generate();
        let document_id = Ulid::generate();
        let mut operation =
            CreateMetadataDocumentOperation::new(config(actor, group_id, document_id));

        assert_validation_effect(operation.start().as_slice(), document_id);
        let effects = operation.step(Event::Metadata(MetadataEvent::Error {
            graph_iri: Some(format!("https://w3id.org/aruna/{document_id}")),
            error: MetadataError::InvalidInput("invalid RO-Crate".to_string()),
        }));

        assert!(effects.is_empty());
        assert!(operation.create_event.is_none());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(CreateMetadataDocumentError::MetadataError(
                MetadataError::InvalidInput("invalid RO-Crate".to_string())
            ))
        );
    }

    #[test]
    fn create_event_append_failure_fails_without_projection_cleanup() {
        let realm_id = RealmId([10u8; 32]);
        let actor = actor(realm_id, 5);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let document_id = held_doc_id(&realm_config, &actor, group_id, "datasets/fast-create");
        let mut operation =
            CreateMetadataDocumentOperation::new(config(actor.clone(), group_id, document_id));

        assert_validation_effect(operation.start().as_slice(), document_id);
        assert_existing_read(operation.step(validation_result(document_id)).as_slice());
        let existing_read = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: document_id.to_bytes().to_vec().into(),
            value: None,
        }));
        let fence_read = begin_transaction(&mut operation, existing_read.as_slice());
        assert_fence_read(fence_read.as_slice());
        let append =
            apply_create_and_pid_fences(&mut operation, &actor, document_id, &realm_config);
        assert_create_event_append(append.as_slice(), document_id, &actor);

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::WriteError("boom".to_string()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(CreateMetadataDocumentError::StorageError(
                aruna_core::errors::StorageError::WriteError("boom".to_string()),
            ))
        );
    }

    // Answers each storage effect of the create flow; the first `conflict_commits`
    // commits fail with a conflict and the rest succeed. Returns StartTransaction
    // count so tests can prove a retry occurred.
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
                    // The realm config read resolves the id's placement.
                    StorageEffect::BatchRead { .. } => StorageEvent::BatchReadResult {
                        values: vec![
                            (Key::from(vec![0u8]), None),
                            (Key::from(vec![1u8]), Some(realm_config.clone().into())),
                        ],
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
        let metadata_handle = MetadataHandle::new(
            temp.join("meta"),
            node_id,
            metadata_storage,
            None,
            None,
            None,
        )
        .unwrap();
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: None,
            compute_handle: None,
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

        let operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor,
            group_id,
            document_id,
        ));
        let result = create_metadata_document(operation, context.clone()).await;
        assert!(result.is_ok(), "retry recovers conflict: {result:?}");

        drop(context);
        let starts = actor_thread.join().unwrap();
        assert_eq!(starts, 3, "attempt 1 opens two txns, the retry opens one");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_exhausts_conflict() {
        // Every commit conflicts, so the wrapper exhausts its retries and returns
        // the conflict; each drive opens two txns via the internal recheck.
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

        let operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor,
            group_id,
            document_id,
        ));
        let result = create_metadata_document(operation, context.clone()).await;
        assert!(matches!(
            result,
            Err(CreateMetadataDocumentError::StorageError(
                StorageError::TransactionConflict
            ))
        ));

        drop(context);
        let starts = actor_thread.join().unwrap();
        assert_eq!(starts, 8, "four drive attempts each open two txns");
    }
}
