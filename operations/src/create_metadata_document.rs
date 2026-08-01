use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::METADATA_CREATE_ACCEPTANCE_KEYSPACE;
use aruna_core::metadata::{
    MetadataCreateCrateRequest, MetadataCreateEventPayload, MetadataCreateEventRecord,
    MetadataEffect, MetadataError, MetadataEvent, MetadataGraphPolicy, MetadataRequestDurability,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    metadata_create_acceptance_key, metadata_create_acceptance_write_entry, metadata_path_key,
    metadata_path_write,
};
use aruna_core::structs::{
    Actor, DocumentClass, MetadataRegistryRecord, PlacementRef, PlacementScope, PlacementStrategy,
    RealmConfigDocument, RealmId, shard_for_subject,
};
use aruna_core::structured_id::{BucketId, PlacementHandle, StructuredIdGenerator};
use aruna_core::types::{Effects, GroupId, TxnId, Value};
use aruna_core::{MetaResourceId, StructuredId};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::projector::schedule_pending_metadata_projection_drain;
use crate::metadata::repository::{
    metadata_create_event_and_pending_projection_write_entries, read_registry_by_document_effect,
};
use crate::placement::{
    PlacementResolutionContext, choose_origin_bucket, holds_placement, meta_bucket_subject,
    strategy_for_target,
};

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
    output: Option<Result<CreateMetadataDocumentResult, CreateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CreateMetadataDocumentState {
    Init,
    ValidateGraph,
    CheckExisting,
    StartTransaction,
    ReadCreateFence,
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
    /// The structured-id generator refused to mint under a clock-health fault.
    #[error(transparent)]
    ClockHealth(#[from] aruna_core::structured_id::ClockHealthError),
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl CreateMetadataDocumentOperation {
    pub fn new(config: CreateMetadataDocumentConfig) -> Self {
        Self {
            config,
            skip_existing_check: false,
            forwarded: false,
            conflict_recheck: false,
            txn_id: None,
            state: CreateMetadataDocumentState::Init,
            record: None,
            create_event: None,
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

    fn current_timestamp_ms() -> u64 {
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
    }

    fn holder_node_ids(&self) -> Vec<NodeId> {
        vec![self.config.actor.node_id]
    }

    /// The document's recorded bucket, resolved from the minted id itself
    /// (D7: `id → handle → binding → strategy`, resolved locally). The id already
    /// carries the bucket the origin chose at mint time — a held bucket for a
    /// local create, the blind-hash bucket for a forwarded one — so re-deriving
    /// it here records the identical choice on every holder and never re-chooses
    /// under a changed config (D4: recorded once, never re-derived).
    ///
    /// `Err(OriginHoldsNoBucket)` when this node does not hold the id's bucket:
    /// it could never publish onto that topic, so the create must go to a holder.
    /// `Err(PlacementBindingUnavailable)` when the id is unstructured or its handle
    /// resolves to no binding — the create fails closed rather than guessing.
    fn placement_from_id(
        &self,
        config: Option<&RealmConfigDocument>,
    ) -> Result<PlacementRef, CreateMetadataDocumentError> {
        let Some(config) = config else {
            return Err(CreateMetadataDocumentError::PlacementBindingUnavailable(
                "realm config unavailable".to_string(),
            ));
        };
        // Hard-error on an unstructured document id: routing/placement must never
        // silently treat a non-structured id as absent (guardrail).
        let id = MetaResourceId::from_bytes(self.config.document_id.to_bytes()).map_err(|error| {
            CreateMetadataDocumentError::PlacementBindingUnavailable(format!(
                "document id is not a structured id: {error}"
            ))
        })?;
        let tuple = config
            .binding_directory()
            .resolve(id.placement_handle())
            .map_err(|error| {
                CreateMetadataDocumentError::PlacementBindingUnavailable(error.to_string())
            })?;
        let placement = PlacementRef {
            strategy_id: tuple.strategy_id,
            epoch: 0,
            shard: u32::from(id.bucket().get()),
        };
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
        let now = Self::current_timestamp_ms();
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
            last_event_id: Ulid::nil(),
        }
    }

    fn create_event_payload(&self) -> MetadataCreateEventPayload {
        match &self.config.payload {
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
        record.last_event_id = event_id;
        let occurred_at_ms = record.created_at_ms;
        MetadataCreateEventRecord {
            event_id,
            record,
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            payload: self.create_event_payload(),
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
                    METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                    metadata_path_key(
                        &self.config.actor.realm_id,
                        self.config.group_id,
                        &self.config.document_path,
                    ),
                ),
                (
                    realm_target.storage_keyspace().to_string(),
                    realm_target.storage_key(),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn accepted_identity_matches(&self, event: &MetadataCreateEventRecord) -> bool {
        event.record.document_id == self.config.document_id
            && event.record.realm_id == self.config.actor.realm_id
            && event.record.group_id == self.config.group_id
            && event.record.document_path
                == MetadataRegistryRecord::normalize_document_path(&self.config.document_path)
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
        path_value: Option<Value>,
        realm_config_value: Option<Value>,
    ) -> Effects {
        if let Some(bytes) = acceptance_value {
            let event: MetadataCreateEventRecord = match postcard::from_bytes(&bytes) {
                Ok(event) => event,
                Err(error) => {
                    return self.fail(CreateMetadataDocumentError::ConversionError(error.into()));
                }
            };
            return if self.accepted_identity_matches(&event) {
                self.finish_accepted_create(event)
            } else {
                self.fail(CreateMetadataDocumentError::DocumentAlreadyExists)
            };
        }
        if let Some(bytes) = path_value {
            let event: MetadataCreateEventRecord = match postcard::from_bytes(&bytes) {
                Ok(event) => event,
                Err(error) => {
                    return self.fail(CreateMetadataDocumentError::ConversionError(error.into()));
                }
            };
            return if self.accepted_identity_matches(&event) {
                self.finish_accepted_create(event)
            } else {
                self.fail(CreateMetadataDocumentError::DocumentAlreadyExists)
            };
        }
        if self.conflict_recheck {
            return self.fail(StorageError::TransactionConflict.into());
        }

        let config = match realm_config_value.as_ref() {
            Some(bytes) => match RealmConfigDocument::from_bytes(bytes) {
                Ok(config) => Some(config),
                Err(error) => return self.fail(error.into()),
            },
            None => None,
        };
        match self.placement_from_id(config.as_ref()) {
            Ok(placement) => self.append_create_event_effect(placement),
            Err(error) => self.fail(error),
        }
    }

    fn append_create_event_effect(&mut self, placement: PlacementRef) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(CreateMetadataDocumentError::MissingTransaction);
        };
        let record = self.build_record(self.holder_node_ids(), placement);
        let create_event = self.create_event_record(&record);
        self.create_event = Some(create_event.clone());
        self.record = Some(create_event.record.clone());
        self.state = CreateMetadataDocumentState::AppendCreateEvent;
        let writes = metadata_create_event_and_pending_projection_write_entries(&create_event)
            .and_then(|mut writes| {
                writes.push(metadata_create_acceptance_write_entry(&create_event)?);
                writes.push(metadata_path_write(&create_event)?);
                Ok(writes)
            });
        match writes {
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

const CREATE_CONFLICT_RETRIES: usize = 3;

// Deterministic per-document jitter decorrelates overlapping create retries so a
// genuine realm-config mutation does not make them all retry in lockstep.
fn create_retry_backoff(attempt: usize, document_id: Ulid) -> std::time::Duration {
    let base = crate::queue_backoff::retry_after_ms(attempt as u32, 25, 250);
    let mut head = [0u8; 8];
    head.copy_from_slice(&document_id.to_bytes()[..8]);
    let jitter = u64::from_le_bytes(head) % base;
    std::time::Duration::from_millis(base.saturating_add(jitter))
}

pub async fn create_metadata_document(
    mut template: CreateMetadataDocumentOperation,
    context: Arc<DriverContext>,
) -> Result<CreateMetadataDocumentResult, CreateMetadataDocumentError> {
    // Mint the structured id for a locally-originated create whose id is still the
    // unminted `nil` sentinel: the handle comes from the pre-provisioned binding
    // and the bucket from the node's held set, so the id carries
    // `ts|handle|bucket|nonce`. A forwarded create keeps the id the origin already
    // minted; a caller that supplied a structured id keeps it too.
    if !template.forwarded && template.config.document_id.is_nil() {
        let document_id = mint_local_create_id(context.as_ref(), &template.config).await?;
        template.config.document_id = document_id.as_ulid();
    }
    let mut attempt = 0usize;
    let created = loop {
        match drive(template.fresh_copy(), context.as_ref()).await {
            Ok(created) => break created,
            Err(CreateMetadataDocumentError::StorageError(StorageError::TransactionConflict))
                if attempt < CREATE_CONFLICT_RETRIES =>
            {
                tokio::time::sleep(create_retry_backoff(attempt, template.config.document_id))
                    .await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    };
    schedule_pending_metadata_projection_drain(context.as_ref(), std::time::Duration::ZERO)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    Ok(created)
}

/// Mints the id for a locally-originated create: a held bucket via
/// [`choose_origin_bucket`]. `OriginHoldsNoBucket` when the node holds no bucket
/// (the routed caller forwards); `PlacementBindingUnavailable` when the scope is
/// not provisioned (fail loud — never allocate a handle on the hot path).
pub async fn mint_local_create_id(
    context: &DriverContext,
    config: &CreateMetadataDocumentConfig,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    let realm_config = load_realm_config_for_create(context, config.actor.realm_id).await?;
    mint_document_id_for(
        &realm_config,
        &config.actor,
        config.group_id,
        &config.document_path,
        false,
    )
}

/// The id a non-holder forwards: the deterministic blind-hash bucket of the D8
/// subject `(realm, group, path)`, so every candidate holder the forwarder tries
/// stamps the same bucket and a retry can never fork the document. An already
/// minted id (a retry of the same request) is reused unchanged so forwarded
/// creates stay idempotent; only the unminted `nil` sentinel is freshly minted.
pub async fn mint_forward_create_id(
    context: &DriverContext,
    config: &CreateMetadataDocumentConfig,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    if !config.document_id.is_nil() {
        return MetaResourceId::from_bytes(config.document_id.to_bytes()).map_err(|error| {
            CreateMetadataDocumentError::PlacementBindingUnavailable(format!(
                "forwarded document id is not a structured id: {error}"
            ))
        });
    }
    let realm_config = load_realm_config_for_create(context, config.actor.realm_id).await?;
    mint_document_id_for(
        &realm_config,
        &config.actor,
        config.group_id,
        &config.document_path,
        true,
    )
}

/// Mints a DETERMINISTIC structured id for a job-driven create, keyed on `seed`
/// (the job id bytes) so a restart re-derives the identical id and adoption stays
/// idempotent instead of clock-minting a duplicate. The bucket is the D8
/// blind-hash bucket, so the create routes to that bucket's holders like any
/// forwarded create; the handle is the pre-provisioned Metadata binding. The
/// job embeds this id in its RO-Crate content before the create runs.
pub async fn mint_job_create_id(
    context: &DriverContext,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
    seed: Ulid,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    let realm_config = load_realm_config_for_create(context, actor.realm_id).await?;
    mint_deterministic_create_id(&realm_config, actor, group_id, document_path, seed)
}

/// The sync core of [`mint_job_create_id`], given an already-loaded config: the
/// D8 blind-hash bucket and the pre-provisioned Metadata handle, with the time
/// and nonce fields keyed on `seed` so the same seed always reconstructs the same
/// id (restart idempotency). The bucket is blind, so the create routes to that
/// shard's holders like any forwarded create.
pub fn mint_deterministic_create_id(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
    seed: Ulid,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(document_path);
    let (handle, placement) =
        resolve_create_placement(config, actor, group_id, &normalized, true)?;
    let bucket = bucket_from_placement(&placement)?;
    // Mask the seed's low 8 bytes to the 48-bit nonce field.
    let nonce = u64::from_be_bytes(seed.to_bytes()[8..16].try_into().expect("8 bytes"))
        & ((1u64 << 48) - 1);
    MetaResourceId::from_parts(seed.timestamp_ms(), handle, bucket, nonce)
        .map_err(|error| CreateMetadataDocumentError::PlacementBindingUnavailable(error.to_string()))
}

/// The blind-hash bucket a forwarded create is offered to and stamps: the D8
/// subject `(realm, group, path)` bucket, independent of any node. The forwarder
/// resolves this bucket's holders to pick where to offer the create, and the
/// forwarded id embeds this exact bucket, so targeting and stamping agree.
pub fn forward_bucket_placement(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
) -> Result<PlacementRef, CreateMetadataDocumentError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(document_path);
    resolve_create_placement(config, actor, group_id, &normalized, true).map(|(_, placement)| placement)
}

/// Mints the held-bucket structured id a local create embeds, given an
/// already-loaded config. Exposed for callers that hold the config and drive the
/// create operation directly, bypassing the async mint in
/// [`create_metadata_document`] (integration tests and internal drivers).
pub fn mint_local_document_id(
    config: &RealmConfigDocument,
    actor: &Actor,
    group_id: GroupId,
    document_path: &str,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    mint_document_id_for(config, actor, group_id, document_path, false)
}

fn mint_document_id_for(
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
            epoch: 0,
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
    BucketId::new(shard)
        .map_err(|error| CreateMetadataDocumentError::PlacementBindingUnavailable(error.to_string()))
}

fn mint_document_id(
    handle: PlacementHandle,
    placement: &PlacementRef,
) -> Result<MetaResourceId, CreateMetadataDocumentError> {
    let bucket = bucket_from_placement(placement)?;
    Ok(StructuredIdGenerator::new().mint(handle, bucket)?)
}

async fn load_realm_config_for_create(
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
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Err(
            CreateMetadataDocumentError::PlacementBindingUnavailable(
                "realm config document missing".to_string(),
            ),
        ),
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
                    let [
                        (_, acceptance_value),
                        (_, path_value),
                        (_, realm_config_value),
                    ] = values.as_slice()
                    else {
                        return self.unexpected_event(
                            "metadata create fence read",
                            format!("batch read with {} values", values.len()),
                        );
                    };
                    self.apply_create_fence(
                        acceptance_value.clone(),
                        path_value.clone(),
                        realm_config_value.clone(),
                    )
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("create fence read result", format!("{other:?}")),
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
            .expect("metadata create operation must set output")
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
        CreateMetadataDocumentPayload, create_metadata_document, create_retry_backoff,
    };

    use std::sync::Arc;

    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        METADATA_CREATE_ACCEPTANCE_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
        METADATA_EVENT_LOG_KEYSPACE, METADATA_PENDING_PROJECTION_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::metadata::{
        MetadataCreateEventPayload, MetadataCreateEventRecord, MetadataEffect, MetadataError,
        MetadataEvent, MetadataRequestDurability,
    };
    use aruna_core::operation::Operation;
    use aruna_core::{MetaResourceId, StructuredId};
    use aruna_core::storage_entries::{
        metadata_create_acceptance_key, metadata_event_log_prefix, metadata_path_key,
        metadata_pending_projection_key,
    };
    use aruna_core::structs::{
        Actor, MetadataRegistryRecord, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind,
    };
    use aruna_core::types::{Effects, GroupId, Key};
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

    // Four servers at the default replication factor: no node holds every
    // bucket, so a stamped bucket the origin holds is a real assertion.
    fn realm_config(actor: &Actor) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(actor.realm_id, Vec::new());
        config.seed_default_placement();
        config.ensure_node(actor.node_id, RealmNodeKind::Server);
        for seed in 20..23u8 {
            config.ensure_node(
                iroh::SecretKey::from_bytes(&[seed; 32]).public(),
                RealmNodeKind::Server,
            );
        }
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
        group_id: GroupId,
        document_id: Ulid,
        config: Option<&RealmConfigDocument>,
    ) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (metadata_create_acceptance_key(document_id), None),
                (
                    metadata_path_key(&actor.realm_id, group_id, "datasets/fast-create"),
                    None,
                ),
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
        assert_eq!(reads.len(), 3);
        assert_eq!(reads[0].0, METADATA_CREATE_ACCEPTANCE_KEYSPACE);
        assert_eq!(reads[1].0, METADATA_CREATE_ACCEPTANCE_KEYSPACE);
        assert_eq!(reads[2].0, REALM_CONFIG_KEYSPACE);
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

        let effects = operation.step(create_fence_read(
            &actor,
            group_id,
            document_id,
            Some(&realm_config),
        ));
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

        let mut winner: MetadataCreateEventRecord = writes
            .iter()
            .find(|(key_space, _, _)| key_space == METADATA_CREATE_ACCEPTANCE_KEYSPACE)
            .and_then(|(_, _, value)| postcard::from_bytes(value.as_ref()).ok())
            .expect("create acceptance decodes");
        winner.event_id = Ulid::from_bytes([33; 16]);
        winner.record.last_event_id = winner.event_id;

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
                (
                    metadata_path_key(&actor.realm_id, group_id, "datasets/fast-create"),
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
    fn path_fence_conflicts() {
        let realm_id = RealmId([35u8; 32]);
        let actor = actor(realm_id, 10);
        let group_id = GroupId::generate();
        let document_id = Ulid::from_bytes([35; 16]);
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));
        let mut winner_record = operation.build_record(vec![actor.node_id], PlacementRef::NIL);
        winner_record.document_id = Ulid::from_bytes([36; 16]);
        winner_record.graph_iri = MetadataRegistryRecord::graph_iri_for(winner_record.document_id);
        let winner = operation.create_event_record(&winner_record);

        operation.start();
        let effects = operation.step(validation_result(document_id));
        let effects = begin_transaction(&mut operation, effects.as_slice());
        assert_fence_read(effects.as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (metadata_create_acceptance_key(document_id), None),
                (
                    metadata_path_key(&realm_id, group_id, "datasets/fast-create"),
                    Some(postcard::to_allocvec(&winner).unwrap().into()),
                ),
                (realm_id.as_bytes().to_vec().into(), None),
            ],
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize(),
            Err(CreateMetadataDocumentError::DocumentAlreadyExists)
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

        let expected = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor,
            group_id,
            document_id,
        ));
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
        assert_eq!(writes.len(), 4);
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
        assert_eq!(event.record.holder_node_ids, vec![actor.node_id]);
        assert_eq!(event.user_id, actor.user_id);
        assert_eq!(event.node_id, actor.node_id);
        assert!(matches!(
            &event.payload,
            MetadataCreateEventPayload::Scaffold { .. }
        ));

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

        let (_, path_key, path_value) = writes
            .iter()
            .find(|(key_space, key, _)| {
                key_space == METADATA_CREATE_ACCEPTANCE_KEYSPACE
                    && key
                        == &metadata_path_key(
                            &event.record.realm_id,
                            event.record.group_id,
                            &event.record.document_path,
                        )
            })
            .expect("metadata path fence write exists");
        assert_eq!(
            path_key,
            &metadata_path_key(
                &event.record.realm_id,
                event.record.group_id,
                &event.record.document_path,
            )
        );
        let path_event: MetadataCreateEventRecord =
            postcard::from_bytes(path_value.as_ref()).expect("path fence decodes");
        assert_eq!(path_event, event);

        let (_, marker_key, marker_value) = writes
            .iter()
            .find(|(key_space, _, _)| key_space == METADATA_PENDING_PROJECTION_KEYSPACE)
            .expect("pending projection marker write exists");
        assert_eq!(
            marker_key,
            &metadata_pending_projection_key(document_id, event.event_id)
        );
        assert!(marker_value.as_ref().is_empty());

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
        let effects = operation.step(create_fence_read(
            &actor,
            group_id,
            document_id,
            Some(&realm_config),
        ));
        assert_create_event_append(effects.as_slice(), document_id, &actor);
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
        let effects = operation.step(create_fence_read(
            &actor,
            group_id,
            document_id,
            Some(&realm_config),
        ));
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
    fn unstructured_id_rejected_at_routing() {
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
    fn job_create_id_is_deterministic_and_routable() {
        // The deterministic job path (run-crate / import): the same seed always
        // mints the same structured id, and the id's bucket is the blind D8 bucket
        // the forwarder routes to (so id.bucket == the create's placement shard).
        let realm_id = RealmId([45u8; 32]);
        let actor = actor(realm_id, 3);
        let group_id = GroupId::generate();
        let realm_config = realm_config(&actor);
        let seed = Ulid::from_parts(1_700_000_000_000, 42);

        let first =
            super::mint_deterministic_create_id(&realm_config, &actor, group_id, "runs/x", seed)
                .expect("mint");
        let again =
            super::mint_deterministic_create_id(&realm_config, &actor, group_id, "runs/x", seed)
                .expect("mint");
        assert_eq!(first, again, "same seed re-derives the same id");

        let blind = super::forward_bucket_placement(&realm_config, &actor, group_id, "runs/x")
            .expect("blind placement");
        assert_eq!(u32::from(first.bucket().get()), blind.shard);
        assert!(MetaResourceId::from_bytes(first.to_bytes()).is_ok());
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
        let effects = operation.step(create_fence_read(
            &actor,
            group_id,
            document_id,
            Some(&realm_config),
        ));
        let create_event_key = assert_create_event_append(effects.as_slice(), document_id, &actor);
        assert_eq!(
            operation
                .record
                .as_ref()
                .map(|record| &record.holder_node_ids),
            Some(&vec![actor.node_id])
        );

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
        let effects = operation.step(create_fence_read(
            &actor,
            group_id,
            document_id,
            Some(&realm_config),
        ));
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
        let append = operation.step(create_fence_read(
            &actor,
            group_id,
            document_id,
            Some(&realm_config),
        ));
        assert_create_event_append(append.as_slice(), document_id, &actor);

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::WriteError,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(CreateMetadataDocumentError::StorageError(
                aruna_core::errors::StorageError::WriteError,
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
                    // The fence's third read is the realm config the derive-from-id
                    // placement needs; the acceptance/path reads stay empty.
                    StorageEffect::BatchRead { .. } => StorageEvent::BatchReadResult {
                        values: vec![
                            (Key::from(vec![0u8]), None),
                            (Key::from(vec![1u8]), None),
                            (Key::from(vec![2u8]), Some(realm_config.clone().into())),
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

    #[test]
    fn backoff_jitters() {
        // Backoff is deterministic per document and stays within [base, 2*base).
        let id = Ulid::from_bytes([7u8; 16]);
        let base = crate::queue_backoff::retry_after_ms(0, 25, 250);
        let first = create_retry_backoff(0, id);
        assert_eq!(first, create_retry_backoff(0, id));
        let ms = first.as_millis() as u64;
        assert!(ms >= base && ms < base.saturating_mul(2));
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
            actor, group_id, document_id,
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
            actor, group_id, document_id,
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
