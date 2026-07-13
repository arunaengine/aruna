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
    metadata_create_acceptance_key, metadata_create_acceptance_write_entry,
};
use aruna_core::structs::{
    Actor, MetadataRegistryRecord, PlacementRef, RealmConfigDocument, shard_for_subject,
};
use aruna_core::types::{Effects, GroupId, TxnId, Value};
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
    strategy_for_target, subject_bytes,
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
        license: String,
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
    /// Set when a non-holder forwarded this create; see [`Self::choose_placement`].
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

    fn lifecycle_target(&self) -> DocumentSyncTarget {
        DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: self.config.document_id,
        }
    }

    /// The document's bucket, chosen once here by the receiving node. Stamped at
    /// event-append time only: the projector re-runs on replay, and re-choosing
    /// under a changed config would fork the document across two topics.
    ///
    /// A locally-originated create picks the best-ranked of the buckets this node
    /// already holds, so the origin can always publish onto the bucket it stamps.
    /// A *forwarded* create instead takes the document's blind-hashed bucket: the
    /// forwarder only ever offers the create to holders of that one bucket, so
    /// every candidate holder stamps the same bucket and a retry after a timed-out
    /// response can never fork the document onto a second topic.
    ///
    /// `Err` when a strategy governs the target but this node holds no usable
    /// bucket for it: it could never publish the document onto the bucket's
    /// topic, so the create must go to a holder rather than be accepted onto a
    /// bucket it cannot replicate.
    fn choose_placement(
        &self,
        config: Option<&RealmConfigDocument>,
    ) -> Result<PlacementRef, CreateMetadataDocumentError> {
        let Some(config) = config else {
            return Ok(PlacementRef::NIL);
        };
        let target = self.lifecycle_target();
        let document_path =
            MetadataRegistryRecord::normalize_document_path(&self.config.document_path);
        let context = PlacementResolutionContext {
            group_id: Some(self.config.group_id),
            metadata_path: Some(document_path.as_str()),
        };
        let Some((strategy, _)) = strategy_for_target(config, &target, context) else {
            return Ok(PlacementRef::NIL);
        };
        if self.forwarded {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard: shard_for_subject(&subject_bytes(&target), strategy.shard_count),
            };
            if !holds_placement(config, &placement, self.config.actor.node_id) {
                return Err(CreateMetadataDocumentError::OriginHoldsNoBucket);
            }
            return Ok(placement);
        }
        // Bucket-choice subject is the canonical `(realm_id, group_id, path)`
        // tuple (spec 6.3.6), never the document id: the id must not steer the
        // bucket it embeds.
        choose_origin_bucket(
            config,
            strategy,
            self.config.actor.node_id,
            &meta_bucket_subject(
                self.config.actor.realm_id,
                self.config.group_id,
                &document_path,
            ),
        )
        .ok_or(CreateMetadataDocumentError::OriginHoldsNoBucket)
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
        let event_id = Ulid::r#gen();
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

    fn accepted_identity_matches(&self, event: &MetadataCreateEventRecord) -> bool {
        event.record.realm_id == self.config.actor.realm_id
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
        match self.choose_placement(config.as_ref()) {
            Ok(placement) => self.append_create_event_effect(placement, realm_config_value),
            Err(error) => self.fail(error),
        }
    }

    fn append_create_event_effect(
        &mut self,
        placement: PlacementRef,
        realm_config_value: Option<Value>,
    ) -> Effects {
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
                Ok(writes)
            });
        match writes {
            Ok(mut writes) => {
                if let Some(value) = realm_config_value {
                    let target = DocumentSyncTarget::RealmConfig {
                        realm_id: self.config.actor.realm_id,
                    };
                    writes.push((
                        target.storage_keyspace().to_string(),
                        target.storage_key(),
                        value,
                    ));
                }
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

pub async fn create_metadata_document(
    operation: CreateMetadataDocumentOperation,
    context: Arc<DriverContext>,
) -> Result<CreateMetadataDocumentResult, CreateMetadataDocumentError> {
    let created = drive(operation, context.as_ref()).await?;
    schedule_pending_metadata_projection_drain(context.as_ref(), std::time::Duration::ZERO)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    Ok(created)
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
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
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
        CreateMetadataDocumentPayload,
    };

    use aruna_core::effects::{Effect, StorageEffect};
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
    use aruna_core::storage_entries::{
        metadata_create_acceptance_key, metadata_event_log_prefix, metadata_pending_projection_key,
    };
    use aruna_core::structs::{Actor, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::types::{Effects, GroupId, Key};
    use ulid::Ulid;

    use crate::placement::resolve_shard_holders;

    fn actor(realm_id: RealmId, key_byte: u8) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[key_byte; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::r#gen(), realm_id),
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

    fn realm_config_read(
        config: Option<&RealmConfigDocument>,
        actor: &Actor,
        document_id: Ulid,
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

    fn assert_realm_config_read(effects: &[Effect]) {
        let [
            Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: Some(_),
                ..
            }),
        ] = effects
        else {
            panic!("expected realm config read");
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
        let document_id = Ulid::from_bytes([31; 16]);
        let realm_config = realm_config(&actor);
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            GroupId::r#gen(),
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
        let [
            Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: Some(read_txn),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected transactional realm config read");
        };
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0].0, METADATA_CREATE_ACCEPTANCE_KEYSPACE);
        assert_eq!(reads[1].0, REALM_CONFIG_KEYSPACE);
        assert_eq!(*read_txn, txn_id);

        let effects = operation.step(realm_config_read(Some(&realm_config), &actor, document_id));
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
        assert!(writes.iter().any(|(key_space, key, _)| {
            key_space == REALM_CONFIG_KEYSPACE && key.as_ref() == actor.realm_id.as_bytes()
        }));
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
        assert_realm_config_read(effects.as_slice());
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
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
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
        assert_eq!(writes.len(), 3);
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
            .find(|(key_space, _, _)| key_space == METADATA_CREATE_ACCEPTANCE_KEYSPACE)
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
        let group_id = GroupId::r#gen();
        let document_id = Ulid::r#gen();
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        let effects = operation.start();
        assert_validation_effect(effects.as_slice(), document_id);
        let effects = operation.step(validation_result(document_id));
        let effects = begin_transaction(&mut operation, effects.as_slice());
        assert_realm_config_read(effects.as_slice());
        let effects = operation.step(realm_config_read(None, &actor, document_id));
        assert_create_event_append(effects.as_slice(), document_id, &actor);
    }

    #[test]
    fn stamped_bucket_is_held() {
        // Topic membership is the holder set, so the origin can only publish a
        // create onto a bucket it holds: the stamp must land in its held set.
        let realm_id = RealmId([21u8; 32]);
        let actor = actor(realm_id, 4);
        let realm_config = realm_config(&actor);
        let document_id = Ulid::r#gen();
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            GroupId::r#gen(),
            document_id,
        ));

        operation.start();
        let effects = operation.step(validation_result(document_id));
        begin_transaction(&mut operation, effects.as_slice());
        operation.step(realm_config_read(Some(&realm_config), &actor, document_id));

        let placement = operation
            .record
            .as_ref()
            .expect("create record is built")
            .placement;
        assert_ne!(placement, PlacementRef::NIL);
        assert!(resolve_shard_holders(&realm_config, &placement).contains(&actor.node_id));
    }

    #[test]
    fn create_checks_existing_after_validation_and_uses_local_holder() {
        let realm_id = RealmId([8u8; 32]);
        let actor = actor(realm_id, 1);
        let group_id = GroupId::r#gen();
        let document_id = Ulid::r#gen();
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
        assert_realm_config_read(effects.as_slice());
        let effects = operation.step(realm_config_read(None, &actor, document_id));
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
        let group_id = GroupId::r#gen();
        let document_id = Ulid::r#gen();
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        assert_validation_effect(operation.start().as_slice(), document_id);
        let effects = operation.step(validation_result(document_id));
        let effects = begin_transaction(&mut operation, effects.as_slice());
        assert_realm_config_read(effects.as_slice());
        let effects = operation.step(realm_config_read(None, &actor, document_id));
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
        let group_id = GroupId::r#gen();
        let document_id = Ulid::r#gen();
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
        let group_id = GroupId::r#gen();
        let document_id = Ulid::r#gen();
        let mut operation =
            CreateMetadataDocumentOperation::new(config(actor.clone(), group_id, document_id));

        assert_validation_effect(operation.start().as_slice(), document_id);
        assert_existing_read(operation.step(validation_result(document_id)).as_slice());
        let config_read = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: document_id.to_bytes().to_vec().into(),
            value: None,
        }));
        let config_read = begin_transaction(&mut operation, config_read.as_slice());
        assert_realm_config_read(config_read.as_slice());
        let append = operation.step(realm_config_read(None, &actor, document_id));
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
}
