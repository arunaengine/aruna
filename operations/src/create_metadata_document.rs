use std::collections::HashSet;

use aruna_core::NodeId;
use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{DhtEffect, Effect, NetEffect, StorageEffect};
use aruna_core::events::{DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::keys::realm_presence_key;
use aruna_core::metadata::{
    MetadataCreateCrateRequest, MetadataEffect, MetadataError, MetadataEvent, MetadataGraphPolicy,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    Actor, MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord, RealmConfigDocument,
};
use aruna_core::types::{Effects, GroupId, TxnId};
use chrono::Utc;
use rand::seq::SliceRandom;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::automerge::repository::read_effect;
use crate::metadata::repository::{
    read_registry_by_document_effect, write_audit_effect, write_document_index_effect,
    write_holders_effect, write_registry_effect,
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

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub struct CreateMetadataDocumentOperation {
    config: CreateMetadataDocumentConfig,
    txn_id: Option<TxnId>,
    state: CreateMetadataDocumentState,
    selected_replication_factor: usize,
    record: Option<MetadataRegistryRecord>,
    pending_error: Option<CreateMetadataDocumentError>,
    output: Option<Result<MetadataRegistryRecord, CreateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CreateMetadataDocumentState {
    Init,
    CheckExisting,
    LoadRealmConfig,
    LoadReplicationTargets,
    CreateGraph,
    ReplicateGraph,
    StartTransaction,
    WriteRegistry,
    WriteDocumentIndex,
    WriteHolders,
    WriteAudit,
    CommitTransaction,
    CleanupGraph,
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
    #[error("missing active transaction")]
    MissingTransaction,
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
            txn_id: None,
            state: CreateMetadataDocumentState::Init,
            selected_replication_factor: 1,
            record: None,
            pending_error: None,
            output: None,
        }
    }

    fn realm_config_ref(&self) -> AutomergeDocumentVariant {
        AutomergeDocumentVariant::RealmConfig {
            realm_id: self.config.actor.realm_id.clone(),
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

    fn build_record(&self, holder_node_ids: Vec<NodeId>) -> MetadataRegistryRecord {
        let now = Self::current_timestamp_ms();
        MetadataRegistryRecord {
            realm_id: self.config.actor.realm_id.clone(),
            group_id: self.config.group_id,
            document_id: self.config.document_id,
            document_path: MetadataRegistryRecord::normalize_document_path(
                &self.config.document_path,
            ),
            graph_iri: self.graph_iri(),
            public: self.config.public,
            permission_path: self.permission_path(),
            holder_node_ids,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    fn audit_record(&self, record: &MetadataRegistryRecord) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id: record.realm_id.clone(),
            group_id: record.group_id,
            document_id: record.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            operation: MetadataAuditOperation::Create,
            occurred_at_ms: record.updated_at_ms,
            details: Some(format!("holders={}", record.holder_node_ids.len())),
        }
    }

    fn graph_policy(&self) -> MetadataGraphPolicy {
        MetadataGraphPolicy {
            public: self.config.public,
            permission_paths: vec![self.permission_path()],
        }
        .normalized()
    }

    fn graph_creation_effect(&self) -> Effect {
        let graph_iri = self.graph_iri();
        let policy = self.graph_policy();
        match &self.config.payload {
            CreateMetadataDocumentPayload::Scaffold {
                name,
                description,
                date_published,
                license,
            } => Effect::Metadata(MetadataEffect::CreateCrate {
                request: MetadataCreateCrateRequest {
                    graph_iri,
                    name: name.clone(),
                    description: description.clone(),
                    date_published: date_published.clone(),
                    license: license.clone(),
                    policy,
                },
            }),
            CreateMetadataDocumentPayload::RoCrate { jsonld } => {
                Effect::Metadata(MetadataEffect::ApplyRoCrate {
                    request: aruna_core::metadata::MetadataApplyRoCrateRequest {
                        graph_iri,
                        jsonld: jsonld.clone(),
                        policy,
                    },
                })
            }
        }
    }

    fn fail(&mut self, error: CreateMetadataDocumentError) -> Effects {
        if self.record.is_some() {
            self.pending_error = Some(error);
            self.state = CreateMetadataDocumentState::CleanupGraph;
            return smallvec![Effect::Metadata(MetadataEffect::DeleteGraph {
                graph_iri: self.graph_iri(),
            })];
        }
        self.state = CreateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn fail_without_cleanup(&mut self, error: CreateMetadataDocumentError) -> Effects {
        self.state = CreateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
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

impl Operation for CreateMetadataDocumentOperation {
    type Output = MetadataRegistryRecord;
    type Error = CreateMetadataDocumentError;

    fn start(&mut self) -> Effects {
        self.state = CreateMetadataDocumentState::CheckExisting;
        smallvec![read_registry_by_document_effect(
            self.config.document_id,
            None
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateMetadataDocumentState::CheckExisting => {
                match crate::metadata::repository::parse_registry_read(event) {
                    Ok(Some(_)) => self
                        .fail_without_cleanup(CreateMetadataDocumentError::DocumentAlreadyExists),
                    Ok(None) => {
                        self.state = CreateMetadataDocumentState::LoadRealmConfig;
                        smallvec![read_effect(&self.realm_config_ref(), None)]
                    }
                    Err(crate::metadata::repository::StorageReadError::Storage(error)) => {
                        self.fail_without_cleanup(error.into())
                    }
                    Err(crate::metadata::repository::StorageReadError::Conversion(error)) => {
                        self.fail_without_cleanup(error.into())
                    }
                }
            }
            CreateMetadataDocumentState::LoadRealmConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let realm_config = match value.as_deref() {
                        Some(bytes) => {
                            RealmConfigDocument::from_bytes(bytes).unwrap_or_else(|_| {
                                RealmConfigDocument::default_for_realm(
                                    self.config.actor.realm_id.clone(),
                                )
                            })
                        }
                        None => RealmConfigDocument::default_for_realm(
                            self.config.actor.realm_id.clone(),
                        ),
                    };
                    self.selected_replication_factor =
                        realm_config.metadata_replication_factor_for(self.config.group_id, None);
                    self.state = CreateMetadataDocumentState::LoadReplicationTargets;
                    smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Get {
                        key: *realm_presence_key(&self.config.actor.realm_id).as_bytes(),
                        realm_filter: Some(self.config.actor.realm_id.clone()),
                    }))]
                }
                Event::Storage(StorageEvent::Error { .. }) => {
                    self.selected_replication_factor =
                        RealmConfigDocument::default_for_realm(self.config.actor.realm_id.clone())
                            .metadata_replication_factor_for(self.config.group_id, None);
                    self.state = CreateMetadataDocumentState::LoadReplicationTargets;
                    smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Get {
                        key: *realm_presence_key(&self.config.actor.realm_id).as_bytes(),
                        realm_filter: Some(self.config.actor.realm_id.clone()),
                    }))]
                }
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::LoadReplicationTargets => match event {
                Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, .. })) => {
                    let holder_node_ids = select_metadata_holders(
                        values.into_iter().map(|entry| entry.node_id).collect(),
                        self.config.actor.node_id,
                        self.selected_replication_factor,
                    );
                    self.record = Some(self.build_record(holder_node_ids));
                    self.state = CreateMetadataDocumentState::CreateGraph;
                    smallvec![self.graph_creation_effect()]
                }
                Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
                | Event::Net(NetEvent::Error(_)) => {
                    self.record = Some(self.build_record(vec![self.config.actor.node_id]));
                    self.state = CreateMetadataDocumentState::CreateGraph;
                    smallvec![self.graph_creation_effect()]
                }
                other => self.unexpected_event("replication target lookup", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CreateGraph => match event {
                Event::Metadata(MetadataEvent::CreateCrateResult { .. })
                | Event::Metadata(MetadataEvent::ApplyRoCrateResult { .. }) => {
                    let Some(record) = self.record.clone() else {
                        return self
                            .fail_without_cleanup(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::ReplicateGraph;
                    smallvec![Effect::Metadata(MetadataEffect::ReplicateBootstrap {
                        record
                    })]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("metadata create result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::ReplicateGraph => match event {
                Event::Metadata(MetadataEvent::BootstrapReplicated {
                    replicated_node_ids,
                    ..
                }) => {
                    if let Some(record) = self.record.as_mut() {
                        record.holder_node_ids = replicated_node_ids;
                    }
                    self.state = CreateMetadataDocumentState::StartTransaction;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("metadata bootstrap result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::WriteRegistry;
                    match write_registry_effect(record, Some(txn_id)) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(CreateMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::WriteRegistry => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::WriteDocumentIndex;
                    match write_document_index_effect(record, Some(txn_id)) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(CreateMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("registry write result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::WriteDocumentIndex => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::WriteHolders;
                    match write_holders_effect(record, Some(txn_id)) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(CreateMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("document index write result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::WriteHolders => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::WriteAudit;
                    match write_audit_effect(&self.audit_record(record), Ulid::new(), Some(txn_id))
                    {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(CreateMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("holders write result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::WriteAudit => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::CommitTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("audit write result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    let Some(record) = self.record.clone() else {
                        return self
                            .fail_without_cleanup(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::Finish;
                    self.output = Some(Ok(record));
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CleanupGraph => match event {
                Event::Metadata(MetadataEvent::GraphDeleted { .. })
                | Event::Metadata(MetadataEvent::Error { .. }) => {
                    let error = self
                        .pending_error
                        .take()
                        .expect("cleanup state must have pending error");
                    self.fail_without_cleanup(error)
                }
                other => self.unexpected_event("metadata cleanup result", format!("{other:?}")),
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

pub(crate) fn select_metadata_holders(
    realm_nodes: HashSet<NodeId>,
    local_node_id: NodeId,
    replication_factor: usize,
) -> Vec<NodeId> {
    let remote_target_count = replication_factor.max(1).saturating_sub(1);
    let mut holders = vec![local_node_id];
    if remote_target_count == 0 {
        return holders;
    }

    let mut candidates: Vec<_> = realm_nodes
        .into_iter()
        .filter(|node_id| *node_id != local_node_id)
        .collect();
    let mut rng = rand::rng();
    candidates.shuffle(&mut rng);
    candidates.truncate(remote_target_count);
    holders.extend(candidates);
    holders
}

#[cfg(test)]
mod tests {
    use super::select_metadata_holders;

    use std::collections::HashSet;

    #[test]
    fn select_metadata_holders_includes_local_node() {
        let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let remote_a = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let remote_b = iroh::SecretKey::from_bytes(&[3u8; 32]).public();

        let holders = select_metadata_holders(HashSet::from([local, remote_a, remote_b]), local, 3);

        assert_eq!(holders.len(), 3);
        assert_eq!(holders[0], local);
    }
}
