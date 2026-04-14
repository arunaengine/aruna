use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::{
    MetadataApplyRoCrateRequest, MetadataBatch, MetadataEffect, MetadataError, MetadataEvent,
    MetadataGraphPolicy, MetadataUpsertEntityRequest,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord};
use aruna_core::types::{Effects, GroupId, TxnId};
use aruna_core::{TopicId, events::SubOperationEvent};
use chrono::Utc;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::announce::AnnounceTopicOperation;
use crate::metadata::repository::{
    StorageReadError, parse_registry_read, read_registry_effect, write_audit_effect,
    write_document_index_effect, write_registry_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateMetadataDocumentConfig {
    pub actor: aruna_core::structs::Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub public: bool,
    pub mutation: UpdateMetadataDocumentMutation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateMetadataDocumentMutation {
    ReplaceRoCrate { jsonld: String },
    UpsertDataEntity { jsonld: String },
    UpsertContextualEntity { jsonld: String },
}

#[derive(Debug, PartialEq)]
pub struct UpdateMetadataDocumentOperation {
    config: UpdateMetadataDocumentConfig,
    txn_id: Option<TxnId>,
    record: Option<MetadataRegistryRecord>,
    batch: Option<MetadataBatch>,
    state: UpdateMetadataDocumentState,
    output: Option<Result<MetadataRegistryRecord, UpdateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateMetadataDocumentState {
    Init,
    ReadCurrent,
    ApplyMutation,
    StartTransaction,
    WriteRegistry,
    WriteDocumentIndex,
    WriteAudit,
    CommitTransaction,
    ReplicateGraph,
    AnnounceTopic,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateMetadataDocumentError {
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] aruna_core::errors::ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document not found")]
    DocumentNotFound,
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

impl UpdateMetadataDocumentOperation {
    pub fn new(config: UpdateMetadataDocumentConfig) -> Self {
        Self {
            config,
            txn_id: None,
            record: None,
            batch: None,
            state: UpdateMetadataDocumentState::Init,
            output: None,
        }
    }

    fn current_timestamp_ms() -> u64 {
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
    }

    fn graph_policy(&self, record: &MetadataRegistryRecord) -> MetadataGraphPolicy {
        MetadataGraphPolicy {
            public: self.config.public,
            permission_paths: vec![record.permission_path.clone()],
        }
        .normalized()
    }

    fn updated_record(&self, mut record: MetadataRegistryRecord) -> MetadataRegistryRecord {
        record.public = self.config.public;
        record.updated_at_ms = Self::current_timestamp_ms();
        record
    }

    fn audit_record(&self, record: &MetadataRegistryRecord) -> MetadataAuditRecord {
        let (operation, details) = match &self.config.mutation {
            UpdateMetadataDocumentMutation::ReplaceRoCrate { .. } => (
                MetadataAuditOperation::ReplaceRoCrate,
                "replace ro-crate".to_string(),
            ),
            UpdateMetadataDocumentMutation::UpsertDataEntity { .. } => (
                MetadataAuditOperation::UpsertDataEntity,
                "upsert data entity".to_string(),
            ),
            UpdateMetadataDocumentMutation::UpsertContextualEntity { .. } => (
                MetadataAuditOperation::UpsertContextualEntity,
                "upsert contextual entity".to_string(),
            ),
        };
        MetadataAuditRecord {
            realm_id: record.realm_id.clone(),
            group_id: record.group_id,
            document_id: record.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            operation,
            occurred_at_ms: record.updated_at_ms,
            details: Some(details),
        }
    }

    fn mutation_effect(&self, record: &MetadataRegistryRecord) -> Effect {
        let graph_iri = record.graph_iri.clone();
        match &self.config.mutation {
            UpdateMetadataDocumentMutation::ReplaceRoCrate { jsonld } => {
                Effect::Metadata(MetadataEffect::ApplyRoCrate {
                    request: MetadataApplyRoCrateRequest {
                        graph_iri,
                        jsonld: jsonld.clone(),
                        policy: self.graph_policy(record),
                    },
                })
            }
            UpdateMetadataDocumentMutation::UpsertDataEntity { jsonld } => {
                Effect::Metadata(MetadataEffect::UpsertDataEntity {
                    request: MetadataUpsertEntityRequest {
                        graph_iri,
                        jsonld: jsonld.clone(),
                    },
                })
            }
            UpdateMetadataDocumentMutation::UpsertContextualEntity { jsonld } => {
                Effect::Metadata(MetadataEffect::UpsertContextualEntity {
                    request: MetadataUpsertEntityRequest {
                        graph_iri,
                        jsonld: jsonld.clone(),
                    },
                })
            }
        }
    }

    fn fail(&mut self, error: UpdateMetadataDocumentError) -> Effects {
        let cleanup = self.abort();
        self.state = UpdateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(UpdateMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for UpdateMetadataDocumentOperation {
    type Output = MetadataRegistryRecord;
    type Error = UpdateMetadataDocumentError;

    fn start(&mut self) -> Effects {
        self.state = UpdateMetadataDocumentState::ReadCurrent;
        smallvec![read_registry_effect(
            self.config.group_id,
            self.config.document_id,
            None
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            UpdateMetadataDocumentState::ReadCurrent => match parse_registry_read(event) {
                Ok(Some(record)) => {
                    self.record = Some(record);
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(UpdateMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = UpdateMetadataDocumentState::ApplyMutation;
                    smallvec![self.mutation_effect(record)]
                }
                Ok(None) => self.fail(UpdateMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            UpdateMetadataDocumentState::ApplyMutation => match event {
                Event::Metadata(MetadataEvent::ApplyRoCrateResult { batch, .. })
                | Event::Metadata(MetadataEvent::EntityUpsertResult { batch, .. }) => {
                    let Some(record) = self.record.take() else {
                        return self.fail(UpdateMetadataDocumentError::DocumentNotFound);
                    };
                    self.record = Some(self.updated_record(record));
                    self.batch = Some(batch);
                    self.state = UpdateMetadataDocumentState::StartTransaction;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => self.fail(error.into()),
                other => self.unexpected_event("metadata mutation result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::WriteRegistry;
                    match write_registry_effect(record, Some(txn_id)) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(UpdateMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::WriteRegistry => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::WriteDocumentIndex;
                    match write_document_index_effect(record, Some(txn_id)) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(UpdateMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("registry write result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::WriteDocumentIndex => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::WriteAudit;
                    match write_audit_effect(&self.audit_record(record), Ulid::new(), Some(txn_id))
                    {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(UpdateMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("document index write result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::WriteAudit => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::CommitTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("audit write result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    let Some(record) = self.record.clone() else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    let Some(batch) = self.batch.take() else {
                        return self.fail(UpdateMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = UpdateMetadataDocumentState::ReplicateGraph;
                    smallvec![Effect::Metadata(MetadataEffect::ReplicateBatch {
                        record,
                        batch,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::ReplicateGraph => match event {
                Event::Metadata(MetadataEvent::BatchReplicated { .. }) => {
                    let Some(record) = self.record.clone() else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::AnnounceTopic;
                    smallvec![Effect::SubOperation(boxed_suboperation(
                        AnnounceTopicOperation::new(
                            TopicId::metadata(record.document_id),
                            self.config.actor.node_id,
                        ),
                        |result| {
                            Event::SubOperation(SubOperationEvent::TopicAnnouncementResult {
                                result: result.map_err(|error| error.to_string()),
                            })
                        },
                    ))]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => self.fail(error.into()),
                other => self.unexpected_event("metadata replication result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::AnnounceTopic => match event {
                Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) => {
                    match result {
                        Ok(()) => {
                            let Some(record) = self.record.clone() else {
                                return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                            };
                            self.state = UpdateMetadataDocumentState::Finish;
                            self.output = Some(Ok(record));
                            smallvec![]
                        }
                        Err(error) => {
                            self.fail(UpdateMetadataDocumentError::TopicAnnouncement(error))
                        }
                    }
                }
                other => self.unexpected_event("topic announcement result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::Finish
            | UpdateMetadataDocumentState::Error
            | UpdateMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            UpdateMetadataDocumentState::Finish | UpdateMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("metadata update operation must set output")
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}
