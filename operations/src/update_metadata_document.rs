use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::{
    MetadataApplyRoCrateRequest, MetadataEffect, MetadataError, MetadataEvent, MetadataGraphPolicy,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord};
use aruna_core::types::{Effects, GroupId, TxnId};
use chrono::Utc;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::repository::{
    parse_registry_read, read_registry_effect, write_audit_effect, write_registry_effect,
    StorageReadError,
};

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateMetadataDocumentConfig {
    pub actor: aruna_core::structs::Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub jsonld: String,
    pub public: bool,
}

#[derive(Debug, PartialEq)]
pub struct UpdateMetadataDocumentOperation {
    config: UpdateMetadataDocumentConfig,
    txn_id: Option<TxnId>,
    record: Option<MetadataRegistryRecord>,
    state: UpdateMetadataDocumentState,
    output: Option<Result<MetadataRegistryRecord, UpdateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateMetadataDocumentState {
    Init,
    ReadCurrent,
    ApplyRoCrate,
    StartTransaction,
    WriteRegistry,
    WriteAudit,
    CommitTransaction,
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
        MetadataAuditRecord {
            realm_id: record.realm_id.clone(),
            group_id: record.group_id,
            document_id: record.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            operation: MetadataAuditOperation::ReplaceRoCrate,
            occurred_at_ms: record.updated_at_ms,
            details: Some("replace ro-crate".to_string()),
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
                    let graph_iri = record.graph_iri.clone();
                    let policy = self.graph_policy(&record);
                    self.record = Some(record);
                    self.state = UpdateMetadataDocumentState::ApplyRoCrate;
                    smallvec![Effect::Metadata(MetadataEffect::ApplyRoCrate {
                        request: MetadataApplyRoCrateRequest {
                            graph_iri,
                            jsonld: self.config.jsonld.clone(),
                            policy,
                        },
                    })]
                }
                Ok(None) => self.fail(UpdateMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            UpdateMetadataDocumentState::ApplyRoCrate => match event {
                Event::Metadata(MetadataEvent::ApplyRoCrateResult { .. }) => {
                    let Some(record) = self.record.take() else {
                        return self.fail(UpdateMetadataDocumentError::DocumentNotFound);
                    };
                    self.record = Some(self.updated_record(record));
                    self.state = UpdateMetadataDocumentState::StartTransaction;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => self.fail(error.into()),
                other => self.unexpected_event("metadata apply result", format!("{other:?}")),
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
                other => self.unexpected_event("registry write result", format!("{other:?}")),
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
                    self.state = UpdateMetadataDocumentState::Finish;
                    self.output = Some(Ok(record));
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
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
