use aruna_core::IrokleEffect;
use aruna_core::document::{DocumentSyncTarget, IrokleEvent};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::repository::{
    StorageReadError, delete_document_index_effect, delete_holders_effect, delete_registry_effect,
    parse_registry_read, read_registry_effect, write_audit_effect,
};

#[derive(Debug, PartialEq)]
pub struct DeleteMetadataDocumentOperation {
    actor: aruna_core::structs::Actor,
    group_id: Ulid,
    document_id: Ulid,
    record: Option<MetadataRegistryRecord>,
    txn_id: Option<Ulid>,
    state: DeleteMetadataDocumentState,
    output: Option<Result<(), DeleteMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum DeleteMetadataDocumentState {
    Init,
    ReadRecord,
    DeleteGraph,
    StartTransaction,
    DeleteRegistry,
    DeleteDocumentIndex,
    DeleteHolders,
    WriteAudit,
    CommitTransaction,
    SyncDelete,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteMetadataDocumentError {
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
    #[error("document delete sync failed: {0}")]
    SyncDelete(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl DeleteMetadataDocumentOperation {
    pub fn new(actor: aruna_core::structs::Actor, group_id: Ulid, document_id: Ulid) -> Self {
        Self {
            actor,
            group_id,
            document_id,
            record: None,
            txn_id: None,
            state: DeleteMetadataDocumentState::Init,
            output: None,
        }
    }

    fn audit_record(&self, record: &MetadataRegistryRecord) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id: record.realm_id,
            group_id: record.group_id,
            document_id: record.document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: self.actor.user_id,
            node_id: self.actor.node_id,
            operation: MetadataAuditOperation::Delete,
            occurred_at_ms: u64::try_from(chrono::Utc::now().timestamp_millis())
                .unwrap_or_default(),
            details: Some("delete metadata graph".to_string()),
        }
    }

    fn fail(&mut self, error: DeleteMetadataDocumentError) -> Effects {
        let cleanup = self.abort();
        self.state = DeleteMetadataDocumentState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(DeleteMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for DeleteMetadataDocumentOperation {
    type Output = ();
    type Error = DeleteMetadataDocumentError;

    fn start(&mut self) -> Effects {
        self.state = DeleteMetadataDocumentState::ReadRecord;
        smallvec![read_registry_effect(self.group_id, self.document_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteMetadataDocumentState::ReadRecord => match parse_registry_read(event) {
                Ok(Some(record)) => {
                    let graph_iri = record.graph_iri.clone();
                    self.record = Some(record);
                    self.state = DeleteMetadataDocumentState::DeleteGraph;
                    smallvec![Effect::Metadata(MetadataEffect::DeleteGraph { graph_iri })]
                }
                Ok(None) => self.fail(DeleteMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            DeleteMetadataDocumentState::DeleteGraph => match event {
                Event::Metadata(MetadataEvent::GraphDeleted { .. }) => {
                    self.state = DeleteMetadataDocumentState::StartTransaction;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => self.fail(error.into()),
                other => self.unexpected_event("metadata delete result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.state = DeleteMetadataDocumentState::DeleteRegistry;
                    smallvec![delete_registry_effect(
                        self.group_id,
                        self.document_id,
                        Some(txn_id)
                    )]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::DeleteRegistry => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    self.state = DeleteMetadataDocumentState::DeleteDocumentIndex;
                    smallvec![delete_document_index_effect(self.document_id, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("registry delete result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::DeleteDocumentIndex => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    self.state = DeleteMetadataDocumentState::DeleteHolders;
                    smallvec![delete_holders_effect(
                        self.group_id,
                        self.document_id,
                        Some(txn_id)
                    )]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("document index delete result", format!("{other:?}"))
                }
            },
            DeleteMetadataDocumentState::DeleteHolders => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    let Some(record) = self.record.as_ref() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::WriteAudit;
                    match write_audit_effect(&self.audit_record(record), Ulid::new(), Some(txn_id))
                    {
                        Ok(effect) => smallvec![effect],
                        Err(error) => {
                            self.fail(DeleteMetadataDocumentError::ConversionError(error))
                        }
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("holders delete result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::WriteAudit => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(DeleteMetadataDocumentError::MissingTransaction);
                    };
                    self.state = DeleteMetadataDocumentState::CommitTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("audit write result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    let Some(record) = self.record.clone() else {
                        return self.fail(DeleteMetadataDocumentError::DocumentNotFound);
                    };
                    self.state = DeleteMetadataDocumentState::SyncDelete;
                    smallvec![Effect::Net(NetEffect::Irokle(
                        IrokleEffect::DeleteDocument {
                            target: DocumentSyncTarget::MetadataRegistry {
                                group_id: record.group_id,
                                document_id: record.document_id,
                            },
                            peers: record.holder_node_ids,
                        }
                    ))]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::SyncDelete => match event {
                Event::Net(NetEvent::Irokle(IrokleEvent::DocumentDeleted { .. })) => {
                    self.state = DeleteMetadataDocumentState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                Event::Net(NetEvent::Irokle(IrokleEvent::Error { error, .. })) => {
                    self.fail(DeleteMetadataDocumentError::SyncDelete(error))
                }
                Event::Net(NetEvent::Error(error)) => self.fail(
                    DeleteMetadataDocumentError::SyncDelete(format!("{error:?}")),
                ),
                other => self.unexpected_event("document delete sync result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::Finish
            | DeleteMetadataDocumentState::Error
            | DeleteMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteMetadataDocumentState::Finish | DeleteMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}
