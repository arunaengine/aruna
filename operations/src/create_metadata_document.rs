use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, MetadataDocument};
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::write_effect;
use crate::automerge_announce::AnnounceAutomergeDocumentOperation;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMetadataDocumentConfig {
    pub actor: Actor,
    pub document: MetadataDocument,
}

#[derive(Debug, PartialEq)]
pub struct CreateMetadataDocumentOperation {
    config: CreateMetadataDocumentConfig,
    txn_id: Option<aruna_core::types::TxnId>,
    state: CreateMetadataDocumentState,
    output: Option<Result<MetadataDocument, CreateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CreateMetadataDocumentState {
    Init,
    StartTransaction,
    CheckExisting,
    WriteDocument,
    CommitTransaction,
    Announce,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateMetadataDocumentError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("document already exists")]
    DocumentAlreadyExists,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("automerge announcement failed: {0}")]
    AutomergeState(String),
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
            output: None,
        }
    }

    fn document_ref(&self) -> AutomergeDocumentVariant {
        AutomergeDocumentVariant::Metadata {
            group_id: self.config.document.group_id,
            document_id: self.config.document.document_id,
        }
    }

    fn fail(&mut self, error: CreateMetadataDocumentError) -> aruna_core::types::Effects {
        let cleanup = self.abort();
        self.state = CreateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(CreateMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for CreateMetadataDocumentOperation {
    type Output = MetadataDocument;
    type Error = CreateMetadataDocumentError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = CreateMetadataDocumentState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            CreateMetadataDocumentState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.state = CreateMetadataDocumentState::CheckExisting;
                    smallvec![crate::automerge::repository::read_effect(
                        &self.document_ref(),
                        Some(txn_id),
                    )]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CheckExisting => match event {
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {
                    self.fail(CreateMetadataDocumentError::DocumentAlreadyExists)
                }
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    let bytes = match self
                        .config
                        .document
                        .reconcile_bytes(None, &self.config.actor)
                    {
                        Ok(bytes) => bytes,
                        Err(error) => return self.fail(error.into()),
                    };
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::WriteDocument;
                    smallvec![write_effect(&self.document_ref(), bytes, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::WriteDocument => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::CommitTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = CreateMetadataDocumentState::Announce;
                    smallvec![Effect::SubOperation(boxed_suboperation(
                        AnnounceAutomergeDocumentOperation::new(self.document_ref()),
                        |result| {
                            Event::SubOperation(SubOperationEvent::AutomergeStateResult {
                                result: result.map_err(|error| error.to_string()),
                            })
                        },
                    ))]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::Announce => match event {
                Event::SubOperation(SubOperationEvent::AutomergeStateResult { result }) => {
                    match result {
                        Ok(()) => {
                            self.state = CreateMetadataDocumentState::Finish;
                            self.output = Some(Ok(self.config.document.clone()));
                            smallvec![]
                        }
                        Err(error) => self.fail(CreateMetadataDocumentError::AutomergeState(error)),
                    }
                }
                other => {
                    self.unexpected_event("automerge announcement result", format!("{other:?}"))
                }
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
        self.output.unwrap_or(Ok(self.config.document))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}
