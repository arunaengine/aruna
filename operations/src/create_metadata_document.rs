use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{boxed_suboperation, Operation};
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
    state: CreateMetadataDocumentState,
    output: Option<Result<MetadataDocument, CreateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CreateMetadataDocumentState {
    Init,
    WriteDocument,
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
        self.state = CreateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
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
        let bytes = match self
            .config
            .document
            .reconcile_bytes(None, &self.config.actor)
        {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        self.state = CreateMetadataDocumentState::WriteDocument;
        smallvec![write_effect(&self.document_ref(), bytes, None)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            CreateMetadataDocumentState::WriteDocument => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
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
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::Announce => match event {
                Event::SubOperation(SubOperationEvent::AutomergeStateResult { .. }) => {
                    self.state = CreateMetadataDocumentState::Finish;
                    self.output = Some(Ok(self.config.document.clone()));
                    smallvec![]
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
        smallvec![]
    }
}
