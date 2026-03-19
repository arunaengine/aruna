use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::MetadataDocument;
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::read_effect;

#[derive(Debug, PartialEq)]
pub struct GetMetadataDocumentOperation {
    document: AutomergeDocumentVariant,
    state: GetMetadataDocumentState,
    output: Option<Result<MetadataDocument, GetMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetMetadataDocumentState {
    Init,
    ReadDocument,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetMetadataDocumentError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl GetMetadataDocumentOperation {
    pub fn new(group_id: aruna_core::types::GroupId, document_id: ulid::Ulid) -> Self {
        Self {
            document: AutomergeDocumentVariant::Metadata {
                group_id,
                document_id,
            },
            state: GetMetadataDocumentState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetMetadataDocumentError) -> aruna_core::types::Effects {
        self.state = GetMetadataDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for GetMetadataDocumentOperation {
    type Output = MetadataDocument;
    type Error = GetMetadataDocumentError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = GetMetadataDocumentState::ReadDocument;
        smallvec![read_effect(&self.document, None)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            GetMetadataDocumentState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(GetMetadataDocumentError::DocumentNotFound);
                    };
                    match MetadataDocument::from_bytes(&value) {
                        Ok(document) => {
                            self.state = GetMetadataDocumentState::Finish;
                            self.output = Some(Ok(document));
                            smallvec![]
                        }
                        Err(error) => self.fail(error.into()),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            GetMetadataDocumentState::Finish
            | GetMetadataDocumentState::Error
            | GetMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetMetadataDocumentState::Finish | GetMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.expect("metadata get operation must set output")
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
