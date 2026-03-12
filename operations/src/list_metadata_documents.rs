use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::MetadataDocument;
use aruna_core::types::GroupId;
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::iter_metadata_effect;

#[derive(Debug, PartialEq)]
pub struct ListMetadataDocumentsOperation {
    group_id: GroupId,
    state: ListMetadataDocumentsState,
    output: Option<Result<Vec<MetadataDocument>, ListMetadataDocumentsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ListMetadataDocumentsState {
    Init,
    ListDocuments,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListMetadataDocumentsError {
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

impl ListMetadataDocumentsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            state: ListMetadataDocumentsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ListMetadataDocumentsError) -> aruna_core::types::Effects {
        self.state = ListMetadataDocumentsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(ListMetadataDocumentsError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for ListMetadataDocumentsOperation {
    type Output = Vec<MetadataDocument>;
    type Error = ListMetadataDocumentsError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = ListMetadataDocumentsState::ListDocuments;
        smallvec![iter_metadata_effect(self.group_id, None, None, usize::MAX)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            ListMetadataDocumentsState::ListDocuments => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    let documents = values
                        .into_iter()
                        .map(|(_, value)| {
                            MetadataDocument::from_bytes(&value)
                                .map_err(ListMetadataDocumentsError::from)
                        })
                        .collect();
                    self.state = ListMetadataDocumentsState::Finish;
                    self.output = Some(documents);
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iteration result", format!("{other:?}")),
            },
            ListMetadataDocumentsState::Finish
            | ListMetadataDocumentsState::Error
            | ListMetadataDocumentsState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListMetadataDocumentsState::Finish | ListMetadataDocumentsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(Vec::new()))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
