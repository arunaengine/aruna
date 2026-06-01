use aruna_core::document::DocumentSyncTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{RealmConfigDocument, RealmId};
use smallvec::smallvec;
use thiserror::Error;

use crate::document_repository::read_effect;

#[derive(Debug, PartialEq)]
pub struct GetRealmConfigOperation {
    document: DocumentSyncTarget,
    state: GetRealmConfigState,
    output: Option<Result<RealmConfigDocument, GetRealmConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetRealmConfigState {
    Init,
    ReadDocument,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetRealmConfigError {
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

impl GetRealmConfigOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            document: DocumentSyncTarget::RealmConfig { realm_id },
            state: GetRealmConfigState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetRealmConfigError) -> aruna_core::types::Effects {
        self.state = GetRealmConfigState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetRealmConfigError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for GetRealmConfigOperation {
    type Output = RealmConfigDocument;
    type Error = GetRealmConfigError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = GetRealmConfigState::ReadDocument;
        smallvec![read_effect(&self.document, None)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            GetRealmConfigState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(GetRealmConfigError::DocumentNotFound);
                    };
                    match RealmConfigDocument::from_bytes(&value) {
                        Ok(document) => {
                            self.state = GetRealmConfigState::Finish;
                            self.output = Some(Ok(document));
                            smallvec![]
                        }
                        Err(error) => self.fail(error.into()),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            GetRealmConfigState::Finish
            | GetRealmConfigState::Error
            | GetRealmConfigState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetRealmConfigState::Finish | GetRealmConfigState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("realm config get operation must set output")
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
