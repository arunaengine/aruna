use aruna_core::document::DocumentSyncTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{RealmAuthorizationDocument, RealmId};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::document_repository::read_effect;

#[derive(Debug, PartialEq)]
pub struct ReadRealmAuthorizationOperation {
    realm_id: RealmId,
    state: ReadRealmAuthorizationState,
    output: Option<Result<Option<RealmAuthorizationDocument>, ReadRealmAuthorizationError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReadRealmAuthorizationState {
    Init,
    ReadAuthorization,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadRealmAuthorizationError {
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
    #[error("read realm authorization did not finish")]
    NotFinished,
}

impl ReadRealmAuthorizationOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            state: ReadRealmAuthorizationState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ReadRealmAuthorizationError) -> Effects {
        self.state = ReadRealmAuthorizationState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_authorization_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                let result = value
                    .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes).map_err(Into::into))
                    .transpose();
                self.state = ReadRealmAuthorizationState::Finish;
                self.output = Some(result);
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.fail(ReadRealmAuthorizationError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "storage read result",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Operation for ReadRealmAuthorizationOperation {
    type Output = Option<RealmAuthorizationDocument>;
    type Error = ReadRealmAuthorizationError;

    fn start(&mut self) -> Effects {
        self.state = ReadRealmAuthorizationState::ReadAuthorization;
        smallvec![read_effect(
            &DocumentSyncTarget::RealmAuthorization {
                realm_id: self.realm_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReadRealmAuthorizationState::ReadAuthorization => self.handle_authorization_read(event),
            ReadRealmAuthorizationState::Init
            | ReadRealmAuthorizationState::Finish
            | ReadRealmAuthorizationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReadRealmAuthorizationState::Finish | ReadRealmAuthorizationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(ReadRealmAuthorizationError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
