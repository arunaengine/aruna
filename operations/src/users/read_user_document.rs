use aruna_core::document::DocumentSyncTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::User;
use aruna_core::types::{Effects, UserId};
use smallvec::smallvec;
use thiserror::Error;

use crate::document_repository::read_effect;

#[derive(Debug, PartialEq)]
pub struct ReadUserDocumentOperation {
    user_id: UserId,
    state: ReadUserDocumentState,
    output: Option<Result<User, ReadUserDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReadUserDocumentState {
    Init,
    ReadUser,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadUserDocumentError {
    #[error("User not found")]
    NotFound,
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
    #[error("read user document did not finish")]
    NotFinished,
}

impl ReadUserDocumentOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            state: ReadUserDocumentState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ReadUserDocumentError) -> Effects {
        self.state = ReadUserDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_user_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                let result = value
                    .ok_or(ReadUserDocumentError::NotFound)
                    .and_then(|bytes| User::from_bytes(&bytes).map_err(Into::into));
                self.state = ReadUserDocumentState::Finish;
                self.output = Some(result);
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.fail(ReadUserDocumentError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "storage read result",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Operation for ReadUserDocumentOperation {
    type Output = User;
    type Error = ReadUserDocumentError;

    fn start(&mut self) -> Effects {
        self.state = ReadUserDocumentState::ReadUser;
        smallvec![read_effect(
            &DocumentSyncTarget::User {
                user_id: self.user_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReadUserDocumentState::ReadUser => self.handle_user_read(event),
            ReadUserDocumentState::Init
            | ReadUserDocumentState::Finish
            | ReadUserDocumentState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReadUserDocumentState::Finish | ReadUserDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ReadUserDocumentError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
