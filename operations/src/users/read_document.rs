use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::user::User;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::document_repository::read_effect;

#[derive(Debug, PartialEq)]
pub struct ReadUserOperation {
    user_id: UserId,
    state: ReadUserState,
    output: Option<Result<User, ReadUserError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReadUserState {
    Init,
    ReadUser,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadUserError {
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

impl ReadUserOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            state: ReadUserState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ReadUserError) -> Effects {
        self.state = ReadUserState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_user_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                let result = value
                    .ok_or(ReadUserError::NotFound)
                    .and_then(|bytes| User::from_bytes(&bytes).map_err(Into::into));
                self.state = ReadUserState::Finish;
                self.output = Some(result);
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.fail(ReadUserError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "storage read result",
                got: format!("{other:?}"),
            }),
        }
    }
}

impl Operation for ReadUserOperation {
    type Output = User;
    type Error = ReadUserError;

    fn start(&mut self) -> Effects {
        self.state = ReadUserState::ReadUser;
        smallvec![read_effect(
            &DocumentTarget::User {
                user_id: self.user_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReadUserState::ReadUser => self.handle_user_read(event),
            ReadUserState::Init | ReadUserState::Finish | ReadUserState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadUserState::Finish | ReadUserState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ReadUserError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
