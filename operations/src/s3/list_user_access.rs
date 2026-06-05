use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::UserAccess;
use aruna_core::types::{Effects, Key, UserId};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct ListUserAccessInput {
    pub user_identity: UserId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListUserAccessState {
    Init,
    ReadPage,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListUserAccessError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListUserAccessState,
        expected: &'static str,
        received: Event,
    },
    #[error("ListUserAccess failed")]
    ListUserAccessFailed,
}

#[derive(Debug, PartialEq)]
pub struct ListUserAccessOperation {
    input: ListUserAccessInput,
    credentials: Vec<UserAccess>,
    state: ListUserAccessState,
    output: Option<Result<Vec<UserAccess>, ListUserAccessError>>,
}

impl ListUserAccessOperation {
    const SCAN_LIMIT: usize = 10_000;

    pub fn new(input: ListUserAccessInput) -> Self {
        Self {
            input,
            credentials: Vec::new(),
            state: ListUserAccessState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListUserAccessError) -> Effects {
        self.state = ListUserAccessState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn iter_effect(&self, start_after: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: USER_ACCESS_KEYSPACE.to_string(),
            prefix: None,
            start_after,
            limit: Self::SCAN_LIMIT,
            txn_id: None,
        })
    }

    fn handle_init(&mut self) -> Effects {
        self.state = ListUserAccessState::ReadPage;
        smallvec![self.iter_effect(None)]
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.emit_error(ListUserAccessError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        for (_, value) in values {
            let access = match UserAccess::from_bytes(&value) {
                Ok(access) => access,
                Err(error) => return self.emit_error(error.into()),
            };
            if access.user_identity == self.input.user_identity {
                self.credentials.push(access);
            }
        }

        if let Some(start_after) = next_start_after {
            return smallvec![self.iter_effect(Some(start_after))];
        }

        self.state = ListUserAccessState::Finish;
        self.output = Some(Ok(std::mem::take(&mut self.credentials)));
        smallvec![]
    }
}

impl Operation for ListUserAccessOperation {
    type Output = Vec<UserAccess>;
    type Error = ListUserAccessError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ListUserAccessState::Init => self.handle_init(),
            ListUserAccessState::ReadPage => self.handle_page(event),
            ListUserAccessState::Finish | ListUserAccessState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListUserAccessState::Finish | ListUserAccessState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListUserAccessState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListUserAccessError::ListUserAccessFailed);
        }
        self.output.unwrap_or_else(|| Ok(Vec::new()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
