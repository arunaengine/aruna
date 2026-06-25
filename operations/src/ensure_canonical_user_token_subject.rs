use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::User;
use aruna_core::types::{Effects, UserId};
use aruna_core::{USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct EnsureCanonicalUserTokenSubjectOperation {
    user_id: UserId,
    subject_ids: Vec<String>,
    subject_index: usize,
    state: EnsureCanonicalUserTokenSubjectState,
    output: Option<Result<(), EnsureCanonicalUserTokenSubjectError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum EnsureCanonicalUserTokenSubjectState {
    Init,
    ReadUser,
    ReadSubjectIndex,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum EnsureCanonicalUserTokenSubjectError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Forbidden")]
    Forbidden,
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
    #[error("canonical user token subject check did not finish")]
    NotFinished,
}

impl EnsureCanonicalUserTokenSubjectOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            subject_ids: Vec::new(),
            subject_index: 0,
            state: EnsureCanonicalUserTokenSubjectState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: EnsureCanonicalUserTokenSubjectError) -> Effects {
        self.state = EnsureCanonicalUserTokenSubjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn finish(&mut self) -> Effects {
        self.state = EnsureCanonicalUserTokenSubjectState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: Event) -> Effects {
        self.fail(EnsureCanonicalUserTokenSubjectError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{got:?}"),
        })
    }

    fn read_user_effect(&self) -> Effect {
        Effect::Storage(StorageEffect::Read {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(self.user_id.to_bytes()),
            txn_id: None,
        })
    }

    fn read_subject_index_effect(&self) -> Result<Effect, EnsureCanonicalUserTokenSubjectError> {
        let subject_id = self
            .subject_ids
            .get(self.subject_index)
            .ok_or(EnsureCanonicalUserTokenSubjectError::Forbidden)?;
        Ok(Effect::Storage(StorageEffect::Read {
            key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            key: ByteView::from(subject_id.as_bytes().to_vec()),
            txn_id: None,
        }))
    }

    fn handle_user_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return match event {
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", other),
            };
        };
        let Some(bytes) = value else {
            return self.fail(EnsureCanonicalUserTokenSubjectError::Unauthorized);
        };
        let user = match User::from_bytes(&bytes) {
            Ok(user) => user,
            Err(error) => return self.fail(error.into()),
        };
        if user.user_id != self.user_id {
            return self.fail(EnsureCanonicalUserTokenSubjectError::Unauthorized);
        }

        self.subject_ids = user.subject_ids;
        self.subject_index = 0;
        if self.subject_ids.is_empty() {
            return self.finish();
        }

        self.state = EnsureCanonicalUserTokenSubjectState::ReadSubjectIndex;
        match self.read_subject_index_effect() {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error),
        }
    }

    fn handle_subject_index_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return match event {
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", other),
            };
        };
        let Some(bytes) = value else {
            return self.fail(EnsureCanonicalUserTokenSubjectError::Forbidden);
        };
        let indexed_user_id = match UserId::from_storage_key(&bytes) {
            Ok(user_id) => user_id,
            Err(error) => return self.fail(error.into()),
        };
        if indexed_user_id != self.user_id {
            return self.fail(EnsureCanonicalUserTokenSubjectError::Forbidden);
        }

        self.subject_index += 1;
        if self.subject_index >= self.subject_ids.len() {
            return self.finish();
        }

        match self.read_subject_index_effect() {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error),
        }
    }
}

impl Operation for EnsureCanonicalUserTokenSubjectOperation {
    type Output = ();
    type Error = EnsureCanonicalUserTokenSubjectError;

    fn start(&mut self) -> Effects {
        self.state = EnsureCanonicalUserTokenSubjectState::ReadUser;
        smallvec![self.read_user_effect()]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            EnsureCanonicalUserTokenSubjectState::ReadUser => self.handle_user_read(event),
            EnsureCanonicalUserTokenSubjectState::ReadSubjectIndex => {
                self.handle_subject_index_read(event)
            }
            EnsureCanonicalUserTokenSubjectState::Init
            | EnsureCanonicalUserTokenSubjectState::Finish
            | EnsureCanonicalUserTokenSubjectState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            EnsureCanonicalUserTokenSubjectState::Finish
                | EnsureCanonicalUserTokenSubjectState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or(EnsureCanonicalUserTokenSubjectError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
