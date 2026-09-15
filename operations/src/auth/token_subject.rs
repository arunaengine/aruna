use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::user::User;
use aruna_core::types::Effects;
use aruna_core::{USER_KEYSPACE, SUBJECT_INDEX_KEYSPACE};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct SubjectCheckOperation {
    user_id: UserId,
    subject_ids: Vec<String>,
    subject_index: usize,
    state: SubjectCheckState,
    output: Option<Result<(), SubjectCheckError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SubjectCheckState {
    Init,
    ReadUser,
    ReadSubjectIndex,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SubjectCheckError {
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

impl SubjectCheckOperation {
    pub fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            subject_ids: Vec::new(),
            subject_index: 0,
            state: SubjectCheckState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: SubjectCheckError) -> Effects {
        self.state = SubjectCheckState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn finish(&mut self) -> Effects {
        self.state = SubjectCheckState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: Event) -> Effects {
        self.fail(SubjectCheckError::UnexpectedEvent {
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

    fn read_subject_effect(&self) -> Result<Effect, SubjectCheckError> {
        let subject_id = self
            .subject_ids
            .get(self.subject_index)
            .ok_or(SubjectCheckError::Forbidden)?;
        Ok(Effect::Storage(StorageEffect::Read {
            key_space: SUBJECT_INDEX_KEYSPACE.to_string(),
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
            return self.fail(SubjectCheckError::Unauthorized);
        };
        let user = match User::from_bytes(&bytes) {
            Ok(user) => user,
            Err(error) => return self.fail(error.into()),
        };
        if user.user_id != self.user_id {
            return self.fail(SubjectCheckError::Unauthorized);
        }

        self.subject_ids = user.subject_ids;
        self.subject_index = 0;
        // Nothing is claimed, so no index entry can resolve this record to a
        // different canonical owner; the record's own id already matched.
        if self.subject_ids.is_empty() {
            return self.finish();
        }

        self.state = SubjectCheckState::ReadSubjectIndex;
        match self.read_subject_effect() {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error),
        }
    }

    fn handle_subject_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return match event {
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", other),
            };
        };
        let Some(bytes) = value else {
            return self.fail(SubjectCheckError::Forbidden);
        };
        let indexed_user_id = match UserId::from_storage_key(&bytes) {
            Ok(user_id) => user_id,
            Err(error) => return self.fail(error.into()),
        };
        // The index resolves this subject to another user in the realm, so this
        // record is a merged alias or stale: issuing a token here would restore
        // a non-canonical identity for a subject that already has an owner.
        if indexed_user_id != self.user_id {
            return self.fail(SubjectCheckError::Forbidden);
        }

        self.subject_index += 1;
        if self.subject_index >= self.subject_ids.len() {
            return self.finish();
        }

        match self.read_subject_effect() {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error),
        }
    }
}

impl Operation for SubjectCheckOperation {
    type Output = ();
    type Error = SubjectCheckError;

    fn start(&mut self) -> Effects {
        self.state = SubjectCheckState::ReadUser;
        smallvec![self.read_user_effect()]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            SubjectCheckState::ReadUser => self.handle_user_read(event),
            SubjectCheckState::ReadSubjectIndex => self.handle_subject_read(event),
            SubjectCheckState::Init | SubjectCheckState::Finish | SubjectCheckState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SubjectCheckState::Finish | SubjectCheckState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SubjectCheckError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
