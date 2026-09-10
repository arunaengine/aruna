use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{User, oidc_subject_key};
use aruna_core::types::{Effects, TxnId, UserId};
use aruna_core::{USER_KEYSPACE, USER_SUBJECT_INDEX_KEYSPACE};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct GetOidcUserInput {
    pub issuer: String,
    pub subject_id: String,
}

#[derive(Debug, PartialEq)]
pub struct GetOidcUserOperation {
    input: GetOidcUserInput,
    state: GetOidcUserState,
    output: Option<Result<User, GetOidcUserError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetOidcUserState {
    Init,
    StartTransaction,
    ReadSubjectIndex { txn_id: TxnId },
    ReadExistingUser { txn_id: TxnId },
    CommitTransaction { user: User },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetOidcUserError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("registration did not finish")]
    NotFinished,
    #[error("User not found")]
    UserNotFound,
}

impl GetOidcUserOperation {
    pub fn new(input: GetOidcUserInput) -> Self {
        Self {
            input,
            state: GetOidcUserState::Init,
            output: None,
        }
    }

    fn subject_key(&self) -> Result<String, GetOidcUserError> {
        Ok(oidc_subject_key(
            &self.input.issuer,
            &self.input.subject_id,
        )?)
    }

    fn fail(&mut self, error: GetOidcUserError) -> Effects {
        let cleanup = self.abort();
        self.state = GetOidcUserState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(GetOidcUserError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn handle_start_txn(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::TransactionStarted { txn_id })",
                got,
            );
        };

        match self.emit_read_subject_index(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_read_subject_index(&mut self, txn_id: TxnId) -> Result<Effects, GetOidcUserError> {
        self.state = GetOidcUserState::ReadSubjectIndex { txn_id };
        let key = ByteView::from(self.subject_key()?.into_bytes());
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_read_subject_idx(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::ReadResult { key, value })",
                got,
            );
        };

        match self.emit_read_existing_user(txn_id, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_read_existing_user(
        &mut self,
        txn_id: TxnId,
        value: Option<ByteView>,
    ) -> Result<Effects, GetOidcUserError> {
        let key = value.ok_or_else(|| GetOidcUserError::UserNotFound)?;
        let user_id = UserId::from_storage_key(&key)?;
        self.state = GetOidcUserState::ReadExistingUser { txn_id };
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(user_id.to_storage_key()),
            txn_id: Some(txn_id),
        })])
    }

    fn handle_read_existing_user(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::ReadResult { key, value })",
                got,
            );
        };

        match self.emit_commit_txn(txn_id, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_commit_txn(
        &mut self,
        txn_id: TxnId,
        value: Option<ByteView>,
    ) -> Result<Effects, GetOidcUserError> {
        let user = User::from_bytes(&value.ok_or_else(|| GetOidcUserError::UserNotFound)?)?;
        self.state = GetOidcUserState::CommitTransaction { user };
        Ok(smallvec![Effect::Storage(
            StorageEffect::CommitTransaction { txn_id }
        )])
    }

    fn handle_commit_transaction(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self
                .unexpected_event("Event::Storage(StorageEvent::TransactionCommitted)", got);
        };
        self.state = GetOidcUserState::Finish;
        self.output = Some(Ok(user));
        smallvec![]
    }
}

impl Operation for GetOidcUserOperation {
    type Output = User;
    type Error = GetOidcUserError;

    fn start(&mut self) -> Effects {
        self.state = GetOidcUserState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };
        match self.state.clone() {
            GetOidcUserState::StartTransaction => self.handle_start_txn(event),
            GetOidcUserState::ReadSubjectIndex { txn_id } => {
                self.handle_read_subject_idx(event, txn_id)
            }
            GetOidcUserState::ReadExistingUser { txn_id } => {
                self.handle_read_existing_user(event, txn_id)
            }
            GetOidcUserState::CommitTransaction { user } => {
                self.handle_commit_transaction(event, user)
            }
            GetOidcUserState::Init | GetOidcUserState::Finish | GetOidcUserState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetOidcUserState::Finish | GetOidcUserState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(GetOidcUserError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            GetOidcUserState::ReadSubjectIndex { txn_id }
            | GetOidcUserState::ReadExistingUser { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}
