use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::oidc_subject_key;
use aruna_core::structs::identity::user::User;
use aruna_core::types::{Effects, TxnId};
use aruna_core::{SUBJECT_INDEX_KEYSPACE, USER_KEYSPACE};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct GetOidcInput {
    pub issuer: String,
    pub subject_id: String,
}

#[derive(Debug, PartialEq)]
pub struct GetOidcOperation {
    input: GetOidcInput,
    state: GetOidcState,
    output: Option<Result<User, GetOidcError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetOidcState {
    Init,
    StartTransaction,
    ReadSubjectIndex { txn_id: TxnId },
    ReadExistingUser { txn_id: TxnId },
    CommitTransaction { user: User },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetOidcError {
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

impl GetOidcOperation {
    pub fn new(input: GetOidcInput) -> Self {
        Self {
            input,
            state: GetOidcState::Init,
            output: None,
        }
    }

    fn subject_key(&self) -> Result<String, GetOidcError> {
        Ok(oidc_subject_key(
            &self.input.issuer,
            &self.input.subject_id,
        )?)
    }

    fn fail(&mut self, error: GetOidcError) -> Effects {
        let cleanup = self.abort();
        self.state = GetOidcState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn fail_storage(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(GetOidcError::UnexpectedEvent {
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

        match self.read_subject(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn read_subject(&mut self, txn_id: TxnId) -> Result<Effects, GetOidcError> {
        self.state = GetOidcState::ReadSubjectIndex { txn_id };
        let key = ByteView::from(self.subject_key()?.into_bytes());
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: SUBJECT_INDEX_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn accept_subject(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::ReadResult { key, value })",
                got,
            );
        };

        match self.read_existing(txn_id, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn read_existing(
        &mut self,
        txn_id: TxnId,
        value: Option<ByteView>,
    ) -> Result<Effects, GetOidcError> {
        let key = value.ok_or_else(|| GetOidcError::UserNotFound)?;
        let user_id = UserId::from_storage_key(&key)?;
        self.state = GetOidcState::ReadExistingUser { txn_id };
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(user_id.to_storage_key()),
            txn_id: Some(txn_id),
        })])
    }

    fn accept_existing(&mut self, event: Event, txn_id: TxnId) -> Effects {
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
    ) -> Result<Effects, GetOidcError> {
        let user = User::from_bytes(&value.ok_or_else(|| GetOidcError::UserNotFound)?)?;
        self.state = GetOidcState::CommitTransaction { user };
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
        self.state = GetOidcState::Finish;
        self.output = Some(Ok(user));
        smallvec![]
    }
}

impl Operation for GetOidcOperation {
    type Output = User;
    type Error = GetOidcError;

    fn start(&mut self) -> Effects {
        self.state = GetOidcState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_storage(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };
        match self.state.clone() {
            GetOidcState::StartTransaction => self.handle_start_txn(event),
            GetOidcState::ReadSubjectIndex { txn_id } => self.accept_subject(event, txn_id),
            GetOidcState::ReadExistingUser { txn_id } => self.accept_existing(event, txn_id),
            GetOidcState::CommitTransaction { user } => self.handle_commit_transaction(event, user),
            GetOidcState::Init | GetOidcState::Finish | GetOidcState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetOidcState::Finish | GetOidcState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(GetOidcError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            GetOidcState::ReadSubjectIndex { txn_id }
            | GetOidcState::ReadExistingUser { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}
