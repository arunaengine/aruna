use super::{S3SessionError, decode_index, encode_index, expiry_key, owner_key};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_SESSION_EXPIRY_KEYSPACE, S3_SESSION_KEYSPACE, S3_SESSION_OWNER_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::S3Session;
use aruna_core::types::{Effects, Key, UserId};
use smallvec::smallvec;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub struct RevokeS3SessionConfig {
    pub access_key: String,
    pub user_identity: UserId,
    pub issued_by: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RevokeSessionState {
    Init,
    StartTransaction,
    ReadSession,
    ReadIndex,
    WriteIndex,
    DeleteRows,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct RevokeS3SessionOperation {
    config: RevokeS3SessionConfig,
    owner_key: Option<Key>,
    deletes: Vec<(String, Key)>,
    txn_id: Option<Ulid>,
    state: RevokeSessionState,
    output: Result<(), S3SessionError>,
}

impl RevokeS3SessionOperation {
    pub fn new(config: RevokeS3SessionConfig) -> Self {
        Self {
            config,
            owner_key: None,
            deletes: Vec::new(),
            txn_id: None,
            state: RevokeSessionState::Init,
            output: Err(S3SessionError::NotFinished),
        }
    }

    fn fail(&mut self, error: S3SessionError) -> Effects {
        self.state = RevokeSessionState::Error;
        self.output = Err(error);
        self.abort()
    }

    fn start_revoke(&mut self) -> Effects {
        if !matches!(self.state, RevokeSessionState::Init) {
            return self.fail(S3SessionError::Failed);
        }
        if !S3Session::valid_access_key(&self.config.access_key) {
            return self.fail(S3SessionError::InvalidAccessKey);
        }
        self.state = RevokeSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionStarted");
        };
        self.txn_id = Some(txn_id);
        self.state = RevokeSessionState::ReadSession;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_SESSION_KEYSPACE.to_string(),
            key: self.config.access_key.as_bytes().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn session_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event, "StorageEvent::ReadResult");
        };
        let Some(value) = value else {
            return self.fail(S3SessionError::NotFound);
        };
        let session = match S3Session::from_bytes(value.as_ref()) {
            Ok(session) => session,
            Err(error) => return self.fail(error.into()),
        };
        if session.access_key != self.config.access_key {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        if session.user_identity != self.config.user_identity {
            return self.fail(S3SessionError::WrongOwner);
        }
        if session.issued_by != self.config.issued_by {
            return self.fail(S3SessionError::WrongIssuer);
        }
        let expiry_key = match expiry_key(session.expiry, &session.access_key) {
            Ok(key) => key,
            Err(error) => return self.fail(error),
        };
        self.deletes = vec![
            (
                S3_SESSION_KEYSPACE.to_string(),
                session.access_key.as_bytes().into(),
            ),
            (S3_SESSION_EXPIRY_KEYSPACE.to_string(), expiry_key),
        ];
        let owner_key = owner_key(session.user_identity, session.group_id);
        self.owner_key = Some(owner_key.clone());
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = RevokeSessionState::ReadIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_SESSION_OWNER_KEYSPACE.to_string(),
            key: owner_key,
            txn_id: Some(txn_id),
        })]
    }

    fn index_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event, "StorageEvent::ReadResult");
        };
        let mut index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.fail(error),
        };
        index.remove(&self.config.access_key);
        let (Some(owner_key), Some(txn_id)) = (self.owner_key.clone(), self.txn_id) else {
            return self.fail(S3SessionError::Failed);
        };
        if index.is_empty() {
            self.deletes
                .push((S3_SESSION_OWNER_KEYSPACE.to_string(), owner_key));
            return self.delete_rows();
        }
        let index_bytes = match encode_index(&index) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error),
        };
        self.state = RevokeSessionState::WriteIndex;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_SESSION_OWNER_KEYSPACE.to_string(),
            key: owner_key,
            value: index_bytes,
            txn_id: Some(txn_id),
        })]
    }

    fn index_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected(event, "StorageEvent::WriteResult");
        };
        self.delete_rows()
    }

    fn delete_rows(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = RevokeSessionState::DeleteRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.deletes),
            txn_id: Some(txn_id),
        })]
    }

    fn rows_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected(event, "StorageEvent::BatchDeleteResult");
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = RevokeSessionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionCommitted");
        };
        self.txn_id = None;
        self.output = Ok(());
        self.state = RevokeSessionState::Finish;
        smallvec![]
    }

    fn unexpected(&mut self, received: Event, expected: &'static str) -> Effects {
        self.fail(S3SessionError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            received,
        })
    }
}

impl Operation for RevokeS3SessionOperation {
    type Output = ();
    type Error = S3SessionError;

    fn start(&mut self) -> Effects {
        self.start_revoke()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            RevokeSessionState::StartTransaction => self.transaction_started(event),
            RevokeSessionState::ReadSession => self.session_read(event),
            RevokeSessionState::ReadIndex => self.index_read(event),
            RevokeSessionState::WriteIndex => self.index_written(event),
            RevokeSessionState::DeleteRows => self.rows_deleted(event),
            RevokeSessionState::CommitTransaction => self.transaction_committed(event),
            RevokeSessionState::Init | RevokeSessionState::Finish | RevokeSessionState::Error => {
                self.unexpected(event, "valid session operation event")
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RevokeSessionState::Finish | RevokeSessionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }

    fn expected_error(error: &Self::Error) -> bool {
        error.expected()
    }
}
