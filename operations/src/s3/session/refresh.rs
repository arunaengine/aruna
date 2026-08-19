use super::{
    S3SessionCredentials, S3SessionError, build_session, decode_index, expiry_key, owner_key,
};
use aruna_core::credential_seal::CredentialSealKey;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_SESSION_EXPIRY_KEYSPACE, S3_SESSION_KEYSPACE, S3_SESSION_OWNER_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{PathRestriction, S3_SESSION_MAX_TTL, S3Session};
use aruna_core::types::{Effects, GroupId, UserId};
use smallvec::smallvec;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub struct RefreshS3SessionConfig {
    pub access_key: String,
    pub user_identity: UserId,
    pub group_id: GroupId,
    pub now: SystemTime,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RefreshSessionState {
    Init,
    StartTransaction,
    ReadRecords,
    DeleteExpiry,
    WriteSession,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct RefreshS3SessionOperation {
    config: RefreshS3SessionConfig,
    seal_key: CredentialSealKey,
    pending: Option<S3SessionCredentials>,
    txn_id: Option<Ulid>,
    state: RefreshSessionState,
    output: Result<S3SessionCredentials, S3SessionError>,
}

impl RefreshS3SessionOperation {
    pub fn new(config: RefreshS3SessionConfig, seal_key: CredentialSealKey) -> Self {
        Self {
            config,
            seal_key,
            pending: None,
            txn_id: None,
            state: RefreshSessionState::Init,
            output: Err(S3SessionError::NotFinished),
        }
    }

    fn fail(&mut self, error: S3SessionError) -> Effects {
        self.state = RefreshSessionState::Error;
        self.output = Err(error);
        self.abort()
    }

    fn start_refresh(&mut self) -> Effects {
        if !matches!(self.state, RefreshSessionState::Init) {
            return self.fail(S3SessionError::Failed);
        }
        if !S3Session::valid_access_key(&self.config.access_key) {
            return self.fail(S3SessionError::InvalidAccessKey);
        }
        let Ok(ttl) = self.config.expiry.duration_since(self.config.now) else {
            return self.fail(S3SessionError::InvalidExpiry);
        };
        if ttl.is_zero() || ttl > S3_SESSION_MAX_TTL {
            return self.fail(S3SessionError::InvalidExpiry);
        }
        self.state = RefreshSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionStarted");
        };
        self.txn_id = Some(txn_id);
        self.state = RefreshSessionState::ReadRecords;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    S3_SESSION_KEYSPACE.to_string(),
                    self.config.access_key.as_bytes().into(),
                ),
                (
                    S3_SESSION_OWNER_KEYSPACE.to_string(),
                    owner_key(self.config.user_identity, self.config.group_id),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn records_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event, "StorageEvent::BatchReadResult");
        };
        if values.len() != 2 {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        let Some(session_value) = values[0].1.as_ref() else {
            return self.fail(S3SessionError::NotFound);
        };
        let session = match S3Session::from_bytes(session_value.as_ref()) {
            Ok(session) => session,
            Err(error) => return self.fail(error.into()),
        };
        if session.access_key != self.config.access_key {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        if session.user_identity != self.config.user_identity {
            return self.fail(S3SessionError::WrongOwner);
        }
        if session.group_id != self.config.group_id {
            return self.fail(S3SessionError::WrongGroup);
        }
        if session.issued_by != self.config.issued_by {
            return self.fail(S3SessionError::WrongIssuer);
        }
        if session.is_expired(self.config.now) {
            return self.fail(S3SessionError::Expired);
        }
        if !session.can_refresh(self.config.now) {
            return self.fail(S3SessionError::TooEarly);
        }
        if session.last_used_at.is_none() {
            return self.fail(S3SessionError::Idle);
        }
        let index = match decode_index(values[1].1.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.fail(error),
        };
        if !index.contains(&self.config.access_key) {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        let pending = match build_session(
            self.config.access_key.clone(),
            self.config.user_identity,
            self.config.group_id,
            self.config.expiry,
            self.config.path_restrictions.clone(),
            self.config.issued_by,
            &self.seal_key,
        ) {
            Ok(pending) => pending,
            Err(error) => return self.fail(error),
        };
        let old_expiry = session.expiry;
        let old_key = match expiry_key(old_expiry, &self.config.access_key) {
            Ok(key) => key,
            Err(error) => return self.fail(error),
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.pending = Some(pending);
        self.state = RefreshSessionState::DeleteExpiry;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: S3_SESSION_EXPIRY_KEYSPACE.to_string(),
            key: old_key,
            txn_id: Some(txn_id),
        })]
    }

    fn expiry_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.unexpected(event, "StorageEvent::DeleteResult");
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        let Some(pending) = self.pending.as_ref() else {
            return self.fail(S3SessionError::Failed);
        };
        let session_bytes = match pending.session.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        let expiry_key = match expiry_key(pending.session.expiry, &pending.access_key_id) {
            Ok(key) => key,
            Err(error) => return self.fail(error),
        };
        let owner_key = owner_key(self.config.user_identity, self.config.group_id);
        self.state = RefreshSessionState::WriteSession;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    S3_SESSION_KEYSPACE.to_string(),
                    pending.access_key_id.as_bytes().into(),
                    session_bytes.into(),
                ),
                (
                    S3_SESSION_EXPIRY_KEYSPACE.to_string(),
                    expiry_key,
                    owner_key,
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn session_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected(event, "StorageEvent::BatchWriteResult");
        };
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        self.state = RefreshSessionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionCommitted");
        };
        self.txn_id = None;
        let Some(pending) = self.pending.take() else {
            return self.fail(S3SessionError::Failed);
        };
        self.output = Ok(pending);
        self.state = RefreshSessionState::Finish;
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

impl Operation for RefreshS3SessionOperation {
    type Output = S3SessionCredentials;
    type Error = S3SessionError;

    fn start(&mut self) -> Effects {
        self.start_refresh()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            RefreshSessionState::StartTransaction => self.transaction_started(event),
            RefreshSessionState::ReadRecords => self.records_read(event),
            RefreshSessionState::DeleteExpiry => self.expiry_deleted(event),
            RefreshSessionState::WriteSession => self.session_written(event),
            RefreshSessionState::CommitTransaction => self.transaction_committed(event),
            RefreshSessionState::Init
            | RefreshSessionState::Finish
            | RefreshSessionState::Error => self.unexpected(event, "valid session operation event"),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RefreshSessionState::Finish | RefreshSessionState::Error
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
