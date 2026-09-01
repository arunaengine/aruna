use super::{
    MAX_GROUP_SESSIONS, S3SessionCredentials, S3SessionError, build_session, decode_index,
    encode_index, expiry_key, owner_key, session_age,
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
use std::collections::BTreeSet;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub struct CreateS3SessionConfig {
    pub user_identity: UserId,
    pub group_id: GroupId,
    pub now: SystemTime,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CreateSessionState {
    Init,
    StartTransaction,
    ReadIndex,
    ReadSessions { index: BTreeSet<String> },
    DeleteSessions { index: BTreeSet<String> },
    WriteSession,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct CreateS3SessionOperation {
    config: CreateS3SessionConfig,
    key_id: String,
    seal_key: CredentialSealKey,
    pending: Option<S3SessionCredentials>,
    txn_id: Option<Ulid>,
    state: CreateSessionState,
    output: Result<S3SessionCredentials, S3SessionError>,
}

impl CreateS3SessionOperation {
    pub fn new(config: CreateS3SessionConfig, seal_key: CredentialSealKey) -> Self {
        Self::with_key(config, Ulid::generate().to_string(), seal_key)
    }

    pub fn with_key(
        config: CreateS3SessionConfig,
        key_id: String,
        seal_key: CredentialSealKey,
    ) -> Self {
        Self {
            config,
            key_id,
            seal_key,
            pending: None,
            txn_id: None,
            state: CreateSessionState::Init,
            output: Err(S3SessionError::NotFinished),
        }
    }

    fn fail(&mut self, error: S3SessionError) -> Effects {
        self.state = CreateSessionState::Error;
        self.output = Err(error);
        self.abort()
    }

    fn start_create(&mut self) -> Effects {
        if !matches!(self.state, CreateSessionState::Init) {
            return self.fail(S3SessionError::Failed);
        }
        let Ok(ttl) = self.config.expiry.duration_since(self.config.now) else {
            return self.fail(S3SessionError::InvalidExpiry);
        };
        if ttl.is_zero() || ttl > S3_SESSION_MAX_TTL {
            return self.fail(S3SessionError::InvalidExpiry);
        }
        let access_key = match S3Session::build_access_key(&self.key_id) {
            Ok(access_key) => access_key,
            Err(error) => return self.fail(error.into()),
        };
        let pending = match build_session(
            access_key,
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
        self.pending = Some(pending);
        self.state = CreateSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event, "StorageEvent::TransactionStarted");
        };
        self.txn_id = Some(txn_id);
        self.state = CreateSessionState::ReadIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_SESSION_OWNER_KEYSPACE.to_string(),
            key: owner_key(self.config.user_identity, self.config.group_id),
            txn_id: Some(txn_id),
        })]
    }

    fn index_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event, "StorageEvent::ReadResult");
        };
        let index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.fail(error),
        };
        let Some(pending) = self.pending.as_ref() else {
            return self.fail(S3SessionError::Failed);
        };
        if index.contains(&pending.access_key_id) {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        let mut reads = index
            .iter()
            .map(|access_key| {
                (
                    S3_SESSION_KEYSPACE.to_string(),
                    access_key.as_bytes().into(),
                )
            })
            .collect::<Vec<_>>();
        reads.push((
            S3_SESSION_KEYSPACE.to_string(),
            pending.access_key_id.as_bytes().into(),
        ));
        self.state = CreateSessionState::ReadSessions {
            index: index.clone(),
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    fn sessions_read(&mut self, event: Event, index: BTreeSet<String>) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event, "StorageEvent::BatchReadResult");
        };
        if values.len() != index.len() + 1 {
            return self.fail(S3SessionError::IndexInconsistent);
        }
        let Some(pending_key) = self
            .pending
            .as_ref()
            .map(|pending| pending.access_key_id.clone())
        else {
            return self.fail(S3SessionError::Failed);
        };
        let mut active = Vec::new();
        let mut removed = Vec::new();
        for (key, value) in values {
            let key_bytes = key.as_ref();
            if key_bytes == pending_key.as_bytes() {
                if value.is_some() {
                    return self.fail(S3SessionError::IndexInconsistent);
                }
                continue;
            }
            let Some(value) = value else {
                return self.fail(S3SessionError::IndexInconsistent);
            };
            let session = match S3Session::from_bytes(value.as_ref()) {
                Ok(session) => session,
                Err(error) => return self.fail(error.into()),
            };
            if session.access_key.as_bytes() != key_bytes
                || !index.contains(&session.access_key)
                || session.user_identity != self.config.user_identity
                || session.group_id != self.config.group_id
            {
                return self.fail(S3SessionError::IndexInconsistent);
            }
            if session.is_expired(self.config.now) {
                removed.push(session);
            } else {
                active.push(session);
            }
        }
        // The new session must fit the bound, so the oldest ones above it are evicted.
        active.sort_by_key(session_age);
        let evicted = active.len().saturating_sub(MAX_GROUP_SESSIONS - 1);
        removed.extend(active.drain(..evicted));
        let mut index: BTreeSet<String> = active
            .into_iter()
            .map(|session| session.access_key)
            .collect();
        index.insert(pending_key);
        if removed.is_empty() {
            return self.write_session(index);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(S3SessionError::Failed);
        };
        let mut deletes = Vec::with_capacity(removed.len() * 2);
        for session in removed {
            let expiry_key = match expiry_key(session.expiry, &session.access_key) {
                Ok(key) => key,
                Err(error) => return self.fail(error),
            };
            deletes.push((
                S3_SESSION_KEYSPACE.to_string(),
                session.access_key.as_bytes().into(),
            ));
            deletes.push((S3_SESSION_EXPIRY_KEYSPACE.to_string(), expiry_key));
        }
        self.state = CreateSessionState::DeleteSessions { index };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })]
    }

    fn sessions_deleted(&mut self, event: Event, index: BTreeSet<String>) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected(event, "StorageEvent::BatchDeleteResult");
        };
        self.write_session(index)
    }

    fn write_session(&mut self, index: BTreeSet<String>) -> Effects {
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
        let index_bytes = match encode_index(&index) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error),
        };
        let expiry_key = match expiry_key(pending.session.expiry, &pending.access_key_id) {
            Ok(key) => key,
            Err(error) => return self.fail(error),
        };
        let owner_key = owner_key(self.config.user_identity, self.config.group_id);
        self.state = CreateSessionState::WriteSession;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    S3_SESSION_KEYSPACE.to_string(),
                    pending.access_key_id.as_bytes().into(),
                    session_bytes.into(),
                ),
                (
                    S3_SESSION_OWNER_KEYSPACE.to_string(),
                    owner_key.clone(),
                    index_bytes,
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
        self.state = CreateSessionState::CommitTransaction;
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
        self.state = CreateSessionState::Finish;
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

impl Operation for CreateS3SessionOperation {
    type Output = S3SessionCredentials;
    type Error = S3SessionError;

    fn start(&mut self) -> Effects {
        self.start_create()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state.clone() {
            CreateSessionState::StartTransaction => self.transaction_started(event),
            CreateSessionState::ReadIndex => self.index_read(event),
            CreateSessionState::ReadSessions { index } => self.sessions_read(event, index),
            CreateSessionState::DeleteSessions { index } => self.sessions_deleted(event, index),
            CreateSessionState::WriteSession => self.session_written(event),
            CreateSessionState::CommitTransaction => self.transaction_committed(event),
            CreateSessionState::Init | CreateSessionState::Finish | CreateSessionState::Error => {
                self.unexpected(event, "valid session operation event")
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateSessionState::Finish | CreateSessionState::Error
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
