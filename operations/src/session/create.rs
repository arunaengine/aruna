use super::index::{decode_index, encode_index, owner_key};
use crate::create_token::{CreateTokenConfig, CreateTokenError, mint_token};
use aruna_core::auth::bearer_token_hash;
use aruna_core::compute::Secret;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{USER_SESSION_KEYSPACE, USER_SESSION_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{NodeCapabilities, RealmId, SessionKind, SessionRef, UserSession};
use aruna_core::types::{Effects, TxnId, UserId};
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

pub const MAX_SESSION_TTL: u64 = 24 * 60 * 60;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateSessionConfig {
    pub time: u64,
    pub expiry: u64,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_capabilities: NodeCapabilities,
    pub kind: SessionKind,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedSession {
    pub token: Secret,
    pub session: UserSession,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CreateSessionState {
    Init,
    StartTransaction,
    ReadOwnerIndex,
    ReadSession { index: BTreeSet<String> },
    WriteSession,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateSessionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Token(#[from] CreateTokenError),
    #[error("session expiry is invalid")]
    InvalidExpiry,
    #[error("session id collision")]
    IdCollision,
    #[error("session operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct CreateSessionOperation {
    config: CreateSessionConfig,
    sid: String,
    session: Option<UserSession>,
    token: Option<Secret>,
    txn_id: Option<TxnId>,
    state: CreateSessionState,
    output: Option<Result<CreatedSession, CreateSessionError>>,
}

pub fn bound_session_expiry(
    now: u64,
    requested: Option<u64>,
    parent_expiry: u64,
) -> Result<u64, CreateSessionError> {
    let lifetime = requested.unwrap_or(MAX_SESSION_TTL).min(MAX_SESSION_TTL);
    let expires_at = now
        .checked_add(lifetime)
        .ok_or(CreateSessionError::InvalidExpiry)?
        .min(parent_expiry);
    if lifetime == 0 || expires_at <= now {
        return Err(CreateSessionError::InvalidExpiry);
    }
    Ok(expires_at)
}

impl CreateSessionOperation {
    pub fn new(config: CreateSessionConfig) -> Self {
        Self::new_with_sid(config, Ulid::generate().to_string())
    }

    pub fn new_with_sid(config: CreateSessionConfig, sid: String) -> Self {
        Self {
            config,
            sid,
            session: None,
            token: None,
            txn_id: None,
            state: CreateSessionState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CreateSessionError) -> Effects {
        let cleanup = self.abort();
        self.state = CreateSessionState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.emit_error(CreateSessionError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn prepare_session(&mut self) -> Result<(), CreateSessionError> {
        Ulid::from_string(&self.sid).map_err(ConversionError::from)?;
        if self.config.expiry <= self.config.time {
            return Err(CreateSessionError::InvalidExpiry);
        }
        let session_ref = SessionRef {
            sid: self.sid.clone(),
            kind: self.config.kind,
        };
        let token = mint_token(&CreateTokenConfig {
            time: self.config.time,
            expiry: Some(self.config.expiry),
            user_id: self.config.user_id,
            realm_id: self.config.realm_id,
            node_capabilities: self.config.node_capabilities.clone(),
            session: Some(session_ref),
        })?;
        self.session = Some(UserSession {
            sid: self.sid.clone(),
            user_id: self.config.user_id,
            kind: self.config.kind,
            label: self.config.label.clone(),
            created_at: self.config.time,
            expires_at: self.config.expiry,
            token_hash: bearer_token_hash(&token),
            revoked: false,
        });
        self.token = Some(Secret::new(token));
        Ok(())
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected("transaction started", event);
        };
        self.txn_id = Some(txn_id);
        self.state = CreateSessionState::ReadOwnerIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_SESSION_OWNER_KEYSPACE.to_string(),
            key: owner_key(self.config.user_id),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_index(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("owner index read", event);
        };
        let mut index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.emit_error(error.into()),
        };
        if !index.insert(self.sid.clone()) {
            return self.emit_error(CreateSessionError::IdCollision);
        }
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        self.state = CreateSessionState::ReadSession {
            index: index.clone(),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_SESSION_KEYSPACE.to_string(),
            key: self.sid.as_bytes().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_session(&mut self, event: Event, index: BTreeSet<String>) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("session read", event);
        };
        if value.is_some() {
            return self.emit_error(CreateSessionError::IdCollision);
        }
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        let Some(session) = self.session.as_ref() else {
            return self.emit_error(CreateSessionError::NotFinished);
        };
        let session_bytes = match session.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.emit_error(error.into()),
        };
        let index_bytes = match encode_index(&index) {
            Ok(bytes) => bytes,
            Err(error) => return self.emit_error(error.into()),
        };
        self.state = CreateSessionState::WriteSession;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    USER_SESSION_KEYSPACE.to_string(),
                    self.sid.as_bytes().into(),
                    session_bytes.into(),
                ),
                (
                    USER_SESSION_OWNER_KEYSPACE.to_string(),
                    owner_key(self.config.user_id),
                    index_bytes,
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected("session write", event);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        let Some(session) = self.session.clone() else {
            return self.emit_error(CreateSessionError::NotFinished);
        };
        let Some(token) = self.token.take() else {
            return self.emit_error(CreateSessionError::NotFinished);
        };
        self.output = Some(Ok(CreatedSession { token, session }));
        self.state = CreateSessionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected("transaction committed", event);
        };
        self.txn_id = None;
        self.state = CreateSessionState::Finish;
        smallvec![]
    }
}

impl Operation for CreateSessionOperation {
    type Output = CreatedSession;
    type Error = CreateSessionError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.prepare_session() {
            return self.emit_error(error);
        }
        self.state = CreateSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(error.into());
        }
        match self.state.clone() {
            CreateSessionState::Init => self.start(),
            CreateSessionState::StartTransaction => self.handle_started(event),
            CreateSessionState::ReadOwnerIndex => self.handle_index(event),
            CreateSessionState::ReadSession { index } => self.handle_session(event, index),
            CreateSessionState::WriteSession => self.handle_written(event),
            CreateSessionState::CommitTransaction => self.handle_committed(event),
            CreateSessionState::Finish | CreateSessionState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateSessionState::Finish | CreateSessionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CreateSessionError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_parent_expiry() {
        assert_eq!(bound_session_expiry(100, Some(600), 500).unwrap(), 500);
        assert_eq!(bound_session_expiry(100, None, 100_000).unwrap(), 86_500);
        assert_eq!(
            bound_session_expiry(100, Some(0), 500),
            Err(CreateSessionError::InvalidExpiry)
        );
    }
}
