use crate::revoke_token::{RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenOperation};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::USER_SESSION_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, UserSession};
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq, Eq)]
enum RevokeSessionState {
    Init,
    ReadSession,
    RevokeToken,
    StartTransaction,
    RereadSession,
    WriteSession,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RevokeSessionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("session not found")]
    NotFound,
    #[error("token revocation failed: {0}")]
    TokenRevoke(String),
    #[error("session revocation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct RevokeSessionOperation {
    actor: Actor,
    session_id: String,
    now: u64,
    session: Option<UserSession>,
    txn_id: Option<TxnId>,
    state: RevokeSessionState,
    output: Option<Result<Option<UserSession>, RevokeSessionError>>,
}

impl RevokeSessionOperation {
    pub fn new(actor: Actor, session_id: String, now: u64) -> Self {
        Self {
            actor,
            session_id,
            now,
            session: None,
            txn_id: None,
            state: RevokeSessionState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: RevokeSessionError) -> Effects {
        let cleanup = self.abort();
        self.state = RevokeSessionState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.emit_error(RevokeSessionError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn read_session(&self, txn_id: Option<TxnId>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_SESSION_KEYSPACE.to_string(),
            key: self.session_id.as_bytes().into(),
            txn_id,
        })]
    }

    fn decode_session(&self, key: &[u8], value: &[u8]) -> Result<UserSession, RevokeSessionError> {
        let session = UserSession::from_bytes(value)?;
        if session.sid.as_bytes() != key
            || session.sid != self.session_id
            || session.user_id != self.actor.user_id
            || session.user_id.realm_id != self.actor.realm_id
        {
            return Err(RevokeSessionError::NotFound);
        }
        Ok(session)
    }

    fn begin_write(&mut self) -> Effects {
        self.state = RevokeSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_session(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { key, value }) = event else {
            return self.unexpected("session read", event);
        };
        let Some(value) = value else {
            self.state = RevokeSessionState::Finish;
            self.output = Some(Ok(None));
            return smallvec![];
        };
        let session = match self.decode_session(key.as_ref(), value.as_ref()) {
            Ok(session) => session,
            Err(error) => return self.emit_error(error),
        };
        if session.revoked {
            self.state = RevokeSessionState::Finish;
            self.output = Some(Ok(Some(session)));
            return smallvec![];
        }
        self.session = Some(session.clone());
        if session.expires_at <= self.now {
            return self.begin_write();
        }
        self.state = RevokeSessionState::RevokeToken;
        smallvec![Effect::SubOperation(boxed_suboperation(
            RevokeTokenOperation::new(RevokeTokenConfig {
                actor: self.actor.clone(),
                token_hash: session.token_hash,
                expires_at: session.expires_at,
                token_owner: session.user_id,
                admission: RevokeTokenAdmission::SelfService,
                now: self.now,
            }),
            |result| Event::SubOperation(SubOperationEvent::TokenRevoked {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn handle_revoked(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::TokenRevoked { result }) = event else {
            return self.unexpected("token revocation result", event);
        };
        match result {
            Ok(()) => self.begin_write(),
            Err(error) => self.emit_error(RevokeSessionError::TokenRevoke(error)),
        }
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected("transaction started", event);
        };
        self.txn_id = Some(txn_id);
        self.state = RevokeSessionState::RereadSession;
        self.read_session(Some(txn_id))
    }

    fn handle_reread(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { key, value }) = event else {
            return self.unexpected("session reread", event);
        };
        let Some(value) = value else {
            return self.emit_error(RevokeSessionError::NotFound);
        };
        let mut session = match self.decode_session(key.as_ref(), value.as_ref()) {
            Ok(session) => session,
            Err(error) => return self.emit_error(error),
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        if session.revoked {
            self.output = Some(Ok(Some(session)));
            self.state = RevokeSessionState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        session.revoked = true;
        let bytes = match session.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.emit_error(error.into()),
        };
        self.session = Some(session);
        self.state = RevokeSessionState::WriteSession;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_SESSION_KEYSPACE.to_string(),
            key: self.session_id.as_bytes().into(),
            value: bytes.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected("session write", event);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        let Some(session) = self.session.clone() else {
            return self.emit_error(RevokeSessionError::NotFinished);
        };
        self.output = Some(Ok(Some(session)));
        self.state = RevokeSessionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected("transaction committed", event);
        };
        self.txn_id = None;
        self.state = RevokeSessionState::Finish;
        smallvec![]
    }
}

impl Operation for RevokeSessionOperation {
    type Output = Option<UserSession>;
    type Error = RevokeSessionError;

    fn start(&mut self) -> Effects {
        if let Err(error) = Ulid::from_string(&self.session_id) {
            return self.emit_error(ConversionError::from(error).into());
        }
        self.state = RevokeSessionState::ReadSession;
        self.read_session(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(error.into());
        }
        match self.state {
            RevokeSessionState::Init => self.start(),
            RevokeSessionState::ReadSession => self.handle_session(event),
            RevokeSessionState::RevokeToken => self.handle_revoked(event),
            RevokeSessionState::StartTransaction => self.handle_started(event),
            RevokeSessionState::RereadSession => self.handle_reread(event),
            RevokeSessionState::WriteSession => self.handle_written(event),
            RevokeSessionState::CommitTransaction => self.handle_committed(event),
            RevokeSessionState::Finish | RevokeSessionState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RevokeSessionState::Finish | RevokeSessionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(RevokeSessionError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, RevokeSessionError::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmId, SessionKind};
    use aruna_core::types::UserId;

    #[test]
    fn revoke_is_idempotent() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
        let session_id = Ulid::from_bytes([5; 16]).to_string();
        let record = UserSession {
            sid: session_id.clone(),
            user_id,
            kind: SessionKind::Assistant,
            label: None,
            created_at: 10,
            expires_at: 20,
            token_hash: "a".repeat(64),
            revoked: true,
        };
        let mut operation = RevokeSessionOperation::new(
            Actor {
                node_id: iroh::SecretKey::generate().public(),
                user_id,
                realm_id,
            },
            session_id.clone(),
            15,
        );
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: session_id.as_bytes().into(),
            value: Some(record.to_bytes().unwrap().into()),
        }));

        assert!(operation.finalize().unwrap().unwrap().revoked);
    }
}
