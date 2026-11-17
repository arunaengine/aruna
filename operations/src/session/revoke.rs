//! Revokes a user session together with its token and rewrites the stored session row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenError, RevokeTokenOperation, owner_cutoff,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::USER_SESSION_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use ulid::Ulid;

/// Moves the actor's token cutoff past `issued_at`, which ends every token the actor was issued
/// up to then. One entry per user, so it is admitted when the per-token set is full.
pub(crate) fn cutoff_revocation(actor: Actor, issued_at: u64, now: u64) -> RevokeTokenConfig {
    let owner = actor.user_id;
    owner_cutoff(actor, owner, issued_at, now)
}

/// Where a session revocation stands before its write transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
enum RevokeSessionState {
    ReadSession,
    RevokeToken,
    RaiseCutoff,
    Write,
}

type RevokeTurn = Result<Next<Option<UserSession>>, RevokeSessionError>;

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
        got: &'static str,
    },
}

impl From<IoError> for RevokeSessionError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "RevokeSession".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

/// Revokes the session's token outside any transaction, then marks the session revoked in a
/// write transaction that reads it again.
#[derive(Debug, PartialEq)]
pub struct RevokeSessionOperation {
    actor: Actor,
    owner: UserId,
    session_id: String,
    now: u64,
    session: Option<UserSession>,
    state: RevokeSessionState,
}

impl RevokeSessionOperation {
    pub fn new(actor: Actor, session_id: String, now: u64) -> Self {
        let owner = actor.user_id;
        Self::for_owner(actor, owner, session_id, now)
    }

    /// Revokes a session of `owner`; the caller has authorized `actor` to do so.
    pub fn for_owner(actor: Actor, owner: UserId, session_id: String, now: u64) -> Self {
        Self {
            actor,
            owner,
            session_id,
            now,
            session: None,
            state: RevokeSessionState::ReadSession,
        }
    }

    fn handle_session(&mut self, value: Option<Value>) -> RevokeTurn {
        let Some(value) = value else {
            return Ok(Next::Done(None));
        };
        let session = decode_session(&self.session_id, self.owner, value.as_ref())?;
        if session.revoked {
            return Ok(Next::Done(Some(session)));
        }
        self.session = Some(session.clone());
        if session.expires_at <= self.now {
            return Ok(self.write());
        }
        self.state = RevokeSessionState::RevokeToken;
        Ok(Next::Call(IoCall::child(RevokeTokenOperation::new(
            RevokeTokenConfig {
                actor: self.actor.clone(),
                token_hash: session.token_hash,
                expires_at: session.expires_at,
                token_owner: session.user_id,
                admission: match session.user_id == self.actor.user_id {
                    true => RevokeTokenAdmission::SelfService,
                    false => RevokeTokenAdmission::Privileged,
                },
                now: self.now,
            },
        ))))
    }

    fn handle_revoked(&mut self, result: IoResult) -> RevokeTurn {
        let revoked = result.child::<RealmConfigDocument, RevokeTokenError>()?;
        match revoked {
            Ok(_) => Ok(self.write()),
            // A full revocation set never keeps a session alive: the user's cutoff moves past
            // it, which also signs out the user's older sessions.
            Err(RevokeTokenError::CapacityReached)
                if self.state == RevokeSessionState::RevokeToken =>
            {
                let session = self
                    .session
                    .as_ref()
                    .ok_or(RevokeSessionError::NotFinished)?;
                let config =
                    owner_cutoff(self.actor.clone(), self.owner, session.created_at, self.now);
                self.state = RevokeSessionState::RaiseCutoff;
                Ok(Next::Call(IoCall::child(RevokeTokenOperation::new(config))))
            }
            Err(error) => Err(RevokeSessionError::TokenRevoke(error.to_string())),
        }
    }

    fn write(&mut self) -> Next<Option<UserSession>> {
        self.state = RevokeSessionState::Write;
        Next::Call(IoCall::child(SessionWrite {
            session_id: self.session_id.clone(),
            owner: self.owner,
            session: None,
        }))
    }
}

fn session_row(session_id: &str) -> (KeySpace, Key) {
    (
        USER_SESSION_KEYSPACE.to_string(),
        session_id.as_bytes().into(),
    )
}

/// The stored session of `owner` under `session_id`; another one is not found.
fn decode_session(
    session_id: &str,
    owner: UserId,
    value: &[u8],
) -> Result<UserSession, RevokeSessionError> {
    let session = UserSession::from_bytes(value)?;
    if session.sid != session_id || session.user_id != owner {
        return Err(RevokeSessionError::NotFound);
    }
    Ok(session)
}

impl Operation for RevokeSessionOperation {
    type Output = Option<UserSession>;
    type Error = RevokeSessionError;

    fn first(&mut self) -> RevokeTurn {
        Ulid::from_string(&self.session_id).map_err(ConversionError::from)?;
        Ok(Next::Call(IoCall::Read(vec![session_row(
            &self.session_id,
        )])))
    }

    fn next(&mut self, result: IoResult) -> RevokeTurn {
        match self.state {
            RevokeSessionState::ReadSession => {
                let [value] = values::<1>(result)?;
                self.handle_session(value)
            }
            RevokeSessionState::RevokeToken | RevokeSessionState::RaiseCutoff => {
                self.handle_revoked(result)
            }
            RevokeSessionState::Write => result
                .child::<Option<UserSession>, RevokeSessionError>()?
                .map(Next::Done),
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, RevokeSessionError::NotFound)
    }
}

/// Reads the session again and marks it revoked in one write transaction.
#[derive(Debug, PartialEq)]
struct SessionWrite {
    session_id: String,
    owner: UserId,
    session: Option<UserSession>,
}

impl Operation for SessionWrite {
    type Output = Option<UserSession>;
    type Error = RevokeSessionError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> RevokeTurn {
        Ok(Next::Call(IoCall::Read(vec![session_row(
            &self.session_id,
        )])))
    }

    fn next(&mut self, result: IoResult) -> RevokeTurn {
        if let Some(session) = self.session.take() {
            done(result)?;
            return Ok(Next::Done(Some(session)));
        }
        let [value] = values::<1>(result)?;
        let value = value.ok_or(RevokeSessionError::NotFound)?;
        let mut session = decode_session(&self.session_id, self.owner, value.as_ref())?;
        if session.revoked {
            return Ok(Next::Done(Some(session)));
        }
        session.revoked = true;
        let bytes = session.to_bytes()?;
        self.session = Some(session);
        let (space, key) = session_row(&self.session_id);
        Ok(Next::Call(IoCall::Write(vec![(space, key, bytes.into())])))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::auth::{user_cutoff_expiry, user_cutoff_hash};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::auth::SessionKind;
    use aruna_core::structs::identity::realm::RealmId;

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
            via: None,
        };
        let mut operation = RevokeSessionOperation::new(
            Actor {
                node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
                user_id,
                realm_id,
            },
            session_id.clone(),
            15,
        );
        operation.first().unwrap();
        let stored = IoResult::Values(vec![Some(record.to_bytes().unwrap().into())]);
        let Ok(Next::Done(Some(session))) = operation.next(stored) else {
            panic!("expected the revoked session");
        };
        assert!(session.revoked);
    }

    #[test]
    fn revokes_federated_session() {
        // A federated user served by realm 3 revokes its own session and token.
        let realm_id = RealmId::from_bytes([3; 32]);
        let user_id = UserId::new(Ulid::from_bytes([4; 16]), RealmId::from_bytes([9; 32]));
        let session_id = Ulid::from_bytes([5; 16]).to_string();
        let record = UserSession {
            sid: session_id.clone(),
            user_id,
            kind: SessionKind::Federated,
            label: None,
            created_at: 10,
            expires_at: 20,
            token_hash: "a".repeat(64),
            revoked: false,
            via: None,
        };
        let mut operation = RevokeSessionOperation::new(
            Actor {
                node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
                user_id,
                realm_id,
            },
            session_id.clone(),
            15,
        );
        operation.first().unwrap();
        let stored = IoResult::Values(vec![Some(record.to_bytes().unwrap().into())]);
        assert!(matches!(
            operation.next(stored),
            Ok(Next::Call(IoCall::Child(_)))
        ));
    }

    // A full revocation set never keeps a session alive: the user's cutoff moves past it.
    #[test]
    fn full_revocation_cutoff() {
        let realm_id = RealmId::from_bytes([3; 32]);
        let user_id = UserId::new(Ulid::from_bytes([4; 16]), realm_id);
        let session_id = Ulid::from_bytes([5; 16]).to_string();
        let record = UserSession {
            sid: session_id.clone(),
            user_id,
            kind: SessionKind::Portal,
            label: None,
            created_at: 10,
            expires_at: 20,
            token_hash: "a".repeat(64),
            revoked: false,
            via: None,
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
            user_id,
            realm_id,
        };
        let stored = || IoResult::Values(vec![Some(record.to_bytes().unwrap().into())]);
        let full = || {
            let full = Err::<RealmConfigDocument, _>(RevokeTokenError::CapacityReached);
            IoResult::Child(ChildOutcome::new(full))
        };
        let mut operation = RevokeSessionOperation::new(actor.clone(), session_id.clone(), 15);
        operation.first().unwrap();
        operation.next(stored()).unwrap();
        let raised = operation.next(full());
        assert!(matches!(raised, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(operation.state, RevokeSessionState::RaiseCutoff);
        let cutoff = cutoff_revocation(actor.clone(), record.created_at, 15);
        assert_eq!(cutoff.token_hash, user_cutoff_hash(&user_id));
        assert_eq!(cutoff.admission, RevokeTokenAdmission::Privileged);
        // Tokens issued in the session's second or before are cut off.
        assert_eq!(cutoff.expires_at, user_cutoff_expiry(11));

        let config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let raised = IoResult::Child(ChildOutcome::new(Ok::<_, RevokeTokenError>(config)));
        assert!(matches!(
            operation.next(raised),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(operation.state, RevokeSessionState::Write);

        // A failed cutoff is reported, never retried as another cutoff.
        let mut operation = RevokeSessionOperation::new(actor, session_id.clone(), 15);
        operation.first().unwrap();
        operation.next(stored()).unwrap();
        operation.next(full()).unwrap();
        assert!(matches!(
            operation.next(full()),
            Err(RevokeSessionError::TokenRevoke(_))
        ));
    }

    #[test]
    fn write_marks_revoked() {
        // The write transaction reads the session again and stores it revoked.
        let realm_id = RealmId::from_bytes([3; 32]);
        let user_id = UserId::new(Ulid::from_bytes([4; 16]), realm_id);
        let session_id = Ulid::from_bytes([5; 16]).to_string();
        let record = UserSession {
            sid: session_id.clone(),
            user_id,
            kind: SessionKind::Portal,
            label: None,
            created_at: 10,
            expires_at: 20,
            token_hash: "a".repeat(64),
            revoked: false,
            via: None,
        };
        let mut write = SessionWrite {
            session_id,
            owner: user_id,
            session: None,
        };
        assert_eq!(write.transaction(), TransactionMode::Write);
        write.first().unwrap();
        let stored = IoResult::Values(vec![Some(record.to_bytes().unwrap().into())]);
        let Ok(Next::Call(IoCall::Write(rows))) = write.next(stored) else {
            panic!("expected the session write");
        };
        let written = UserSession::from_bytes(rows[0].2.as_ref()).unwrap();
        assert!(written.revoked);
        assert_eq!(write.next(IoResult::Done), Ok(Next::Done(Some(written))));
    }
}
