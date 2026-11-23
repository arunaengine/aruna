//! Refreshes an S3 session with a new secret and expiry, and rewrites its index rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    S3SessionCredentials, S3SessionError, SESSION_SECRETS_LEN, build_session, decode_index,
    expiry_key, owner_key,
};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::{Secret, draw_secret, drawn_secret};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{S3_SESSION_KEYSPACE, SESSION_EXPIRY_KEYSPACE, SESSION_OWNER_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::s3_session::{PreviousCredential, S3Session, SESSION_MAX_TTL};
use aruna_core::types::{GroupId, Value};
use aruna_core::user::id::UserId;
use std::time::SystemTime;

#[derive(Debug, PartialEq)]
pub struct RefreshS3Config {
    pub access_key: String,
    pub user_identity: UserId,
    pub group_id: GroupId,
    pub now: SystemTime,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefreshPhase {
    Secret,
    Read,
    Delete,
    Write,
}

#[derive(Debug, PartialEq)]
pub struct RefreshS3Operation {
    config: RefreshS3Config,
    encryption_key: CredentialEncryptionKey,
    /// The secrets drawn for the successor, until it is built.
    drawn: Option<Secret>,
    pending: Option<S3SessionCredentials>,
    phase: RefreshPhase,
    ids: IdMint,
    now_ms: u64,
}

impl RefreshS3Operation {
    pub fn new(config: RefreshS3Config, encryption_key: CredentialEncryptionKey) -> Self {
        Self {
            config,
            encryption_key,
            drawn: None,
            pending: None,
            phase: RefreshPhase::Secret,
            ids: IdMint::default(),
            now_ms: 0,
        }
    }

    /// Checks that the session may be refreshed now and builds its successor; the old expiry
    /// row goes next.
    fn records_read(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        let [session_value, index_value] = <[Option<Value>; 2]>::try_from(values)
            .map_err(|_| S3SessionError::IndexInconsistent)?;
        let session_value = session_value.ok_or(S3SessionError::NotFound)?;
        let session = S3Session::from_bytes(session_value.as_ref())?;
        if session.access_key != self.config.access_key {
            return Err(S3SessionError::IndexInconsistent);
        }
        if session.user_identity != self.config.user_identity {
            return Err(S3SessionError::WrongOwner);
        }
        if session.group_id != self.config.group_id {
            return Err(S3SessionError::WrongGroup);
        }
        if session.issued_by != self.config.issued_by {
            return Err(S3SessionError::WrongIssuer);
        }
        if session.is_expired(self.config.now) {
            return Err(S3SessionError::Expired);
        }
        if !session.can_refresh(self.config.now) {
            return Err(S3SessionError::TooEarly);
        }
        if session.last_used_at.is_none() {
            return Err(S3SessionError::Idle);
        }
        if self.config.expiry <= session.expiry {
            return Err(S3SessionError::NotExtended);
        }
        // Only one replaced pair is kept, so it must lapse before the next rotation.
        if (session.previous.as_ref()).is_some_and(|previous| previous.expiry > self.config.now) {
            return Err(S3SessionError::TooEarly);
        }
        let index = decode_index(index_value.as_ref())?;
        if !index.contains(&self.config.access_key) {
            return Err(S3SessionError::IndexInconsistent);
        }
        let mut pending = build_session(
            self.config.access_key.clone(),
            self.config.user_identity,
            self.config.group_id,
            self.config.expiry,
            self.config.path_restrictions.clone(),
            self.config.issued_by,
            &self.encryption_key,
            self.drawn.take().ok_or(S3SessionError::Failed)?,
            aruna_core::id_mint::id_nonce(self.ids.mint(self.now_ms)),
        )?;
        // Requests signed before this refresh stay valid until the old expiry.
        pending.session.previous = Some(PreviousCredential {
            secret: session.secret.clone(),
            token_hash: session.token_hash.clone(),
            expiry: session.expiry,
        });
        let old_key = expiry_key(session.expiry, &self.config.access_key)?;
        self.pending = Some(pending);
        self.phase = RefreshPhase::Delete;
        Ok(Next::Call(IoCall::Delete(vec![(
            SESSION_EXPIRY_KEYSPACE.to_string(),
            old_key,
        )])))
    }

    /// The refreshed session row and its new expiry row.
    fn write(&mut self) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        let pending = self.pending.as_ref().ok_or(S3SessionError::Failed)?;
        let session_bytes = pending.session.to_bytes()?;
        let expiry = expiry_key(pending.session.expiry, &pending.access_key_id)?;
        let owner = owner_key(self.config.user_identity, self.config.group_id);
        self.phase = RefreshPhase::Write;
        Ok(Next::Call(IoCall::Write(vec![
            (
                S3_SESSION_KEYSPACE.to_string(),
                pending.access_key_id.as_bytes().into(),
                session_bytes.into(),
            ),
            (SESSION_EXPIRY_KEYSPACE.to_string(), expiry, owner),
        ])))
    }
}

impl Operation for RefreshS3Operation {
    type Output = S3SessionCredentials;
    type Error = S3SessionError;

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        if !S3Session::valid_access_key(&self.config.access_key) {
            return Err(S3SessionError::InvalidAccessKey);
        }
        let ttl = (self.config.expiry.duration_since(self.config.now))
            .map_err(|_| S3SessionError::InvalidExpiry)?;
        if ttl.is_zero() || ttl > SESSION_MAX_TTL {
            return Err(S3SessionError::InvalidExpiry);
        }
        self.phase = RefreshPhase::Secret;
        Ok(Next::Call(draw_secret(SESSION_SECRETS_LEN)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        match self.phase {
            RefreshPhase::Secret => {
                self.drawn = Some(drawn_secret(result)?);
                self.phase = RefreshPhase::Read;
                Ok(Next::Call(IoCall::Read(vec![
                    (
                        S3_SESSION_KEYSPACE.to_string(),
                        self.config.access_key.as_bytes().into(),
                    ),
                    (
                        SESSION_OWNER_KEYSPACE.to_string(),
                        owner_key(self.config.user_identity, self.config.group_id),
                    ),
                ])))
            }
            RefreshPhase::Read => self.records_read(result.values()?),
            RefreshPhase::Delete => {
                result.done()?;
                self.write()
            }
            RefreshPhase::Write => {
                result.done()?;
                let pending = self.pending.take().ok_or(S3SessionError::Failed)?;
                Ok(Next::Done(pending))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn expected_error(error: &Self::Error) -> bool {
        error.expected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::{Effect, NetEffect};
    use aruna_core::events::{Event, NetEvent};
    use aruna_core::structs::identity::realm::RealmId;
    use std::time::Duration;
    use ulid::Ulid;

    fn config(expiry: Duration) -> RefreshS3Config {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        RefreshS3Config {
            access_key: S3Session::build_access_key("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap(),
            user_identity: UserId::local(Ulid::from_parts(2, 2), RealmId::from_bytes([1; 32])),
            group_id: Ulid::from_parts(3, 3),
            now,
            expiry: now + expiry,
            path_restrictions: None,
            issued_by: [1; 32],
        }
    }

    fn operation(expiry: Duration) -> RefreshS3Operation {
        let key = CredentialEncryptionKey::derive(&[7u8; 32]);
        RefreshS3Operation::new(config(expiry), key)
    }

    #[test]
    fn reads_session_index() {
        let mut refresh = operation(Duration::from_secs(60));
        let Ok(Next::Call(IoCall::Effect(draw))) = refresh.first() else {
            panic!("expected the secret draw");
        };
        assert_eq!(
            *draw,
            Effect::Net(NetEffect::DrawSecret(SESSION_SECRETS_LEN))
        );
        let secret = Secret::new("d".repeat(SESSION_SECRETS_LEN));
        let drawn = IoResult::Event(Box::new(Event::Net(NetEvent::SecretDrawn(secret))));
        let Ok(Next::Call(IoCall::Read(reads))) = refresh.next(drawn) else {
            panic!("expected the session and index read");
        };
        assert_eq!(reads.len(), 2);
        assert_eq!(
            refresh.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let missing = refresh.next(IoResult::Values(vec![None, None]));
        assert_eq!(missing, Err(S3SessionError::NotFound));
        let short = refresh.next(IoResult::Values(vec![None]));
        assert_eq!(short, Err(S3SessionError::IndexInconsistent));
    }

    #[test]
    fn refuses_bad_expiry() {
        let mut zero = operation(Duration::ZERO);
        assert_eq!(zero.first(), Err(S3SessionError::InvalidExpiry));
        let mut long = operation(SESSION_MAX_TTL + Duration::from_secs(1));
        assert_eq!(long.first(), Err(S3SessionError::InvalidExpiry));
    }
}
