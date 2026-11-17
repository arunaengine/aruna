//! Creates a user session with a fresh token and drops stale sessions past the user limit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::index::{MAX_USER_SESSIONS, decode_index, encode_index, owner_key};
use crate::auth::create_token::{CreateTokenConfig, CreateTokenError, mint_login_token};
use aruna_core::UserId;
use aruna_core::auth::bearer_token_hash;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::Secret;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{USER_KEYSPACE, USER_OWNER_KEYSPACE, USER_SESSION_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::{
    NodeCapabilities, PathRestriction, SessionKind, SessionRef,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::User;
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::types::{GroupId, Value};
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
    /// Display name a federated session carries in its token.
    pub name: Option<String>,
    pub restrictions: Option<Vec<PathRestriction>>,
    /// The linked login of another realm a federated session of a local account came through.
    pub via: Option<UserId>,
    /// Time of the primary login this session descends from; `None` for child sessions.
    pub auth_time: Option<u64>,
    /// The user's token cutoff: sessions issued before it no longer work and count as stale.
    pub cutoff: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedSession {
    pub token: Secret,
    pub session: UserSession,
}

/// What the session transaction waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Index,
    Sessions,
    DeleteStale,
    Write,
}

type SessionTurn = Result<Next<CreatedSession>, CreateSessionError>;

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
    #[error("session owner index is inconsistent")]
    IndexInconsistent,
    /// `oldest` is when the oldest active session was issued.
    #[error("active session limit reached")]
    LimitReached { oldest: u64 },
    #[error("making room for the session failed: {0}")]
    Retire(String),
    #[error("session operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for CreateSessionError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "CreateSession".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

/// Mints the session's token, then reads the owner's sessions, drops the stale ones and writes
/// the new session with its index in one write transaction.
#[derive(Debug, PartialEq)]
pub struct CreateSessionOperation {
    config: CreateSessionConfig,
    sid: String,
    session: Option<UserSession>,
    token: Option<Secret>,
    /// The owner's session ids: the stored ones, then the ones the new index keeps.
    index: BTreeSet<String>,
    state: Stage,
    /// Who issued a restricted session for another account, and the group owning that account.
    issuer: Option<(UserId, Option<GroupId>)>,
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
            index: BTreeSet::new(),
            state: Stage::Index,
            issuer: None,
        }
    }

    /// Records `actor` instead of the session owner as the issuer of a restricted session.
    pub fn issued_by(mut self, actor: UserId, group_id: Option<GroupId>) -> Self {
        self.issuer = Some((actor, group_id));
        self
    }

    fn prepare_session(&mut self) -> Result<(), CreateSessionError> {
        Ulid::from_string(&self.sid).map_err(ConversionError::from)?;
        if self.config.expiry <= self.config.time {
            return Err(CreateSessionError::InvalidExpiry);
        }
        let session_ref = SessionRef {
            sid: self.sid.clone(),
            kind: self.config.kind,
            name: self.config.name.clone(),
            via: self.config.via,
        };
        let token = mint_login_token(
            &CreateTokenConfig {
                time: self.config.time,
                expiry: Some(self.config.expiry),
                user_id: self.config.user_id,
                realm_id: self.config.realm_id,
                node_capabilities: self.config.node_capabilities.clone(),
                session: Some(session_ref),
                restrictions: self.config.restrictions.clone(),
            },
            self.config.auth_time,
        )?;
        self.session = Some(UserSession {
            sid: self.sid.clone(),
            user_id: self.config.user_id,
            kind: self.config.kind,
            label: self.config.label.clone(),
            created_at: self.config.time,
            expires_at: self.config.expiry,
            token_hash: bearer_token_hash(&token),
            revoked: false,
            via: self.config.via,
        });
        self.token = Some(Secret::new(token));
        Ok(())
    }

    /// A restricted session audits under the group owning its account, read in the transaction.
    fn reads_owner(&self) -> bool {
        self.config.restrictions.is_some() && !matches!(self.issuer, Some((_, Some(_))))
    }

    fn handle_index(&mut self, value: Option<Value>) -> SessionTurn {
        let index = decode_index(value.as_ref())?;
        if index.contains(&self.sid) {
            return Err(CreateSessionError::IdCollision);
        }
        let mut reads = index
            .iter()
            .map(|sid| (USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into()))
            .collect::<Vec<_>>();
        reads.push((
            USER_SESSION_KEYSPACE.to_string(),
            self.sid.as_bytes().into(),
        ));
        if self.reads_owner() {
            let key = self.config.user_id.to_bytes().into();
            reads.push((USER_KEYSPACE.to_string(), key));
        }
        self.index = index;
        self.state = Stage::Sessions;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Answers in read order: the indexed sessions, the new id, then the owner when it is read.
    fn handle_sessions(&mut self, mut values: Vec<Option<Value>>) -> SessionTurn {
        if self.reads_owner() {
            let owner = values.pop().flatten();
            let owner = owner.as_deref().map(User::from_bytes).transpose()?;
            let group_id = owner.as_ref().and_then(User::service_group);
            let actor = self.issuer.map_or(self.config.user_id, |(actor, _)| actor);
            self.issuer = Some((actor, group_id));
        }
        if values.len() != self.index.len() + 1 {
            return Err(CreateSessionError::IndexInconsistent);
        }
        if values.pop().flatten().is_some() {
            return Err(CreateSessionError::IdCollision);
        }
        let mut active = BTreeSet::new();
        let mut stale = BTreeSet::new();
        let mut oldest = u64::MAX;
        for (sid, value) in self.index.iter().zip(values) {
            let value = value.ok_or(CreateSessionError::IndexInconsistent)?;
            let session = UserSession::from_bytes(value.as_ref())?;
            if session.user_id != self.config.user_id || session.sid != *sid {
                return Err(CreateSessionError::IndexInconsistent);
            }
            let cut = self
                .config
                .cutoff
                .is_some_and(|cutoff| session.created_at < cutoff);
            if session.revoked || cut || session.expires_at <= self.config.time {
                stale.insert(session.sid);
            } else {
                oldest = oldest.min(session.created_at);
                active.insert(session.sid);
            }
        }
        if active.len() >= MAX_USER_SESSIONS {
            return Err(CreateSessionError::LimitReached { oldest });
        }
        active.insert(self.sid.clone());
        self.index = active;
        if stale.is_empty() {
            return self.write_session();
        }
        self.state = Stage::DeleteStale;
        Ok(Next::Call(IoCall::Delete(
            stale
                .into_iter()
                .map(|sid| (USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into()))
                .collect(),
        )))
    }

    fn write_session(&mut self) -> SessionTurn {
        let session = self
            .session
            .as_ref()
            .ok_or(CreateSessionError::NotFinished)?;
        let session_bytes = session.to_bytes()?;
        let index_bytes = encode_index(&self.index)?;
        let mut writes = vec![
            (
                USER_SESSION_KEYSPACE.to_string(),
                self.sid.as_bytes().into(),
                session_bytes.into(),
            ),
            (
                USER_OWNER_KEYSPACE.to_string(),
                owner_key(self.config.user_id),
                index_bytes,
            ),
        ];
        if self.config.restrictions.is_some() {
            let (actor, group_id) = self.issuer.unwrap_or((self.config.user_id, None));
            let mut audit = AdminAuditRecord::new(
                AdminAuditAction::RestrictedTokenIssued,
                actor,
                self.config.user_id,
            );
            (audit.group_id, audit.session_id) = (group_id, Some(self.sid.clone()));
            writes.extend(audit.entries()?);
        }
        self.state = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for CreateSessionOperation {
    type Output = CreatedSession;
    type Error = CreateSessionError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> SessionTurn {
        self.prepare_session()?;
        self.state = Stage::Index;
        Ok(Next::Call(IoCall::Read(vec![(
            USER_OWNER_KEYSPACE.to_string(),
            owner_key(self.config.user_id),
        )])))
    }

    fn next(&mut self, result: IoResult) -> SessionTurn {
        match self.state {
            Stage::Index => {
                let [value] = values::<1>(result)?;
                self.handle_index(value)
            }
            Stage::Sessions => {
                let IoResult::Values(values) = result else {
                    return Err(CreateSessionError::NotFinished);
                };
                self.handle_sessions(values)
            }
            Stage::DeleteStale => {
                done(result)?;
                self.write_session()
            }
            Stage::Write => {
                done(result)?;
                let session = self
                    .session
                    .clone()
                    .ok_or(CreateSessionError::NotFinished)?;
                let token = self.token.take().ok_or(CreateSessionError::NotFinished)?;
                Ok(Next::Done(CreatedSession { token, session }))
            }
        }
    }
}

#[cfg(test)]
pub(super) mod pure_tests {
    use super::*;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::keyspaces::ADMIN_AUDIT_KEYSPACE;
    use aruna_core::structs::identity::auth::Permission;
    use aruna_core::types::Key;

    pub(crate) fn session_config() -> CreateSessionConfig {
        let signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        CreateSessionConfig {
            time: 100,
            expiry: 200,
            user_id: UserId::local(Ulid::from_bytes([4; 16]), realm_id),
            realm_id,
            node_capabilities: NodeCapabilities::management_node(signing_key).unwrap(),
            kind: SessionKind::Portal,
            label: None,
            name: None,
            restrictions: None,
            via: None,
            auth_time: None,
            cutoff: None,
        }
    }

    fn session_record(
        config: &CreateSessionConfig,
        sid: String,
        expires_at: u64,
        revoked: bool,
    ) -> UserSession {
        UserSession {
            sid,
            user_id: config.user_id,
            kind: SessionKind::Portal,
            label: None,
            created_at: 1,
            expires_at,
            token_hash: "a".repeat(64),
            revoked,
            via: None,
        }
    }

    fn begin_read(operation: &mut CreateSessionOperation, index: &BTreeSet<String>) {
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        let stored = IoResult::Values(vec![Some(encode_index(index).unwrap())]);
        operation.next(stored).unwrap();
    }

    fn rows(values: Vec<Option<Value>>) -> IoResult {
        IoResult::Values(values)
    }

    #[test]
    fn bounds_parent_expiry() {
        assert_eq!(bound_session_expiry(100, Some(600), 500).unwrap(), 500);
        assert_eq!(bound_session_expiry(100, None, 100_000).unwrap(), 86_500);
        assert_eq!(
            bound_session_expiry(100, Some(0), 500),
            Err(CreateSessionError::InvalidExpiry)
        );
    }

    #[test]
    fn prunes_stale_sessions() {
        let config = session_config();
        let expired_sid = Ulid::from_parts(1, 1).to_string();
        let revoked_sid = Ulid::from_parts(2, 2).to_string();
        let pending_sid = Ulid::from_parts(3, 3).to_string();
        let index = BTreeSet::from([expired_sid.clone(), revoked_sid.clone()]);
        let mut operation =
            CreateSessionOperation::new_with_sid(config.clone(), pending_sid.clone());
        begin_read(&mut operation, &index);

        let deleted = operation.next(rows(vec![
            Some(
                session_record(&config, expired_sid.clone(), 99, false)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
            Some(
                session_record(&config, revoked_sid.clone(), 200, true)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
            None,
        ]));
        let Ok(Next::Call(IoCall::Delete(deletes))) = deleted else {
            panic!("stale session records must be deleted");
        };
        assert_eq!(
            deletes
                .iter()
                .map(|(_, key)| key.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([expired_sid.as_bytes().into(), revoked_sid.as_bytes().into()])
        );

        let Ok(Next::Call(IoCall::Write(writes))) = operation.next(IoResult::Done) else {
            panic!("the new session and bounded index must be written together");
        };
        let stored_index = writes
            .iter()
            .find(|(key_space, _, _)| key_space == USER_OWNER_KEYSPACE)
            .map(|(_, _, value)| value)
            .unwrap();
        assert_eq!(
            decode_index(Some(stored_index)).unwrap(),
            BTreeSet::from([pending_sid])
        );
        assert!(matches!(
            operation.next(IoResult::Done),
            Ok(Next::Done(created)) if created.session.sid == operation.sid
        ));
    }

    fn written_rows(mut operation: CreateSessionOperation) -> Vec<(String, Key, Value)> {
        begin_read(&mut operation, &BTreeSet::new());
        let Ok(Next::Call(IoCall::Write(writes))) = operation.next(rows(vec![None])) else {
            panic!("the session must be written");
        };
        writes
    }

    #[test]
    fn restricted_session_audited() {
        let mut config = session_config();
        let rows = written_rows(CreateSessionOperation::new(config.clone()));
        assert!(rows.iter().all(|row| row.0 != ADMIN_AUDIT_KEYSPACE));

        config.restrictions = Some(vec![PathRestriction {
            pattern: "/**".to_string(),
            permission: Permission::READ,
        }]);
        let admin = UserId::local(Ulid::from_bytes([5; 16]), config.realm_id);
        let group = Ulid::from_bytes([6; 16]);
        let operation = CreateSessionOperation::new(config.clone()).issued_by(admin, Some(group));
        let sid = operation.sid.clone();
        let rows = written_rows(operation);
        let audit: Vec<_> = rows
            .iter()
            .filter(|row| row.0 == ADMIN_AUDIT_KEYSPACE)
            .collect();
        assert_eq!(audit.len(), 2, "one realm row and one group row");
        let record = AdminAuditRecord::from_bytes(&audit[1].2).unwrap();
        assert_eq!(record.action, AdminAuditAction::RestrictedTokenIssued);
        assert_eq!((record.actor, record.target), (admin, config.user_id));
        assert_eq!(record.group_id, Some(group));
        assert_eq!(record.session_id, Some(sid));
    }

    #[test]
    fn audits_owner_group() {
        // A service account's own restricted session reaches its group's audit rows.
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::user::validation::SERVICE_GROUP_ATTRIBUTE;
        let mut config = session_config();
        config.restrictions = Some(Vec::new());
        let group = Ulid::from_bytes([6; 16]);
        let owner = User {
            user_id: config.user_id,
            name: "service".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: [(SERVICE_GROUP_ATTRIBUTE.to_string(), group.to_string())].into(),
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id: config.user_id,
            realm_id: config.realm_id,
        };
        let mut operation = CreateSessionOperation::new(config.clone());
        begin_read(&mut operation, &BTreeSet::new());
        let owner = Some(owner.to_bytes(&actor).unwrap().into());
        let Ok(Next::Call(IoCall::Write(writes))) = operation.next(rows(vec![None, owner])) else {
            panic!("the session must be written");
        };
        let audit = writes
            .iter()
            .rfind(|row| row.0 == ADMIN_AUDIT_KEYSPACE)
            .unwrap();
        let record = AdminAuditRecord::from_bytes(&audit.2).unwrap();
        assert_eq!(
            (record.actor, record.group_id),
            (config.user_id, Some(group))
        );
    }

    #[test]
    fn rejects_session_limit() {
        // At the limit the oldest active session is named; past the user's cutoff the same
        // sessions are stale and the new session is created.
        let config = session_config();
        let pending_sid = Ulid::from_parts(1_000, 1_000).to_string();
        let mut index = BTreeSet::new();
        let mut values = Vec::new();
        for value in 0..MAX_USER_SESSIONS {
            let sid = Ulid::from_parts(value as u64, value as u128).to_string();
            index.insert(sid.clone());
            values.push(Some(
                session_record(&config, sid, 200, false)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ));
        }
        values.push(None);
        let mut operation =
            CreateSessionOperation::new_with_sid(config.clone(), pending_sid.clone());
        begin_read(&mut operation, &index);
        assert_eq!(
            operation.next(rows(values.clone())).err(),
            Some(CreateSessionError::LimitReached { oldest: 1 })
        );

        let cut = CreateSessionConfig {
            cutoff: Some(2),
            ..config
        };
        let mut operation = CreateSessionOperation::new_with_sid(cut, pending_sid);
        begin_read(&mut operation, &index);
        assert!(matches!(
            operation.next(rows(values)),
            Ok(Next::Call(IoCall::Delete(deletes))) if deletes.len() == MAX_USER_SESSIONS
        ));
    }
}
