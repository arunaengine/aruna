//! Collects the tokens this node holds that grant access to one group, for its administrators.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "token_list.rs"]
pub mod list;
#[path = "token_revoke.rs"]
pub mod revoke;

use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_SESSION_KEYSPACE, USER_ACCESS_KEYSPACE, USER_KEYSPACE, USER_OWNER_KEYSPACE,
    USER_SESSION_KEYSPACE,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::s3_session::{S3Session, SESSION_ACCESS_PREFIX};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use ulid::Ulid;

use crate::session::index::{decode_index, owner_key};

const PAGE: usize = 256;

/// One page of rows and the key the next page starts after.
type Page = (Vec<(Key, Value)>, Option<Key>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenKind {
    /// A bearer session of one of the group's service accounts.
    Session,
    S3Credential,
    S3Session,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenStatus {
    Active,
    Expired,
    Revoked,
}

/// One token held by this node; never carries a secret or a token hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupToken {
    pub kind: TokenKind,
    pub id: String,
    pub owner: UserId,
    /// Unix seconds; unknown when the id carries no time.
    pub created_at: Option<u64>,
    pub expires_at: u64,
    pub last_used_at: Option<u64>,
    pub status: TokenStatus,
    pub issued_by: Option<[u8; 32]>,
}

#[derive(Debug, Error, PartialEq)]
pub enum GroupTokensError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error("caller does not administer this group")]
    Refused,
    #[error("this node holds no such token of the group")]
    NotFound,
    #[error("token revocation failed: {0}")]
    Revocation(String),
    #[error("group token operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanStep {
    Config,
    Users,
    Owners,
    Sessions,
    Credentials,
    S3Sessions,
}

/// Reads the realm cutoffs, the group's service account sessions, then every S3 credential and
/// S3 session of this node, keeping those bound to the group.
#[derive(Debug, PartialEq)]
pub(crate) struct TokenScan {
    realm_id: RealmId,
    group_id: GroupId,
    now: u64,
    step: ScanStep,
    config: Option<RealmConfigDocument>,
    pub(crate) accounts: Vec<UserId>,
    session_ids: Vec<String>,
    pub(crate) tokens: Vec<GroupToken>,
}

fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn key_time(key_id: &str) -> Option<u64> {
    Ulid::from_string(key_id)
        .ok()
        .map(|id| id.timestamp_ms() / 1000)
}

impl TokenScan {
    pub(crate) fn new(realm_id: RealmId, group_id: GroupId, now: u64) -> Self {
        Self {
            realm_id,
            group_id,
            now,
            step: ScanStep::Config,
            config: None,
            accounts: Vec::new(),
            session_ids: Vec::new(),
            tokens: Vec::new(),
        }
    }

    pub(crate) fn start(&mut self) -> Effects {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        self.step = ScanStep::Config;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })]
    }

    fn iter(&mut self, step: ScanStep, key_space: &str, start: Option<Key>) -> Effects {
        self.step = step;
        let prefix = (step == ScanStep::Users).then(|| UserId::storage_prefix(self.realm_id));
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix,
            start: start.map(IterStart::After),
            limit: PAGE,
            txn_id: None,
        })]
    }

    /// A token issued before its owner's cutoff is revoked like a bearer token issued before it.
    fn status(
        &self,
        owner: &UserId,
        issued: Option<u64>,
        revoked: bool,
        expiry: u64,
    ) -> TokenStatus {
        let cutoff = self
            .config
            .as_ref()
            .and_then(|config| config.user_cutoff(owner, self.now));
        if revoked || cutoff.is_some_and(|cutoff| issued.unwrap_or(0) < cutoff) {
            TokenStatus::Revoked
        } else if expiry <= self.now {
            TokenStatus::Expired
        } else {
            TokenStatus::Active
        }
    }

    fn unexpected(&self, expected: &'static str, event: Event) -> GroupTokensError {
        GroupTokensError::UnexpectedEvent {
            state: format!("{:?}", self.step),
            expected,
            got: format!("{event:?}"),
        }
    }

    /// The next effects, or `None` once every keyspace was read.
    pub(crate) fn step(&mut self, event: Event) -> Result<Option<Effects>, GroupTokensError> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(error.into());
        }
        match self.step {
            ScanStep::Config => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return Err(self.unexpected("realm config read", event));
                };
                self.config = value
                    .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
                    .transpose()?;
                Ok(Some(self.iter(ScanStep::Users, USER_KEYSPACE, None)))
            }
            ScanStep::Users => {
                let (values, next) = self.page(event, "user page")?;
                for (_, value) in values {
                    let user = User::from_bytes(&value)?;
                    if user.service_group() == Some(self.group_id) {
                        self.accounts.push(user.user_id);
                    }
                }
                Ok(Some(match next {
                    Some(start) => self.iter(ScanStep::Users, USER_KEYSPACE, Some(start)),
                    None => self.read_owners(),
                }))
            }
            ScanStep::Owners => {
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return Err(self.unexpected("session owner index read", event));
                };
                for (_, value) in values {
                    self.session_ids.extend(decode_index(value.as_ref())?);
                }
                Ok(Some(self.read_sessions()))
            }
            ScanStep::Sessions => {
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return Err(self.unexpected("session read", event));
                };
                for (key, value) in values {
                    let Some(value) = value else { continue };
                    let session = UserSession::from_bytes(&value)?;
                    if session.sid.as_bytes() == key.as_ref()
                        && self.accounts.contains(&session.user_id)
                    {
                        self.push_session(session);
                    }
                }
                Ok(Some(self.iter(
                    ScanStep::Credentials,
                    USER_ACCESS_KEYSPACE,
                    None,
                )))
            }
            ScanStep::Credentials => {
                let (values, next) = self.page(event, "credential page")?;
                for (_, value) in values {
                    let access = UserAccess::from_bytes(&value)?;
                    if access.group_id == self.group_id {
                        self.push_credential(access);
                    }
                }
                Ok(Some(match next {
                    Some(start) => {
                        self.iter(ScanStep::Credentials, USER_ACCESS_KEYSPACE, Some(start))
                    }
                    None => self.iter(ScanStep::S3Sessions, S3_SESSION_KEYSPACE, None),
                }))
            }
            ScanStep::S3Sessions => {
                let (values, next) = self.page(event, "S3 session page")?;
                for (_, value) in values {
                    let session = S3Session::from_bytes(&value)?;
                    if session.group_id == self.group_id {
                        self.push_s3_session(session);
                    }
                }
                Ok(next
                    .map(|start| self.iter(ScanStep::S3Sessions, S3_SESSION_KEYSPACE, Some(start))))
            }
        }
    }

    fn page(&self, event: Event, expected: &'static str) -> Result<Page, GroupTokensError> {
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => Ok((values, next_start_after)),
            other => Err(self.unexpected(expected, other)),
        }
    }

    fn read_owners(&mut self) -> Effects {
        if self.accounts.is_empty() {
            return self.iter(ScanStep::Credentials, USER_ACCESS_KEYSPACE, None);
        }
        self.step = ScanStep::Owners;
        let reads = self
            .accounts
            .iter()
            .map(|account| (USER_OWNER_KEYSPACE.to_string(), owner_key(*account)))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn read_sessions(&mut self) -> Effects {
        if self.session_ids.is_empty() {
            return self.iter(ScanStep::Credentials, USER_ACCESS_KEYSPACE, None);
        }
        self.step = ScanStep::Sessions;
        let reads = self
            .session_ids
            .iter()
            .map(|sid| (USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into()))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn push_session(&mut self, session: UserSession) {
        let status = self.status(
            &session.user_id,
            Some(session.created_at),
            session.revoked,
            session.expires_at,
        );
        self.tokens.push(GroupToken {
            kind: TokenKind::Session,
            id: session.sid,
            owner: session.user_id,
            created_at: Some(session.created_at),
            expires_at: session.expires_at,
            last_used_at: None,
            status,
            issued_by: None,
        });
    }

    fn push_credential(&mut self, access: UserAccess) {
        let created_at = key_time(&access.access_key);
        let expires_at = unix_secs(access.expiry);
        let revoked = access.revoked_at.is_some();
        let status = self.status(&access.user_identity, created_at, revoked, expires_at);
        self.tokens.push(GroupToken {
            kind: TokenKind::S3Credential,
            id: access.access_key,
            owner: access.user_identity,
            created_at,
            expires_at,
            last_used_at: None,
            status,
            issued_by: Some(access.issued_by),
        });
    }

    fn push_s3_session(&mut self, session: S3Session) {
        let key_id = session.access_key.strip_prefix(SESSION_ACCESS_PREFIX);
        let created_at = key_id.and_then(key_time);
        let expires_at = unix_secs(session.expiry);
        let status = self.status(&session.user_identity, created_at, false, expires_at);
        self.tokens.push(GroupToken {
            kind: TokenKind::S3Session,
            id: session.access_key,
            owner: session.user_identity,
            created_at,
            expires_at,
            last_used_at: session.last_used_at.map(unix_secs),
            status,
            issued_by: Some(session.issued_by),
        });
    }
}

#[cfg(test)]
#[path = "tokens_tests.rs"]
mod tests;
