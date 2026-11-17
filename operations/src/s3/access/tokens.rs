//! Pages through the tokens this node holds that grant access to one group, for its admins.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "token_list.rs"]
pub mod list;
#[path = "token_revoke.rs"]
pub mod revoke;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::keyspaces::{
    S3_SESSION_KEYSPACE, USER_ACCESS_KEYSPACE, USER_KEYSPACE, USER_OWNER_KEYSPACE,
    USER_SESSION_KEYSPACE,
};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::s3_session::{S3Session, SESSION_ACCESS_PREFIX};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::{GroupId, Key, Value};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use ulid::Ulid;

use crate::s3::session::S3SessionError;
use crate::session::index::{decode_index, owner_key};

const PAGE: usize = 256;
/// Storage pages one listing request reads before it returns a cursor, so a sparse group pages
/// in bounded steps; each keyspace change may add one more page.
const SCAN_PAGES: usize = 4;

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
    #[error(transparent)]
    S3Session(#[from] S3SessionError),
    #[error("caller does not administer this group")]
    Refused,
    #[error("this node holds no such token of the group")]
    NotFound,
    /// Only the account-wide cutoff ends them, and only a user administrator may move it.
    #[error("a service account's bearer sessions are not confined to this group")]
    NotConfined,
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
    #[error("group token storage failed: {0}")]
    Io(IoError),
}

impl From<IoError> for GroupTokensError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanStep {
    Config,
    Users,
    Owner,
    Sessions,
    Credentials,
    S3Sessions,
}

/// Where a listing page stopped: the keyspace and the last key handled there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenCursor {
    step: ScanStep,
    after: Key,
}

impl TokenCursor {
    pub fn to_bytes(&self) -> Vec<u8> {
        let step = match self.step {
            ScanStep::Credentials => 1,
            ScanStep::S3Sessions => 2,
            _ => 0,
        };
        let mut bytes = vec![step];
        bytes.extend_from_slice(self.after.as_ref());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (step, after) = bytes.split_first()?;
        let step = match step {
            0 => ScanStep::Users,
            1 => ScanStep::Credentials,
            2 => ScanStep::S3Sessions,
            _ => return None,
        };
        (!after.is_empty()).then(|| Self {
            step,
            after: after.to_vec().into(),
        })
    }
}

/// Reads one page of the group's tokens: the sessions of its service accounts, then the S3
/// credentials and S3 sessions of this node bound to it, stopping once `limit` are found or the
/// scan budget is spent.
#[derive(Debug, PartialEq)]
pub(crate) struct TokenScan {
    realm_id: RealmId,
    group_id: GroupId,
    now: u64,
    limit: usize,
    start: Option<TokenCursor>,
    step: ScanStep,
    config: Option<RealmConfigDocument>,
    /// Service accounts of the current user page still to read, last first.
    accounts: Vec<(Key, UserId)>,
    /// The last key of the current user page, and whether another page follows.
    page_end: Option<(Key, bool)>,
    current: Option<(Key, UserId)>,
    /// The session ids of the current account, in the order they were read.
    session_ids: Vec<String>,
    /// Storage pages read so far by this request.
    pages: usize,
    pub(crate) tokens: Vec<GroupToken>,
    pub(crate) next: Option<TokenCursor>,
}

impl TokenScan {
    pub(crate) fn new(
        (realm_id, group_id): (RealmId, GroupId),
        now: u64,
        start: Option<TokenCursor>,
        limit: usize,
    ) -> Self {
        Self {
            realm_id,
            group_id,
            now,
            limit: limit.max(1),
            start,
            step: ScanStep::Config,
            config: None,
            accounts: Vec::new(),
            page_end: None,
            current: None,
            session_ids: Vec::new(),
            pages: 0,
            tokens: Vec::new(),
            next: None,
        }
    }

    pub(crate) fn start(&mut self) -> IoCall {
        self.step = ScanStep::Config;
        config_read(self.realm_id)
    }

    fn iter(&mut self, step: ScanStep, after: Option<Key>) -> IoCall {
        self.step = step;
        self.pages += 1;
        let (space, prefix) = match step {
            ScanStep::Credentials => (USER_ACCESS_KEYSPACE, Key::from(Vec::<u8>::new())),
            ScanStep::S3Sessions => (S3_SESSION_KEYSPACE, Key::from(Vec::<u8>::new())),
            _ => (USER_KEYSPACE, UserId::storage_prefix(self.realm_id)),
        };
        IoCall::Page {
            space: space.to_string(),
            prefix,
            after,
            limit: PAGE,
        }
    }

    fn full(&self) -> bool {
        self.tokens.len() >= self.limit
    }

    fn spent(&self) -> bool {
        self.pages >= SCAN_PAGES
    }

    fn stop(&mut self, step: ScanStep, after: Key) -> Option<IoCall> {
        self.next = Some(TokenCursor { step, after });
        None
    }

    /// The next call, or `None` once the page is full or every keyspace was read.
    pub(crate) fn step(&mut self, result: IoResult) -> Result<Option<IoCall>, GroupTokensError> {
        match self.step {
            ScanStep::Config => {
                self.config = parse_config(result.value()?)?;
                let (step, after) = match self.start.take() {
                    Some(cursor) => (cursor.step, Some(cursor.after)),
                    None => (ScanStep::Users, None),
                };
                Ok(Some(self.iter(step, after)))
            }
            ScanStep::Users => {
                let (values, more) = result.page()?;
                self.page_end = values.last().map(|(key, _)| (key.clone(), more.is_some()));
                for (key, value) in values {
                    let user = User::from_bytes(&value)?;
                    if user.service_group() == Some(self.group_id) {
                        self.accounts.push((key, user.user_id));
                    }
                }
                self.accounts.reverse();
                Ok(self.next_account())
            }
            ScanStep::Owner => {
                let index = decode_index(result.value()?.as_ref())?;
                self.session_ids = index.into_iter().collect();
                if self.session_ids.is_empty() {
                    return Ok(self.after_account());
                }
                self.step = ScanStep::Sessions;
                let reads = (self.session_ids.iter())
                    .map(|sid| (USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into()))
                    .collect();
                Ok(Some(IoCall::Read(reads)))
            }
            ScanStep::Sessions => {
                let account = self.current.as_ref().map(|(_, account)| *account);
                let ids = std::mem::take(&mut self.session_ids);
                for (sid, value) in ids.iter().zip(result.values()?) {
                    let Some(value) = value else { continue };
                    let session = UserSession::from_bytes(&value)?;
                    if &session.sid == sid && Some(session.user_id) == account {
                        let token = self.cutoffs().session(session);
                        self.tokens.push(token);
                    }
                }
                Ok(self.after_account())
            }
            ScanStep::Credentials | ScanStep::S3Sessions => self.walk(result.page()?),
        }
    }

    fn next_account(&mut self) -> Option<IoCall> {
        if let Some((key, account)) = self.accounts.pop() {
            self.current = Some((key, account));
            self.step = ScanStep::Owner;
            return Some(IoCall::Read(vec![(
                USER_OWNER_KEYSPACE.to_string(),
                owner_key(account),
            )]));
        }
        match self.page_end.take() {
            Some((last, _)) if self.full() => self.stop(ScanStep::Users, last),
            Some((last, true)) if self.spent() => self.stop(ScanStep::Users, last),
            Some((last, true)) => Some(self.iter(ScanStep::Users, Some(last))),
            _ => Some(self.iter(ScanStep::Credentials, None)),
        }
    }

    /// An account's sessions are listed whole, so a page may exceed `limit` by up to one account.
    fn after_account(&mut self) -> Option<IoCall> {
        match self.current.take() {
            Some((key, _)) if self.full() => self.stop(ScanStep::Users, key),
            _ => self.next_account(),
        }
    }

    fn walk(&mut self, (values, more): Page) -> Result<Option<IoCall>, GroupTokensError> {
        let step = self.step;
        for (key, value) in values {
            let token = match step {
                ScanStep::Credentials => {
                    let access = UserAccess::from_bytes(&value)?;
                    (access.group_id == self.group_id).then(|| self.cutoffs().credential(access))
                }
                _ => {
                    let session = S3Session::from_bytes(&value)?;
                    (session.group_id == self.group_id).then(|| self.cutoffs().s3_session(session))
                }
            };
            self.tokens.extend(token);
            if self.full() {
                return Ok(self.stop(step, key));
            }
        }
        Ok(match (more, step) {
            (Some(start), _) if self.spent() => self.stop(step, start),
            (Some(start), _) => Some(self.iter(step, Some(start))),
            (None, ScanStep::Credentials) => Some(self.iter(ScanStep::S3Sessions, None)),
            (None, _) => None,
        })
    }

    fn cutoffs(&self) -> Cutoffs<'_> {
        Cutoffs {
            config: self.config.as_ref(),
            now: self.now,
        }
    }
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

/// Reads the realm configuration that holds the owners' revocation cutoffs.
pub(crate) fn config_read(realm_id: RealmId) -> IoCall {
    let target = DocumentTarget::RealmConfig { realm_id };
    read_row(target.storage_keyspace(), target.storage_key())
}

pub(crate) fn parse_config(
    value: Option<Value>,
) -> Result<Option<RealmConfigDocument>, GroupTokensError> {
    Ok(value
        .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
        .transpose()?)
}

/// Turns stored records into listed tokens, revoked when issued before the owner's cutoff.
pub(crate) struct Cutoffs<'a> {
    pub(crate) config: Option<&'a RealmConfigDocument>,
    pub(crate) now: u64,
}

impl Cutoffs<'_> {
    fn status(
        &self,
        owner: &UserId,
        issued: Option<u64>,
        revoked: bool,
        expiry: u64,
    ) -> TokenStatus {
        let cutoff = self
            .config
            .and_then(|config| config.user_cutoff(owner, self.now));
        if revoked || cutoff.is_some_and(|cutoff| issued.unwrap_or(0) < cutoff) {
            TokenStatus::Revoked
        } else if expiry <= self.now {
            TokenStatus::Expired
        } else {
            TokenStatus::Active
        }
    }

    pub(crate) fn session(&self, session: UserSession) -> GroupToken {
        let status = self.status(
            &session.user_id,
            Some(session.created_at),
            session.revoked,
            session.expires_at,
        );
        GroupToken {
            kind: TokenKind::Session,
            id: session.sid,
            owner: session.user_id,
            created_at: Some(session.created_at),
            expires_at: session.expires_at,
            last_used_at: None,
            status,
            issued_by: None,
        }
    }

    pub(crate) fn credential(&self, access: UserAccess) -> GroupToken {
        let created_at = key_time(&access.access_key);
        let expires_at = unix_secs(access.expiry);
        let revoked = access.revoked_at.is_some();
        let status = self.status(&access.user_identity, created_at, revoked, expires_at);
        GroupToken {
            kind: TokenKind::S3Credential,
            id: access.access_key,
            owner: access.user_identity,
            created_at,
            expires_at,
            last_used_at: None,
            status,
            issued_by: Some(access.issued_by),
        }
    }

    pub(crate) fn s3_session(&self, session: S3Session) -> GroupToken {
        let key_id = session.access_key.strip_prefix(SESSION_ACCESS_PREFIX);
        let created_at = key_id.and_then(key_time);
        let expires_at = unix_secs(session.expiry);
        let status = self.status(&session.user_identity, created_at, false, expires_at);
        GroupToken {
            kind: TokenKind::S3Session,
            id: session.access_key,
            owner: session.user_identity,
            created_at,
            expires_at,
            last_used_at: session.last_used_at.map(unix_secs),
            status,
            issued_by: Some(session.issued_by),
        }
    }
}

#[cfg(test)]
#[path = "tokens_tests.rs"]
mod tests;
