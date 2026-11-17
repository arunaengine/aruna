//! Revokes one group token, or all group-bound access of one member, held by this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::keyspaces::{
    ACCESS_OWNER_KEYSPACE, S3_SESSION_KEYSPACE, SESSION_OWNER_KEYSPACE, USER_ACCESS_KEYSPACE,
    USER_KEYSPACE, USER_OWNER_KEYSPACE, USER_SESSION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::s3_session::S3Session;
use aruna_core::structs::identity::user::User;
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::{GroupId, Key, Value};

use super::list::{admin_check, authorized};
use super::{Cutoffs, GroupToken, GroupTokensError, TokenKind, TokenStatus};
use super::{config_read, parse_config};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::revoke_token::{RevokeTokenError, RevokeTokenOperation, owner_cutoff};
use crate::s3::access::revoke::{RevokeUserError, RevokeUserOperation};
use crate::s3::session::{RevokeS3Config, RevokeS3Operation, S3SessionError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RevokeTarget {
    /// An S3 credential or S3 session bound to the group; bearer sessions are refused.
    Token { kind: TokenKind, id: String },
    /// Every group-bound S3 token of one member. A service account of the group is ended
    /// realm-wide instead, which needs `account_wide` and WRITE on its user admin path.
    Member { user: UserId, account_wide: bool },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RevokeTokensConfig {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    pub target: RevokeTarget,
    pub now: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Revocation {
    /// Active tokens of this node that were revoked.
    pub revoked: usize,
    /// How many of them were S3 credentials.
    pub credentials: usize,
    /// The member is a service account of the group and lost every token realm-wide.
    pub all_tokens: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RevokeTokensState {
    Authorize,
    Config,
    Lookup,
    Records,
    UserAdmin,
    /// A revocation of a token of this kind runs as a child.
    Revoke(TokenKind),
    Cutoff,
}

type RevokeStep = Result<Next<Revocation>, GroupTokensError>;

/// Reads tokens by id or through the member's owner indexes, never by scanning the group.
#[derive(Debug, PartialEq)]
pub struct RevokeTokensOperation {
    config: RevokeTokensConfig,
    realm_config: Option<RealmConfigDocument>,
    service_account: bool,
    /// The kind and id of each record read, in read order.
    kinds: Vec<(TokenKind, String)>,
    pending: Vec<GroupToken>,
    summary: Revocation,
    state: RevokeTokensState,
}

impl RevokeTokensOperation {
    pub fn new(config: RevokeTokensConfig) -> Self {
        Self {
            config,
            realm_config: None,
            service_account: false,
            kinds: Vec::new(),
            pending: Vec::new(),
            summary: Revocation::default(),
            state: RevokeTokensState::Authorize,
        }
    }

    fn finish(&mut self) -> Next<Revocation> {
        Next::Done(std::mem::take(&mut self.summary))
    }

    fn cutoffs(&self) -> Cutoffs<'_> {
        Cutoffs {
            config: self.realm_config.as_ref(),
            now: self.config.now,
        }
    }

    /// Reads the named token, or the member's record and owner indexes.
    fn lookup(&mut self) -> IoCall {
        self.state = RevokeTokensState::Lookup;
        let group_id = self.config.group_id;
        match &self.config.target {
            RevokeTarget::Token { kind, id } => {
                let key_space = match kind {
                    TokenKind::S3Credential => USER_ACCESS_KEYSPACE,
                    _ => S3_SESSION_KEYSPACE,
                };
                IoCall::Read(vec![(key_space.to_string(), id.as_bytes().into())])
            }
            RevokeTarget::Member { user, .. } => IoCall::Read(vec![
                (USER_KEYSPACE.to_string(), user.to_bytes().into()),
                (
                    ACCESS_OWNER_KEYSPACE.to_string(),
                    crate::s3::access::index::owner_key(*user),
                ),
                (
                    SESSION_OWNER_KEYSPACE.to_string(),
                    crate::s3::session::owner_key(*user, group_id),
                ),
                (
                    USER_OWNER_KEYSPACE.to_string(),
                    crate::session::index::owner_key(*user),
                ),
            ]),
        }
    }

    fn handle_lookup(&mut self, values: Vec<Option<Value>>) -> RevokeStep {
        let target = self.config.target.clone();
        let RevokeTarget::Member { user, account_wide } = target else {
            let value = values.into_iter().next().flatten();
            let token = value.map(|value| self.decode(value.as_ref())).transpose()?;
            let token = token.flatten().ok_or(GroupTokensError::NotFound)?;
            self.pending
                .extend((token.status == TokenStatus::Active).then_some(token));
            return self.next_token();
        };
        let mut values = values.into_iter();
        let record = values.next().flatten();
        let account = record.map(|value| User::from_bytes(&value)).transpose()?;
        self.service_account =
            account.is_some_and(|user| user.service_group() == Some(self.config.group_id));
        if self.service_account && !account_wide {
            return Err(GroupTokensError::NotConfined);
        }
        let credentials = crate::s3::access::index::decode_index(values.next().flatten().as_ref())?;
        let s3_sessions = crate::s3::session::decode_index(values.next().flatten().as_ref())?;
        let sessions = match self.service_account {
            true => crate::session::index::decode_index(values.next().flatten().as_ref())?,
            false => Default::default(),
        };
        let mut reads: Vec<(String, Key)> = Vec::new();
        let records = [
            (TokenKind::S3Credential, USER_ACCESS_KEYSPACE, credentials),
            (TokenKind::S3Session, S3_SESSION_KEYSPACE, s3_sessions),
            (TokenKind::Session, USER_SESSION_KEYSPACE, sessions),
        ];
        for (kind, key_space, ids) in records {
            for id in ids {
                reads.push((key_space.to_string(), id.as_bytes().into()));
                self.kinds.push((kind, id));
            }
        }
        if reads.is_empty() {
            return self.after_records(user);
        }
        self.state = RevokeTokensState::Records;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// The token a stored record names when it is bound to this group, keyed by its kind.
    fn decode(&self, value: &[u8]) -> Result<Option<GroupToken>, GroupTokensError> {
        let group_id = self.config.group_id;
        let RevokeTarget::Token { kind, .. } = &self.config.target else {
            return Ok(None);
        };
        Ok(match kind {
            TokenKind::S3Credential => {
                let access = UserAccess::from_bytes(value)?;
                (access.group_id == group_id).then(|| self.cutoffs().credential(access))
            }
            _ => {
                let session = S3Session::from_bytes(value)?;
                (session.group_id == group_id).then(|| self.cutoffs().s3_session(session))
            }
        })
    }

    fn handle_records(&mut self, values: Vec<Option<Value>>) -> RevokeStep {
        let RevokeTarget::Member { user, .. } = self.config.target else {
            return Err(GroupTokensError::NotFinished);
        };
        let group_id = self.config.group_id;
        if values.len() != self.kinds.len() {
            return Err(GroupTokensError::UnexpectedEvent {
                state: "Records".to_string(),
                expected: "one value per token read",
                got: format!("{} values for {} reads", values.len(), self.kinds.len()),
            });
        }
        let kinds = std::mem::take(&mut self.kinds);
        for (value, (kind, id)) in values.into_iter().zip(kinds) {
            let Some(value) = value else { continue };
            let token = match kind {
                TokenKind::S3Credential => {
                    let access = UserAccess::from_bytes(&value)?;
                    let bound = access.group_id == group_id && access.access_key == id;
                    bound.then(|| self.cutoffs().credential(access))
                }
                TokenKind::S3Session => {
                    let session = S3Session::from_bytes(&value)?;
                    let bound = session.group_id == group_id && session.access_key == id;
                    bound.then(|| self.cutoffs().s3_session(session))
                }
                TokenKind::Session => {
                    let session = UserSession::from_bytes(&value)?;
                    (session.sid == id).then(|| self.cutoffs().session(session))
                }
            };
            self.pending.extend(
                token.filter(|token| token.owner == user && token.status == TokenStatus::Active),
            );
        }
        self.after_records(user)
    }

    fn after_records(&mut self, user: UserId) -> RevokeStep {
        if !self.service_account {
            return self.next_token();
        }
        self.summary.revoked = std::mem::take(&mut self.pending).len();
        self.summary.all_tokens = true;
        self.state = RevokeTokensState::UserAdmin;
        let auth = &self.config.auth_context;
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!("/{}/admin/u/{user}", auth.realm_id),
            required_permission: Permission::WRITE,
        });
        Ok(Next::Call(IoCall::child(check)))
    }

    fn cutoff(&mut self, member: UserId) -> Next<Revocation> {
        self.state = RevokeTokensState::Cutoff;
        let now = self.config.now;
        let config = owner_cutoff(self.config.actor.clone(), member, now, now);
        Next::Call(IoCall::child(RevokeTokenOperation::new(config)))
    }

    fn next_token(&mut self) -> RevokeStep {
        let Some(token) = self.pending.pop() else {
            return Ok(self.finish());
        };
        self.state = RevokeTokensState::Revoke(token.kind);
        self.summary.revoked += 1;
        let call = match token.kind {
            TokenKind::Session => return Err(GroupTokensError::NotConfined),
            TokenKind::S3Credential => {
                self.summary.credentials += 1;
                IoCall::child(RevokeUserOperation::new(token.id))
            }
            TokenKind::S3Session => IoCall::child(RevokeS3Operation::new(RevokeS3Config {
                access_key: token.id,
                user_identity: token.owner,
                issued_by: token.issued_by.unwrap_or_default(),
            })),
        };
        Ok(Next::Call(call))
    }

    fn user_admin(&mut self, result: IoResult) -> RevokeStep {
        let RevokeTarget::Member { user, .. } = self.config.target else {
            return Err(GroupTokensError::NotFinished);
        };
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => Ok(self.cutoff(user)),
            Ok(false) => Err(GroupTokensError::NotConfined),
            Err(error) => Err(error.into()),
        }
    }
}

/// The outcome of a revocation child as the reason it failed.
fn revoked<T, E: std::fmt::Display>(
    result: Result<Result<T, E>, aruna_core::calls::IoError>,
) -> Result<(), GroupTokensError> {
    result?.map_err(|error| GroupTokensError::Revocation(error.to_string()))?;
    Ok(())
}

impl Operation for RevokeTokensOperation {
    type Output = Revocation;
    type Error = GroupTokensError;

    fn first(&mut self) -> RevokeStep {
        let auth = &self.config.auth_context;
        if auth.user_id != self.config.actor.user_id || auth.realm_id != self.config.actor.realm_id
        {
            return Err(GroupTokensError::Refused);
        }
        Ok(Next::Call(admin_check(auth, self.config.group_id)))
    }

    fn next(&mut self, result: IoResult) -> RevokeStep {
        match self.state {
            RevokeTokensState::Authorize => {
                authorized(result)?;
                if let RevokeTarget::Token {
                    kind: TokenKind::Session,
                    ..
                } = self.config.target
                {
                    return Err(GroupTokensError::NotConfined);
                }
                self.state = RevokeTokensState::Config;
                Ok(Next::Call(config_read(self.config.auth_context.realm_id)))
            }
            RevokeTokensState::Config => {
                self.realm_config = parse_config(result.value()?)?;
                Ok(Next::Call(self.lookup()))
            }
            RevokeTokensState::Lookup => self.handle_lookup(result.values()?),
            RevokeTokensState::Records => self.handle_records(result.values()?),
            RevokeTokensState::UserAdmin => self.user_admin(result),
            RevokeTokensState::Revoke(TokenKind::S3Credential) => {
                revoked(result.child::<UserAccess, RevokeUserError>())?;
                self.next_token()
            }
            RevokeTokensState::Revoke(_) => {
                revoked(result.child::<(), S3SessionError>())?;
                self.next_token()
            }
            RevokeTokensState::Cutoff => {
                revoked(result.child::<RealmConfigDocument, RevokeTokenError>())?;
                Ok(self.finish())
            }
        }
    }
}
