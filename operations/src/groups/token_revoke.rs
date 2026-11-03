//! Revokes one group token, or all group-bound access of one member, held by this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    ACCESS_OWNER_KEYSPACE, S3_SESSION_KEYSPACE, SESSION_OWNER_KEYSPACE, USER_ACCESS_KEYSPACE,
    USER_KEYSPACE, USER_OWNER_KEYSPACE, USER_SESSION_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::s3_session::S3Session;
use aruna_core::structs::identity::user::User;
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;

use super::list::{admin_check, authorized};
use super::{Cutoffs, GroupToken, GroupTokensError, TokenKind, TokenStatus};
use super::{parse_config, read_config};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::revoke_token::RevokeTokenOperation;
use crate::groups::admin_check::authorization_event;
use crate::s3::access::revoke::RevokeUserOperation;
use crate::s3::session::{RevokeS3Config, RevokeS3Operation};
use crate::session::revoke::owner_cutoff;

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
    Init,
    Authorize,
    Config,
    Lookup,
    Records,
    UserAdmin,
    Revoke,
    Cutoff,
    Finish,
    Error,
}

/// Reads tokens by id or through the member's owner indexes, never by scanning the group.
#[derive(Debug, PartialEq)]
pub struct RevokeTokensOperation {
    config: RevokeTokensConfig,
    realm_config: Option<RealmConfigDocument>,
    service_account: bool,
    /// The kind of each record read, in read order.
    kinds: Vec<TokenKind>,
    pending: Vec<GroupToken>,
    summary: Revocation,
    state: RevokeTokensState,
    output: Option<Result<Revocation, GroupTokensError>>,
}

fn revoked_event<T, E: std::fmt::Display>(result: Result<T, E>) -> Event {
    Event::SubOperation(SubOperationEvent::TokenRevoked {
        result: result.map(|_| ()).map_err(|error| error.to_string()),
    })
}

fn batch_read(reads: Vec<(String, Key)>) -> Effects {
    smallvec![Effect::Storage(StorageEffect::BatchRead {
        reads,
        txn_id: None,
    })]
}

fn batch_values(event: Event) -> Result<Vec<(Key, Option<Value>)>, GroupTokensError> {
    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(GroupTokensError::UnexpectedEvent {
            state: "Lookup".to_string(),
            expected: "batch read result",
            got: format!("{other:?}"),
        }),
    }
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
            state: RevokeTokensState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GroupTokensError) -> Effects {
        self.state = RevokeTokensState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn finish(&mut self) -> Effects {
        self.state = RevokeTokensState::Finish;
        self.output = Some(Ok(std::mem::take(&mut self.summary)));
        smallvec![]
    }

    fn cutoffs(&self) -> Cutoffs<'_> {
        Cutoffs {
            config: self.realm_config.as_ref(),
            now: self.config.now,
        }
    }

    /// Reads the named token, or the member's record and owner indexes.
    fn lookup(&mut self) -> Effects {
        self.state = RevokeTokensState::Lookup;
        let group_id = self.config.group_id;
        match &self.config.target {
            RevokeTarget::Token { kind, id } => {
                let key_space = match kind {
                    TokenKind::S3Credential => USER_ACCESS_KEYSPACE,
                    _ => S3_SESSION_KEYSPACE,
                };
                batch_read(vec![(key_space.to_string(), id.as_bytes().into())])
            }
            RevokeTarget::Member { user, .. } => batch_read(vec![
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

    fn handle_lookup(&mut self, event: Event) -> Result<Effects, GroupTokensError> {
        let values = batch_values(event)?;
        let target = self.config.target.clone();
        let RevokeTarget::Member { user, account_wide } = target else {
            let value = values.into_iter().next().and_then(|(_, value)| value);
            let token = value.map(|value| self.decode(value.as_ref())).transpose()?;
            let token = token.flatten().ok_or(GroupTokensError::NotFound)?;
            self.pending
                .extend((token.status == TokenStatus::Active).then_some(token));
            return Ok(self.next());
        };
        let mut values = values.into_iter().map(|(_, value)| value);
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
                self.kinds.push(kind);
                reads.push((key_space.to_string(), id.as_bytes().into()));
            }
        }
        if reads.is_empty() {
            return Ok(self.after_records(user));
        }
        self.state = RevokeTokensState::Records;
        Ok(batch_read(reads))
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

    fn handle_records(&mut self, event: Event) -> Result<Effects, GroupTokensError> {
        let RevokeTarget::Member { user, .. } = self.config.target else {
            return Err(GroupTokensError::NotFinished);
        };
        let group_id = self.config.group_id;
        let values = batch_values(event)?;
        if values.len() != self.kinds.len() {
            return Err(GroupTokensError::UnexpectedEvent {
                state: "Records".to_string(),
                expected: "one value per token read",
                got: format!("{} values for {} reads", values.len(), self.kinds.len()),
            });
        }
        let kinds = std::mem::take(&mut self.kinds);
        for ((key, value), kind) in values.into_iter().zip(kinds) {
            let Some(value) = value else { continue };
            let token = match kind {
                TokenKind::S3Credential => {
                    let access = UserAccess::from_bytes(&value)?;
                    let bound =
                        access.group_id == group_id && access.access_key.as_bytes() == key.as_ref();
                    bound.then(|| self.cutoffs().credential(access))
                }
                TokenKind::S3Session => {
                    let session = S3Session::from_bytes(&value)?;
                    let bound = session.group_id == group_id
                        && session.access_key.as_bytes() == key.as_ref();
                    bound.then(|| self.cutoffs().s3_session(session))
                }
                TokenKind::Session => {
                    let session = UserSession::from_bytes(&value)?;
                    (session.sid.as_bytes() == key.as_ref())
                        .then(|| self.cutoffs().session(session))
                }
            };
            self.pending.extend(
                token.filter(|token| token.owner == user && token.status == TokenStatus::Active),
            );
        }
        Ok(self.after_records(user))
    }

    fn after_records(&mut self, user: UserId) -> Effects {
        if !self.service_account {
            return self.next();
        }
        self.summary.revoked = std::mem::take(&mut self.pending).len();
        self.summary.all_tokens = true;
        self.state = RevokeTokensState::UserAdmin;
        let auth = &self.config.auth_context;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth.clone(),
                path: format!("/{}/admin/u/{user}", auth.realm_id),
                required_permission: Permission::WRITE,
            }),
            authorization_event,
        ))]
    }

    fn cutoff(&mut self, member: UserId) -> Effects {
        self.state = RevokeTokensState::Cutoff;
        let now = self.config.now;
        let config = owner_cutoff(self.config.actor.clone(), member, now, now);
        smallvec![Effect::SubOperation(boxed_suboperation(
            RevokeTokenOperation::new(config),
            revoked_event,
        ))]
    }

    fn next(&mut self) -> Effects {
        let Some(token) = self.pending.pop() else {
            return self.finish();
        };
        self.state = RevokeTokensState::Revoke;
        self.summary.revoked += 1;
        let sub_operation = match token.kind {
            TokenKind::Session => return self.fail(GroupTokensError::NotConfined),
            TokenKind::S3Credential => {
                self.summary.credentials += 1;
                boxed_suboperation(RevokeUserOperation::new(token.id), revoked_event)
            }
            TokenKind::S3Session => boxed_suboperation(
                RevokeS3Operation::new(RevokeS3Config {
                    access_key: token.id,
                    user_identity: token.owner,
                    issued_by: token.issued_by.unwrap_or_default(),
                }),
                revoked_event,
            ),
        };
        smallvec![Effect::SubOperation(sub_operation)]
    }

    fn handle_revoked(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::TokenRevoked { result: Ok(()) }) => {
                match self.state {
                    RevokeTokensState::Cutoff => self.finish(),
                    _ => self.next(),
                }
            }
            Event::SubOperation(SubOperationEvent::TokenRevoked { result: Err(error) }) => {
                self.fail(GroupTokensError::Revocation(error))
            }
            other => self.fail(GroupTokensError::UnexpectedEvent {
                state: format!("{:?}", self.state),
                expected: "token revocation result",
                got: format!("{other:?}"),
            }),
        }
    }

    fn handle_user_admin(&mut self, event: Event) -> Effects {
        let RevokeTarget::Member { user, .. } = self.config.target else {
            return self.fail(GroupTokensError::NotFinished);
        };
        match authorized(event, "UserAdmin") {
            Ok(()) => self.cutoff(user),
            Err(GroupTokensError::Refused) => self.fail(GroupTokensError::NotConfined),
            Err(error) => self.fail(error),
        }
    }
}

impl Operation for RevokeTokensOperation {
    type Output = Revocation;
    type Error = GroupTokensError;

    fn start(&mut self) -> Effects {
        let auth = &self.config.auth_context;
        if auth.user_id != self.config.actor.user_id || auth.realm_id != self.config.actor.realm_id
        {
            return self.fail(GroupTokensError::Refused);
        }
        self.state = RevokeTokensState::Authorize;
        admin_check(&self.config.auth_context, self.config.group_id)
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match self.state {
            RevokeTokensState::Authorize => authorized(event, "Authorize").and_then(|()| {
                if let RevokeTarget::Token {
                    kind: TokenKind::Session,
                    ..
                } = self.config.target
                {
                    return Err(GroupTokensError::NotConfined);
                }
                self.state = RevokeTokensState::Config;
                Ok(smallvec![read_config(self.config.auth_context.realm_id)])
            }),
            RevokeTokensState::Config => parse_config(event).map(|config| {
                self.realm_config = config;
                self.lookup()
            }),
            RevokeTokensState::Lookup => self.handle_lookup(event),
            RevokeTokensState::Records => self.handle_records(event),
            RevokeTokensState::UserAdmin => Ok(self.handle_user_admin(event)),
            RevokeTokensState::Revoke | RevokeTokensState::Cutoff => Ok(self.handle_revoked(event)),
            RevokeTokensState::Init => Err(GroupTokensError::UnexpectedEvent {
                state: "Init".to_string(),
                expected: "no event before start",
                got: format!("{event:?}"),
            }),
            RevokeTokensState::Finish | RevokeTokensState::Error => Ok(smallvec![]),
        };
        result.unwrap_or_else(|error| self.fail(error))
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RevokeTokensState::Finish | RevokeTokensState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(GroupTokensError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
