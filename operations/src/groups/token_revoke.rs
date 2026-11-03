//! Revokes one group token, or all group-bound access of one member, held by this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::Effect;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;

use super::list::{admin_check, authorized};
use super::{GroupToken, GroupTokensError, TokenKind, TokenScan, TokenStatus};
use crate::auth::revoke_token::RevokeTokenOperation;
use crate::s3::access::revoke::RevokeUserOperation;
use crate::s3::session::{RevokeS3Config, RevokeS3Operation};
use crate::session::revoke::{RevokeSessionOperation, owner_cutoff};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RevokeTarget {
    Token {
        kind: TokenKind,
        id: String,
    },
    /// Every group-bound token of one member; all tokens when it is the group's service account.
    Member(UserId),
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
    Scan,
    Revoke,
    Cutoff,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct RevokeTokensOperation {
    config: RevokeTokensConfig,
    scan: TokenScan,
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

impl RevokeTokensOperation {
    pub fn new(config: RevokeTokensConfig) -> Self {
        let scan = TokenScan::new(config.auth_context.realm_id, config.group_id, config.now);
        Self {
            config,
            scan,
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

    /// Picks the active tokens to revoke once the scan has shown what this node holds.
    fn plan(&mut self) -> Effects {
        let tokens = std::mem::take(&mut self.scan.tokens);
        match self.config.target.clone() {
            RevokeTarget::Token { kind, id } => {
                let Some(token) = tokens.into_iter().find(|t| t.kind == kind && t.id == id) else {
                    return self.fail(GroupTokensError::NotFound);
                };
                self.pending
                    .extend((token.status == TokenStatus::Active).then_some(token));
            }
            RevokeTarget::Member(member) => {
                let active = tokens
                    .into_iter()
                    .filter(|token| token.owner == member && token.status == TokenStatus::Active);
                if self.scan.accounts.contains(&member) {
                    self.summary.revoked = active.count();
                    self.summary.all_tokens = true;
                    return self.cutoff(member);
                }
                self.pending.extend(active);
            }
        }
        self.next()
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
            TokenKind::Session => boxed_suboperation(
                RevokeSessionOperation::for_owner(
                    self.config.actor.clone(),
                    token.owner,
                    token.id,
                    self.config.now,
                ),
                revoked_event,
            ),
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
        match self.state {
            RevokeTokensState::Authorize => match authorized(event, "Authorize") {
                Ok(()) => {
                    self.state = RevokeTokensState::Scan;
                    self.scan.start()
                }
                Err(error) => self.fail(error),
            },
            RevokeTokensState::Scan => match self.scan.step(event) {
                Ok(Some(effects)) => effects,
                Ok(None) => self.plan(),
                Err(error) => self.fail(error),
            },
            RevokeTokensState::Revoke | RevokeTokensState::Cutoff => self.handle_revoked(event),
            RevokeTokensState::Init => self.fail(GroupTokensError::UnexpectedEvent {
                state: "Init".to_string(),
                expected: "no event before start",
                got: format!("{event:?}"),
            }),
            RevokeTokensState::Finish | RevokeTokensState::Error => smallvec![],
        }
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
