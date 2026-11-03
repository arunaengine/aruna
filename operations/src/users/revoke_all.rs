//! Ends every token of one account realm-wide by moving its cutoff, for user administrators.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::Effect;
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::revoke_token::RevokeTokenOperation;
use crate::groups::admin_check::authorization_event;
use crate::session::revoke::owner_cutoff;

#[derive(Clone, Debug, PartialEq)]
pub struct RevokeAllConfig {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub target: UserId,
    pub now: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum RevokeAllError {
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error("caller may not administer this account")]
    Refused,
    #[error("token cutoff failed: {0}")]
    Cutoff(String),
    #[error("revoking all tokens did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RevokeAllState {
    Init,
    Authorize,
    Cutoff,
    Finish,
    Error,
}

/// Bearer tokens, S3 credentials and S3 sessions issued up to `now` are refused on every node.
#[derive(Debug, PartialEq)]
pub struct RevokeAllOperation {
    config: RevokeAllConfig,
    state: RevokeAllState,
    output: Option<Result<(), RevokeAllError>>,
}

impl RevokeAllOperation {
    pub fn new(config: RevokeAllConfig) -> Self {
        Self {
            config,
            state: RevokeAllState::Init,
            output: None,
        }
    }

    fn done(&mut self, result: Result<(), RevokeAllError>) -> Effects {
        self.state = match result {
            Ok(()) => RevokeAllState::Finish,
            Err(_) => RevokeAllState::Error,
        };
        self.output = Some(result);
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.done(Err(RevokeAllError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        }))
    }

    fn handle_authorized(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected("authorization result", event);
        };
        match allowed {
            Ok(true) => {
                self.state = RevokeAllState::Cutoff;
                let now = self.config.now;
                let config = owner_cutoff(self.config.actor.clone(), self.config.target, now, now);
                smallvec![Effect::SubOperation(boxed_suboperation(
                    RevokeTokenOperation::new(config),
                    |result| Event::SubOperation(SubOperationEvent::TokenRevoked {
                        result: result.map(|_| ()).map_err(|error| error.to_string()),
                    }),
                ))]
            }
            Ok(false) => self.done(Err(RevokeAllError::Refused)),
            Err(error) => self.done(Err(error.into())),
        }
    }

    fn handle_cutoff(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::TokenRevoked { result }) => {
                self.done(result.map_err(RevokeAllError::Cutoff))
            }
            other => self.unexpected("token revocation result", other),
        }
    }
}

impl Operation for RevokeAllOperation {
    type Output = ();
    type Error = RevokeAllError;

    fn start(&mut self) -> Effects {
        let auth = &self.config.auth_context;
        // Federated callers never administer accounts of this realm.
        if auth.user_id != self.config.actor.user_id
            || auth.realm_id != self.config.actor.realm_id
            || auth.user_id.realm_id != auth.realm_id
            || self.config.target.realm_id != auth.realm_id
        {
            return self.done(Err(RevokeAllError::Refused));
        }
        self.state = RevokeAllState::Authorize;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth.clone(),
                path: format!("/{}/admin/u/{}", auth.realm_id, self.config.target),
                required_permission: Permission::WRITE,
            }),
            authorization_event,
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            RevokeAllState::Authorize => self.handle_authorized(event),
            RevokeAllState::Cutoff => self.handle_cutoff(event),
            RevokeAllState::Init => self.unexpected("no event before start", event),
            RevokeAllState::Finish | RevokeAllState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RevokeAllState::Finish | RevokeAllState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(RevokeAllError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
