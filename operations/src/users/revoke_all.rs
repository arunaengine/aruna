//! Ends every token of one account realm-wide by moving its cutoff, for user administrators.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use thiserror::Error;

use crate::auth::check_permissions::authorization_event;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::revoke_token::RevokeTokenOperation;
use crate::auth::revoke_token::owner_cutoff;

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
        got: &'static str,
    },
}

impl From<IoError> for RevokeAllError {
    fn from(error: IoError) -> Self {
        Self::Authorization(error.into())
    }
}

/// Bearer tokens, S3 credentials and S3 sessions issued up to `now` are refused on every node.
#[derive(Debug, PartialEq)]
pub struct RevokeAllOperation {
    config: RevokeAllConfig,
}

impl RevokeAllOperation {
    pub fn new(config: RevokeAllConfig) -> Self {
        Self { config }
    }

    fn cutoff(&self) -> Next<()> {
        let now = self.config.now;
        let config = owner_cutoff(self.config.actor.clone(), self.config.target, now, now);
        let child = boxed_suboperation(RevokeTokenOperation::new(config), |result| {
            Event::SubOperation(SubOperationEvent::TokenRevoked {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            })
        });
        Next::Call(IoCall::Child(child))
    }
}

impl Operation for RevokeAllOperation {
    type Output = ();
    type Error = RevokeAllError;

    fn first(&mut self) -> Result<Next<()>, RevokeAllError> {
        let auth = &self.config.auth_context;
        // Federated callers never administer accounts of this realm.
        if auth.user_id != self.config.actor.user_id
            || auth.realm_id != self.config.actor.realm_id
            || auth.user_id.realm_id != auth.realm_id
            || self.config.target.realm_id != auth.realm_id
        {
            return Err(RevokeAllError::Refused);
        }
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!("/{}/admin/u/{}", auth.realm_id, self.config.target),
            required_permission: Permission::WRITE,
        });
        let child = boxed_suboperation(check, authorization_event);
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, RevokeAllError> {
        let IoResult::Event(event) = result else {
            return Err(RevokeAllError::NotFinished);
        };
        match *event {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                match allowed {
                    Ok(true) => Ok(self.cutoff()),
                    Ok(false) => Err(RevokeAllError::Refused),
                    Err(error) => Err(error.into()),
                }
            }
            Event::SubOperation(SubOperationEvent::TokenRevoked { result }) => {
                result.map_err(RevokeAllError::Cutoff)?;
                Ok(Next::Done(()))
            }
            other => Err(RevokeAllError::UnexpectedEvent {
                state: "RevokeAll".to_string(),
                expected: "authorization or token revocation result",
                got: other.label(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn config(target_realm: u8) -> RevokeAllConfig {
        let realm_id = RealmId::from_bytes([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let auth_context = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let target_realm = RealmId::from_bytes([target_realm; 32]);
        RevokeAllConfig {
            actor: Actor {
                node_id: iroh::SecretKey::from_bytes(&[7; 32]).public(),
                user_id,
                realm_id,
            },
            auth_context,
            target: UserId::local(Ulid::from_bytes([3; 16]), target_realm),
            now: 7,
        }
    }

    fn answer(event: SubOperationEvent) -> IoResult {
        IoResult::Event(Box::new(Event::SubOperation(event)))
    }

    #[test]
    fn cutoff_after_authorization() {
        // A foreign target is refused at once; an allowed caller moves the cutoff.
        let mut foreign = RevokeAllOperation::new(config(9));
        assert_eq!(foreign.first(), Err(RevokeAllError::Refused));
        let mut operation = RevokeAllOperation::new(config(1));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let denied = SubOperationEvent::AuthorizationResult { allowed: Ok(false) };
        assert_eq!(operation.next(answer(denied)), Err(RevokeAllError::Refused));
        let allowed = SubOperationEvent::AuthorizationResult { allowed: Ok(true) };
        assert!(matches!(
            operation.next(answer(allowed)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let revoked = SubOperationEvent::TokenRevoked { result: Ok(()) };
        assert_eq!(operation.next(answer(revoked)), Ok(Next::Done(())));
    }
}
