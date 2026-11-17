//! Reads one user by id after checking the caller's permission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::USER_KEYSPACE;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::User;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct GetUserInput {
    pub auth_context: AuthContext,
    pub self_realm_id: RealmId,
    pub user_id: String,
}

#[derive(Debug, PartialEq)]
pub struct GetUserOperation {
    input: GetUserInput,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetUserError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    AuthorizationError(#[from] AuthorizationError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
    #[error("registration did not finish")]
    NotFinished,
    #[error("User not found")]
    UserNotFound,
}

impl From<IoError> for GetUserError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "GetUser".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl GetUserOperation {
    pub fn new(input: GetUserInput) -> Self {
        Self { input }
    }
}

impl Operation for GetUserOperation {
    type Output = User;
    type Error = GetUserError;

    fn first(&mut self) -> Result<Next<User>, GetUserError> {
        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: format!(
                "/{}/admin/u/{}",
                self.input.self_realm_id, self.input.user_id
            ),
            required_permission: Permission::READ,
        };
        let check = boxed_suboperation(CheckPermissionsOperation::new(auth_config), |result| {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: result })
        });
        Ok(Next::Call(IoCall::Child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<User>, GetUserError> {
        let IoResult::Event(event) = result else {
            let [value] = values::<1>(result)?;
            let user = User::from_bytes(&value.ok_or(GetUserError::UserNotFound)?)?;
            return Ok(Next::Done(user));
        };
        let got = event.label();
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = *event else {
            return Err(GetUserError::UnexpectedEvent {
                state: "Auth".to_string(),
                expected: "authorization result",
                got,
            });
        };
        if !allowed? {
            return Err(GetUserError::Unauthorized);
        }
        let user_id = UserId::from_string(&self.input.user_id)?;
        Ok(Next::Call(read_row(USER_KEYSPACE, user_id.to_bytes())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn allowed(allowed: bool) -> IoResult {
        let event = SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        };
        IoResult::Event(Box::new(Event::SubOperation(event)))
    }

    #[test]
    fn reads_after_authorization() {
        // A refused caller reads nothing; an allowed one reads the user row.
        let realm_id = RealmId::from_bytes([1; 32]);
        let user_id = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let input = GetUserInput {
            auth_context: AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            self_realm_id: realm_id,
            user_id: user_id.to_string(),
        };
        let mut refused = GetUserOperation::new(input.clone());
        refused.first().unwrap();
        assert_eq!(
            refused.next(allowed(false)),
            Err(GetUserError::Unauthorized)
        );
        let mut operation = GetUserOperation::new(input);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let read = read_row(USER_KEYSPACE, user_id.to_bytes());
        assert_eq!(operation.next(allowed(true)), Ok(Next::Call(read)));
        let missing = operation.next(IoResult::Values(vec![None]));
        assert_eq!(missing, Err(GetUserError::UserNotFound));
    }
}
