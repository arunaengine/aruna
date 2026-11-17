//! Checks that a caller may administer a group's service accounts, and the account it names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::user::User;
use aruna_core::types::GroupId;
use byteview::ByteView;
use thiserror::Error;

use super::group_admin_path;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct ServiceCheckConfig {
    pub auth_context: AuthContext,
    pub group_id: GroupId,
    /// A service account that must belong to the group and be active.
    pub target: Option<UserId>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ServiceCheckError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error("caller may not administer this group's service accounts")]
    Refused,
    #[error("no service account of this group has that id")]
    NotFound,
    #[error("the service account is deactivated")]
    Deactivated,
    #[error("service account check did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl ServiceCheckError {
    /// The result a composing operation sees: refusals deny, failures stay failures.
    pub fn allowed(result: Result<Option<User>, Self>) -> Result<bool, AuthorizationError> {
        match result {
            Ok(_) => Ok(true),
            Err(Self::Storage(error)) => Err(error.into()),
            Err(Self::Conversion(error)) => Err(error.into()),
            Err(Self::Authorization(error)) => Err(error),
            Err(_) => Ok(false),
        }
    }
}

impl From<IoError> for ServiceCheckError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ServiceCheck".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ServiceCheckOperation {
    config: ServiceCheckConfig,
    /// The target account once the records are read; the outer `None` means not read yet.
    target: Option<Option<User>>,
}

impl ServiceCheckOperation {
    pub fn new(config: ServiceCheckConfig) -> Self {
        Self {
            config,
            target: None,
        }
    }

    fn authorize(
        &mut self,
        values: Vec<Option<aruna_core::types::Value>>,
    ) -> Result<Next<Option<User>>, ServiceCheckError> {
        let users = (values.into_iter())
            .map(|value| value.as_deref().map(User::from_bytes).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        // Service accounts never administer accounts, whatever roles they hold.
        if users
            .first()
            .and_then(Option::as_ref)
            .is_some_and(|caller| caller.service_group().is_some())
        {
            return Err(ServiceCheckError::Refused);
        }
        let target = users.into_iter().nth(1).flatten();
        if self.config.target.is_some() {
            let user = target.as_ref().ok_or(ServiceCheckError::NotFound)?;
            if user.service_group() != Some(self.config.group_id) {
                return Err(ServiceCheckError::NotFound);
            }
            if user.is_deactivated() {
                return Err(ServiceCheckError::Deactivated);
            }
        }
        self.target = Some(target);
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path: group_admin_path(self.config.auth_context.realm_id, self.config.group_id),
            required_permission: Permission::WRITE,
        });
        let child = boxed_suboperation(check, |allowed| {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })
        });
        Ok(Next::Call(IoCall::Child(child)))
    }
}

fn user_read(user_id: &UserId) -> (String, ByteView) {
    (
        USER_KEYSPACE.to_string(),
        ByteView::from(user_id.to_bytes()),
    )
}

impl Operation for ServiceCheckOperation {
    type Output = Option<User>;
    type Error = ServiceCheckError;

    fn first(&mut self) -> Result<Next<Option<User>>, ServiceCheckError> {
        // Federated users never administer service accounts, whatever group roles they hold.
        let auth = &self.config.auth_context;
        if auth.user_id.realm_id != auth.realm_id {
            return Err(ServiceCheckError::Refused);
        }
        let mut reads = vec![user_read(&self.config.auth_context.user_id)];
        reads.extend(self.config.target.as_ref().map(user_read));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<User>>, ServiceCheckError> {
        let event = match result {
            IoResult::Values(values) if self.target.is_none() => return self.authorize(values),
            IoResult::Event(event) => *event,
            other => {
                return Err(ServiceCheckError::UnexpectedEvent {
                    state: "ServiceCheck".to_string(),
                    expected: "batch read or authorization result",
                    got: other.label(),
                });
            }
        };
        let got = event.label();
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return Err(ServiceCheckError::UnexpectedEvent {
                state: "Authorize".to_string(),
                expected: "authorization result",
                got,
            });
        };
        if !allowed? {
            return Err(ServiceCheckError::Refused);
        }
        Ok(Next::Done(self.target.take().flatten()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    #[test]
    fn federated_caller_refused() {
        // A federated group administrator reads nothing and is refused.
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let caller = UserId::new(Ulid::from_bytes([1u8; 16]), RealmId::from_bytes([6u8; 32]));
        let mut operation = ServiceCheckOperation::new(ServiceCheckConfig {
            auth_context: AuthContext {
                user_id: caller,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id: Ulid::from_bytes([2u8; 16]),
            target: None,
        });
        assert_eq!(operation.first(), Err(ServiceCheckError::Refused));
    }

    #[test]
    fn missing_target_refused() {
        // A named account that does not exist is not found before any permission check.
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let caller = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
        let target = UserId::new(Ulid::from_bytes([3u8; 16]), realm_id);
        let mut operation = ServiceCheckOperation::new(ServiceCheckConfig {
            auth_context: AuthContext {
                user_id: caller,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id: Ulid::from_bytes([2u8; 16]),
            target: Some(target),
        });
        let reads = vec![user_read(&caller), user_read(&target)];
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(reads))));
        let missing = operation.next(IoResult::Values(vec![None, None]));
        assert_eq!(missing, Err(ServiceCheckError::NotFound));
    }
}
