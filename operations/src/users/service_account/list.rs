//! Lists the service accounts one group owns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::USER_KEYSPACE;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::PrefixPages;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::user::User;
use aruna_core::types::GroupId;
use thiserror::Error;

use super::check::{ServiceCheckConfig, ServiceCheckError, ServiceCheckOperation};

const PAGE: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum ListServiceError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error("caller may not administer this group's service accounts")]
    Unauthorized,
    #[error("service account listing did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for ListServiceError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ListService".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

/// Scans the realm's user records, since ownership lives on the account and not on the group.
#[derive(Debug, PartialEq)]
pub struct ListServiceOperation {
    auth_context: AuthContext,
    group_id: GroupId,
    pages: PrefixPages,
    accounts: Vec<User>,
}

impl ListServiceOperation {
    pub fn new(auth_context: AuthContext, group_id: GroupId) -> Self {
        let prefix = UserId::storage_prefix(auth_context.realm_id);
        Self {
            auth_context,
            group_id,
            pages: PrefixPages::new(USER_KEYSPACE, prefix, PAGE),
            accounts: Vec::new(),
        }
    }
}

impl Operation for ListServiceOperation {
    type Output = Vec<User>;
    type Error = ListServiceError;

    fn first(&mut self) -> Result<Next<Vec<User>>, ListServiceError> {
        let check = ServiceCheckOperation::new(ServiceCheckConfig {
            auth_context: self.auth_context.clone(),
            group_id: self.group_id,
            target: None,
        });
        let child = boxed_suboperation(check, |result| {
            Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: ServiceCheckError::allowed(result),
            })
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<User>>, ListServiceError> {
        if let IoResult::Event(event) = result {
            let got = event.label();
            let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = *event
            else {
                return Err(ListServiceError::UnexpectedEvent {
                    state: "Authorize".to_string(),
                    expected: "authorization result",
                    got,
                });
            };
            if !allowed? {
                return Err(ListServiceError::Unauthorized);
            }
            return Ok(Next::Call(self.pages.call()));
        }
        for (_, value) in self.pages.take(result)? {
            let user = User::from_bytes(&value)?;
            if user.service_group() == Some(self.group_id) {
                self.accounts.push(user);
            }
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        Ok(Next::Done(std::mem::take(&mut self.accounts)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    #[test]
    fn scans_after_authorization() {
        // A refused caller scans nothing; an allowed one pages through the realm's users.
        let realm_id = RealmId::from_bytes([1; 32]);
        let auth = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([2; 16]), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let group_id = Ulid::from_bytes([3; 16]);
        let answer = |allowed| {
            let event = SubOperationEvent::AuthorizationResult {
                allowed: Ok(allowed),
            };
            IoResult::Event(Box::new(Event::SubOperation(event)))
        };
        let mut refused = ListServiceOperation::new(auth.clone(), group_id);
        refused.first().unwrap();
        assert_eq!(
            refused.next(answer(false)),
            Err(ListServiceError::Unauthorized)
        );
        let mut operation = ListServiceOperation::new(auth, group_id);
        operation.first().unwrap();
        let scan = PrefixPages::new(USER_KEYSPACE, UserId::storage_prefix(realm_id), PAGE);
        assert_eq!(operation.next(answer(true)), Ok(Next::Call(scan.call())));
        let last = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(operation.next(last), Ok(Next::Done(Vec::new())));
    }
}
