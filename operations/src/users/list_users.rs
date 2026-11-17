//! Lists a realm's users in pages after checking the caller's permission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::USER_KEYSPACE;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::User;
use aruna_core::types::Key;
use thiserror::Error;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct ListUsersInput {
    pub auth_context: AuthContext,
    pub self_realm_id: RealmId,
    pub limit: usize,
    pub start_after: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListUsersOutput {
    pub users: Vec<User>,
    pub next_start_after: Option<String>,
}

/// Checks the caller's permission, then pages through the realm's users.
#[derive(Debug, PartialEq)]
pub struct ListUsersOperation {
    input: ListUsersInput,
    users: Vec<User>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListUsersError {
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
    #[error("list users did not finish")]
    NotFinished,
}

impl From<IoError> for ListUsersError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ListUsers".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl ListUsersOperation {
    pub fn new(mut input: ListUsersInput) -> Self {
        input.limit = input.limit.max(1);
        Self {
            input,
            users: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: USER_KEYSPACE.to_string(),
            prefix: UserId::storage_prefix(self.input.self_realm_id),
            after,
            limit: self.input.limit.saturating_add(1),
        }
    }

    fn authorized(&mut self, event: Event) -> Result<Next<ListUsersOutput>, ListUsersError> {
        let got = event.label();
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return Err(ListUsersError::UnexpectedEvent {
                state: "Auth".to_string(),
                expected: "authorization result",
                got,
            });
        };
        if !allowed? {
            return Err(ListUsersError::Unauthorized);
        }
        let after = (self.input.start_after.as_deref())
            .map(UserId::from_string)
            .transpose()?
            .map(|user_id| Key::from(user_id.to_storage_key()));
        Ok(Next::Call(self.page(after)))
    }

    fn finish(&mut self, next_start_after: Option<String>) -> Next<ListUsersOutput> {
        Next::Done(ListUsersOutput {
            users: std::mem::take(&mut self.users),
            next_start_after,
        })
    }
}

impl Operation for ListUsersOperation {
    type Output = ListUsersOutput;
    type Error = ListUsersError;

    fn first(&mut self) -> Result<Next<ListUsersOutput>, ListUsersError> {
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: format!("/{}/admin/u/**", self.input.self_realm_id),
            required_permission: Permission::READ,
        });
        let child = boxed_suboperation(check, |allowed| {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ListUsersOutput>, ListUsersError> {
        let (rows, next) = match result {
            IoResult::Event(event) => return self.authorized(*event),
            IoResult::Page { rows, next } => (rows, next),
            other => {
                return Err(ListUsersError::UnexpectedEvent {
                    state: "ListUsers".to_string(),
                    expected: "authorization result or IoResult::Page",
                    got: other.label(),
                });
            }
        };
        for (key, value) in rows {
            if UserId::from_storage_key(&key)?.realm_id != self.input.self_realm_id {
                continue;
            }
            let user = User::from_bytes(&value)?;
            if user.user_id.realm_id == self.input.self_realm_id {
                self.users.push(user);
            }
        }
        if self.users.len() > self.input.limit {
            self.users.truncate(self.input.limit);
            let next_start_after = self.users.last().map(|user| user.user_id.to_string());
            return Ok(self.finish(next_start_after));
        }
        match next {
            Some(after) => Ok(Next::Call(self.page(Some(after)))),
            None => Ok(self.finish(None)),
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{ListUsersError, ListUsersInput, ListUsersOperation, ListUsersOutput};
    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::events::{Event, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::user::User;
    use ulid::Ulid;

    fn actor(realm_id: RealmId, user_id: UserId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id,
            realm_id,
        }
    }

    fn auth_context(realm_id: RealmId, user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn user(realm_id: RealmId, seed: u8, name: &str) -> User {
        User {
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
            name: name.to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        }
    }

    fn input(realm_id: RealmId, limit: usize, start_after: Option<String>) -> ListUsersInput {
        let caller = UserId::local(Ulid::from_bytes([1u8; 16]), realm_id);
        ListUsersInput {
            auth_context: auth_context(realm_id, caller),
            self_realm_id: realm_id,
            limit,
            start_after,
        }
    }

    fn answer(allowed: bool) -> IoResult {
        let event = SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        };
        IoResult::Event(Box::new(Event::SubOperation(event)))
    }

    type Turn = Result<Next<ListUsersOutput>, ListUsersError>;

    fn authorize(operation: &mut ListUsersOperation) -> Turn {
        operation.next(answer(true))
    }

    #[test]
    fn authorized_emits_iter() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let mut operation = ListUsersOperation::new(input(realm_id, 10, None));

        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let scan = IoCall::Page {
            space: aruna_core::USER_KEYSPACE.to_string(),
            prefix: UserId::storage_prefix(realm_id),
            after: None,
            limit: 11,
        };
        assert_eq!(authorize(&mut operation), Ok(Next::Call(scan)));
    }

    #[test]
    fn filters_realm_cursor() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let foreign_realm_id = RealmId::from_bytes([4u8; 32]);
        let alice = user(realm_id, 2, "alice");
        let bob = user(realm_id, 3, "bob");
        let carol = user(realm_id, 4, "carol");
        let foreign = user(foreign_realm_id, 5, "foreign");
        let mut operation = ListUsersOperation::new(input(realm_id, 2, None));
        operation.first().unwrap();
        authorize(&mut operation).unwrap();

        let page = IoResult::Page {
            rows: vec![
                (
                    alice.user_id.to_storage_key().into(),
                    alice
                        .to_bytes(&actor(realm_id, alice.user_id))
                        .unwrap()
                        .into(),
                ),
                (
                    foreign.user_id.to_storage_key().into(),
                    foreign
                        .to_bytes(&actor(foreign_realm_id, foreign.user_id))
                        .unwrap()
                        .into(),
                ),
                (
                    bob.user_id.to_storage_key().into(),
                    bob.to_bytes(&actor(realm_id, bob.user_id)).unwrap().into(),
                ),
                (
                    carol.user_id.to_storage_key().into(),
                    carol
                        .to_bytes(&actor(realm_id, carol.user_id))
                        .unwrap()
                        .into(),
                ),
            ],
            next: None,
        };
        let output = ListUsersOutput {
            users: vec![alice.clone(), bob.clone()],
            next_start_after: Some(bob.user_id.to_string()),
        };
        assert_eq!(operation.next(page), Ok(Next::Done(output)));
    }

    #[test]
    fn uses_cursor_start() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let start_after = user(realm_id, 6, "cursor").user_id;
        let mut operation =
            ListUsersOperation::new(input(realm_id, 10, Some(start_after.to_string())));
        operation.first().unwrap();

        assert!(matches!(
            authorize(&mut operation),
            Ok(Next::Call(IoCall::Page { after: Some(key), .. }))
                if key.as_ref() == start_after.to_storage_key().as_slice()
        ));
    }

    #[test]
    fn unauthorized_fails() {
        // A refused caller reads no user; an answer of another kind fails the listing.
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let mut operation = ListUsersOperation::new(input(realm_id, 10, None));
        operation.first().unwrap();
        assert_eq!(
            operation.next(answer(false)),
            Err(ListUsersError::Unauthorized)
        );
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(ListUsersError::UnexpectedEvent { .. })
        ));
    }
}
