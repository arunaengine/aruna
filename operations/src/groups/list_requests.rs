//! Lists join requests as a page, optionally only the pending ones, for a permitted caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_core::admin_documents::AdminDocumentTarget;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::join_request::{JoinDecisionKind, JoinRequestState};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub struct ListJoinInput {
    pub auth: AuthContext,
    pub group_id: Option<Ulid>,
    pub pending_only: bool,
    pub start_after: Option<(Ulid, Ulid)>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinRequestsPage {
    pub requests: Vec<JoinRequestState>,
    pub next_start_after: Option<(Ulid, Ulid)>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListJoinError {
    #[error("not authorized to list membership requests")]
    Unauthorized,
    #[error("unexpected event while listing membership requests")]
    UnexpectedEvent,
    #[error("membership request listing did not finish")]
    NotFinished,
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

impl From<IoError> for ListJoinError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            _ => Self::UnexpectedEvent,
        }
    }
}

/// Checks a group administrator, then scans the group reducer states for join requests.
#[derive(Debug, PartialEq)]
pub struct ListJoinOperation {
    input: ListJoinInput,
    requests: Vec<JoinRequestState>,
}

impl ListJoinOperation {
    pub fn new(mut input: ListJoinInput) -> Self {
        input.limit = input.limit.clamp(1, 100);
        Self {
            input,
            requests: Vec::new(),
        }
    }

    /// The next page of reducer states; the first starts at the cursor's group, inclusive, which
    /// `IoCall::Page` cannot express, so it passes the storage scan through.
    fn read(&self, after: Option<Key>) -> Next<JoinRequestsPage> {
        let prefix = self.input.group_id.map_or_else(
            || Key::from(vec![b'g']),
            |group_id| reducer_state_key(&AdminDocumentTarget::Group { group_id }),
        );
        let start = after.map(IterStart::After).or_else(|| {
            self.input.start_after.map(|(group_id, _)| {
                IterStart::At(reducer_state_key(&AdminDocumentTarget::Group { group_id }))
            })
        });
        let scan = Effect::Storage(StorageEffect::Iter {
            key_space: DOCUMENT_STATE_KEYSPACE.into(),
            prefix: Some(prefix),
            start,
            limit: 64,
            txn_id: None,
        });
        Next::Call(IoCall::Effect(Box::new(scan)))
    }

    fn collect(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Next<JoinRequestsPage>, ListJoinError> {
        for (key, value) in values {
            let state = decode_reducer_state(&value).map_err(ConversionError::from)?;
            let AdminDocumentTarget::Group { group_id } = state.target else {
                return Err(ListJoinError::UnexpectedEvent);
            };
            if key != reducer_state_key(&state.target) {
                return Err(ListJoinError::UnexpectedEvent);
            }
            let local_group = state.materialized_group_realm() == Some(self.input.auth.realm_id);
            for entry in state.join_requests() {
                if (entry.request.user_id.realm_id != self.input.auth.realm_id && !local_group)
                    || (self.input.group_id.is_none()
                        && entry.request.user_id != self.input.auth.user_id)
                    || (self.input.pending_only && entry.decision.is_some())
                    || entry
                        .decision
                        .as_ref()
                        .is_some_and(|decision| decision.kind == JoinDecisionKind::Withdrawn)
                    || self
                        .input
                        .start_after
                        .is_some_and(|cursor| (group_id, entry.request.request_id) <= cursor)
                {
                    continue;
                }
                self.requests.push(entry);
                if self.requests.len() > self.input.limit {
                    self.requests.truncate(self.input.limit);
                    let last = self
                        .requests
                        .last()
                        .map(|entry| (entry.request.group_id, entry.request.request_id));
                    return Ok(Next::Done(self.page(last)));
                }
            }
        }
        Ok(match next {
            Some(key) => self.read(Some(key)),
            None => Next::Done(self.page(None)),
        })
    }

    fn page(&mut self, next_start_after: Option<(Ulid, Ulid)>) -> JoinRequestsPage {
        JoinRequestsPage {
            requests: std::mem::take(&mut self.requests),
            next_start_after,
        }
    }
}

impl Operation for ListJoinOperation {
    type Output = JoinRequestsPage;
    type Error = ListJoinError;

    fn first(&mut self) -> Result<Next<JoinRequestsPage>, ListJoinError> {
        if self.input.auth.path_restrictions.is_some() || self.input.auth.user_id.is_nil() {
            return Err(ListJoinError::Unauthorized);
        }
        let Some(group_id) = self.input.group_id else {
            return Ok(self.read(None));
        };
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.input.auth.clone(),
            path: format!("/{}/g/{group_id}/admin/users/**", self.input.auth.realm_id),
            required_permission: Permission::WRITE,
        });
        let child = boxed_suboperation(check, |allowed| {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JoinRequestsPage>, ListJoinError> {
        let IoResult::Event(event) = result else {
            return Err(ListJoinError::UnexpectedEvent);
        };
        match *event {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                match allowed? {
                    true => Ok(self.read(None)),
                    false => Err(ListJoinError::Unauthorized),
                }
            }
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => self.collect(values, next_start_after),
            _ => Err(ListJoinError::UnexpectedEvent),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::admin_documents::AdminDocumentOperation;
    use aruna_core::join_request::JoinRequest;
    use aruna_core::reducer::AdminDocumentState;
    use aruna_core::storage_entries::reducer_state_entry;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;

    fn realm() -> RealmId {
        RealmId::from_bytes([7; 32])
    }

    fn foreign_user() -> UserId {
        UserId::new(Ulid::from_bytes([2; 16]), RealmId::from_bytes([9; 32]))
    }

    /// A group of the serving realm with one join request of a federated user.
    fn group_entry() -> (Key, Value) {
        let group_id = Ulid::from_bytes([3; 16]);
        let admin = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([1; 16]), realm()),
            realm_id: realm(),
        };
        let mut state = AdminDocumentState::new(AdminDocumentTarget::Group { group_id });
        state
            .apply_operation(
                &admin,
                AdminDocumentOperation::GroupCreated {
                    realm_id: realm(),
                    display_name: "Group".into(),
                    owner: admin.user_id,
                },
            )
            .unwrap();
        let requester = Actor {
            user_id: foreign_user(),
            ..admin
        };
        state
            .apply_operation(
                &requester,
                AdminDocumentOperation::GroupJoinRequested {
                    request: JoinRequest {
                        request_id: Ulid::from_bytes([5; 16]),
                        group_id,
                        user_id: foreign_user(),
                        message: None,
                        created_at: 1,
                    },
                },
            )
            .unwrap();
        let (_, key, value) = reducer_state_entry(&state).unwrap();
        (key, value)
    }

    fn listed(user_id: UserId, group_id: Option<Ulid>) -> Vec<JoinRequestState> {
        let mut operation = ListJoinOperation::new(ListJoinInput {
            auth: AuthContext {
                user_id,
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            group_id,
            pending_only: false,
            start_after: None,
            limit: 10,
        });
        let answer = |event| IoResult::Event(Box::new(event));
        let mut turn = operation.first();
        if group_id.is_some() {
            let allowed = SubOperationEvent::AuthorizationResult { allowed: Ok(true) };
            turn = operation.next(answer(Event::SubOperation(allowed)));
        }
        assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
        let page = Event::Storage(StorageEvent::IterResult {
            values: vec![group_entry()],
            next_start_after: None,
        });
        match operation.next(answer(page)) {
            Ok(Next::Done(page)) => page.requests,
            other => panic!("expected a page, got {other:?}"),
        }
    }

    #[test]
    fn lists_foreign_requests() {
        // The federated requester sees its own request and a group admin sees it too.
        assert_eq!(listed(foreign_user(), None).len(), 1);
        let admin = UserId::local(Ulid::from_bytes([1; 16]), realm());
        assert_eq!(listed(admin, Some(Ulid::from_bytes([3; 16]))).len(), 1);
        // A local user with the same ULID is someone else.
        let twin = UserId::local(foreign_user().user_ulid, realm());
        assert!(listed(twin, None).is_empty());
    }

    #[test]
    fn cursor_group_included() {
        // The cursor's group may hold later requests, so the first scan starts at it, inclusive.
        let group_id = Ulid::from_bytes([3; 16]);
        let mut operation = ListJoinOperation::new(ListJoinInput {
            auth: AuthContext {
                user_id: foreign_user(),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            group_id: None,
            pending_only: false,
            start_after: Some((group_id, Ulid::from_bytes([5; 16]))),
            limit: 10,
        });
        let Ok(Next::Call(IoCall::Effect(effect))) = operation.first() else {
            panic!("expected the scan");
        };
        let start = reducer_state_key(&AdminDocumentTarget::Group { group_id });
        assert!(matches!(
            *effect,
            Effect::Storage(StorageEffect::Iter { start: Some(IterStart::At(key)), .. }) if key == start
        ));
    }
}
