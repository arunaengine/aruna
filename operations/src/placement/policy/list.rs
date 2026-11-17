//! Lists the placement policy documents this node holds, one bounded page at a time.
//! A page names local rows only, so it never claims to be the full realm catalog.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::PLACEMENT_POLICY_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, group_admin_path, policy_admin_path,
};
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use tracing::warn;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

/// Page size a listing uses when the caller names none.
pub const POLICY_LIST_DEFAULT: usize = 50;
/// Upper bound on one listing page.
pub const POLICY_LIST_LIMIT: usize = 200;

#[derive(Clone, Debug, PartialEq)]
pub struct ListPoliciesInput {
    pub auth_context: AuthContext,
    /// Names a group: the page then holds the realm-wide rules plus that
    /// group's own, and administering the group is enough to read it. Without
    /// it the caller must administer the realm configuration and sees all.
    pub group_id: Option<GroupId>,
    /// Exclusive cursor: the storage key of the last policy of the previous page.
    pub start_after: Option<Key>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PolicyListPage {
    /// Ascending by policy id, which is also the row key order.
    pub policies: Vec<PlacementPolicyDocument>,
    pub cursor: Option<Key>,
    /// True only when this node's bounded iterator was exhausted in this pass.
    pub complete: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListPoliciesError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("caller may not administer the realm configuration")]
    Unauthorized,
    #[error("unexpected event during the policy listing")]
    InvalidEvent,
}

impl From<IoError> for ListPoliciesError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            _ => Self::InvalidEvent,
        }
    }
}

/// Checks the caller's permission, then reads one page of policy rows.
#[derive(Debug, PartialEq)]
pub struct ListPoliciesOperation {
    input: ListPoliciesInput,
}

impl ListPoliciesOperation {
    pub fn new(input: ListPoliciesInput) -> Self {
        Self { input }
    }

    fn authorized(&self, event: Event) -> Result<Next<PolicyListPage>, ListPoliciesError> {
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return Err(ListPoliciesError::InvalidEvent);
        };
        match allowed {
            Ok(true) => Ok(Next::Call(IoCall::Page {
                space: PLACEMENT_POLICY_KEYSPACE.to_string(),
                prefix: Key::from(Vec::new()),
                after: self.input.start_after.clone(),
                limit: self.input.limit.clamp(1, POLICY_LIST_LIMIT),
            })),
            Ok(false) => Err(ListPoliciesError::Unauthorized),
            Err(error) => {
                warn!(error = %error, "Policy listing authorization check failed");
                Err(ListPoliciesError::Unauthorized)
            }
        }
    }

    fn collect(
        &self,
        rows: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<PolicyListPage, ListPoliciesError> {
        let realm_id: RealmId = self.input.auth_context.realm_id;
        let mut policies = Vec::with_capacity(rows.len());
        for (_, value) in rows {
            let document = PlacementPolicyDocument::from_bytes(value.as_ref())?;
            // A row stored for another realm, or owned by a group the caller
            // did not ask about, is not part of this page.
            let visible = document.realm_id == realm_id
                && match (self.input.group_id, document.policy.owner_group_id) {
                    (_, None) => true,
                    (Some(group_id), Some(owner)) => owner == group_id,
                    (None, Some(_)) => true,
                };
            if visible {
                policies.push(document);
            }
        }
        Ok(PolicyListPage {
            policies,
            complete: next.is_none(),
            cursor: next,
        })
    }
}

impl Operation for ListPoliciesOperation {
    type Output = PolicyListPage;
    type Error = ListPoliciesError;

    fn first(&mut self) -> Result<Next<PolicyListPage>, ListPoliciesError> {
        let realm_id = self.input.auth_context.realm_id;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: match self.input.group_id {
                Some(group_id) => group_admin_path(realm_id, group_id),
                None => policy_admin_path(realm_id),
            },
            required_permission: Permission::READ,
        };
        let child = boxed_suboperation(CheckPermissionsOperation::new(auth_config), |result| {
            Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: result })
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PolicyListPage>, ListPoliciesError> {
        match result {
            IoResult::Event(event) => self.authorized(*event),
            IoResult::Page { rows, next } => self.collect(rows, next).map(Next::Done),
            _ => Err(ListPoliciesError::InvalidEvent),
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, ListPoliciesError::Unauthorized)
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{ListPoliciesError, ListPoliciesInput, ListPoliciesOperation, PolicyListPage};
    use aruna_core::UserId;
    use aruna_core::calls::{IoResult, Next};
    use aruna_core::events::{Event, SubOperationEvent};
    use aruna_core::id::NodeId;
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::document::placement_policy_key;
    use aruna_core::structs::placement::policy::{
        PlacementPolicy, PlacementSelector, VerifiedPolicy,
    };
    use aruna_core::types::Key;
    use ulid::Ulid;

    use crate::tests::policy::signed_document;

    fn realm_id() -> RealmId {
        RealmId::from_bytes([1u8; 32])
    }

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    fn policy(seed: u8) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            format!("residency-{seed}"),
            vec![PlacementSelector {
                node_id: Some(node_id()),
                location: None,
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn operation() -> ListPoliciesOperation {
        ListPoliciesOperation::new(ListPoliciesInput {
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id()),
                realm_id: realm_id(),
                path_restrictions: None,
                session: None,
            },
            group_id: None,
            start_after: None,
            limit: 16,
        })
    }

    fn authorized(allowed: bool) -> IoResult {
        let event = SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        };
        IoResult::Event(Box::new(Event::SubOperation(event)))
    }

    /// Runs `operation` through an allowed check and one page of `rows`.
    fn listed(
        mut operation: ListPoliciesOperation,
        rows: Vec<(Key, Key)>,
        next: Option<Key>,
    ) -> PolicyListPage {
        operation.first().unwrap();
        operation.next(authorized(true)).unwrap();
        match operation.next(IoResult::Page { rows, next }) {
            Ok(Next::Done(page)) => page,
            other => panic!("expected a page, got {other:?}"),
        }
    }

    fn page(seeds: &[u8], truncated: bool) -> PolicyListPage {
        let rows: Vec<_> = seeds
            .iter()
            .map(|seed| {
                let document = signed_document(realm_id(), &policy(*seed), 9);
                (
                    Key::from(placement_policy_key(Ulid::from_bytes([*seed; 16]))),
                    Key::from(document.to_bytes().expect("document encodes")),
                )
            })
            .collect();
        let next = truncated.then(|| rows.last().expect("a truncated page has rows").0.clone());
        listed(operation(), rows, next)
    }

    #[test]
    fn lists_by_id() {
        let page = page(&[1, 2, 3], false);

        let ids: Vec<_> = page
            .policies
            .iter()
            .map(|document| document.policy.policy_id)
            .collect();
        assert_eq!(
            ids,
            vec![
                Ulid::from_bytes([1u8; 16]),
                Ulid::from_bytes([2u8; 16]),
                Ulid::from_bytes([3u8; 16])
            ]
        );
        assert!(page.complete);
        assert!(page.cursor.is_none());
    }

    #[test]
    fn pages_with_cursor() {
        // A truncated page hands back the last key so the next page resumes after it.
        let page = page(&[1, 2], true);

        assert_eq!(page.policies.len(), 2);
        assert!(!page.complete);
        assert_eq!(
            page.cursor,
            Some(Key::from(placement_policy_key(Ulid::from_bytes([2u8; 16]))))
        );
    }

    #[test]
    fn scopes_to_group() {
        // A group's page holds the realm-wide rules plus that group's own, and
        // nothing another group owns.
        let group_id = Ulid::from_bytes([9u8; 16]);
        let owned = |seed: u8, owner: Ulid| {
            let policy = PlacementPolicy::new(
                Ulid::from_bytes([seed; 16]),
                format!("residency-{seed}"),
                vec![PlacementSelector {
                    node_id: Some(node_id()),
                    location: None,
                    labels: Vec::new(),
                    executor_kind: None,
                }],
            )
            .expect("policy is valid")
            .owned_by(owner)
            .expect("owner is valid");
            VerifiedPolicy::verify(policy).expect("policy verifies")
        };
        let rows = vec![
            (
                Key::from(placement_policy_key(Ulid::from_bytes([1u8; 16]))),
                Key::from(
                    signed_document(realm_id(), &policy(1), 9)
                        .to_bytes()
                        .expect("document encodes"),
                ),
            ),
            (
                Key::from(placement_policy_key(Ulid::from_bytes([2u8; 16]))),
                Key::from(
                    signed_document(realm_id(), &owned(2, group_id), 9)
                        .to_bytes()
                        .expect("document encodes"),
                ),
            ),
            (
                Key::from(placement_policy_key(Ulid::from_bytes([3u8; 16]))),
                Key::from(
                    signed_document(realm_id(), &owned(3, Ulid::from_bytes([8u8; 16])), 9)
                        .to_bytes()
                        .expect("document encodes"),
                ),
            ),
        ];
        let operation = ListPoliciesOperation::new(ListPoliciesInput {
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id()),
                realm_id: realm_id(),
                path_restrictions: None,
                session: None,
            },
            group_id: Some(group_id),
            start_after: None,
            limit: 16,
        });
        let ids: Vec<_> = listed(operation, rows, None)
            .policies
            .iter()
            .map(|document| document.policy.policy_id)
            .collect();
        assert_eq!(
            ids,
            vec![Ulid::from_bytes([1u8; 16]), Ulid::from_bytes([2u8; 16])]
        );
    }

    #[test]
    fn reports_empty_set() {
        let page = page(&[], false);

        assert!(page.policies.is_empty());
        assert!(page.complete);
        assert!(page.cursor.is_none());
    }

    #[test]
    fn denies_non_admin() {
        // Nothing is read for a denied caller.
        let mut operation = operation();
        operation.first().unwrap();
        assert_eq!(
            operation.next(authorized(false)),
            Err(ListPoliciesError::Unauthorized)
        );
    }
}
