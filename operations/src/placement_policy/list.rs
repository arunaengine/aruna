//! Responder-local listing of the placement policies this node holds.
//!
//! Documents replicate only to the holders their policy id resolves to, so a
//! page names what this node stores and never claims to be the realm catalog.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::PLACEMENT_POLICY_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, Permission, PlacementPolicyDocument, RealmId, policy_admin_path,
};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

/// Page size a listing uses when the caller names none.
pub const POLICY_LIST_DEFAULT: usize = 50;
/// Upper bound on one listing page.
pub const POLICY_LIST_LIMIT: usize = 200;

#[derive(Clone, Debug, PartialEq)]
pub struct ListPoliciesInput {
    pub auth_context: AuthContext,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListState {
    Init,
    Authorize,
    Scan,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ListPoliciesOperation {
    input: ListPoliciesInput,
    state: ListState,
    output: Option<Result<PolicyListPage, ListPoliciesError>>,
}

impl ListPoliciesOperation {
    pub fn new(input: ListPoliciesInput) -> Self {
        Self {
            input,
            state: ListState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ListPoliciesError) -> Effects {
        self.state = ListState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn scan(&mut self) -> Effects {
        self.state = ListState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: PLACEMENT_POLICY_KEYSPACE.to_string(),
            prefix: None,
            start: self.input.start_after.clone().map(IterStart::After),
            limit: self.input.limit.clamp(1, POLICY_LIST_LIMIT),
            txn_id: None,
        })]
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.fail(ListPoliciesError::InvalidEvent);
        };
        let realm_id: RealmId = self.input.auth_context.realm_id;
        let mut policies = Vec::with_capacity(values.len());
        for (_, value) in values {
            let document = match PlacementPolicyDocument::from_bytes(value.as_ref()) {
                Ok(document) => document,
                Err(error) => return self.fail(error.into()),
            };
            // A row stored for another realm is not this caller's rule.
            if document.realm_id == realm_id {
                policies.push(document);
            }
        }
        self.output = Some(Ok(PolicyListPage {
            policies,
            complete: next_start_after.is_none(),
            cursor: next_start_after,
        }));
        self.state = ListState::Finish;
        smallvec![]
    }
}

impl Operation for ListPoliciesOperation {
    type Output = PolicyListPage;
    type Error = ListPoliciesError;

    fn start(&mut self) -> Effects {
        self.state = ListState::Authorize;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: policy_admin_path(self.input.auth_context.realm_id),
            required_permission: Permission::READ,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(auth_config),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            ListState::Init => self.start(),
            ListState::Authorize => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.fail(ListPoliciesError::InvalidEvent);
                };
                match allowed {
                    Ok(true) => self.scan(),
                    Ok(false) => self.fail(ListPoliciesError::Unauthorized),
                    Err(error) => {
                        warn!(error = %error, "Policy listing authorization check failed");
                        self.fail(ListPoliciesError::Unauthorized)
                    }
                }
            }
            ListState::Scan => self.handle_page(event),
            ListState::Finish | ListState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListState::Finish | ListState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ListPoliciesError::InvalidEvent))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, ListPoliciesError::Unauthorized)
    }
}

#[cfg(test)]
mod tests {
    use super::{ListPoliciesError, ListPoliciesInput, ListPoliciesOperation, PolicyListPage};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, PlacementPolicy, PlacementSelector, RealmId, VerifiedPolicy,
        placement_policy_key,
    };
    use aruna_core::types::{Key, NodeId, UserId};
    use ulid::Ulid;

    use crate::placement_policy::fixtures::signed_document;

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
            start_after: None,
            limit: 16,
        })
    }

    fn authorized(allowed: bool) -> Event {
        Event::SubOperation(SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        })
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
        let mut operation = operation();
        operation.start();
        operation.step(authorized(true));
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: rows,
            next_start_after: next,
        }));
        operation.finalize().expect("page returned")
    }

    #[test]
    fn lists_in_id_order() {
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
    fn reports_empty_set() {
        let page = page(&[], false);

        assert!(page.policies.is_empty());
        assert!(page.complete);
        assert!(page.cursor.is_none());
    }

    #[test]
    fn denies_non_admin() {
        let mut operation = operation();
        operation.start();
        let effects = operation.step(authorized(false));

        assert!(effects.is_empty(), "nothing is read for a denied caller");
        assert_eq!(operation.finalize(), Err(ListPoliciesError::Unauthorized));
    }
}
