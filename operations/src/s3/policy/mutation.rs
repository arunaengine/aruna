//! Attaches an exact policy set to one object as a realm admin mutation.
//! It resolves and authorizes the refs first, then mints a collision checked successor version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{AuthorizationError, ConversionError};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::policy::attachment::PolicyRefMode;
use aruna_core::structs::placement::policy::document::{group_admin_path, policy_admin_path};
use aruna_core::structs::placement::policy::{
    PlacementPolicyRef, PlacementSubject, PolicyResolution,
};
use aruna_core::structs::storage::blob::{BucketIdentity, CurrentVersionPointer};
use std::collections::BTreeMap;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::blob::records::HeadAliasContext;
use crate::placement::policy::foreign_owner;
use crate::placement::policy::read::ReadPolicyError;
use crate::placement::policy::resolve_set::{
    PolicySetError, PolicySetResolver, Resolutions, ResolveMode,
};
use crate::s3::policy::successor::{
    MintSuccessorOperation, SuccessorError, SuccessorOutcome, SuccessorPlan,
};

#[derive(Clone, Debug, PartialEq)]
pub struct PolicyMutationConfig {
    pub context: HeadAliasContext,
    pub auth_context: AuthContext,
    /// Replay key: the same id with the same parameters returns the recorded
    /// successor instead of minting a second one.
    pub mutation_id: Ulid,
    /// The exact pointer the caller read; a moved head is a conflict.
    pub expected_head: CurrentVersionPointer,
    /// The bucket record the caller resolved; a recreated bucket conflicts.
    pub bucket_identity: BucketIdentity,
    pub target_refs: Vec<PlacementPolicyRef>,
    pub subject: PlacementSubject,
    pub created_at: SystemTime,
    pub now_ms: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum PolicyMutationError {
    #[error("caller may not administer placement for this bucket")]
    Unauthorized,
    /// A group-owned rule governs only its owner's buckets.
    #[error("placement policy {policy_id} belongs to another group")]
    ForeignPolicy { policy_id: Ulid },
    /// The ref was not authenticated, so no version was minted.
    #[error("placement policy {policy_id} could not be resolved")]
    PolicyUnavailable {
        policy_id: Ulid,
        #[source]
        source: ReadPolicyError,
    },
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Successor(#[from] SuccessorError),
    #[error("unexpected answer during the policy mutation")]
    InvalidEvent,
}

impl From<IoError> for PolicyMutationError {
    fn from(error: IoError) -> Self {
        Self::Successor(SuccessorError::Storage(error.into()))
    }
}

type MutationTurn = Result<Next<SuccessorOutcome>, PolicyMutationError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationState {
    Authorize,
    AuthorizeGroup,
    Resolve,
    Mint,
}

#[derive(Debug, PartialEq)]
pub struct PolicyMutationOperation {
    config: PolicyMutationConfig,
    successor_version_id: Ulid,
    state: MutationState,
}

impl PolicyMutationOperation {
    pub fn new(config: PolicyMutationConfig) -> Self {
        Self::new_with_version(config, Ulid::generate())
    }

    /// Pins the successor id, for a caller that resumes its own mutation.
    pub fn new_with_version(config: PolicyMutationConfig, successor_version_id: Ulid) -> Self {
        Self {
            config,
            successor_version_id,
            state: MutationState::Authorize,
        }
    }

    pub fn successor_version_id(&self) -> Ulid {
        self.successor_version_id
    }

    fn call(&mut self, state: MutationState, call: IoCall) -> MutationTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn authorize(&mut self, state: MutationState, path: String) -> MutationTurn {
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path,
            required_permission: Permission::WRITE,
        });
        self.call(state, IoCall::child(check))
    }

    /// A failed check counts as a denial.
    fn allowed(result: Result<bool, AuthorizationError>) -> bool {
        result.unwrap_or_else(|error| {
            warn!(error = %error, "Policy mutation authorization check failed");
            false
        })
    }

    fn realm_checked(&mut self, result: Result<bool, AuthorizationError>) -> MutationTurn {
        let failed = result.is_err();
        if Self::allowed(result) {
            return self.resolve_refs();
        }
        if failed {
            return Err(PolicyMutationError::Unauthorized);
        }
        // A group administrator governs that group's own objects.
        let (realm_id, group_id) = (
            self.config.auth_context.realm_id,
            self.config.context.group_id,
        );
        self.authorize(
            MutationState::AuthorizeGroup,
            group_admin_path(realm_id, group_id),
        )
    }

    fn group_checked(&mut self, result: Result<bool, AuthorizationError>) -> MutationTurn {
        match Self::allowed(result) {
            true => self.resolve_refs(),
            false => Err(PolicyMutationError::Unauthorized),
        }
    }

    fn resolve_refs(&mut self) -> MutationTurn {
        let resolver = PolicySetResolver::new(
            self.config.auth_context.realm_id,
            self.config.subject.node_id,
            self.config.now_ms,
            ResolveMode::Strict,
            &self.config.target_refs,
        );
        self.call(MutationState::Resolve, IoCall::child(resolver))
    }

    fn resolved(&mut self, result: Result<Resolutions, PolicySetError>) -> MutationTurn {
        let resolved = match result {
            Ok(resolved) => resolved,
            Err(PolicySetError::Unresolved { policy_ref, source }) => {
                return Err(PolicyMutationError::PolicyUnavailable {
                    policy_id: policy_ref.policy_id,
                    source,
                });
            }
            Err(PolicySetError::Io(_)) => return Err(PolicyMutationError::InvalidEvent),
        };
        if let Some(policy_id) = foreign_owner(&resolved, self.config.context.group_id) {
            return Err(PolicyMutationError::ForeignPolicy { policy_id });
        }
        self.start_mint(resolved)
    }

    fn start_mint(&mut self, resolved: BTreeMap<Ulid, PolicyResolution>) -> MutationTurn {
        let mint = MintSuccessorOperation::new(SuccessorPlan {
            context: self.config.context.clone(),
            mutation_id: self.config.mutation_id,
            expected_head: self.config.expected_head.clone(),
            bucket_identity: self.config.bucket_identity,
            target_refs: self.config.target_refs.clone(),
            // The explicit admin mutation is an exact replacement; only the
            // bucket-default pass unions.
            mode: PolicyRefMode::Replace,
            successor_version_id: self.successor_version_id,
            created_at: self.config.created_at,
            auth_context: self.config.auth_context.clone(),
            subject: self.config.subject.clone(),
            resolved,
            intent: None,
            captured_default: None,
        });
        self.call(MutationState::Mint, IoCall::child(mint))
    }
}

impl Operation for PolicyMutationOperation {
    type Output = SuccessorOutcome;
    type Error = PolicyMutationError;

    fn first(&mut self) -> MutationTurn {
        let path = policy_admin_path(self.config.auth_context.realm_id);
        self.authorize(MutationState::Authorize, path)
    }

    fn next(&mut self, result: IoResult) -> MutationTurn {
        match self.state {
            MutationState::Authorize => {
                self.realm_checked(result.child::<bool, AuthorizationError>()?)
            }
            MutationState::AuthorizeGroup => {
                self.group_checked(result.child::<bool, AuthorizationError>()?)
            }
            MutationState::Resolve => self.resolved(result.child::<Resolutions, PolicySetError>()?),
            MutationState::Mint => {
                let minted = result.child::<SuccessorOutcome, SuccessorError>()??;
                Ok(Next::Done(minted))
            }
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        match error {
            PolicyMutationError::Unauthorized
            | PolicyMutationError::ForeignPolicy { .. }
            | PolicyMutationError::PolicyUnavailable { .. } => true,
            PolicyMutationError::Successor(error) => MintSuccessorOperation::expected_error(error),
            _ => false,
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::errors::StorageError;
    use aruna_core::events::Event;
    use aruna_core::id::NodeId;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::{
        PlacementPolicy, PlacementSelector, PlacementSubject, VerifiedPolicy,
    };
    use aruna_core::structs::storage::blob::CurrentVersionPointer;
    use std::time::UNIX_EPOCH;

    fn realm_id() -> RealmId {
        RealmId::from_bytes([1u8; 32])
    }

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    fn policy(seed: u8) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "residency".to_string(),
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

    fn config(policies: &[VerifiedPolicy]) -> PolicyMutationConfig {
        PolicyMutationConfig {
            context: HeadAliasContext::new(
                realm_id(),
                Ulid::from_bytes([2u8; 16]),
                node_id(),
                "bucket",
                "object.txt",
            ),
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id()),
                realm_id: realm_id(),
                path_restrictions: None,
                session: None,
            },
            mutation_id: Ulid::from_bytes([8u8; 16]),
            expected_head: CurrentVersionPointer::new_with_generation(
                Ulid::from_bytes([7u8; 16]),
                4,
            ),
            bucket_identity: (
                Ulid::from_bytes([2u8; 16]),
                UNIX_EPOCH,
                UserId::nil(realm_id()),
            ),
            target_refs: policies.iter().map(VerifiedPolicy::policy_ref).collect(),
            subject: PlacementSubject {
                node_id: node_id(),
                generation: 1,
                location: "eu-west".to_string(),
                labels: BTreeMap::new(),
                executor_kind: None,
                local_to_controller: true,
            },
            created_at: UNIX_EPOCH,
            now_ms: 1_000,
        }
    }

    fn authorized(allowed: bool) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizationError>(allowed)))
    }

    /// The answer of the policy resolver, as the shared fixtures build it.
    fn answer(event: Event) -> IoResult {
        let Event::Child(outcome) = event else {
            panic!("expected a child outcome");
        };
        IoResult::Child(outcome)
    }

    fn resolved(policy: &VerifiedPolicy) -> IoResult {
        answer(crate::tests::policy::resolved_set(std::slice::from_ref(
            policy,
        )))
    }

    fn is_child(turn: &MutationTurn) -> bool {
        matches!(turn, Ok(Next::Call(IoCall::Child(_))))
    }

    fn group_id() -> Ulid {
        Ulid::from_bytes([2u8; 16])
    }

    fn owned(seed: u8, owner: Ulid) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "residency".to_string(),
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
    }

    #[test]
    fn denies_non_admin() {
        // Neither realm-configuration write nor admin write on the object's group leaves
        // nothing to open.
        let mut operation = PolicyMutationOperation::new(config(&[policy(1)]));
        assert!(is_child(&operation.first()));
        assert!(is_child(&operation.next(authorized(false))));
        assert_eq!(
            operation.next(authorized(false)),
            Err(PolicyMutationError::Unauthorized)
        );
    }

    #[test]
    fn failed_check_denies() {
        // A check that cannot run never falls through to the group check.
        let mut operation = PolicyMutationOperation::new(config(&[policy(1)]));
        operation.first().unwrap();
        let failed = Err::<bool, _>(AuthorizationError::DocNotFound);
        assert_eq!(
            operation.next(IoResult::Child(ChildOutcome::new(failed))),
            Err(PolicyMutationError::Unauthorized)
        );
    }

    #[test]
    fn accepts_group_admin() {
        // The bucket's own group administers its objects' rules.
        let policy = owned(1, group_id());
        let mut operation = PolicyMutationOperation::new(config(std::slice::from_ref(&policy)));
        operation.first().unwrap();
        operation.next(authorized(false)).unwrap();
        operation.next(authorized(true)).unwrap();
        assert!(is_child(&operation.next(resolved(&policy))));
        assert_eq!(operation.state, MutationState::Mint);
    }

    #[test]
    fn refuses_foreign_owner() {
        // A rule another group owns never governs this object.
        let policy = owned(1, Ulid::from_bytes([9u8; 16]));
        let mut operation = PolicyMutationOperation::new(config(std::slice::from_ref(&policy)));
        operation.first().unwrap();
        operation.next(authorized(true)).unwrap();
        assert_eq!(
            operation.next(resolved(&policy)),
            Err(PolicyMutationError::ForeignPolicy {
                policy_id: Ulid::from_bytes([1u8; 16])
            })
        );
    }

    #[test]
    fn refuses_unresolved_ref() {
        // An unauthenticated ref must not reach a mint transaction.
        let mut operation = PolicyMutationOperation::new(config(&[policy(1)]));
        operation.first().unwrap();
        operation.next(authorized(true)).unwrap();
        let unresolved = crate::tests::policy::unresolved_set(policy(1).policy_ref());
        assert!(matches!(
            operation.next(answer(unresolved)),
            Err(PolicyMutationError::PolicyUnavailable { .. })
        ));
    }

    #[test]
    fn mints_after_resolve() {
        // An authorized caller with an authenticated ref reaches the mint, and the mint owns
        // the assigned successor id.
        let policy = policy(1);
        let mut operation = PolicyMutationOperation::new(config(std::slice::from_ref(&policy)));
        operation.first().unwrap();
        operation.next(authorized(true)).unwrap();
        assert!(is_child(&operation.next(resolved(&policy))));
        assert!(!operation.successor_version_id().is_nil());
    }

    #[test]
    fn mint_failure_surfaces() {
        // The mint's typed error reaches the caller; the child cleans up after itself.
        let policy = policy(1);
        let mut operation = PolicyMutationOperation::new(config(std::slice::from_ref(&policy)));
        operation.first().unwrap();
        operation.next(authorized(true)).unwrap();
        operation.next(resolved(&policy)).unwrap();
        let failed =
            Err::<SuccessorOutcome, _>(SuccessorError::Storage(StorageError::CommitFailed));
        assert_eq!(
            operation.next(IoResult::Child(ChildOutcome::new(failed))),
            Err(PolicyMutationError::Successor(SuccessorError::Storage(
                StorageError::CommitFailed
            )))
        );
    }
}
