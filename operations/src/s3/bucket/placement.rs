//! Sets the bucket default placement refs and bumps the placement policy generation.
//! Realm admins only; the default applies to later versions and never rewrites stored ones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::policy::document::{group_admin_path, policy_admin_path};
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::policy::foreign_owner;
use crate::placement::policy::read::ReadPolicyError;
use crate::placement::policy::resolve_set::{
    PolicySetError, PolicySetResolver, Resolutions, ResolveMode,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutPlacementInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub policies: Vec<PlacementPolicyRef>,
    /// The generation the caller read. `Some` makes the change a compare-and-set,
    /// so a concurrent default change is refused instead of silently overwritten.
    pub expected_generation: Option<u64>,
    pub auth_context: AuthContext,
    /// This node, so a ref it holds itself is not fetched from a peer.
    pub local_node_id: NodeId,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketPlacementDefault {
    pub policies: Vec<PlacementPolicyRef>,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutPlacementState {
    Authorize,
    AuthorizeGroup,
    Resolve,
    Write,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutPlacementError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("caller may not administer placement for this bucket")]
    Unauthorized,
    /// A group-owned rule governs only its owner's buckets.
    #[error("placement policy {policy_id} belongs to another group")]
    ForeignPolicy { policy_id: Ulid },
    /// The ref was not authenticated, so it never became a default.
    #[error("placement policy {policy_id} could not be resolved")]
    PolicyUnavailable {
        policy_id: Ulid,
        #[source]
        source: ReadPolicyError,
    },
    #[error("the bucket changed owner while the default was written")]
    GroupMismatch,
    #[error("bucket default generation is {current}, not the expected {expected}")]
    GenerationConflict { expected: u64, current: u64 },
    /// A mutation may never leave the generation unchanged, so an exhausted
    /// counter fails the write instead of silently reusing its value.
    #[error("bucket default generation is exhausted")]
    GenerationExhausted,
    #[error("the policy refs could not be read: {0}")]
    Resolve(IoError),
}

impl From<IoError> for PutPlacementError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type PlacementTurn = Result<Next<BucketPlacementDefault>, PutPlacementError>;

/// Sets the bucket default ref set after checking the caller and every ref.
#[derive(Debug, PartialEq)]
pub struct PutPlacementOperation {
    input: PutPlacementInput,
    state: PutPlacementState,
}

impl PutPlacementOperation {
    pub fn new(input: PutPlacementInput) -> Self {
        Self {
            input,
            state: PutPlacementState::Authorize,
        }
    }

    fn call(&mut self, state: PutPlacementState, call: IoCall) -> PlacementTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn authorize(&mut self, state: PutPlacementState, path: String) -> PlacementTurn {
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path,
            required_permission: Permission::WRITE,
        });
        self.call(state, IoCall::child(check))
    }

    /// A failed check counts as a denial.
    fn allowed(result: Result<bool, AuthorizationError>) -> Result<bool, PutPlacementError> {
        result.map_err(|error| {
            warn!(error = %error, "Bucket placement authorization check failed");
            PutPlacementError::Unauthorized
        })
    }

    /// Every ref is authenticated before it can become a default; an empty set
    /// clears the default and needs no resolution.
    fn resolve_refs(&mut self) -> PlacementTurn {
        let resolver = PolicySetResolver::new(
            self.input.auth_context.realm_id,
            self.input.local_node_id,
            self.input.now_ms,
            ResolveMode::Strict,
            &self.input.policies,
        );
        self.call(PutPlacementState::Resolve, IoCall::child(resolver))
    }

    fn resolved(&mut self, result: Result<Resolutions, PolicySetError>) -> PlacementTurn {
        let resolved = match result {
            Ok(resolved) => resolved,
            Err(PolicySetError::Unresolved { policy_ref, source }) => {
                return Err(PutPlacementError::PolicyUnavailable {
                    policy_id: policy_ref.policy_id,
                    source,
                });
            }
            Err(PolicySetError::Io(error)) => return Err(PutPlacementError::Resolve(error)),
        };
        if let Some(policy_id) = foreign_owner(&resolved, self.input.group_id) {
            return Err(PutPlacementError::ForeignPolicy { policy_id });
        }
        let write = WriteDefault {
            input: self.input.clone(),
            written: None,
        };
        self.call(PutPlacementState::Write, IoCall::child(write))
    }
}

impl Operation for PutPlacementOperation {
    type Output = BucketPlacementDefault;
    type Error = PutPlacementError;

    fn first(&mut self) -> PlacementTurn {
        let path = policy_admin_path(self.input.auth_context.realm_id);
        self.authorize(PutPlacementState::Authorize, path)
    }

    fn next(&mut self, result: IoResult) -> PlacementTurn {
        match self.state {
            PutPlacementState::Authorize => {
                if Self::allowed(result.child::<bool, AuthorizationError>()?)? {
                    return self.resolve_refs();
                }
                // A group administrator governs that group's own buckets.
                let realm_id = self.input.auth_context.realm_id;
                let path = group_admin_path(realm_id, self.input.group_id);
                self.authorize(PutPlacementState::AuthorizeGroup, path)
            }
            PutPlacementState::AuthorizeGroup => {
                match Self::allowed(result.child::<bool, AuthorizationError>()?)? {
                    true => self.resolve_refs(),
                    false => Err(PutPlacementError::Unauthorized),
                }
            }
            PutPlacementState::Resolve => {
                self.resolved(result.child::<Resolutions, PolicySetError>()?)
            }
            PutPlacementState::Write => {
                let default = result.child::<BucketPlacementDefault, PutPlacementError>()??;
                Ok(Next::Done(default))
            }
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            PutPlacementError::Unauthorized
                | PutPlacementError::NoSuchBucket
                | PutPlacementError::GroupMismatch
                | PutPlacementError::GenerationConflict { .. }
                | PutPlacementError::PolicyUnavailable { .. }
                | PutPlacementError::Policy(_)
        )
    }
}

/// Stores the default ref set and advances `placement_policy_generation` in one transaction,
/// so a write that read an older default is detectable.
#[derive(Debug, PartialEq)]
struct WriteDefault {
    input: PutPlacementInput,
    written: Option<BucketPlacementDefault>,
}

impl WriteDefault {
    fn bucket_key(&self) -> Key {
        self.input.bucket.as_bytes().to_vec().into()
    }

    fn bucket_read(&mut self, value: Option<Value>) -> PlacementTurn {
        let value = value.ok_or(PutPlacementError::NoSuchBucket)?;
        let current = BucketInfo::from_bytes(value.as_ref())?;
        if current.group_id != self.input.group_id {
            return Err(PutPlacementError::GroupMismatch);
        }
        let generation = current.placement_policy_generation;
        if let Some(expected) = self.input.expected_generation
            && expected != generation
        {
            return Err(PutPlacementError::GenerationConflict {
                expected,
                current: generation,
            });
        }
        let previous = current.placement_policies.clone();
        let mut updated = current.with_policies(self.input.policies.clone())?;
        // An unchanged default is not a change: replay must not inflate the
        // generation and supersede runs that stored the same refs.
        if updated.placement_policies == previous {
            return Ok(Next::Done(BucketPlacementDefault {
                policies: previous,
                generation,
            }));
        }
        let generation = generation
            .checked_add(1)
            .ok_or(PutPlacementError::GenerationExhausted)?;
        updated.placement_policy_generation = generation;
        let row = (
            S3_BUCKET_KEYSPACE.to_string(),
            self.bucket_key(),
            updated.to_bytes()?.into(),
        );
        self.written = Some(BucketPlacementDefault {
            policies: updated.placement_policies,
            generation,
        });
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for WriteDefault {
    type Output = BucketPlacementDefault;
    type Error = PutPlacementError;

    fn first(&mut self) -> PlacementTurn {
        Ok(Next::Call(read_row(S3_BUCKET_KEYSPACE, self.bucket_key())))
    }

    fn next(&mut self, result: IoResult) -> PlacementTurn {
        match self.written.take() {
            Some(written) => {
                result.done()?;
                Ok(Next::Done(written))
            }
            None => self.bucket_read(result.value()?),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::events::Event;
    use aruna_core::id::NodeId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::{
        PlacementPolicy, PlacementSelector, VerifiedPolicy,
    };
    use aruna_core::structs::storage::format::Compression;
    use std::time::UNIX_EPOCH;

    fn group_id() -> Ulid {
        Ulid::from_bytes([2u8; 16])
    }

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
                node_id: None,
                location: Some("eu-west".to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn stored(policies: Vec<PlacementPolicyRef>) -> BucketInfo {
        BucketInfo {
            group_id: group_id(),
            created_at: UNIX_EPOCH,
            created_by: UserId::nil(realm_id()),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: policies,
            placement_policy_generation: 3,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn input(policies: &[VerifiedPolicy], expected_generation: Option<u64>) -> PutPlacementInput {
        PutPlacementInput {
            bucket: "bucket".to_string(),
            group_id: group_id(),
            policies: policies.iter().map(VerifiedPolicy::policy_ref).collect(),
            expected_generation,
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id()),
                realm_id: realm_id(),
                path_restrictions: None,
                session: None,
            },
            local_node_id: node_id(),
            now_ms: 1_000,
        }
    }

    fn authorized(allowed: bool) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, AuthorizationError>(allowed)))
    }

    fn resolved(policies: &[VerifiedPolicy]) -> IoResult {
        let Event::Child(outcome) = crate::tests::policy::resolved_set(policies) else {
            panic!("expected a child outcome");
        };
        IoResult::Child(outcome)
    }

    fn is_child(turn: &PlacementTurn) -> bool {
        matches!(turn, Ok(Next::Call(IoCall::Child(_))))
    }

    /// The write transaction of `policies`, answered with `bucket`.
    fn written(
        policies: &[VerifiedPolicy],
        expected_generation: Option<u64>,
        bucket: &BucketInfo,
    ) -> (WriteDefault, PlacementTurn) {
        let mut write = WriteDefault {
            input: input(policies, expected_generation),
            written: None,
        };
        assert_eq!(write.transaction(), TransactionMode::Write);
        assert!(matches!(write.first(), Ok(Next::Call(IoCall::Read(_)))));
        let value = bucket.to_bytes().expect("bucket encodes").into();
        let turn = write.next(IoResult::Values(vec![Some(value)]));
        (write, turn)
    }

    #[test]
    fn bumps_generation_once() {
        let policies = vec![policy(6), policy(1)];
        let (mut write, turn) = written(&policies, Some(3), &stored(Vec::new()));
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("expected one bucket write");
        };
        let written = BucketInfo::from_bytes(value.as_ref()).expect("bucket decodes");
        assert_eq!(written.placement_policy_generation, 4);
        assert_eq!(
            written.placement_policies,
            PlacementPolicyRef::canonical_set(
                &policies
                    .iter()
                    .map(VerifiedPolicy::policy_ref)
                    .collect::<Vec<_>>()
            )
            .expect("refs are canonical")
        );
    }

    #[test]
    fn skips_unchanged_default() {
        // Replay must not inflate the generation and supersede stored runs.
        let policies = vec![policy(1)];
        let mut operation = operation(&policies, None);
        started(&mut operation, &policies);
        let effects = operation.step(read(&stored(vec![policies[0].policy_ref()])));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
        let default = operation.finalize().expect("default returned");
        assert_eq!(default.generation, 3);
    }

    #[test]
    fn rejects_stale_generation() {
        let policies = vec![policy(1)];
        let mut operation = operation(&policies, Some(2));
        started(&mut operation, &policies);
        let effects = operation.step(read(&stored(Vec::new())));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize(),
            Err(PutPlacementError::GenerationConflict {
                expected: 2,
                current: 3
            })
        );
    }

    #[test]
    fn rejects_other_group() {
        let mut operation = operation(&[], None);
        started(&mut operation, &[]);
        let mut foreign = stored(Vec::new());
        foreign.group_id = Ulid::from_bytes([9u8; 16]);
        operation.step(read(&foreign));

        assert_eq!(operation.finalize(), Err(PutPlacementError::GroupMismatch));
    }

    #[test]
    fn denies_non_admin() {
        // Neither realm-configuration write nor admin write on the bucket's
        // group: nothing is opened for the caller.
        let policies = vec![policy(1)];
        let mut operation = operation(&policies, None);
        operation.start();
        operation.step(authorized(false));
        let effects = operation.step(authorized(false));

        assert!(effects.is_empty(), "nothing is opened for a denied caller");
        assert_eq!(operation.finalize(), Err(PutPlacementError::Unauthorized));
    }

    fn owned_policy(seed: u8, owner: Ulid) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some("eu-west".to_string()),
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
    fn accepts_group_admin() {
        // A group administrator governs that group's own bucket, and its own
        // rule is a legal reference there.
        let policies = vec![owned_policy(1, group_id())];
        let mut operation = operation(&policies, None);
        operation.start();
        operation.step(authorized(false));
        operation.step(authorized(true));
        operation.step(crate::tests::policy::resolved_set(&policies));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
    }

    #[test]
    fn refuses_foreign_owner() {
        // A rule another group owns never governs this bucket, and the refusal
        // lands before any transaction is opened.
        let foreign = Ulid::from_bytes([8u8; 16]);
        let policies = vec![owned_policy(1, foreign)];
        let mut operation = operation(&policies, None);
        operation.start();
        operation.step(authorized(true));
        let effects = operation.step(crate::tests::policy::resolved_set(&policies));

        assert!(effects.is_empty(), "a foreign rule opens no transaction");
        assert_eq!(
            operation.finalize(),
            Err(PutPlacementError::ForeignPolicy {
                policy_id: Ulid::from_bytes([1u8; 16])
            })
        );
    }

    #[test]
    fn refuses_unresolved_ref() {
        // An unauthenticated ref must never become a default, and no bucket
        // record may be touched on the way there.
        let policies = vec![policy(1)];
        let mut operation = operation(&policies, None);
        operation.start();
        operation.step(authorized(true));
        let unresolved = crate::tests::policy::unresolved_set(policies[0].policy_ref());
        let effects = operation.step(unresolved);

        assert!(effects.is_empty(), "a failed resolve opens no transaction");
        assert!(matches!(
            operation.finalize(),
            Err(PutPlacementError::PolicyUnavailable { .. })
        ));
    }

    #[test]
    fn refuses_exhausted_generation() {
        let policies = vec![policy(1)];
        let mut operation = operation(&policies, None);
        started(&mut operation, &policies);
        let mut bucket = stored(Vec::new());
        bucket.placement_policy_generation = u64::MAX;
        let effects = operation.step(read(&bucket));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize(),
            Err(PutPlacementError::GenerationExhausted)
        );
    }
}
