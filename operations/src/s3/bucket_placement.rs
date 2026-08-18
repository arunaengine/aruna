//! Bucket default placement refs.
//!
//! The default governs versions minted after it is set; it never rewrites a
//! stored version. This operation owns the generation bump, because
//! `BucketInfo::with_policies` validates the set without advancing it. Only a
//! realm administrator may set it, and every ref is authenticated through the
//! ordinary read path before it becomes a default.

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, BucketInfo, Permission, PlacementPolicyError, PlacementPolicyRef,
    policy_admin_path,
};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement_policy::read::ReadPolicyError;
use crate::placement_policy::resolve_set::{PolicySetResolver, ResolveMode, ResolveStep};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutBucketPlacementInput {
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutPlacementState {
    Init,
    Authorize,
    Resolve,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutBucketPlacementError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("caller may not administer the realm configuration")]
    Unauthorized,
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
    #[error("no transaction found")]
    NoTransactionFound,
    #[error("unexpected event in state {state}")]
    InvalidStateEvent { state: &'static str },
}

/// Sets the bucket default ref set and advances `placement_policy_generation`
/// in the same transaction, so a write that read an older default is detectable.
#[derive(Debug, PartialEq)]
pub struct PutBucketPlacementOperation {
    input: PutBucketPlacementInput,
    state: PutPlacementState,
    resolver: Option<PolicySetResolver>,
    txn_id: Option<TxnId>,
    output: Option<Result<BucketPlacementDefault, PutBucketPlacementError>>,
}

impl PutBucketPlacementOperation {
    pub fn new(input: PutBucketPlacementInput) -> Self {
        Self {
            input,
            state: PutPlacementState::Init,
            resolver: None,
            txn_id: None,
            output: None,
        }
    }

    fn bucket_key(&self) -> Key {
        self.input.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PutPlacementState::Init => "Init",
            PutPlacementState::Authorize => "Authorize",
            PutPlacementState::Resolve => "Resolve",
            PutPlacementState::StartTransaction => "StartTransaction",
            PutPlacementState::ReadBucket => "ReadBucket",
            PutPlacementState::WriteBucket => "WriteBucket",
            PutPlacementState::CommitTransaction => "CommitTransaction",
            PutPlacementState::Finish => "Finish",
            PutPlacementState::Error => "Error",
        }
    }

    /// Every ref is authenticated before it can become a default; an empty set
    /// clears the default and needs no resolution.
    fn resolve_refs(&mut self) -> Effects {
        let mut resolver = PolicySetResolver::new(
            self.input.auth_context.realm_id,
            self.input.local_node_id,
            self.input.now_ms,
            ResolveMode::Strict,
            &self.input.policies,
        );
        let step = resolver.start();
        self.resolver = Some(resolver);
        self.state = PutPlacementState::Resolve;
        self.after_resolve(step)
    }

    fn after_resolve(&mut self, step: ResolveStep) -> Effects {
        match step {
            ResolveStep::Pending(effects) => effects,
            ResolveStep::Done => {
                self.resolver = None;
                self.state = PutPlacementState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            ResolveStep::Failed(policy_ref, source) => {
                self.resolver = None;
                self.fail(PutBucketPlacementError::PolicyUnavailable {
                    policy_id: policy_ref.policy_id,
                    source,
                })
            }
        }
    }

    fn fail(&mut self, error: PutBucketPlacementError) -> Effects {
        self.state = PutPlacementState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn invalid_event(&mut self) -> Effects {
        let state = self.state_name();
        self.fail(PutBucketPlacementError::InvalidStateEvent { state })
    }

    fn finish(&mut self, default: BucketPlacementDefault) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(PutBucketPlacementError::NoTransactionFound);
        };
        self.output = Some(Ok(default));
        self.state = PutPlacementState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_bucket_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.invalid_event();
        };
        let Some(value) = value else {
            return self.fail(PutBucketPlacementError::NoSuchBucket);
        };
        let current = match BucketInfo::from_bytes(value.as_ref()) {
            Ok(current) => current,
            Err(error) => return self.fail(error.into()),
        };
        if current.group_id != self.input.group_id {
            return self.fail(PutBucketPlacementError::GroupMismatch);
        }
        if let Some(expected) = self.input.expected_generation
            && expected != current.placement_policy_generation
        {
            return self.fail(PutBucketPlacementError::GenerationConflict {
                expected,
                current: current.placement_policy_generation,
            });
        }
        let generation = current.placement_policy_generation;
        let previous = current.placement_policies.clone();
        let updated = match current.with_policies(self.input.policies.clone()) {
            Ok(updated) => updated,
            Err(error) => return self.fail(error.into()),
        };
        // An unchanged default is not a change: replay must not inflate the
        // generation and supersede runs that sealed the same refs.
        if updated.placement_policies == previous {
            return self.finish(BucketPlacementDefault {
                policies: previous,
                generation,
            });
        }
        match generation.checked_add(1) {
            Some(next) => self.write_default(updated, next),
            None => self.fail(PutBucketPlacementError::GenerationExhausted),
        }
    }

    fn write_default(&mut self, mut updated: BucketInfo, generation: u64) -> Effects {
        updated.placement_policy_generation = generation;
        let policies = updated.placement_policies.clone();
        let value = match updated.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.output = Some(Ok(BucketPlacementDefault {
            policies,
            generation,
        }));
        self.state = PutPlacementState::WriteBucket;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket_key(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for PutBucketPlacementOperation {
    type Output = BucketPlacementDefault;
    type Error = PutBucketPlacementError;

    fn start(&mut self) -> Effects {
        self.state = PutPlacementState::Authorize;
        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: policy_admin_path(self.input.auth_context.realm_id),
            required_permission: Permission::WRITE,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(auth_config),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PutPlacementState::Init => self.start(),
            PutPlacementState::Authorize => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.invalid_event();
                };
                match allowed {
                    Ok(true) => self.resolve_refs(),
                    Ok(false) => self.fail(PutBucketPlacementError::Unauthorized),
                    Err(error) => {
                        warn!(error = %error, "Bucket placement authorization check failed");
                        self.fail(PutBucketPlacementError::Unauthorized)
                    }
                }
            }
            PutPlacementState::Resolve => {
                let Some(resolver) = self.resolver.as_mut() else {
                    return self.invalid_event();
                };
                let step = resolver.step(event);
                self.after_resolve(step)
            }
            PutPlacementState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.invalid_event();
                };
                self.txn_id = Some(txn_id);
                self.state = PutPlacementState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.bucket_key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutPlacementState::ReadBucket => self.handle_bucket_read(event),
            PutPlacementState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.invalid_event();
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutBucketPlacementError::NoTransactionFound);
                };
                self.state = PutPlacementState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutPlacementState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.invalid_event();
                };
                self.txn_id = None;
                self.state = PutPlacementState::Finish;
                smallvec![]
            }
            PutPlacementState::Finish => smallvec![],
            PutPlacementState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutPlacementState::Finish | PutPlacementState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(result) => result,
            None => Err(PutBucketPlacementError::InvalidStateEvent { state: "Finish" }),
        }
    }

    fn abort(&mut self) -> Effects {
        let mut effects = match self.resolver.as_mut() {
            Some(resolver) => resolver.abort(),
            None => smallvec![],
        };
        if let Some(txn_id) = self.txn_id.take() {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        effects
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            PutBucketPlacementError::Unauthorized
                | PutBucketPlacementError::NoSuchBucket
                | PutBucketPlacementError::GroupMismatch
                | PutBucketPlacementError::GenerationConflict { .. }
                | PutBucketPlacementError::PolicyUnavailable { .. }
                | PutBucketPlacementError::Policy(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{PutBucketPlacementError, PutBucketPlacementInput, PutBucketPlacementOperation};
    use crate::placement_policy::cache::PolicyCacheEntry;
    use crate::placement_policy::fixtures::signed_document;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, BucketInfo, PlacementPolicy, PlacementPolicyRef, PlacementSelector, RealmId,
        VerifiedPolicy,
    };
    use aruna_core::types::{NodeId, UserId};
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

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
        }
    }

    fn operation(
        policies: &[VerifiedPolicy],
        expected_generation: Option<u64>,
    ) -> PutBucketPlacementOperation {
        PutBucketPlacementOperation::new(PutBucketPlacementInput {
            bucket: "bucket".to_string(),
            group_id: group_id(),
            policies: policies.iter().map(VerifiedPolicy::policy_ref).collect(),
            expected_generation,
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id()),
                realm_id: realm_id(),
                path_restrictions: None,
            },
            local_node_id: node_id(),
            now_ms: 1_000,
        })
    }

    fn authorized(allowed: bool) -> Event {
        Event::SubOperation(SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        })
    }

    /// A durable cache hit, so a unit test resolves a ref without a holder.
    fn cached(policy: &VerifiedPolicy) -> Event {
        let entry = PolicyCacheEntry::verified(&signed_document(realm_id(), policy, 9), 10);
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(entry.to_bytes().expect("entry encodes").into()),
        })
    }

    /// Drives authorization and ref resolution up to the open transaction.
    fn started(operation: &mut PutBucketPlacementOperation, policies: &[VerifiedPolicy]) {
        operation.start();
        operation.step(authorized(true));
        let mut refs: Vec<&VerifiedPolicy> = policies.iter().collect();
        refs.sort_by_key(|policy| policy.policy_ref());
        for policy in refs {
            operation.step(cached(policy));
        }
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
    }

    fn read(bucket: &BucketInfo) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(bucket.to_bytes().expect("bucket encodes").into()),
        })
    }

    #[test]
    fn bumps_generation_once() {
        let policies = vec![policy(6), policy(1)];
        let mut operation = operation(&policies, Some(3));
        started(&mut operation, &policies);
        let effects = operation.step(read(&stored(Vec::new())));

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
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
        // Replay must not inflate the generation and supersede sealed runs.
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
            Err(PutBucketPlacementError::GenerationConflict {
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

        assert_eq!(
            operation.finalize(),
            Err(PutBucketPlacementError::GroupMismatch)
        );
    }

    #[test]
    fn denies_non_admin() {
        // Setting a default is a realm-configuration change, so it is refused
        // before the bucket is even read.
        let policies = vec![policy(1)];
        let mut operation = operation(&policies, None);
        operation.start();
        let effects = operation.step(authorized(false));

        assert!(effects.is_empty(), "nothing is opened for a denied caller");
        assert_eq!(
            operation.finalize(),
            Err(PutBucketPlacementError::Unauthorized)
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
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        let key: aruna_core::types::Key = Vec::new().into();
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(key.clone(), None), (key.clone(), None), (key, None)],
        }));

        assert!(effects.is_empty(), "a failed resolve opens no transaction");
        assert!(matches!(
            operation.finalize(),
            Err(PutBucketPlacementError::PolicyUnavailable { .. })
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
            Err(PutBucketPlacementError::GenerationExhausted)
        );
    }
}
