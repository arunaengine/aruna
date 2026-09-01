//! The realm-admin mutation that attaches an exact policy set to one object.
//!
//! Authorization and ref authentication run here, before the transactional mint
//! is started: a caller who may not administer the realm, or a ref that cannot
//! be authenticated, never reaches a write. The successor VersionId is owned by
//! this operation and collision-checked inside the mint transaction.

use aruna_core::effects::Effect;
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, BucketIdentity, CurrentVersionPointer, Permission, PlacementPolicyRef,
    PlacementSubject, PolicyRefMode, PolicyResolution, group_admin_path, policy_admin_path,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::blob::blob_keyspace_helper::HeadAliasContext;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement_policy::foreign_owner;
use crate::placement_policy::read::ReadPolicyError;
use crate::placement_policy::resolve_set::{PolicySetResolver, ResolveMode, ResolveStep};
use crate::s3::policy_successor::{
    MintPolicySuccessorOperation, SuccessorError, SuccessorOutcome, SuccessorPlan,
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
    #[error("unexpected event during the policy mutation")]
    InvalidEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationState {
    Init,
    AuthorizeGroup,
    Authorize,
    Resolve,
    Mint,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct PolicyMutationOperation {
    config: PolicyMutationConfig,
    successor_version_id: Ulid,
    state: MutationState,
    resolver: Option<PolicySetResolver>,
    resolved: BTreeMap<Ulid, PolicyResolution>,
    mint: Option<MintPolicySuccessorOperation>,
    output: Option<Result<SuccessorOutcome, PolicyMutationError>>,
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
            state: MutationState::Init,
            resolver: None,
            resolved: BTreeMap::new(),
            mint: None,
            output: None,
        }
    }

    pub fn successor_version_id(&self) -> Ulid {
        self.successor_version_id
    }

    fn authorize(&self, path: String) -> Effects {
        let auth_config = CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path,
            required_permission: Permission::WRITE,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(auth_config),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn fail(&mut self, error: PolicyMutationError) -> Effects {
        let cleanup = self.abort();
        self.state = MutationState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn resolve_refs(&mut self) -> Effects {
        let mut resolver = PolicySetResolver::new(
            self.config.auth_context.realm_id,
            self.config.subject.node_id,
            self.config.now_ms,
            ResolveMode::Strict,
            &self.config.target_refs,
        );
        let step = resolver.start();
        self.resolver = Some(resolver);
        self.state = MutationState::Resolve;
        self.after_resolve(step)
    }

    fn after_resolve(&mut self, step: ResolveStep) -> Effects {
        match step {
            ResolveStep::Pending(effects) => effects,
            ResolveStep::Done => {
                if let Some(resolver) = self.resolver.take() {
                    self.resolved = resolver.into_resolutions();
                }
                match foreign_owner(&self.resolved, self.config.context.group_id) {
                    Some(policy_id) => self.fail(PolicyMutationError::ForeignPolicy { policy_id }),
                    None => self.start_mint(),
                }
            }
            ResolveStep::Failed(policy_ref, source) => {
                self.resolver = None;
                self.fail(PolicyMutationError::PolicyUnavailable {
                    policy_id: policy_ref.policy_id,
                    source,
                })
            }
        }
    }

    fn start_mint(&mut self) -> Effects {
        let plan = SuccessorPlan {
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
            resolved: self.resolved.clone(),
            intent: None,
            sealed_default: None,
        };
        let mut mint = MintPolicySuccessorOperation::new(plan);
        let effects = mint.start();
        self.mint = Some(mint);
        self.state = MutationState::Mint;
        effects
    }
}

impl Operation for PolicyMutationOperation {
    type Output = SuccessorOutcome;
    type Error = PolicyMutationError;

    fn start(&mut self) -> Effects {
        self.state = MutationState::Authorize;
        self.authorize(policy_admin_path(self.config.auth_context.realm_id))
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            MutationState::Init => self.start(),
            MutationState::Authorize => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.fail(PolicyMutationError::InvalidEvent);
                };
                match allowed {
                    Ok(true) => self.resolve_refs(),
                    // A group administrator governs that group's own objects.
                    Ok(false) => {
                        self.state = MutationState::AuthorizeGroup;
                        self.authorize(group_admin_path(
                            self.config.auth_context.realm_id,
                            self.config.context.group_id,
                        ))
                    }
                    Err(error) => {
                        warn!(error = %error, "Policy mutation authorization check failed");
                        self.fail(PolicyMutationError::Unauthorized)
                    }
                }
            }
            MutationState::AuthorizeGroup => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.fail(PolicyMutationError::InvalidEvent);
                };
                match allowed {
                    Ok(true) => self.resolve_refs(),
                    Ok(false) => self.fail(PolicyMutationError::Unauthorized),
                    Err(error) => {
                        warn!(error = %error, "Policy mutation authorization check failed");
                        self.fail(PolicyMutationError::Unauthorized)
                    }
                }
            }
            MutationState::Resolve => {
                let Some(resolver) = self.resolver.as_mut() else {
                    return self.fail(PolicyMutationError::InvalidEvent);
                };
                let step = resolver.step(event);
                self.after_resolve(step)
            }
            MutationState::Mint => {
                let Some(mint) = self.mint.as_mut() else {
                    return self.fail(PolicyMutationError::InvalidEvent);
                };
                let effects = mint.step(event);
                if !mint.is_complete() {
                    return effects;
                }
                let Some(mint) = self.mint.take() else {
                    return self.fail(PolicyMutationError::InvalidEvent);
                };
                match mint.finalize() {
                    Ok(outcome) => {
                        self.output = Some(Ok(outcome));
                        self.state = MutationState::Finish;
                        effects
                    }
                    // The mint's own cleanup still has to run before the failure
                    // is reported.
                    Err(error) => {
                        let mut cleanup = effects;
                        cleanup.extend(self.fail(error.into()));
                        cleanup
                    }
                }
            }
            MutationState::Finish | MutationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, MutationState::Finish | MutationState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(PolicyMutationError::InvalidEvent))
    }

    fn abort(&mut self) -> Effects {
        let mut effects: Effects = match self.resolver.as_mut() {
            Some(resolver) => resolver.abort(),
            None => smallvec![],
        };
        if let Some(mint) = self.mint.as_mut() {
            effects.extend(mint.abort());
        }
        effects
    }

    fn expected_error(error: &Self::Error) -> bool {
        match error {
            PolicyMutationError::Unauthorized
            | PolicyMutationError::ForeignPolicy { .. }
            | PolicyMutationError::PolicyUnavailable { .. } => {
                true
            }
            PolicyMutationError::Successor(error) => {
                MintPolicySuccessorOperation::expected_error(error)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PolicyMutationConfig, PolicyMutationError, PolicyMutationOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, CurrentVersionPointer, PlacementPolicy, PlacementSelector, PlacementSubject,
        RealmId, VerifiedPolicy,
    };
    use aruna_core::types::{Key, NodeId, UserId};
    use std::collections::BTreeMap;
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

    use crate::blob::blob_keyspace_helper::HeadAliasContext;
    use crate::placement_policy::cache::PolicyCacheEntry;
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

    fn authorized(allowed: bool) -> Event {
        Event::SubOperation(SubOperationEvent::AuthorizationResult {
            allowed: Ok(allowed),
        })
    }

    fn cached(policy: &VerifiedPolicy) -> Event {
        let entry = PolicyCacheEntry::verified(&signed_document(realm_id(), policy, 9), 10);
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(entry.to_bytes().expect("entry encodes").into()),
        })
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
        // Neither realm-configuration write nor admin write on the object's
        // group leaves nothing to open.
        let mut operation = PolicyMutationOperation::new(config(&[policy(1)]));
        operation.start();
        operation.step(authorized(false));
        let effects = operation.step(authorized(false));

        assert!(effects.is_empty(), "a denied caller opens no transaction");
        assert_eq!(operation.finalize(), Err(PolicyMutationError::Unauthorized));
    }

    #[test]
    fn accepts_group_admin() {
        // The bucket's own group administers its objects' rules.
        let policy = owned(1, group_id());
        let mut operation = PolicyMutationOperation::new(config(std::slice::from_ref(&policy)));
        operation.start();
        operation.step(authorized(false));
        operation.step(authorized(true));
        operation.step(cached(&policy));
        let effects = operation.step(crate::placement_policy::fixtures::group_authority(
            realm_id(),
            group_id(),
        ));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
    }

    #[test]
    fn refuses_foreign_owner() {
        // A rule another group owns never governs this object.
        let foreign = Ulid::from_bytes([9u8; 16]);
        let policy = owned(1, foreign);
        let mut operation = PolicyMutationOperation::new(config(std::slice::from_ref(&policy)));
        operation.start();
        operation.step(authorized(true));
        operation.step(cached(&policy));
        let effects = operation.step(crate::placement_policy::fixtures::group_authority(
            realm_id(),
            foreign,
        ));

        assert!(effects.is_empty(), "a foreign rule opens no transaction");
        assert_eq!(
            operation.finalize(),
            Err(PolicyMutationError::ForeignPolicy {
                policy_id: Ulid::from_bytes([1u8; 16])
            })
        );
    }

    #[test]
    fn refuses_unresolved_ref() {
        // An unauthenticated ref must not reach a mint transaction.
        let mut operation = PolicyMutationOperation::new(config(&[policy(1)]));
        operation.start();
        operation.step(authorized(true));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        let key: Key = Vec::new().into();
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(key.clone(), None), (key.clone(), None), (key, None)],
        }));

        assert!(effects.is_empty(), "no transaction is opened");
        assert!(matches!(
            operation.finalize(),
            Err(PolicyMutationError::PolicyUnavailable { .. })
        ));
    }

    #[test]
    fn mints_after_resolve() {
        // An authorized caller with an authenticated ref reaches the mint, and
        // the mint owns the assigned successor id.
        let policy = policy(1);
        let mut operation = PolicyMutationOperation::new(config(std::slice::from_ref(&policy)));
        operation.start();
        operation.step(authorized(true));
        operation.step(cached(&policy));
        let effects = operation.step(crate::placement_policy::fixtures::authority(realm_id()));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        assert!(!operation.successor_version_id().is_nil());
    }
}
