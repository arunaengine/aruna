//! Resolves a bounded set of policy refs one after another through the authenticated read.
//! It only obtains the definitions a mutation must verify, and never evaluates a subject.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyRef, PolicyResolution};
use std::collections::BTreeMap;
use thiserror::Error;
use tracing::debug;
use ulid::Ulid;

use super::read::ReadPolicyError;
use super::resolve::{ResolvePolicyConfig, ResolvePolicyOperation, ResolvedPolicy};

/// Each policy and whether it resolved, by policy id.
pub type Resolutions = BTreeMap<Ulid, PolicyResolution>;

/// How an unresolvable ref is treated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolveMode {
    /// An administrative mutation: a ref that cannot be authenticated fails the
    /// whole set, so nothing unverified is ever stored.
    Strict,
    /// A resumable pass: the ref is recorded as unresolved and the placement
    /// evaluation blocks on it instead of failing the run.
    Lenient,
}

#[derive(Debug, Error, PartialEq)]
pub enum PolicySetError {
    /// In strict mode, the first ref that could not be authenticated.
    #[error("placement policy {} could not be resolved: {source}", policy_ref.policy_id)]
    Unresolved {
        policy_ref: PlacementPolicyRef,
        source: ReadPolicyError,
    },
    #[error(transparent)]
    Io(#[from] IoError),
}

type SetTurn = Result<Next<Resolutions>, PolicySetError>;

/// Resolves every distinct ref, one resolve child after another.
#[derive(Debug, PartialEq)]
pub struct PolicySetResolver {
    realm_id: RealmId,
    local_node_id: NodeId,
    now_ms: u64,
    mode: ResolveMode,
    /// Reversed, so the next ref pops off the end.
    remaining: Vec<PlacementPolicyRef>,
    current: Option<PlacementPolicyRef>,
    resolved: Resolutions,
}

impl PolicySetResolver {
    pub fn new(
        realm_id: RealmId,
        local_node_id: NodeId,
        now_ms: u64,
        mode: ResolveMode,
        refs: &[PlacementPolicyRef],
    ) -> Self {
        let mut remaining = refs.to_vec();
        remaining.sort_unstable();
        remaining.dedup();
        remaining.reverse();
        Self {
            realm_id,
            local_node_id,
            now_ms,
            mode,
            remaining,
            current: None,
            resolved: BTreeMap::new(),
        }
    }

    fn next_ref(&mut self) -> SetTurn {
        let Some(policy_ref) = self.remaining.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.resolved)));
        };
        self.current = Some(policy_ref);
        Ok(Next::Call(IoCall::child(ResolvePolicyOperation::new(
            ResolvePolicyConfig {
                realm_id: self.realm_id,
                policy_ref,
                local_node_id: self.local_node_id,
                now_ms: self.now_ms,
            },
        ))))
    }
}

impl Operation for PolicySetResolver {
    type Output = Resolutions;
    type Error = PolicySetError;

    fn first(&mut self) -> SetTurn {
        self.next_ref()
    }

    /// In lenient mode a failed resolve counts as unresolved, so it blocks instead of allowing.
    fn next(&mut self, result: IoResult) -> SetTurn {
        let policy_ref = self.current.take().ok_or(IoError::Unfinished)?;
        let resolution = match result.child::<ResolvedPolicy, ReadPolicyError>()? {
            Ok(resolved) => PolicyResolution::Known(resolved.policy),
            Err(source) if self.mode == ResolveMode::Strict => {
                return Err(PolicySetError::Unresolved { policy_ref, source });
            }
            Err(error) => {
                debug!(error = %error, "Placement policy stayed unresolved");
                PolicyResolution::Unresolved
            }
        };
        self.resolved.insert(policy_ref.policy_id, resolution);
        self.next_ref()
    }
}

#[cfg(test)]
mod pure_tests {
    use super::super::read::PolicySource;
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::placement::policy::{
        PlacementPolicy, PlacementSelector, VerifiedPolicy,
    };

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
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

    fn resolved(policy: &VerifiedPolicy) -> IoResult {
        let resolved = ResolvedPolicy {
            policy: policy.clone(),
            source: PolicySource::Cached,
            stats: Default::default(),
        };
        IoResult::Child(ChildOutcome::new(Ok::<_, ReadPolicyError>(resolved)))
    }

    fn unavailable() -> IoResult {
        let error = ReadPolicyError::Unavailable("no holder answered".to_string());
        IoResult::Child(ChildOutcome::new(Err::<ResolvedPolicy, _>(error)))
    }

    fn resolver(refs: &[PlacementPolicyRef], mode: ResolveMode) -> PolicySetResolver {
        PolicySetResolver::new(realm(), node(9), 1_000, mode, refs)
    }

    #[test]
    fn resolves_every_ref() {
        // Each distinct ref is resolved once, in ref order.
        let (first, second) = (policy(1), policy(2));
        let refs = [second.policy_ref(), first.policy_ref(), first.policy_ref()];
        let mut resolver = resolver(&refs, ResolveMode::Strict);
        assert!(matches!(resolver.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert!(matches!(
            resolver.next(resolved(&first)),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let Ok(Next::Done(resolutions)) = resolver.next(resolved(&second)) else {
            panic!("expected every ref resolved");
        };
        assert_eq!(resolutions.len(), 2);
        assert!(matches!(
            resolutions.get(&first.policy().policy_id),
            Some(PolicyResolution::Known(_))
        ));
    }

    #[test]
    fn strict_fails_unresolved() {
        // An administrative mutation may not store a ref it could not verify.
        let policy = policy(1);
        let mut resolver = resolver(&[policy.policy_ref()], ResolveMode::Strict);
        resolver.first().unwrap();
        assert!(matches!(
            resolver.next(unavailable()),
            Err(PolicySetError::Unresolved { policy_ref, .. }) if policy_ref == policy.policy_ref()
        ));
    }

    #[test]
    fn lenient_records_unresolved() {
        let policy = policy(1);
        let mut resolver = resolver(&[policy.policy_ref()], ResolveMode::Lenient);
        resolver.first().unwrap();
        let Ok(Next::Done(resolutions)) = resolver.next(unavailable()) else {
            panic!("a lenient resolve ends with every ref recorded");
        };
        assert_eq!(
            resolutions.get(&policy.policy().policy_id),
            Some(&PolicyResolution::Unresolved)
        );
    }

    #[test]
    fn empty_set_ends() {
        let mut resolver = resolver(&[], ResolveMode::Lenient);
        assert_eq!(resolver.first(), Ok(Next::Done(BTreeMap::new())));
    }
}
