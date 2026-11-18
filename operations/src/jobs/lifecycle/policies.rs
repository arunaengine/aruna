//! Resolves the placement policies a request names, each through the verifying resolver.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyRef, PolicyResolution};
use tracing::debug;
use ulid::Ulid;

use crate::placement::policy::{
    ReadPolicyError, ResolvePolicyConfig, ResolvePolicyOperation, ResolvedPolicy,
};

/// Each policy and whether it resolved, by policy id.
pub type Resolutions = BTreeMap<Ulid, PolicyResolution>;

/// Resolves every distinct ref, one child resolve after another. A ref that stays unresolved is
/// recorded as such, so it blocks a target instead of allowing it.
#[derive(Debug, PartialEq)]
pub(crate) struct ResolvePoliciesOperation {
    realm_id: RealmId,
    local: NodeId,
    now_ms: u64,
    /// Reversed, so the next ref pops off the end.
    remaining: Vec<PlacementPolicyRef>,
    current: Option<Ulid>,
    resolved: Resolutions,
}

impl ResolvePoliciesOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        local: NodeId,
        now_ms: u64,
        mut refs: Vec<PlacementPolicyRef>,
    ) -> Self {
        refs.sort_unstable();
        refs.dedup();
        refs.reverse();
        Self {
            realm_id,
            local,
            now_ms,
            remaining: refs,
            current: None,
            resolved: BTreeMap::new(),
        }
    }

    fn ask(&mut self) -> Result<Next<Resolutions>, IoError> {
        let Some(policy_ref) = self.remaining.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.resolved)));
        };
        self.current = Some(policy_ref.policy_id);
        Ok(Next::Call(IoCall::child(ResolvePolicyOperation::new(
            ResolvePolicyConfig {
                realm_id: self.realm_id,
                policy_ref,
                local_node_id: self.local,
                now_ms: self.now_ms,
            },
        ))))
    }
}

impl Operation for ResolvePoliciesOperation {
    type Output = Resolutions;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Resolutions>, IoError> {
        self.ask()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Resolutions>, IoError> {
        let resolution = match result.child::<ResolvedPolicy, ReadPolicyError>()? {
            Ok(resolved) => PolicyResolution::Known(resolved.policy),
            Err(error) => {
                debug!(error = %error, "Placement policy stayed unresolved");
                PolicyResolution::Unresolved
            }
        };
        let policy_id = self.current.take().ok_or(IoError::Unfinished)?;
        self.resolved.insert(policy_id, resolution);
        self.ask()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::policy::{PolicyCacheStats, PolicySource};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::errors::StorageError;
    use aruna_core::structs::placement::policy::{
        PlacementPolicy, PlacementSelector, VerifiedPolicy,
    };

    fn policy_ref(seed: u8) -> PlacementPolicyRef {
        PlacementPolicyRef {
            policy_id: Ulid::from_bytes([seed; 16]),
            digest: [seed; 32],
        }
    }

    fn verified() -> VerifiedPolicy {
        let selector = PlacementSelector {
            node_id: None,
            location: Some("eu".to_string()),
            labels: Vec::new(),
            executor_kind: None,
        };
        let policy = PlacementPolicy::new(Ulid::from_bytes([1; 16]), "eu".into(), vec![selector]);
        VerifiedPolicy::verify(policy.expect("policy is valid")).expect("policy is canonical")
    }

    fn answer(result: Result<ResolvedPolicy, ReadPolicyError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    // Each distinct ref is resolved once, in order; a failed resolve is recorded as unresolved.
    #[test]
    fn resolves_each_ref() {
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let refs = vec![policy_ref(2), policy_ref(1), policy_ref(2)];
        let mut operation = ResolvePoliciesOperation::new(RealmId([1; 32]), node, 9, refs);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let known = ResolvedPolicy {
            policy: verified(),
            source: PolicySource::Cached,
            stats: PolicyCacheStats::default(),
        };
        assert!(matches!(
            operation.next(answer(Ok(known))),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let failed = Err(ReadPolicyError::Storage(StorageError::TransactionConflict));
        let Ok(Next::Done(resolved)) = operation.next(answer(failed)) else {
            panic!("two distinct refs resolve");
        };
        assert_eq!(resolved.len(), 2);
        assert_eq!(
            resolved[&policy_ref(1).policy_id],
            PolicyResolution::Known(verified())
        );
        assert_eq!(
            resolved[&policy_ref(2).policy_id],
            PolicyResolution::Unresolved
        );
    }

    // A request without refs resolves nothing.
    #[test]
    fn empty_resolves_nothing() {
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let mut operation = ResolvePoliciesOperation::new(RealmId([1; 32]), node, 9, Vec::new());
        assert_eq!(operation.first(), Ok(Next::Done(BTreeMap::new())));
    }
}
