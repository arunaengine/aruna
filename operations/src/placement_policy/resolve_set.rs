//! Sequential resolution of a bounded ref set through the authenticated read
//! path, shared by the administration operations.
//!
//! Nothing here evaluates a subject: it only obtains the definitions an
//! administrative mutation must authenticate before it stores a ref, and the
//! resolutions a later placement evaluation needs.

use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::{PlacementPolicyRef, PolicyResolution, RealmId};
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::collections::BTreeMap;
use ulid::Ulid;

use super::cache::PolicyCacheStats;
use super::read::ReadPolicyError;
use super::resolve::{ResolvePolicyConfig, ResolvePolicyOperation};

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

/// What one fed event produced.
#[derive(Debug, PartialEq)]
pub enum ResolveStep {
    Pending(Effects),
    /// Every ref has an outcome.
    Done,
    Failed(PlacementPolicyRef, ReadPolicyError),
}

#[derive(Debug, PartialEq)]
pub struct PolicySetResolver {
    realm_id: RealmId,
    local_node_id: NodeId,
    now_ms: u64,
    mode: ResolveMode,
    /// Reversed, so the next ref pops off the end.
    remaining: Vec<PlacementPolicyRef>,
    current: Option<PlacementPolicyRef>,
    resolver: Option<ResolvePolicyOperation>,
    resolved: BTreeMap<Ulid, PolicyResolution>,
    stats: PolicyCacheStats,
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
            resolver: None,
            resolved: BTreeMap::new(),
            stats: PolicyCacheStats::default(),
        }
    }

    pub fn start(&mut self) -> ResolveStep {
        self.next_ref()
    }

    pub fn step(&mut self, event: Event) -> ResolveStep {
        let Some(resolver) = self.resolver.as_mut() else {
            return ResolveStep::Done;
        };
        let effects = resolver.step(event);
        if !resolver.is_complete() {
            return ResolveStep::Pending(effects);
        }
        let (Some(resolver), Some(policy_ref)) = (self.resolver.take(), self.current.take()) else {
            return ResolveStep::Done;
        };
        match resolver.finalize() {
            Ok(resolved) => {
                self.stats.merge(resolved.stats);
                self.resolved.insert(
                    policy_ref.policy_id,
                    PolicyResolution::Known(resolved.policy),
                );
                self.next_ref()
            }
            Err(error) => match self.mode {
                ResolveMode::Strict => ResolveStep::Failed(policy_ref, error),
                ResolveMode::Lenient => {
                    self.resolved
                        .insert(policy_ref.policy_id, PolicyResolution::Unresolved);
                    self.next_ref()
                }
            },
        }
    }

    pub fn resolutions(&self) -> &BTreeMap<Ulid, PolicyResolution> {
        &self.resolved
    }

    pub fn into_resolutions(self) -> BTreeMap<Ulid, PolicyResolution> {
        self.resolved
    }

    pub fn stats(&self) -> PolicyCacheStats {
        self.stats
    }

    pub fn abort(&mut self) -> Effects {
        match self.resolver.as_mut() {
            Some(resolver) => resolver.abort(),
            None => smallvec![],
        }
    }

    fn next_ref(&mut self) -> ResolveStep {
        let Some(policy_ref) = self.remaining.pop() else {
            return ResolveStep::Done;
        };
        let mut resolver = ResolvePolicyOperation::new(ResolvePolicyConfig {
            realm_id: self.realm_id,
            policy_ref,
            local_node_id: self.local_node_id,
            now_ms: self.now_ms,
        });
        let effects = resolver.start();
        self.current = Some(policy_ref);
        self.resolver = Some(resolver);
        ResolveStep::Pending(effects)
    }
}

#[cfg(test)]
mod tests {
    use super::{PolicySetResolver, ResolveMode, ResolveStep};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::{
        PlacementPolicy, PlacementPolicyRef, PlacementSelector, PolicyResolution, RealmId,
        VerifiedPolicy,
    };
    use aruna_core::types::NodeId;
    use byteview::ByteView;
    use ulid::Ulid;

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

    fn cached(policy: &VerifiedPolicy) -> Event {
        let entry = super::super::cache::PolicyCacheEntry::verified(
            &super::super::tests::signed_document(realm(), policy, 1),
            10,
        );
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value: Some(ByteView::from(entry.to_bytes().expect("entry encodes"))),
        })
    }

    fn resolver(refs: &[PlacementPolicyRef], mode: ResolveMode) -> PolicySetResolver {
        PolicySetResolver::new(realm(), node(9), 1_000, mode, refs)
    }

    #[test]
    fn resolves_every_ref() {
        let first = policy(1);
        let second = policy(2);
        let mut resolver = resolver(
            &[first.policy_ref(), second.policy_ref()],
            ResolveMode::Strict,
        );
        assert!(matches!(resolver.start(), ResolveStep::Pending(_)));
        assert!(matches!(
            resolver.step(cached(&first)),
            ResolveStep::Pending(_)
        ));
        assert!(matches!(
            resolver.step(super::super::fixtures::authority(realm())),
            ResolveStep::Pending(_)
        ));
        assert!(matches!(
            resolver.step(cached(&second)),
            ResolveStep::Pending(_)
        ));
        assert_eq!(
            resolver.step(super::super::fixtures::authority(realm())),
            ResolveStep::Done
        );

        assert_eq!(resolver.resolutions().len(), 2);
        assert!(matches!(
            resolver.resolutions().get(&first.policy().policy_id),
            Some(PolicyResolution::Known(_))
        ));
    }

    /// Cache miss followed by a realm view this node cannot read.
    fn drive_miss(resolver: &mut PolicySetResolver) -> ResolveStep {
        resolver.start();
        resolver.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value: None,
        }));
        let key = ByteView::from(Vec::new());
        resolver.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(key.clone(), None), (key.clone(), None), (key, None)],
        }))
    }

    #[test]
    fn strict_fails_unresolved() {
        // An administrative mutation may not store a ref it could not verify.
        let policy = policy(1);
        let mut resolver = resolver(&[policy.policy_ref()], ResolveMode::Strict);

        assert!(matches!(drive_miss(&mut resolver), ResolveStep::Failed(..)));
    }

    #[test]
    fn lenient_records_unresolved() {
        let policy = policy(1);
        let mut resolver = resolver(&[policy.policy_ref()], ResolveMode::Lenient);

        assert_eq!(drive_miss(&mut resolver), ResolveStep::Done);
        assert_eq!(
            resolver.resolutions().get(&policy.policy().policy_id),
            Some(&PolicyResolution::Unresolved)
        );
    }
}
