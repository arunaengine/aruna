//! Resolving a governed ref set and evaluating one subject against it.
//!
//! This is the sans-I/O gate every enforcement point shares: it only obtains the
//! rules and calls the pure evaluator. Realm and group authorization has already
//! run when it is reached, and a matching selector can never grant access that
//! authorization denied.

use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    PlacementDecision, PlacementPolicyRef, PlacementSubject, PolicyResolution, RealmId,
    evaluate_placement,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::collections::BTreeMap;
use tracing::debug;
use ulid::Ulid;

use super::cache::PolicyCacheStats;
use super::read::ReadPolicyError;
use super::resolve::{ResolvePolicyConfig, ResolvePolicyOperation};

#[derive(Debug, Clone, PartialEq)]
pub struct PolicyGateConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    /// Refs of the governed record; every one of them must admit the subject.
    pub refs: Vec<PlacementPolicyRef>,
    pub subject: PlacementSubject,
    pub now_ms: u64,
}

/// The decision plus the cache diagnostics of the run that produced it.
#[derive(Debug, Clone, PartialEq)]
pub struct PolicyGateOutcome {
    pub decision: PlacementDecision,
    pub stats: PolicyCacheStats,
}

#[derive(Debug, PartialEq)]
pub struct PolicyGateOperation {
    config: PolicyGateConfig,
    /// Refs still to resolve, in reverse order so the next one pops off the end.
    remaining: Vec<PlacementPolicyRef>,
    current: Option<PlacementPolicyRef>,
    resolver: Option<ResolvePolicyOperation>,
    resolved: BTreeMap<Ulid, PolicyResolution>,
    stats: PolicyCacheStats,
    state: GateState,
    result: Option<Result<PolicyGateOutcome, ReadPolicyError>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum GateState {
    Init,
    Resolve,
    Finish,
    Error,
}

impl PolicyGateOperation {
    pub fn new(config: PolicyGateConfig) -> Self {
        Self {
            config,
            remaining: Vec::new(),
            current: None,
            resolver: None,
            resolved: BTreeMap::new(),
            stats: PolicyCacheStats::default(),
            state: GateState::Init,
            result: None,
        }
    }

    fn next_ref(&mut self) -> Effects {
        let Some(policy_ref) = self.remaining.pop() else {
            return self.evaluate();
        };
        let mut resolver = ResolvePolicyOperation::new(ResolvePolicyConfig {
            realm_id: self.config.realm_id,
            policy_ref,
            local_node_id: self.config.local_node_id,
            now_ms: self.config.now_ms,
        });
        let effects = resolver.start();
        self.current = Some(policy_ref);
        self.resolver = Some(resolver);
        self.state = GateState::Resolve;
        effects
    }

    /// Records one resolution. Only an obtained definition becomes `Known`; every
    /// other outcome either blocks the decision or is reported as unresolved.
    fn record(&mut self, policy_ref: PlacementPolicyRef, result: ResolveResult) -> Effects {
        match result {
            Ok(resolved) => {
                self.stats.merge(resolved.stats);
                self.resolved.insert(
                    policy_ref.policy_id,
                    PolicyResolution::Known(resolved.policy),
                );
                self.next_ref()
            }
            Err(
                ReadPolicyError::NotFound { .. }
                | ReadPolicyError::Unavailable(_)
                | ReadPolicyError::UnexpectedPublisher,
            ) => {
                self.resolved
                    .insert(policy_ref.policy_id, PolicyResolution::Unresolved);
                self.next_ref()
            }
            Err(ReadPolicyError::DigestMismatch | ReadPolicyError::RealmMismatch) => {
                self.decide(PlacementDecision::DigestMismatch {
                    refs: vec![policy_ref],
                })
            }
            Err(ReadPolicyError::Policy(_)) => self.decide(PlacementDecision::Invalid {
                policy_ids: vec![policy_ref.policy_id],
            }),
            Err(error) => self.fail(error),
        }
    }

    fn evaluate(&mut self) -> Effects {
        let decision = evaluate_placement(&self.config.refs, &self.resolved, &self.config.subject);
        self.decide(decision)
    }

    fn decide(&mut self, decision: PlacementDecision) -> Effects {
        let allowed = decision == PlacementDecision::Allowed;
        self.result = Some(Ok(PolicyGateOutcome {
            decision,
            stats: self.stats,
        }));
        self.state = GateState::Finish;
        debug!(
            refs = self.config.refs.len(),
            allowed,
            hits = self.stats.hits,
            misses = self.stats.misses,
            evictions = self.stats.evictions,
            "Placement gate decided"
        );
        smallvec![]
    }

    fn fail(&mut self, error: ReadPolicyError) -> Effects {
        self.result = Some(Err(error));
        self.state = GateState::Error;
        smallvec![]
    }
}

type ResolveResult = Result<super::resolve::ResolvedPolicy, ReadPolicyError>;

impl Operation for PolicyGateOperation {
    type Output = PolicyGateOutcome;
    type Error = ReadPolicyError;

    fn start(&mut self) -> Effects {
        match PlacementPolicyRef::canonical_set(&self.config.refs) {
            Ok(refs) => {
                self.remaining = refs;
                self.remaining.reverse();
                self.next_ref()
            }
            Err(reason) => self.decide(PlacementDecision::InvalidInput { reason }),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GateState::Resolve => {
                let Some(resolver) = self.resolver.as_mut() else {
                    return self.fail(ReadPolicyError::NotFinished);
                };
                let effects = resolver.step(event);
                if !resolver.is_complete() {
                    return effects;
                }
                let (Some(resolver), Some(policy_ref)) =
                    (self.resolver.take(), self.current.take())
                else {
                    return self.fail(ReadPolicyError::NotFinished);
                };
                self.record(policy_ref, resolver.finalize())
            }
            GateState::Init | GateState::Finish | GateState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GateState::Finish | GateState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(ReadPolicyError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.resolver.as_mut() {
            Some(resolver) => resolver.abort(),
            None => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        ResolvePolicyOperation::expected_error(error)
    }
}

#[cfg(test)]
mod tests {
    use super::super::cache::PolicyCacheEntry;
    use super::*;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::{PlacementPolicy, PlacementSelector, VerifiedPolicy};
    use aruna_core::types::Value;
    use byteview::ByteView;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn policy(seed: u8, location: &str) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([seed; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some(location.to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn subject(location: &str) -> PlacementSubject {
        PlacementSubject {
            node_id: node(9),
            generation: 1,
            location: location.to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        }
    }

    fn operation(refs: Vec<PlacementPolicyRef>, location: &str) -> PolicyGateOperation {
        PolicyGateOperation::new(PolicyGateConfig {
            realm_id: realm(),
            local_node_id: node(9),
            refs,
            subject: subject(location),
            now_ms: 1_000,
        })
    }

    fn cached(entry: &PolicyCacheEntry) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value: Some(Value::from(entry.to_bytes().expect("entry encodes"))),
        })
    }

    #[test]
    fn allows_ungoverned() {
        let mut operation = operation(Vec::new(), "eu-west");
        assert!(operation.start().is_empty());
        assert_eq!(
            operation.finalize().expect("gate decides").decision,
            PlacementDecision::Allowed
        );
    }

    #[test]
    fn denies_subject() {
        // Two refs intersect: a subject outside either one is denied.
        let first = policy(1, "eu-west");
        let second = policy(2, "us-east");
        let mut operation = operation(vec![first.policy_ref(), second.policy_ref()], "eu-west");
        operation.start();
        operation.step(cached(&PolicyCacheEntry::verified(realm(), &first, 10)));
        operation.step(cached(&PolicyCacheEntry::verified(realm(), &second, 10)));

        let outcome = operation.finalize().expect("gate decides");
        assert_eq!(
            outcome.decision,
            PlacementDecision::Denied {
                policy_ids: vec![second.policy().policy_id]
            }
        );
        assert_eq!(outcome.stats.hits, 2);
    }

    #[test]
    fn unresolved_stays_closed() {
        // A live availability hint must block the subject without denying it, so
        // a later valid policy is still evaluated on its merits.
        let rule = policy(1, "eu-west");
        let mut operation = operation(vec![rule.policy_ref()], "eu-west");
        operation.start();
        operation.step(cached(&PolicyCacheEntry::unavailable(1_000)));

        assert_eq!(
            operation.finalize().expect("gate decides").decision,
            PlacementDecision::Unavailable {
                policy_ids: vec![rule.policy().policy_id]
            }
        );
    }

    #[test]
    fn mismatch_stays_closed() {
        // The cache row holds another definition, so the fetch path answers with
        // a mismatch and the gate refuses instead of falling back to allow.
        let requested = policy(1, "eu-west");
        let mut operation = operation(vec![requested.policy_ref()], "eu-west");
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value: None,
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value: Some(document(&policy(1, "us-east"))),
        }));

        assert_eq!(
            operation.finalize().expect("gate decides").decision,
            PlacementDecision::DigestMismatch {
                refs: vec![requested.policy_ref()]
            }
        );
    }

    fn document(policy: &VerifiedPolicy) -> Value {
        ByteView::from(
            crate::placement_policy::tests::signed_document(realm(), policy, 1)
                .to_bytes()
                .expect("document encodes"),
        )
    }
}
