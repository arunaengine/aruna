//! Resolving a governed ref set and evaluating one subject against it.
//!
//! This is the sans-I/O gate every enforcement point shares: it only obtains the
//! rules and calls the pure evaluator. Realm and group authorization has already
//! run when it is reached, and a matching selector can never grant access that
//! authorization denied.

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::Event;
use aruna_core::keyspaces::{NODE_SUBJECT_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BucketIdentity, BucketInfo, NODE_SUBJECT_KEY, NodeSubjectRecord, PlacementDecision,
    PlacementPolicyRef, PlacementSubject, PolicyResolution, RealmId, evaluate_placement,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeMap;
use thiserror::Error;
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
            // An unauthenticated publication is an invalid rule, never a grant.
            Err(ReadPolicyError::Policy(_) | ReadPolicyError::Authority(_)) => {
                self.decide(PlacementDecision::Invalid {
                    policy_ids: vec![policy_ref.policy_id],
                })
            }
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

/// The destination facts a governed write or serve is evaluated against.
/// Absent means this node advertises no subject, so nothing governed may be
/// materialized or served here.
#[derive(Clone, Debug, PartialEq)]
pub struct GateContext {
    pub realm_id: RealmId,
    pub subject: PlacementSubject,
    pub now_ms: u64,
    /// False while this node revalidates its inventory. Only a governed write
    /// is stopped by it; an ungoverned one never consults the gate.
    pub admitting: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum PolicyGateError {
    #[error("this node advertises no placement subject for governed data")]
    NoSubject,
    /// Ids stay out of the message: a public caller must not learn them.
    #[error("placement policy denies this destination")]
    Denied { policy_ids: Vec<Ulid> },
    #[error("a referenced placement policy is not available")]
    Unavailable { policy_ids: Vec<Ulid> },
    #[error("a referenced placement policy is invalid or its digest does not match")]
    Invalid,
    /// The requester has never resolved these refs; internal peers learn them
    /// through the handshake, public callers never do.
    #[error("the destination has not resolved every required placement policy")]
    Required { refs: Vec<PlacementPolicyRef> },
    #[error("the destination policy generation changed during this write")]
    Drift,
    /// This node is mid-transition, so it admits nothing governed until its
    /// inventory has been revalidated under the new subject.
    #[error("this node is not admitting governed data right now")]
    AdmissionStopped,
    #[error("unexpected event during a placement drift re-check")]
    InvalidEvent,
    #[error(transparent)]
    Conversion(#[from] aruna_core::errors::ConversionError),
    #[error(transparent)]
    Read(#[from] ReadPolicyError),
    #[error(transparent)]
    Policy(#[from] aruna_core::structs::PlacementPolicyError),
}

/// Builds the gate for one governed destination. `Ok(None)` means the ref set
/// is empty, so the ungoverned path runs unchanged and performs no extra I/O.
/// A node that stopped admitting governed data refuses only a governed write.
pub fn write_gate(
    context: Option<&GateContext>,
    refs: &[PlacementPolicyRef],
) -> Result<Option<PolicyGateOperation>, PolicyGateError> {
    let refs = PlacementPolicyRef::canonical_set(refs)?;
    if refs.is_empty() {
        return Ok(None);
    }
    let Some(context) = context else {
        return Err(PolicyGateError::NoSubject);
    };
    if !context.admitting {
        return Err(PolicyGateError::AdmissionStopped);
    }
    Ok(Some(PolicyGateOperation::new(PolicyGateConfig {
        realm_id: context.realm_id,
        local_node_id: context.subject.node_id,
        refs,
        subject: context.subject.clone(),
        now_ms: context.now_ms,
    })))
}

/// Every non-`Allowed` decision blocks. Nothing here is reinterpreted as a
/// grant, and an incomplete evaluation is never reported as a denial.
pub fn gate_decision(decision: PlacementDecision) -> Result<(), PolicyGateError> {
    match decision {
        PlacementDecision::Allowed => Ok(()),
        PlacementDecision::Denied { policy_ids } => Err(PolicyGateError::Denied { policy_ids }),
        PlacementDecision::Unavailable { policy_ids } => {
            Err(PolicyGateError::Unavailable { policy_ids })
        }
        PlacementDecision::Required { refs } => Err(PolicyGateError::Required { refs }),
        PlacementDecision::DigestMismatch { .. } | PlacementDecision::Invalid { .. } => {
            Err(PolicyGateError::Invalid)
        }
        PlacementDecision::InvalidInput { reason } => Err(reason.into()),
    }
}

/// The destination facts the gate decided on. A change between the gate and the
/// exposing transaction means the copy would commit refs nothing evaluated.
#[derive(Clone, Debug, PartialEq)]
pub struct GatedBucket {
    pub identity: Option<BucketIdentity>,
    pub generation: u64,
    pub policies: Vec<PlacementPolicyRef>,
    /// Subject generation the gate ran against. `None` leaves the write
    /// ungoverned, so no subject advance can invalidate it.
    pub subject_generation: Option<u64>,
}

impl GatedBucket {
    pub fn observe(bucket: Option<&BucketInfo>) -> Self {
        Self {
            identity: bucket.map(BucketInfo::identity),
            generation: bucket.map_or(0, |bucket| bucket.placement_policy_generation),
            policies: bucket
                .map(|bucket| bucket.placement_policies.clone())
                .unwrap_or_default(),
            subject_generation: None,
        }
    }

    /// Seals the subject the gate decided under. Only a governed write has one.
    pub fn sealed_under(mut self, context: Option<&GateContext>, governed: bool) -> Self {
        self.subject_generation = governed
            .then(|| context.map(|c| c.subject.generation))
            .flatten();
        self
    }

    /// True when the destination facts the gate decided on still hold. The
    /// sealed subject is checked separately, against the live subject row.
    pub fn matches(&self, observed: &Self) -> bool {
        self.identity == observed.identity
            && self.generation == observed.generation
            && self.policies == observed.policies
    }

    /// Re-check inside the exposing transaction. A missing subject row, an
    /// advanced generation, or a node that entered draining all mean the copy
    /// would commit refs that nothing evaluated against the current subject.
    pub fn check_subject(
        &self,
        observed: Option<&NodeSubjectRecord>,
    ) -> Result<(), PolicyGateError> {
        let Some(sealed) = self.subject_generation else {
            return Ok(());
        };
        match observed {
            Some(record)
                if record.subject.generation == sealed
                    && !record.serving_blocked
                    && !record.policy_draining =>
            {
                Ok(())
            }
            _ => Err(PolicyGateError::Drift),
        }
    }
}

/// Reads the destination bucket and the local subject in one round trip, so the
/// exposing transaction re-checks every fact the gate decided on.
pub fn drift_reads(bucket: &str, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::BatchRead {
        reads: vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.as_bytes().into()),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
        ],
        txn_id,
    })
}

/// Splits the `drift_reads` answer into the two records it re-checks.
pub fn split_drift_reads(
    values: Vec<(Key, Option<Value>)>,
) -> Result<(Option<BucketInfo>, Option<NodeSubjectRecord>), PolicyGateError> {
    let mut values = values.into_iter();
    let (_, bucket) = values.next().ok_or(PolicyGateError::InvalidEvent)?;
    let (_, subject) = values.next().ok_or(PolicyGateError::InvalidEvent)?;
    Ok((
        bucket
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?,
        subject
            .map(|value| NodeSubjectRecord::from_bytes(value.as_ref()))
            .transpose()?,
    ))
}

/// Union of what a write inherits and what its destination requires. A sender
/// or copy can only tighten: an inherited ref is never dropped.
pub fn union_refs(
    destination: &[PlacementPolicyRef],
    inherited: &[PlacementPolicyRef],
) -> Result<Vec<PlacementPolicyRef>, PolicyGateError> {
    let mut refs = destination.to_vec();
    refs.extend_from_slice(inherited);
    Ok(PlacementPolicyRef::canonical_set(&refs)?)
}

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

    fn context(location: &str) -> GateContext {
        GateContext {
            realm_id: realm(),
            subject: subject(location),
            now_ms: 0,
            admitting: true,
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
        operation.step(cached(&PolicyCacheEntry::verified(&document(&first), 10)));
        operation.step(crate::placement_policy::fixtures::authority(realm()));
        operation.step(cached(&PolicyCacheEntry::verified(&document(&second), 10)));
        operation.step(crate::placement_policy::fixtures::authority(realm()));

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
        operation.step(opened(Some(encoded(&policy(1, "us-east")))));

        assert_eq!(
            operation.finalize().expect("gate decides").decision,
            PlacementDecision::DigestMismatch {
                refs: vec![requested.policy_ref()]
            }
        );
    }

    #[test]
    fn sealed_subject_must_hold() {
        // The subject that admitted the write must still be the one advertised
        // when the exposing transaction runs.
        let gated = GatedBucket::observe(None).sealed_under(Some(&context("eu-west")), true);
        let record =
            aruna_core::structs::NodeSubjectRecord::seed(subject("eu-west")).expect("subject");
        assert_eq!(gated.check_subject(Some(&record)), Ok(()));
        assert_eq!(gated.check_subject(None), Err(PolicyGateError::Drift));

        let mut advanced = record.clone();
        advanced.subject.generation = 2;
        assert_eq!(
            gated.check_subject(Some(&advanced)),
            Err(PolicyGateError::Drift)
        );

        let mut draining = record.clone();
        draining.policy_draining = true;
        assert_eq!(
            gated.check_subject(Some(&draining)),
            Err(PolicyGateError::Drift)
        );
    }

    #[test]
    fn blocked_admits_ungoverned() {
        // A node that stopped admitting governed data still writes ungoverned
        // objects; only a write carrying refs is refused.
        let mut blocked = context("eu-west");
        blocked.admitting = false;
        let governed = vec![policy(1, "eu-west").policy_ref()];

        assert_eq!(write_gate(Some(&blocked), &[]), Ok(None));
        assert_eq!(
            write_gate(Some(&blocked), &governed).err(),
            Some(PolicyGateError::AdmissionStopped)
        );
        assert!(write_gate(Some(&context("eu-west")), &governed).is_ok());
    }

    #[test]
    fn ungoverned_ignores_subject() {
        // Nothing evaluated an ungoverned write, so no subject change can
        // invalidate it.
        let gated = GatedBucket::observe(None).sealed_under(None, false);
        assert_eq!(gated.check_subject(None), Ok(()));
    }

    fn document(policy: &VerifiedPolicy) -> aruna_core::structs::PlacementPolicyDocument {
        crate::placement_policy::tests::signed_document(realm(), policy, 1)
    }

    fn encoded(policy: &VerifiedPolicy) -> Value {
        ByteView::from(document(policy).to_bytes().expect("document encodes"))
    }

    /// The realm view and policy row the inner read starts with.
    fn opened(policy_row: Option<Value>) -> Event {
        let mut config = aruna_core::structs::RealmConfigDocument::new(realm(), Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), aruna_core::structs::RealmNodeKind::Server);
        }
        let (config_value, auth_value) = crate::placement_policy::tests::realm_view(
            &config,
            crate::placement_policy::tests::admin_user(realm()),
        );
        let key = ByteView::from(Vec::new());
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (key.clone(), policy_row),
                (key.clone(), Some(config_value)),
                (key, Some(auth_value)),
            ],
        })
    }
}
