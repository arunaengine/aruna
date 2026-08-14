//! Candidate maps, activations, and transition records: the replicated state
//! that pins a bucket's holder set until a proof-gated handoff moves it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::{AffinityRule, PlacementOverride, RealmId, RealmNodeKind};

/// Domain tag for the tuple a target holder signs to prove it verified a bucket.
pub const TRANSITION_PROOF_DOMAIN: &[u8] = b"aruna-transition-proof-v1";
/// Domain tag for the digest over a bucket's reduced barrier frontier join.
pub const TRANSITION_BARRIER_DOMAIN: &[u8] = b"aruna-transition-barrier-v1";
/// Default window a cut-over bucket keeps its old holders as members and
/// retained publishers before they are released.
pub const DEFAULT_TRANSITION_GRACE_MS: u64 = 3_600_000;
/// A transition still incomplete after this long is surfaced to operators. It
/// is a health signal only: nothing about the record changes.
pub const TRANSITION_OVERDUE_MS: u64 = 86_400_000;
/// Upper bound on a reported barrier frontier: a postcard `ActorClock` is tens
/// of bytes per actor, so this covers realms far beyond current scale.
pub const MAX_BARRIER_FRONTIER_BYTES: usize = 65_536;
/// Upper bound on a stall or force reason; reasons are diagnostics.
pub const MAX_STALL_REASON_BYTES: usize = 1_024;

/// One eligible node as frozen into a candidate map. Selection input only: the
/// selector ranks over these fields and never over live config state.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CandidateMapNode {
    pub node_id: NodeId,
    pub kind: RealmNodeKind,
    pub location: String,
    pub weight: u32,
    pub full: bool,
    pub draining: bool,
    pub labels: BTreeMap<String, String>,
}

/// Holder-affecting strategy fields as frozen into a candidate map, so a live
/// strategy edit can never move an activated bucket's holders. `shard_count`
/// is deliberately live: edits to it are rejected while activations exist.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct FrozenStrategySelector {
    pub strategy_id: Ulid,
    pub replica_count: Option<u32>,
    pub distinct_locations: bool,
    pub affinity: Vec<AffinityRule>,
}

/// Immutable snapshot of the placement view at one publisher-chosen epoch.
/// Two different maps at one epoch conflict: the epoch stays unusable rather
/// than picking a winner.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CandidatePlacementMap {
    pub epoch: u64,
    pub nodes: Vec<CandidateMapNode>,
    /// One frozen selector per strategy known when the map was published.
    pub selectors: Vec<FrozenStrategySelector>,
    /// Shard-subject pin/exclude overrides frozen with the map. Document
    /// overrides stay live: they steer strategy selection, never holders.
    pub shard_overrides: Vec<PlacementOverride>,
}

impl CandidatePlacementMap {
    pub fn selector(&self, strategy_id: &Ulid) -> Option<&FrozenStrategySelector> {
        self.selectors
            .iter()
            .find(|selector| selector.strategy_id == *strategy_id)
    }

    pub fn shard_override(&self, subject: &[u8]) -> Option<&PlacementOverride> {
        self.shard_overrides
            .iter()
            .find(|record| record.subject == subject)
    }
}

/// The authoritative holder-set input for one bucket. Advances only by
/// reduction of a completed transition, never by a config edit.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementActivation {
    pub strategy_id: Ulid,
    pub shard: u32,
    pub activation_epoch: u64,
    pub candidate_map_epoch: u64,
    /// Set while a transition is in progress for the bucket.
    pub transition_id: Option<Ulid>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TransitionLimits {
    /// Buckets that may be in flight at once; at least one.
    pub max_incomplete_buckets: u32,
    pub grace_ms: u64,
}

impl Default for TransitionLimits {
    fn default() -> Self {
        Self {
            max_incomplete_buckets: 1,
            grace_ms: DEFAULT_TRANSITION_GRACE_MS,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum TransitionStatus {
    Active,
    Aborted,
}

/// One bucket of a transition with the two holder sets it moves between.
///
/// Both sets are pure functions of immutable inputs (the bucket's activated map
/// and the target map), so naming them here is a restatement any node can
/// re-derive - which is exactly what the admission guard does. Carrying them
/// keeps completion a set-membership question the reducer can settle without a
/// selector.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BucketPlan {
    pub bucket: u32,
    pub old_holders: Vec<NodeId>,
    pub target_holders: Vec<NodeId>,
    /// The bucket's activation epoch when the plan was derived. A transition
    /// applies only from exactly this epoch, so two concurrent plans from one
    /// base can never replay as each other's successor.
    pub predecessor_epoch: u64,
}

/// The operator-authored part of a transition. Immutable once admitted; every
/// other field of [`PlacementTransition`] is reduced from later events.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TransitionPlan {
    pub transition_id: Ulid,
    pub strategy_id: Ulid,
    pub buckets: Vec<BucketPlan>,
    pub target_map_epoch: u64,
    pub limits: TransitionLimits,
    pub created_by: NodeId,
    /// Audit only; never a correctness input.
    pub created_at_ms: u64,
}

impl TransitionPlan {
    pub fn bucket_plan(&self, bucket: u32) -> Option<&BucketPlan> {
        self.buckets.iter().find(|plan| plan.bucket == bucket)
    }

    pub fn covers(&self, bucket: u32) -> bool {
        self.bucket_plan(bucket).is_some()
    }

    pub fn bucket_list(&self) -> Vec<u32> {
        self.buckets.iter().map(|plan| plan.bucket).collect()
    }

    /// The first `limits.max_incomplete_buckets` incomplete bucket plans in
    /// plan order: the admitted in-flight window. Only these buckets' targets
    /// may join, pull, and prove; completion frees the next slot.
    pub fn admitted_buckets<'a>(
        &'a self,
        completed: &'a [BucketCompletion],
    ) -> impl Iterator<Item = &'a BucketPlan> {
        self.buckets
            .iter()
            .filter(move |plan| !completed.iter().any(|entry| entry.bucket == plan.bucket))
            .take(self.limits.max_incomplete_buckets as usize)
    }
}

/// One old holder's frozen frontier for a bucket. The bucket's barrier is the
/// join of every old holder's report.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BucketBarrier {
    pub bucket: u32,
    pub reported_by: NodeId,
    pub frontier: Vec<u8>,
}

/// A target holder's signed statement that it holds the bucket's verified
/// history at the barrier. The signature covers [`ProofClaim`].
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CompletionProof {
    pub bucket: u32,
    pub holder: NodeId,
    pub old_activation_epoch: u64,
    pub target_map_epoch: u64,
    pub barrier_digest: [u8; 32],
    pub checkpoint_root: [u8; 32],
    pub signature: iroh::Signature,
}

/// The tuple a completion proof signs. Reconstructed from the record plus the
/// transition it rides, so a proof cannot be replayed onto another transition,
/// strategy, realm, or bucket.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ProofClaim {
    pub realm_id: RealmId,
    pub transition_id: Ulid,
    pub strategy_id: Ulid,
    pub bucket: u32,
    pub old_activation_epoch: u64,
    pub target_map_epoch: u64,
    pub barrier_digest: [u8; 32],
    pub checkpoint_root: [u8; 32],
    pub holder: NodeId,
}

impl ProofClaim {
    pub fn signing_bytes(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(TRANSITION_PROOF_DOMAIN);
        hasher.update(&postcard::to_allocvec(self).expect("proof claim serializes"));
        *hasher.finalize().as_bytes()
    }

    pub fn sign(&self, secret: &iroh::SecretKey) -> CompletionProof {
        self.signed_with(|message| secret.sign(message))
    }

    /// Signs with the node's own signer, for holders that hold a handle rather
    /// than the key itself.
    pub fn signed_with(&self, sign: impl FnOnce(&[u8]) -> iroh::Signature) -> CompletionProof {
        CompletionProof {
            bucket: self.bucket,
            holder: self.holder,
            old_activation_epoch: self.old_activation_epoch,
            target_map_epoch: self.target_map_epoch,
            barrier_digest: self.barrier_digest,
            checkpoint_root: self.checkpoint_root,
            signature: sign(&self.signing_bytes()),
        }
    }
}

impl CompletionProof {
    pub fn claim(&self, realm_id: RealmId, transition_id: Ulid, strategy_id: Ulid) -> ProofClaim {
        ProofClaim {
            realm_id,
            transition_id,
            strategy_id,
            bucket: self.bucket,
            old_activation_epoch: self.old_activation_epoch,
            target_map_epoch: self.target_map_epoch,
            barrier_digest: self.barrier_digest,
            checkpoint_root: self.checkpoint_root,
            holder: self.holder,
        }
    }

    /// Verifies the signature against the claiming holder's own node key.
    pub fn verify(&self, realm_id: RealmId, transition_id: Ulid, strategy_id: Ulid) -> bool {
        let claim = self.claim(realm_id, transition_id, strategy_id);
        self.holder
            .verify(&claim.signing_bytes(), &self.signature)
            .is_ok()
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BucketCompletion {
    pub bucket: u32,
    pub completed_at_ms: u64,
}

/// An operator override that cut a bucket over without every proof. Rejected
/// unless at least one verified proof exists, so a verified copy always
/// survives the cut.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BucketForceFinalize {
    pub bucket: u32,
    pub at_risk_report: String,
}

/// Diagnostics only: a stall never moves authority.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct StallReport {
    pub bucket: u32,
    pub reported_by: NodeId,
    pub reason: String,
}

/// A departing old holder's reduced statement that its outbox drained for the
/// bucket. Retention ends only on grace AND this.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BucketDrain {
    pub bucket: u32,
    pub reported_by: NodeId,
}

/// One transition of a strategy's buckets from their activated map to a target
/// map. Every field but `plan` is reduced from replicated events.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct PlacementTransition {
    pub plan: TransitionPlan,
    pub status: TransitionStatus,
    pub barriers: Vec<BucketBarrier>,
    pub proofs: Vec<CompletionProof>,
    pub completed: Vec<BucketCompletion>,
    pub forced: Vec<BucketForceFinalize>,
    pub stalls: Vec<StallReport>,
    pub drained: Vec<BucketDrain>,
}

impl PlacementTransition {
    pub fn new(plan: TransitionPlan) -> Self {
        Self {
            plan,
            status: TransitionStatus::Active,
            barriers: Vec::new(),
            proofs: Vec::new(),
            completed: Vec::new(),
            forced: Vec::new(),
            stalls: Vec::new(),
            drained: Vec::new(),
        }
    }

    /// Whether `node_id`'s drain report for `bucket` has reduced.
    pub fn drain_reported(&self, bucket: u32, node_id: NodeId) -> bool {
        self.drained
            .iter()
            .any(|drain| drain.bucket == bucket && drain.reported_by == node_id)
    }

    /// Departing old holders of a completed bucket: old minus target. Only
    /// these ever retain, and only these owe a drain report.
    pub fn departing_holders(&self, bucket: u32) -> Vec<NodeId> {
        self.plan
            .bucket_plan(bucket)
            .map(|plan| {
                plan.old_holders
                    .iter()
                    .filter(|holder| !plan.target_holders.contains(holder))
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn completion(&self, bucket: u32) -> Option<&BucketCompletion> {
        self.completed.iter().find(|entry| entry.bucket == bucket)
    }

    /// A transition is terminal once it is aborted or every covered bucket has
    /// cut over; a terminal record moves no authority.
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, TransitionStatus::Aborted)
            || self
                .plan
                .buckets
                .iter()
                .all(|plan| self.completion(plan.bucket).is_some())
    }

    /// Whether the record has outlived its purpose: it is terminal, its last
    /// cut-over is older than the grace window, and every departing old
    /// holder of every completed bucket has a reduced drain report.
    ///
    /// The grace is what keeps a departing old holder in the topic while a
    /// reader mid-cutover may still need it; the drain report is what proves
    /// its accepted writes reached the topic. A permanently dead holder keeps
    /// the record alive until an operator removes the node from the realm,
    /// never a clock. An aborted record that cut nothing over releases at
    /// once - it moved nobody.
    pub fn released(&self, now_ms: u64) -> bool {
        self.is_terminal()
            && self
                .completed
                .iter()
                .map(|entry| entry.completed_at_ms)
                .max()
                .is_none_or(|at| now_ms >= at.saturating_add(self.plan.limits.grace_ms))
            && self.completed.iter().all(|entry| {
                self.departing_holders(entry.bucket)
                    .into_iter()
                    .all(|holder| self.drain_reported(entry.bucket, holder))
            })
    }

    /// Covered buckets that have not cut over yet.
    pub fn incomplete_buckets(&self) -> usize {
        self.plan
            .buckets
            .iter()
            .filter(|plan| self.completion(plan.bucket).is_none())
            .count()
    }

    /// The one structural predicate a counted proof must pass: a planned
    /// target attesting exactly this bucket's predecessor epoch, the plan's
    /// target epoch, and the bucket's reduced barrier digest. Signatures are
    /// verified at admission, not re-checked here.
    pub fn proof_valid(&self, bucket: u32, proof: &CompletionProof) -> bool {
        let Some(plan) = self.plan.bucket_plan(bucket) else {
            return false;
        };
        proof.bucket == bucket
            && plan.target_holders.contains(&proof.holder)
            && proof.old_activation_epoch == plan.predecessor_epoch
            && proof.target_map_epoch == self.plan.target_map_epoch
            && proof.barrier_digest == self.barrier_digest(bucket)
    }

    /// Whether the bucket may cut over: every old holder reported its barrier
    /// and every target holder proved the full tuple against one shared
    /// checkpoint root. A forced bucket needs one valid proof instead, so the
    /// last verified copy is never the one being cut away.
    ///
    /// Deliberately independent of [`TransitionStatus`]: a bucket whose proofs
    /// are all in has cut over, and an abort that arrives afterwards must not
    /// un-cut it. An abort stops the executors, so an incomplete bucket simply
    /// never completes.
    pub fn bucket_ready(&self, bucket: u32) -> bool {
        let Some(plan) = self.plan.bucket_plan(bucket) else {
            return false;
        };
        let valid_roots: Vec<[u8; 32]> = self
            .proofs_for(bucket)
            .filter(|proof| self.proof_valid(bucket, proof))
            .map(|proof| proof.checkpoint_root)
            .collect();
        let single_root = valid_roots.windows(2).all(|pair| pair[0] == pair[1]);
        let proved = |holder: &NodeId| {
            self.proofs_for(bucket)
                .any(|proof| proof.holder == *holder && self.proof_valid(bucket, proof))
        };
        if self.forced.iter().any(|entry| entry.bucket == bucket) {
            return single_root && plan.target_holders.iter().any(proved);
        }
        self.barrier_established(bucket, &plan.old_holders)
            && !plan.target_holders.is_empty()
            && single_root
            && plan.target_holders.iter().all(proved)
    }

    /// Digest over the bucket's reduced barrier join: the sorted
    /// `(holder, frontier)` pairs a proof commits to. Only planned old holders
    /// enter it, so a foreign report can never move the digest proofs match.
    pub fn barrier_digest(&self, bucket: u32) -> [u8; 32] {
        let old_holders: &[NodeId] = self
            .plan
            .bucket_plan(bucket)
            .map(|plan| plan.old_holders.as_slice())
            .unwrap_or_default();
        let mut reported: Vec<(&NodeId, &Vec<u8>)> = self
            .barriers
            .iter()
            .filter(|barrier| {
                barrier.bucket == bucket && old_holders.contains(&barrier.reported_by)
            })
            .map(|barrier| (&barrier.reported_by, &barrier.frontier))
            .collect();
        reported.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
        let mut hasher = blake3::Hasher::new();
        hasher.update(TRANSITION_BARRIER_DOMAIN);
        hasher.update(&self.plan.transition_id.to_bytes());
        hasher.update(&bucket.to_be_bytes());
        for (node_id, frontier) in reported {
            hasher.update(node_id.as_bytes());
            hasher.update(&(frontier.len() as u64).to_be_bytes());
            hasher.update(frontier);
        }
        *hasher.finalize().as_bytes()
    }

    /// Whether every node in `old_holders` has reported its barrier. A bucket
    /// with no old holder has no history to fence, so it is vacuously fenced;
    /// completion still needs every target's proof.
    pub fn barrier_established(&self, bucket: u32, old_holders: &[NodeId]) -> bool {
        old_holders.iter().all(|holder| {
            self.barriers
                .iter()
                .any(|barrier| barrier.bucket == bucket && barrier.reported_by == *holder)
        })
    }

    pub fn proofs_for(&self, bucket: u32) -> impl Iterator<Item = &CompletionProof> {
        self.proofs
            .iter()
            .filter(move |proof| proof.bucket == bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret(seed: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[seed; 32])
    }

    fn bucket_plan(bucket: u32) -> BucketPlan {
        BucketPlan {
            bucket,
            old_holders: vec![secret(1).public(), secret(2).public()],
            target_holders: vec![secret(2).public(), secret(3).public()],
            predecessor_epoch: 1,
        }
    }

    fn plan() -> TransitionPlan {
        TransitionPlan {
            transition_id: Ulid::from_bytes([1; 16]),
            strategy_id: Ulid::from_bytes([2; 16]),
            buckets: vec![bucket_plan(1), bucket_plan(3)],
            target_map_epoch: 4,
            limits: TransitionLimits::default(),
            created_by: secret(9).public(),
            created_at_ms: 7,
        }
    }

    fn claim(bucket: u32, holder: NodeId, digest: [u8; 32]) -> ProofClaim {
        ProofClaim {
            realm_id: RealmId::from_bytes([5; 32]),
            transition_id: Ulid::from_bytes([1; 16]),
            strategy_id: Ulid::from_bytes([2; 16]),
            bucket,
            old_activation_epoch: 1,
            target_map_epoch: 4,
            barrier_digest: digest,
            checkpoint_root: [6; 32],
            holder,
        }
    }

    #[test]
    fn transition_round_trips() {
        let mut transition = PlacementTransition::new(plan());
        transition.barriers.push(BucketBarrier {
            bucket: 1,
            reported_by: secret(1).public(),
            frontier: vec![1, 2, 3],
        });
        transition
            .proofs
            .push(claim(1, secret(2).public(), [0; 32]).sign(&secret(2)));
        transition.completed.push(BucketCompletion {
            bucket: 1,
            completed_at_ms: 11,
        });
        transition.forced.push(BucketForceFinalize {
            bucket: 3,
            at_risk_report: "one holder lost".to_string(),
        });
        transition.stalls.push(StallReport {
            bucket: 3,
            reported_by: secret(3).public(),
            reason: "no old holder reachable".to_string(),
        });

        let bytes = postcard::to_allocvec(&transition).unwrap();
        assert_eq!(
            postcard::from_bytes::<PlacementTransition>(&bytes).unwrap(),
            transition
        );
    }

    #[test]
    fn map_and_activation_round_trip() {
        let map = CandidatePlacementMap {
            epoch: 2,
            nodes: vec![CandidateMapNode {
                node_id: secret(4).public(),
                kind: RealmNodeKind::Server,
                location: "eu".to_string(),
                weight: 100,
                full: false,
                draining: true,
                labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
            }],
            selectors: Vec::new(),
            shard_overrides: Vec::new(),
        };
        let bytes = postcard::to_allocvec(&map).unwrap();
        assert_eq!(
            postcard::from_bytes::<CandidatePlacementMap>(&bytes).unwrap(),
            map
        );

        let activation = PlacementActivation {
            strategy_id: Ulid::from_bytes([2; 16]),
            shard: 9,
            activation_epoch: 3,
            candidate_map_epoch: 2,
            transition_id: Some(Ulid::from_bytes([1; 16])),
        };
        let bytes = postcard::to_allocvec(&activation).unwrap();
        assert_eq!(
            postcard::from_bytes::<PlacementActivation>(&bytes).unwrap(),
            activation
        );
    }

    #[test]
    fn proof_binds_its_claim() {
        // A signature is valid only for the exact tuple it was made over.
        let holder = secret(2).public();
        let proof = claim(1, holder, [0; 32]).sign(&secret(2));
        let realm_id = RealmId::from_bytes([5; 32]);
        let transition_id = Ulid::from_bytes([1; 16]);
        let strategy_id = Ulid::from_bytes([2; 16]);

        assert!(proof.verify(realm_id, transition_id, strategy_id));
        assert!(!proof.verify(RealmId::from_bytes([6; 32]), transition_id, strategy_id));
        assert!(!proof.verify(realm_id, Ulid::from_bytes([7; 16]), strategy_id));
        assert!(!proof.verify(realm_id, transition_id, Ulid::from_bytes([8; 16])));

        let mut moved = proof.clone();
        moved.bucket = 2;
        assert!(!moved.verify(realm_id, transition_id, strategy_id));
        let mut foreign = proof.clone();
        foreign.holder = secret(3).public();
        assert!(!foreign.verify(realm_id, transition_id, strategy_id));
        let mut retargeted = proof;
        retargeted.target_map_epoch = 5;
        assert!(!retargeted.verify(realm_id, transition_id, strategy_id));
    }

    #[test]
    fn barrier_digest_ignores_order() {
        let mut left = PlacementTransition::new(plan());
        left.barriers.push(BucketBarrier {
            bucket: 1,
            reported_by: secret(1).public(),
            frontier: vec![1],
        });
        left.barriers.push(BucketBarrier {
            bucket: 1,
            reported_by: secret(2).public(),
            frontier: vec![2],
        });
        left.barriers.push(BucketBarrier {
            bucket: 3,
            reported_by: secret(3).public(),
            frontier: vec![3],
        });
        let mut right = PlacementTransition::new(plan());
        right.barriers = left.barriers.iter().rev().cloned().collect();

        assert_eq!(left.barrier_digest(1), right.barrier_digest(1));
        assert_ne!(left.barrier_digest(1), left.barrier_digest(3));

        let holders = [secret(1).public(), secret(2).public()];
        assert!(left.barrier_established(1, &holders));
        assert!(!left.barrier_established(3, &holders));
        // No old holder means no history to fence.
        assert!(left.barrier_established(1, &[]));
    }

    #[test]
    fn plan_names_its_buckets() {
        assert_eq!(plan().bucket_list(), vec![1, 3]);
        assert!(plan().covers(1));
        assert!(!plan().covers(2));
        assert_eq!(plan().bucket_plan(3).unwrap().target_holders.len(), 2);
    }

    #[test]
    fn terminal_needs_every_bucket() {
        let mut transition = PlacementTransition::new(plan());
        assert!(!transition.is_terminal());
        transition.completed.push(BucketCompletion {
            bucket: 1,
            completed_at_ms: 1,
        });
        assert!(!transition.is_terminal());
        transition.completed.push(BucketCompletion {
            bucket: 3,
            completed_at_ms: 2,
        });
        assert!(transition.is_terminal());

        let mut aborted = PlacementTransition::new(plan());
        aborted.status = TransitionStatus::Aborted;
        assert!(aborted.is_terminal());
    }

    #[test]
    fn ready_needs_barrier_and_every_proof() {
        // Old holders 1 and 2 must fence; target holders 2 and 3 must prove
        // against the final barrier digest.
        let mut transition = PlacementTransition::new(plan());
        let barrier = |seed: u8| BucketBarrier {
            bucket: 1,
            reported_by: secret(seed).public(),
            frontier: vec![seed],
        };
        transition.barriers.push(barrier(1));
        let partial = transition.barrier_digest(1);
        transition
            .proofs
            .push(claim(1, secret(2).public(), partial).sign(&secret(2)));
        transition
            .proofs
            .push(claim(1, secret(3).public(), partial).sign(&secret(3)));
        assert!(!transition.bucket_ready(1));

        // The barrier completing invalidates proofs signed over a partial one.
        transition.barriers.push(barrier(2));
        assert!(!transition.bucket_ready(1));

        let digest = transition.barrier_digest(1);
        transition.proofs.clear();
        transition
            .proofs
            .push(claim(1, secret(2).public(), digest).sign(&secret(2)));
        assert!(!transition.bucket_ready(1), "one target proof missing");
        transition
            .proofs
            .push(claim(1, secret(3).public(), digest).sign(&secret(3)));
        assert!(transition.bucket_ready(1));
        assert!(!transition.bucket_ready(3));
        assert!(!transition.bucket_ready(9));

        // An abort never un-cuts a bucket whose proofs are already in.
        transition.status = TransitionStatus::Aborted;
        assert!(transition.bucket_ready(1));
        assert!(!transition.bucket_ready(3));
    }

    #[test]
    fn stale_proof_ignored() {
        // A stale predecessor epoch, wrong target epoch, or foreign holder
        // never counts toward completion.
        let mut transition = PlacementTransition::new(plan());
        for seed in [1u8, 2] {
            transition.barriers.push(BucketBarrier {
                bucket: 1,
                reported_by: secret(seed).public(),
                frontier: vec![seed],
            });
        }
        let digest = transition.barrier_digest(1);
        let mut stale_epoch = claim(1, secret(2).public(), digest);
        stale_epoch.old_activation_epoch = 9;
        let mut wrong_target = claim(1, secret(3).public(), digest);
        wrong_target.target_map_epoch = 9;
        let foreign = claim(1, secret(7).public(), digest);
        transition.proofs.push(stale_epoch.sign(&secret(2)));
        transition.proofs.push(wrong_target.sign(&secret(3)));
        transition.proofs.push(foreign.sign(&secret(7)));
        assert!(!transition.bucket_ready(1));

        transition.proofs.clear();
        transition
            .proofs
            .push(claim(1, secret(2).public(), digest).sign(&secret(2)));
        transition
            .proofs
            .push(claim(1, secret(3).public(), digest).sign(&secret(3)));
        assert!(transition.bucket_ready(1));
    }

    #[test]
    fn split_roots_block() {
        // Two targets proving different checkpoint roots must not cut over.
        let mut transition = PlacementTransition::new(plan());
        for seed in [1u8, 2] {
            transition.barriers.push(BucketBarrier {
                bucket: 1,
                reported_by: secret(seed).public(),
                frontier: vec![seed],
            });
        }
        let digest = transition.barrier_digest(1);
        let mut split = claim(1, secret(3).public(), digest);
        split.checkpoint_root = [9; 32];
        transition
            .proofs
            .push(claim(1, secret(2).public(), digest).sign(&secret(2)));
        transition.proofs.push(split.sign(&secret(3)));
        assert!(!transition.bucket_ready(1));

        // Resubmitting the shared root completes the bucket.
        transition.proofs.pop();
        transition
            .proofs
            .push(claim(1, secret(3).public(), digest).sign(&secret(3)));
        assert!(transition.bucket_ready(1));
    }

    #[test]
    fn release_awaits_grace() {
        let mut transition = PlacementTransition::new(plan());
        transition.plan.limits.grace_ms = 100;
        assert!(
            !transition.released(u64::MAX),
            "a live record never releases"
        );

        transition.completed.push(BucketCompletion {
            bucket: 1,
            completed_at_ms: 10,
        });
        transition.completed.push(BucketCompletion {
            bucket: 3,
            completed_at_ms: 40,
        });
        assert_eq!(transition.incomplete_buckets(), 0);
        for bucket in [1, 3] {
            transition.drained.push(BucketDrain {
                bucket,
                reported_by: secret(1).public(),
            });
        }
        // The window runs from the newest cut-over, not the first.
        assert!(!transition.released(139));
        assert!(transition.released(140));

        let mut aborted = PlacementTransition::new(plan());
        aborted.status = TransitionStatus::Aborted;
        assert_eq!(aborted.incomplete_buckets(), 2);
        assert!(aborted.released(0));
    }

    #[test]
    fn drain_gates_release() {
        // Grace alone never releases: the departing holder (node 1) must have
        // a reduced drain report for every completed bucket.
        let mut transition = PlacementTransition::new(plan());
        transition.plan.limits.grace_ms = 100;
        for bucket in [1, 3] {
            transition.completed.push(BucketCompletion {
                bucket,
                completed_at_ms: 10,
            });
        }
        assert!(!transition.released(u64::MAX));

        transition.drained.push(BucketDrain {
            bucket: 1,
            reported_by: secret(1).public(),
        });
        assert!(!transition.released(u64::MAX), "one bucket still undrained");
        transition.drained.push(BucketDrain {
            bucket: 3,
            reported_by: secret(1).public(),
        });
        assert!(transition.released(200));
        assert!(!transition.released(50), "grace still gates");
    }

    #[test]
    fn force_needs_one_proof() {
        let mut transition = PlacementTransition::new(plan());
        transition.forced.push(BucketForceFinalize {
            bucket: 3,
            at_risk_report: "old holders lost".to_string(),
        });
        assert!(!transition.bucket_ready(3));
        let digest = transition.barrier_digest(3);
        transition
            .proofs
            .push(claim(3, secret(3).public(), digest).sign(&secret(3)));
        assert!(transition.bucket_ready(3));
    }
}
