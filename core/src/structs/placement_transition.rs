//! Candidate maps, activations, and transition records: the replicated state
//! that pins a bucket's holder set until a proof-gated handoff moves it.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::{RealmId, RealmNodeKind};

/// Domain tag for the tuple a target holder signs to prove it verified a bucket.
pub const TRANSITION_PROOF_DOMAIN: &[u8] = b"aruna-transition-proof-v1";
/// Domain tag for the digest over a bucket's reduced barrier frontier join.
pub const TRANSITION_BARRIER_DOMAIN: &[u8] = b"aruna-transition-barrier-v1";
/// Default window a cut-over bucket keeps its old holders as members and
/// retained publishers before they are released.
pub const DEFAULT_TRANSITION_GRACE_MS: u64 = 3_600_000;

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

/// Immutable snapshot of the placement view at one publisher-chosen epoch.
/// Two different maps at one epoch conflict: the epoch stays unusable rather
/// than picking a winner.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CandidatePlacementMap {
    pub epoch: u64,
    pub nodes: Vec<CandidateMapNode>,
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

/// The operator-authored part of a transition. Immutable once admitted; every
/// other field of [`PlacementTransition`] is reduced from later events.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TransitionPlan {
    pub transition_id: Ulid,
    pub strategy_id: Ulid,
    /// Explicit bucket list; empty means every bucket of the strategy.
    pub buckets: Vec<u32>,
    pub target_map_epoch: u64,
    pub limits: TransitionLimits,
    pub created_by: NodeId,
    /// Audit only; never a correctness input.
    pub created_at_ms: u64,
}

impl TransitionPlan {
    /// Whether `bucket` is in scope, honoring the empty-means-all convention.
    pub fn covers(&self, bucket: u32) -> bool {
        self.buckets.is_empty() || self.buckets.contains(&bucket)
    }

    /// In-scope buckets in ascending order for `shard_count`.
    pub fn bucket_list(&self, shard_count: u32) -> Vec<u32> {
        if self.buckets.is_empty() {
            return (0..shard_count).collect();
        }
        let mut buckets: Vec<u32> = self
            .buckets
            .iter()
            .copied()
            .filter(|bucket| *bucket < shard_count)
            .collect();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
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
        CompletionProof {
            bucket: self.bucket,
            holder: self.holder,
            old_activation_epoch: self.old_activation_epoch,
            target_map_epoch: self.target_map_epoch,
            barrier_digest: self.barrier_digest,
            checkpoint_root: self.checkpoint_root,
            signature: secret.sign(&self.signing_bytes()),
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
        }
    }

    pub fn completion(&self, bucket: u32) -> Option<&BucketCompletion> {
        self.completed
            .iter()
            .find(|entry| entry.bucket == bucket)
            .or(None)
    }

    /// A transition is terminal once it is aborted or every covered bucket has
    /// cut over; a terminal record moves no authority.
    pub fn is_terminal(&self, shard_count: u32) -> bool {
        if matches!(self.status, TransitionStatus::Aborted) {
            return true;
        }
        self.plan
            .bucket_list(shard_count)
            .iter()
            .all(|bucket| self.completion(*bucket).is_some())
    }

    /// Digest over the bucket's reduced barrier join: the sorted
    /// `(holder, frontier)` pairs a proof commits to.
    pub fn barrier_digest(&self, bucket: u32) -> [u8; 32] {
        let mut reported: Vec<(&NodeId, &Vec<u8>)> = self
            .barriers
            .iter()
            .filter(|barrier| barrier.bucket == bucket)
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

    /// Whether every node in `old_holders` has reported its barrier.
    pub fn barrier_established(&self, bucket: u32, old_holders: &[NodeId]) -> bool {
        !old_holders.is_empty()
            && old_holders.iter().all(|holder| {
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

    fn plan() -> TransitionPlan {
        TransitionPlan {
            transition_id: Ulid::from_bytes([1; 16]),
            strategy_id: Ulid::from_bytes([2; 16]),
            buckets: vec![3, 1, 1],
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
        assert!(!left.barrier_established(1, &[]));
    }

    #[test]
    fn bucket_list_expands_and_normalizes() {
        assert_eq!(plan().bucket_list(8), vec![1, 3]);
        assert_eq!(plan().bucket_list(2), vec![1]);
        let mut all = plan();
        all.buckets.clear();
        assert_eq!(all.bucket_list(3), vec![0, 1, 2]);
        assert!(all.covers(2));
        assert!(!plan().covers(2));
    }

    #[test]
    fn terminal_needs_every_bucket() {
        let mut transition = PlacementTransition::new(plan());
        assert!(!transition.is_terminal(8));
        transition.completed.push(BucketCompletion {
            bucket: 1,
            completed_at_ms: 1,
        });
        assert!(!transition.is_terminal(8));
        transition.completed.push(BucketCompletion {
            bucket: 3,
            completed_at_ms: 2,
        });
        assert!(transition.is_terminal(8));

        let mut aborted = PlacementTransition::new(plan());
        aborted.status = TransitionStatus::Aborted;
        assert!(aborted.is_terminal(8));
    }
}
