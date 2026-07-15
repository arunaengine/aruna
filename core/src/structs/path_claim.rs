//! Deterministic metadata path claims (spec 6.3.6, DEC-PATH, #416).
//!
//! Path uniqueness is NOT enforced at commit time: two authorized creates for
//! one normalized path both commit, each keeping its own unique
//! `MetaResourceId`. Instead every committed claim is retained and a single
//! deterministic winner is served under the path; the losers are surfaced as an
//! authorized conflict list under that same path. The winner is a pure function
//! of the retained candidate set: arrival time, wall-clock, event ULID order,
//! and serving node are never inputs, so every causally complete holder
//! converges on the same winner. Mirrors the binding conflict register's
//! order-independence, but resolves a winner rather than failing closed.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::structs::RealmId;
use crate::types::GroupId;
use crate::{MetaResourceId, StructuredId};

/// Domain tag separating the path-claim winner digest from every other hash in
/// the system (a MetaResourceId or event id must never be reusable as a winner
/// preimage elsewhere).
const PATH_CLAIM_WINNER_DOMAIN: &[u8] = b"aruna-path-claim-winner-v1";

/// One retained claim of a normalized canonical path by a committed Meta
/// Resource. Identified by its `MetaResourceId` (the `document_id`) plus the
/// causal event that established the claim.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathClaimRecord {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    /// The claiming Meta Resource id (a structured `MetaResourceId` as a `Ulid`).
    pub document_id: MetaResourceId,
    /// The causal event that established this claim generation.
    pub establishing_event_id: Ulid,
    /// The normalized canonical path this id requested (retained for the loser's
    /// authorization and for re-pathing).
    pub requested_path: String,
}

impl PathClaimRecord {
    /// The domain-separated winner digest over `(path, document_id, event id)`.
    /// Deliberately excludes any arrival/wall-clock/serving-node input so the
    /// order-independence and cross-node convergence hold.
    fn winner_digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(PATH_CLAIM_WINNER_DOMAIN);
        hasher.update(self.requested_path.as_bytes());
        hasher.update(&self.document_id.to_bytes());
        hasher.update(&self.establishing_event_id.to_bytes());
        *hasher.finalize().as_bytes()
    }

    /// Total order over candidates: lowest winner digest wins, tie-broken by the
    /// document id then the establishing event id so the order is total even on
    /// an (astronomically unlikely) digest collision.
    fn winner_order(&self) -> ([u8; 32], u128, u128) {
        (
            self.winner_digest(),
            self.document_id.as_u128(),
            self.establishing_event_id.0,
        )
    }
}

/// The deterministic resolution of one normalized path: the single served
/// winner plus every retained loser as a conflict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathResolution {
    pub winner: PathClaimRecord,
    pub conflicts: Vec<PathClaimRecord>,
}

impl PathResolution {
    /// The id served for the path.
    pub fn winner_id(&self) -> MetaResourceId {
        self.winner.document_id
    }

    /// Whether more than one id claims the path.
    pub fn is_conflicted(&self) -> bool {
        !self.conflicts.is_empty()
    }
}

/// Reduces a causally closed candidate set for one normalized path to its
/// deterministic winner and retained conflicts. `None` for an empty set.
///
/// Duplicate claims (same document id, e.g. an idempotent create retry or a
/// re-delivered event) collapse to one candidate: a retry never adds a rival.
/// The result is invariant under the input order, so two nodes holding the same
/// claim set produce the same winner and conflict list.
pub fn resolve_path_claim(claims: &[PathClaimRecord]) -> Option<PathResolution> {
    let mut deduped: Vec<PathClaimRecord> = Vec::with_capacity(claims.len());
    for claim in claims {
        if !deduped
            .iter()
            .any(|kept| kept.document_id == claim.document_id)
        {
            deduped.push(claim.clone());
        }
    }
    deduped.sort_by_key(|claim| claim.winner_order());
    let mut ordered = deduped.into_iter();
    let winner = ordered.next()?;
    Some(PathResolution {
        winner,
        conflicts: ordered.collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(doc: u8, event: u8, path: &str) -> PathClaimRecord {
        PathClaimRecord {
            realm_id: RealmId([1u8; 32]),
            group_id: Ulid::from_bytes([2u8; 16]),
            document_id: MetaResourceId::from_bytes([doc; 16]).unwrap(),
            establishing_event_id: Ulid::from_bytes([event; 16]),
            requested_path: path.to_string(),
        }
    }

    #[test]
    fn empty_has_no_winner() {
        assert_eq!(resolve_path_claim(&[]), None);
    }

    #[test]
    fn single_claim_wins_uncontested() {
        let resolution = resolve_path_claim(&[claim(10, 11, "datasets/x")]).unwrap();
        assert_eq!(
            resolution.winner_id(),
            MetaResourceId::from_bytes([10; 16]).unwrap()
        );
        assert!(!resolution.is_conflicted());
    }

    #[test]
    fn winner_is_order_independent() {
        // Same candidate set in every permutation resolves to the same winner
        // and the same conflict set: convergent regardless of arrival order.
        let a = claim(10, 40, "datasets/x");
        let b = claim(20, 41, "datasets/x");
        let c = claim(30, 42, "datasets/x");
        let forward = resolve_path_claim(&[a.clone(), b.clone(), c.clone()]).unwrap();
        let reverse = resolve_path_claim(&[c.clone(), b.clone(), a.clone()]).unwrap();
        let shuffled = resolve_path_claim(&[b, a, c]).unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(forward, shuffled);
        assert_eq!(forward.conflicts.len(), 2);
    }

    #[test]
    fn loser_is_retained_as_conflict() {
        let a = claim(10, 40, "datasets/x");
        let b = claim(20, 41, "datasets/x");
        let resolution = resolve_path_claim(&[a.clone(), b.clone()]).unwrap();
        let mut seen: Vec<MetaResourceId> = std::iter::once(resolution.winner_id())
            .chain(resolution.conflicts.iter().map(|c| c.document_id))
            .collect();
        seen.sort();
        let mut expected = vec![a.document_id, b.document_id];
        expected.sort();
        assert_eq!(seen, expected);
        assert!(resolution.is_conflicted());
    }

    #[test]
    fn duplicate_claim_never_adds_a_rival() {
        // An idempotent retry / re-delivered event for the same id collapses.
        let a = claim(10, 40, "datasets/x");
        let resolution = resolve_path_claim(&[a.clone(), a.clone(), a]).unwrap();
        assert!(!resolution.is_conflicted());
    }

    #[test]
    fn repathing_the_loser_clears_the_conflict() {
        let a = claim(10, 40, "datasets/x");
        let b = claim(20, 41, "datasets/x");
        let contested = resolve_path_claim(&[a.clone(), b.clone()]).unwrap();
        let loser = contested.conflicts[0].document_id;
        // The human re-paths the loser: it no longer claims this path.
        let remaining: Vec<PathClaimRecord> = [a, b]
            .into_iter()
            .filter(|claim| claim.document_id != loser)
            .collect();
        let resolved = resolve_path_claim(&remaining).unwrap();
        assert!(!resolved.is_conflicted());
        assert_ne!(resolved.winner_id(), loser);
    }
}
