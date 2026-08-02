//! Deterministic metadata path claims (spec 6.3.6, DEC-PATH, #416).
//! Every committed claim is retained; the lowest digest over its stable record
//! and establishing event identity wins independently of arrival order.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::structs::RealmId;
use crate::types::GroupId;
use crate::{MetaResourceId, StructuredId};

/// Domain tag separating the path-claim winner digest from every other hash in
/// the system (a MetaResourceId or event id must never be reusable as a winner
/// preimage elsewhere).
const PATH_CLAIM_WINNER_DOMAIN: &[u8] = b"aruna-path-claim-v1";

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
    /// The domain-separated digest over path, document, and establishing event.
    fn winner_digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(PATH_CLAIM_WINNER_DOMAIN);
        hasher.update(self.requested_path.as_bytes());
        hasher.update(&self.document_id.to_bytes());
        hasher.update(&self.establishing_event_id.to_bytes());
        *hasher.finalize().as_bytes()
    }

    /// Total order over candidates, with identity tie-breakers for digest collisions.
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

/// Resolves a causally closed claim set, retaining the lowest event per document.
/// Returns `None` for an empty set and is invariant under input order.
pub fn resolve_path_claim(claims: &[PathClaimRecord]) -> Option<PathResolution> {
    let mut deduped = claims.to_vec();
    deduped.sort_by_key(|claim| (claim.document_id, claim.establishing_event_id));
    deduped.dedup_by_key(|claim| claim.document_id);
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
    fn empty_has_none() {
        assert_eq!(resolve_path_claim(&[]), None);
    }

    #[test]
    fn single_claim_wins() {
        let resolution = resolve_path_claim(&[claim(10, 11, "datasets/x")]).unwrap();
        assert_eq!(
            resolution.winner_id(),
            MetaResourceId::from_bytes([10; 16]).unwrap()
        );
        assert!(!resolution.is_conflicted());
    }

    #[test]
    fn winner_ignores_order() {
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
    fn duplicate_uses_lowest() {
        let low = claim(10, 40, "datasets/x");
        let high = claim(10, 99, "datasets/x");
        let forward = resolve_path_claim(&[high.clone(), low.clone()]).unwrap();
        let reverse = resolve_path_claim(&[low.clone(), high]).unwrap();

        assert_eq!(forward, reverse);
        assert_eq!(forward.winner, low);
        assert!(forward.conflicts.is_empty());
    }

    #[test]
    fn loser_is_retained() {
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
    fn duplicate_claim_collapses() {
        // An idempotent retry / re-delivered event for the same id collapses.
        let a = claim(10, 40, "datasets/x");
        let resolution = resolve_path_claim(&[a.clone(), a.clone(), a]).unwrap();
        assert!(!resolution.is_conflicted());
    }

    #[test]
    fn repathing_clears_conflict() {
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
