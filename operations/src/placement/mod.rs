#[cfg(test)]
mod distribution;
pub mod resolver;
pub mod selector;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{
    PlacementOverride, PlacementRef, PlacementStrategy, RealmConfigDocument, bucket_for_subject,
};

pub use resolver::{
    PlacementView, ResolvedNode, build_view, document_class, resolve_holders, strategy_for_target,
    subject_bytes,
};

/// Canonical rendezvous subject for a bucket's holder resolution:
/// `strategy_id(16) ‖ epoch(8, little-endian) ‖ bucket(4, big-endian)`. Every
/// document hashing into the bucket resolves the same holder set from this, so
/// one sync topic per bucket has one authoritative holder set (unlike stage 1,
/// where the rendezvous subject was the individual document).
pub fn bucket_subject_bytes(placement: &PlacementRef) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(28);
    bytes.extend_from_slice(&placement.strategy_id.to_bytes());
    bytes.extend_from_slice(&placement.epoch.to_le_bytes());
    bytes.extend_from_slice(&placement.bucket.to_be_bytes());
    bytes
}

/// Holder pin/exclude override for a bucket: matched on the bucket subject, not
/// a document subject, because one bucket topic has exactly one holder set.
/// Per-document overrides still steer strategy selection (see
/// [`strategy_for_target`]); their pin/exclude lists are inert for holders.
fn bucket_override<'a>(
    config: &'a RealmConfigDocument,
    placement: &PlacementRef,
) -> Option<&'a PlacementOverride> {
    let subject = bucket_subject_bytes(placement);
    config
        .placement_overrides
        .iter()
        .find(|over| over.subject == subject)
}

/// Placement reference stamped into a change's sync envelope for `target`.
///
/// Resolves the governing strategy from the realm config (passing the metadata
/// document path when the caller has it). Falls back to [`PlacementRef::NIL`]
/// only when the realm has no strategies (early bootstrap). Epoch is fixed 0
/// for this arc.
pub fn placement_ref_for_target(
    config: &RealmConfigDocument,
    target: &DocumentSyncTarget,
    metadata_path: Option<&str>,
) -> PlacementRef {
    match strategy_for_target(config, target, metadata_path) {
        Some((strategy, _)) => PlacementRef {
            strategy_id: strategy.strategy_id,
            epoch: 0,
            bucket: bucket_for_subject(&subject_bytes(target), strategy.bucket_count),
        },
        None => PlacementRef::NIL,
    }
}

/// Placement plan for a document `target`: its bucket's rank-ordered holder set
/// (the same set every document in the bucket resolves), the nominal replica
/// target the pending machinery tops up toward, and the envelope reference.
/// `None` when no strategy governs the target.
pub struct TargetPlacementPlan {
    pub holders: Vec<NodeId>,
    pub desired_count: usize,
    pub placement: PlacementRef,
}

pub fn plan_target_placement(
    config: &RealmConfigDocument,
    target: &DocumentSyncTarget,
    metadata_path: Option<&str>,
) -> Option<TargetPlacementPlan> {
    let (strategy, _override) = strategy_for_target(config, target, metadata_path)?;
    let placement = PlacementRef {
        strategy_id: strategy.strategy_id,
        epoch: 0,
        bucket: bucket_for_subject(&subject_bytes(target), strategy.bucket_count),
    };
    let holders = resolve_bucket_holders_with(config, strategy, &placement);
    let desired_count = match strategy.replica_count {
        Some(count) => count as usize,
        None => holders.len(),
    };
    Some(TargetPlacementPlan {
        holders,
        desired_count,
        placement,
    })
}

/// Full eligible-node ranking for the bucket that `target` hashes into
/// (ignoring the strategy's replica cap) so callers can top up beyond
/// `replica_count`. Rank order is the bucket's, not the individual document's.
pub fn rank_eligible_holders(
    config: &RealmConfigDocument,
    target: &DocumentSyncTarget,
    metadata_path: Option<&str>,
) -> Vec<NodeId> {
    let Some((strategy, _override)) = strategy_for_target(config, target, metadata_path) else {
        return Vec::new();
    };
    let placement = PlacementRef {
        strategy_id: strategy.strategy_id,
        epoch: 0,
        bucket: bucket_for_subject(&subject_bytes(target), strategy.bucket_count),
    };
    let mut uncapped = strategy.clone();
    uncapped.replica_count = None;
    let view = build_view(config);
    resolve_holders(
        &view,
        &uncapped,
        &bucket_subject_bytes(&placement),
        placement.epoch,
        bucket_override(config, &placement),
    )
}

/// Rank-ordered holders of a specific bucket (capped by the strategy's
/// `replica_count`, or all eligible for an everywhere strategy). Used by the
/// placement reconciler and the startup restore to enumerate the co-holders of
/// each bucket the local node is responsible for. Returns `Vec::new()` when the
/// referenced strategy is unknown.
pub fn resolve_bucket_holders(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    let Some(strategy) = config.strategy(&placement.strategy_id) else {
        return Vec::new();
    };
    resolve_bucket_holders_with(config, strategy, placement)
}

fn resolve_bucket_holders_with(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    let view = build_view(config);
    resolve_holders(
        &view,
        strategy,
        &bucket_subject_bytes(placement),
        placement.epoch,
        bucket_override(config, placement),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmId, RealmNodeKind};
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn config_and_placement() -> (RealmConfigDocument, PlacementRef) {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([3u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([5u8; 16]),
            name: "default".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
            bucket_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        (
            config,
            PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                bucket: 7,
            },
        )
    }

    #[test]
    fn bucket_subject_override_pins_and_excludes_holders() {
        let (mut config, placement) = config_and_placement();
        let baseline = resolve_bucket_holders(&config, &placement);
        assert_eq!(baseline.len(), 2);

        let pinned = *baseline.last().unwrap();
        let excluded = baseline[0];
        config.placement_overrides = vec![PlacementOverride {
            subject: bucket_subject_bytes(&placement),
            pinned: vec![pinned],
            excluded: vec![excluded],
            strategy_id: None,
        }];

        let overridden = resolve_bucket_holders(&config, &placement);
        assert_eq!(overridden[0], pinned);
        assert!(!overridden.contains(&excluded));
    }

    #[test]
    fn document_subject_override_does_not_touch_bucket_holders() {
        let (mut config, placement) = config_and_placement();
        let baseline = resolve_bucket_holders(&config, &placement);

        // Same pin/exclude, but keyed by a document subject: holder resolution
        // ignores it (strategy selection is its only remaining effect).
        config.placement_overrides = vec![PlacementOverride {
            subject: Ulid::from_bytes([9u8; 16]).to_bytes().to_vec(),
            pinned: vec![*baseline.last().unwrap()],
            excluded: vec![baseline[0]],
            strategy_id: None,
        }];

        assert_eq!(resolve_bucket_holders(&config, &placement), baseline);
    }
}
