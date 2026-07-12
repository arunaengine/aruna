#[cfg(test)]
mod distribution;
pub mod resolver;
pub mod selector;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{
    PlacementOverride, PlacementRef, PlacementStrategy, RealmConfigDocument, shard_for_subject,
};

use crate::placement::selector::{ROLE_SHARD, rank_weighted};

pub use resolver::{
    PlacementResolutionContext, PlacementView, ResolvedNode, build_view, document_class,
    meta_bucket_subject, resolve_holders, strategy_for_target, subject_bytes,
};

/// Canonical rendezvous subject for a shard's holder resolution:
/// `strategy_id(16) ‖ shard(4, big-endian)`. The epoch is deliberately excluded
/// (spec 6.3.1): the holder set is a pure function of the bucket, so a rebalance
/// stays a map change and never a per-document rewrite. Every document hashing
/// into the shard resolves the same holder set from this, so one sync topic per
/// shard has one authoritative holder set.
pub fn shard_subject_bytes(placement: &PlacementRef) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);
    bytes.extend_from_slice(&placement.strategy_id.to_bytes());
    bytes.extend_from_slice(&placement.shard.to_be_bytes());
    bytes
}

/// Holder pin/exclude override for a shard: matched on the shard subject, not
/// a document subject, because one shard topic has exactly one holder set.
/// Per-document overrides still steer strategy selection (see
/// [`strategy_for_target`]); their pin/exclude lists are inert for holders.
fn shard_override<'a>(
    config: &'a RealmConfigDocument,
    placement: &PlacementRef,
) -> Option<&'a PlacementOverride> {
    let subject = shard_subject_bytes(placement);
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
    context: PlacementResolutionContext<'_>,
) -> PlacementRef {
    match strategy_for_target(config, target, context) {
        Some((strategy, _)) => PlacementRef {
            strategy_id: strategy.strategy_id,
            epoch: 0,
            shard: shard_for_subject(&subject_bytes(target), strategy.shard_count),
        },
        None => PlacementRef::NIL,
    }
}

/// Bucket the document's registry row rides.
///
/// Resolved for the registry target itself, not for the document's own bucket:
/// the registry class is bound "everywhere" so every node carries the row, while
/// the document's bucket is replica-capped and reaches only its holders. The
/// document path is deliberately not passed — a path-prefix binding steers where
/// the *document* lives, and letting it cap the registry row too would put the
/// row back out of reach of the nodes that need it to route.
pub fn registry_placement(
    config: &RealmConfigDocument,
    record: &aruna_core::structs::MetadataRegistryRecord,
) -> PlacementRef {
    placement_ref_for_target(
        config,
        &DocumentSyncTarget::MetadataRegistry {
            group_id: record.group_id,
            document_id: record.document_id,
        },
        PlacementResolutionContext {
            group_id: Some(record.group_id),
            metadata_path: None,
        },
    )
}

/// Placement plan for a document `target`: its shard's rank-ordered holder set
/// (the same set every document in the shard resolves), the nominal replica
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
    context: PlacementResolutionContext<'_>,
) -> Option<TargetPlacementPlan> {
    let (strategy, _override) = strategy_for_target(config, target, context)?;
    let placement = PlacementRef {
        strategy_id: strategy.strategy_id,
        epoch: 0,
        shard: shard_for_subject(&subject_bytes(target), strategy.shard_count),
    };
    let holders = resolve_shard_holders_with(config, strategy, &placement);
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

/// Full eligible-node ranking for the shard that `target` hashes into
/// (ignoring the strategy's replica cap) so callers can top up beyond
/// `replica_count`. Rank order is the shard's, not the individual document's.
pub fn rank_eligible_holders(
    config: &RealmConfigDocument,
    target: &DocumentSyncTarget,
    context: PlacementResolutionContext<'_>,
) -> Vec<NodeId> {
    let Some((strategy, _override)) = strategy_for_target(config, target, context) else {
        return Vec::new();
    };
    let placement = PlacementRef {
        strategy_id: strategy.strategy_id,
        epoch: 0,
        shard: shard_for_subject(&subject_bytes(target), strategy.shard_count),
    };
    let mut uncapped = strategy.clone();
    uncapped.replica_count = None;
    let view = build_view(config);
    resolve_holders(
        &view,
        &uncapped,
        &shard_subject_bytes(&placement),
        shard_override(config, &placement),
    )
}

/// Rank-ordered holders of a specific shard (capped by the strategy's
/// `replica_count`, or all eligible for an everywhere strategy). Used by the
/// placement reconciler and the startup restore to enumerate the co-holders of
/// each shard the local node is responsible for. Returns `Vec::new()` when the
/// referenced strategy is unknown.
pub fn resolve_shard_holders(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    let Some(strategy) = config.strategy(&placement.strategy_id) else {
        return Vec::new();
    };
    resolve_shard_holders_with(config, strategy, placement)
}

/// Whether `node_id` holds `placement`, and may therefore publish onto its
/// topic. An empty holder set means no strategy governs the bucket (early
/// bootstrap, [`PlacementRef::NIL`]): nobody shards it, so it is nobody's to
/// withhold and the local node counts as a holder.
///
/// The presence of a local copy of a document is never evidence of holdership:
/// a rebalance leaves a stale copy behind on a node that is no longer a holder.
pub fn holds_placement(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    node_id: NodeId,
) -> bool {
    let holders = resolve_shard_holders(config, placement);
    holders.is_empty() || holders.contains(&node_id)
}

/// Buckets of `strategy` that `node_id` is a holder of. Empty when the node is
/// not sync-eligible, is unknown to the config, or is filtered out everywhere.
pub fn held_buckets(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    node_id: NodeId,
) -> Vec<u32> {
    (0..strategy.shard_count)
        .filter(|shard| {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                epoch: 0,
                shard: *shard,
            };
            resolve_shard_holders_with(config, strategy, &placement).contains(&node_id)
        })
        .collect()
}

/// Bucket the create-receiving node picks for `subject`: the best-ranked of the
/// buckets it already holds, so the origin is always a holder of the bucket it
/// stamps and can always publish onto that bucket's topic. Weighted rendezvous
/// on the subject spreads one node's documents across all its held buckets.
/// `None` when the origin holds no bucket of the strategy.
pub fn choose_origin_bucket(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    origin: NodeId,
    subject: &[u8],
) -> Option<PlacementRef> {
    let held = held_buckets(config, strategy, origin);
    let candidates: Vec<([u8; 4], u64)> =
        held.iter().map(|shard| (shard.to_be_bytes(), 1)).collect();
    let best = *rank_weighted(ROLE_SHARD, subject, &candidates).first()?;
    Some(PlacementRef {
        strategy_id: strategy.strategy_id,
        epoch: 0,
        shard: held[best],
    })
}

fn resolve_shard_holders_with(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    let view = build_view(config);
    resolve_holders(
        &view,
        strategy,
        &shard_subject_bytes(placement),
        shard_override(config, placement),
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
            shard_count: 64,
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
                shard: 7,
            },
        )
    }

    fn strategy_of(config: &RealmConfigDocument) -> &PlacementStrategy {
        config
            .strategy(&config.default_strategy_id.expect("default strategy"))
            .expect("default strategy resolves")
    }

    fn subject(seed: u64) -> [u8; 32] {
        *blake3::hash(&seed.to_le_bytes()).as_bytes()
    }

    #[test]
    fn origin_bucket_is_deterministic() {
        let (config, _) = config_and_placement();
        let strategy = strategy_of(&config);
        let first = choose_origin_bucket(&config, strategy, node(1), &subject(1));
        let second = choose_origin_bucket(&config, strategy, node(1), &subject(1));
        assert_eq!(first, second);
        assert_eq!(
            first.expect("origin holds buckets").strategy_id,
            strategy.strategy_id
        );
    }

    #[test]
    fn origin_holds_chosen_bucket() {
        let (config, _) = config_and_placement();
        let strategy = strategy_of(&config);
        // Replica 2 of 4 nodes: no node holds every bucket, so a blind hash
        // would land outside the origin's held set for some subjects.
        let held = held_buckets(&config, strategy, node(1));
        assert!(!held.is_empty() && held.len() < strategy.shard_count as usize);
        for seed in 0..256u64 {
            let placement = choose_origin_bucket(&config, strategy, node(1), &subject(seed))
                .expect("origin holds buckets");
            assert!(held.contains(&placement.shard));
            assert!(resolve_shard_holders(&config, &placement).contains(&node(1)));
        }
    }

    #[test]
    fn origin_buckets_spread() {
        let (config, _) = config_and_placement();
        let strategy = strategy_of(&config);
        let held = held_buckets(&config, strategy, node(1));
        let chosen: std::collections::HashSet<u32> = (0..1_000u64)
            .filter_map(|seed| choose_origin_bucket(&config, strategy, node(1), &subject(seed)))
            .map(|placement| placement.shard)
            .collect();
        assert!(
            chosen.len() * 2 > held.len(),
            "chosen {} of {} held buckets",
            chosen.len(),
            held.len()
        );
    }

    #[test]
    fn unknown_origin_holds_nothing() {
        let (config, _) = config_and_placement();
        let strategy = strategy_of(&config);
        assert!(held_buckets(&config, strategy, node(9)).is_empty());
        assert_eq!(
            choose_origin_bucket(&config, strategy, node(9), &subject(1)),
            None
        );
    }

    #[test]
    fn user_origin_holds_nothing() {
        let (mut config, _) = config_and_placement();
        config.ensure_node(node(5), RealmNodeKind::User);
        let strategy = strategy_of(&config);
        assert!(held_buckets(&config, strategy, node(5)).is_empty());
        assert_eq!(
            choose_origin_bucket(&config, strategy, node(5), &subject(1)),
            None
        );
    }

    #[test]
    fn shard_subject_override_pins_and_excludes_holders() {
        let (mut config, placement) = config_and_placement();
        let baseline = resolve_shard_holders(&config, &placement);
        assert_eq!(baseline.len(), 2);

        let pinned = *baseline.last().unwrap();
        let excluded = baseline[0];
        config.placement_overrides = vec![PlacementOverride {
            subject: shard_subject_bytes(&placement),
            pinned: vec![pinned],
            excluded: vec![excluded],
            strategy_id: None,
        }];

        let overridden = resolve_shard_holders(&config, &placement);
        assert_eq!(overridden[0], pinned);
        assert!(!overridden.contains(&excluded));
    }

    #[test]
    fn document_subject_override_does_not_touch_shard_holders() {
        let (mut config, placement) = config_and_placement();
        let baseline = resolve_shard_holders(&config, &placement);

        // Same pin/exclude, but keyed by a document subject: holder resolution
        // ignores it (strategy selection is its only remaining effect).
        config.placement_overrides = vec![PlacementOverride {
            subject: Ulid::from_bytes([9u8; 16]).to_bytes().to_vec(),
            pinned: vec![*baseline.last().unwrap()],
            excluded: vec![baseline[0]],
            strategy_id: None,
        }];

        assert_eq!(resolve_shard_holders(&config, &placement), baseline);
    }
}
