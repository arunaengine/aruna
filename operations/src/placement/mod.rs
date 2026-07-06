pub mod resolver;
pub mod selector;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{NodeInfoDocument, PlacementRef, RealmConfigDocument};

pub use resolver::{
    PlacementView, ResolvedNode, build_view, document_class, resolve_holders, strategy_for_target,
    subject_bytes,
};

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
        },
        None => PlacementRef::NIL,
    }
}

/// Placement plan for a whole-document sync `target`: the currently achievable
/// rank-ordered holder set, the nominal replica target the pending machinery
/// tops up toward (`replica_count`, or all eligible for the everywhere
/// strategy), and the envelope reference. `None` when no strategy governs the
/// target.
pub struct TargetPlacementPlan {
    pub holders: Vec<NodeId>,
    pub desired_count: usize,
    pub placement: PlacementRef,
}

pub fn plan_target_placement(
    config: &RealmConfigDocument,
    node_infos: &[NodeInfoDocument],
    target: &DocumentSyncTarget,
    metadata_path: Option<&str>,
) -> Option<TargetPlacementPlan> {
    let (strategy, override_) = strategy_for_target(config, target, metadata_path)?;
    let view = build_view(config, node_infos);
    let holders = resolve_holders(&view, strategy, &subject_bytes(target), 0, override_);
    let desired_count = match strategy.replica_count {
        Some(count) => count as usize,
        None => holders.len(),
    };
    Some(TargetPlacementPlan {
        holders,
        desired_count,
        placement: PlacementRef {
            strategy_id: strategy.strategy_id,
            epoch: 0,
        },
    })
}

/// Full eligible-node ranking for `target` (ignoring the strategy's replica
/// cap) so callers can top up beyond `replica_count`. Preserves affinity and
/// `distinct_locations` eligibility from the resolved strategy.
pub fn rank_eligible_holders(
    config: &RealmConfigDocument,
    node_infos: &[NodeInfoDocument],
    target: &DocumentSyncTarget,
    metadata_path: Option<&str>,
) -> Vec<NodeId> {
    let Some((strategy, override_)) = strategy_for_target(config, target, metadata_path) else {
        return Vec::new();
    };
    let mut uncapped = strategy.clone();
    uncapped.replica_count = None;
    let view = build_view(config, node_infos);
    resolve_holders(&view, &uncapped, &subject_bytes(target), 0, override_)
}
