//! Planning and preview for placement transitions: the pure functions that
//! turn "move these buckets onto that candidate map" into a self-describing
//! plan every node can re-derive.

use aruna_core::NodeId;
use aruna_core::structs::{
    BucketPlan, CandidatePlacementMap, PlacementRef, PlacementStrategy, RealmConfigDocument,
    TRANSITION_OVERDUE_MS, TransitionLimits, TransitionPlan,
};
use thiserror::Error;
use ulid::Ulid;

use crate::placement::resolver::view_from_map;
use crate::placement::{resolve_holders, shard_override, shard_subject_bytes};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransitionPlanError {
    #[error("placement strategy {0} is unknown")]
    StrategyUnknown(Ulid),
    #[error("candidate map epoch {0} is missing or conflicted")]
    MapUnavailable(u64),
    #[error("bucket {0} is outside the strategy's shard count")]
    BucketOutOfRange(u32),
    #[error("bucket {0} has no activation to move")]
    ActivationUnavailable(u32),
    #[error("bucket {0} resolves no holder in the target map")]
    TargetHoldersEmpty(u32),
    #[error("strategy {0} already has a transition in flight")]
    TransitionInFlight(Ulid),
    #[error("a transition must allow at least one bucket in flight")]
    InvalidLimits,
}

/// What a transition would do to one bucket. Computed with the same resolver
/// the executor uses, so a preview equals the outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketPreview {
    pub bucket: u32,
    pub old_holders: Vec<NodeId>,
    pub new_holders: Vec<NodeId>,
    pub overlap: Vec<NodeId>,
    pub old_locations: Vec<String>,
    pub new_locations: Vec<String>,
    pub disjoint: bool,
}

/// What operators need to see about the realm's transitions. Counts only: a
/// stall or an overdue bucket is a diagnostic and never moves authority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransitionHealth {
    pub active: usize,
    pub incomplete_buckets: usize,
    pub stalled_buckets: usize,
    /// Active transitions older than [`TRANSITION_OVERDUE_MS`].
    pub overdue: usize,
}

pub fn transition_health(config: &RealmConfigDocument, now_ms: u64) -> TransitionHealth {
    let mut health = TransitionHealth::default();
    for transition in config
        .placement_transitions
        .iter()
        .filter(|transition| !transition.is_terminal())
    {
        health.active += 1;
        health.incomplete_buckets += transition.incomplete_buckets();
        let mut stalled: Vec<u32> = transition
            .stalls
            .iter()
            .map(|report| report.bucket)
            .collect();
        stalled.sort_unstable();
        stalled.dedup();
        health.stalled_buckets += stalled.len();
        if now_ms
            >= transition
                .plan
                .created_at_ms
                .saturating_add(TRANSITION_OVERDUE_MS)
        {
            health.overdue += 1;
        }
    }
    health
}

/// Rank-ordered holders of `placement` under one frozen candidate map.
pub fn holders_in_map(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    placement: &PlacementRef,
    map: &CandidatePlacementMap,
) -> Vec<NodeId> {
    resolve_holders(
        &view_from_map(map),
        strategy,
        &shard_subject_bytes(placement),
        shard_override(config, placement),
    )
}

/// Holders of `placement` under its activated map, or `None` when the bucket
/// has no usable activation (missing, conflicted, or naming an unusable map).
pub fn activation_holders(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    placement: &PlacementRef,
) -> Option<Vec<NodeId>> {
    let activation = config.activation(&placement.strategy_id, placement.shard)?;
    let map = config.candidate_map(activation.candidate_map_epoch)?;
    Some(holders_in_map(config, strategy, placement, map))
}

/// Per-bucket effect of moving `buckets` of `strategy_id` onto
/// `target_map_epoch`. An empty `buckets` previews every bucket.
pub fn preview_transition(
    config: &RealmConfigDocument,
    strategy_id: Ulid,
    buckets: &[u32],
    target_map_epoch: u64,
) -> Result<Vec<BucketPreview>, TransitionPlanError> {
    let strategy = config
        .strategy(&strategy_id)
        .ok_or(TransitionPlanError::StrategyUnknown(strategy_id))?;
    let target = config
        .candidate_map(target_map_epoch)
        .ok_or(TransitionPlanError::MapUnavailable(target_map_epoch))?;

    let mut scope: Vec<u32> = if buckets.is_empty() {
        (0..strategy.shard_count).collect()
    } else {
        buckets.to_vec()
    };
    scope.sort_unstable();
    scope.dedup();

    let mut previews = Vec::with_capacity(scope.len());
    for bucket in scope {
        if bucket >= strategy.shard_count {
            return Err(TransitionPlanError::BucketOutOfRange(bucket));
        }
        let placement = PlacementRef {
            strategy_id,
            shard: bucket,
        };
        let old_holders = activation_holders(config, strategy, &placement)
            .ok_or(TransitionPlanError::ActivationUnavailable(bucket))?;
        let new_holders = holders_in_map(config, strategy, &placement, target);
        if new_holders.is_empty() {
            return Err(TransitionPlanError::TargetHoldersEmpty(bucket));
        }
        let overlap: Vec<NodeId> = old_holders
            .iter()
            .filter(|node_id| new_holders.contains(node_id))
            .copied()
            .collect();
        previews.push(BucketPreview {
            bucket,
            old_locations: locations_of(config, &old_holders),
            new_locations: locations_of(config, &new_holders),
            disjoint: overlap.is_empty(),
            overlap,
            old_holders,
            new_holders,
        });
    }
    Ok(previews)
}

/// Buckets of `strategy_id` whose holder set under `target_map_epoch` strictly
/// contains its current one: the only shape a machine-initiated transition may
/// take, because no node loses authority and nothing has to move off.
pub fn expansion_buckets(
    config: &RealmConfigDocument,
    strategy_id: Ulid,
    target_map_epoch: u64,
) -> Result<Vec<u32>, TransitionPlanError> {
    Ok(
        preview_transition(config, strategy_id, &[], target_map_epoch)?
            .into_iter()
            .filter(|preview| {
                preview.new_holders != preview.old_holders
                    && preview
                        .old_holders
                        .iter()
                        .all(|holder| preview.new_holders.contains(holder))
            })
            .map(|preview| preview.bucket)
            .collect(),
    )
}

/// Builds the plan a `StartPlacementTransition` carries from the preview, so
/// the record names exactly the sets the resolver derives.
pub fn plan_transition(
    config: &RealmConfigDocument,
    request: TransitionRequest,
) -> Result<TransitionPlan, TransitionPlanError> {
    if request.limits.max_incomplete_buckets == 0 {
        return Err(TransitionPlanError::InvalidLimits);
    }
    if let Some(transition) = config.placement_transitions.iter().find(|transition| {
        transition.plan.strategy_id == request.strategy_id && !transition.is_terminal()
    }) {
        return Err(TransitionPlanError::TransitionInFlight(
            transition.plan.strategy_id,
        ));
    }
    let buckets = preview_transition(
        config,
        request.strategy_id,
        &request.buckets,
        request.target_map_epoch,
    )?
    .into_iter()
    .map(|preview| BucketPlan {
        bucket: preview.bucket,
        old_holders: preview.old_holders,
        target_holders: preview.new_holders,
    })
    .collect();
    Ok(TransitionPlan {
        transition_id: request.transition_id,
        strategy_id: request.strategy_id,
        buckets,
        target_map_epoch: request.target_map_epoch,
        limits: request.limits,
        created_by: request.created_by,
        created_at_ms: request.created_at_ms,
    })
}

/// A transition an operator asks for, before its holder sets are resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionRequest {
    pub transition_id: Ulid,
    pub strategy_id: Ulid,
    /// Empty means every bucket of the strategy.
    pub buckets: Vec<u32>,
    pub target_map_epoch: u64,
    pub limits: TransitionLimits,
    pub created_by: NodeId,
    pub created_at_ms: u64,
}

/// Whether `plan` states exactly what this node derives from the same
/// replicated inputs. Admission runs it so a plan can never name a holder set
/// the resolver disagrees with.
pub fn plan_is_derivable(config: &RealmConfigDocument, plan: &TransitionPlan) -> bool {
    let Ok(previews) = preview_transition(
        config,
        plan.strategy_id,
        &plan.bucket_list(),
        plan.target_map_epoch,
    ) else {
        return false;
    };
    previews.len() == plan.buckets.len()
        && previews.iter().all(|preview| {
            plan.bucket_plan(preview.bucket).is_some_and(|bucket| {
                bucket.old_holders == preview.old_holders
                    && bucket.target_holders == preview.new_holders
            })
        })
}

fn locations_of(config: &RealmConfigDocument, holders: &[NodeId]) -> Vec<String> {
    let mut locations: Vec<String> = holders
        .iter()
        .filter_map(|node_id| {
            config
                .placement_entry(*node_id)
                .map(|entry| entry.effective_location().to_string())
        })
        .collect();
    locations.sort_unstable();
    locations.dedup();
    locations
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{
        PlacementTransition, RealmId, RealmNodeKind, StallReport, TransitionStatus,
    };

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// Three nodes activated at epoch one, four published at epoch two.
    fn seeded() -> (RealmConfigDocument, Ulid, Ulid) {
        let everywhere = Ulid::from_bytes([1; 16]);
        let capped = Ulid::from_bytes([2; 16]);
        let strategy = |strategy_id, replica_count| PlacementStrategy {
            strategy_id,
            name: "seeded".to_string(),
            replica_count,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 8,
        };
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([9; 32]), Vec::new(), 3);
        config.strategies = vec![strategy(everywhere, None), strategy(capped, Some(1))];
        for seed in 1..=3u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        config.snapshot_candidate_map();
        config.ensure_node(node(4), RealmNodeKind::Server);
        config.snapshot_candidate_map();
        (config, everywhere, capped)
    }

    #[test]
    fn expansion_needs_superset() {
        let (config, everywhere, capped) = seeded();
        assert_eq!(
            expansion_buckets(&config, everywhere, 2).unwrap(),
            (0..8).collect::<Vec<u32>>()
        );
        // A capped bucket either keeps its holder or swaps it: unchanged is not
        // worth a handoff and a swap is not an expansion.
        assert!(expansion_buckets(&config, capped, 2).unwrap().is_empty());
    }

    #[test]
    fn health_counts_stalls() {
        let (mut config, everywhere, _) = seeded();
        assert_eq!(
            transition_health(&config, u64::MAX),
            TransitionHealth::default()
        );

        let plan = plan_transition(
            &config,
            TransitionRequest {
                transition_id: Ulid::from_bytes([3; 16]),
                strategy_id: everywhere,
                buckets: vec![0, 1],
                target_map_epoch: 2,
                limits: TransitionLimits::default(),
                created_by: node(1),
                created_at_ms: 10,
            },
        )
        .expect("the plan derives");
        let mut transition = PlacementTransition::new(plan);
        // Two reports about one bucket are one stalled bucket.
        for seed in 1..=2u8 {
            transition.stalls.push(StallReport {
                bucket: 0,
                reported_by: node(seed),
                reason: "no old holder reachable".to_string(),
            });
        }
        config.placement_transitions.push(transition);

        assert_eq!(
            transition_health(&config, 10 + TRANSITION_OVERDUE_MS - 1),
            TransitionHealth {
                active: 1,
                incomplete_buckets: 2,
                stalled_buckets: 1,
                overdue: 0,
            }
        );
        assert_eq!(
            transition_health(&config, 10 + TRANSITION_OVERDUE_MS).overdue,
            1
        );

        // A terminal record is history, not health.
        config.placement_transitions[0].status = TransitionStatus::Aborted;
        assert_eq!(
            transition_health(&config, u64::MAX),
            TransitionHealth::default()
        );
    }
}
