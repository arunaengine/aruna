#[cfg(test)]
mod distribution;
pub mod resolver;
pub mod selector;
pub mod transition;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::structs::{
    DocumentClass, PlacementOverride, PlacementRef, PlacementStrategy, RealmConfigDocument,
    shard_for_subject,
};
use aruna_core::types::GroupId;
use ulid::Ulid;

use crate::placement::selector::{ROLE_SHARD, rank_weighted};

pub use resolver::{
    PlacementResolutionContext, PlacementView, ResolvedNode, build_view, document_class,
    meta_bucket_subject, resolve_holders, strategy_for_target, subject_bytes, view_from_map,
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
pub(crate) fn shard_override<'a>(
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
            shard: shard_for_subject(&subject_bytes(target), strategy.shard_count),
        },
        None => PlacementRef::NIL,
    }
}

/// Bucket the document's registry row rides.
///
/// Resolved directly from the registry class, not from the document's general
/// precedence chain: the registry class is bound "everywhere" so every node
/// carries the row, while the document's bucket is replica-capped and reaches
/// only its holders. Document overrides and group/path bindings steer where the
/// *document* lives and must not cap the registry row too.
pub fn registry_placement(
    config: &RealmConfigDocument,
    record: &aruna_core::structs::MetadataRegistryRecord,
) -> PlacementRef {
    registry_placement_for(config, record.group_id, record.document_id)
}

pub(crate) fn registry_placement_for(
    config: &RealmConfigDocument,
    group_id: GroupId,
    document_id: Ulid,
) -> PlacementRef {
    let Some(strategy) = registry_strategy(config) else {
        return PlacementRef::NIL;
    };
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    PlacementRef {
        strategy_id: strategy.strategy_id,
        shard: shard_for_subject(&subject_bytes(&target), strategy.shard_count),
    }
}

pub(crate) fn registry_strategy(config: &RealmConfigDocument) -> Option<&PlacementStrategy> {
    resolver::strategy_for_class(config, DocumentClass::MetadataRegistry)
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

/// Why a bucket resolves no holder. Routing fails closed on these: they mean
/// "not resolvable here", never "definitively absent".
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PlacementResolveError {
    #[error("placement strategy {0} is unknown")]
    StrategyUnknown(Ulid),
    #[error("bucket {0} has no placement activation")]
    ActivationUnavailable(u32),
    #[error("bucket {0} has divergent placement activations")]
    ActivationConflicted(u32),
    #[error("candidate map epoch {0} is missing or conflicted")]
    CandidateMapUnavailable(u64),
}

/// Rank-ordered holders of a specific shard (capped by the strategy's
/// `replica_count`, or all eligible for an everywhere strategy). Used by the
/// placement reconciler and the startup restore to enumerate the co-holders of
/// each shard the local node is responsible for. Returns `Vec::new()` for the
/// housekeeping callers whenever [`resolve_shard_holders_checked`] fails.
pub fn resolve_shard_holders(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    resolve_shard_holders_checked(config, placement).unwrap_or_default()
}

/// Holder resolution pinned to the bucket's activated candidate map: selection
/// runs over the frozen view, so a config edit moves no holder and only a
/// completed transition can. Routing callers use this variant and map its
/// failures to 503, because an unresolvable bucket is not an absent document.
pub fn resolve_shard_holders_checked(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Result<Vec<NodeId>, PlacementResolveError> {
    let strategy =
        config
            .strategy(&placement.strategy_id)
            .ok_or(PlacementResolveError::StrategyUnknown(
                placement.strategy_id,
            ))?;
    let view = view_for_epoch(config, selection_epoch(config, placement)?)?;
    Ok(resolve_shard_holders_from_view(
        config, &view, strategy, placement,
    ))
}

pub(crate) const MAX_READ_HOLDERS: usize = 32;

/// Resolves a shard's canonical holder order with a bounded replica target.
pub(crate) fn resolve_holders_limit(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    limit: usize,
) -> Vec<NodeId> {
    holders_limit_checked(config, placement, limit).unwrap_or_default()
}

fn holders_limit_checked(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    limit: usize,
) -> Result<Vec<NodeId>, PlacementResolveError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let strategy =
        config
            .strategy(&placement.strategy_id)
            .ok_or(PlacementResolveError::StrategyUnknown(
                placement.strategy_id,
            ))?;
    let target = limit.min(u32::MAX as usize) as u32;
    let mut bounded = strategy.clone();
    bounded.replica_count = Some(
        strategy
            .replica_count
            .map_or(target, |replicas| replicas.min(target)),
    );
    let view = view_for_epoch(config, selection_epoch(config, placement)?)?;
    Ok(resolve_shard_holders_from_view(
        config, &view, &bounded, placement,
    ))
}

/// Read fan-out for a bucket: its activated holders first, then every holder a
/// transition still names for it, in rank order and deduped.
///
/// A bucket in flight has its history on the old set and, once a target has
/// proved, on the new one; a bucket that just cut over may still be catching a
/// reader up from an old holder. The transition record's own lifetime is that
/// window - it is pruned only after the grace release - so no clock is read
/// here. Writes deliberately do not union: authority is the activation alone.
pub fn read_holder_sets(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Result<Vec<NodeId>, PlacementResolveError> {
    let mut holders = holders_limit_checked(config, placement, MAX_READ_HOLDERS)?;
    for transition in &config.placement_transitions {
        if transition.plan.strategy_id != placement.strategy_id {
            continue;
        }
        let Some(bucket) = transition.plan.bucket_plan(placement.shard) else {
            continue;
        };
        for node_id in bucket.target_holders.iter().chain(&bucket.old_holders) {
            if !holders.contains(node_id) {
                holders.push(*node_id);
            }
        }
    }
    holders.truncate(MAX_READ_HOLDERS);
    Ok(holders)
}

/// Candidate map epoch a bucket selects from. `None` while the realm has
/// published no map at all: nothing is pinned yet, so the live view is the only
/// input there is (bootstrap, before the first `PublishCandidateMap`).
fn selection_epoch(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Result<Option<u64>, PlacementResolveError> {
    if config.candidate_maps.is_empty() {
        return Ok(None);
    }
    match config.activation(&placement.strategy_id, placement.shard) {
        Some(activation) => Ok(Some(activation.candidate_map_epoch)),
        None if config.placement_activations.iter().any(|entry| {
            entry.strategy_id == placement.strategy_id && entry.shard == placement.shard
        }) =>
        {
            Err(PlacementResolveError::ActivationConflicted(placement.shard))
        }
        None => Err(PlacementResolveError::ActivationUnavailable(
            placement.shard,
        )),
    }
}

fn view_for_epoch(
    config: &RealmConfigDocument,
    epoch: Option<u64>,
) -> Result<PlacementView, PlacementResolveError> {
    match epoch {
        None => Ok(build_view(config)),
        Some(epoch) => config
            .candidate_map(epoch)
            .map(view_from_map)
            .ok_or(PlacementResolveError::CandidateMapUnavailable(epoch)),
    }
}

/// First shard of a referenced strategy that resolves to zero holders while the
/// realm still has usable capacity: a filter, affinity, or override that leaves
/// documents routed to it with nowhere to live. Bootstrap, full drain, and
/// all-full realms have no usable node and are not flagged (fail-early for a
/// genuine misconfiguration, not for an empty realm).
pub fn first_empty_referenced_shard(config: &RealmConfigDocument) -> Option<PlacementRef> {
    // Checked against the newest map, which is what a future activation would
    // pin: activated buckets are already frozen and cannot be emptied by an edit.
    let view = match config
        .newest_map_epoch()
        .and_then(|epoch| config.candidate_map(epoch))
    {
        Some(map) => view_from_map(map),
        None => build_view(config),
    };
    let has_capacity = view.nodes.iter().any(|node| {
        node.kind.is_sync_eligible() && !node.full && !node.draining && node.weight > 0
    });
    if !has_capacity {
        return None;
    }
    for strategy in &config.strategies {
        let id = strategy.strategy_id;
        let referenced = config.default_strategy_id == Some(id)
            || config
                .strategy_bindings
                .iter()
                .any(|binding| binding.strategy_id == id)
            || config
                .placement_bindings
                .iter()
                .any(|binding| binding.strategy_id == id)
            || config
                .placement_overrides
                .iter()
                .any(|record| record.strategy_id == Some(id));
        if !referenced {
            continue;
        }
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: id,
                shard,
            };
            if resolve_shard_holders_from_view(config, &view, strategy, &placement).is_empty() {
                return Some(placement);
            }
        }
    }
    None
}

/// Whether `node_id` holds `placement`, and may therefore publish onto its
/// topic. [`PlacementRef::NIL`] means no strategy governs the bucket during
/// early bootstrap: nobody shards it, so it is nobody's to withhold and the
/// local node counts as a holder.
///
/// The presence of a local copy of a document is never evidence of holdership:
/// a rebalance leaves a stale copy behind on a node that is no longer a holder.
pub fn holds_placement(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    node_id: NodeId,
) -> bool {
    let holders = resolve_shard_holders(config, placement);
    *placement == PlacementRef::NIL || holders.contains(&node_id)
}

/// Whether `node_id` is a draining former-holder of `placement`: it is marked
/// draining in the config yet would hold the shard with its own draining flag
/// cleared. Such a node keeps publish rights on shards it previously held until
/// its outbox has flushed (flush-then-leave), so its retained records stay
/// deliverable. A node that was never a holder, or is fully removed from the
/// config rather than draining, is not a former-holder and its records must
/// remain undeliverable (DECISIONS K3): only a departing holder may flush.
pub fn is_draining_former_holder(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    node_id: NodeId,
) -> bool {
    if *placement == PlacementRef::NIL {
        return false;
    }
    if !config
        .placement_entry(node_id)
        .is_some_and(|entry| entry.draining)
    {
        return false;
    }
    let Some(strategy) = config.strategy(&placement.strategy_id) else {
        return false;
    };
    let mut view = build_view(config);
    for node in view.nodes.iter_mut() {
        if node.node_id == node_id {
            node.draining = false;
        }
    }
    resolve_holders(
        &view,
        strategy,
        &shard_subject_bytes(placement),
        shard_override(config, placement),
    )
    .contains(&node_id)
}

/// First shard whose retained holdership would be lost for a node that remains
/// draining across a config transition. Rejecting such transitions preserves
/// its acknowledged writes until that node un-drains or is removed.
pub fn first_draining_holder_set_change(
    pre: &RealmConfigDocument,
    post: &RealmConfigDocument,
) -> Option<(NodeId, PlacementRef)> {
    for entry in pre.placement_map.iter().filter(|entry| entry.draining) {
        let node_id = entry.node_id;
        if !post
            .placement_entry(node_id)
            .is_some_and(|entry| entry.draining)
        {
            continue;
        }
        for strategy in pre.strategies.iter().chain(
            post.strategies
                .iter()
                .filter(|strategy| pre.strategy(&strategy.strategy_id).is_none()),
        ) {
            let shard_count = post
                .strategy(&strategy.strategy_id)
                .map_or(strategy.shard_count, |post| {
                    strategy.shard_count.max(post.shard_count)
                });
            for shard in 0..shard_count {
                let placement = PlacementRef {
                    strategy_id: strategy.strategy_id,
                    shard,
                };
                if is_draining_former_holder(pre, &placement, node_id)
                    && !is_draining_former_holder(post, &placement, node_id)
                {
                    return Some((node_id, placement));
                }
            }
        }
    }
    None
}

/// Every draining former-holder of `placement` (see [`is_draining_former_holder`]).
/// Co-holders keep these peers in the shard topic's membership and publisher set
/// until they leave the config, so their in-flight flush is never cut off. Cheap
/// no-op when nothing is draining.
pub fn draining_former_holders(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    if !config.placement_map.iter().any(|entry| entry.draining) {
        return Vec::new();
    }
    config
        .placement_map
        .iter()
        .filter(|entry| entry.draining)
        .map(|entry| entry.node_id)
        .filter(|node_id| is_draining_former_holder(config, placement, *node_id))
        .collect()
}

/// Buckets of `strategy` that `node_id` is a holder of. Empty when the node is
/// not sync-eligible, is unknown to the config, or is filtered out everywhere.
pub fn held_buckets(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    node_id: NodeId,
) -> Vec<u32> {
    // Buckets of one strategy usually share an activated epoch, so the frozen
    // views are built once each rather than once per bucket.
    let mut views: std::collections::BTreeMap<Option<u64>, PlacementView> =
        std::collections::BTreeMap::new();
    let mut held = Vec::new();
    for shard in 0..strategy.shard_count {
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            shard,
        };
        let Ok(epoch) = selection_epoch(config, &placement) else {
            continue;
        };
        if !views.contains_key(&epoch) {
            let Ok(view) = view_for_epoch(config, epoch) else {
                continue;
            };
            views.insert(epoch, view);
        }
        let view = &views[&epoch];
        if resolve_shard_holders_from_view(config, view, strategy, &placement).contains(&node_id) {
            held.push(shard);
        }
    }
    held
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
        shard: held[best],
    })
}

fn resolve_shard_holders_with(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    let view = build_view(config);
    resolve_shard_holders_from_view(config, &view, strategy, placement)
}

fn resolve_shard_holders_from_view(
    config: &RealmConfigDocument,
    view: &PlacementView,
    strategy: &PlacementStrategy,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    resolve_holders(
        view,
        strategy,
        &shard_subject_bytes(placement),
        shard_override(config, placement),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{
        BindingScope, CandidatePlacementMap, MetadataRegistryRecord, NodePlacementEntry, RealmId,
        RealmNodeKind, StrategyBinding,
    };
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
    fn registry_uses_class() {
        let (mut config, _) = config_and_placement();
        let group_id = Ulid::from_bytes([6u8; 16]);
        let document_id = Ulid::from_bytes([7u8; 16]);
        let general_strategy_id = config.default_strategy_id.unwrap();
        let class_strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([8u8; 16]),
            name: "registry".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 16,
        };
        config.strategies.push(class_strategy.clone());
        config.strategy_bindings = vec![
            StrategyBinding {
                scope: BindingScope::Group(group_id),
                strategy_id: general_strategy_id,
            },
            StrategyBinding {
                scope: BindingScope::Class(DocumentClass::MetadataRegistry),
                strategy_id: class_strategy.strategy_id,
            },
        ];
        config.placement_overrides = vec![PlacementOverride {
            subject: document_id.to_bytes().to_vec(),
            pinned: Vec::new(),
            excluded: Vec::new(),
            strategy_id: Some(general_strategy_id),
        }];
        let record = MetadataRegistryRecord {
            realm_id: RealmId::from_bytes([3u8; 32]),
            group_id,
            document_id,
            document_path: "datasets/example".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: String::new(),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
            establishing_event_id: Ulid::from_bytes([9u8; 16]),
            last_event_id: Ulid::from_bytes([9u8; 16]),
        };

        let placement = registry_placement(&config, &record);
        assert_eq!(placement.strategy_id, class_strategy.strategy_id);
        assert_eq!(
            placement.shard,
            shard_for_subject(&document_id.to_bytes(), class_strategy.shard_count)
        );

        config.strategy_bindings[1].strategy_id = Ulid::from_bytes([99u8; 16]);
        assert_eq!(registry_placement(&config, &record), PlacementRef::NIL);

        config.strategy_bindings.truncate(1);
        let fallback = registry_placement(&config, &record);
        assert_eq!(fallback.strategy_id, config.default_strategy_id.unwrap());

        config.default_strategy_id = Some(Ulid::from_bytes([99u8; 16]));
        assert_eq!(registry_placement(&config, &record), PlacementRef::NIL);

        config.default_strategy_id = None;
        assert_eq!(
            registry_placement(&config, &record).strategy_id,
            config.strategies[0].strategy_id
        );

        config.strategies.clear();
        assert_eq!(registry_placement(&config, &record), PlacementRef::NIL);
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
    fn empty_holders_rejected() {
        let (mut config, placement) = config_and_placement();
        assert!(holds_placement(&config, &placement, node(1)));

        config.nodes.clear();
        assert!(resolve_shard_holders(&config, &placement).is_empty());
        assert!(!holds_placement(&config, &placement, node(1)));

        let dangling = PlacementRef {
            strategy_id: Ulid::from_bytes([99u8; 16]),
            ..placement
        };
        assert!(resolve_shard_holders(&config, &dangling).is_empty());
        assert!(!holds_placement(&config, &dangling, node(1)));
        assert!(holds_placement(&config, &PlacementRef::NIL, node(1)));
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

    #[test]
    fn activation_pins_holders() {
        // Once a map is activated, edits to the live placement map are inert:
        // only a completed transition may move the bucket.
        let (mut config, placement) = config_and_placement();
        config.snapshot_candidate_map();
        let pinned = resolve_shard_holders(&config, &placement);
        assert_eq!(pinned.len(), 2);

        for seed in 5..=8u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        config.placement_map.push(NodePlacementEntry {
            node_id: pinned[0],
            location: "moved".to_string(),
            weight: 1,
            full: true,
            draining: false,
            labels: std::collections::BTreeMap::new(),
        });
        assert_eq!(resolve_shard_holders(&config, &placement), pinned);

        // Publishing a newer map is not activation either.
        config.snapshot_candidate_map();
        assert_eq!(resolve_shard_holders(&config, &placement), pinned);
        assert_ne!(
            resolve_holders(
                &view_from_map(config.candidate_map(2).expect("newest map")),
                strategy_of(&config),
                &shard_subject_bytes(&placement),
                None,
            ),
            pinned
        );
    }

    #[test]
    fn missing_activation_fails_closed() {
        let (mut config, placement) = config_and_placement();
        config.snapshot_candidate_map();
        let strategy_id = placement.strategy_id;

        config
            .placement_activations
            .retain(|activation| activation.shard != placement.shard);
        assert_eq!(
            resolve_shard_holders_checked(&config, &placement),
            Err(PlacementResolveError::ActivationUnavailable(
                placement.shard
            ))
        );
        assert!(resolve_shard_holders(&config, &placement).is_empty());
        assert!(!holds_placement(&config, &placement, node(1)));

        // Two divergent activations for one bucket are a conflict, not a winner.
        for epoch in [1, 2] {
            config
                .placement_activations
                .push(aruna_core::structs::PlacementActivation {
                    strategy_id,
                    shard: placement.shard,
                    activation_epoch: 1,
                    candidate_map_epoch: epoch,
                    transition_id: None,
                });
        }
        assert_eq!(
            resolve_shard_holders_checked(&config, &placement),
            Err(PlacementResolveError::ActivationConflicted(placement.shard))
        );

        // An activation naming an unpublished map resolves nothing either.
        config
            .placement_activations
            .retain(|activation| activation.candidate_map_epoch != 2);
        config.candidate_maps.clear();
        config.candidate_maps.push(CandidatePlacementMap {
            epoch: 5,
            nodes: Vec::new(),
        });
        assert_eq!(
            resolve_shard_holders_checked(&config, &placement),
            Err(PlacementResolveError::CandidateMapUnavailable(1))
        );

        let unknown = PlacementRef {
            strategy_id: Ulid::from_bytes([99u8; 16]),
            shard: 0,
        };
        assert_eq!(
            resolve_shard_holders_checked(&config, &unknown),
            Err(PlacementResolveError::StrategyUnknown(unknown.strategy_id))
        );
    }

    #[test]
    fn empty_shard_check_uses_newest_map() {
        // The guard validates the map a future activation would pin, not the
        // one already activated.
        let (mut config, _) = config_and_placement();
        for seed in 1..=4u8 {
            config.placement_map.push(NodePlacementEntry {
                node_id: node(seed),
                location: String::new(),
                weight: aruna_core::structs::DEFAULT_NODE_WEIGHT,
                full: false,
                draining: false,
                labels: std::collections::BTreeMap::from([("tier".to_string(), "hot".to_string())]),
            });
        }
        config.snapshot_candidate_map();
        for strategy in config.strategies.iter_mut() {
            strategy.affinity = vec![aruna_core::structs::AffinityRule {
                matcher: aruna_core::structs::LabelMatch {
                    key: "tier".to_string(),
                    value: "hot".to_string(),
                },
                effect: aruna_core::structs::AffinityEffect::Filter,
            }];
        }
        assert_eq!(first_empty_referenced_shard(&config), None);

        // The frozen map still carries the matching labels, so nothing is empty
        // until the edit is snapshotted into a new map.
        for entry in config.placement_map.iter_mut() {
            entry.labels.insert("tier".to_string(), "cold".to_string());
        }
        assert_eq!(first_empty_referenced_shard(&config), None);
        config.snapshot_candidate_map();
        assert!(first_empty_referenced_shard(&config).is_some());
    }

    #[test]
    fn read_union_spans_transition() {
        // Readers must reach both sides of a bucket in flight; writers must not.
        let (mut config, placement) = config_and_placement();
        config.snapshot_candidate_map();
        let activated = resolve_shard_holders(&config, &placement);
        assert_eq!(read_holder_sets(&config, &placement), Ok(activated.clone()));

        let targets: Vec<NodeId> = (5..=6u8).map(node).collect();
        config
            .placement_transitions
            .push(aruna_core::structs::PlacementTransition::new(
                aruna_core::structs::TransitionPlan {
                    transition_id: Ulid::from_bytes([7; 16]),
                    strategy_id: placement.strategy_id,
                    buckets: vec![aruna_core::structs::BucketPlan {
                        bucket: placement.shard,
                        old_holders: activated.clone(),
                        target_holders: targets.clone(),
                    }],
                    target_map_epoch: 2,
                    limits: Default::default(),
                    created_by: node(1),
                    created_at_ms: 1,
                },
            ));

        let union = read_holder_sets(&config, &placement).expect("bucket resolves");
        assert_eq!(union[..activated.len()], activated[..]);
        for target in &targets {
            assert!(union.contains(target));
        }
        assert_eq!(union.len(), activated.len() + targets.len());
        // Write authority stays the activation alone.
        assert_eq!(resolve_shard_holders(&config, &placement), activated);
        for target in &targets {
            assert!(!holds_placement(&config, &placement, *target));
        }
    }

    #[test]
    fn holders_limit_order() {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([3u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([5u8; 16]),
            name: "default".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 1,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy];
        for seed in 1..=64u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let placement = PlacementRef {
            strategy_id: config.default_strategy_id.expect("default strategy"),
            shard: 0,
        };
        let full = resolve_shard_holders(&config, &placement);
        let bounded = resolve_holders_limit(&config, &placement, MAX_READ_HOLDERS);

        assert_eq!(full.len(), 64);
        assert_eq!(bounded, full[..MAX_READ_HOLDERS].to_vec());
    }
}
