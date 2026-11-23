//! Maps documents to placement buckets and resolves which nodes hold each bucket, as pure
//! functions of the realm config. Also holds the drain and membership rules built on them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(test)]
mod distribution;
pub mod fence;
pub mod resolver;
pub mod selector;
pub mod transition;

use crate::document::DocumentTarget;
use crate::id::NodeId;
use crate::structs::identity::realm::RealmConfigDocument;
use crate::structs::placement::record::{
    DocumentClass, PlacementOverride, PlacementRef, PlacementStrategy, shard_for_subject,
};
use crate::structs::placement::transition::CandidatePlacementMap;
use crate::types::GroupId;
use ulid::Ulid;

use crate::placement::selector::{ROLE_SHARD, rank_weighted};

use resolver::{
    PlacementResolutionContext, PlacementView, build_view, resolve_holders, strategy_for_target,
    subject_bytes, view_from_map,
};

/// Canonical rendezvous subject for a shard's holder resolution:
/// `strategy_id(16) ‖ shard(4, big-endian)`. The epoch is excluded (spec 6.3.1),
/// so the holder set is a pure function of the bucket and never per-document.
pub fn shard_subject_bytes(placement: &PlacementRef) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20);
    bytes.extend_from_slice(&placement.strategy_id.to_bytes());
    bytes.extend_from_slice(&placement.shard.to_be_bytes());
    bytes
}

/// Holder pin/exclude override for a shard, matched on the shard subject because
/// one shard topic has one holder set. Per-document overrides steer strategy
/// selection only; their pin/exclude lists are inert for holders.
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

/// Placement reference stamped into a change's sync envelope for `target`:
/// resolves the governing strategy from the realm config, falling back to
/// [`PlacementRef::NIL`] only when the realm has no strategies (early bootstrap).
pub fn target_placement_ref(
    config: &RealmConfigDocument,
    target: &DocumentTarget,
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

/// Bucket the document's registry row rides: resolved from the registry class,
/// not the document's precedence chain, so every node carries the row while the
/// document's replica-capped bucket reaches only its holders.
pub fn registry_placement(
    config: &RealmConfigDocument,
    record: &crate::structs::storage::metadata_registry::MetadataRegistryRecord,
) -> PlacementRef {
    registry_placement_for(config, record.group_id, record.document_id)
}

pub fn registry_placement_for(
    config: &RealmConfigDocument,
    group_id: GroupId,
    document_id: Ulid,
) -> PlacementRef {
    let Some(strategy) = registry_strategy(config) else {
        return PlacementRef::NIL;
    };
    let target = DocumentTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    PlacementRef {
        strategy_id: strategy.strategy_id,
        shard: shard_for_subject(&subject_bytes(&target), strategy.shard_count),
    }
}

pub fn registry_strategy(config: &RealmConfigDocument) -> Option<&PlacementStrategy> {
    resolver::strategy_for_class(config, DocumentClass::MetadataRegistry)
}

/// Placement plan for a document `target`: its shard's rank-ordered holder set,
/// the nominal replica target the pending machinery tops up toward, and the
/// envelope reference. `None` when no strategy governs the target.
pub struct TargetPlacementPlan {
    pub holders: Vec<NodeId>,
    pub desired_count: usize,
    pub placement: PlacementRef,
}

/// `Ok(None)` only when no strategy governs the target; a governed target
/// whose bucket cannot resolve is an error the caller must retry durably,
/// never an empty placement.
pub fn plan_target_placement(
    config: &RealmConfigDocument,
    target: &DocumentTarget,
    context: PlacementResolutionContext<'_>,
) -> Result<Option<TargetPlacementPlan>, PlacementResolveError> {
    let Some((strategy, _override)) = strategy_for_target(config, target, context) else {
        return Ok(None);
    };
    let placement = PlacementRef {
        strategy_id: strategy.strategy_id,
        shard: shard_for_subject(&subject_bytes(target), strategy.shard_count),
    };
    let selection = frozen_selection(config, strategy, selection_epoch(config, &placement)?)?;
    let holders = selection.resolve(&placement);
    // The nominal replica target is the effective selector's: frozen once the
    // bucket is pinned, live only during bootstrap.
    let desired_count = match selection.replica_count() {
        Some(count) => count as usize,
        None => holders.len(),
    };
    Ok(Some(TargetPlacementPlan {
        holders,
        desired_count,
        placement,
    }))
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

/// Rank-ordered holders of a specific shard, capped by `replica_count` or all
/// eligible for an everywhere strategy. Returns `Vec::new()` for housekeeping
/// callers whenever [`try_resolve_holders`] fails.
pub fn resolve_shard_holders(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Vec<NodeId> {
    try_resolve_holders(config, placement).unwrap_or_default()
}

/// Holder resolution pinned to the bucket's activated candidate map: a config
/// edit moves no holder, only a completed transition can. Routing callers map
/// failures to 503, because an unresolvable bucket is not an absent document.
pub fn try_resolve_holders(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
) -> Result<Vec<NodeId>, PlacementResolveError> {
    let strategy =
        config
            .strategy(&placement.strategy_id)
            .ok_or(PlacementResolveError::StrategyUnknown(
                placement.strategy_id,
            ))?;
    let selection = frozen_selection(config, strategy, selection_epoch(config, placement)?)?;
    Ok(selection.resolve(placement))
}

pub(crate) const MAX_READ_HOLDERS: usize = 32;

/// Resolves a shard's canonical holder order with a bounded replica target.
pub fn resolve_holders_limit(
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
    let mut selection = frozen_selection(config, strategy, selection_epoch(config, placement)?)?;
    // The read cap bounds the frozen replica count, never the live one.
    selection.bound_replicas(limit.min(u32::MAX as usize) as u32);
    Ok(selection.resolve(placement))
}

/// Read fan-out for a bucket: activated holders first, then every holder a
/// transition still names, rank-ordered and deduped. The record's lifetime is
/// the catch-up window, so no clock is read; writes union nothing.
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

/// The complete selection inputs a bucket resolves from: either the live
/// config (bootstrap, before any map) or one frozen candidate map whose
/// selector and shard overrides never see later config edits.
pub(crate) struct FrozenSelection<'a> {
    view: PlacementView,
    strategy: PlacementStrategy,
    map: Option<&'a CandidatePlacementMap>,
    config: &'a RealmConfigDocument,
}

impl FrozenSelection<'_> {
    pub(crate) fn resolve(&self, placement: &PlacementRef) -> Vec<NodeId> {
        let subject = shard_subject_bytes(placement);
        let override_ = match self.map {
            Some(map) => map.shard_override(&subject),
            None => shard_override(self.config, placement),
        };
        resolve_holders(&self.view, &self.strategy, &subject, override_)
    }

    fn bound_replicas(&mut self, limit: u32) {
        self.strategy.replica_count = Some(
            self.strategy
                .replica_count
                .map_or(limit, |replicas| replicas.min(limit)),
        );
    }

    pub(crate) fn replica_count(&self) -> Option<u32> {
        self.strategy.replica_count
    }
}

/// Builds the selection inputs for `strategy` at `epoch`. A map that predates
/// the strategy fails closed: an activated bucket must never fall back to
/// live selector fields.
pub(crate) fn frozen_selection<'a>(
    config: &'a RealmConfigDocument,
    strategy: &PlacementStrategy,
    epoch: Option<u64>,
) -> Result<FrozenSelection<'a>, PlacementResolveError> {
    match epoch {
        None => Ok(FrozenSelection {
            view: build_view(config),
            strategy: strategy.clone(),
            map: None,
            config,
        }),
        Some(epoch) => {
            let map = config
                .candidate_map(epoch)
                .ok_or(PlacementResolveError::CandidateMapUnavailable(epoch))?;
            map_selection(config, strategy, map)
                .ok_or(PlacementResolveError::CandidateMapUnavailable(epoch))
        }
    }
}

/// Selection inputs for one explicit map; `None` when the map predates the
/// strategy and carries no frozen selector for it.
pub(crate) fn map_selection<'a>(
    config: &'a RealmConfigDocument,
    strategy: &PlacementStrategy,
    map: &'a CandidatePlacementMap,
) -> Option<FrozenSelection<'a>> {
    let selector = map.selector(&strategy.strategy_id)?;
    Some(FrozenSelection {
        view: view_from_map(map),
        strategy: PlacementStrategy {
            strategy_id: strategy.strategy_id,
            name: strategy.name.clone(),
            replica_count: selector.replica_count,
            distinct_locations: selector.distinct_locations,
            affinity: selector.affinity.clone(),
            shard_count: strategy.shard_count,
        },
        map: Some(map),
        config,
    })
}

/// First shard of a referenced strategy that resolves to zero holders while the
/// realm still has usable capacity: a filter, affinity or override leaves
/// documents with nowhere to live. Usable-node-free realms are not flagged.
pub fn first_empty_shard(config: &RealmConfigDocument) -> Option<PlacementRef> {
    // Checked against the newest map, which is what a future activation would
    // pin: activated buckets are already frozen and cannot be emptied by an edit.
    let newest_epoch = config
        .newest_map_epoch()
        .filter(|epoch| config.candidate_map(*epoch).is_some());
    let capacity_view = match newest_epoch.and_then(|epoch| config.candidate_map(epoch)) {
        Some(map) => view_from_map(map),
        None => build_view(config),
    };
    let has_capacity = capacity_view.nodes.iter().any(|node| {
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
        // A strategy the newest map predates checks against the live view: a
        // future map publication is what would freeze it.
        let epoch = newest_epoch.filter(|epoch| {
            config
                .candidate_map(*epoch)
                .is_some_and(|map| map.selector(&id).is_some())
        });
        let Ok(selection) = frozen_selection(config, strategy, epoch) else {
            continue;
        };
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: id,
                shard,
            };
            if selection.resolve(&placement).is_empty() {
                return Some(placement);
            }
        }
    }
    None
}

/// Desired shard-topic membership for a bucket: activated holders plus every
/// holder a live transition still names. Targets need membership to pull history,
/// old holders to serve it until release; `grace_ms` bounds the window (#399).
pub struct BucketMembership {
    /// Sync membership and pull/delivery peers.
    pub members: Vec<NodeId>,
    /// Accepted authors: activated holders plus retained departing holders.
    pub publishers: Vec<NodeId>,
}

/// Per-bucket membership and publish authority, derived from each bucket's own
/// completion and status: admitted incomplete buckets add targets as members
/// only, completed ones retain departing holders through grace, aborted add none.
pub fn bucket_membership(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    now_ms: u64,
) -> BucketMembership {
    let base = resolve_shard_holders(config, placement);
    let mut members = base.clone();
    let mut publishers = base;
    for transition in &config.placement_transitions {
        if transition.plan.strategy_id != placement.strategy_id {
            continue;
        }
        let Some(bucket) = transition.plan.bucket_plan(placement.shard) else {
            continue;
        };
        match transition.completion(bucket.bucket) {
            None => {
                // Only targets inside the admitted window join, bounding the
                // join/pull fan-out to the plan's in-flight limit.
                if matches!(
                    transition.status,
                    crate::structs::placement::transition::TransitionStatus::Active
                ) && transition
                    .plan
                    .admitted_buckets(&transition.completed)
                    .any(|admitted| admitted.bucket == bucket.bucket)
                {
                    for node_id in &bucket.target_holders {
                        push_unique(&mut members, *node_id);
                    }
                }
            }
            Some(completion) => {
                // Retention ends after both grace and the holder's reduced drain report.
                let retained_until = completion
                    .completed_at_ms
                    .saturating_add(transition.plan.limits.grace_ms);
                for node_id in &bucket.old_holders {
                    if !bucket.target_holders.contains(node_id)
                        && (now_ms < retained_until
                            || !transition.drain_reported(bucket.bucket, *node_id))
                    {
                        push_unique(&mut members, *node_id);
                        push_unique(&mut publishers, *node_id);
                    }
                }
            }
        }
    }
    BucketMembership {
        members,
        publishers,
    }
}

fn push_unique(nodes: &mut Vec<NodeId>, node_id: NodeId) {
    if !nodes.contains(&node_id) {
        nodes.push(node_id);
    }
}

/// The earliest future instant a completed bucket's retention can end (its
/// grace deadline). Buckets already past grace contribute nothing here: they
/// either released or await a drain report, which the normal retry drives.
pub fn next_release_ms(config: &RealmConfigDocument, now_ms: u64) -> Option<u64> {
    let mut earliest: Option<u64> = None;
    for transition in &config.placement_transitions {
        for completion in &transition.completed {
            if transition.departing_holders(completion.bucket).is_empty() {
                continue;
            }
            let deadline = completion
                .completed_at_ms
                .saturating_add(transition.plan.limits.grace_ms);
            if deadline > now_ms {
                earliest = Some(earliest.map_or(deadline, |current| current.min(deadline)));
            }
        }
    }
    earliest
}

/// Whether any completed bucket past its grace still awaits a departing
/// holder's drain report, so the caller keeps a normal retry armed.
pub fn drain_pending(config: &RealmConfigDocument, now_ms: u64) -> bool {
    config.placement_transitions.iter().any(|transition| {
        transition.completed.iter().any(|completion| {
            let deadline = completion
                .completed_at_ms
                .saturating_add(transition.plan.limits.grace_ms);
            now_ms >= deadline
                && transition
                    .departing_holders(completion.bucket)
                    .into_iter()
                    .any(|holder| !transition.drain_reported(completion.bucket, holder))
        })
    })
}

/// Whether `node_id` is a cut-over old holder whose retention has not ended:
/// its queued pre-cutover records stay deliverable until grace elapses AND its
/// drain report reduces (never a clock alone).
pub fn retained_departing_holder(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    node_id: NodeId,
) -> bool {
    config.placement_transitions.iter().any(|transition| {
        transition.plan.strategy_id == placement.strategy_id
            && transition.completion(placement.shard).is_some()
            && transition
                .departing_holders(placement.shard)
                .contains(&node_id)
            && !transition.drain_reported(placement.shard, node_id)
    })
}

/// Whether `node_id` holds `placement` and may publish onto its topic. NIL means
/// no strategy governs the bucket during bootstrap, so all nodes count as
/// holders. A local document copy is never evidence of holdership.
pub fn holds_placement(
    config: &RealmConfigDocument,
    placement: &PlacementRef,
    node_id: NodeId,
) -> bool {
    let holders = resolve_shard_holders(config, placement);
    *placement == PlacementRef::NIL || holders.contains(&node_id)
}

/// Whether `node_id` is a draining former-holder of `placement`: marked draining
/// yet it would hold the shard with that flag cleared. It keeps publish rights
/// until its outbox flushes (DECISIONS K3); a fully removed node does not.
pub fn is_draining_holder(
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
pub fn first_draining_change(
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
                if is_draining_holder(pre, &placement, node_id)
                    && !is_draining_holder(post, &placement, node_id)
                {
                    return Some((node_id, placement));
                }
            }
        }
    }
    None
}

/// Every draining former-holder of `placement` (see [`is_draining_holder`]).
/// Co-holders keep these peers in the topic's membership and publisher set until
/// they leave the config, so their in-flight flush is never cut off.
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
        .filter(|node_id| is_draining_holder(config, placement, *node_id))
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
    // selections are built once each rather than once per bucket.
    let mut selections: std::collections::BTreeMap<Option<u64>, FrozenSelection<'_>> =
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
        let selection = match selections.entry(epoch) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                let Ok(selection) = frozen_selection(config, strategy, epoch) else {
                    continue;
                };
                entry.insert(selection)
            }
        };
        if selection.resolve(&placement).contains(&node_id) {
            held.push(shard);
        }
    }
    held
}

/// Bucket the create-receiving node picks for `subject`: the best-ranked of the
/// buckets it already holds, so the origin can always publish onto its topic.
/// `None` when the origin holds no bucket of the strategy.
pub fn choose_origin_bucket(
    config: &RealmConfigDocument,
    strategy: &PlacementStrategy,
    origin: NodeId,
    subject: &[u8],
) -> Option<PlacementRef> {
    let held = held_buckets(config, strategy, origin);
    // Buckets this node leads keep the create local: the leading holder is
    // also the persistent-id authority, so no forward is needed.
    let led: Vec<u32> = held
        .iter()
        .copied()
        .filter(|shard| {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                shard: *shard,
            };
            resolve_shard_holders(config, &placement).first() == Some(&origin)
        })
        .collect();
    let pool = if led.is_empty() { &held } else { &led };
    let candidates: Vec<([u8; 4], u64)> =
        pool.iter().map(|shard| (shard.to_be_bytes(), 1)).collect();
    let best = *rank_weighted(ROLE_SHARD, subject, &candidates).first()?;
    Some(PlacementRef {
        strategy_id: strategy.strategy_id,
        shard: pool[best],
    })
}

#[cfg(test)]
#[path = "tests.rs"]
mod pure_tests;
