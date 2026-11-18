//! Plans a bounded restore of this node's sync topics from the realm config alone: held shards
//! grouped by co-holders into work units, the cursor that resumes them, and the pass summary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::document::{DocumentTarget, shard_topic_id};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;

use crate::node::recovery::RecoveryError;
use aruna_core::placement::{draining_former_holders, resolve_shard_holders};

/// Shared realm-scoped topics every node subscribes to (placement is inert on
/// these; see [`DocumentTarget::sync_topic_id`]).
pub const RESTORE_TOPIC_COUNT: usize = 5;

pub(crate) fn shared_targets(
    realm_id: RealmId,
    node_id: NodeId,
) -> [DocumentTarget; RESTORE_TOPIC_COUNT] {
    [
        DocumentTarget::RealmAuthorization { realm_id },
        DocumentTarget::RealmConfig { realm_id },
        DocumentTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        },
        DocumentTarget::NodeInfo { realm_id, node_id },
        DocumentTarget::WatchInterest { realm_id, node_id },
    ]
}

/// Whether a shared target belongs to the realm rather than to one node.
/// Node-owned targets are planned by that node alone; realm-wide ones need a
/// single designated minter.
pub(crate) fn realm_wide_target(target: &DocumentTarget) -> bool {
    matches!(
        target,
        DocumentTarget::RealmConfig { .. } | DocumentTarget::RealmAuthorization { .. }
    )
}

pub(crate) fn sync_eligible_node(config: &RealmConfigDocument, node_id: NodeId) -> bool {
    let node_id = node_id.to_string();
    config
        .nodes
        .iter()
        .any(|node| node.node_id == node_id && node.kind.is_sync_eligible())
}

pub(crate) fn shared_topic_peers(config: &RealmConfigDocument, node_id: NodeId) -> Vec<NodeId> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .filter(|candidate| *candidate != node_id)
        .collect()
}

/// Work units one bounded restore pass may process. A work-unit limit, never a
/// wall-clock deadline, so the bound holds however many shards share a
/// co-holder set.
pub(crate) const RESTORE_UNIT_BUDGET: usize = 8;
/// Topics per work unit; one unit still fills a document sync topic batch.
pub(crate) const RESTORE_CHUNK_TOPICS: usize = 64;

/// What a [`restore_shard_subscriptions`] pass touched. The load-bearing
/// invariant is `shard_topics == held_shards`, i.e. one topic per held shard,
/// never one per stored document, as asserted by the restart-traffic gate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RestoreShardSummary {
    /// Shards the local node resolves into a holder of.
    pub held_shards: usize,
    /// Shard sync topics ensured (rank-0) or joined that this pass touched.
    pub shard_topics: usize,
    /// Fixed shared realm topics restored ([`RESTORE_TOPIC_COUNT`]).
    pub shared_topics: usize,
    /// Held topics this pass could not finish because a co-holder was absent or
    /// refused a genesis probe. Retryable background state, never a start error.
    pub withheld_topics: usize,
}

impl RestoreShardSummary {
    /// Total distinct topics the restore ensured or joined.
    pub fn total_topics(&self) -> usize {
        self.shard_topics + self.shared_topics
    }
}

/// Where a bounded restore resumes, bound to the exact current work plan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ShardRestoreCursor {
    pub(crate) next_unit: usize,
    pub(crate) plan_hash: Option<[u8; 32]>,
}

/// What one bounded [`restore_shard_pass`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShardRestorePass {
    pub summary: RestoreShardSummary,
    /// Never more than [`RESTORE_UNIT_BUDGET`].
    pub units_processed: usize,
    pub topics_completed: usize,
    pub units_total: usize,
    pub unvisited_topics: usize,
    pub unresolved_topics: BTreeSet<::irokle::TopicId>,
    pub error: Option<RecoveryError>,
    pub plan_changed: bool,
    /// Whether the cursor reached the end and wrapped back to the head.
    pub wrapped: bool,
    /// False when storage failed before the plan could be read: the pass
    /// observed nothing about outstanding work.
    pub enumerated: bool,
}

pub(crate) type ShardGroup = (Vec<NodeId>, Vec<NodeId>, BTreeSet<NodeId>);

/// Restore work grouped by co-holder and retained peer sets, so matching shards
/// ride one ensure and one sync instead of one round trip each.
pub(crate) struct ShardPlan {
    pub(crate) summary: RestoreShardSummary,
    pub(crate) shared_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>>,
    pub(crate) shared_join_groups: BTreeMap<Vec<NodeId>, Vec<::irokle::TopicId>>,
    pub(crate) rank0_groups: BTreeMap<ShardGroup, Vec<::irokle::TopicId>>,
    pub(crate) join_groups: BTreeMap<ShardGroup, Vec<::irokle::TopicId>>,
}

/// Derives the restore plan from the realm config alone. I/O-free, so the local
/// serving gate and the bounded remote pass read the same view.
pub(crate) fn plan_shard_groups(
    config: &RealmConfigDocument,
    node_id: NodeId,
    realm_id: RealmId,
    now_ms: u64,
) -> ShardPlan {
    let mut plan = ShardPlan {
        summary: RestoreShardSummary::default(),
        shared_groups: BTreeMap::new(),
        shared_join_groups: BTreeMap::new(),
        rank0_groups: BTreeMap::new(),
        join_groups: BTreeMap::new(),
    };

    let mut shared_peers = shared_topic_peers(config, node_id);
    shared_peers.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    // The lowest sync-eligible node mints realm-wide topics.
    // Applying this bound everywhere prevents concurrent minters from forking documents.
    let realm_minter = sync_eligible_node(config, node_id)
        && shared_peers
            .iter()
            .all(|peer| node_id.as_bytes() < peer.as_bytes());
    let device_local = !sync_eligible_node(config, node_id);
    for target in shared_targets(realm_id, node_id) {
        let wide = realm_wide_target(&target);
        // Devices route realm-wide reads and mint only their private local topics.
        if device_local && wide {
            continue;
        }
        let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
        let groups = if wide && !realm_minter {
            &mut plan.shared_join_groups
        } else {
            &mut plan.shared_groups
        };
        let peers = match device_local {
            true => Vec::new(),
            false => shared_peers.clone(),
        };
        groups.entry(peers).or_default().push(topic);
        plan.summary.shared_topics += 1;
    }

    for strategy in &config.strategies {
        for shard in 0..strategy.shard_count {
            let placement = PlacementRef {
                strategy_id: strategy.strategy_id,
                shard,
            };
            let holders = resolve_shard_holders(config, &placement);
            // During transitions, target and retained departing holders stay members.
            // Publishers remain the authority set.
            let membership = aruna_core::placement::bucket_membership(config, &placement, now_ms);
            if !membership.members.contains(&node_id) {
                continue;
            }
            plan.summary.held_shards += 1;
            let local_is_rank0 = holders.first() == Some(&node_id);
            let mut co_members: Vec<NodeId> = membership
                .members
                .into_iter()
                .filter(|candidate| *candidate != node_id)
                .collect();
            co_members.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            if co_members.is_empty() {
                continue;
            }
            let mut publishers = membership.publishers;
            publishers.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            let topic = shard_topic_id(realm_id, &placement);
            let retained: BTreeSet<NodeId> = draining_former_holders(config, &placement)
                .into_iter()
                .collect();
            let groups = if local_is_rank0 {
                &mut plan.rank0_groups
            } else {
                &mut plan.join_groups
            };
            groups
                .entry((co_members, publishers, retained))
                .or_default()
                .push(topic);
            plan.summary.shard_topics += 1;
        }
    }
    plan
}

/// The half-open unit range one bounded pass processes, and whether it wraps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RestoreRange {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) wrapped: bool,
}

pub(crate) fn restore_range(cursor: ShardRestoreCursor, units_total: usize) -> RestoreRange {
    let start = cursor.next_unit.min(units_total);
    let end = (start + RESTORE_UNIT_BUDGET).min(units_total);
    RestoreRange {
        start,
        end,
        wrapped: end >= units_total,
    }
}

/// One bounded unit of restore work: one co-holder group's ensure and sync over
/// a chunk of topics.
#[derive(Clone, Debug)]
pub(crate) struct RestoreUnit {
    pub(crate) kind: RestoreKind,
    pub(crate) peers: Vec<NodeId>,
    pub(crate) publishers: Vec<NodeId>,
    pub(crate) retained: BTreeSet<NodeId>,
    pub(crate) topics: Vec<::irokle::TopicId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RestoreKind {
    /// Shared topics this node may mint: its own node-owned ones, and the
    /// realm-wide ones when it is the designated minter.
    Shared,
    /// Realm-wide topics minted elsewhere: probe and adopt, never create.
    SharedJoin,
    /// Rank-0: create a genesis only with positive co-holder confirmation.
    Rank0,
    /// Held at another rank: join-only.
    Join,
}

pub(crate) fn bind_restore_plan(cursor: &mut ShardRestoreCursor, units: &[RestoreUnit]) -> bool {
    let plan_hash = restore_plan_hash(units);
    let changed = cursor.plan_hash != Some(plan_hash);
    if changed {
        cursor.next_unit = 0;
        cursor.plan_hash = Some(plan_hash);
    }
    changed
}

pub(crate) fn restore_plan_hash(units: &[RestoreUnit]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for unit in units {
        let kind = match unit.kind {
            RestoreKind::Shared => 0,
            RestoreKind::Rank0 => 1,
            RestoreKind::Join => 2,
            RestoreKind::SharedJoin => 3,
        };
        hasher.update(&[kind]);
        hasher.update(&(unit.peers.len() as u64).to_be_bytes());
        for peer in &unit.peers {
            hasher.update(peer.as_bytes());
        }
        hasher.update(&(unit.publishers.len() as u64).to_be_bytes());
        for peer in &unit.publishers {
            hasher.update(peer.as_bytes());
        }
        hasher.update(&(unit.retained.len() as u64).to_be_bytes());
        for peer in &unit.retained {
            hasher.update(peer.as_bytes());
        }
        hasher.update(&(unit.topics.len() as u64).to_be_bytes());
        for topic in &unit.topics {
            hasher.update(topic.as_bytes());
        }
    }
    *hasher.finalize().as_bytes()
}

impl ShardPlan {
    /// Flattens the plan into a deterministic list of bounded work units.
    pub(crate) fn into_units(self) -> Vec<RestoreUnit> {
        let mut units = Vec::new();
        // A unit without peers still has local work: its topics are ensured and
        // minted here, and only the peer exchange is skipped.
        let mut push = |kind,
                        peers: Vec<NodeId>,
                        publishers: Vec<NodeId>,
                        retained: BTreeSet<NodeId>,
                        topics: Vec<_>| {
            if topics.is_empty() {
                return;
            }
            for chunk in topics.chunks(RESTORE_CHUNK_TOPICS) {
                units.push(RestoreUnit {
                    kind,
                    peers: peers.clone(),
                    publishers: publishers.clone(),
                    retained: retained.clone(),
                    topics: chunk.to_vec(),
                });
            }
        };
        for (peers, topics) in self.shared_groups {
            push(
                RestoreKind::Shared,
                peers,
                Vec::new(),
                BTreeSet::new(),
                topics,
            );
        }
        for (peers, topics) in self.shared_join_groups {
            push(
                RestoreKind::SharedJoin,
                peers,
                Vec::new(),
                BTreeSet::new(),
                topics,
            );
        }
        for ((peers, publishers, retained), topics) in self.rank0_groups {
            push(RestoreKind::Rank0, peers, publishers, retained, topics);
        }
        for ((peers, publishers, retained), topics) in self.join_groups {
            push(RestoreKind::Join, peers, publishers, retained, topics);
        }
        units
    }
}

#[derive(Default)]
pub(crate) struct RestoreUnitOutcome {
    pub(crate) unresolved_topics: BTreeSet<::irokle::TopicId>,
    pub(crate) error: Option<RecoveryError>,
}

impl RestoreUnitOutcome {
    pub(crate) fn fail_topics(&mut self, topics: impl IntoIterator<Item = ::irokle::TopicId>) {
        let before = self.unresolved_topics.len();
        self.unresolved_topics.extend(topics);
        if self.unresolved_topics.len() > before {
            self.error = RecoveryError::merge(self.error, Some(RecoveryError::PeerUnavailable));
        }
    }
}
