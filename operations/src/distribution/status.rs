//! Summarizes a distributed bucket from this node's record and heartbeats: members, ranges, a
//! degraded range per unreachable coordinator, and whether configuration changes can reach
//! the publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_KEYSPACE};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionRevision, DistributionSnapshot, DistributionView, MemberState,
    MetadataCoverage, SetPhase, TransferRole,
};
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::node::heartbeat::{MAX_HEARTBEAT_AGE, held_heartbeats};
use crate::placement::process_placements::load_realm_config;

#[derive(Debug, Error, PartialEq)]
pub enum StatusError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

/// Whether this node currently hears from a node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reach {
    Reachable,
    /// No fresh heartbeat; the last one arrived at this wall time, if any did.
    Unreachable {
        since_ms: Option<u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberStatus {
    pub member_id: Ulid,
    pub node_id: NodeId,
    pub state: MemberState,
    pub coverage: MetadataCoverage,
    pub strategy: DataStrategy,
    pub admitted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeStatus {
    pub range_id: Ulid,
    pub lower: String,
    pub upper: Option<String>,
    /// The range this one was split from.
    pub lineage: Option<Ulid>,
    pub coordinator: Ulid,
    pub coordinator_node: Option<NodeId>,
    pub epoch: u64,
    pub replicas: Vec<Ulid>,
    /// The coordinator is unreachable, so writes to the range fail.
    pub degraded: Option<Reach>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributionStatus {
    pub set_id: Ulid,
    pub bucket: String,
    pub owner_group: GroupId,
    pub publisher: NodeId,
    pub revision: DistributionRevision,
    pub phase: SetPhase,
    pub serving: bool,
    /// The publisher signed two revisions of one number; configuration changes stopped.
    pub conflicted: bool,
    /// The publisher is unreachable or was removed from the realm, so configuration changes
    /// wait; data service continues.
    pub configuration_unavailable: bool,
    pub degraded: bool,
    pub members: Vec<MemberStatus>,
    pub ranges: Vec<RangeStatus>,
    /// The grant that covers the bucket on every member node, so reads that pull from another
    /// member pass that member's own check.
    pub recommended_grant: String,
    pub lifecycle: LifecycleStatus,
}

/// Open transfers and deletion from the record, and this node's own fences.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LifecycleStatus {
    pub transfers: Vec<TransferStatus>,
    pub deletion: Option<DeletionStatus>,
    /// Ranges this node fenced as their former coordinator; it never writes them again at the
    /// fenced epoch, whatever reconnects.
    pub fenced_ranges: Vec<Ulid>,
    /// This node applies no incoming publication of the set: it leaves or the set is deleted.
    pub publications_closed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferStatus {
    pub transfer_id: Ulid,
    pub range_id: Ulid,
    pub from: Ulid,
    pub to: Ulid,
    /// The fresh epoch the new coordinator takes.
    pub epoch: u64,
    pub role: TransferRole,
    /// The former coordinator fenced the range; writes to it fail until the activation.
    pub fenced: bool,
    pub completed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeletionStatus {
    pub committed: bool,
    pub fenced: Vec<Ulid>,
    /// Coordinators that have not fenced their ranges yet; the commit waits for them.
    pub waiting: Vec<Ulid>,
}

/// Builds the status from a record; `reach` tells which nodes this node hears from and
/// `eligible` whether the realm config still lets the publisher publish.
pub fn summarize(
    realm_id: RealmId,
    view: &DistributionView,
    reach: impl Fn(NodeId) -> Reach,
    eligible: bool,
) -> DistributionStatus {
    let snapshot = view.snapshot();
    let node_of = |member_id: Ulid| snapshot.member(member_id).map(|member| member.node_id);
    let ranges: Vec<RangeStatus> = snapshot
        .config
        .ranges
        .iter()
        .map(|range| {
            let coordinator_node = node_of(range.coordinator);
            let degraded = coordinator_node
                .map(&reach)
                .filter(|reach| *reach != Reach::Reachable);
            RangeStatus {
                range_id: range.range_id,
                lower: range.lower.clone(),
                upper: range.upper.clone(),
                lineage: range.lineage,
                coordinator: range.coordinator,
                coordinator_node,
                epoch: range.epoch,
                replicas: range.replicas.clone(),
                degraded,
            }
        })
        .collect();
    let members = snapshot
        .config
        .members
        .iter()
        .map(|member| MemberStatus {
            member_id: member.member_id,
            node_id: member.node_id,
            state: member.state,
            coverage: member.coverage,
            strategy: member.strategy,
            admitted: member.incarnation.is_some(),
        })
        .collect();
    let bucket = snapshot.config.bucket.clone();
    let owner_group = snapshot.config.owner_group;
    DistributionStatus {
        set_id: snapshot.set_id(),
        recommended_grant: format!("/{realm_id}/g/{owner_group}/data/*/{bucket}/**"),
        bucket,
        owner_group,
        publisher: snapshot.root.publisher,
        revision: snapshot.revision(),
        phase: snapshot.phase,
        serving: snapshot.serving(),
        conflicted: view.conflicted(),
        configuration_unavailable: !eligible || reach(snapshot.root.publisher) != Reach::Reachable,
        degraded: ranges.iter().any(|range| range.degraded.is_some()),
        members,
        ranges,
        lifecycle: lifecycle(snapshot),
    }
}

/// The record's open transfers and deletion; this node's fences are added by the reader.
fn lifecycle(snapshot: &DistributionSnapshot) -> LifecycleStatus {
    let transfers = (snapshot.lifecycle.transfers.iter())
        .map(|open| TransferStatus {
            transfer_id: open.transfer_id,
            range_id: open.range_id,
            from: open.from,
            to: open.to,
            epoch: open.epoch,
            role: open.role,
            fenced: open.fenced.is_some(),
            completed: open.completed.is_some(),
        })
        .collect();
    let deletion = snapshot
        .lifecycle
        .deletion
        .as_ref()
        .map(|deletion| DeletionStatus {
            committed: deletion.committed,
            fenced: deletion.fenced.clone(),
            waiting: (snapshot.coordinators().into_iter())
                .filter(|coordinator| !deletion.fenced.contains(coordinator))
                .collect(),
        });
    LifecycleStatus {
        transfers,
        deletion,
        ..LifecycleStatus::default()
    }
}

/// Reach from heartbeat ages and wall receive times: this node always, others with a fresh
/// heartbeat.
pub fn heartbeat_reach(
    local: Option<NodeId>,
    heard: BTreeMap<NodeId, (u64, u64)>,
) -> impl Fn(NodeId) -> Reach {
    let max_age_ms = MAX_HEARTBEAT_AGE.as_millis() as u64;
    move |node_id| match heard.get(&node_id) {
        _ if Some(node_id) == local => Reach::Reachable,
        Some((age_ms, _)) if *age_ms <= max_age_ms => Reach::Reachable,
        Some((_, received_at_ms)) => Reach::Unreachable {
            since_ms: Some(*received_at_ms),
        },
        None => Reach::Unreachable { since_ms: None },
    }
}

impl Reach {
    /// The hint an unreachable node adds to a refusal: since when this node has not heard it.
    pub fn hint(&self) -> String {
        match self {
            Self::Reachable => "reachable".to_string(),
            Self::Unreachable { since_ms: Some(ms) } => {
                format!("unreachable since its last heartbeat at {ms} ms Unix time")
            }
            Self::Unreachable { since_ms: None } => {
                "unreachable; no heartbeat arrived since this node started".to_string()
            }
        }
    }
}

/// Whether this node hears from `node_id` now, from its heartbeats.
pub fn node_reach(context: &DriverContext, node_id: NodeId) -> Reach {
    let net = context.net_handle.as_ref();
    let heard = held_heartbeats(net)
        .into_iter()
        .map(|(node_id, held)| (node_id, (held.age_ms, held.received_at_ms)))
        .collect();
    heartbeat_reach(net.map(|net| net.node_id()), heard)(node_id)
}

/// This node's record of the set, if it holds one.
pub async fn read_view(
    context: &Arc<DriverContext>,
    set_id: Ulid,
) -> Result<Option<DistributionView>, StatusError> {
    let read = StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: set_id.to_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .map(|value| DistributionView::from_bytes(value.as_ref()))
            .transpose()?),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError("unexpected storage answer".to_string()).into()),
    }
}

/// The status of a set this node holds the record of.
pub async fn read_status(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    set_id: Ulid,
) -> Result<Option<DistributionStatus>, StatusError> {
    let Some(view) = read_view(context, set_id).await? else {
        return Ok(None);
    };
    let net = context.net_handle.as_ref();
    let heard = held_heartbeats(net)
        .into_iter()
        .map(|(node_id, held)| (node_id, (held.age_ms, held.received_at_ms)))
        .collect();
    let reach = heartbeat_reach(net.map(|net| net.node_id()), heard);
    // A publisher removed from the realm accepts no change, reachable or not.
    let publisher = view.snapshot().root.publisher;
    let eligible = load_realm_config(context, realm_id)
        .await
        .and_then(|config| config.sync_eligible_nodes().ok())
        .is_some_and(|nodes| nodes.contains(&publisher));
    let mut status = summarize(realm_id, &view, reach, eligible);
    let read = StorageEffect::Read {
        key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
        key: LocalFence::key(set_id).into(),
        txn_id: None,
    };
    if let Event::Storage(StorageEvent::ReadResult { value, .. }) =
        context.storage_handle.send_storage_effect(read).await
    {
        let fence = LocalFence::decode(value.as_deref())?;
        status.lifecycle.fenced_ranges = fence.ranges.iter().map(|f| f.range_id).collect();
        status.lifecycle.publications_closed = !fence.receiving();
    }
    Ok(Some(status))
}

#[cfg(test)]
#[path = "status_tests.rs"]
mod tests;
