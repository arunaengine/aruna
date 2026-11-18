//! Builds the status of a distributed bucket from this node's record and heartbeats: members,
//! ranges, a degraded range per unreachable coordinator, and what this node still owes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::heartbeat::MAX_HEARTBEAT_AGE;
use aruna_core::keyspaces::{DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, value};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::distribution::bootstrap::{BootstrapCursor, bootstrap_key};
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::snapshot::RangeFill;
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionRevision, DistributionSettings, DistributionSnapshot,
    DistributionView, MemberState, MetadataCoverage, SetPhase, TransferRole,
};
use aruna_core::types::{GroupId, Value};
use thiserror::Error;
use ulid::Ulid;

use super::here::Heard;
use crate::distribution::feed_walks::{FeedError, IndexProgress, RangeProgressOperation};
use crate::distribution::inventory::{StagedCall, outside_answer};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::replication::bootstrap::{BootstrapError, ReadCursorOperation};
use crate::replication::members::{CopyLag, CopyLagOperation, LagMap, RecordError};

#[derive(Debug, Error, PartialEq)]
pub enum StatusError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

impl From<IoError> for StatusError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
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
    /// This node's search index of the range, while it holds the range and the set keeps one.
    pub search_index: Option<IndexProgress>,
    /// What this node, as the range's coordinator, still owes each member it publishes to.
    pub copies: Vec<CopyStatus>,
    /// New copies of the range that are still filling.
    pub fills: Vec<FillStatus>,
}

/// A new copy of a range while its coordinator fills it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FillStatus {
    pub fill_id: Ulid,
    pub member_id: Ulid,
    /// How far the walk got; only the range's coordinator knows it.
    pub progress: Option<FillProgress>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FillProgress {
    /// Versions and delete markers queued for the copy so far, and the bytes of those versions.
    pub versions: u64,
    pub bytes: u64,
    /// The walk reached the end of the range; the last page may still be delivered.
    pub walked: bool,
}

/// The deliveries a coordinator still owes one member of a range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyStatus {
    pub member_id: Ulid,
    /// The member keeps the bytes; otherwise it receives records only.
    pub bytes: bool,
    pub lag: CopyLag,
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
    /// The current settings in full, as a settings change replaces them.
    pub settings: DistributionSettings,
    pub lifecycle: LifecycleStatus,
    /// The copy queues of the ranges this node coordinates were read to their end, so the
    /// pending counts of `copies` are exact rather than lower bounds.
    pub copies_counted: bool,
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
                search_index: None,
                copies: Vec::new(),
                fills: (snapshot.lifecycle.fills.iter())
                    .filter(|fill| fill.range_id == range.range_id)
                    .map(|fill| FillStatus {
                        fill_id: fill.fill_id,
                        member_id: fill.member_id,
                        progress: None,
                    })
                    .collect(),
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
        settings: snapshot.config.settings.clone(),
        lifecycle: lifecycle(snapshot),
        copies_counted: false,
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

fn record_read(set_id: Ulid) -> IoCall {
    read_row(DISTRIBUTION_KEYSPACE, set_id.to_bytes().to_vec())
}

fn decode_view(stored: Option<Value>) -> Result<Option<DistributionView>, StatusError> {
    let view = stored.map(|value| DistributionView::from_bytes(value.as_ref()));
    Ok(view.transpose()?)
}

/// This node's record of the set, if it holds one.
#[derive(Debug, PartialEq)]
pub struct ReadViewOperation {
    pub set_id: Ulid,
}

impl Operation for ReadViewOperation {
    type Output = Option<DistributionView>;
    type Error = StatusError;

    fn first(&mut self) -> Result<Next<Option<DistributionView>>, StatusError> {
        Ok(Next::Call(record_read(self.set_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<DistributionView>>, StatusError> {
        Ok(Next::Done(decode_view(value(result)?)?))
    }
}

/// What the pending call of a status read answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StatusStep {
    View,
    Config,
    Fence,
}

/// The status of a set this node holds the record of.
#[derive(Debug, PartialEq)]
pub struct ReadStatusOperation {
    realm_id: RealmId,
    set_id: Ulid,
    heard: Heard,
    step: StatusStep,
    view: Option<DistributionView>,
    status: Option<DistributionStatus>,
}

impl ReadStatusOperation {
    pub(super) fn new(realm_id: RealmId, set_id: Ulid, heard: Heard) -> Self {
        Self {
            realm_id,
            set_id,
            heard,
            step: StatusStep::View,
            view: None,
            status: None,
        }
    }

    /// Summarizes the record; a publisher removed from the realm accepts no change, reachable
    /// or not, and a config this node cannot read counts as removing it.
    fn summarized(&mut self, config: Option<RealmConfigDocument>) -> Option<IoCall> {
        let view = self.view.take()?;
        let publisher = view.snapshot().root.publisher;
        let eligible = config
            .and_then(|config| config.sync_eligible_nodes().ok())
            .is_some_and(|nodes| nodes.contains(&publisher));
        let heard = self.heard.clone();
        let reach = heartbeat_reach(heard.local, heard.nodes);
        self.status = Some(summarize(self.realm_id, &view, reach, eligible));
        let key = LocalFence::key(self.set_id);
        let read = read_row(DISTRIBUTION_FENCE_KEYSPACE, key);
        Some(StagedCall::Outside(read).into_call())
    }
}

impl Operation for ReadStatusOperation {
    type Output = Option<DistributionStatus>;
    type Error = StatusError;

    fn first(&mut self) -> Result<Next<Option<DistributionStatus>>, StatusError> {
        Ok(Next::Call(record_read(self.set_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<DistributionStatus>>, StatusError> {
        let unfinished = || StorageError::ReadError("the status was not read".to_string());
        match self.step {
            StatusStep::View => {
                let Some(view) = decode_view(value(result)?)? else {
                    return Ok(Next::Done(None));
                };
                self.view = Some(view);
                self.step = StatusStep::Config;
                let config = GetConfigOperation::new(self.realm_id);
                Ok(Next::Call(IoCall::child(config)))
            }
            StatusStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.step = StatusStep::Fence;
                let read = self.summarized(config.ok()).ok_or_else(unfinished)?;
                Ok(Next::Call(read))
            }
            StatusStep::Fence => {
                let mut status = self.status.take().ok_or_else(unfinished)?;
                // An unreadable fence row leaves the status without this node's fences.
                if let Ok(stored) = outside_answer(result).and_then(|read| read.value()) {
                    let fence = LocalFence::decode(stored.as_deref())?;
                    status.lifecycle.fenced_ranges =
                        fence.ranges.iter().map(|f| f.range_id).collect();
                    status.lifecycle.publications_closed = !fence.receiving();
                }
                Ok(Next::Done(Some(status)))
            }
        }
    }
}

/// What the pending call of [`AddProgressOperation`] answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgressStep {
    Index,
    View,
    Lag,
    Cursor,
}

/// One filling copy whose walk position is being read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FillSlot {
    range: usize,
    fill: usize,
}

/// Adds this node's search index progress and, for the ranges it coordinates, what it still
/// owes each member. Both are hints, so a failed read leaves them out.
#[derive(Debug, PartialEq)]
pub struct AddProgressOperation {
    status: DistributionStatus,
    local: Option<NodeId>,
    has_indexes: bool,
    step: ProgressStep,
    fills: VecDeque<FillSlot>,
    current: Option<FillSlot>,
    set_snapshot: Option<(DistributionSnapshot, Ulid)>,
}

impl AddProgressOperation {
    /// `has_indexes` tells whether this node runs range indexes at all.
    pub(super) fn new(
        status: DistributionStatus,
        local: Option<NodeId>,
        has_indexes: bool,
    ) -> Self {
        Self {
            status,
            local,
            has_indexes,
            step: ProgressStep::Index,
            fills: VecDeque::new(),
            current: None,
            set_snapshot: None,
        }
    }

    fn read_view(&mut self) -> IoCall {
        self.step = ProgressStep::View;
        IoCall::child(ReadViewOperation {
            set_id: self.status.set_id,
        })
    }

    fn finished(&mut self) -> Next<DistributionStatus> {
        Next::Done(self.status.clone())
    }

    fn view_read(&mut self, stored: Option<DistributionView>) -> Next<DistributionStatus> {
        let Some(view) = stored else {
            return self.finished();
        };
        let snapshot = view.head.snapshot;
        let member = (snapshot.config.members.iter()).find(|m| Some(m.node_id) == self.local);
        let Some(member_id) = member.map(|member| member.member_id) else {
            return self.finished();
        };
        let lag = CopyLagOperation::new(snapshot.clone(), member_id);
        self.set_snapshot = Some((snapshot, member_id));
        self.step = ProgressStep::Lag;
        Next::Call(IoCall::child(lag))
    }

    fn lag_read(&mut self, (lag, counted): (LagMap, bool)) -> Next<DistributionStatus> {
        let Some((snapshot, member_id)) = self.set_snapshot.take() else {
            return self.finished();
        };
        self.status.copies_counted = counted;
        for (index, range) in self.status.ranges.iter_mut().enumerate() {
            let Some(config) =
                (snapshot.config.ranges.iter()).find(|r| r.range_id == range.range_id)
            else {
                continue;
            };
            range.copies = (snapshot.range_deliveries(member_id, config).into_iter())
                .map(|delivery| CopyStatus {
                    member_id: delivery.member_id,
                    bytes: delivery.bytes,
                    lag: (lag.get(&(range.range_id, delivery.member_id)).copied())
                        .unwrap_or_default(),
                })
                .collect();
            if range.coordinator == member_id {
                self.fills
                    .extend((0..range.fills.len()).map(|fill| FillSlot { range: index, fill }));
            }
        }
        self.next_fill()
    }

    fn next_fill(&mut self) -> Next<DistributionStatus> {
        let Some(slot) = self.fills.pop_front() else {
            return self.finished();
        };
        let range = &self.status.ranges[slot.range];
        let fill = &range.fills[slot.fill];
        let key = bootstrap_key(self.status.set_id, fill.member_id, range.range_id);
        self.current = Some(slot);
        self.step = ProgressStep::Cursor;
        Next::Call(IoCall::child(ReadCursorOperation::new(key)))
    }

    /// The walk of one filling copy, from its cursor.
    fn cursor_read(&mut self, cursor: Option<BootstrapCursor>) {
        let Some(slot) = self.current.take() else {
            return;
        };
        let range = &mut self.status.ranges[slot.range];
        let range_id = range.range_id;
        let fill = &mut range.fills[slot.fill];
        let intent = RangeFill {
            fill_id: fill.fill_id,
            range_id,
            member_id: fill.member_id,
            admitted: true,
        }
        .intent();
        fill.progress = Some(match cursor.filter(|cursor| cursor.intent == intent) {
            Some(cursor) => FillProgress {
                versions: cursor.versions,
                bytes: cursor.bytes,
                walked: cursor.walked,
            },
            None => FillProgress::default(),
        });
    }
}

impl Operation for AddProgressOperation {
    type Output = DistributionStatus;
    type Error = StatusError;

    fn first(&mut self) -> Result<Next<DistributionStatus>, StatusError> {
        if !self.has_indexes {
            return Ok(Next::Call(self.read_view()));
        }
        let progress = RangeProgressOperation::new(&self.status.bucket);
        Ok(Next::Call(IoCall::child(progress)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DistributionStatus>, StatusError> {
        match self.step {
            ProgressStep::Index => {
                let progress = result.child::<BTreeMap<Ulid, IndexProgress>, FeedError>()?;
                let progress = progress.unwrap_or_default();
                for range in &mut self.status.ranges {
                    range.search_index = progress.get(&range.range_id).copied();
                }
                Ok(Next::Call(self.read_view()))
            }
            ProgressStep::View => {
                let view = result.child::<Option<DistributionView>, StatusError>()?;
                Ok(match view {
                    Ok(view) => self.view_read(view),
                    Err(_) => self.finished(),
                })
            }
            ProgressStep::Lag => Ok(match result.child::<(LagMap, bool), RecordError>()? {
                Ok(lag) => self.lag_read(lag),
                Err(_) => self.finished(),
            }),
            ProgressStep::Cursor => {
                let cursor = result.child::<Option<BootstrapCursor>, BootstrapError>()?;
                self.cursor_read(cursor.ok().flatten());
                Ok(self.next_fill())
            }
        }
    }
}

#[cfg(test)]
#[path = "status_tests.rs"]
mod tests;
