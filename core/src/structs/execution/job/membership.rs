//! Derives which executions and launches of a family are not run because their node left.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use ulid::Ulid;

use super::family::{JobFamilyRecord, JobRecordEnvelope, PhysicalExecutionState};
use crate::NodeId;
use crate::admin_documents::{AdminDocumentClock, NodeInterval};

/// What this node's applied realm config says about a node a record cites.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Membership {
    /// At least one candidate interval is still open.
    Open,
    /// Every candidate interval has a removal: the node left the realm.
    Left,
    /// The cited clock shows no interval of the node, so the record names no membership.
    Absent,
    /// This node's clock does not cover the cited clock yet.
    Unknown,
}

/// Membership of `node` for a record that cites `cited`, judged with this node's applied
/// `clock` and `intervals`. The candidates are all intervals of the node open at the cited clock.
pub fn membership(
    node: NodeId,
    cited: &AdminDocumentClock,
    intervals: &[NodeInterval],
    clock: &AdminDocumentClock,
) -> Membership {
    if !clock.covers(cited) {
        return Membership::Unknown;
    }
    let mut candidates = (intervals.iter())
        .filter(|interval| interval.node_id == node && interval.open_at(cited))
        .peekable();
    if candidates.peek().is_none() {
        return Membership::Absent;
    }
    match candidates.all(|interval| !interval.removed.is_empty()) {
        true => Membership::Left,
        false => Membership::Open,
    }
}

/// The local executions whose node left the realm: every interval of `local` open at the clock
/// a reservation cites has a removal. Their work is cancelled and never resubmitted.
pub fn left_executions<'a, T>(
    local: NodeId,
    reservations: impl IntoIterator<Item = (T, &'a AdminDocumentClock)>,
    intervals: &[NodeInterval],
    clock: &AdminDocumentClock,
) -> Vec<T> {
    (reservations.into_iter())
        .filter(|(_, cited)| membership(local, cited, intervals, clock) == Membership::Left)
        .map(|(execution, _)| execution)
        .collect()
}

/// Executions and launches of one family whose node left the realm, as this node sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NotRun {
    pub executions: BTreeSet<Ulid>,
    pub launches: BTreeSet<Ulid>,
    /// No success or signed failure, at least one item not run, and every open item not run.
    pub family: bool,
}

/// Marks each execution without a stop record whose executor left, and each launch without a
/// receipt whose target left. Nothing is published: every node derives it from its own view.
pub fn not_run(
    records: &[JobRecordEnvelope],
    intervals: &[NodeInterval],
    clock: &AdminDocumentClock,
) -> NotRun {
    let mut receipts = BTreeMap::new();
    let mut launches = BTreeMap::new();
    let mut stopped = BTreeSet::new();
    let mut received = BTreeSet::new();
    let mut decided = false;
    for envelope in records {
        match &envelope.record {
            JobFamilyRecord::Receipt(receipt) => {
                received.insert(receipt.launch_id);
                receipts.insert(receipt.execution_id, receipt.as_ref());
            }
            JobFamilyRecord::Launch(launch) => {
                launches.insert(launch.launch_id, launch.as_ref());
            }
            JobFamilyRecord::Stop(stop) => {
                stopped.insert(stop.execution_id);
            }
            JobFamilyRecord::Update(update) => {
                decided |= matches!(
                    update.state,
                    PhysicalExecutionState::Succeeded | PhysicalExecutionState::Failed
                );
            }
            _ => {}
        }
    }
    let left = |node, cited| membership(node, cited, intervals, clock) == Membership::Left;
    let mut result = NotRun::default();
    let mut open = 0usize;
    for (execution_id, receipt) in receipts {
        if stopped.contains(&execution_id) {
            continue;
        }
        open += 1;
        if left(receipt.executor_node_id, &receipt.config_clock) {
            result.executions.insert(execution_id);
        }
    }
    for (launch_id, launch) in launches {
        if received.contains(&launch_id) {
            continue;
        }
        open += 1;
        if left(launch.target.node_id, &launch.config_clock) {
            result.launches.insert(launch_id);
        }
    }
    let closed = result.executions.len() + result.launches.len();
    result.family = !decided && closed > 0 && closed == open;
    result
}
