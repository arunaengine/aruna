//! Decides from one node's own view whether a job family may be pruned, and when to look again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::family::{CLOCK_MARGIN_MS, JobFamilyRecord, JobRecordEnvelope, LogicalJobState};
use super::identity::{JobId, RETENTION_MS};
use super::membership::NotRun;
use crate::{NodeId, UserId};

/// How long tombstones and pending rows are kept; it is also the bound of spec freshness.
pub const TOMBSTONE_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// What stays of a pruned family on one node until it expires.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FamilyTombstone {
    /// When this node pruned the family.
    pub pruned_at_ms: u64,
    /// The largest record time of the pruned family.
    pub last_record_ms: u64,
    /// Every body page is deleted; until then every record of the family is refused for now.
    pub finished: bool,
    /// The submitter of the pruned spec: only they learn that the job was pruned.
    pub created_by: Option<UserId>,
    /// The job ids of the family's claims, whose alias rows go when the tombstone expires.
    pub aliases: Vec<JobId>,
}

impl FamilyTombstone {
    /// When a finished tombstone expires: a replayed spec is stale by then without it.
    pub fn expires_at_ms(&self) -> u64 {
        (self.pruned_at_ms.max(self.last_record_ms))
            .saturating_add(TOMBSTONE_RETENTION_MS)
            .saturating_add(CLOCK_MARGIN_MS)
    }
}

/// What one node knows about one family when it decides.
#[derive(Clone, Copy, Debug)]
pub struct PruneInput<'a> {
    pub records: &'a [JobRecordEnvelope],
    /// The reduced logical state, without the read-time not-run overlay.
    pub state: LogicalJobState,
    /// The reduction read fewer records than the family holds.
    pub truncated: bool,
    /// Launch ids this node holds a closure row for.
    pub closed: &'a BTreeSet<Ulid>,
    pub not_run: &'a NotRun,
    /// A local job record or a reservation row of the family exists here.
    pub local_work: bool,
    /// This node's outbox still holds an entry of the family.
    pub undelivered: bool,
    pub now_ms: u64,
}

/// Why a family stays, in the order the rules are checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldReason {
    Truncated,
    /// No success, signed failure, cancel or not-run outcome yet.
    Undecided,
    /// An execution of this executor has no stop record and is not run.
    Running(NodeId),
    /// A launch to this target has no receipt, no closure and is not run.
    LaunchOpen(NodeId),
    LocalWork,
    Undelivered,
    Retention,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruneVerdict {
    Prunable,
    /// `recheck_ms` is the earliest time the reason can change by itself; `None` when another
    /// event must change it first.
    Held {
        reason: HoldReason,
        recheck_ms: Option<u64>,
    },
}

/// Rules 1 to 6 and 8 of the family prune plan, decided from `input` alone. Each node decides
/// from its own copy and clock; there is no vote and no lease.
pub fn prune_verdict(input: &PruneInput<'_>) -> PruneVerdict {
    let held = |reason, recheck_ms| PruneVerdict::Held { reason, recheck_ms };
    if input.truncated {
        return held(HoldReason::Truncated, None);
    }
    let settled = matches!(
        input.state,
        LogicalJobState::Succeeded | LogicalJobState::Failed | LogicalJobState::Cancelled
    );
    if !settled && !input.not_run.family {
        return held(HoldReason::Undecided, None);
    }
    let mut stopped = BTreeSet::new();
    let mut received = BTreeSet::new();
    for envelope in input.records {
        match &envelope.record {
            JobFamilyRecord::Stop(stop) => {
                stopped.insert(stop.execution_id);
            }
            JobFamilyRecord::Receipt(receipt) => {
                received.insert(receipt.launch_id);
            }
            _ => {}
        }
    }
    for envelope in input.records {
        match &envelope.record {
            JobFamilyRecord::Receipt(receipt)
                if !stopped.contains(&receipt.execution_id)
                    && !input.not_run.executions.contains(&receipt.execution_id) =>
            {
                return held(HoldReason::Running(receipt.executor_node_id), None);
            }
            JobFamilyRecord::Launch(launch)
                if !received.contains(&launch.launch_id)
                    && !input.closed.contains(&launch.launch_id)
                    && !input.not_run.launches.contains(&launch.launch_id) =>
            {
                let due = launch.closure_due();
                let recheck = (due > input.now_ms).then_some(due);
                return held(HoldReason::LaunchOpen(launch.target.node_id), recheck);
            }
            _ => {}
        }
    }
    if input.local_work {
        return held(HoldReason::LocalWork, None);
    }
    if input.undelivered {
        return held(HoldReason::Undelivered, None);
    }
    let retention = (input.records.iter())
        .find_map(|envelope| match &envelope.record {
            JobFamilyRecord::Spec(spec) => Some(spec.retention_ms),
            _ => None,
        })
        .unwrap_or(RETENTION_MS);
    let expires = last_record_ms(input.records).saturating_add(retention);
    match expires > input.now_ms {
        true => held(HoldReason::Retention, Some(expires)),
        false => PruneVerdict::Prunable,
    }
}

/// The time a record states about itself. A budget states none.
pub fn record_time(record: &JobFamilyRecord) -> Option<u64> {
    match record {
        JobFamilyRecord::Spec(spec) => Some(spec.created_at_ms),
        JobFamilyRecord::Claim(claim) => Some(claim.accepted_at_ms),
        JobFamilyRecord::Budget(_) => None,
        JobFamilyRecord::Launch(launch) => Some(launch.created_at_ms),
        JobFamilyRecord::Receipt(receipt) => Some(receipt.accepted_at_ms),
        JobFamilyRecord::Update(update) => Some(update.observed_at_ms),
        JobFamilyRecord::Output(output) => Some(output.committed_at_ms),
        JobFamilyRecord::Cancel(cancel) => Some(cancel.requested_at_ms),
        JobFamilyRecord::Stop(stop) => Some(stop.stopped_at_ms),
    }
}

/// The largest record time of a family; a pruned family's tombstone keeps it.
pub fn last_record_ms(records: &[JobRecordEnvelope]) -> u64 {
    (records.iter())
        .filter_map(|envelope| record_time(&envelope.record))
        .max()
        .unwrap_or(0)
}

/// Whether a pending row first seen at `first_seen_ms` is past the horizon at `now_ms`.
pub fn pending_expired(first_seen_ms: u64, now_ms: u64) -> bool {
    first_seen_ms.saturating_add(TOMBSTONE_RETENTION_MS) <= now_ms
}

/// Whether a spec may start a family on a node that holds no accepted spec of it yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    /// Older than the horizon.
    Stale,
    /// Not newer than the last record of a pruned incarnation plus the clock margin.
    Pruned,
}

/// Decides spec freshness at `now_ms`; `pruned_last_ms` is C of a finished tombstone, if any.
pub fn spec_freshness(created_at_ms: u64, now_ms: u64, pruned_last_ms: Option<u64>) -> Freshness {
    if pruned_last_ms.is_some_and(|last| created_at_ms <= last.saturating_add(CLOCK_MARGIN_MS)) {
        return Freshness::Pruned;
    }
    match created_at_ms.saturating_add(TOMBSTONE_RETENTION_MS) <= now_ms {
        true => Freshness::Stale,
        false => Freshness::Fresh,
    }
}
