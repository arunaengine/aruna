//! Makes one decided change to a plugin task's work row and task row in a transaction that reads
//! them again, so a change never applies to rows that moved on since it was decided.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{FAMILY_RECORD_KEYSPACE, PLUGIN_WORK_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::family::{JobFamilyId, SubmissionId};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::record_keys::submission_prefix;
use ulid::Ulid;

use super::super::rows::{StoreError, decode, delete_call, encode, read_call, row, values};
use aruna_core::install_work::{
    FinalState, Found, SendBoundary, Summary, WorkLookup, WorkRow, WorkState, index_key,
};
use aruna_core::install_work::{TaskRow, task_key, task_place};
use aruna_core::job_request::SubmissionRequest;

/// What one send of a task proved.
#[derive(Clone, Debug, PartialEq)]
pub enum SendOutcome {
    /// A holder admitted it under `alias`; the row runs that family.
    Dispatch { alias: JobId, family: JobFamilyId },
    /// Proven not admitted: the work settles failed and frees its task slot.
    Fail(String),
    /// Uncertain: the row stays reserved until a claim is adopted or absence is proven.
    Keep,
}

/// One change to a task's rows.
#[derive(Clone, Debug, PartialEq)]
pub enum TaskChange {
    /// Marks the next send before it happens: its attempt in the work row, its targets, its
    /// deadline and the stored request in the task row.
    Mark {
        targets: Vec<NodeId>,
        admit_by: u64,
        request: Box<SubmissionRequest>,
        digest: [u8; 32],
    },
    /// What a send proved, applied to a reserved row.
    Record(SendOutcome),
    /// A removed install's settled row goes with its keys.
    Drop,
    /// A reservation that was never sent goes, if it is still unsent.
    DropUnsent,
    /// A finished family settles a dispatched row, with the report the task row keeps.
    Settle(Box<Summary>),
    /// Proven non-admission ends a reserved row: failed while the install is active, else
    /// deleted. A record of the submission here is a claim or a launch and keeps the row; the
    /// launch reads the row in its own transaction, so one of the two aborts.
    End {
        active: bool,
        submission: SubmissionId,
    },
}

/// What a change did: whether the task counts as settled by it, and for a mark the attempt it
/// marked and whether an earlier answer was lost.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Changed {
    pub settled: bool,
    pub marked: Option<(u32, bool)>,
}

impl Changed {
    fn settled(settled: bool) -> Self {
        Self {
            settled,
            marked: None,
        }
    }
}

/// What a send's outcome changes in a reserved work row and its task row.
pub fn apply_outcome((row, task): (&mut WorkRow, &mut TaskRow), outcome: SendOutcome, now_ms: u64) {
    match outcome {
        SendOutcome::Dispatch { alias, family } => {
            (row.state, row.job) = (WorkState::Dispatched, Some(alias));
            task.family = Some(family);
        }
        SendOutcome::Fail(reason) => {
            let mut summary = Summary::of(FinalState::Failed);
            (summary.target, summary.message) = (row.target.clone(), Some(reason));
            (row.state, row.summary) = (WorkState::Settled, Some(summary));
            row.settled_ms = Some(now_ms);
        }
        SendOutcome::Keep => task.uncertain = true,
    }
}

#[derive(Debug, PartialEq)]
enum Phase {
    Records,
    Lookup(WorkLookup),
    Task(Vec<u8>, Box<WorkRow>),
    Written(Changed),
}

/// Applies one [`TaskChange`] to work `work_id` of an install in one write transaction.
#[derive(Debug, PartialEq)]
pub struct ChangeTask {
    install_id: Ulid,
    work_id: Ulid,
    change: TaskChange,
    now_ms: u64,
    phase: Phase,
}

type ChangeNext = Result<Next<Changed>, StoreError>;

impl ChangeTask {
    pub fn new((install_id, work_id): (Ulid, Ulid), change: TaskChange, now_ms: u64) -> Self {
        Self {
            install_id,
            work_id,
            change,
            now_ms,
            phase: Phase::Lookup(WorkLookup::new(install_id, work_id)),
        }
    }

    fn lookup(&mut self) -> ChangeNext {
        let lookup = WorkLookup::new(self.install_id, self.work_id);
        let ask = lookup.ask();
        self.phase = Phase::Lookup(lookup);
        Ok(Next::Call(ask))
    }

    fn put(
        &mut self,
        (key, work): (Vec<u8>, &WorkRow),
        task: &TaskRow,
        changed: Changed,
    ) -> ChangeNext {
        let (space, task_key) = task_place(self.install_id, self.work_id);
        let rows = vec![row_of(key, work)?, row(space, task_key, encode(task)?)];
        self.phase = Phase::Written(changed);
        Ok(Next::Call(IoCall::Write(rows)))
    }

    fn delete(&mut self, key: Vec<u8>, changed: Changed) -> ChangeNext {
        let keys = vec![
            (
                PLUGIN_WORK_KEYSPACE,
                index_key(self.install_id, self.work_id),
            ),
            (PLUGIN_WORK_KEYSPACE, key),
            (
                PLUGIN_WORK_KEYSPACE,
                task_key(self.install_id, self.work_id),
            ),
        ];
        self.phase = Phase::Written(changed);
        Ok(Next::Call(delete_call(keys)))
    }

    /// Applies the change to the rows read in the transaction.
    fn apply(&mut self, key: Vec<u8>, mut row: WorkRow, mut task: TaskRow) -> ChangeNext {
        let unchanged = |settled| Ok(Next::Done(Changed::settled(settled)));
        let reserved = row.state == WorkState::Reserved;
        match self.change.clone() {
            TaskChange::Drop => self.delete(key, Changed::settled(false)),
            TaskChange::DropUnsent if reserved && row.boundary.is_none() => {
                self.delete(key, Changed::settled(true))
            }
            TaskChange::DropUnsent => unchanged(true),
            TaskChange::Settle(mut summary) if row.state == WorkState::Dispatched => {
                summary.report = task.report.clone();
                (row.state, row.summary) = (WorkState::Settled, Some(*summary));
                row.settled_ms = Some(self.now_ms);
                self.put((key, &row), &task, Changed::settled(true))
            }
            TaskChange::Settle(_) => unchanged(true),
            TaskChange::Mark { .. } | TaskChange::Record(_) | TaskChange::End { .. }
                if !reserved =>
            {
                unchanged(false)
            }
            TaskChange::Mark {
                targets,
                admit_by,
                request,
                digest,
            } => {
                let attempt = match &row.boundary {
                    Some(SendBoundary::Sending { attempt }) => attempt + 1,
                    _ => 1,
                };
                row.boundary = Some(SendBoundary::Sending { attempt });
                if task.request.is_none() {
                    task.admit_by = admit_by;
                    (task.request, task.digest) = (Some(*request), Some(digest));
                }
                // A second send means the first one's outcome was never recorded.
                task.uncertain |= attempt > 1;
                for target in targets {
                    if !task.targets.contains(&target) {
                        task.targets.push(target);
                    }
                }
                let marked = Some((attempt, task.uncertain));
                let changed = Changed {
                    settled: false,
                    marked,
                };
                self.put((key, &row), &task, changed)
            }
            TaskChange::Record(outcome) => {
                let settled = matches!(outcome, SendOutcome::Fail(_));
                apply_outcome((&mut row, &mut task), outcome, self.now_ms);
                self.put((key, &row), &task, Changed::settled(settled))
            }
            TaskChange::End { active: false, .. } => self.delete(key, Changed::settled(true)),
            TaskChange::End { .. } => {
                let reason = "the task was not admitted before its deadline".to_string();
                apply_outcome(
                    (&mut row, &mut task),
                    SendOutcome::Fail(reason),
                    self.now_ms,
                );
                self.put((key, &row), &task, Changed::settled(true))
            }
        }
    }
}

fn row_of(key: Vec<u8>, work: &WorkRow) -> Result<super::super::rows::Row, StoreError> {
    Ok(row(PLUGIN_WORK_KEYSPACE, key, encode(work)?))
}

impl Operation for ChangeTask {
    type Output = Changed;
    type Error = StoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ChangeNext {
        let TaskChange::End { submission, .. } = &self.change else {
            return self.lookup();
        };
        self.phase = Phase::Records;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: submission_prefix(*submission),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> ChangeNext {
        match std::mem::replace(&mut self.phase, Phase::Records) {
            Phase::Records => match result {
                // A claim or a launch of the submission here keeps the row.
                IoResult::Page { rows, .. } if !rows.is_empty() => {
                    Ok(Next::Done(Changed::settled(false)))
                }
                IoResult::Page { .. } => self.lookup(),
                other => Err(super::super::rows::unexpected(other)),
            },
            Phase::Lookup(mut lookup) => match lookup.take(result)? {
                Found::Ask(ask) => {
                    self.phase = Phase::Lookup(lookup);
                    Ok(Next::Call(ask))
                }
                Found::Row(None) => {
                    let settled = self.change == TaskChange::DropUnsent;
                    Ok(Next::Done(Changed::settled(settled)))
                }
                Found::Row(Some((key, row))) => {
                    let read = read_call(vec![task_place(self.install_id, self.work_id)]);
                    self.phase = Phase::Task(key, Box::new(row));
                    Ok(Next::Call(read))
                }
            },
            Phase::Task(key, row) => {
                let [task] = values::<1>(result)?;
                let task = task.map(|bytes| decode::<TaskRow>(&bytes)).transpose()?;
                self.apply(key, *row, task.unwrap_or_default())
            }
            Phase::Written(changed) => {
                super::super::rows::done(result)?;
                Ok(Next::Done(changed))
            }
        }
    }
}

#[cfg(test)]
#[path = "change_tests.rs"]
mod tests;
