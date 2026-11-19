//! Settles one plugin task row on its receiver: settles a finished family, adopts a claim, drops
//! or ends the row, or proves absence; resends and cancels go back to the pass that asked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{JobRecordFrame, MAX_RECORD_HOLDERS};
use aruna_core::jobs::ClosureAnswer;
use aruna_core::keyspaces::FAMILY_RECORD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::plugin_installs::InstallState;
use aruna_core::structs::execution::job::record_keys::submission_prefix;
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::structs::execution::job::{
    CLOCK_MARGIN_MS, JobFamilyId, JobFamilyRecord, JobId, JobProjection, JobRecordEnvelope,
    LogicalJobState, SubmissionId,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::{Key, Value};
use tracing::warn;
use ulid::Ulid;

use super::rows::{StoreError, decode, read_call, unexpected, values};
use super::task_change::{ChangeTask, Changed, SendOutcome, TaskChange};
use super::task_row::{ADMIT_WINDOW_MS, TaskRow, task_place};
use super::work::{FinalState, Summary, WorkRow, WorkState};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::closure::{ASK_DEADLINE, AskClosureOperation};
use crate::jobs::lifecycle::fetch::{FetchFamilyOperation, FetchedFamily};
use crate::jobs::lifecycle::ids::{SubmissionScope, submission_of};
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::append::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin,
};
use crate::jobs::records::project::{
    FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation, ProjectedFamily,
};

/// Family records read per page while looking for a claim.
const CLAIM_PAGE: usize = 256;

/// What the pass runs after a row's decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Followup {
    Nothing,
    /// An active install's uncertain task is sent again, before its deadline.
    Resend,
    /// A removed install's dispatched task is cancelled at the holders.
    Cancel {
        alias: JobId,
        family: JobFamilyId,
    },
}

/// The decision for one row: whether it counts as settled, and what runs next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settled {
    pub changed: bool,
    pub followup: Followup,
}

/// What the answers of every node a submission went to prove after its deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Absence {
    /// A node holds a claim: the family and its alias are adopted.
    Claimed { family: JobFamilyId, job_id: JobId },
    /// Every target committed "absent", and the receiver holds nothing itself.
    Proven,
    /// Some target did not decide yet; ask again later.
    Waiting,
}

/// Decides absence from the closure answers of `targets` and the receiver's own claim; a
/// target in `departed` left the realm and counts as absent.
pub fn absence(
    targets: &[NodeId],
    answers: &[(NodeId, ClosureAnswer)],
    local: Option<(JobFamilyId, JobId)>,
    departed: &BTreeSet<NodeId>,
) -> Absence {
    if let Some((family, job_id)) = local {
        return Absence::Claimed { family, job_id };
    }
    let mut proven = 0;
    for target in targets {
        let answer =
            (answers.iter()).find_map(|(node, answer)| (node == target).then_some(*answer));
        match answer {
            Some(ClosureAnswer::Claimed { family, job_id }) => {
                return Absence::Claimed { family, job_id };
            }
            Some(ClosureAnswer::Absent) => proven += 1,
            // A node that left the realm never answers; the ending transaction's own check
            // and the receiver fence decide in its place.
            _ if departed.contains(target) => proven += 1,
            _ => {}
        }
    }
    match proven == targets.len() && !targets.is_empty() {
        true => Absence::Proven,
        false => Absence::Waiting,
    }
}

/// The summary a finished family settles its work with; `None` while it may still change.
/// An indeterminate family waits for terminal evidence or a cancel.
pub fn final_of(projection: &JobProjection) -> Option<Summary> {
    let state = match projection.state {
        LogicalJobState::Succeeded => FinalState::Succeeded,
        LogicalJobState::Failed => FinalState::Failed,
        LogicalJobState::Cancelled => FinalState::Cancelled,
        // Every open execution belongs to a node that left, so the outcome is unknown.
        LogicalJobState::NotRun => FinalState::Unknown,
        LogicalJobState::Queued | LogicalJobState::Running | LogicalJobState::Indeterminate => {
            return None;
        }
    };
    let canonical = (projection.canonical_execution_id).and_then(|id| {
        (projection.executions.iter()).find(|execution| execution.execution_id == id)
    });
    let result = canonical.and_then(|execution| execution.result.as_ref());
    let mut summary = Summary::of(state);
    summary.message = result
        .and_then(|result| result.message.as_ref())
        .map(|message| message.as_str().to_string());
    summary.exit_code = result.and_then(|result| result.exit_code);
    summary.outputs = projection.outputs.as_slice().to_vec();
    Some(summary)
}

/// The submission a task's family runs under.
pub fn task_submission(row: &WorkRow) -> SubmissionId {
    let scope = SubmissionScope::Keyed(format!("plugin-work/{}", row.work_id));
    submission_of(row.account, &scope)
}

/// The first claim among stored family records, with its family.
fn first_claim(rows: &[(Key, Value)]) -> Option<(JobFamilyId, JobId)> {
    rows.iter().find_map(|(_, value)| {
        let envelope = from_bytes::<JobRecordEnvelope>(value).ok()?;
        match &envelope.record {
            JobFamilyRecord::Claim(claim) => Some((envelope.family(), claim.job_id)),
            _ => None,
        }
    })
}

/// Where a pull continues once the fetched records are appended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum After {
    Project(JobId),
    Claims,
}

#[derive(Debug, PartialEq)]
enum SettlePhase {
    Task,
    Fetch(After),
    Append(VecDeque<JobRecordFrame>, RecordOrigin, After),
    Project,
    Close(NodeId, VecDeque<NodeId>),
    Claims,
    Change,
}

/// Decides and applies the settlement of one task row of `install_id`.
#[derive(Debug, PartialEq)]
pub struct SettleTask {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    install_id: Ulid,
    row: WorkRow,
    task: Option<TaskRow>,
    submission: SubmissionId,
    /// Closure answers of the targets, once the deadline passed.
    answers: Vec<(NodeId, ClosureAnswer)>,
    late: bool,
    now_ms: u64,
    phase: SettlePhase,
}

type SettleNext = Result<Next<Settled>, StoreError>;

impl SettleTask {
    pub fn new(
        config: Arc<RealmConfigDocument>,
        local: NodeId,
        (install_id, row): (Ulid, WorkRow),
    ) -> Self {
        Self {
            config,
            local,
            install_id,
            submission: task_submission(&row),
            row,
            task: None,
            answers: Vec::new(),
            late: false,
            now_ms: 0,
            phase: SettlePhase::Task,
        }
    }

    fn install(&self) -> Option<InstallState> {
        (self.config.installs.iter())
            .find(|install| install.install_id == self.install_id)
            .map(|install| install.state)
    }

    fn removed(&self) -> bool {
        !matches!(self.install(), Some(state) if state != InstallState::Removing)
    }

    fn done(&self, changed: bool, followup: Followup) -> SettleNext {
        Ok(Next::Done(Settled { changed, followup }))
    }

    /// The first decision, from the rows alone.
    fn decide(&mut self) -> SettleNext {
        let (row, removed, now_ms) = (&self.row, self.removed(), self.now_ms);
        match row.state {
            WorkState::Settled if removed => return self.change(TaskChange::Drop),
            WorkState::Settled => return self.done(false, Followup::Nothing),
            _ => {}
        }
        let Some(task) = self.task.clone() else {
            // Reserved but never marked: the call ended before its first send. A removed
            // install's row goes at once, any other once no call can still be running.
            let stale = now_ms > row.created_ms.saturating_add(ADMIT_WINDOW_MS);
            return match row.state == WorkState::Reserved && (removed || stale) {
                true => self.change(TaskChange::DropUnsent),
                false => self.done(false, Followup::Nothing),
            };
        };
        match (row.state, row.job, task.family) {
            // A removed install's running task keeps its obligation until a holder accepted.
            (WorkState::Dispatched, Some(alias), Some(family)) if removed && !row.cancel => {
                self.done(false, Followup::Cancel { alias, family })
            }
            (WorkState::Dispatched, Some(alias), Some(family)) => {
                self.pull(Some(family.request_digest), After::Project(alias))
            }
            (WorkState::Reserved, ..) if task.request.is_none() => {
                self.done(false, Followup::Nothing)
            }
            (WorkState::Reserved, ..) if now_ms <= task.admit_by => self.pull(None, After::Claims),
            (WorkState::Reserved, ..)
                if now_ms <= task.admit_by.saturating_add(CLOCK_MARGIN_MS) =>
            {
                self.done(false, Followup::Nothing)
            }
            (WorkState::Reserved, ..) => self.close(&task),
            _ => self.done(false, Followup::Nothing),
        }
    }

    /// Fetches the family's records from its holders, unless this node holds them.
    fn pull(&mut self, digest: Option<[u8; 32]>, after: After) -> SettleNext {
        let config = &self.config;
        let Ok(placement) = config.family_placement(self.submission) else {
            return self.continue_after(after);
        };
        let mut holders = resolve_shard_holders(config, &placement);
        if holders.contains(&self.local) {
            return self.continue_after(after);
        }
        holders.truncate(MAX_RECORD_HOLDERS);
        let family = (self.submission, digest);
        let fetch = FetchFamilyOperation::new(placement, holders, family, ASK_DEADLINE);
        self.phase = SettlePhase::Fetch(after);
        Ok(Next::Call(IoCall::child(fetch)))
    }

    /// Appends the next fetched record, or goes on once all are appended.
    fn append_next(
        &mut self,
        mut frames: VecDeque<JobRecordFrame>,
        origin: RecordOrigin,
        after: After,
    ) -> SettleNext {
        let Some(record) = frames.pop_front() else {
            return self.continue_after(after);
        };
        let append = AppendRecordOperation::new(AppendRecordConfig {
            realm_id: self.config.realm_id,
            local_node_id: self.local,
            record,
            local: None,
            origin,
            now_ms: self.now_ms,
        });
        self.phase = SettlePhase::Append(frames, origin, after);
        Ok(Next::Call(IoCall::child(append)))
    }

    fn continue_after(&mut self, after: After) -> SettleNext {
        match after {
            After::Project(alias) => {
                let project = ProjectFamilyOperation::new(ProjectFamilyConfig {
                    family: FamilyRef::Alias(alias),
                    now_ms: self.now_ms,
                    rebuild: false,
                });
                self.phase = SettlePhase::Project;
                Ok(Next::Call(IoCall::child(project)))
            }
            After::Claims => self.claims(None),
        }
    }

    /// Asks every target that is still a member whether it admitted the submission.
    fn close(&mut self, task: &TaskRow) -> SettleNext {
        self.late = true;
        let members = self.members();
        let asked = (task.targets.iter()).filter(|node| members.contains(*node));
        self.ask_next(asked.copied().collect())
    }

    fn ask_next(&mut self, mut asked: VecDeque<NodeId>) -> SettleNext {
        let (Some(node), Some(task)) = (asked.pop_front(), self.task.as_ref()) else {
            return self.claims(None);
        };
        let ask = AskClosureOperation::new(node, (self.submission, task.admit_by));
        self.phase = SettlePhase::Close(node, asked);
        Ok(Next::Call(IoCall::child(ask)))
    }

    fn members(&self) -> BTreeSet<NodeId> {
        (self.config.nodes.iter())
            .filter_map(|node| node.node_id.parse().ok())
            .collect()
    }

    /// Reads this node's records of the submission for its first claim, page by page.
    fn claims(&mut self, after: Option<Key>) -> SettleNext {
        self.phase = SettlePhase::Claims;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: submission_prefix(self.submission),
            after,
            limit: CLAIM_PAGE,
        }))
    }

    /// Decides from the claim found here and, after the deadline, the targets' answers.
    fn claimed(&mut self, found: Option<(JobFamilyId, JobId)>) -> SettleNext {
        if !self.late {
            return match (found, self.install()) {
                (Some((family, alias)), _) => self.adopt(alias, family),
                (None, Some(InstallState::Active)) => self.done(false, Followup::Resend),
                (None, _) => self.done(false, Followup::Nothing),
            };
        }
        let Some(task) = self.task.as_ref() else {
            return self.done(false, Followup::Nothing);
        };
        let members = self.members();
        let departed: BTreeSet<NodeId> = (task.targets.iter())
            .filter(|node| !members.contains(*node))
            .copied()
            .collect();
        match absence(&task.targets, &self.answers, found, &departed) {
            Absence::Claimed { family, job_id } => self.adopt(job_id, family),
            Absence::Proven => {
                let active = !self.removed();
                let submission = self.submission;
                self.change(TaskChange::End { active, submission })
            }
            Absence::Waiting => self.done(false, Followup::Nothing),
        }
    }

    fn adopt(&mut self, alias: JobId, family: JobFamilyId) -> SettleNext {
        self.change(TaskChange::Record(SendOutcome::Dispatch { alias, family }))
    }

    /// Hands the decided change to its own operation, which reads the rows again.
    fn change(&mut self, change: TaskChange) -> SettleNext {
        let ids = (self.install_id, self.row.work_id);
        self.phase = SettlePhase::Change;
        Ok(Next::Call(IoCall::child(ChangeTask::new(
            ids,
            change,
            self.now_ms,
        ))))
    }
}

impl Operation for SettleTask {
    type Output = Settled;
    type Error = StoreError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> SettleNext {
        let read = read_call(vec![task_place(self.install_id, self.row.work_id)]);
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> SettleNext {
        match std::mem::replace(&mut self.phase, SettlePhase::Change) {
            SettlePhase::Task => {
                let [task] = values::<1>(result)?;
                self.task = task.map(|bytes| decode::<TaskRow>(&bytes)).transpose()?;
                self.decide()
            }
            SettlePhase::Fetch(after) => {
                let fetched = result.child::<FetchedFamily, LifecycleError>()?;
                let (source, frames) = fetched.unwrap_or_default();
                let origin = source.map_or(RecordOrigin::Local, RecordOrigin::Peer);
                self.append_next(frames.into(), origin, after)
            }
            SettlePhase::Append(frames, origin, after) => {
                if let Err(error) = result.child::<AppendOutcome, RecordStoreError>()? {
                    warn!(%error, "A pulled plugin task record was not appended");
                }
                self.append_next(frames, origin, after)
            }
            SettlePhase::Project => {
                // A partial projection or a family not finished yet waits for the next pass.
                let projected = result.child::<ProjectedFamily, RecordStoreError>()?;
                let summary = match projected {
                    Ok(ProjectedFamily {
                        truncated: false,
                        projection: Some(projection),
                        ..
                    }) => final_of(&projection),
                    _ => None,
                };
                match summary {
                    Some(summary) => self.change(TaskChange::Settle(Box::new(summary))),
                    None => self.done(false, Followup::Nothing),
                }
            }
            SettlePhase::Close(node, asked) => {
                if let Ok(answer) = result.child::<ClosureAnswer, IoError>()? {
                    self.answers.push((node, answer));
                }
                self.ask_next(asked)
            }
            SettlePhase::Claims => match result {
                IoResult::Page { rows, next } => match (first_claim(&rows), next) {
                    (None, Some(after)) => self.claims(Some(after)),
                    (found, _) => self.claimed(found),
                },
                other => Err(unexpected(other)),
            },
            SettlePhase::Change => {
                let changed: Changed = result.child::<Changed, StoreError>()??;
                self.done(changed.settled, Followup::Nothing)
            }
        }
    }
}

#[cfg(test)]
#[path = "task_settle_tests.rs"]
mod tests;
