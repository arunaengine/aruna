//! Advances active automation firings through their DAG: polls child runs, fans out ready
//! workflows, executes copy/dataset/notify/approval stages and finalizes async cancellation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::{Firing, FiringState, StageState};
use aruna_core::automation::keys::{active_key, firing_key, record_key};
use aruna_core::automation::record::{Automation, DEFAULT_APPROVAL_MS, Lifecycle, StageKind};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord, run_key};
use aruna_core::structs::identity::realm::RealmId;
use ulid::Ulid;

use super::copy_stage::{CopyStageOp, StageOutcome};
use super::dataset_stage::DatasetStageOp;
use super::firing_reads::decode_ulid;
use super::notify_stage::NotifyStageOp;
use super::run_stage::{StartStageRun, render_text};
use super::{AutoError, active_page, firing_row};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StepReport {
    pub checked: usize,
    pub advanced: usize,
    pub finished: usize,
}

#[derive(Debug, PartialEq)]
enum StepPhase {
    Active,
    LoadPair(Ulid, Ulid),
    PollRuns(Vec<(usize, Ulid)>),
    StartRuns(usize, Vec<usize>),
    RunCopy(usize),
    RunDataset(usize),
    RunNotify(usize),
    Save,
    DropActive(Ulid),
}

/// Drives every active automation firing one tick forward on `local`.
#[derive(Debug, PartialEq)]
pub struct StepFiringsOperation {
    local: (NodeId, RealmId),
    now_ms: u64,
    queue: Vec<(Ulid, Ulid)>,
    auto: Option<Automation>,
    firing: Option<Firing>,
    dirty: bool,
    report: StepReport,
    phase: StepPhase,
}

impl StepFiringsOperation {
    pub fn new(local: (NodeId, RealmId)) -> Self {
        Self {
            local,
            now_ms: 0,
            queue: Vec::new(),
            auto: None,
            firing: None,
            dirty: false,
            report: StepReport::default(),
            phase: StepPhase::Active,
        }
    }

    fn next_firing(&mut self) -> Result<Next<StepReport>, AutoError> {
        let Some((firing_id, auto_id)) = self.queue.pop() else {
            return Ok(Next::Done(self.report));
        };
        self.auto = None;
        self.firing = None;
        self.dirty = false;
        self.phase = StepPhase::LoadPair(firing_id, auto_id);
        Ok(Next::Call(IoCall::Read(vec![
            (AUTOMATIONS_KEYSPACE.to_string(), record_key(auto_id)),
            (
                AUTOMATION_RUNS_KEYSPACE.to_string(),
                firing_key(auto_id, firing_id),
            ),
        ])))
    }

    fn save_current(&mut self) -> Result<Next<StepReport>, AutoError> {
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
        let firing = self.firing.as_mut().ok_or(IoError::Unfinished)?;
        let before = firing.state;
        firing.evaluate(&auto.spec, self.now_ms);
        if firing.state != before {
            self.dirty = true;
        }
        let firing_id = firing.firing_id;
        let terminal = firing.state.is_terminal();
        if !self.dirty && !terminal {
            return self.next_firing();
        }
        if self.dirty {
            self.report.advanced += 1;
        }
        let row = firing_row(firing)?;
        self.phase = StepPhase::Save;
        let _ = firing_id;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn dispatch_ready(&mut self) -> Result<Next<StepReport>, AutoError> {
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?.clone();
        let firing = self.firing.as_mut().ok_or(IoError::Unfinished)?;
        firing.evaluate(&auto.spec, self.now_ms);
        if firing.state == FiringState::Cancelling {
            // Stop any in-flight copy stage cleanly between files while keeping committed receipts.
            for (i, stage) in firing.stages.iter_mut().enumerate() {
                if stage.state == StageState::Cancelling
                    && matches!(auto.spec.stages[i].kind, StageKind::Copy { .. })
                {
                    stage.state = StageState::Cancelled;
                    stage.finished_at_ms = Some(self.now_ms);
                    self.dirty = true;
                }
            }
            firing.finish_cancel(self.now_ms);
            return self.save_current();
        }
        if firing.state != FiringState::Running || auto.lifecycle != Lifecycle::Active {
            return self.save_current();
        }
        // Enter any Ready approval stages immediately (no side effects).
        let snap = firing.clone();
        for (i, stage) in firing.stages.iter_mut().enumerate() {
            if stage.state == StageState::Ready
                && let StageKind::Approval {
                    message,
                    timeout_ms,
                } = &auto.spec.stages[i].kind
            {
                let preview = render_text(message, &snap);
                stage.state = StageState::WaitingForApproval;
                stage.started_at_ms = Some(self.now_ms);
                stage.receipt.expires_at_ms = Some(
                    self.now_ms
                        .saturating_add(timeout_ms.unwrap_or(DEFAULT_APPROVAL_MS)),
                );
                stage.receipt.preview_json = Some(preview);
                self.dirty = true;
            }
        }
        // 1. Launch ready `run` stages in parallel up to `max_concurrent_workflows`.
        let mut ready_runs = firing.ready_runs(&auto.spec);
        if let Some(first) = ready_runs.pop() {
            let firing_snap = firing.clone();
            return self.launch_run(auto, firing_snap, first, ready_runs);
        }
        // 2. Continue an in-flight copy stage or start one ready copy stage at a time.
        let active_copy = firing.stages.iter().enumerate().find_map(|(i, s)| {
            (s.state == StageState::Running
                && matches!(auto.spec.stages[i].kind, StageKind::Copy { .. }))
            .then_some(i)
        });
        if let Some(idx) = active_copy.or_else(|| firing.next_copy(&auto.spec)) {
            if firing.stages[idx].started_at_ms.is_none() {
                firing.stages[idx].started_at_ms = Some(self.now_ms);
                firing.stages[idx].state = StageState::Running;
                self.dirty = true;
            }
            let op = CopyStageOp::new(auto, firing.clone(), idx, self.local)?;
            self.phase = StepPhase::RunCopy(idx);
            return Ok(Next::Call(IoCall::child(op)));
        }
        // 3. Execute first ready `dataset` stage.
        if let Some(idx) = firing.stages.iter().enumerate().find_map(|(i, s)| {
            (s.state == StageState::Ready
                && matches!(auto.spec.stages[i].kind, StageKind::Dataset { .. }))
            .then_some(i)
        }) {
            firing.stages[idx].started_at_ms = Some(self.now_ms);
            let op = DatasetStageOp::new(auto, firing.clone(), idx, self.local)?;
            self.phase = StepPhase::RunDataset(idx);
            return Ok(Next::Call(IoCall::child(op)));
        }
        // 4. Execute first ready `notify` stage.
        if let Some(idx) = firing.stages.iter().enumerate().find_map(|(i, s)| {
            (s.state == StageState::Ready
                && matches!(auto.spec.stages[i].kind, StageKind::Notify { .. }))
            .then_some(i)
        }) {
            firing.stages[idx].started_at_ms = Some(self.now_ms);
            let op = NotifyStageOp::new(auto, firing, idx, self.local.1)?;
            self.phase = StepPhase::RunNotify(idx);
            return Ok(Next::Call(IoCall::child(op)));
        }
        self.save_current()
    }

    fn launch_run(
        &mut self,
        auto: Automation,
        firing_snap: Firing,
        idx: usize,
        rest: Vec<usize>,
    ) -> Result<Next<StepReport>, AutoError> {
        match StartStageRun::new(auto, firing_snap, idx, self.local) {
            Ok(op) => {
                self.phase = StepPhase::StartRuns(idx, rest);
                Ok(Next::Call(IoCall::child(op)))
            }
            Err(err) => {
                let firing = self.firing.as_mut().ok_or(IoError::Unfinished)?;
                let st = &mut firing.stages[idx];
                st.state = StageState::Failed;
                st.error = Some(err.to_string());
                st.finished_at_ms = Some(self.now_ms);
                self.dirty = true;
                self.dispatch_ready()
            }
        }
    }

    fn apply_outcome(
        &mut self,
        idx: usize,
        outcome: StageOutcome,
    ) -> Result<Next<StepReport>, AutoError> {
        let firing = self.firing.as_mut().ok_or(IoError::Unfinished)?;
        let st = &mut firing.stages[idx];
        self.dirty = true;
        match outcome {
            StageOutcome::Progress(copied) => {
                st.receipt.copied.push(copied);
                st.state = StageState::Running;
                // Persist each committed file receipt before continuing so cancel stops cleanly.
                return self.save_current();
            }
            StageOutcome::CopiedAll => {
                st.state = StageState::Succeeded;
                st.finished_at_ms = Some(self.now_ms);
            }
            StageOutcome::Dataset(doc_id) => {
                st.receipt.dataset_id = Some(doc_id);
                st.state = StageState::Succeeded;
                st.finished_at_ms = Some(self.now_ms);
            }
            StageOutcome::Notified(count) => {
                st.receipt.notified = Some(count);
                st.state = StageState::Succeeded;
                st.finished_at_ms = Some(self.now_ms);
            }
            StageOutcome::Failed(err) => {
                st.state = StageState::Failed;
                st.error = Some(err);
                st.finished_at_ms = Some(self.now_ms);
            }
            StageOutcome::NeedsAttention(err) => {
                st.state = StageState::NeedsAttention;
                st.error = Some(err);
            }
        }
        self.dispatch_ready()
    }
}

impl Operation for StepFiringsOperation {
    type Output = StepReport;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<StepReport>, AutoError> {
        Ok(Next::Call(active_page(None)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StepReport>, AutoError> {
        match std::mem::replace(&mut self.phase, StepPhase::Active) {
            StepPhase::Active => {
                let (rows, next) = result.page()?;
                for (key, value) in rows {
                    if let (Some(firing_id), Some(auto_id)) =
                        (key.get(1..17).and_then(decode_ulid), decode_ulid(&value))
                    {
                        self.queue.push((firing_id, auto_id));
                    }
                }
                if let Some(after) = next {
                    self.phase = StepPhase::Active;
                    return Ok(Next::Call(active_page(Some(after))));
                }
                self.next_firing()
            }
            StepPhase::LoadPair(firing_id, _auto_id) => {
                let values = result.values()?;
                let (Some(auto_bytes), Some(firing_bytes)) = (
                    values.first().and_then(Option::as_ref),
                    values.get(1).and_then(Option::as_ref),
                ) else {
                    self.phase = StepPhase::DropActive(firing_id);
                    return Ok(Next::Call(IoCall::Delete(vec![(
                        AUTOMATION_RUNS_KEYSPACE.to_string(),
                        active_key(firing_id),
                    )])));
                };
                let auto = Automation::from_bytes(auto_bytes)?;
                let firing = Firing::from_bytes(firing_bytes)?;
                self.report.checked += 1;
                let polling: Vec<(usize, Ulid)> = firing
                    .stages
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| {
                        (s.state.is_in_flight()
                            && matches!(auto.spec.stages[i].kind, StageKind::Run { .. }))
                        .then_some(s.receipt.workflow_run_id.map(|id| (i, id)))
                        .flatten()
                    })
                    .collect();
                self.auto = Some(auto);
                self.firing = Some(firing);
                if !polling.is_empty() {
                    let reads = polling
                        .iter()
                        .map(|(_, id)| (WORKFLOW_RUN_KEYSPACE.to_string(), run_key(*id)))
                        .collect();
                    self.phase = StepPhase::PollRuns(polling);
                    return Ok(Next::Call(IoCall::Read(reads)));
                }
                self.dispatch_ready()
            }
            StepPhase::PollRuns(polling) => {
                let values = result.values()?;
                let firing = self.firing.as_mut().ok_or(IoError::Unfinished)?;
                for ((idx, _), bytes) in polling.into_iter().zip(values) {
                    let Some(bytes) = bytes else {
                        continue;
                    };
                    let rec = WorkflowRunRecord::from_bytes(&bytes)?;
                    if !rec.state.is_terminal() {
                        continue;
                    }
                    let st = &mut firing.stages[idx];
                    match rec.state {
                        RunState::Succeeded => {
                            st.state = StageState::Succeeded;
                            st.receipt.outputs = rec.outputs;
                        }
                        RunState::Failed => {
                            st.state = StageState::Failed;
                            st.error = rec.error;
                        }
                        RunState::Cancelled => {
                            st.state = StageState::Cancelled;
                        }
                        _ => {}
                    }
                    st.finished_at_ms = Some(self.now_ms);
                    self.dirty = true;
                }
                self.dispatch_ready()
            }
            StepPhase::StartRuns(idx, mut rest) => {
                let firing = self.firing.as_mut().ok_or(IoError::Unfinished)?;
                let st = &mut firing.stages[idx];
                self.dirty = true;
                match result.child::<Ulid, AutoError>()? {
                    Ok(run_id) => {
                        st.state = StageState::Running;
                        st.started_at_ms = Some(self.now_ms);
                        st.receipt.workflow_run_id = Some(run_id);
                    }
                    Err(err) => {
                        st.state = StageState::Failed;
                        st.error = Some(err.to_string());
                        st.finished_at_ms = Some(self.now_ms);
                    }
                }
                if let Some(next_idx) = rest.pop() {
                    let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?.clone();
                    let snap = self.firing.as_ref().ok_or(IoError::Unfinished)?.clone();
                    return self.launch_run(auto, snap, next_idx, rest);
                }
                self.save_current()
            }
            StepPhase::RunCopy(idx) | StepPhase::RunDataset(idx) | StepPhase::RunNotify(idx) => {
                let outcome = match result.child::<StageOutcome, AutoError>()? {
                    Ok(out) => out,
                    Err(err) => StageOutcome::Failed(err.to_string()),
                };
                self.apply_outcome(idx, outcome)
            }
            StepPhase::Save => {
                result.done()?;
                let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
                if firing.state.is_terminal() {
                    self.report.finished += 1;
                    let id = firing.firing_id;
                    self.phase = StepPhase::DropActive(id);
                    return Ok(Next::Call(IoCall::Delete(vec![(
                        AUTOMATION_RUNS_KEYSPACE.to_string(),
                        active_key(id),
                    )])));
                }
                self.next_firing()
            }
            StepPhase::DropActive(_) => {
                result.done()?;
                self.next_firing()
            }
        }
    }
}
