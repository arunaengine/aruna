//! Transactional per-firing delta updates that preserve control decisions and active rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::{CopiedFile, Firing, FiringState, StageState};
use aruna_core::automation::keys::{active_key, firing_key, record_key};
use aruna_core::automation::record::{Automation, DEFAULT_APPROVAL_MS, StageKind};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::workflow_run::RunFile;
use aruna_core::types::{Key, KeySpace, Value};
use aruna_core::user::id::UserId;
use byteview::ByteView;
use ulid::Ulid;

use super::copy_stage::StageOutcome;
use super::firing_reads::decode_ulid;
use super::run_stage::render_text;
use super::{AutoError, active_page, firing_row};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PollDelta {
    Finished {
        idx: usize,
        run_id: Ulid,
        state: StageState,
        outputs: Vec<RunFile>,
        error: Option<String>,
    },
    Missing {
        idx: usize,
        run_id: Ulid,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FiringDelta {
    BeginCancel,
    CancelReply {
        idx: usize,
        run_id: Ulid,
        error: Option<String>,
    },
    Resolve {
        stage: String,
        done: bool,
        copy_progress: Option<(Option<CopiedFile>, usize)>,
        actor: UserId,
        note: Option<String>,
    },
    Resume,
    Decide {
        stage: String,
        approve: bool,
        actor: UserId,
        note: Option<String>,
    },
    Poll(Vec<PollDelta>),
    Evaluate(bool),
    ClaimStage(usize),
    ReleaseStage(usize),
    RunStarted {
        idx: usize,
        outcome: Result<Ulid, String>,
    },
    StageOutcome {
        idx: usize,
        outcome: StageOutcome,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FiringTxResult {
    pub firing: Firing,
    pub changed: bool,
    pub claimed: bool,
    pub child_runs: Vec<(usize, Ulid)>,
}

#[derive(Clone, Debug, PartialEq)]
enum TxStep {
    Read,
    CheckActive {
        out: Box<FiringTxResult>,
        limit: u32,
        active_count: u32,
    },
    Write {
        out: Box<FiringTxResult>,
        deletes: Vec<(KeySpace, Key)>,
    },
    Delete(Box<FiringTxResult>),
}

/// Applies one conditional [`FiringDelta`] inside a write transaction and reconciles `active_key`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FiringTxOp {
    auto_id: Ulid,
    firing_id: Ulid,
    delta: FiringDelta,
    extra_writes: Vec<(KeySpace, Key, Value)>,
    extra_deletes: Vec<(KeySpace, Key)>,
    now_ms: u64,
    step: TxStep,
}

type DeltaOutcome = (bool, bool, Vec<(usize, Ulid)>);

impl FiringTxOp {
    pub(crate) fn new(auto_id: Ulid, firing_id: Ulid, delta: FiringDelta) -> Self {
        Self {
            auto_id,
            firing_id,
            delta,
            extra_writes: Vec::new(),
            extra_deletes: Vec::new(),
            now_ms: 0,
            step: TxStep::Read,
        }
    }

    pub(crate) fn with_extras(
        mut self,
        writes: Vec<(KeySpace, Key, Value)>,
        deletes: Vec<(KeySpace, Key)>,
    ) -> Self {
        self.extra_writes = writes;
        self.extra_deletes = deletes;
        self
    }

    fn apply_delta(&self, firing: &mut Firing) -> Result<DeltaOutcome, AutoError> {
        let now_ms = self.now_ms;
        let mut changed = false;
        let mut claimed = false;
        let mut child_runs = Vec::new();
        match &self.delta {
            FiringDelta::BeginCancel => {
                let before = firing.state;
                child_runs = firing.begin_cancel(now_ms);
                changed = !before.is_terminal();
            }
            FiringDelta::CancelReply { idx, run_id, error } => {
                if let Some(st) = firing.stages.get_mut(*idx)
                    && st.state == StageState::Cancelling
                    && st.receipt.workflow_run_id == Some(*run_id)
                {
                    match error {
                        None => {
                            st.receipt.cancel_sent = true;
                            st.error = None;
                        }
                        Some(msg) => {
                            st.receipt.cancel_sent = false;
                            st.error = Some(msg.clone());
                        }
                    }
                    firing.updated_at_ms = now_ms;
                    changed = true;
                }
            }
            FiringDelta::Resolve {
                stage,
                done,
                copy_progress,
                actor,
                note,
            } => {
                firing
                    .resolve_item(
                        stage,
                        *done,
                        copy_progress.clone(),
                        *actor,
                        note.clone(),
                        now_ms,
                    )
                    .map_err(AutoError::Conflict)?;
                changed = true;
            }
            FiringDelta::Resume => {
                firing.prepare_resume(now_ms).map_err(AutoError::Conflict)?;
                changed = true;
            }
            FiringDelta::Decide {
                stage,
                approve,
                actor,
                note,
            } => {
                if firing.state == FiringState::Cancelling || firing.state.is_terminal() {
                    return Err(AutoError::Conflict(format!(
                        "firing is already {}",
                        firing.state.as_str()
                    )));
                }
                let idx = firing.stage_index(stage).ok_or(AutoError::NotFound)?;
                if !matches!(firing.spec.stages[idx].kind, StageKind::Approval { .. })
                    || firing.stages[idx].state != StageState::WaitingForApproval
                {
                    return Err(AutoError::Conflict(
                        "stage is not waiting for approval".into(),
                    ));
                }
                if firing.stages[idx]
                    .receipt
                    .expires_at_ms
                    .is_some_and(|exp| now_ms >= exp)
                {
                    return Err(AutoError::Conflict("approval stage expired".into()));
                }
                let st = &mut firing.stages[idx];
                if *approve {
                    st.state = StageState::Succeeded;
                    st.receipt.approved_by = Some(*actor);
                } else {
                    st.state = StageState::Failed;
                    st.receipt.rejected_by = Some(*actor);
                    st.error = Some(
                        note.clone()
                            .unwrap_or_else(|| "rejected by approver".into()),
                    );
                }
                st.finished_at_ms = Some(now_ms);
                firing.evaluate(now_ms);
                changed = true;
            }
            FiringDelta::Poll(updates) => {
                for update in updates {
                    match update {
                        PollDelta::Finished {
                            idx,
                            run_id,
                            state,
                            outputs,
                            error,
                        } => {
                            if let Some(st) = firing.stages.get_mut(*idx)
                                && st.state.is_in_flight()
                                && st.receipt.workflow_run_id == Some(*run_id)
                            {
                                st.state = *state;
                                if *state == StageState::Succeeded {
                                    st.receipt.outputs = outputs.clone();
                                } else if *state == StageState::Failed {
                                    st.error = error.clone();
                                }
                                st.finished_at_ms = Some(now_ms);
                                changed = true;
                            }
                        }
                        PollDelta::Missing { idx, run_id } => {
                            let cancelling = firing.state == FiringState::Cancelling;
                            if let Some(st) = firing.stages.get_mut(*idx)
                                && st.state.is_in_flight()
                                && st.receipt.workflow_run_id == Some(*run_id)
                            {
                                if cancelling || st.state == StageState::Cancelling {
                                    st.state = StageState::Cancelled;
                                    st.finished_at_ms = Some(now_ms);
                                    st.error = Some(format!(
                                        "child workflow run {run_id} missing while cancelling"
                                    ));
                                } else {
                                    st.state = StageState::NeedsAttention;
                                    st.error =
                                        Some(format!("child workflow run {run_id} is missing"));
                                }
                                changed = true;
                            }
                        }
                    }
                }
                if changed {
                    firing.evaluate(now_ms);
                }
            }
            FiringDelta::Evaluate(dispatch) => {
                if firing.state.is_terminal() {
                    return Ok((false, false, Vec::new()));
                }
                let before = (firing.state, firing.stages.clone());
                firing.evaluate(now_ms);
                if firing.state == FiringState::Cancelling {
                    for (i, stage) in firing.stages.iter_mut().enumerate() {
                        if stage.state == StageState::Cancelling
                            && matches!(firing.spec.stages[i].kind, StageKind::Copy { .. })
                        {
                            stage.state = StageState::Cancelled;
                            stage.finished_at_ms = Some(now_ms);
                        }
                    }
                    firing.finish_cancel(now_ms);
                } else if *dispatch && firing.state == FiringState::Running {
                    let snap = firing.clone();
                    for (i, stage) in firing.stages.iter_mut().enumerate() {
                        if stage.state == StageState::Ready
                            && let StageKind::Approval {
                                message,
                                timeout_ms,
                            } = &firing.spec.stages[i].kind
                        {
                            stage.state = StageState::WaitingForApproval;
                            stage.started_at_ms = Some(now_ms);
                            stage.receipt.expires_at_ms = Some(
                                now_ms.saturating_add(timeout_ms.unwrap_or(DEFAULT_APPROVAL_MS)),
                            );
                            stage.receipt.preview_json = Some(render_text(message, &snap));
                        }
                    }
                    firing.evaluate(now_ms);
                }
                changed = firing.state != before.0 || firing.stages != before.1;
            }
            FiringDelta::ClaimStage(idx) => {
                if firing.state == FiringState::Running
                    && let Some(st) = firing.stages.get_mut(*idx)
                {
                    if st.state == StageState::Ready {
                        st.state = StageState::Running;
                        st.started_at_ms.get_or_insert(now_ms);
                        firing.updated_at_ms = now_ms;
                        (claimed, changed) = (true, true);
                    } else if st.state == StageState::Running
                        && matches!(firing.spec.stages[*idx].kind, StageKind::Copy { .. })
                    {
                        claimed = true;
                    }
                }
            }
            FiringDelta::ReleaseStage(idx) => {
                let cancelling = firing.state == FiringState::Cancelling;
                if let Some(st) = firing.stages.get_mut(*idx)
                    && st.receipt.workflow_run_id.is_none()
                    && st.state.is_in_flight()
                {
                    if cancelling || st.state == StageState::Cancelling {
                        st.state = StageState::Cancelled;
                        st.finished_at_ms = Some(now_ms);
                        firing.finish_cancel(now_ms);
                    } else {
                        st.state = StageState::Ready;
                        st.started_at_ms = None;
                        firing.evaluate(now_ms);
                    }
                    changed = true;
                }
            }
            FiringDelta::RunStarted { idx, outcome } => {
                let cancelling = firing.state == FiringState::Cancelling;
                if let Some(st) = firing.stages.get_mut(*idx) {
                    let stage_cancel = cancelling || st.state == StageState::Cancelling;
                    match outcome {
                        Ok(run_id) => {
                            st.receipt.workflow_run_id = Some(*run_id);
                            st.started_at_ms.get_or_insert(now_ms);
                            if stage_cancel {
                                st.state = StageState::Cancelling;
                                st.receipt.cancel_sent = false;
                                child_runs.push((*idx, *run_id));
                            } else {
                                st.state = StageState::Running;
                            }
                        }
                        Err(err) => {
                            st.finished_at_ms = Some(now_ms);
                            if stage_cancel {
                                st.state = StageState::Cancelled;
                                firing.finish_cancel(now_ms);
                            } else {
                                st.state = StageState::Failed;
                                st.error = Some(err.clone());
                                firing.evaluate(now_ms);
                            }
                        }
                    }
                    firing.updated_at_ms = now_ms;
                    changed = true;
                }
            }
            FiringDelta::StageOutcome { idx, outcome } => {
                let cancelling = firing.state == FiringState::Cancelling;
                if let Some(st) = firing.stages.get_mut(*idx) {
                    let stage_cancel = cancelling || st.state == StageState::Cancelling;
                    let mut finish_cancel = false;
                    match outcome {
                        StageOutcome::Progress(copied) => {
                            let exists = st.receipt.copied.iter().any(|c| {
                                c.dest_version == copied.dest_version
                                    && c.dest_key == copied.dest_key
                            });
                            if !exists {
                                st.receipt.copied.push(copied.clone());
                            }
                            st.receipt.pending_copy = None;
                            if stage_cancel {
                                st.state = StageState::Cancelled;
                                st.finished_at_ms = Some(now_ms);
                                finish_cancel = true;
                            } else {
                                st.state = StageState::Running;
                            }
                        }
                        StageOutcome::NeedsAttention(err, pending) => {
                            st.receipt.pending_copy = pending.clone();
                            st.state = StageState::NeedsAttention;
                            st.error = Some(err.clone());
                        }
                        StageOutcome::CopiedAll | StageOutcome::Failed(_) => {
                            st.receipt.pending_copy = None;
                            st.finished_at_ms = Some(now_ms);
                            if let StageOutcome::Failed(err) = outcome {
                                st.error = Some(err.clone());
                            }
                            st.state = match (stage_cancel, outcome) {
                                (true, _) => StageState::Cancelled,
                                (false, StageOutcome::CopiedAll) => StageState::Succeeded,
                                (false, StageOutcome::Failed(_)) => StageState::Failed,
                                _ => unreachable!(),
                            };
                        }
                        StageOutcome::Dataset(doc_id) => {
                            st.receipt.dataset_id = Some(*doc_id);
                            st.state = StageState::Succeeded;
                            st.finished_at_ms = Some(now_ms);
                        }
                        StageOutcome::Notified(count) => {
                            st.receipt.notified = Some(*count);
                            st.state = StageState::Succeeded;
                            st.finished_at_ms = Some(now_ms);
                        }
                    }
                    if finish_cancel {
                        firing.finish_cancel(now_ms);
                    } else {
                        firing.evaluate(now_ms);
                    }
                    changed = true;
                }
            }
        }
        Ok((changed, claimed, child_runs))
    }
}

impl Operation for FiringTxOp {
    type Output = FiringTxResult;
    type Error = AutoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<FiringTxResult>, AutoError> {
        self.step = TxStep::Read;
        let mut reads = vec![(
            AUTOMATION_RUNS_KEYSPACE.to_string(),
            firing_key(self.auto_id, self.firing_id),
        )];
        if matches!(self.delta, FiringDelta::Resume) {
            reads.push((AUTOMATIONS_KEYSPACE.to_string(), record_key(self.auto_id)));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FiringTxResult>, AutoError> {
        match std::mem::replace(&mut self.step, TxStep::Read) {
            TxStep::Read => {
                let (bytes, auto_bytes) = if matches!(self.delta, FiringDelta::Resume) {
                    let values = result.values()?;
                    let firing_bytes = values
                        .first()
                        .and_then(Option::as_ref)
                        .cloned()
                        .ok_or(AutoError::NotFound)?;
                    let auto_bytes = values
                        .get(1)
                        .and_then(Option::as_ref)
                        .cloned()
                        .ok_or(AutoError::NotFound)?;
                    (firing_bytes, Some(auto_bytes))
                } else {
                    (result.value()?.ok_or(AutoError::NotFound)?, None)
                };
                let mut firing = Firing::from_bytes(&bytes)?;
                let (changed, claimed, child_runs) = self.apply_delta(&mut firing)?;
                let out = FiringTxResult {
                    firing,
                    changed,
                    claimed,
                    child_runs,
                };
                if let Some(auto_bytes) = auto_bytes {
                    let auto = Automation::from_bytes(&auto_bytes)?;
                    self.step = TxStep::CheckActive {
                        out: Box::new(out),
                        limit: auto.spec.scope.limits.max_active_firings,
                        active_count: 0,
                    };
                    return Ok(Next::Call(active_page(None)));
                }
                self.commit_out(out)
            }
            TxStep::CheckActive {
                out,
                limit,
                mut active_count,
            } => {
                let (rows, next) = result.page()?;
                for (key, value) in rows {
                    let same_auto = decode_ulid(&value) == Some(self.auto_id);
                    let same_firing = key.get(1..17).and_then(decode_ulid) == Some(self.firing_id);
                    if same_auto && !same_firing {
                        active_count = active_count.saturating_add(1);
                    }
                }
                if let Some(after) = next {
                    self.step = TxStep::CheckActive {
                        out,
                        limit,
                        active_count,
                    };
                    return Ok(Next::Call(active_page(Some(after))));
                }
                if active_count >= limit {
                    return Err(AutoError::Limited(
                        "max_active_firings limit reached".into(),
                    ));
                }
                self.commit_out(*out)
            }
            TxStep::Write { out, deletes } => {
                result.done()?;
                if !deletes.is_empty() {
                    self.step = TxStep::Delete(out);
                    return Ok(Next::Call(IoCall::Delete(deletes)));
                }
                Ok(Next::Done(*out))
            }
            TxStep::Delete(out) => {
                result.done()?;
                Ok(Next::Done(*out))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

impl FiringTxOp {
    fn commit_out(&mut self, out: FiringTxResult) -> Result<Next<FiringTxResult>, AutoError> {
        let terminal = out.firing.state.is_terminal();
        let mut writes = std::mem::take(&mut self.extra_writes);
        let mut deletes = std::mem::take(&mut self.extra_deletes);
        if out.changed {
            writes.push(firing_row(&out.firing)?);
            if !terminal {
                writes.push((
                    AUTOMATION_RUNS_KEYSPACE.to_string(),
                    active_key(out.firing.firing_id),
                    ByteView::from(out.firing.automation_id.to_bytes().to_vec()),
                ));
            }
        }
        if terminal {
            deletes.push((
                AUTOMATION_RUNS_KEYSPACE.to_string(),
                active_key(out.firing.firing_id),
            ));
        }
        if !writes.is_empty() {
            self.step = TxStep::Write {
                out: Box::new(out),
                deletes,
            };
            return Ok(Next::Call(IoCall::Write(writes)));
        }
        if !deletes.is_empty() {
            self.step = TxStep::Delete(Box::new(out));
            return Ok(Next::Call(IoCall::Delete(deletes)));
        }
        Ok(Next::Done(out))
    }
}
