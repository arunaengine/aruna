//! Advances active automation firings through their pinned DAG with conditional updates,
//! cross-firing workflow slot accounting, feature switches, child cancel retry and recovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::firing::Firing;
use aruna_core::automation::record::Automation;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use ulid::Ulid;

use super::control::CancelAck;
use super::firing_tx::{FiringDelta, FiringTxResult};
use super::schedule::{
    CancelRetryOutcome, ClaimRecovery, DispatchPlan, NextAction, RecoveryStep, RunTrack, StepPhase,
    build_poll_deltas, build_stage_dispatch, cancel_reply_outcome, claim_recovery_call,
    decode_pair, drop_active_call, firing_tx_call, first_stranded_claim, in_flight_runs,
    load_pair_call, poll_reads_call, resolve_claim_recovery, resolve_reconciled_run,
    stage_outcome_delta, start_run_delta,
};
use super::{AutoError, active_page};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StepReport {
    pub checked: usize,
    pub advanced: usize,
    pub finished: usize,
    pub cancel_jobs: Vec<JobId>,
    pub wake_runs: Vec<Ulid>,
    pub cancel_acks: Vec<CancelAck>,
}

/// Drives every active automation firing one tick forward on `local`.
#[derive(Debug, PartialEq)]
pub struct StepFiringsOperation {
    local: (NodeId, RealmId),
    track: RunTrack,
    auto: Option<Automation>,
    firing: Option<Firing>,
    report: StepReport,
    phase: StepPhase,
}

impl StepFiringsOperation {
    pub fn new(local: (NodeId, RealmId)) -> Self {
        Self {
            local,
            track: RunTrack::default(),
            auto: None,
            firing: None,
            report: StepReport::default(),
            phase: StepPhase::Config,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.track.disabled_features = disabled;
        self
    }

    fn tx_step(
        &mut self,
        delta: FiringDelta,
        phase: StepPhase,
    ) -> Result<Next<StepReport>, AutoError> {
        let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
        self.phase = phase;
        Ok(Next::Call(firing_tx_call(firing, delta)))
    }

    fn take_tx(&mut self, result: IoResult) -> Result<(bool, Vec<(usize, Ulid)>), AutoError> {
        let tx = result.child::<FiringTxResult, AutoError>()??;
        if tx.changed {
            self.report.advanced += 1;
        }
        self.firing = Some(tx.firing);
        Ok((tx.claimed, tx.child_runs))
    }

    fn count_batch(&mut self, offset: usize) -> Result<Next<StepReport>, AutoError> {
        let Some((end, call)) = self.track.count_batch_call(offset) else {
            return self.next_firing();
        };
        self.phase = StepPhase::CountRuns(end);
        Ok(Next::Call(call))
    }

    fn next_firing(&mut self) -> Result<Next<StepReport>, AutoError> {
        let Some((firing_id, auto_id)) = self.track.queue.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.report)));
        };
        (self.auto, self.firing) = (None, None);
        self.phase = StepPhase::LoadPair(firing_id, auto_id);
        Ok(Next::Call(load_pair_call(auto_id, firing_id)))
    }

    fn evaluate_current(&mut self) -> Result<Next<StepReport>, AutoError> {
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
        let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
        let delta = self.track.evaluate_delta(auto, firing);
        self.tx_step(delta, StepPhase::Evaluate)
    }

    fn apply_action(&mut self, action: NextAction) -> Result<Next<StepReport>, AutoError> {
        match action {
            NextAction::Finished => {
                self.report.finished += 1;
                self.next_firing()
            }
            NextAction::NextFiring => self.next_firing(),
            NextAction::Call(phase, call) => {
                self.phase = phase;
                Ok(Next::Call(call))
            }
            NextAction::Tx(phase, delta) => self.tx_step(delta, phase),
        }
    }

    fn continue_after_load(&mut self) -> Result<Next<StepReport>, AutoError> {
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
        let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
        if let Some(mut claim) = first_stranded_claim(firing) {
            if let Some(call) = claim_recovery_call(&mut claim, auto, firing) {
                self.phase = StepPhase::RecoverClaim(claim);
                return Ok(Next::Call(call));
            }
            if let ClaimRecovery::RunIdem { idx, .. } = claim {
                if let Some(count) = self.track.active_runs.get_mut(&firing.automation_id) {
                    *count = count.saturating_sub(1);
                }
                return self.tx_step(
                    FiringDelta::ReleaseStage(idx),
                    StepPhase::CommitRecovery(false),
                );
            }
        }
        let polling = in_flight_runs(firing);
        if polling.is_empty() {
            return self.evaluate_current();
        }
        let call = poll_reads_call(&polling);
        self.phase = StepPhase::PollRuns(polling);
        Ok(Next::Call(call))
    }

    fn dispatch_next(&mut self) -> Result<Next<StepReport>, AutoError> {
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
        let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
        let action = self.track.plan_next(auto, firing, self.local);
        self.apply_action(action)
    }
}

impl Operation for StepFiringsOperation {
    type Output = StepReport;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<StepReport>, AutoError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.local.1,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StepReport>, AutoError> {
        match std::mem::replace(&mut self.phase, StepPhase::Active) {
            StepPhase::Config => {
                self.track.config = result.child::<RealmConfigDocument, GetConfigError>()?.ok();
                self.phase = StepPhase::Active;
                Ok(Next::Call(active_page(None)))
            }
            StepPhase::Active => {
                let (rows, next) = result.page()?;
                self.track.collect_active(rows);
                if let Some(after) = next {
                    self.phase = StepPhase::Active;
                    return Ok(Next::Call(active_page(Some(after))));
                }
                self.count_batch(0)
            }
            StepPhase::CountRuns(offset) => {
                self.track.tally_active_runs(result.values()?);
                self.count_batch(offset)
            }
            StepPhase::LoadPair(firing_id, _) => {
                let Some((auto, firing)) = decode_pair(result.values()?)? else {
                    self.phase = StepPhase::DropActive(firing_id);
                    return Ok(Next::Call(drop_active_call(firing_id)));
                };
                self.report.checked += 1;
                (self.auto, self.firing) = (Some(auto), Some(firing));
                self.continue_after_load()
            }
            StepPhase::RecoverClaim(claim) => {
                let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
                match resolve_claim_recovery(claim, firing, self.local, result, &mut self.track)? {
                    RecoveryStep::Commit(call) => {
                        self.phase = StepPhase::CommitRecovery(false);
                        Ok(Next::Call(call))
                    }
                    RecoveryStep::ReconcileRun(idx, call) => {
                        self.phase = StepPhase::ReconcileRun(idx);
                        Ok(Next::Call(call))
                    }
                }
            }
            StepPhase::ReconcileRun(idx) => {
                let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
                let (delta, terminal) =
                    resolve_reconciled_run(idx, firing, result, &mut self.track)?;
                self.tx_step(delta, StepPhase::CommitRecovery(terminal))
            }
            StepPhase::CommitRecovery(terminal) => {
                let child = self.take_tx(result)?.1.into_iter().next();
                match (terminal, child) {
                    (false, Some((idx, run_id))) => {
                        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                        let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
                        let action = self
                            .track
                            .retry_cancel_action(auto, firing, self.local, idx, run_id);
                        self.apply_action(action)
                    }
                    _ => self.continue_after_load(),
                }
            }
            StepPhase::PollRuns(polling) => {
                let aid = self
                    .firing
                    .as_ref()
                    .ok_or(IoError::Unfinished)?
                    .automation_id;
                let deltas =
                    build_poll_deltas(polling, result.values()?, &mut self.track.active_runs, aid)?;
                match deltas.is_empty() {
                    true => self.evaluate_current(),
                    false => self.tx_step(FiringDelta::Poll(deltas), StepPhase::CommitPoll),
                }
            }
            StepPhase::CommitPoll => {
                self.take_tx(result)?;
                self.evaluate_current()
            }
            StepPhase::Evaluate | StepPhase::CommitCancel | StepPhase::ReleaseRun => {
                self.take_tx(result)?;
                self.dispatch_next()
            }
            StepPhase::RetryCancel(idx, run_id) => {
                match cancel_reply_outcome(idx, run_id, result)? {
                    CancelRetryOutcome::Ack { running, woken } => {
                        let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
                        self.report.cancel_jobs.extend(running);
                        if let Some(woken_id) = woken {
                            self.report.wake_runs.push(woken_id);
                        }
                        self.report.cancel_acks.push(CancelAck {
                            automation_id: firing.automation_id,
                            firing_id: firing.firing_id,
                            stage_idx: idx,
                            run_id,
                        });
                        self.report.advanced += 1;
                        self.dispatch_next()
                    }
                    CancelRetryOutcome::RecordError(delta) => {
                        self.tx_step(delta, StepPhase::CommitCancel)
                    }
                }
            }
            StepPhase::Claim(work) => {
                if !self.take_tx(result)?.0 {
                    return self.dispatch_next();
                }
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?.clone();
                let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?.clone();
                match build_stage_dispatch(work, auto, firing, self.local)? {
                    DispatchPlan::StartRun(idx, call) => {
                        self.phase = StepPhase::StartRun(idx);
                        Ok(Next::Call(call))
                    }
                    DispatchPlan::RunStage(idx, call) => {
                        self.phase = StepPhase::RunStage(idx);
                        Ok(Next::Call(call))
                    }
                    DispatchPlan::Commit(delta) => self.tx_step(delta, StepPhase::CommitRun),
                }
            }
            StepPhase::StartRun(idx) => {
                let aid = self
                    .firing
                    .as_ref()
                    .ok_or(IoError::Unfinished)?
                    .automation_id;
                let (delta, next) = start_run_delta(idx, aid, result, &mut self.track)?;
                self.tx_step(delta, next)
            }
            StepPhase::CommitRun => match self.take_tx(result)?.1.into_iter().next() {
                Some((idx, run_id)) => {
                    let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                    let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
                    let action = self
                        .track
                        .retry_cancel_action(auto, firing, self.local, idx, run_id);
                    self.apply_action(action)
                }
                None => self.evaluate_current(),
            },
            StepPhase::RunStage(idx) => {
                let (delta, stop) = stage_outcome_delta(idx, result)?;
                self.tx_step(delta, StepPhase::CommitOutcome(stop))
            }
            StepPhase::CommitOutcome(stop_after) => {
                self.take_tx(result)?;
                if self.firing.as_ref().is_some_and(|f| f.state.is_terminal()) {
                    self.report.finished += 1;
                    return self.next_firing();
                }
                if stop_after {
                    self.next_firing()
                } else {
                    self.evaluate_current()
                }
            }
            StepPhase::DropActive(_) => {
                result.done()?;
                self.next_firing()
            }
        }
    }
}
