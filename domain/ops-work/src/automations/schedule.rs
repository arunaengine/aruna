//! Stage planning, polling, dispatch construction and run-slot accounting for the firing runner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::automation::firing::{Firing, FiringState, StageState};
use aruna_core::automation::keys::{active_key, firing_key, record_key};
use aruna_core::automation::record::{Automation, Lifecycle, StageKind};
use aruna_core::calls::{IoCall, IoResult};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    AUTOMATION_INTENTS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE,
    RUN_IDEMPOTENCY_KEYSPACE, WORKFLOW_RUN_KEYSPACE,
};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::workflow_run::{
    RunKeyRecord, RunState, WorkflowRunRecord, run_key, run_key_row,
};
use aruna_core::structs::identity::features::{Feature, resolve};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{GroupId, Key, Value};
use ulid::Ulid;

use super::copy_stage::{CopyStageOp, StageOutcome, stage_intent_id};
use super::dataset_stage::DatasetStageOp;
use super::firing_reads::decode_ulid;
use super::firing_tx::{FiringDelta, FiringTxOp, PollDelta};
use super::notify_stage::NotifyStageOp;
use super::run_stage::{StartStageRun, run_idempotency_key};
use super::{AutoError, automation_auth_for};
use crate::plugins::intents::{IntentRecord, IntentState, intent_key};
use crate::plugins::rows::{decode, encode, row};
use crate::workflows::CatalogError;
use crate::workflows::runs::admit::{ReconcileRunOperation, ReconciledRun};
use crate::workflows::runs::cancel::{CancelRunOperation, CancelledRun};

#[derive(Debug, PartialEq)]
pub(crate) enum StageWork {
    Run(usize),
    Copy(usize),
    Dataset(usize),
    Notify(usize),
}

#[derive(Debug, PartialEq)]
pub(crate) enum ClaimRecovery {
    RunIdem {
        idx: usize,
        stage: String,
        attempt: u32,
        idem_row: Option<Key>,
    },
    StageIntent {
        idx: usize,
        stage_id: Ulid,
        is_dataset: bool,
    },
}

pub(crate) enum RecoveryStep {
    Commit(IoCall),
    ReconcileRun(usize, IoCall),
}

pub(crate) enum ReconcileAction {
    Commit(FiringDelta, bool),
    Defer,
}

pub(crate) enum CancelRetryOutcome {
    Ack {
        running: Vec<JobId>,
        woken: Option<Ulid>,
    },
    RecordError(FiringDelta),
}

#[derive(Debug, PartialEq)]
pub(crate) enum StepPhase {
    Config,
    Active,
    CountRuns(usize),
    LoadPair(Ulid, Ulid),
    RecoverClaim(ClaimRecovery),
    ReconcileRun(usize),
    CommitRecovery(bool),
    PollRuns(Vec<(usize, Ulid)>),
    CommitPoll,
    Evaluate,
    RetryCancel(usize, Ulid),
    CommitCancel,
    Claim(StageWork),
    StartRun(usize),
    CommitRun,
    ReleaseRun,
    RunStage(usize),
    CommitOutcome(bool),
    DropActive(Ulid),
}

pub(crate) enum DispatchPlan {
    StartRun(usize, IoCall),
    RunStage(usize, IoCall),
    Commit(FiringDelta),
}

pub(crate) enum NextAction {
    Finished,
    NextFiring,
    Call(StepPhase, IoCall),
    Tx(StepPhase, FiringDelta),
}

#[derive(Debug, Default, PartialEq)]
pub(crate) struct RunTrack {
    pub disabled_features: BTreeSet<Feature>,
    pub config: Option<RealmConfigDocument>,
    pub queue: Vec<(Ulid, Ulid)>,
    pub active_runs: BTreeMap<Ulid, usize>,
    pub capped: BTreeSet<Ulid>,
    pub cancel_tried: BTreeSet<Ulid>,
}

impl RunTrack {
    pub(crate) fn feature_on(&self, feature: Feature, group_id: GroupId) -> bool {
        self.config
            .as_ref()
            .is_some_and(|cfg| resolve(cfg, &self.disabled_features, feature, Some(&group_id)).on)
    }

    pub(crate) fn collect_active(&mut self, rows: Vec<(Key, Value)>) {
        for (key, value) in rows {
            if let (Some(fid), Some(aid)) =
                (key.get(1..17).and_then(decode_ulid), decode_ulid(&value))
            {
                self.queue.push((fid, aid));
            }
        }
    }

    pub(crate) fn tally_active_runs(&mut self, values: Vec<Option<Value>>) {
        for bytes in values.into_iter().flatten() {
            if let Ok(firing) = Firing::from_bytes(&bytes) {
                *self.active_runs.entry(firing.automation_id).or_default() += firing.active_runs();
            }
        }
    }

    pub(crate) fn count_batch_call(&self, offset: usize) -> Option<(usize, IoCall)> {
        if offset >= self.queue.len() {
            return None;
        }
        let end = (offset + 256).min(self.queue.len());
        let reads = self.queue[offset..end]
            .iter()
            .map(|(fid, aid)| (AUTOMATION_RUNS_KEYSPACE.to_string(), firing_key(*aid, *fid)))
            .collect();
        Some((end, IoCall::Read(reads)))
    }

    pub(crate) fn evaluate_delta(&self, auto: &Automation, firing: &Firing) -> FiringDelta {
        let dispatch = auto.lifecycle == Lifecycle::Active
            && self.feature_on(Feature::Automations, firing.group_id);
        FiringDelta::Evaluate(dispatch)
    }

    pub(crate) fn retry_cancel_action(
        &mut self,
        auto: &Automation,
        firing: &Firing,
        local: (NodeId, RealmId),
        idx: usize,
        run_id: Ulid,
    ) -> NextAction {
        self.cancel_tried.insert(run_id);
        let Some(caller) = automation_auth_for(auto, &firing.spec, local.1) else {
            return NextAction::Tx(
                StepPhase::CommitCancel,
                FiringDelta::CancelReply {
                    idx,
                    run_id,
                    error: Some("automation has no active service account to cancel child".into()),
                },
            );
        };
        let call = IoCall::child(CancelRunOperation::new(&caller, run_id, local));
        NextAction::Call(StepPhase::RetryCancel(idx, run_id), call)
    }

    pub(crate) fn plan_next(
        &mut self,
        auto: &Automation,
        firing: &Firing,
        local: (NodeId, RealmId),
    ) -> NextAction {
        if firing.state.is_terminal() {
            return NextAction::Finished;
        }
        if firing.state == FiringState::Cancelling {
            return match pending_cancel(firing, &self.cancel_tried) {
                Some((idx, run_id)) => self.retry_cancel_action(auto, firing, local, idx, run_id),
                None => NextAction::NextFiring,
            };
        }
        if firing.state != FiringState::Running
            || auto.lifecycle != Lifecycle::Active
            || !self.feature_on(Feature::Automations, firing.group_id)
        {
            return NextAction::NextFiring;
        }
        let used = self
            .active_runs
            .get(&firing.automation_id)
            .copied()
            .unwrap_or(0);
        let can_run = self.feature_on(Feature::Workflows, firing.group_id)
            && !self.capped.contains(&firing.automation_id);
        match next_work(firing, can_run, used) {
            Some((idx, work)) => {
                NextAction::Tx(StepPhase::Claim(work), FiringDelta::ClaimStage(idx))
            }
            None => NextAction::NextFiring,
        }
    }
}

pub(crate) fn in_flight_runs(firing: &Firing) -> Vec<(usize, Ulid)> {
    firing
        .stages
        .iter()
        .enumerate()
        .filter_map(|(i, stage)| {
            (stage.state.is_in_flight()
                && matches!(firing.spec.stages[i].kind, StageKind::Run { .. }))
            .then_some(stage.receipt.workflow_run_id)
            .flatten()
            .map(|id| (i, id))
        })
        .collect()
}

pub(crate) fn build_poll_deltas(
    polling: Vec<(usize, Ulid)>,
    values: Vec<Option<Value>>,
    active_runs: &mut BTreeMap<Ulid, usize>,
    auto_id: Ulid,
) -> Result<Vec<PollDelta>, AutoError> {
    let mut deltas = Vec::new();
    for ((idx, run_id), bytes) in polling.into_iter().zip(values) {
        let delta = match bytes {
            None => PollDelta::Missing { idx, run_id },
            Some(raw) => {
                let rec = WorkflowRunRecord::from_bytes(&raw)?;
                let state = match rec.state {
                    RunState::Succeeded => StageState::Succeeded,
                    RunState::Failed => StageState::Failed,
                    RunState::Cancelled => StageState::Cancelled,
                    _ => continue,
                };
                PollDelta::Finished {
                    idx,
                    run_id,
                    state,
                    outputs: rec.outputs,
                    error: rec.error,
                }
            }
        };
        if let Some(count) = active_runs.get_mut(&auto_id) {
            *count = count.saturating_sub(1);
        }
        deltas.push(delta);
    }
    Ok(deltas)
}

fn pending_cancel(firing: &Firing, tried: &BTreeSet<Ulid>) -> Option<(usize, Ulid)> {
    firing.stages.iter().enumerate().find_map(|(i, stage)| {
        let run_id = stage.receipt.workflow_run_id?;
        let retry = stage.state == StageState::Cancelling
            && !stage.receipt.cancel_sent
            && matches!(firing.spec.stages[i].kind, StageKind::Run { .. })
            && !tried.contains(&run_id);
        retry.then_some((i, run_id))
    })
}

pub(crate) fn first_stranded_claim(firing: &Firing) -> Option<ClaimRecovery> {
    firing.stages.iter().enumerate().find_map(|(idx, stage)| {
        if !stage.state.is_in_flight() {
            return None;
        }
        match &firing.spec.stages[idx].kind {
            StageKind::Run { .. } if stage.receipt.workflow_run_id.is_none() => {
                Some(ClaimRecovery::RunIdem {
                    idx,
                    stage: stage.name.clone(),
                    attempt: firing.attempt,
                    idem_row: None,
                })
            }
            StageKind::Dataset { .. } => Some(ClaimRecovery::StageIntent {
                idx,
                stage_id: stage_intent_id(firing.firing_id, &stage.name),
                is_dataset: true,
            }),
            StageKind::Notify { .. } => Some(ClaimRecovery::StageIntent {
                idx,
                stage_id: stage_intent_id(firing.firing_id, &stage.name),
                is_dataset: false,
            }),
            _ => None,
        }
    })
}

pub(crate) fn claim_recovery_call(
    claim: &mut ClaimRecovery,
    auto: &Automation,
    firing: &Firing,
) -> Option<IoCall> {
    match claim {
        ClaimRecovery::RunIdem {
            stage,
            attempt,
            idem_row,
            ..
        } => {
            let account = auto.account?;
            let key = run_key_row(
                firing.group_id,
                account,
                &run_idempotency_key(firing.firing_id, stage, *attempt),
            );
            *idem_row = Some(key.clone());
            Some(IoCall::Read(vec![(
                RUN_IDEMPOTENCY_KEYSPACE.to_string(),
                key,
            )]))
        }
        ClaimRecovery::StageIntent { stage_id, .. } => Some(IoCall::Read(vec![(
            AUTOMATION_INTENTS_KEYSPACE.to_string(),
            Key::from(intent_key(*stage_id, 0)),
        )])),
    }
}

pub(crate) fn resolve_claim_recovery(
    claim: ClaimRecovery,
    firing: &Firing,
    local: (NodeId, RealmId),
    result: IoResult,
    track: &mut RunTrack,
) -> Result<RecoveryStep, AutoError> {
    let bytes = result.value()?;
    match claim {
        ClaimRecovery::RunIdem { idx, idem_row, .. } => {
            if let Some(raw) = bytes {
                let rec = RunKeyRecord::from_bytes(&raw)?;
                let cancelling = firing.state == FiringState::Cancelling
                    || firing.stages[idx].state == StageState::Cancelling;
                let op = ReconcileRunOperation::new(rec.run_id, local, cancelling, idem_row);
                return Ok(RecoveryStep::ReconcileRun(idx, IoCall::child(op)));
            }
            if let Some(count) = track.active_runs.get_mut(&firing.automation_id) {
                *count = count.saturating_sub(1);
            }
            Ok(RecoveryStep::Commit(firing_tx_call(
                firing,
                FiringDelta::ReleaseStage(idx),
            )))
        }
        ClaimRecovery::StageIntent {
            idx,
            stage_id,
            is_dataset,
        } => {
            let Some(raw) = bytes else {
                return Ok(RecoveryStep::Commit(firing_tx_call(
                    firing,
                    FiringDelta::ReleaseStage(idx),
                )));
            };
            let mut rec =
                decode::<IntentRecord>(&raw).map_err(|e| AutoError::Storage(e.detail()))?;
            let cancelling = firing.state == FiringState::Cancelling
                || firing.stages[idx].state == StageState::Cancelling;
            match rec.state {
                IntentState::Settled { reply } => {
                    let outcome = if is_dataset {
                        let doc_id = reply
                            .split_whitespace()
                            .next()
                            .and_then(|s| Ulid::from_string(s).ok())
                            .ok_or_else(|| AutoError::Storage("invalid dataset receipt".into()))?;
                        StageOutcome::Dataset(doc_id)
                    } else {
                        StageOutcome::Notified(reply.parse::<u32>().unwrap_or(0))
                    };
                    Ok(RecoveryStep::Commit(firing_tx_call(
                        firing,
                        FiringDelta::StageOutcome { idx, outcome },
                    )))
                }
                IntentState::Pending => {
                    let msg = "an earlier attempt of this call did not finish".to_string();
                    rec.state = IntentState::Unknown {
                        reason: msg.clone(),
                    };
                    let encoded = encode(&rec).map_err(|e| AutoError::Storage(e.detail()))?;
                    let extra = vec![row(
                        AUTOMATION_INTENTS_KEYSPACE,
                        intent_key(stage_id, 0),
                        encoded,
                    )];
                    let outcome = if cancelling {
                        StageOutcome::Failed(msg)
                    } else {
                        StageOutcome::NeedsAttention(msg, None)
                    };
                    let op = FiringTxOp::new(
                        firing.automation_id,
                        firing.firing_id,
                        FiringDelta::StageOutcome { idx, outcome },
                    )
                    .with_extras(extra, Vec::new());
                    Ok(RecoveryStep::Commit(IoCall::retry(
                        op,
                        &firing.firing_id.to_bytes(),
                    )))
                }
                IntentState::Unknown { reason } => {
                    let outcome = if cancelling {
                        StageOutcome::Failed(reason)
                    } else {
                        StageOutcome::NeedsAttention(reason, None)
                    };
                    Ok(RecoveryStep::Commit(firing_tx_call(
                        firing,
                        FiringDelta::StageOutcome { idx, outcome },
                    )))
                }
                IntentState::Resolved { .. } => {
                    let msg = "stage intent already resolved".to_string();
                    let outcome = if cancelling {
                        StageOutcome::Failed(msg)
                    } else {
                        StageOutcome::NeedsAttention(msg, None)
                    };
                    Ok(RecoveryStep::Commit(firing_tx_call(
                        firing,
                        FiringDelta::StageOutcome { idx, outcome },
                    )))
                }
            }
        }
    }
}

pub(crate) fn resolve_reconciled_run(
    idx: usize,
    firing: &Firing,
    result: IoResult,
    track: &mut RunTrack,
) -> Result<ReconcileAction, AutoError> {
    let reconciled = result
        .child::<ReconciledRun, CatalogError>()?
        .map_err(|err| AutoError::Storage(err.to_string()))?;
    match reconciled {
        ReconciledRun::Ready { run_id, terminal } => Ok(ReconcileAction::Commit(
            FiringDelta::RunStarted {
                idx,
                outcome: Ok(run_id),
            },
            terminal,
        )),
        ReconciledRun::Missing(run_id) => {
            if let Some(count) = track.active_runs.get_mut(&firing.automation_id) {
                *count = count.saturating_sub(1);
            }
            Ok(ReconcileAction::Commit(
                FiringDelta::Poll(vec![PollDelta::Missing { idx, run_id }]),
                true,
            ))
        }
        ReconciledRun::Capacity => {
            track.capped.insert(firing.automation_id);
            if let Some(count) = track.active_runs.get_mut(&firing.automation_id) {
                *count = count.saturating_sub(1);
            }
            Ok(ReconcileAction::Commit(
                FiringDelta::ReleaseStage(idx),
                false,
            ))
        }
        ReconciledRun::Deferred => Ok(ReconcileAction::Defer),
    }
}

pub(crate) fn cancel_reply_outcome(
    idx: usize,
    run_id: Ulid,
    result: IoResult,
) -> Result<CancelRetryOutcome, AutoError> {
    match result.child::<CancelledRun, CatalogError>()? {
        Ok(cancelled) => Ok(CancelRetryOutcome::Ack {
            running: cancelled.running,
            woken: Some(cancelled.record.run_id),
        }),
        Err(CatalogError::NotFound) => Ok(CancelRetryOutcome::Ack {
            running: Vec::new(),
            woken: None,
        }),
        Err(err) => Ok(CancelRetryOutcome::RecordError(FiringDelta::CancelReply {
            idx,
            run_id,
            error: Some(format!("child cancel retry failed: {err}")),
        })),
    }
}

fn next_work(firing: &Firing, can_run: bool, used_runs: usize) -> Option<(usize, StageWork)> {
    let slots =
        (firing.spec.scope.limits.max_concurrent_workflows as usize).saturating_sub(used_runs);
    if can_run && let Some(&idx) = firing.ready_runs(slots).first() {
        return Some((idx, StageWork::Run(idx)));
    }
    let find = |want: StageState, pred: fn(&StageKind) -> bool| {
        firing.stages.iter().enumerate().find_map(|(i, stage)| {
            (stage.state == want && pred(&firing.spec.stages[i].kind)).then_some(i)
        })
    };
    if let Some(idx) = find(StageState::Running, |k| matches!(k, StageKind::Copy { .. }))
        .or_else(|| firing.next_copy())
    {
        return Some((idx, StageWork::Copy(idx)));
    }
    if let Some(idx) = find(StageState::Ready, |k| {
        matches!(k, StageKind::Dataset { .. })
    }) {
        return Some((idx, StageWork::Dataset(idx)));
    }
    find(StageState::Ready, |k| matches!(k, StageKind::Notify { .. }))
        .map(|idx| (idx, StageWork::Notify(idx)))
}

pub(crate) fn build_stage_dispatch(
    work: StageWork,
    auto: Automation,
    firing: Firing,
    local: (NodeId, RealmId),
) -> Result<DispatchPlan, AutoError> {
    match work {
        StageWork::Run(idx) => match StartStageRun::new(auto, firing, idx, local) {
            Ok(op) => Ok(DispatchPlan::StartRun(idx, IoCall::child(op))),
            Err(err) => Ok(DispatchPlan::Commit(FiringDelta::RunStarted {
                idx,
                outcome: Err(err.to_string()),
            })),
        },
        StageWork::Copy(idx) => Ok(DispatchPlan::RunStage(
            idx,
            IoCall::child(CopyStageOp::new(auto, firing, idx, local)?),
        )),
        StageWork::Dataset(idx) => Ok(DispatchPlan::RunStage(
            idx,
            IoCall::child(DatasetStageOp::new(auto, firing, idx, local)?),
        )),
        StageWork::Notify(idx) => Ok(DispatchPlan::RunStage(
            idx,
            IoCall::child(NotifyStageOp::new(auto, &firing, idx, local.1)?),
        )),
    }
}

pub(crate) fn load_pair_call(auto_id: Ulid, firing_id: Ulid) -> IoCall {
    IoCall::Read(vec![
        (AUTOMATIONS_KEYSPACE.to_string(), record_key(auto_id)),
        (
            AUTOMATION_RUNS_KEYSPACE.to_string(),
            firing_key(auto_id, firing_id),
        ),
    ])
}

pub(crate) fn poll_reads_call(polling: &[(usize, Ulid)]) -> IoCall {
    let reads = polling
        .iter()
        .map(|(_, id)| (WORKFLOW_RUN_KEYSPACE.to_string(), run_key(*id)))
        .collect();
    IoCall::Read(reads)
}

pub(crate) fn start_run_delta(
    idx: usize,
    aid: Ulid,
    result: IoResult,
    track: &mut RunTrack,
) -> Result<(FiringDelta, StepPhase), AutoError> {
    match result.child::<Ulid, AutoError>()? {
        Ok(run_id) => {
            *track.active_runs.entry(aid).or_default() += 1;
            Ok((
                FiringDelta::RunStarted {
                    idx,
                    outcome: Ok(run_id),
                },
                StepPhase::CommitRun,
            ))
        }
        Err(AutoError::Capacity(_)) => {
            track.capped.insert(aid);
            Ok((FiringDelta::ReleaseStage(idx), StepPhase::ReleaseRun))
        }
        Err(err) => Ok((
            FiringDelta::RunStarted {
                idx,
                outcome: Err(err.to_string()),
            },
            StepPhase::CommitRun,
        )),
    }
}

pub(crate) fn stage_outcome_delta(
    idx: usize,
    result: IoResult,
) -> Result<(FiringDelta, bool), AutoError> {
    let outcome = result
        .child::<StageOutcome, AutoError>()?
        .unwrap_or_else(|err| StageOutcome::Failed(err.to_string()));
    let stop = matches!(outcome, StageOutcome::Progress(_));
    Ok((FiringDelta::StageOutcome { idx, outcome }, stop))
}

pub(crate) fn firing_tx_call(firing: &Firing, delta: FiringDelta) -> IoCall {
    let op = FiringTxOp::new(firing.automation_id, firing.firing_id, delta);
    IoCall::retry(op, &firing.firing_id.to_bytes())
}

pub(crate) fn drop_active_call(firing_id: Ulid) -> IoCall {
    IoCall::Delete(vec![(
        AUTOMATION_RUNS_KEYSPACE.to_string(),
        active_key(firing_id),
    )])
}

pub(crate) fn decode_pair(
    values: Vec<Option<Value>>,
) -> Result<Option<(Automation, Firing)>, AutoError> {
    let (Some(auto_bytes), Some(firing_bytes)) = (
        values.first().and_then(Option::as_ref),
        values.get(1).and_then(Option::as_ref),
    ) else {
        return Ok(None);
    };
    Ok(Some((
        Automation::from_bytes(auto_bytes)?,
        Firing::from_bytes(firing_bytes)?,
    )))
}
