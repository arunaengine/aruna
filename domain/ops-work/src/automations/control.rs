//! Firing control operations: async cancel, audited resolution, safe resume and stage approval.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::{CopiedFile, Firing, FiringState, StageState};
use aruna_core::automation::keys::copy_plan_key;
use aruna_core::automation::record::{Automation, StageKind};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTOMATION_INTENTS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Key, KeySpace, Value};
use aruna_ops_data::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use aruna_ops_identity::groups::admin_check::AdminCheckOperation;
use ulid::Ulid;

use super::copy_stage::{source_count, source_length, stage_intent_id};
use super::firing_reads::GetFiringOperation;
pub(crate) use super::firing_tx::{FiringDelta, FiringTxOp, FiringTxResult};
use super::{AutoAccessOperation, AutoError, Need, automation_auth_for};
use crate::plugins::intents::{IntentRecord, IntentState, intent_scan_in, resolved_rows_in};
use crate::plugins::rows::{Rows, StoreError, decode, encode, row, scan_rows};
use crate::workflows::CatalogError;
use crate::workflows::runs::cancel::{CancelRunOperation, CancelledRun};

/// Result of cancelling a firing, including local job and coordinator run IDs to signal.
#[derive(Clone, Debug, PartialEq)]
pub struct CancelledFiring {
    pub firing: Firing,
    pub running_jobs: Vec<JobId>,
    pub woken_runs: Vec<Ulid>,
}

#[derive(Debug, PartialEq)]
enum CancelStep {
    Load,
    Access,
    Mark,
    ChildRun(usize, Ulid),
    RecordReply,
}

/// Moves a firing into `Cancelling`, persists it atomically, and propagates cancellation to every
/// active child `WorkflowRunRecord`, returning local running job IDs and run IDs to signal.
#[derive(Debug, PartialEq)]
pub struct CancelFiringOperation {
    auth: AuthContext,
    firing_id: Ulid,
    local: (NodeId, RealmId),
    auto: Option<Automation>,
    firing: Option<Firing>,
    child_runs: Vec<(usize, Ulid)>,
    running_jobs: Vec<JobId>,
    woken_runs: Vec<Ulid>,
    step: CancelStep,
}

impl CancelFiringOperation {
    pub fn new(auth: &AuthContext, firing_id: Ulid, local: (NodeId, RealmId)) -> Self {
        Self {
            auth: auth.clone(),
            firing_id,
            local,
            auto: None,
            firing: None,
            child_runs: Vec::new(),
            running_jobs: Vec::new(),
            woken_runs: Vec::new(),
            step: CancelStep::Load,
        }
    }

    fn next_child(&mut self) -> Result<Next<CancelledFiring>, AutoError> {
        let Some((idx, run_id)) = self.child_runs.pop() else {
            return Ok(Next::Done(CancelledFiring {
                firing: self.firing.take().ok_or(IoError::Unfinished)?,
                running_jobs: std::mem::take(&mut self.running_jobs),
                woken_runs: std::mem::take(&mut self.woken_runs),
            }));
        };
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
        let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
        let caller = automation_auth_for(auto, &firing.spec, self.local.1)
            .unwrap_or_else(|| self.auth.clone());
        self.step = CancelStep::ChildRun(idx, run_id);
        Ok(Next::Call(IoCall::child(CancelRunOperation::new(
            &caller, run_id, self.local,
        ))))
    }
}

impl Operation for CancelFiringOperation {
    type Output = CancelledFiring;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<CancelledFiring>, AutoError> {
        let get = GetFiringOperation::new(&self.auth, self.firing_id, self.local.0);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CancelledFiring>, AutoError> {
        match std::mem::replace(&mut self.step, CancelStep::Load) {
            CancelStep::Load => {
                let (_, auto) = result.child::<(Firing, Automation), AutoError>()??;
                self.step = CancelStep::Access;
                Ok(Next::Call(write_access(
                    &self.auth,
                    auto.automation_id,
                    self.local.0,
                )))
            }
            CancelStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                let tx =
                    FiringTxOp::new(auto.automation_id, self.firing_id, FiringDelta::BeginCancel);
                self.auto = Some(auto);
                self.step = CancelStep::Mark;
                Ok(Next::Call(IoCall::retry(tx, &self.firing_id.to_bytes())))
            }
            CancelStep::Mark => {
                let tx = result.child::<FiringTxResult, AutoError>()??;
                self.firing = Some(tx.firing);
                self.child_runs = tx.child_runs;
                self.next_child()
            }
            CancelStep::ChildRun(idx, run_id) => {
                let err = match result.child::<CancelledRun, CatalogError>()? {
                    Ok(cancelled) => {
                        self.running_jobs.extend(cancelled.running);
                        self.woken_runs.push(cancelled.record.run_id);
                        None
                    }
                    Err(CatalogError::NotFound) => None,
                    Err(e) => Some(format!("child cancel pending: {e}")),
                };
                let auto_id = self.auto.as_ref().ok_or(IoError::Unfinished)?.automation_id;
                let tx = FiringTxOp::new(
                    auto_id,
                    self.firing_id,
                    FiringDelta::CancelReply {
                        idx,
                        run_id,
                        error: err,
                    },
                );
                self.step = CancelStep::RecordReply;
                Ok(Next::Call(IoCall::retry(tx, &self.firing_id.to_bytes())))
            }
            CancelStep::RecordReply => {
                let tx = result.child::<FiringTxResult, AutoError>()??;
                self.firing = Some(tx.firing);
                self.next_child()
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

#[derive(Debug, PartialEq)]
enum ResolveStep {
    Load,
    Access,
    ScanIntents,
    LoadPlan(Option<(Vec<u8>, IntentRecord, u32)>),
    HeadDest(Option<(Vec<u8>, IntentRecord, u32)>, Box<CopiedFile>),
    Commit,
}

/// Records an audited human decision (`done: true | false`) for a `NeedsAttention` stage, verifying
/// planned copy destinations and committing intent and firing updates in one transaction.
#[derive(Debug, PartialEq)]
pub struct ResolveStageOperation {
    auth: AuthContext,
    firing_id: Ulid,
    stage: String,
    done: bool,
    note: Option<String>,
    local: NodeId,
    auto_id: Option<Ulid>,
    stage_id: Option<Ulid>,
    copy_total: Option<usize>,
    planned: Option<CopiedFile>,
    step: ResolveStep,
}

impl ResolveStageOperation {
    pub fn new(
        auth: &AuthContext,
        firing_id: Ulid,
        stage: String,
        done: bool,
        note: Option<String>,
        local: NodeId,
    ) -> Self {
        Self {
            auth: auth.clone(),
            firing_id,
            stage,
            done,
            note,
            local,
            auto_id: None,
            stage_id: None,
            copy_total: None,
            planned: None,
            step: ResolveStep::Load,
        }
    }

    fn commit_with(
        &mut self,
        writes: Vec<(KeySpace, Key, Value)>,
        deletes: Vec<(KeySpace, Key)>,
    ) -> Result<Next<Firing>, AutoError> {
        let auto_id = self.auto_id.ok_or(IoError::Unfinished)?;
        let copy_progress = self.copy_total.map(|total| (self.planned.clone(), total));
        let tx = FiringTxOp::new(
            auto_id,
            self.firing_id,
            FiringDelta::Resolve {
                stage: self.stage.clone(),
                done: self.done,
                copy_progress,
                actor: self.auth.user_id,
                note: self.note.clone(),
            },
        )
        .with_extras(writes, deletes);
        self.step = ResolveStep::Commit;
        Ok(Next::Call(IoCall::retry(tx, &self.firing_id.to_bytes())))
    }

    fn head_planned(
        &mut self,
        open: Option<(Vec<u8>, IntentRecord, u32)>,
        planned: CopiedFile,
    ) -> Next<Firing> {
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: planned.dest_bucket.clone(),
            key: planned.dest_key.clone(),
            version_id: Some(planned.dest_version),
        });
        self.step = ResolveStep::HeadDest(open, Box::new(planned));
        Next::Call(IoCall::child(head))
    }
}

fn intent_ordinal(key: &[u8]) -> Option<u32> {
    key.get(16..20)
        .and_then(|s| <[u8; 4]>::try_from(s).ok())
        .map(u32::from_be_bytes)
}

impl Operation for ResolveStageOperation {
    type Output = Firing;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Firing>, AutoError> {
        let get = GetFiringOperation::new(&self.auth, self.firing_id, self.local);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Firing>, AutoError> {
        match std::mem::replace(&mut self.step, ResolveStep::Load) {
            ResolveStep::Load => {
                let (firing, auto) = result.child::<(Firing, Automation), AutoError>()??;
                if firing.state == FiringState::Cancelling || firing.state.is_terminal() {
                    return Err(AutoError::Conflict(format!(
                        "firing is already {}",
                        firing.state.as_str()
                    )));
                }
                let idx = firing.stage_index(&self.stage).ok_or_else(|| {
                    AutoError::Conflict(format!("unknown stage `{}`", self.stage))
                })?;
                if firing.stages[idx].state != StageState::NeedsAttention {
                    return Err(AutoError::Conflict(format!(
                        "stage `{}` is not in needs_attention",
                        self.stage
                    )));
                }
                if let StageKind::Copy {
                    from_stage, file, ..
                } = &firing.spec.stages[idx].kind
                {
                    self.copy_total = Some(source_count(&firing, from_stage, file.as_deref()));
                    self.planned = firing.stages[idx].receipt.pending_copy.clone();
                }
                self.auto_id = Some(auto.automation_id);
                self.stage_id = Some(stage_intent_id(firing.firing_id, &self.stage));
                self.step = ResolveStep::Access;
                Ok(Next::Call(write_access(
                    &self.auth,
                    auto.automation_id,
                    self.local,
                )))
            }
            ResolveStep::Access => {
                let _ = result.child::<Automation, AutoError>()??;
                let stage_id = self.stage_id.ok_or(IoError::Unfinished)?;
                self.step = ResolveStep::ScanIntents;
                Ok(Next::Call(IoCall::child(scan_rows(intent_scan_in(
                    AUTOMATION_INTENTS_KEYSPACE,
                    stage_id,
                )))))
            }
            ResolveStep::ScanIntents => {
                let rows = result
                    .child::<Rows, StoreError>()?
                    .map_err(|e| AutoError::Storage(e.detail()))?;
                let stage_id = self.stage_id.ok_or(IoError::Unfinished)?;
                if self.copy_total.is_some() {
                    let mut open_rows = Vec::new();
                    for (key, value) in &rows {
                        let rec = decode::<IntentRecord>(value)
                            .map_err(|e| AutoError::Storage(e.detail()))?;
                        if matches!(
                            rec.state,
                            IntentState::Pending | IntentState::Unknown { .. }
                        ) && let Some(ord) = intent_ordinal(key)
                        {
                            open_rows.push((key.clone(), rec, ord));
                        }
                    }
                    if !self.done {
                        let mut deletes = Vec::new();
                        for (key, _, ord) in open_rows {
                            deletes.push((AUTOMATION_INTENTS_KEYSPACE.to_string(), Key::from(key)));
                            deletes.push((
                                AUTOMATION_RUNS_KEYSPACE.to_string(),
                                copy_plan_key(stage_id, ord),
                            ));
                        }
                        return self.commit_with(Vec::new(), deletes);
                    }
                    let open = open_rows.into_iter().next();
                    if let Some(planned) = self.planned.clone() {
                        return Ok(self.head_planned(open, planned));
                    }
                    if let Some((_, _, ord)) = open.as_ref() {
                        let ord = *ord;
                        self.step = ResolveStep::LoadPlan(open);
                        return Ok(Next::Call(IoCall::Read(vec![(
                            AUTOMATION_RUNS_KEYSPACE.to_string(),
                            copy_plan_key(stage_id, ord),
                        )])));
                    }
                    return Err(AutoError::Conflict(
                        "copy plan missing; resolve as done=false to retry".into(),
                    ));
                }
                if !self.done {
                    let mut deletes = Vec::new();
                    for (key, value) in rows {
                        let rec = decode::<IntentRecord>(&value)
                            .map_err(|e| AutoError::Storage(e.detail()))?;
                        if matches!(
                            rec.state,
                            IntentState::Pending
                                | IntentState::Unknown { .. }
                                | IntentState::Resolved { done: false }
                        ) {
                            deletes.push((AUTOMATION_INTENTS_KEYSPACE.to_string(), Key::from(key)));
                        }
                    }
                    return self.commit_with(Vec::new(), deletes);
                }
                let writes = resolved_rows_in(AUTOMATION_INTENTS_KEYSPACE, rows, true)
                    .map_err(|e| AutoError::Storage(e.detail()))?;
                self.commit_with(writes, Vec::new())
            }
            ResolveStep::LoadPlan(open) => {
                if let Some(bytes) = result.value()? {
                    self.planned = postcard::from_bytes::<CopiedFile>(&bytes).ok();
                }
                let Some(planned) = self.planned.clone() else {
                    return Err(AutoError::Conflict(
                        "copy plan missing; resolve as done=false to retry".into(),
                    ));
                };
                Ok(self.head_planned(open, planned))
            }
            ResolveStep::HeadDest(open, mut planned) => {
                let head = match result.child::<HeadObjectResult, HeadObjectError>()? {
                    Ok(head) => head,
                    Err(
                        HeadObjectError::NoSuchKey
                        | HeadObjectError::NoSuchVersion
                        | HeadObjectError::DeleteMarker,
                    ) => {
                        return Err(AutoError::Conflict(
                            "planned copy destination version not found; resolve as done=false to retry"
                                .into(),
                        ));
                    }
                    Err(err) => {
                        return Err(AutoError::Storage(format!(
                            "failed to verify planned copy destination: {err}"
                        )));
                    }
                };
                let matched_ver = head.resolved_version_id == Some(planned.dest_version)
                    || head.version_id == Some(planned.dest_version);
                if !matched_ver {
                    return Err(AutoError::Conflict(
                        "destination object version does not match planned copy".into(),
                    ));
                }
                if let Some(bytes) = source_length(&head) {
                    planned.bytes = planned.bytes.max(bytes);
                }
                let stage_id = self.stage_id.ok_or(IoError::Unfinished)?;
                let mut writes = Vec::new();
                let mut deletes = Vec::new();
                if let Some((key, mut record, ordinal)) = open {
                    record.state = IntentState::Settled {
                        reply: serde_json::to_string(&*planned)
                            .map_err(|e| AutoError::Storage(e.to_string()))?,
                    };
                    let encoded = encode(&record).map_err(|e| AutoError::Storage(e.detail()))?;
                    writes.push(row(AUTOMATION_INTENTS_KEYSPACE, key, encoded));
                    deletes.push((
                        AUTOMATION_RUNS_KEYSPACE.to_string(),
                        copy_plan_key(stage_id, ordinal),
                    ));
                }
                self.planned = Some(*planned);
                self.commit_with(writes, deletes)
            }
            ResolveStep::Commit => {
                let tx = result.child::<FiringTxResult, AutoError>()??;
                Ok(Next::Done(tx.firing))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

#[derive(Debug, PartialEq)]
enum ResumeStep {
    Load,
    Access,
    Commit,
}

/// Resumes a `Failed` firing, refusing unresolved `NeedsAttention` stages and preserving every
/// already-`Succeeded` stage receipt.
#[derive(Debug, PartialEq)]
pub struct ResumeFiringOperation {
    auth: AuthContext,
    firing_id: Ulid,
    local: NodeId,
    step: ResumeStep,
}

impl ResumeFiringOperation {
    pub fn new(auth: &AuthContext, firing_id: Ulid, local: NodeId) -> Self {
        Self {
            auth: auth.clone(),
            firing_id,
            local,
            step: ResumeStep::Load,
        }
    }
}

impl Operation for ResumeFiringOperation {
    type Output = Firing;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Firing>, AutoError> {
        let get = GetFiringOperation::new(&self.auth, self.firing_id, self.local);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Firing>, AutoError> {
        match std::mem::replace(&mut self.step, ResumeStep::Load) {
            ResumeStep::Load => {
                let (_, auto) = result.child::<(Firing, Automation), AutoError>()??;
                self.step = ResumeStep::Access;
                Ok(Next::Call(write_access(
                    &self.auth,
                    auto.automation_id,
                    self.local,
                )))
            }
            ResumeStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                let tx = FiringTxOp::new(auto.automation_id, self.firing_id, FiringDelta::Resume);
                self.step = ResumeStep::Commit;
                Ok(Next::Call(IoCall::retry(
                    tx,
                    &auto.automation_id.to_bytes(),
                )))
            }
            ResumeStep::Commit => {
                let tx = result.child::<FiringTxResult, AutoError>()??;
                Ok(Next::Done(tx.firing))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

#[derive(Debug, PartialEq)]
enum DecideStep {
    Load,
    Admin(Ulid),
    Commit,
}

/// Human group admin approval or rejection of a `WaitingForApproval` stage, verifying expiry inside
/// the same transaction that commits the decision.
#[derive(Debug, PartialEq)]
pub struct DecideStageOperation {
    auth: AuthContext,
    firing_id: Ulid,
    stage: String,
    approve: bool,
    note: Option<String>,
    local: NodeId,
    step: DecideStep,
}

impl DecideStageOperation {
    pub fn new(
        auth: &AuthContext,
        firing_id: Ulid,
        stage: String,
        approve: bool,
        note: Option<String>,
        local: NodeId,
    ) -> Self {
        Self {
            auth: auth.clone(),
            firing_id,
            stage,
            approve,
            note,
            local,
            step: DecideStep::Load,
        }
    }
}

impl Operation for DecideStageOperation {
    type Output = Firing;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Firing>, AutoError> {
        let get = GetFiringOperation::new(&self.auth, self.firing_id, self.local);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Firing>, AutoError> {
        match std::mem::replace(&mut self.step, DecideStep::Load) {
            DecideStep::Load => {
                let (_, auto) = result.child::<(Firing, Automation), AutoError>()??;
                let check = AdminCheckOperation::new(self.auth.clone(), auto.group_id);
                self.step = DecideStep::Admin(auto.automation_id);
                Ok(Next::Call(IoCall::child(check)))
            }
            DecideStep::Admin(auto_id) => {
                let is_admin = result
                    .child::<bool, AuthorizationError>()?
                    .map_err(|e| AutoError::Storage(e.to_string()))?;
                if !is_admin {
                    return Err(AutoError::Forbidden);
                }
                let tx = FiringTxOp::new(
                    auto_id,
                    self.firing_id,
                    FiringDelta::Decide {
                        stage: self.stage.clone(),
                        approve: self.approve,
                        actor: self.auth.user_id,
                        note: self.note.clone(),
                    },
                );
                self.step = DecideStep::Commit;
                Ok(Next::Call(IoCall::retry(tx, &self.firing_id.to_bytes())))
            }
            DecideStep::Commit => {
                let tx = result.child::<FiringTxResult, AutoError>()??;
                Ok(Next::Done(tx.firing))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

fn write_access(auth: &AuthContext, auto_id: Ulid, local: NodeId) -> IoCall {
    IoCall::child(AutoAccessOperation::new(auth, auto_id, local, Need::Write))
}
