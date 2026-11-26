//! Firing control operations: async cancel, audited resolution, safe resume and stage approval.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::{Firing, FiringState, StageState};
use aruna_core::automation::keys::active_key;
use aruna_core::automation::record::{Automation, StageKind};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTOMATION_INTENTS_KEYSPACE, AUTOMATION_RUNS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Key, KeySpace};
use aruna_ops_identity::groups::admin_check::AdminCheckOperation;
use byteview::ByteView;
use ulid::Ulid;

use super::copy_stage::stage_intent_id;
use super::firing_reads::GetFiringOperation;
use super::{AutoAccessOperation, AutoError, Need, automation_auth, firing_row};
use crate::plugins::intents::{intent_scan_in, resolved_rows_in, undone_keys_in};
use crate::plugins::rows::{Rows, StoreError, delete_call, scan_rows};
use crate::workflows::CatalogError;
use crate::workflows::runs::cancel::{CancelRunOperation, CancelledRun};

#[derive(Debug, PartialEq)]
enum CancelStep {
    Load,
    Access(Box<Firing>),
    Write,
    DropActive,
    ChildRun,
}

/// Moves a firing into `Cancelling`, cancels pending/waiting stages, persists the firing, and
/// propagates cancellation to every active child `WorkflowRunRecord`.
#[derive(Debug, PartialEq)]
pub struct CancelFiringOperation {
    auth: AuthContext,
    firing_id: Ulid,
    local: (NodeId, RealmId),
    now_ms: u64,
    auto: Option<Automation>,
    firing: Option<Firing>,
    child_runs: Vec<Ulid>,
    step: CancelStep,
}

impl CancelFiringOperation {
    pub fn new(auth: &AuthContext, firing_id: Ulid, local: (NodeId, RealmId)) -> Self {
        Self {
            auth: auth.clone(),
            firing_id,
            local,
            now_ms: 0,
            auto: None,
            firing: None,
            child_runs: Vec::new(),
            step: CancelStep::Load,
        }
    }

    fn next_child(&mut self) -> Result<Next<Firing>, AutoError> {
        let Some(run_id) = self.child_runs.pop() else {
            return Ok(Next::Done(self.firing.take().ok_or(IoError::Unfinished)?));
        };
        let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
        let caller = automation_auth(auto, self.local.1).unwrap_or_else(|| self.auth.clone());
        self.step = CancelStep::ChildRun;
        Ok(Next::Call(IoCall::child(CancelRunOperation::new(
            &caller, run_id, self.local,
        ))))
    }
}

impl Operation for CancelFiringOperation {
    type Output = Firing;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Firing>, AutoError> {
        let get = GetFiringOperation::new(&self.auth, self.firing_id, self.local.0);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Firing>, AutoError> {
        match std::mem::replace(&mut self.step, CancelStep::Load) {
            CancelStep::Load => {
                let (firing, auto) = result.child::<(Firing, Automation), AutoError>()??;
                self.step = CancelStep::Access(Box::new(firing));
                Ok(Next::Call(write_access(
                    &self.auth,
                    auto.automation_id,
                    self.local.0,
                )))
            }
            CancelStep::Access(mut firing) => {
                let auto = result.child::<Automation, AutoError>()??;
                self.child_runs = firing.begin_cancel(&auto.spec, self.now_ms);
                let row = firing_row(&firing)?;
                self.auto = Some(auto);
                self.firing = Some(*firing);
                self.step = CancelStep::Write;
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            CancelStep::Write => {
                result.done()?;
                let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
                if firing.state == FiringState::Cancelled {
                    self.step = CancelStep::DropActive;
                    return Ok(Next::Call(drop_active(active_key(firing.firing_id))));
                }
                self.next_child()
            }
            CancelStep::DropActive => {
                result.done()?;
                self.next_child()
            }
            CancelStep::ChildRun => {
                let _ = result.child::<CancelledRun, CatalogError>()?;
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
    Access(Box<Firing>),
    ScanIntents,
    WriteIntents(Rows),
    DropUndone,
    WriteFiring,
    SyncActive,
}

/// Records an audited human decision (`done: true | false`) for a `NeedsAttention` stage and
/// settles its intent rows in `AUTOMATION_INTENTS_KEYSPACE`.
#[derive(Debug, PartialEq)]
pub struct ResolveStageOperation {
    auth: AuthContext,
    firing_id: Ulid,
    stage: String,
    done: bool,
    note: Option<String>,
    local: NodeId,
    now_ms: u64,
    firing: Option<Firing>,
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
            now_ms: 0,
            firing: None,
            step: ResolveStep::Load,
        }
    }

    fn write_firing(&mut self) -> Result<Next<Firing>, AutoError> {
        let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
        let row = firing_row(firing)?;
        self.step = ResolveStep::WriteFiring;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for ResolveStageOperation {
    type Output = Firing;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Firing>, AutoError> {
        let get = GetFiringOperation::new(&self.auth, self.firing_id, self.local);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Firing>, AutoError> {
        match std::mem::replace(&mut self.step, ResolveStep::Load) {
            ResolveStep::Load => {
                let (firing, auto) = result.child::<(Firing, Automation), AutoError>()??;
                self.step = ResolveStep::Access(Box::new(firing));
                Ok(Next::Call(write_access(
                    &self.auth,
                    auto.automation_id,
                    self.local,
                )))
            }
            ResolveStep::Access(mut firing) => {
                let auto = result.child::<Automation, AutoError>()??;
                firing
                    .resolve(
                        &auto.spec,
                        &self.stage,
                        self.done,
                        self.auth.user_id,
                        self.note.clone(),
                        self.now_ms,
                    )
                    .map_err(AutoError::Conflict)?;
                let stage_id = stage_intent_id(firing.firing_id, &self.stage);
                self.firing = Some(*firing);
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
                let updates =
                    resolved_rows_in(AUTOMATION_INTENTS_KEYSPACE, rows.clone(), self.done)
                        .map_err(|e| AutoError::Storage(e.detail()))?;
                if updates.is_empty() {
                    return self.write_firing();
                }
                self.step = ResolveStep::WriteIntents(rows);
                Ok(Next::Call(IoCall::Write(updates)))
            }
            ResolveStep::WriteIntents(rows) => {
                result.done()?;
                if !self.done {
                    let keys = undone_keys_in(AUTOMATION_INTENTS_KEYSPACE, rows)
                        .map_err(|e| AutoError::Storage(e.detail()))?;
                    if !keys.is_empty() {
                        self.step = ResolveStep::DropUndone;
                        return Ok(Next::Call(delete_call(keys)));
                    }
                }
                self.write_firing()
            }
            ResolveStep::DropUndone => {
                result.done()?;
                self.write_firing()
            }
            ResolveStep::WriteFiring => {
                result.done()?;
                let firing = self.firing.as_ref().ok_or(IoError::Unfinished)?;
                self.step = ResolveStep::SyncActive;
                if firing.state.is_terminal() {
                    return Ok(Next::Call(drop_active(active_key(firing.firing_id))));
                }
                Ok(Next::Call(IoCall::Write(vec![active_row(firing)])))
            }
            ResolveStep::SyncActive => {
                result.done()?;
                Ok(Next::Done(self.firing.take().ok_or(IoError::Unfinished)?))
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
    Access(Box<Firing>),
    Write(Box<Firing>),
}

/// Resumes a `Failed` firing, refusing unresolved `NeedsAttention` stages and preserving every
/// already-`Succeeded` stage receipt.
#[derive(Debug, PartialEq)]
pub struct ResumeFiringOperation {
    auth: AuthContext,
    firing_id: Ulid,
    local: NodeId,
    now_ms: u64,
    step: ResumeStep,
}

impl ResumeFiringOperation {
    pub fn new(auth: &AuthContext, firing_id: Ulid, local: NodeId) -> Self {
        Self {
            auth: auth.clone(),
            firing_id,
            local,
            now_ms: 0,
            step: ResumeStep::Load,
        }
    }
}

impl Operation for ResumeFiringOperation {
    type Output = Firing;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Firing>, AutoError> {
        let get = GetFiringOperation::new(&self.auth, self.firing_id, self.local);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Firing>, AutoError> {
        match std::mem::replace(&mut self.step, ResumeStep::Load) {
            ResumeStep::Load => {
                let (firing, auto) = result.child::<(Firing, Automation), AutoError>()??;
                self.step = ResumeStep::Access(Box::new(firing));
                Ok(Next::Call(write_access(
                    &self.auth,
                    auto.automation_id,
                    self.local,
                )))
            }
            ResumeStep::Access(mut firing) => {
                let auto = result.child::<Automation, AutoError>()??;
                firing
                    .prepare_resume(&auto.spec, self.now_ms)
                    .map_err(AutoError::Conflict)?;
                let writes = vec![firing_row(&firing)?, active_row(&firing)];
                self.step = ResumeStep::Write(firing);
                Ok(Next::Call(IoCall::Write(writes)))
            }
            ResumeStep::Write(firing) => {
                result.done()?;
                Ok(Next::Done(*firing))
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
    Admin(Box<(Firing, Automation)>),
    Write(Box<Firing>),
    DropActive(Box<Firing>),
}

/// Human group admin approval or rejection of a `WaitingForApproval` stage.
#[derive(Debug, PartialEq)]
pub struct DecideStageOperation {
    auth: AuthContext,
    firing_id: Ulid,
    stage: String,
    approve: bool,
    note: Option<String>,
    local: NodeId,
    now_ms: u64,
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
            now_ms: 0,
            step: DecideStep::Load,
        }
    }
}

impl Operation for DecideStageOperation {
    type Output = Firing;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Firing>, AutoError> {
        let get = GetFiringOperation::new(&self.auth, self.firing_id, self.local);
        Ok(Next::Call(IoCall::child(get)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Firing>, AutoError> {
        match std::mem::replace(&mut self.step, DecideStep::Load) {
            DecideStep::Load => {
                let (firing, auto) = result.child::<(Firing, Automation), AutoError>()??;
                let check = AdminCheckOperation::new(self.auth.clone(), auto.group_id);
                self.step = DecideStep::Admin(Box::new((firing, auto)));
                Ok(Next::Call(IoCall::child(check)))
            }
            DecideStep::Admin(boxed) => {
                let is_admin = result
                    .child::<bool, AuthorizationError>()?
                    .map_err(|e| AutoError::Storage(e.to_string()))?;
                if !is_admin {
                    return Err(AutoError::Forbidden);
                }
                let (mut firing, auto) = *boxed;
                let idx = firing.stage_index(&self.stage).ok_or(AutoError::NotFound)?;
                if !matches!(auto.spec.stages[idx].kind, StageKind::Approval { .. })
                    || firing.stages[idx].state != StageState::WaitingForApproval
                {
                    return Err(AutoError::Conflict(
                        "stage is not waiting for approval".into(),
                    ));
                }
                let st = &mut firing.stages[idx];
                if self.approve {
                    st.state = StageState::Succeeded;
                    st.receipt.approved_by = Some(self.auth.user_id);
                } else {
                    st.state = StageState::Failed;
                    st.receipt.rejected_by = Some(self.auth.user_id);
                    st.error = Some(
                        self.note
                            .clone()
                            .unwrap_or_else(|| "rejected by approver".into()),
                    );
                }
                st.finished_at_ms = Some(self.now_ms);
                firing.evaluate(&auto.spec, self.now_ms);
                let row = firing_row(&firing)?;
                self.step = DecideStep::Write(Box::new(firing));
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            DecideStep::Write(firing) => {
                result.done()?;
                if firing.state.is_terminal() {
                    let key = active_key(firing.firing_id);
                    self.step = DecideStep::DropActive(firing);
                    return Ok(Next::Call(drop_active(key)));
                }
                Ok(Next::Done(*firing))
            }
            DecideStep::DropActive(firing) => {
                result.done()?;
                Ok(Next::Done(*firing))
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

fn drop_active(key: Key) -> IoCall {
    IoCall::Delete(vec![(AUTOMATION_RUNS_KEYSPACE.to_string(), key)])
}

fn active_row(firing: &Firing) -> (KeySpace, Key, ByteView) {
    (
        AUTOMATION_RUNS_KEYSPACE.to_string(),
        active_key(firing.firing_id),
        ByteView::from(firing.automation_id.to_bytes().to_vec()),
    )
}
