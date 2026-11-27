//! Firing and per-stage state machine with audited resolution, safe resume, caps and cancel.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::dag::validate_spec;
use super::record::{RunStatus, Spec, StageKind, When};
use crate::applet::record::SubmissionFile;
use crate::errors::ConversionError;
use crate::structs::execution::workflow_run::RunFile;
use crate::types::GroupId;
use crate::user::id::UserId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FiringState {
    Running,
    WaitingForApproval,
    NeedsAttention,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

impl FiringState {
    pub fn as_str(self) -> &'static str {
        use FiringState::*;
        match self {
            Running => "running",
            WaitingForApproval => "waiting_for_approval",
            NeedsAttention => "needs_attention",
            Cancelling => "cancelling",
            Succeeded => "succeeded",
            Failed => "failed",
            Cancelled => "cancelled",
        }
    }
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
    pub fn is_active(self) -> bool {
        !self.is_terminal()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageState {
    #[default]
    Pending,
    Ready,
    Running,
    WaitingForApproval,
    NeedsAttention,
    Cancelling,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
}

impl StageState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::NeedsAttention => "needs_attention",
            Self::Cancelling => "cancelling",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        }
    }

    /// `NeedsAttention` is intentionally non-terminal until a person resolves it.
    pub fn is_terminal(self) -> bool {
        use StageState::*;
        matches!(self, Succeeded | Failed | Skipped | Cancelled)
    }

    pub fn is_in_flight(self) -> bool {
        matches!(self, Self::Running | Self::Cancelling)
    }
}

/// One file committed by a `copy` stage, kept even if a later file fails or is cancelled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopiedFile {
    pub source_key: String,
    pub source_version: Ulid,
    pub dest_bucket: String,
    pub dest_key: String,
    pub dest_version: Ulid,
    pub bytes: u64,
}

/// Durable outputs and decisions recorded for one stage of a firing.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageReceipt {
    pub workflow_run_id: Option<Ulid>,
    pub outputs: Vec<RunFile>,
    pub copied: Vec<CopiedFile>,
    /// Unsettled copy target awaiting human resolution on a `NeedsAttention` copy stage.
    pub pending_copy: Option<CopiedFile>,
    /// Whether cancellation has been acknowledged by the child workflow run.
    pub cancel_sent: bool,
    pub dataset_id: Option<Ulid>,
    pub notified: Option<u32>,
    pub approved_by: Option<UserId>,
    pub rejected_by: Option<UserId>,
    pub expires_at_ms: Option<u64>,
    pub preview_json: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageRun {
    pub name: String,
    pub state: StageState,
    pub attempt: u32,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
    pub receipt: StageReceipt,
}

impl StageRun {
    fn pending(name: &str) -> Self {
        Self {
            name: name.into(),
            state: StageState::Pending,
            attempt: 1,
            ..Self::default()
        }
    }
}

/// Audited human resolution of a `NeedsAttention` stage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resolution {
    pub stage: String,
    pub done: bool,
    pub actor: UserId,
    pub note: Option<String>,
    pub at_ms: u64,
}

/// Event or manual payload that started one firing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerPayload {
    Manual {
        values_json: String,
        actor: UserId,
    },
    DataUploaded {
        bucket: String,
        key: String,
        version_id: Ulid,
        path: String,
        size_bytes: u64,
    },
    RunFinished {
        workflow_id: Ulid,
        run_id: Ulid,
        status: RunStatus,
        outputs: Vec<RunFile>,
        applet_id: Option<Ulid>,
        outside_input: bool,
        depth: u8,
    },
    AppletSubmission {
        applet_id: Ulid,
        version_id: Ulid,
        pseudonym: String,
        guest: bool,
        outside_input: bool,
        bucket: String,
        key: String,
        files: Vec<SubmissionFile>,
    },
}

impl TriggerPayload {
    pub fn chain_depth(&self) -> u8 {
        if let Self::RunFinished { depth, .. } = self {
            *depth
        } else {
            0
        }
    }

    pub fn outside_input(&self) -> bool {
        match self {
            Self::AppletSubmission {
                guest,
                outside_input,
                ..
            } => *guest || *outside_input,
            Self::RunFinished { outside_input, .. } => *outside_input,
            _ => false,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Manual { .. } => "manual",
            Self::DataUploaded { .. } => "data_uploaded",
            Self::RunFinished { .. } => "run_finished",
            Self::AppletSubmission { .. } => "applet_submission",
        }
    }
}

/// One execution of an automation DAG, pinned to the approved `spec` at start time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Firing {
    pub firing_id: Ulid,
    pub automation_id: Ulid,
    pub group_id: GroupId,
    pub revision: u64,
    pub attempt: u32,
    pub state: FiringState,
    pub spec: Spec,
    pub trigger: TriggerPayload,
    pub stages: Vec<StageRun>,
    pub resolutions: Vec<Resolution>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
}

impl Firing {
    pub fn new(
        firing_id: Ulid,
        automation_id: Ulid,
        group_id: GroupId,
        revision: u64,
        spec: &Spec,
        trigger: TriggerPayload,
        now_ms: u64,
    ) -> Self {
        let stages = spec
            .stages
            .iter()
            .map(|s| StageRun::pending(&s.name))
            .collect();
        let mut firing = Self {
            firing_id,
            automation_id,
            group_id,
            revision,
            attempt: 1,
            state: FiringState::Running,
            spec: spec.clone(),
            trigger,
            stages,
            resolutions: Vec::new(),
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            finished_at_ms: None,
            error: None,
        };
        firing.evaluate(now_ms);
        firing
    }

    pub fn stage_index(&self, name: &str) -> Option<usize> {
        self.stages.iter().position(|stage| stage.name == name)
    }

    /// Evaluates stage readiness in topological order and recomputes firing state.
    pub fn evaluate(&mut self, now_ms: u64) {
        if self.state.is_terminal() {
            return;
        }
        for stage in &mut self.stages {
            if stage.state == StageState::WaitingForApproval
                && stage.receipt.expires_at_ms.is_some_and(|exp| now_ms >= exp)
            {
                stage.state = StageState::Failed;
                stage.finished_at_ms = Some(now_ms);
                stage.error = Some("approval stage expired".into());
            }
        }
        if self.state == FiringState::Cancelling {
            self.finish_cancel(now_ms);
            return;
        }
        let Ok(plan) = validate_spec(&self.spec) else {
            return;
        };
        for &index in &plan.order {
            if self.stages[index].state != StageState::Pending {
                continue;
            }
            let upstreams: Vec<StageState> = plan.needs[index]
                .iter()
                .map(|&need| self.stages[need].state)
                .collect();
            if upstreams.iter().any(|state| !state.is_terminal()) {
                continue;
            }
            let ready = if upstreams.is_empty() {
                true
            } else {
                match self.spec.stages[index].when {
                    When::Passed => upstreams.iter().all(|&s| s == StageState::Succeeded),
                    When::Failed => upstreams.contains(&StageState::Failed),
                    When::Always => upstreams
                        .iter()
                        .any(|&s| matches!(s, StageState::Succeeded | StageState::Failed)),
                }
            };
            self.stages[index].state = match ready {
                true => StageState::Ready,
                false => {
                    self.stages[index].finished_at_ms = Some(now_ms);
                    StageState::Skipped
                }
            };
        }
        self.recompute_state(now_ms);
    }

    /// Derives firing state from current stage states so repaired resumes finish `Succeeded`.
    pub fn recompute_state(&mut self, now_ms: u64) {
        if self.state == FiringState::Cancelling {
            self.finish_cancel(now_ms);
            return;
        }
        let has = |target| self.stages.iter().any(|s| s.state == target);
        self.state = if has(StageState::Running) || has(StageState::Ready) {
            FiringState::Running
        } else if has(StageState::NeedsAttention) {
            FiringState::NeedsAttention
        } else if has(StageState::WaitingForApproval) {
            FiringState::WaitingForApproval
        } else if has(StageState::Pending) || has(StageState::Cancelling) {
            FiringState::Running
        } else if has(StageState::Cancelled) {
            FiringState::Cancelled
        } else if has(StageState::Failed) {
            FiringState::Failed
        } else {
            FiringState::Succeeded
        };
        self.updated_at_ms = now_ms;
        self.finished_at_ms = self
            .state
            .is_terminal()
            .then(|| self.finished_at_ms.unwrap_or(now_ms));
    }

    /// Audited human resolution of a `NeedsAttention` stage (`done=true` -> Succeeded, `false` -> Failed).
    pub fn resolve(
        &mut self,
        stage_name: &str,
        done: bool,
        actor: UserId,
        note: Option<String>,
        now_ms: u64,
    ) -> Result<(), String> {
        self.resolve_item(stage_name, done, None, actor, note, now_ms)
    }

    /// Resolves a `NeedsAttention` stage, recovering a single uncertain copy file into `copied` and
    /// keeping multi-file copy stages `Running` when remaining files still need to be copied.
    pub fn resolve_item(
        &mut self,
        stage_name: &str,
        done: bool,
        copy_progress: Option<(Option<CopiedFile>, usize)>,
        actor: UserId,
        note: Option<String>,
        now_ms: u64,
    ) -> Result<(), String> {
        if self.state == FiringState::Cancelling || self.state.is_terminal() {
            return Err(format!("firing is already {}", self.state.as_str()));
        }
        let index = self
            .stage_index(stage_name)
            .ok_or_else(|| format!("unknown stage `{stage_name}`"))?;
        if self.stages[index].state != StageState::NeedsAttention {
            return Err(format!("stage `{stage_name}` is not in needs_attention"));
        }
        let recovered_copy = if done && let Some((recovered, total_files)) = copy_progress {
            let file = recovered
                .or_else(|| self.stages[index].receipt.pending_copy.clone())
                .ok_or_else(|| {
                    "missing verified copy destination for done resolution".to_string()
                })?;
            Some((file, total_files))
        } else {
            None
        };
        self.resolutions.push(Resolution {
            stage: stage_name.to_string(),
            done,
            actor,
            note,
            at_ms: now_ms,
        });
        let stage = &mut self.stages[index];
        stage.receipt.pending_copy = None;
        if done {
            stage.error = None;
            if let Some((file, total_files)) = recovered_copy {
                let exists =
                    stage.receipt.copied.iter().any(|c| {
                        c.dest_version == file.dest_version && c.dest_key == file.dest_key
                    });
                if !exists {
                    stage.receipt.copied.push(file);
                }
                if stage.receipt.copied.len() < total_files {
                    stage.state = StageState::Running;
                    stage.finished_at_ms = None;
                    self.evaluate(now_ms);
                    return Ok(());
                }
            }
            stage.state = StageState::Succeeded;
        } else {
            stage.state = StageState::Failed;
        }
        stage.finished_at_ms = Some(now_ms);
        self.evaluate(now_ms);
        Ok(())
    }

    /// Resumes a failed firing while refusing unresolved `NeedsAttention` stages and preserving
    /// every already-`Succeeded` stage receipt (including attempt-1 `when: failed` branches).
    pub fn prepare_resume(&mut self, now_ms: u64) -> Result<(), String> {
        if self
            .stages
            .iter()
            .any(|s| s.state == StageState::NeedsAttention)
        {
            return Err("resolve every needs_attention stage before resuming".into());
        }
        if self.state != FiringState::Failed {
            return Err("only failed firings can be resumed".into());
        }
        let plan = validate_spec(&self.spec)?;
        let mut reset = BTreeSet::new();
        for &index in &plan.order {
            let state = self.stages[index].state;
            let upstream = plan.needs[index].iter().any(|need| reset.contains(need));
            if state == StageState::Failed
                || (upstream && matches!(state, StageState::Skipped | StageState::Pending))
            {
                reset.insert(index);
            }
        }
        if reset.is_empty() {
            return Err("no failed stages remain to resume".into());
        }
        self.attempt += 1;
        (self.state, self.error, self.finished_at_ms) = (FiringState::Running, None, None);
        for index in reset {
            let stage = &mut self.stages[index];
            (stage.state, stage.attempt, stage.error) = (StageState::Pending, self.attempt, None);
            (stage.started_at_ms, stage.finished_at_ms) = (None, None);
            let r = &mut stage.receipt;
            (
                r.workflow_run_id,
                r.pending_copy,
                r.cancel_sent,
                r.approved_by,
                r.rejected_by,
                r.expires_at_ms,
            ) = (None, None, false, None, None, None);
        }
        self.evaluate(now_ms);
        Ok(())
    }

    pub fn active_runs(&self) -> usize {
        self.stages
            .iter()
            .enumerate()
            .filter(|(i, s)| {
                s.state.is_in_flight() && matches!(self.spec.stages[*i].kind, StageKind::Run { .. })
            })
            .count()
    }

    /// Ready `run` stage indices bounded by the remaining `available` workflow slots.
    pub fn ready_runs(&self, available: usize) -> Vec<usize> {
        let slots = (self.spec.scope.limits.max_concurrent_workflows as usize)
            .saturating_sub(self.active_runs())
            .min(available);
        self.stages
            .iter()
            .enumerate()
            .filter(|(i, s)| {
                s.state == StageState::Ready
                    && matches!(self.spec.stages[*i].kind, StageKind::Run { .. })
            })
            .map(|(i, _)| i)
            .take(slots)
            .collect()
    }

    /// Next ready `copy` stage if no other `copy` stage is currently in flight in this firing.
    pub fn next_copy(&self) -> Option<usize> {
        if self.stages.iter().enumerate().any(|(i, s)| {
            s.state.is_in_flight() && matches!(self.spec.stages[i].kind, StageKind::Copy { .. })
        }) {
            return None;
        }
        self.stages.iter().enumerate().find_map(|(i, s)| {
            (s.state == StageState::Ready
                && matches!(self.spec.stages[i].kind, StageKind::Copy { .. }))
            .then_some(i)
        })
    }

    /// Moves firing to `Cancelling`, cancels pending/waiting stages and returns active child runs.
    pub fn begin_cancel(&mut self, now_ms: u64) -> Vec<(usize, Ulid)> {
        if self.state.is_terminal() {
            return Vec::new();
        }
        self.state = FiringState::Cancelling;
        self.updated_at_ms = now_ms;
        let mut child_runs = Vec::new();
        for (index, stage) in self.stages.iter_mut().enumerate() {
            match stage.state {
                StageState::Pending | StageState::Ready | StageState::WaitingForApproval => {
                    stage.state = StageState::Cancelled;
                    stage.finished_at_ms = Some(now_ms);
                }
                StageState::Running | StageState::Cancelling => {
                    stage.state = StageState::Cancelling;
                    if matches!(self.spec.stages[index].kind, StageKind::Run { .. })
                        && let Some(run_id) = stage.receipt.workflow_run_id
                    {
                        stage.receipt.cancel_sent = false;
                        child_runs.push((index, run_id));
                    }
                }
                _ => {}
            }
        }
        self.finish_cancel(now_ms);
        child_runs
    }

    /// Finalizes `Cancelled` once every in-flight stage has terminated.
    pub fn finish_cancel(&mut self, now_ms: u64) {
        if self.state == FiringState::Cancelling
            && !self.stages.iter().any(|s| s.state.is_in_flight())
        {
            (self.state, self.finished_at_ms, self.updated_at_ms) =
                (FiringState::Cancelled, Some(now_ms), now_ms);
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
#[path = "firing_tests.rs"]
mod tests;
