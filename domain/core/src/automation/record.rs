//! Automation records: lifecycle, triggers, bounded DAG stages and approved scope.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::scope::{Audience, Scope};
use crate::structs::execution::workflow_run::WorkflowSecretBinding;

/// Default expiry window for an approval stage: 7 days.
pub const DEFAULT_APPROVAL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lifecycle {
    Draft,
    WaitingForApproval,
    Active,
    Paused,
    Removed,
}

impl Lifecycle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Removed => "removed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PauseReason {
    Owner,
    Approver,
    ChainDepth,
    MissingGrant,
    Error(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Succeeded,
    Failed,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum When {
    #[default]
    Passed,
    Failed,
    Always,
}

impl When {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Always => "always",
        }
    }
}

/// One trigger that starts an automation firing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Trigger {
    Manual {
        form_json: Option<String>,
    },
    DataUploaded {
        bucket: String,
        prefix: String,
        pattern: Option<String>,
    },
    RunFinished {
        workflow_id: Ulid,
        status: Option<RunStatus>,
        applet_id: Option<Ulid>,
    },
    AppletSubmission {
        applet_id: Ulid,
    },
}

/// What one stage in an automation DAG does when its upstream needs satisfy `when`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageKind {
    Run {
        workflow_id: Ulid,
        version: u32,
        values_json: String,
        inputs_json: Option<String>,
        output_bucket: Option<String>,
        output_prefix: Option<String>,
        placement_site: Option<String>,
        secrets: BTreeMap<String, WorkflowSecretBinding>,
        provenance_crate: bool,
    },
    Copy {
        from_stage: String,
        file: Option<String>,
        dest_bucket: String,
        dest_template: String,
        allow_downgrade: bool,
    },
    Dataset {
        from_stage: String,
        metadata_path: String,
        name_template: String,
        description: String,
        license: Option<String>,
        public: bool,
    },
    Notify {
        audience: Audience,
        message: String,
    },
    Approval {
        message: String,
        timeout_ms: Option<u64>,
    },
}

impl StageKind {
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Run { .. } => "run",
            Self::Copy { .. } => "copy",
            Self::Dataset { .. } => "dataset",
            Self::Notify { .. } => "notify",
            Self::Approval { .. } => "approval",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stage {
    pub name: String,
    /// Upstream stage names; `None` defaults linearly to `[previous_stage]`.
    pub needs: Option<Vec<String>>,
    #[serde(default)]
    pub when: When,
    pub kind: StageKind,
}

/// Complete trigger, stage DAG and scope definition of an automation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spec {
    pub triggers: Vec<Trigger>,
    pub stages: Vec<Stage>,
    pub scope: Scope,
}

impl Spec {
    pub fn uses_applets(&self) -> bool {
        self.triggers
            .iter()
            .any(|trigger| matches!(trigger, Trigger::AppletSubmission { .. }))
    }

    pub fn has_runs(&self) -> bool {
        self.stages
            .iter()
            .any(|stage| matches!(stage.kind, StageKind::Run { .. }))
    }
}
