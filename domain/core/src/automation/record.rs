//! Automation records: lifecycle, triggers, bounded DAG stages and approved scope.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::firing::TriggerPayload;
use super::rules::{Conditions, ScheduleSpec};
use super::scope::{Audience, Scope};
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::permission_path::escape_permission_path;
use crate::plugin_runs::SourceEvent;
use crate::structs::execution::workflow::workflow_permission_path;
use crate::structs::execution::workflow_run::WorkflowSecretBinding;
use crate::structs::identity::auth::Permission;
use crate::structs::identity::realm::RealmId;
use crate::types::{GroupId, RoleId};
use crate::user::id::UserId;

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
        /// `None` matches any applet in the group naming this automation in its own settings.
        applet_id: Option<Ulid>,
    },
    Schedule(ScheduleSpec),
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

/// Complete trigger, condition, stage DAG and scope definition of an automation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spec {
    pub triggers: Vec<Trigger>,
    #[serde(default)]
    pub conditions: Conditions,
    pub stages: Vec<Stage>,
    pub scope: Scope,
}

impl Spec {
    pub fn uses_applets(&self) -> bool {
        self.triggers
            .iter()
            .any(|trigger| matches!(trigger, Trigger::AppletSubmission { .. }))
    }

    pub fn has_schedules(&self) -> bool {
        self.triggers
            .iter()
            .any(|trigger| matches!(trigger, Trigger::Schedule(_)))
    }

    pub fn has_runs(&self) -> bool {
        self.stages
            .iter()
            .any(|stage| matches!(stage.kind, StageKind::Run { .. }))
    }

    pub fn runs_workflow(&self, target: Ulid) -> bool {
        self.stages.iter().any(|stage| {
            matches!(
                stage.kind,
                StageKind::Run { workflow_id, .. } if workflow_id == target
            )
        })
    }
}

/// A proposed definition waiting for a human group admin's approval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingChange {
    pub spec: Spec,
    pub requested_by: UserId,
    pub requested_at_ms: u64,
}

/// Node-local automation record, kept outside `RealmConfigDocument`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Automation {
    pub automation_id: Ulid,
    pub group_id: GroupId,
    pub name: String,
    pub description: String,
    pub host_node: NodeId,
    pub lifecycle: Lifecycle,
    pub pause_reason: Option<PauseReason>,
    pub spec: Spec,
    pub pending: Option<PendingChange>,
    pub account: Option<UserId>,
    pub role_id: Option<RoleId>,
    pub approved_by: Option<UserId>,
    pub approved_at_ms: Option<u64>,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub revision: u64,
    /// Optional primary workflow linked to this automation (e.g. from unified Studio saves).
    pub workflow_id: Option<Ulid>,
}

impl Automation {
    /// Whether this automation should hold an active service account and role right now.
    pub fn wants_account(&self) -> bool {
        self.lifecycle == Lifecycle::Active && self.approved_by.is_some()
    }

    /// The human admin whose authority manages the service account and role.
    pub fn granter(&self) -> Option<UserId> {
        self.approved_by
    }

    /// Permission paths granted to the automation's service account role.
    pub fn role_paths(&self, realm_id: RealmId, node: NodeId) -> Vec<(String, Permission)> {
        self.role_paths_for(&self.spec, realm_id, node)
    }

    /// Permission paths derived from a pinned `spec` (e.g. a firing's execution spec).
    pub fn role_paths_for(
        &self,
        spec: &Spec,
        realm_id: RealmId,
        node: NodeId,
    ) -> Vec<(String, Permission)> {
        let mut paths = Vec::new();
        for pattern in &spec.scope.read {
            paths.push((pattern.clone(), Permission::READ));
        }
        for pattern in &spec.scope.write {
            paths.push((pattern.clone(), Permission::WRITE));
        }
        let meta_root = format!("/{realm_id}/g/{}/meta", self.group_id);
        for prefix in &spec.scope.metadata {
            let trimmed = prefix.trim_matches('/');
            let full = match trimmed.is_empty() {
                true => meta_root.clone(),
                false => format!("{meta_root}/{}", escape_permission_path(trimmed)),
            };
            paths.push((format!("{full}/**"), Permission::WRITE));
        }
        if !spec.scope.workflows.is_empty() || spec.has_runs() {
            paths.push((
                workflow_permission_path(realm_id, self.group_id),
                Permission::WRITE,
            ));
        }
        let _ = node;
        paths
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Matched event stored while an automation is paused, awaiting approval or throttled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeptEvent {
    pub event_id: Ulid,
    pub automation_id: Ulid,
    pub event: SourceEvent,
    pub reason: String,
    pub kept_at_ms: u64,
    pub payload: Option<TriggerPayload>,
}

impl KeptEvent {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
