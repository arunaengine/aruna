//! Wire request and response bodies for automations, approval diffs, dry-run checks and graphs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::diff::ApprovalDiff;
use aruna_core::automation::graph::{AutoGraph, AutoNodeKind};
use aruna_core::automation::record::{Automation, Lifecycle, PauseReason, PendingChange};
use aruna_ops_work::automations::check::{AutoCheckReport, CopyPreview};
use aruna_ops_work::automations::diff::AutoDiffResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

pub use super::spec_wire::{LimitsBody, SpecBody, TriggerBody, WorkflowRefBody};
use crate::error::{ServerError, ServerResult};
use crate::jobs::rfc3339;
use crate::routes::execution::workflow::wire::{
    GraphEnd, GraphNodeKind, WorkflowGraphBody, WorkflowGraphEdge, WorkflowGraphNode,
};

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationPending)]
pub struct PendingBody {
    pub spec: SpecBody,
    pub requested_by: String,
    pub requested_at: String,
}

impl PendingBody {
    fn from_domain(pending: &PendingChange) -> Self {
        Self {
            spec: SpecBody::from_domain(&pending.spec),
            requested_by: pending.requested_by.to_string(),
            requested_at: rfc3339(pending.requested_at_ms),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = Automation, example = json!({
    "automation_id": "01K8W3A0000000000000000000",
    "group_id": "01JABCDEF0123456789ABCDEFG",
    "name": "ingest",
    "description": "Notify admins when triggered",
    "host_node": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
    "lifecycle": "draft",
    "pause_reason": null,
    "spec": {
        "triggers": [{"kind": "manual"}],
        "stages": [{"name": "alert", "kind": "notify", "audience": "group_admins", "message": "Done"}],
        "scope": {
            "read": [], "write": [], "metadata": [], "workflows": [],
            "audiences": ["group_admins"], "secrets": [], "allow_downgrade": false,
            "limits": {"max_active_firings": 4}
        }
    },
    "pending": null,
    "account": null,
    "approved_by": null,
    "approved_at": null,
    "created_by": "01K8W1A0000000000000000000",
    "created_at": "2026-11-26T12:00:00+00:00",
    "updated_at": "2026-11-26T12:00:00+00:00",
    "revision": 1
}))]
pub struct AutomationBody {
    pub automation_id: String,
    pub group_id: String,
    pub name: String,
    pub description: String,
    pub host_node: String,
    pub lifecycle: String,
    pub pause_reason: Option<String>,
    pub spec: SpecBody,
    pub pending: Option<PendingBody>,
    pub account: Option<String>,
    pub approved_by: Option<String>,
    pub approved_at: Option<String>,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
    pub revision: u64,
}

impl AutomationBody {
    pub fn from_domain(auto: &Automation) -> Self {
        Self {
            automation_id: auto.automation_id.to_string(),
            group_id: auto.group_id.to_string(),
            name: auto.name.clone(),
            description: auto.description.clone(),
            host_node: auto.host_node.to_string(),
            lifecycle: auto.lifecycle.as_str().into(),
            pause_reason: auto.pause_reason.as_ref().map(pause_text),
            spec: SpecBody::from_domain(&auto.spec),
            pending: auto.pending.as_ref().map(PendingBody::from_domain),
            account: auto.account.map(|a| a.to_string()),
            approved_by: auto.approved_by.map(|a| a.to_string()),
            approved_at: auto.approved_at_ms.map(rfc3339),
            created_by: auto.created_by.to_string(),
            created_at: rfc3339(auto.created_at_ms),
            updated_at: rfc3339(auto.updated_at_ms),
            revision: auto.revision,
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationPage, example = json!({
    "items": [{
        "automation_id": "01K8W3A0000000000000000000",
        "group_id": "01JABCDEF0123456789ABCDEFG",
        "name": "ingest",
        "description": "Notify admins when triggered",
        "host_node": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
        "lifecycle": "active",
        "pause_reason": null,
        "spec": {
            "triggers": [{"kind": "manual"}],
            "stages": [{"name": "alert", "kind": "notify", "audience": "group_admins", "message": "Done"}],
            "scope": {
                "read": [], "write": [], "metadata": [], "workflows": [],
                "audiences": ["group_admins"], "secrets": [], "allow_downgrade": false,
                "limits": {"max_active_firings": 4}
            }
        },
        "pending": null,
        "account": "01K8W3B0000000000000000000",
        "approved_by": "01K8W1A0000000000000000000",
        "approved_at": "2026-11-26T12:01:00+00:00",
        "created_by": "01K8W1A0000000000000000000",
        "created_at": "2026-11-26T12:00:00+00:00",
        "updated_at": "2026-11-26T12:01:00+00:00",
        "revision": 2
    }],
    "next_cursor": null
}))]
pub struct AutomationPage {
    pub items: Vec<AutomationBody>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListAutoQuery {
    /// Group ULID whose automations are listed.
    pub group_id: String,
    /// Optional lifecycle filter (`draft`, `waiting_for_approval`, `active`, `paused`, `removed`).
    pub lifecycle: Option<String>,
    /// Pagination cursor returned by the previous page.
    pub cursor: Option<String>,
    /// Maximum items to return (1 to 500).
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = CreateAutomationRequest, example = json!({
    "group_id": "01JABCDEF0123456789ABCDEFG",
    "name": "ingest",
    "description": "Notify admins when triggered",
    "spec": {
        "triggers": [{"kind": "manual"}],
        "stages": [{"name": "alert", "kind": "notify", "audience": "group_admins", "message": "Done"}],
        "scope": {
            "read": [], "write": [], "metadata": [], "workflows": [],
            "audiences": ["group_admins"], "secrets": [], "allow_downgrade": false,
            "limits": {"max_active_firings": 4}
        }
    }
}))]
pub struct CreateAutoRequest {
    pub group_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub spec: SpecBody,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = UpdateAutomationRequest, example = json!({
    "expected_revision": 1,
    "name": "ingest-v2",
    "description": "Updated automation description"
}))]
pub struct UpdateAutoRequest {
    pub expected_revision: u64,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub spec: Option<SpecBody>,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = AutomationRevisionRequest, example = json!({"expected_revision": 1}))]
pub struct RevisionBody {
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationDiff, example = json!({
    "pending": true,
    "revision": 2,
    "triggers_added": [{"kind": "manual"}],
    "triggers_removed": [],
    "read_added": [],
    "read_removed": [],
    "write_added": [],
    "write_removed": [],
    "metadata_added": [],
    "metadata_removed": [],
    "workflows_added": [],
    "workflows_removed": [],
    "audiences_added": ["group_admins"],
    "audiences_removed": [],
    "secrets_added": [],
    "secrets_removed": [],
    "allow_downgrade_before": false,
    "allow_downgrade_after": false,
    "stages_added": ["alert"],
    "stages_removed": [],
    "stages_changed": [],
    "limits_before": null,
    "limits_after": {"max_active_firings": 4},
    "outside_input": false,
    "unsandboxed": false,
    "loop_warnings": [],
    "applet_warnings": []
}))]
pub struct DiffBody {
    pub pending: bool,
    pub revision: u64,
    pub triggers_added: Vec<TriggerBody>,
    pub triggers_removed: Vec<TriggerBody>,
    pub read_added: Vec<String>,
    pub read_removed: Vec<String>,
    pub write_added: Vec<String>,
    pub write_removed: Vec<String>,
    pub metadata_added: Vec<String>,
    pub metadata_removed: Vec<String>,
    pub workflows_added: Vec<WorkflowRefBody>,
    pub workflows_removed: Vec<WorkflowRefBody>,
    pub audiences_added: Vec<String>,
    pub audiences_removed: Vec<String>,
    pub secrets_added: Vec<String>,
    pub secrets_removed: Vec<String>,
    pub allow_downgrade_before: bool,
    pub allow_downgrade_after: bool,
    pub stages_added: Vec<String>,
    pub stages_removed: Vec<String>,
    pub stages_changed: Vec<String>,
    pub limits_before: Option<LimitsBody>,
    pub limits_after: LimitsBody,
    pub outside_input: bool,
    pub unsandboxed: bool,
    pub loop_warnings: Vec<String>,
    pub applet_warnings: Vec<String>,
}

impl DiffBody {
    pub fn from_result(res: AutoDiffResult) -> Self {
        let d: ApprovalDiff = res.diff;
        let wf = |list: Vec<(Ulid, u32)>| {
            list.into_iter()
                .map(|(id, version)| WorkflowRefBody {
                    workflow_id: id.to_string(),
                    version,
                })
                .collect()
        };
        Self {
            pending: res.pending,
            revision: res.revision,
            triggers_added: d
                .triggers_added
                .iter()
                .map(TriggerBody::from_domain)
                .collect(),
            triggers_removed: d
                .triggers_removed
                .iter()
                .map(TriggerBody::from_domain)
                .collect(),
            read_added: d.read_added,
            read_removed: d.read_removed,
            write_added: d.write_added,
            write_removed: d.write_removed,
            metadata_added: d.metadata_added,
            metadata_removed: d.metadata_removed,
            workflows_added: wf(d.workflows_added),
            workflows_removed: wf(d.workflows_removed),
            audiences_added: d
                .audiences_added
                .iter()
                .map(|a| a.as_str().into())
                .collect(),
            audiences_removed: d
                .audiences_removed
                .iter()
                .map(|a| a.as_str().into())
                .collect(),
            secrets_added: d.secrets_added,
            secrets_removed: d.secrets_removed,
            allow_downgrade_before: d.allow_downgrade.0,
            allow_downgrade_after: d.allow_downgrade.1,
            stages_added: d.stages_added,
            stages_removed: d.stages_removed,
            stages_changed: d.stages_changed,
            limits_before: d.limits_before.as_ref().map(LimitsBody::from_domain),
            limits_after: LimitsBody::from_domain(&d.limits_after),
            outside_input: d.outside_input,
            unsandboxed: d.unsandboxed,
            loop_warnings: d.loop_warnings,
            applet_warnings: d.applet_warnings,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = AutomationCheckRequest, example = json!({"sample_values": {"bucket": "reads", "key": "run1/a.fastq"}}))]
pub struct CheckRequest {
    #[schema(value_type = Option<Object>)]
    pub sample_values: Option<Value>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationCopyPreview)]
pub struct CopyPreviewBody {
    pub stage: String,
    pub dest_bucket: String,
    pub sample_key: String,
    pub within_scope: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationCheckReport, example = json!({
    "ok": true,
    "diagnostics": [],
    "outside_input": false,
    "unsandboxed": false,
    "copy_previews": [],
    "workflows_checked": 1
}))]
pub struct CheckBody {
    pub ok: bool,
    pub diagnostics: Vec<String>,
    pub outside_input: bool,
    pub unsandboxed: bool,
    pub copy_previews: Vec<CopyPreviewBody>,
    pub workflows_checked: usize,
}

impl CheckBody {
    pub fn from_report(rep: AutoCheckReport) -> Self {
        Self {
            ok: rep.ok,
            diagnostics: rep.diagnostics,
            outside_input: rep.outside_input,
            unsandboxed: rep.unsandboxed,
            copy_previews: rep
                .copy_previews
                .into_iter()
                .map(|p: CopyPreview| CopyPreviewBody {
                    stage: p.stage,
                    dest_bucket: p.dest_bucket,
                    sample_key: p.sample_key,
                    within_scope: p.within_scope,
                })
                .collect(),
            workflows_checked: rep.workflows_checked,
        }
    }
}

/// Converts an automation static DAG into the shared `WorkflowGraphBody` wire format.
pub fn graph_body(graph: &AutoGraph) -> WorkflowGraphBody {
    WorkflowGraphBody {
        nodes: graph
            .nodes
            .iter()
            .map(|n| WorkflowGraphNode {
                id: n.id.clone(),
                kind: match n.kind {
                    AutoNodeKind::Input => GraphNodeKind::Input,
                    AutoNodeKind::Step => GraphNodeKind::Step,
                    AutoNodeKind::Subworkflow => GraphNodeKind::Subworkflow,
                    AutoNodeKind::Output => GraphNodeKind::Output,
                },
                label: n.label.clone(),
                tool: n.tool.clone(),
                version: n.version.clone(),
            })
            .collect(),
        edges: graph
            .edges
            .iter()
            .map(|e| WorkflowGraphEdge {
                from: GraphEnd {
                    node: e.from_node.clone(),
                    port: e.from_port.clone(),
                },
                to: GraphEnd {
                    node: e.to_node.clone(),
                    port: e.to_port.clone(),
                },
                scatter: None,
                fit: None,
            })
            .collect(),
        complete: graph.complete,
    }
}

pub fn parse_lifecycle(text: &str) -> ServerResult<Lifecycle> {
    match text {
        "draft" => Ok(Lifecycle::Draft),
        "waiting_for_approval" => Ok(Lifecycle::WaitingForApproval),
        "active" => Ok(Lifecycle::Active),
        "paused" => Ok(Lifecycle::Paused),
        "removed" => Ok(Lifecycle::Removed),
        other => Err(ServerError::BadRequestReason(format!(
            "unknown lifecycle `{other}`"
        ))),
    }
}

fn pause_text(reason: &PauseReason) -> String {
    match reason {
        PauseReason::Owner => "owner".into(),
        PauseReason::Approver => "approver".into(),
        PauseReason::ChainDepth => "chain_depth".into(),
        PauseReason::MissingGrant => "missing_grant".into(),
        PauseReason::Error(msg) => format!("error: {msg}"),
    }
}
