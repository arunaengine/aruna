//! Wire schemas for automation firings, stage receipts, trigger events, conditions and kept events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::{
    CopiedFile, Firing, FiringState, Resolution, StageReceipt, StageRun, TriggerPayload,
};
use aruna_core::automation::record::KeptEvent;
use aruna_core::automation::rules::ConditionCheck;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

use crate::error::{ServerError, ServerResult};
use crate::jobs::rfc3339;

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationCopiedFile)]
pub struct CopiedFileBody {
    pub source_key: String,
    pub source_version: String,
    pub dest_bucket: String,
    pub dest_key: String,
    pub dest_version: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationStageReceipt)]
pub struct StageReceiptBody {
    pub workflow_run_id: Option<String>,
    pub outputs_count: usize,
    pub copied: Vec<CopiedFileBody>,
    pub dataset_id: Option<String>,
    pub notified: Option<u32>,
    pub approved_by: Option<String>,
    pub rejected_by: Option<String>,
    pub expires_at: Option<String>,
    pub preview: Option<String>,
}

impl StageReceiptBody {
    fn from_domain(rec: &StageReceipt) -> Self {
        Self {
            workflow_run_id: rec.workflow_run_id.map(|id| id.to_string()),
            outputs_count: rec.outputs.len(),
            copied: rec
                .copied
                .iter()
                .map(|c: &CopiedFile| CopiedFileBody {
                    source_key: c.source_key.clone(),
                    source_version: c.source_version.to_string(),
                    dest_bucket: c.dest_bucket.clone(),
                    dest_key: c.dest_key.clone(),
                    dest_version: c.dest_version.to_string(),
                    bytes: c.bytes,
                })
                .collect(),
            dataset_id: rec.dataset_id.map(|id| id.to_string()),
            notified: rec.notified,
            approved_by: rec.approved_by.map(|u| u.to_string()),
            rejected_by: rec.rejected_by.map(|u| u.to_string()),
            expires_at: rec.expires_at_ms.map(rfc3339),
            preview: rec.preview_json.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationStageRun)]
pub struct StageRunBody {
    pub name: String,
    pub state: String,
    pub attempt: u32,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub receipt: StageReceiptBody,
}

impl StageRunBody {
    fn from_domain(stage: &StageRun) -> Self {
        Self {
            name: stage.name.clone(),
            state: stage.state.as_str().into(),
            attempt: stage.attempt,
            started_at: stage.started_at_ms.map(rfc3339),
            finished_at: stage.finished_at_ms.map(rfc3339),
            error: stage.error.clone(),
            receipt: StageReceiptBody::from_domain(&stage.receipt),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationResolution)]
pub struct ResolutionBody {
    pub stage: String,
    pub done: bool,
    pub actor: String,
    pub note: Option<String>,
    pub at: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationConditionCheck)]
pub struct ConditionCheckBody {
    pub kind: String,
    pub text: String,
    pub held: bool,
    pub why: String,
}

impl From<&ConditionCheck> for ConditionCheckBody {
    fn from(check: &ConditionCheck) -> Self {
        Self {
            kind: check.kind.as_str().into(),
            text: check.text.clone(),
            held: check.held,
            why: check.why.clone(),
        }
    }
}

/// Structured event payload that started (or attempted to start) an automation run.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schema(as = AutomationTriggerEvent)]
pub enum TriggerEventBody {
    Manual {
        actor: String,
        #[schema(value_type = Object)]
        values: Value,
    },
    DataUploaded {
        bucket: String,
        key: String,
        version_id: String,
        path: String,
        /// Object size in bytes, or `null` if the uploaded version was unavailable when matched.
        size_bytes: Option<u64>,
    },
    RunFinished {
        workflow_id: String,
        run_id: String,
        status: String,
        outputs_count: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        applet_id: Option<String>,
        outside_input: bool,
        depth: u8,
    },
    AppletSubmission {
        applet_id: String,
        version_id: String,
        pseudonym: String,
        guest: bool,
        outside_input: bool,
        bucket: String,
        key: String,
        files_count: usize,
    },
    Schedule {
        at: String,
    },
}

impl TriggerEventBody {
    pub fn from_payload(payload: &TriggerPayload) -> Self {
        match payload {
            TriggerPayload::Manual { values_json, actor } => Self::Manual {
                actor: actor.to_string(),
                values: serde_json::from_str(values_json)
                    .unwrap_or_else(|_| Value::Object(Default::default())),
            },
            TriggerPayload::DataUploaded {
                bucket,
                key,
                version_id,
                path,
                size_bytes,
            } => Self::DataUploaded {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: version_id.to_string(),
                path: path.clone(),
                size_bytes: *size_bytes,
            },
            TriggerPayload::RunFinished {
                workflow_id,
                run_id,
                status,
                outputs,
                applet_id,
                outside_input,
                depth,
            } => Self::RunFinished {
                workflow_id: workflow_id.to_string(),
                run_id: run_id.to_string(),
                status: status.as_str().into(),
                outputs_count: outputs.len(),
                applet_id: applet_id.map(|id| id.to_string()),
                outside_input: *outside_input,
                depth: *depth,
            },
            TriggerPayload::AppletSubmission {
                applet_id,
                version_id,
                pseudonym,
                guest,
                outside_input,
                bucket,
                key,
                files,
            } => Self::AppletSubmission {
                applet_id: applet_id.to_string(),
                version_id: version_id.to_string(),
                pseudonym: pseudonym.clone(),
                guest: *guest,
                outside_input: *outside_input,
                bucket: bucket.clone(),
                key: key.clone(),
                files_count: files.len(),
            },
            TriggerPayload::Schedule { at_ms } => Self::Schedule {
                at: rfc3339(*at_ms),
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationRun, example = json!({
    "firing_id": "01K8W4A0000000000000000000",
    "automation_id": "01K8W3A0000000000000000000",
    "group_id": "01JABCDEF0123456789ABCDEFG",
    "revision": 2,
    "attempt": 1,
    "state": "running",
    "trigger_kind": "manual",
    "trigger_event": {"kind": "manual", "actor": "01K8W1A0000000000000000000", "values": {}},
    "stages": [],
    "resolutions": [],
    "created_at": "2026-11-26T12:05:00+00:00",
    "updated_at": "2026-11-26T12:05:00+00:00",
    "finished_at": null,
    "error": null,
    "reason": null,
    "conditions_checked": []
}))]
pub struct FiringBody {
    pub firing_id: String,
    pub automation_id: String,
    pub group_id: String,
    pub revision: u64,
    pub attempt: u32,
    pub state: String,
    pub trigger_kind: String,
    pub trigger_event: TriggerEventBody,
    pub stages: Vec<StageRunBody>,
    pub resolutions: Vec<ResolutionBody>,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub reason: Option<String>,
    pub conditions_checked: Vec<ConditionCheckBody>,
}

impl FiringBody {
    pub fn from_domain(firing: &Firing) -> Self {
        Self {
            firing_id: firing.firing_id.to_string(),
            automation_id: firing.automation_id.to_string(),
            group_id: firing.group_id.to_string(),
            revision: firing.revision,
            attempt: firing.attempt,
            state: firing.state.as_str().into(),
            trigger_kind: firing.trigger.kind_name().into(),
            trigger_event: TriggerEventBody::from_payload(&firing.trigger),
            stages: firing
                .stages
                .iter()
                .map(StageRunBody::from_domain)
                .collect(),
            resolutions: firing
                .resolutions
                .iter()
                .map(|resolution: &Resolution| ResolutionBody {
                    stage: resolution.stage.clone(),
                    done: resolution.done,
                    actor: resolution.actor.to_string(),
                    note: resolution.note.clone(),
                    at: rfc3339(resolution.at_ms),
                })
                .collect(),
            created_at: rfc3339(firing.created_at_ms),
            updated_at: rfc3339(firing.updated_at_ms),
            finished_at: firing.finished_at_ms.map(rfc3339),
            error: firing.error.clone(),
            reason: firing.reason.clone(),
            conditions_checked: firing
                .conditions_checked
                .iter()
                .map(ConditionCheckBody::from)
                .collect(),
        }
    }
}

/// 409 response returned when a manual start is refused by automation conditions.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = ConditionsNotMetResponse, example = json!({
    "error": "conditions_not_met",
    "code": "conditions_not_met",
    "message": "It was 22:00 UTC, outside 08:00 to 18:00 UTC.",
    "firing": {
        "firing_id": "01K8W4A0000000000000000000",
        "automation_id": "01K8W3A0000000000000000000",
        "group_id": "01JABCDEF0123456789ABCDEFG",
        "revision": 2,
        "attempt": 1,
        "state": "skipped",
        "trigger_kind": "manual",
        "trigger_event": {"kind": "manual", "actor": "01K8W1A0000000000000000000", "values": {}},
        "stages": [],
        "resolutions": [],
        "created_at": "2026-11-26T22:00:00+00:00",
        "updated_at": "2026-11-26T22:00:00+00:00",
        "finished_at": "2026-11-26T22:00:00+00:00",
        "error": null,
        "reason": "It was 22:00 UTC, outside 08:00 to 18:00 UTC.",
        "conditions_checked": [{
            "kind": "time_of_day",
            "text": "Between 08:00 and 18:00 UTC",
            "held": false,
            "why": "It was 22:00 UTC, outside 08:00 to 18:00 UTC."
        }]
    }
}))]
pub struct ConditionsRefusalBody {
    /// Always `"conditions_not_met"`.
    pub error: String,
    /// Always `"conditions_not_met"`.
    pub code: String,
    /// One-sentence explanation of why conditions did not hold.
    pub message: String,
    /// The persisted `skipped` automation run record.
    pub firing: FiringBody,
}

/// Standard 409 conflict body returned when an automation cannot start in its current state.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = AutomationStateConflict)]
pub struct StateConflictBody {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

/// 409 response union for `POST /compute/automations/{automation_id}/start`.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(untagged)]
#[schema(as = StartAutomationConflict)]
pub enum StartConflictBody {
    Conditions(Box<ConditionsRefusalBody>),
    Conflict(StateConflictBody),
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationRunPage, example = json!({
    "items": [{
        "firing_id": "01K8W4A0000000000000000000",
        "automation_id": "01K8W3A0000000000000000000",
        "group_id": "01JABCDEF0123456789ABCDEFG",
        "revision": 2,
        "attempt": 1,
        "state": "succeeded",
        "trigger_kind": "manual",
        "trigger_event": {"kind": "manual", "actor": "01K8W1A0000000000000000000", "values": {}},
        "stages": [],
        "resolutions": [],
        "created_at": "2026-11-26T12:05:00+00:00",
        "updated_at": "2026-11-26T12:05:10+00:00",
        "finished_at": "2026-11-26T12:05:10+00:00",
        "error": null,
        "reason": null,
        "conditions_checked": []
    }],
    "next_cursor": null
}))]
pub struct FiringPageBody {
    pub items: Vec<FiringBody>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct FiringQuery {
    /// Optional group ULID filter.
    pub group_id: Option<String>,
    /// Optional automation ULID filter.
    pub automation_id: Option<String>,
    /// Optional firing state filter (`running`, `waiting_for_approval`, `needs_attention`,
    /// `cancelling`, `succeeded`, `failed`, `cancelled`, `skipped`).
    pub state: Option<String>,
    /// Pagination cursor returned by the previous page.
    pub cursor: Option<String>,
    /// Maximum items to return (1 to 500).
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = StartAutomationRequest, example = json!({"values": {"sample": "s1"}}))]
pub struct StartRequest {
    #[schema(value_type = Option<Object>)]
    pub values: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = ResolveStageRequest, example = json!({"stage": "copy_out", "done": true, "note": "Verified destination object"}))]
pub struct ResolveRequest {
    pub stage: String,
    pub done: bool,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = DecideStageRequest, example = json!({"note": "Approved after reviewing preview"}))]
pub struct DecideRequest {
    pub note: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationKeptEvent, example = json!({
    "event_id": "01K8W4B0000000000000000000",
    "kind": "data_uploaded",
    "resource": "reads/run1/a.fastq",
    "revision": "01K8W1C0000000000000000000",
    "reason": "automation paused",
    "kept_at": "2026-11-26T12:04:00+00:00",
    "actor": null,
    "event": {
        "kind": "data_uploaded",
        "bucket": "reads",
        "key": "run1/a.fastq",
        "version_id": "01K8W1C0000000000000000000",
        "path": "/realm/g/01JABCDEF0123456789ABCDEFG/data/node/reads/run1/a.fastq",
        "size_bytes": 1024
    }
}))]
pub struct KeptEventBody {
    pub event_id: String,
    pub kind: String,
    pub resource: String,
    pub revision: String,
    pub reason: String,
    pub kept_at: String,
    pub actor: Option<String>,
    pub event: TriggerEventBody,
}

impl KeptEventBody {
    pub fn from_domain(kept: &KeptEvent) -> Self {
        Self {
            event_id: kept.event_id.to_string(),
            kind: kept.event.kind.name().into(),
            resource: kept.event.resource.clone(),
            revision: kept.event.revision.clone(),
            reason: kept.reason.clone(),
            kept_at: rfc3339(kept.kept_at_ms),
            actor: kept.event.actor.map(|user| user.to_string()),
            event: TriggerEventBody::from_payload(&kept.payload),
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = StartKeptEventsRequest, example = json!({"event_ids": ["01K8W4B0000000000000000000"]}))]
pub struct StartEventsRequest {
    pub event_ids: Vec<String>,
}

pub fn parse_firing_state(text: &str) -> ServerResult<FiringState> {
    match text {
        "running" => Ok(FiringState::Running),
        "waiting_for_approval" => Ok(FiringState::WaitingForApproval),
        "needs_attention" => Ok(FiringState::NeedsAttention),
        "cancelling" => Ok(FiringState::Cancelling),
        "succeeded" => Ok(FiringState::Succeeded),
        "failed" => Ok(FiringState::Failed),
        "cancelled" => Ok(FiringState::Cancelled),
        "skipped" => Ok(FiringState::Skipped),
        other => Err(ServerError::BadRequestReason(format!(
            "unknown firing state `{other}`"
        ))),
    }
}
