//! Wire schemas for automation firings, stage receipts, resolutions and kept event replay.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::{
    CopiedFile, Firing, FiringState, Resolution, StageReceipt, StageRun,
};
use aruna_core::automation::record::KeptEvent;
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
#[schema(as = AutomationRun)]
pub struct FiringBody {
    pub firing_id: String,
    pub automation_id: String,
    pub group_id: String,
    pub revision: u64,
    pub attempt: u32,
    pub state: String,
    pub trigger_kind: String,
    pub stages: Vec<StageRunBody>,
    pub resolutions: Vec<ResolutionBody>,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
    pub error: Option<String>,
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
            stages: firing
                .stages
                .iter()
                .map(StageRunBody::from_domain)
                .collect(),
            resolutions: firing
                .resolutions
                .iter()
                .map(|r: &Resolution| ResolutionBody {
                    stage: r.stage.clone(),
                    done: r.done,
                    actor: r.actor.to_string(),
                    note: r.note.clone(),
                    at: rfc3339(r.at_ms),
                })
                .collect(),
            created_at: rfc3339(firing.created_at_ms),
            updated_at: rfc3339(firing.updated_at_ms),
            finished_at: firing.finished_at_ms.map(rfc3339),
            error: firing.error.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationRunPage)]
pub struct FiringPageBody {
    pub items: Vec<FiringBody>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct FiringQuery {
    pub group_id: Option<String>,
    pub automation_id: Option<String>,
    pub state: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = StartAutomationRequest)]
pub struct StartRequest {
    #[schema(value_type = Option<Object>)]
    pub values: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = ResolveStageRequest)]
pub struct ResolveRequest {
    pub stage: String,
    pub done: bool,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = DecideStageRequest)]
pub struct DecideRequest {
    pub note: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AutomationKeptEvent)]
pub struct KeptEventBody {
    pub event_id: String,
    pub kind: String,
    pub resource: String,
    pub revision: String,
    pub reason: String,
    pub kept_at: String,
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
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = StartKeptEventsRequest)]
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
        other => Err(ServerError::BadRequestReason(format!(
            "unknown firing state `{other}`"
        ))),
    }
}
