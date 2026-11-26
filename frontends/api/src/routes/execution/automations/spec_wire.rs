//! Wire types for automation triggers, scope, limits and top-level specs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::record::{RunStatus, Spec, Trigger};
use aruna_core::automation::scope::{Audience, Limits, Scope};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;

use super::stage_wire::{StageBody, stage_body, stage_domain};
use crate::error::{ServerError, ServerResult};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = AutomationWorkflowRef)]
pub struct WorkflowRefBody {
    pub workflow_id: String,
    pub version: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = AutomationSecretRef)]
pub struct SecretRefBody {
    pub scope: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schema(as = AutomationTrigger)]
pub enum TriggerBody {
    Manual {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        form_json: Option<String>,
    },
    DataUploaded {
        bucket: String,
        #[serde(default)]
        prefix: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pattern: Option<String>,
    },
    RunFinished {
        workflow_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        applet_id: Option<String>,
    },
    AppletSubmission {
        applet_id: String,
    },
}

impl TriggerBody {
    pub fn from_domain(trigger: &Trigger) -> Self {
        match trigger {
            Trigger::Manual { form_json } => Self::Manual {
                form_json: form_json.clone(),
            },
            Trigger::DataUploaded {
                bucket,
                prefix,
                pattern,
            } => Self::DataUploaded {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                pattern: pattern.clone(),
            },
            Trigger::RunFinished {
                workflow_id,
                status,
                applet_id,
            } => Self::RunFinished {
                workflow_id: workflow_id.to_string(),
                status: status.map(|s| s.as_str().into()),
                applet_id: applet_id.map(|id| id.to_string()),
            },
            Trigger::AppletSubmission { applet_id } => Self::AppletSubmission {
                applet_id: applet_id.to_string(),
            },
        }
    }

    pub fn to_domain(&self) -> ServerResult<Trigger> {
        Ok(match self {
            Self::Manual { form_json } => Trigger::Manual {
                form_json: form_json.clone(),
            },
            Self::DataUploaded {
                bucket,
                prefix,
                pattern,
            } => Trigger::DataUploaded {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                pattern: pattern.clone(),
            },
            Self::RunFinished {
                workflow_id,
                status,
                applet_id,
            } => Trigger::RunFinished {
                workflow_id: parse_ulid(workflow_id, "workflow_id")?,
                status: status.as_deref().map(parse_status).transpose()?,
                applet_id: applet_id
                    .as_deref()
                    .map(|id| parse_ulid(id, "applet_id"))
                    .transpose()?,
            },
            Self::AppletSubmission { applet_id } => Trigger::AppletSubmission {
                applet_id: parse_ulid(applet_id, "applet_id")?,
            },
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = AutomationLimits)]
pub struct LimitsBody {
    pub max_active_firings: Option<u32>,
    pub max_concurrent_workflows: Option<u32>,
    pub per_hour: Option<u32>,
    pub per_day: Option<u32>,
    pub bytes_per_day: Option<u64>,
    pub cpu_cores: Option<u32>,
    pub ram_bytes: Option<u64>,
    pub walltime_ms: Option<u64>,
    pub max_chain_depth: Option<u8>,
}

impl LimitsBody {
    pub fn from_domain(limits: &Limits) -> Self {
        Self {
            max_active_firings: Some(limits.max_active_firings),
            max_concurrent_workflows: Some(limits.max_concurrent_workflows),
            per_hour: Some(limits.per_hour),
            per_day: Some(limits.per_day),
            bytes_per_day: Some(limits.bytes_per_day),
            cpu_cores: limits.cpu_cores,
            ram_bytes: limits.ram_bytes,
            walltime_ms: limits.walltime_ms,
            max_chain_depth: Some(limits.max_chain_depth),
        }
    }

    pub fn to_domain(&self) -> Limits {
        let def = Limits::default();
        Limits {
            max_active_firings: self.max_active_firings.unwrap_or(def.max_active_firings),
            max_concurrent_workflows: self
                .max_concurrent_workflows
                .unwrap_or(def.max_concurrent_workflows),
            per_hour: self.per_hour.unwrap_or(def.per_hour),
            per_day: self.per_day.unwrap_or(def.per_day),
            bytes_per_day: self.bytes_per_day.unwrap_or(def.bytes_per_day),
            cpu_cores: self.cpu_cores,
            ram_bytes: self.ram_bytes,
            walltime_ms: self.walltime_ms,
            max_chain_depth: self.max_chain_depth.unwrap_or(def.max_chain_depth),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = AutomationScope)]
pub struct ScopeBody {
    pub read: Vec<String>,
    pub write: Vec<String>,
    pub metadata: Vec<String>,
    pub workflows: Vec<WorkflowRefBody>,
    pub audiences: Vec<String>,
    pub secrets: Vec<String>,
    pub allow_downgrade: bool,
    pub limits: LimitsBody,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = AutomationSpec)]
pub struct SpecBody {
    pub triggers: Vec<TriggerBody>,
    pub stages: Vec<StageBody>,
    pub scope: ScopeBody,
}

impl SpecBody {
    pub fn from_domain(spec: &Spec) -> Self {
        Self {
            triggers: spec.triggers.iter().map(TriggerBody::from_domain).collect(),
            stages: spec.stages.iter().map(stage_body).collect(),
            scope: ScopeBody {
                read: spec.scope.read.clone(),
                write: spec.scope.write.clone(),
                metadata: spec.scope.metadata.clone(),
                workflows: spec
                    .scope
                    .workflows
                    .iter()
                    .map(|(id, version)| WorkflowRefBody {
                        workflow_id: id.to_string(),
                        version: *version,
                    })
                    .collect(),
                audiences: spec
                    .scope
                    .audiences
                    .iter()
                    .map(|a| a.as_str().into())
                    .collect(),
                secrets: spec.scope.secrets.clone(),
                allow_downgrade: spec.scope.allow_downgrade,
                limits: LimitsBody::from_domain(&spec.scope.limits),
            },
        }
    }

    pub fn to_domain(&self) -> ServerResult<Spec> {
        let triggers = self
            .triggers
            .iter()
            .map(TriggerBody::to_domain)
            .collect::<ServerResult<Vec<_>>>()?;
        let stages = self
            .stages
            .iter()
            .map(stage_domain)
            .collect::<ServerResult<Vec<_>>>()?;
        let workflows = self
            .scope
            .workflows
            .iter()
            .map(|w| Ok((parse_ulid(&w.workflow_id, "workflow_id")?, w.version)))
            .collect::<ServerResult<Vec<_>>>()?;
        let audiences = self
            .scope
            .audiences
            .iter()
            .map(|a| parse_audience(a))
            .collect::<ServerResult<Vec<_>>>()?;
        Ok(Spec {
            triggers,
            stages,
            scope: Scope {
                read: self.scope.read.clone(),
                write: self.scope.write.clone(),
                metadata: self.scope.metadata.clone(),
                workflows,
                audiences,
                secrets: self.scope.secrets.clone(),
                allow_downgrade: self.scope.allow_downgrade,
                limits: self.scope.limits.to_domain(),
            },
        })
    }
}

pub(crate) fn parse_ulid(text: &str, name: &str) -> ServerResult<Ulid> {
    Ulid::from_string(text)
        .map_err(|_| ServerError::BadRequestReason(format!("{name} is not a ULID")))
}

fn parse_status(text: &str) -> ServerResult<RunStatus> {
    match text {
        "succeeded" => Ok(RunStatus::Succeeded),
        "failed" => Ok(RunStatus::Failed),
        other => Err(ServerError::BadRequestReason(format!(
            "unknown run status `{other}`"
        ))),
    }
}

pub(crate) fn parse_audience(text: &str) -> ServerResult<Audience> {
    match text {
        "group_admins" => Ok(Audience::GroupAdmins),
        "group_members" => Ok(Audience::GroupMembers),
        other => Err(ServerError::BadRequestReason(format!(
            "unknown audience `{other}`"
        ))),
    }
}
