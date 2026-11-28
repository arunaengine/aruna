//! Wire types for automation triggers, conditions, scope, limits and top-level specs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::record::{RunStatus, Spec, Trigger};
use aruna_core::automation::rules::{
    Condition, ConditionMode, Conditions, ScheduleEvery, ScheduleSpec, Weekday,
};
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
        /// Omit to match any applet of the group that lists this automation in its settings.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        applet_id: Option<String>,
    },
    Schedule {
        /// `"day"`, `"week"` or `"hours"`.
        every: String,
        /// Wall-clock anchor `"HH:MM"` (`00:00`..=`23:59`).
        at: String,
        /// Selected weekdays (`mon`..`sun`) when `every` is `"week"`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        days: Vec<String>,
        /// Hour step (`1..=24`) when `every` is `"hours"`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hours: Option<u8>,
        /// Required IANA time zone name (e.g. `"Europe/Berlin"`).
        #[serde(default)]
        zone: Option<String>,
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
                applet_id: applet_id.map(|id| id.to_string()),
            },
            Trigger::Schedule(sched) => Self::Schedule {
                every: sched.every.as_str().into(),
                at: sched.at.clone(),
                days: sched.days.iter().map(|d| d.as_str().into()).collect(),
                hours: sched.hours,
                zone: Some(sched.zone.clone()),
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
                applet_id: applet_id
                    .as_deref()
                    .map(|id| parse_ulid(id, "applet_id"))
                    .transpose()?,
            },
            Self::Schedule {
                every,
                at,
                days,
                hours,
                zone,
            } => {
                let every = ScheduleEvery::parse(every).ok_or_else(|| {
                    ServerError::BadRequestReason(format!(
                        "unknown schedule `every` value `{every}` (expected day, week or hours)"
                    ))
                })?;
                let parsed_days = days
                    .iter()
                    .map(|d| parse_weekday(d))
                    .collect::<ServerResult<Vec<_>>>()?;
                let zone = require_zone(zone.as_deref(), "schedule trigger")?;
                Trigger::Schedule(ScheduleSpec {
                    every,
                    at: at.clone(),
                    days: parsed_days,
                    hours: *hours,
                    zone,
                })
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schema(as = AutomationCondition)]
pub enum ConditionBody {
    TimeOfDay {
        from: String,
        to: String,
        #[serde(default)]
        zone: Option<String>,
    },
    Weekdays {
        days: Vec<String>,
        #[serde(default)]
        zone: Option<String>,
    },
    File {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pattern: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_bytes: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_bytes: Option<u64>,
    },
}

impl ConditionBody {
    pub fn from_domain(cond: &Condition) -> Self {
        match cond {
            Condition::TimeOfDay { from, to, zone } => Self::TimeOfDay {
                from: from.clone(),
                to: to.clone(),
                zone: Some(zone.clone()),
            },
            Condition::Weekdays { days, zone } => Self::Weekdays {
                days: days.iter().map(|d| d.as_str().into()).collect(),
                zone: Some(zone.clone()),
            },
            Condition::File {
                pattern,
                min_bytes,
                max_bytes,
            } => Self::File {
                pattern: pattern.clone(),
                min_bytes: *min_bytes,
                max_bytes: *max_bytes,
            },
        }
    }

    pub fn to_domain(&self) -> ServerResult<Condition> {
        Ok(match self {
            Self::TimeOfDay { from, to, zone } => Condition::TimeOfDay {
                from: from.clone(),
                to: to.clone(),
                zone: require_zone(zone.as_deref(), "time_of_day condition")?,
            },
            Self::Weekdays { days, zone } => Condition::Weekdays {
                days: days
                    .iter()
                    .map(|d| parse_weekday(d))
                    .collect::<ServerResult<Vec<_>>>()?,
                zone: require_zone(zone.as_deref(), "weekdays condition")?,
            },
            Self::File {
                pattern,
                min_bytes,
                max_bytes,
            } => Condition::File {
                pattern: pattern.clone(),
                min_bytes: *min_bytes,
                max_bytes: *max_bytes,
            },
        })
    }
}

fn default_mode() -> String {
    ConditionMode::All.as_str().into()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
#[schema(as = AutomationConditions)]
pub struct ConditionsBody {
    /// `"all"` (every condition must hold) or `"any"` (at least one applicable condition holds).
    pub mode: String,
    pub items: Vec<ConditionBody>,
}

impl Default for ConditionsBody {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            items: Vec::new(),
        }
    }
}

impl ConditionsBody {
    pub fn from_domain(conds: &Conditions) -> Self {
        Self {
            mode: conds.mode.as_str().into(),
            items: conds.items.iter().map(ConditionBody::from_domain).collect(),
        }
    }

    pub fn to_domain(&self) -> ServerResult<Conditions> {
        let mode = ConditionMode::parse(&self.mode).ok_or_else(|| {
            ServerError::BadRequestReason(format!(
                "unknown condition mode `{}` (expected all or any)",
                self.mode
            ))
        })?;
        let items = self
            .items
            .iter()
            .map(ConditionBody::to_domain)
            .collect::<ServerResult<Vec<_>>>()?;
        Ok(Conditions { mode, items })
    }
}

/// Effective numeric limits enforced at runtime (combining explicit overrides and node defaults).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = AutomationEffectiveLimits)]
pub struct EffectiveLimitsBody {
    pub max_active_firings: u32,
    pub max_concurrent_workflows: u32,
    pub per_hour: u32,
    pub per_day: u32,
    pub bytes_per_day: u64,
    pub max_chain_depth: u8,
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
    /// Read-only effective limits after applying node defaults; ignored on write requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective: Option<EffectiveLimitsBody>,
}

impl LimitsBody {
    pub fn from_domain(limits: &Limits) -> Self {
        Self {
            max_active_firings: limits.max_active_firings,
            max_concurrent_workflows: limits.max_concurrent_workflows,
            per_hour: limits.per_hour,
            per_day: limits.per_day,
            bytes_per_day: limits.bytes_per_day,
            cpu_cores: limits.cpu_cores,
            ram_bytes: limits.ram_bytes,
            walltime_ms: limits.walltime_ms,
            max_chain_depth: limits.max_chain_depth,
            effective: Some(EffectiveLimitsBody {
                max_active_firings: limits.active_firings(),
                max_concurrent_workflows: limits.concurrent_workflows(),
                per_hour: limits.hour_limit(),
                per_day: limits.day_limit(),
                bytes_per_day: limits.copy_bytes_day(),
                max_chain_depth: limits.chain_depth(),
            }),
        }
    }

    pub fn to_domain(&self) -> Limits {
        Limits {
            max_active_firings: self.max_active_firings,
            max_concurrent_workflows: self.max_concurrent_workflows,
            per_hour: self.per_hour,
            per_day: self.per_day,
            bytes_per_day: self.bytes_per_day,
            cpu_cores: self.cpu_cores,
            ram_bytes: self.ram_bytes,
            walltime_ms: self.walltime_ms,
            max_chain_depth: self.max_chain_depth,
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
    pub conditions: ConditionsBody,
    pub stages: Vec<StageBody>,
    pub scope: ScopeBody,
}

impl SpecBody {
    pub fn from_domain(spec: &Spec) -> Self {
        Self {
            triggers: spec.triggers.iter().map(TriggerBody::from_domain).collect(),
            conditions: ConditionsBody::from_domain(&spec.conditions),
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
        let conditions = self.conditions.to_domain()?;
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
            conditions,
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

fn require_zone(zone: Option<&str>, label: &str) -> ServerResult<String> {
    let trimmed = zone.map(str::trim).unwrap_or_default();
    if trimmed.is_empty() {
        return Err(ServerError::BadRequestReason(format!(
            "{label} needs an IANA time zone in `zone`"
        )));
    }
    Ok(trimmed.to_string())
}

fn parse_weekday(text: &str) -> ServerResult<Weekday> {
    Weekday::parse(text).ok_or_else(|| {
        ServerError::BadRequestReason(format!(
            "unknown weekday `{text}` (expected mon, tue, wed, thu, fri, sat or sun)"
        ))
    })
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
