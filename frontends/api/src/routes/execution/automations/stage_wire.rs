//! Wire stage definitions and conversions for automation DAG stages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::automation::record::{Stage, StageKind, When};
use aruna_core::secrets::SecretName;
use aruna_core::structs::execution::workflow_run::{WorkflowSecretBinding, WorkflowSecretScope};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::spec_wire::{SecretRefBody, parse_audience, parse_ulid};
use crate::error::{ServerError, ServerResult};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schema(as = AutomationStageKind)]
pub enum StageKindBody {
    Run {
        workflow_id: String,
        version: u32,
        #[serde(default = "empty_obj")]
        values_json: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inputs_json: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_bucket: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output_prefix: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        placement_site: Option<String>,
        #[serde(default)]
        secrets: BTreeMap<String, SecretRefBody>,
        #[serde(default)]
        provenance_crate: bool,
    },
    Copy {
        from_stage: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file: Option<String>,
        dest_bucket: String,
        dest_template: String,
        #[serde(default)]
        allow_downgrade: bool,
    },
    Dataset {
        from_stage: String,
        metadata_path: String,
        name_template: String,
        #[serde(default)]
        description: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        license: Option<String>,
        #[serde(default)]
        public: bool,
    },
    Notify {
        audience: String,
        message: String,
    },
    Approval {
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
}

fn empty_obj() -> String {
    "{}".into()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = AutomationStage)]
pub struct StageBody {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needs: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    #[serde(flatten)]
    pub kind: StageKindBody,
}

fn parse_when(text: Option<&str>) -> ServerResult<When> {
    match text {
        None | Some("passed") => Ok(When::Passed),
        Some("failed") => Ok(When::Failed),
        Some("always") => Ok(When::Always),
        Some(other) => Err(ServerError::BadRequestReason(format!(
            "unknown stage condition `{other}`"
        ))),
    }
}

pub(crate) fn stage_body(stage: &Stage) -> StageBody {
    let kind = match &stage.kind {
        StageKind::Run {
            workflow_id,
            version,
            values_json,
            inputs_json,
            output_bucket,
            output_prefix,
            placement_site,
            secrets,
            provenance_crate,
        } => StageKindBody::Run {
            workflow_id: workflow_id.to_string(),
            version: *version,
            values_json: values_json.clone(),
            inputs_json: inputs_json.clone(),
            output_bucket: output_bucket.clone(),
            output_prefix: output_prefix.clone(),
            placement_site: placement_site.clone(),
            secrets: secrets
                .iter()
                .map(|(k, v)| {
                    let scope = match v.scope {
                        WorkflowSecretScope::User => "user",
                        WorkflowSecretScope::Group => "group",
                        WorkflowSecretScope::Realm => "realm",
                        WorkflowSecretScope::Node => "node",
                    };
                    (
                        k.clone(),
                        SecretRefBody {
                            scope: scope.into(),
                            name: v.name.to_string(),
                            field: v.field.clone(),
                        },
                    )
                })
                .collect(),
            provenance_crate: *provenance_crate,
        },
        StageKind::Copy {
            from_stage,
            file,
            dest_bucket,
            dest_template,
            allow_downgrade,
        } => StageKindBody::Copy {
            from_stage: from_stage.clone(),
            file: file.clone(),
            dest_bucket: dest_bucket.clone(),
            dest_template: dest_template.clone(),
            allow_downgrade: *allow_downgrade,
        },
        StageKind::Dataset {
            from_stage,
            metadata_path,
            name_template,
            description,
            license,
            public,
        } => StageKindBody::Dataset {
            from_stage: from_stage.clone(),
            metadata_path: metadata_path.clone(),
            name_template: name_template.clone(),
            description: description.clone(),
            license: license.clone(),
            public: *public,
        },
        StageKind::Notify { audience, message } => StageKindBody::Notify {
            audience: audience.as_str().into(),
            message: message.clone(),
        },
        StageKind::Approval {
            message,
            timeout_ms,
        } => StageKindBody::Approval {
            message: message.clone(),
            timeout_ms: *timeout_ms,
        },
    };
    StageBody {
        name: stage.name.clone(),
        needs: stage.needs.clone(),
        when: Some(stage.when.as_str().into()),
        kind,
    }
}

pub(crate) fn stage_domain(body: &StageBody) -> ServerResult<Stage> {
    let kind = match &body.kind {
        StageKindBody::Run {
            workflow_id,
            version,
            values_json,
            inputs_json,
            output_bucket,
            output_prefix,
            placement_site,
            secrets,
            provenance_crate,
        } => {
            let mut mapped = BTreeMap::new();
            for (k, v) in secrets {
                let scope = match v.scope.as_str() {
                    "user" => WorkflowSecretScope::User,
                    "group" => WorkflowSecretScope::Group,
                    "realm" => WorkflowSecretScope::Realm,
                    "node" => WorkflowSecretScope::Node,
                    other => {
                        return Err(ServerError::BadRequestReason(format!(
                            "unknown secret scope `{other}`"
                        )));
                    }
                };
                let name = SecretName::try_from(v.name.clone())
                    .map_err(|e| ServerError::BadRequestReason(e.to_string()))?;
                mapped.insert(
                    k.clone(),
                    WorkflowSecretBinding {
                        scope,
                        name,
                        field: v.field.clone(),
                    },
                );
            }
            StageKind::Run {
                workflow_id: parse_ulid(workflow_id, "workflow_id")?,
                version: *version,
                values_json: values_json.clone(),
                inputs_json: inputs_json.clone(),
                output_bucket: output_bucket.clone(),
                output_prefix: output_prefix.clone(),
                placement_site: placement_site.clone(),
                secrets: mapped,
                provenance_crate: *provenance_crate,
            }
        }
        StageKindBody::Copy {
            from_stage,
            file,
            dest_bucket,
            dest_template,
            allow_downgrade,
        } => StageKind::Copy {
            from_stage: from_stage.clone(),
            file: file.clone(),
            dest_bucket: dest_bucket.clone(),
            dest_template: dest_template.clone(),
            allow_downgrade: *allow_downgrade,
        },
        StageKindBody::Dataset {
            from_stage,
            metadata_path,
            name_template,
            description,
            license,
            public,
        } => StageKind::Dataset {
            from_stage: from_stage.clone(),
            metadata_path: metadata_path.clone(),
            name_template: name_template.clone(),
            description: description.clone(),
            license: license.clone(),
            public: *public,
        },
        StageKindBody::Notify { audience, message } => StageKind::Notify {
            audience: parse_audience(audience)?,
            message: message.clone(),
        },
        StageKindBody::Approval {
            message,
            timeout_ms,
        } => StageKind::Approval {
            message: message.clone(),
            timeout_ms: *timeout_ms,
        },
    };
    Ok(Stage {
        name: body.name.clone(),
        needs: body.needs.clone(),
        when: parse_when(body.when.as_deref())?,
        kind,
    })
}
