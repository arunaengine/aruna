//! Template rendering and workflow-run stage starter with sandbox and cache-scope enforcement.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::applet::record::{Access, Applet};
use aruna_core::automation::firing::{Firing, TriggerPayload};
use aruna_core::automation::record::{Automation, StageKind};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{
    AutomationOrigin, CacheScope, RunOutput, WorkflowSecretBinding,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::workflow::WorkflowError;
use serde_json::Value;
use ulid::Ulid;

use super::{AutoError, automation_auth};
use crate::applets::AppletError;
use crate::applets::backends::{SandboxOperation, Sandboxing, public_ready};
use crate::applets::reads::LoadAppletOperation;
use crate::workflows::CatalogError;
use crate::workflows::inspect::{ValidateValuesOperation, ValuesReport};
use crate::workflows::runs::admit::{
    AdmittedRun, Lineage, PinnedInput, RunRequest, StartRunOperation,
};

/// Resolves `{{trigger.*}}` and `{{stages.<name>.*}}` references inside JSON or text templates.
pub(crate) fn render_text(raw: &str, firing: &Firing) -> String {
    let mut map = BTreeMap::new();
    match &firing.trigger {
        TriggerPayload::Manual { values_json, .. } => {
            map.insert("trigger.values".to_string(), values_json.clone());
            if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(values_json) {
                for (key, value) in obj {
                    let text = value
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| value.to_string());
                    map.insert(format!("trigger.{key}"), text);
                }
            }
        }
        TriggerPayload::DataUploaded {
            bucket,
            key,
            version_id,
            path,
            ..
        } => {
            let file = key.rsplit('/').next().unwrap_or(key).to_string();
            map.insert("trigger.bucket".into(), bucket.clone());
            map.insert("trigger.key".into(), key.clone());
            map.insert("trigger.version_id".into(), version_id.to_string());
            map.insert("trigger.path".into(), path.clone());
            map.insert("trigger.filename".into(), file);
        }
        TriggerPayload::RunFinished {
            workflow_id,
            run_id,
            status,
            ..
        } => {
            map.insert("trigger.workflow_id".into(), workflow_id.to_string());
            map.insert("trigger.run_id".into(), run_id.to_string());
            map.insert("trigger.status".into(), status.as_str().into());
        }
        TriggerPayload::AppletSubmission {
            applet_id,
            version_id,
            pseudonym,
            bucket,
            key,
            ..
        } => {
            map.insert("trigger.applet_id".into(), applet_id.to_string());
            map.insert("trigger.version_id".into(), version_id.to_string());
            map.insert("trigger.pseudonym".into(), pseudonym.clone());
            map.insert("trigger.bucket".into(), bucket.clone());
            map.insert("trigger.key".into(), key.clone());
        }
    }
    for stage in &firing.stages {
        if let Some(run_id) = stage.receipt.workflow_run_id {
            map.insert(format!("stages.{}.run_id", stage.name), run_id.to_string());
        }
        if let Some(doc_id) = stage.receipt.dataset_id {
            map.insert(
                format!("stages.{}.dataset_id", stage.name),
                doc_id.to_string(),
            );
        }
        for (idx, file) in stage.receipt.outputs.iter().enumerate() {
            if let (Some(bucket), Some(key)) = (&file.bucket, &file.key) {
                let addr = match &file.version_id {
                    Some(ver) => format!("aruna://{bucket}/{key}?version={ver}"),
                    None => format!("aruna://{bucket}/{key}"),
                };
                map.insert(
                    format!("stages.{}.output.{}", stage.name, file.name),
                    addr.clone(),
                );
                if idx == 0 {
                    map.insert(format!("stages.{}.first_output", stage.name), addr);
                }
            }
        }
    }
    let mut out = raw.to_string();
    for (needle, val) in map {
        out = out.replace(&format!("{{{{{needle}}}}}"), &val);
    }
    out
}

fn parse_inputs(raw: Option<&str>, firing: &Firing, local: NodeId) -> Vec<PinnedInput> {
    let mut pins = Vec::new();
    if let TriggerPayload::AppletSubmission { files, .. } = &firing.trigger {
        for file in files {
            pins.push(PinnedInput {
                name: file.name.clone(),
                bucket: file.bucket.clone(),
                key: file.key.clone(),
                version_id: file.version_id,
                source_node_id: Some(local),
            });
        }
    }
    let Some(text) = raw else {
        return pins;
    };
    let rendered = render_text(text, firing);
    let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&rendered) else {
        return pins;
    };
    for (name, val) in map {
        let items = match val {
            Value::Array(list) => list,
            other => vec![other],
        };
        for item in items {
            let (Some(bucket), Some(key), Some(ver)) = (
                item["bucket"].as_str(),
                item["key"].as_str(),
                item["version_id"]
                    .as_str()
                    .and_then(|s| Ulid::from_string(s).ok()),
            ) else {
                continue;
            };
            pins.push(PinnedInput {
                name: name.clone(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: ver,
                source_node_id: Some(local),
            });
        }
    }
    pins
}

#[derive(Debug, PartialEq)]
enum RunStep {
    Applet,
    Sandbox,
    Validate,
    Start,
}

/// Starts one `run` stage as the automation's service account, validating values and enforcing
/// sandboxed backends plus `CacheScope::Run` when inputs originate from an outside applet.
#[derive(Debug, PartialEq)]
pub struct StartStageRun {
    auto: Automation,
    firing: Firing,
    stage_idx: usize,
    local: (NodeId, RealmId),
    auth: Option<AuthContext>,
    applet: Option<Applet>,
    outside_input: bool,
    workflow_id: Ulid,
    version: u32,
    values: Value,
    inputs: Vec<PinnedInput>,
    inputs_json: Option<String>,
    output: Option<RunOutput>,
    placement_site: Option<String>,
    secrets: BTreeMap<String, WorkflowSecretBinding>,
    provenance_crate: bool,
    step: RunStep,
}

impl StartStageRun {
    pub fn new(
        auto: Automation,
        firing: Firing,
        stage_idx: usize,
        local: (NodeId, RealmId),
    ) -> Result<Self, AutoError> {
        let (
            workflow_id,
            version,
            values_json,
            inputs_json,
            output_bucket,
            output_prefix,
            placement_site,
            secrets,
            provenance_crate,
        ) = match auto.spec.stages.get(stage_idx).map(|s| &s.kind) {
            Some(StageKind::Run {
                workflow_id,
                version,
                values_json,
                inputs_json,
                output_bucket,
                output_prefix,
                placement_site,
                secrets,
                provenance_crate,
            }) => (
                *workflow_id,
                *version,
                values_json.clone(),
                inputs_json.clone(),
                output_bucket.clone(),
                output_prefix.clone(),
                placement_site.clone(),
                secrets.clone(),
                *provenance_crate,
            ),
            Some(_) => return Err(AutoError::Invalid("stage is not a run stage".into())),
            None => return Err(AutoError::Invalid("missing run stage".into())),
        };
        let rendered = render_text(&values_json, &firing);
        let values: Value = serde_json::from_str(&rendered)
            .map_err(|err| AutoError::Invalid(format!("invalid run values JSON: {err}")))?;
        let inputs = parse_inputs(inputs_json.as_deref(), &firing, local.0);
        let output = output_bucket.map(|bucket| RunOutput {
            bucket,
            prefix: output_prefix.unwrap_or_default(),
            node_id: Some(local.0),
            dataset_json: None,
        });
        let rendered_inputs = inputs_json.as_deref().map(|t| render_text(t, &firing));
        Ok(Self {
            auto,
            firing,
            stage_idx,
            local,
            auth: None,
            applet: None,
            outside_input: false,
            workflow_id,
            version,
            values,
            inputs,
            inputs_json: rendered_inputs,
            output,
            placement_site,
            secrets,
            provenance_crate,
            step: RunStep::Applet,
        })
    }

    fn applet_id(&self) -> Option<Ulid> {
        match &self.firing.trigger {
            TriggerPayload::AppletSubmission { applet_id, .. } => Some(*applet_id),
            TriggerPayload::RunFinished { applet_id, .. } => *applet_id,
            _ => None,
        }
    }

    fn validate_call(&mut self) -> Result<Next<Ulid>, AutoError> {
        let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
        self.step = RunStep::Validate;
        let check =
            ValidateValuesOperation::new(auth, self.workflow_id, self.version, self.values.clone());
        Ok(Next::Call(IoCall::child(check)))
    }

    fn start_call(&mut self) -> Result<Next<Ulid>, AutoError> {
        let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
        let stage_name = self.auto.spec.stages[self.stage_idx].name.clone();
        let mut output = self.output.clone();
        if output.is_none()
            && let (
                TriggerPayload::AppletSubmission {
                    pseudonym, bucket, ..
                },
                Some(applet),
            ) = (&self.firing.trigger, &self.applet)
        {
            let sub = applet
                .grants
                .submissions
                .as_ref()
                .map(|f| f.prefix.trim_end_matches('/'))
                .unwrap_or_default();
            let prefix = match sub {
                "" => format!("{pseudonym}/runs"),
                p => format!("{p}/{pseudonym}/runs"),
            };
            output = Some(RunOutput {
                bucket: bucket.clone(),
                prefix,
                node_id: Some(self.local.0),
                dataset_json: None,
            });
        }
        let req = RunRequest {
            group_id: self.auto.group_id,
            workflow_id: self.workflow_id,
            version: self.version,
            values: self.values.clone(),
            inputs: self.inputs.clone(),
            inputs_json: self.inputs_json.clone(),
            output,
            provenance_crate: self.provenance_crate,
            idempotency_key: Some(format!(
                "auto:{}:{}:{}",
                self.firing.firing_id, stage_name, self.firing.attempt
            )),
            placement_site: self.placement_site.clone(),
            secrets: self.secrets.clone(),
        };
        let origin = AutomationOrigin {
            automation_id: self.auto.automation_id,
            firing_id: self.firing.firing_id,
            stage: stage_name,
            applet_id: self.applet_id(),
            depth: self.firing.trigger.chain_depth(),
        };
        let lineage = Lineage {
            cache_scope: CacheScope::Run,
            ..Lineage::default()
        };
        let start = StartRunOperation::new(auth, self.local, req)
            .in_lineage(lineage)
            .with_limits(self.auto.spec.scope.limits.run_limits())
            .with_automation(origin);
        self.step = RunStep::Start;
        Ok(Next::Call(IoCall::child(start)))
    }
}

impl Operation for StartStageRun {
    type Output = Ulid;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Ulid>, AutoError> {
        let auth = automation_auth(&self.auto, self.local.1).ok_or_else(|| {
            AutoError::Conflict("automation has no active service account".into())
        })?;
        self.auth = Some(auth);
        if let Some(applet_id) = self.applet_id() {
            self.step = RunStep::Applet;
            return Ok(Next::Call(IoCall::child(LoadAppletOperation::new(
                applet_id,
            ))));
        }
        self.validate_call()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Ulid>, AutoError> {
        match self.step {
            RunStep::Applet => {
                if let Ok(Some(applet)) = result.child::<Option<Applet>, AppletError>()? {
                    self.outside_input = applet.access == Access::Public || applet.guests.is_some();
                    self.applet = Some(applet);
                }
                if self.outside_input {
                    self.step = RunStep::Sandbox;
                    return Ok(Next::Call(IoCall::child(SandboxOperation)));
                }
                self.validate_call()
            }
            RunStep::Sandbox => {
                let backends = result
                    .child::<Vec<Sandboxing>, AppletError>()?
                    .unwrap_or_default();
                if !public_ready(&backends) {
                    return Err(AutoError::Forbidden);
                }
                self.validate_call()
            }
            RunStep::Validate => {
                match result.child::<ValuesReport, CatalogError>()? {
                    Ok(report) if !report.ok => {
                        let msg = report
                            .diagnostics
                            .first()
                            .and_then(|d| d["message"].as_str())
                            .unwrap_or("workflow values failed validation");
                        return Err(AutoError::Invalid(msg.to_string()));
                    }
                    Err(CatalogError::Coordinator(WorkflowError::Unavailable)) => {}
                    Err(CatalogError::NotFound) => return Err(AutoError::NotFound),
                    Err(CatalogError::Forbidden) => return Err(AutoError::Forbidden),
                    Err(err) => return Err(AutoError::Invalid(err.to_string())),
                    Ok(_) => {}
                }
                self.start_call()
            }
            RunStep::Start => match result.child::<AdmittedRun, CatalogError>()? {
                Ok(admitted) => Ok(Next::Done(admitted.record.run_id)),
                Err(CatalogError::Forbidden) => Err(AutoError::Forbidden),
                Err(CatalogError::Conflict(msg)) => Err(AutoError::Conflict(msg)),
                Err(err) => Err(AutoError::Invalid(err.to_string())),
            },
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
