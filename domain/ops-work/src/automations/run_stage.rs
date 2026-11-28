//! Template rendering and workflow-run stage starter with sandbox and cache-scope enforcement.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::applet::record::{Access, Applet, SubmissionFile};
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

use super::{AutoError, automation_auth_for};
use crate::applets::AppletError;
use crate::applets::backends::{SandboxOperation, Sandboxing, public_ready};
use crate::applets::reads::LoadAppletOperation;
use crate::workflows::CatalogError;
use crate::workflows::inspect::{ValidateValuesOperation, ValuesReport};
use crate::workflows::runs::admit::{
    AdmittedRun, Lineage, PinnedInput, RunRequest, StartRunOperation,
};
use crate::workflows::runs::run::merge_run_inputs;

/// Builds `trigger.*` template variables for a trigger payload using execution binding rules.
pub(crate) fn trigger_vars(trigger: &TriggerPayload) -> BTreeMap<String, Value> {
    let mut map = BTreeMap::new();
    match trigger {
        TriggerPayload::Manual { values_json, .. } => {
            let parsed = serde_json::from_str::<Value>(values_json)
                .unwrap_or_else(|_| Value::String(values_json.clone()));
            if let Value::Object(obj) = &parsed {
                for (key, value) in obj {
                    map.insert(format!("trigger.{key}"), value.clone());
                }
            }
            map.insert("trigger.values".to_string(), parsed);
        }
        TriggerPayload::DataUploaded {
            bucket,
            key,
            version_id,
            path,
            size_bytes,
        } => {
            let file = key.rsplit('/').next().unwrap_or(key).to_string();
            map.insert("trigger.bucket".into(), Value::String(bucket.clone()));
            map.insert("trigger.key".into(), Value::String(key.clone()));
            map.insert(
                "trigger.version_id".into(),
                Value::String(version_id.to_string()),
            );
            map.insert("trigger.path".into(), Value::String(path.clone()));
            map.insert("trigger.filename".into(), Value::String(file));
            map.insert(
                "trigger.size_bytes".into(),
                Value::Number((*size_bytes).into()),
            );
        }
        TriggerPayload::RunFinished {
            workflow_id,
            run_id,
            status,
            ..
        } => {
            map.insert(
                "trigger.workflow_id".into(),
                Value::String(workflow_id.to_string()),
            );
            map.insert("trigger.run_id".into(), Value::String(run_id.to_string()));
            map.insert(
                "trigger.status".into(),
                Value::String(status.as_str().into()),
            );
        }
        TriggerPayload::AppletSubmission {
            applet_id,
            version_id,
            pseudonym,
            bucket,
            key,
            ..
        } => {
            map.insert(
                "trigger.applet_id".into(),
                Value::String(applet_id.to_string()),
            );
            map.insert(
                "trigger.version_id".into(),
                Value::String(version_id.to_string()),
            );
            map.insert("trigger.pseudonym".into(), Value::String(pseudonym.clone()));
            map.insert("trigger.bucket".into(), Value::String(bucket.clone()));
            map.insert("trigger.key".into(), Value::String(key.clone()));
        }
        TriggerPayload::Schedule { at_ms } => {
            let at = i64::try_from(*at_ms)
                .ok()
                .and_then(chrono::DateTime::from_timestamp_millis)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| at_ms.to_string());
            map.insert("trigger.at".into(), Value::String(at));
            map.insert("trigger.at_ms".into(), Value::Number((*at_ms).into()));
        }
    }
    map
}

fn template_values(firing: &Firing) -> BTreeMap<String, Value> {
    let mut map = trigger_vars(&firing.trigger);
    for stage in &firing.stages {
        if let Some(run_id) = stage.receipt.workflow_run_id {
            map.insert(
                format!("stages.{}.run_id", stage.name),
                Value::String(run_id.to_string()),
            );
        }
        if let Some(doc_id) = stage.receipt.dataset_id {
            map.insert(
                format!("stages.{}.dataset_id", stage.name),
                Value::String(doc_id.to_string()),
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
                    Value::String(addr.clone()),
                );
                if idx == 0 {
                    map.insert(
                        format!("stages.{}.first_output", stage.name),
                        Value::String(addr),
                    );
                }
            }
        }
    }
    map
}

fn render_str(raw: &str, vars: &BTreeMap<String, Value>) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            out.push_str(&rest[open..]);
            return out;
        };
        let name = &after[..close];
        if let Some(val) = vars.get(name) {
            match val.as_str() {
                Some(text) => out.push_str(text),
                None => out.push_str(&val.to_string()),
            }
        } else {
            out.push_str(&rest[open..open + 2 + close + 2]);
        }
        rest = &after[close + 2..];
    }
    out.push_str(rest);
    out
}

fn render_json_template(raw: &str, vars: &BTreeMap<String, Value>) -> Result<String, AutoError> {
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            let mut escaped = false;
            while i < bytes.len() {
                if escaped {
                    escaped = false;
                } else if bytes[i] == b'\\' {
                    escaped = true;
                } else if bytes[i] == b'"' {
                    i += 1;
                    break;
                }
                i += 1;
            }
            let decoded: String = serde_json::from_str(&raw[start..i]).map_err(|err| {
                AutoError::Invalid(format!("invalid template JSON string: {err}"))
            })?;
            let is_key = raw[i..].trim_start().starts_with(':');
            if !is_key
                && let Some(inner) = decoded
                    .strip_prefix("{{")
                    .and_then(|s| s.strip_suffix("}}"))
                && !inner.contains("{{")
                && !inner.contains("}}")
                && let Some(typed) = vars.get(inner)
            {
                let encoded = serde_json::to_string(typed)
                    .map_err(|err| AutoError::Invalid(err.to_string()))?;
                out.push_str(&encoded);
            } else {
                let substituted = render_str(&decoded, vars);
                let encoded = serde_json::to_string(&substituted)
                    .map_err(|err| AutoError::Invalid(err.to_string()))?;
                out.push_str(&encoded);
            }
            continue;
        }
        if raw[i..].starts_with("{{")
            && let Some(rel_end) = raw[i + 2..].find("}}")
        {
            let name = &raw[i + 2..i + 2 + rel_end];
            if !name.contains('{')
                && !name.contains('}')
                && !name.contains('"')
                && let Some(typed) = vars.get(name)
            {
                let encoded = serde_json::to_string(typed)
                    .map_err(|err| AutoError::Invalid(err.to_string()))?;
                out.push_str(&encoded);
                i += 2 + rel_end + 2;
                continue;
            }
        }
        let ch = raw[i..]
            .chars()
            .next()
            .ok_or_else(|| AutoError::Invalid("invalid UTF-8 in template JSON".into()))?;
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

/// Resolves `{{trigger.*}}` and `{{stages.<name>.*}}` references inside plain text templates.
pub(crate) fn render_text(raw: &str, firing: &Firing) -> String {
    let vars = template_values(firing);
    render_str(raw, &vars)
}

/// Substitutes `{{...}}` placeholders in both JSON value positions and string literals, then parses.
pub(crate) fn render_json_with(
    raw: &str,
    vars: &BTreeMap<String, Value>,
) -> Result<Value, AutoError> {
    let rendered = render_json_template(raw, vars)?;
    serde_json::from_str(&rendered)
        .map_err(|err| AutoError::Invalid(format!("invalid template JSON: {err}")))
}

/// Substitutes `{{...}}` placeholders from `firing` in JSON templates, then parses.
pub(crate) fn render_json(raw: &str, firing: &Firing) -> Result<Value, AutoError> {
    let vars = template_values(firing);
    render_json_with(raw, &vars)
}

/// Deterministic idempotency key for a run stage attempt so restarts recover already-admitted runs.
pub(crate) fn run_idempotency_key(firing_id: Ulid, stage_name: &str, attempt: u32) -> String {
    format!("auto:{firing_id}:{stage_name}:{attempt}")
}

fn pin_to_json(pin: &PinnedInput) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("bucket".into(), Value::String(pin.bucket.clone()));
    obj.insert("key".into(), Value::String(pin.key.clone()));
    obj.insert(
        "version_id".into(),
        Value::String(pin.version_id.to_string()),
    );
    if let Some(node) = pin.source_node_id {
        obj.insert("source_node_id".into(), Value::String(node.to_string()));
    }
    Value::Object(obj)
}

pub(crate) fn parse_inputs(
    raw: Option<&str>,
    firing: &Firing,
    local: NodeId,
) -> Result<(Vec<PinnedInput>, Option<String>), AutoError> {
    let vars = template_values(firing);
    let files = match &firing.trigger {
        TriggerPayload::AppletSubmission { files, .. } => files.as_slice(),
        _ => &[],
    };
    parse_inputs_with(raw, &vars, files, local)
}

pub(crate) fn parse_inputs_with(
    raw: Option<&str>,
    vars: &BTreeMap<String, Value>,
    files: &[SubmissionFile],
    local: NodeId,
) -> Result<(Vec<PinnedInput>, Option<String>), AutoError> {
    let mut pins = Vec::new();
    let mut grouped: BTreeMap<String, (bool, Vec<Value>)> = BTreeMap::new();
    for file in files {
        let pin = PinnedInput {
            name: file.name.clone(),
            bucket: file.bucket.clone(),
            key: file.key.clone(),
            version_id: file.version_id,
            source_node_id: Some(local),
        };
        grouped
            .entry(pin.name.clone())
            .or_insert_with(|| (false, Vec::new()))
            .1
            .push(pin_to_json(&pin));
        pins.push(pin);
    }
    if let Some(text) = raw {
        let rendered = render_json_with(text, vars)?;
        let Value::Object(map) = &rendered else {
            return Err(AutoError::Invalid(
                "run inputs_json must be a JSON object".into(),
            ));
        };
        for (name, val) in map {
            let (is_array, items): (bool, Vec<&Value>) = match val {
                Value::Array(list) => (true, list.iter().collect()),
                Value::Object(_) => (false, vec![val]),
                _ => {
                    return Err(AutoError::Invalid(format!(
                        "input `{name}` must be an object or array of objects"
                    )));
                }
            };
            let entry = grouped
                .entry(name.clone())
                .or_insert_with(|| (is_array, Vec::new()));
            entry.0 |= is_array;
            for item in items {
                let Some(obj) = item.as_object() else {
                    return Err(AutoError::Invalid(format!(
                        "input `{name}` entry must be an object"
                    )));
                };
                let bucket = obj
                    .get("bucket")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| AutoError::Invalid(format!("input `{name}` missing bucket")))?;
                let key = obj
                    .get("key")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| AutoError::Invalid(format!("input `{name}` missing key")))?;
                let ver = obj
                    .get("version_id")
                    .and_then(Value::as_str)
                    .and_then(|s| Ulid::from_string(s).ok())
                    .ok_or_else(|| {
                        AutoError::Invalid(format!("input `{name}` missing valid ULID version_id"))
                    })?;
                let source_node_id = match obj.get("source_node_id") {
                    None | Some(Value::Null) => Some(local),
                    Some(Value::String(node)) => {
                        let parsed = node.parse::<NodeId>().map_err(|_| {
                            AutoError::Invalid(format!("input `{name}` has invalid source_node_id"))
                        })?;
                        Some(parsed)
                    }
                    Some(_) => {
                        return Err(AutoError::Invalid(format!(
                            "input `{name}` source_node_id must be a string"
                        )));
                    }
                };
                let pin = PinnedInput {
                    name: name.clone(),
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: ver,
                    source_node_id,
                };
                entry.1.push(pin_to_json(&pin));
                pins.push(pin);
            }
        }
    }
    if grouped.is_empty() {
        return Ok((pins, None));
    }
    let map: serde_json::Map<String, Value> = grouped
        .into_iter()
        .map(|(name, (is_array, mut items))| {
            let value = if !is_array && items.len() == 1 {
                items.pop().unwrap_or(Value::Null)
            } else {
                Value::Array(items)
            };
            (name, value)
        })
        .collect();
    let encoded = serde_json::to_string(&Value::Object(map))
        .map_err(|err| AutoError::Invalid(err.to_string()))?;
    Ok((pins, Some(encoded)))
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
        ) = match firing.spec.stages.get(stage_idx).map(|s| &s.kind) {
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
        let values = render_json(&values_json, &firing)?;
        if !values.is_object() {
            return Err(AutoError::Invalid(
                "run values_json must render to a JSON object".into(),
            ));
        }
        let (inputs, rendered_inputs) = parse_inputs(inputs_json.as_deref(), &firing, local.0)?;
        let output = output_bucket.map(|bucket| RunOutput {
            bucket,
            prefix: output_prefix.unwrap_or_default(),
            node_id: Some(local.0),
            dataset_json: None,
        });
        let outside_input = firing.trigger.outside_input();
        Ok(Self {
            auto,
            firing,
            stage_idx,
            local,
            auth: None,
            applet: None,
            outside_input,
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

    pub(crate) fn validation_values(&self) -> Result<Value, AutoError> {
        merge_run_inputs(
            self.local.0,
            &self.values.to_string(),
            self.inputs_json.as_deref(),
        )
        .map_err(|err| AutoError::Invalid(err.to_string()))
    }

    fn validate_call(&mut self) -> Result<Next<Ulid>, AutoError> {
        let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
        let values = self.validation_values()?;
        self.step = RunStep::Validate;
        let check = ValidateValuesOperation::new(auth, self.workflow_id, self.version, values);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn start_call(&mut self) -> Result<Next<Ulid>, AutoError> {
        let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
        let stage_name = self.firing.spec.stages[self.stage_idx].name.clone();
        let mut output = self.output.clone();
        if output.is_none()
            && let TriggerPayload::AppletSubmission {
                pseudonym, bucket, ..
            } = &self.firing.trigger
        {
            let Some(applet) = &self.applet else {
                return Err(AutoError::Conflict(
                    "applet no longer exists for default submission output".into(),
                ));
            };
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
            idempotency_key: Some(run_idempotency_key(
                self.firing.firing_id,
                &stage_name,
                self.firing.attempt,
            )),
            placement_site: self.placement_site.clone(),
            secrets: self.secrets.clone(),
        };
        let origin = AutomationOrigin {
            automation_id: self.auto.automation_id,
            firing_id: self.firing.firing_id,
            stage: stage_name,
            applet_id: self.applet_id(),
            outside_input: self.outside_input,
            depth: self.firing.trigger.chain_depth(),
        };
        let lineage = Lineage {
            cache_scope: CacheScope::Run,
            ..Lineage::default()
        };
        let start = StartRunOperation::new(auth, self.local, req)
            .in_lineage(lineage)
            .with_limits(self.firing.spec.scope.limits.run_limits())
            .with_automation(origin);
        self.step = RunStep::Start;
        Ok(Next::Call(IoCall::child(start)))
    }
}

impl Operation for StartStageRun {
    type Output = Ulid;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Ulid>, AutoError> {
        let auth =
            automation_auth_for(&self.auto, &self.firing.spec, self.local.1).ok_or_else(|| {
                AutoError::Conflict("automation has no active service account".into())
            })?;
        self.auth = Some(auth);
        if let Some(applet_id) = self.applet_id() {
            self.step = RunStep::Applet;
            return Ok(Next::Call(IoCall::child(LoadAppletOperation::new(
                applet_id,
            ))));
        }
        if self.outside_input {
            self.step = RunStep::Sandbox;
            return Ok(Next::Call(IoCall::child(SandboxOperation)));
        }
        self.validate_call()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Ulid>, AutoError> {
        match self.step {
            RunStep::Applet => {
                match result.child::<Option<Applet>, AppletError>()? {
                    Ok(Some(applet)) => {
                        self.outside_input |=
                            applet.access == Access::Public || applet.guests.is_some();
                        self.applet = Some(applet);
                    }
                    Ok(None) => {
                        self.outside_input = true;
                    }
                    Err(err) => return Err(AutoError::Storage(err.to_string())),
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
                Err(CatalogError::Capacity(msg)) => Err(AutoError::Capacity(msg)),
                Err(CatalogError::Conflict(msg)) => Err(AutoError::Conflict(msg)),
                Err(err) => Err(AutoError::Invalid(err.to_string())),
            },
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
