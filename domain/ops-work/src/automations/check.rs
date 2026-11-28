//! Side-effect-free dry-run check of an automation DAG, workflow forms and copy destinations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::applet::record::{Access, Applet, Lifecycle as AppletLifecycle, SubmissionFile};
use aruna_core::automation::dag::{validate_scope, validate_spec};
use aruna_core::automation::firing::TriggerPayload;
use aruna_core::automation::glob::{expand_copy, format_date};
use aruna_core::automation::record::{Automation, RunStatus, Spec, StageKind, Trigger};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::permission_path::permission_pattern_matches;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::user::id::UserId;
use serde_json::Value;
use ulid::Ulid;

use super::run_stage::{parse_inputs_with, render_json_with, trigger_vars};
use super::{AutoAccessOperation, AutoError, Need};
use crate::applets::AppletError;
use crate::applets::backends::{SandboxOperation, Sandboxing, public_ready};
use crate::applets::reads::{GroupAppletsOperation, LoadAppletOperation};
use crate::workflows::CatalogError;
use crate::workflows::inspect::{ValidateValuesOperation, ValuesReport};
use crate::workflows::runs::run::merge_run_inputs;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyPreview {
    pub stage: String,
    pub dest_bucket: String,
    pub sample_key: String,
    pub within_scope: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoCheckReport {
    pub ok: bool,
    pub diagnostics: Vec<String>,
    pub outside_input: bool,
    pub unsandboxed: bool,
    pub copy_previews: Vec<CopyPreview>,
    pub workflows_checked: usize,
}

#[derive(Debug, PartialEq)]
enum CheckStep {
    Access,
    GroupApplets,
    Applets,
    Backends,
    Workflow(String, bool),
}

#[derive(Debug, PartialEq)]
struct RunCheck {
    stage: String,
    workflow_id: Ulid,
    version: u32,
    values: Value,
    no_files: bool,
}

fn sample_str(sample: Option<&Value>, key: &str) -> Option<String> {
    sample
        .and_then(|v| v.get(key))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn sample_ulid(sample: Option<&Value>, key: &str) -> Option<Ulid> {
    sample
        .and_then(|v| v.get(key))
        .and_then(Value::as_str)
        .and_then(|s| Ulid::from_string(s).ok())
}

fn sample_u64(sample: Option<&Value>, key: &str) -> Option<u64> {
    sample.and_then(|v| v.get(key)).and_then(Value::as_u64)
}

fn sample_files(sample: Option<&Value>) -> Result<Vec<SubmissionFile>, AutoError> {
    let Some(raw) = sample.and_then(|v| v.get("files")) else {
        return Ok(Vec::new());
    };
    if raw.is_null() {
        return Ok(Vec::new());
    }
    let Some(items) = raw.as_array() else {
        return Err(AutoError::Invalid(
            "sample files must be a JSON array".into(),
        ));
    };
    let mut files = Vec::with_capacity(items.len());
    for item in items {
        let Some(obj) = item.as_object() else {
            return Err(AutoError::Invalid(
                "sample file entry must be a JSON object".into(),
            ));
        };
        let name = obj
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AutoError::Invalid("sample file missing name".into()))?;
        let bucket = obj
            .get("bucket")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AutoError::Invalid(format!("sample file `{name}` missing bucket")))?;
        let key = obj
            .get("key")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AutoError::Invalid(format!("sample file `{name}` missing key")))?;
        let version_id = match obj.get("version_id") {
            None | Some(Value::Null) => Ulid::nil(),
            Some(Value::String(text)) => Ulid::from_string(text).map_err(|_| {
                AutoError::Invalid(format!("sample file `{name}` has invalid ULID version_id"))
            })?,
            Some(_) => {
                return Err(AutoError::Invalid(format!(
                    "sample file `{name}` version_id must be a string"
                )));
            }
        };
        files.push(SubmissionFile {
            name: name.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
        });
    }
    Ok(files)
}

fn sample_trigger(
    trigger: &Trigger,
    sample: Option<&Value>,
    actor: UserId,
) -> Result<TriggerPayload, AutoError> {
    Ok(match trigger {
        Trigger::Manual { .. } => TriggerPayload::Manual {
            values_json: sample.map(Value::to_string).unwrap_or_else(|| "{}".into()),
            actor,
        },
        Trigger::DataUploaded { bucket, prefix, .. } => {
            let bkt = sample_str(sample, "bucket").unwrap_or_else(|| bucket.clone());
            let key = sample_str(sample, "key").unwrap_or_else(|| {
                let file = sample_str(sample, "filename").unwrap_or_else(|| "sample.dat".into());
                format!("{prefix}{file}")
            });
            let version_id = sample_ulid(sample, "version_id").unwrap_or(Ulid::nil());
            let path = sample_str(sample, "path").unwrap_or_else(|| format!("/{bkt}/{key}"));
            let size_bytes = sample_u64(sample, "size_bytes");
            TriggerPayload::DataUploaded {
                bucket: bkt,
                key,
                version_id,
                path,
                size_bytes,
            }
        }
        Trigger::RunFinished {
            workflow_id,
            status,
            applet_id,
        } => {
            let run_id = sample_ulid(sample, "run_id").unwrap_or(Ulid::nil());
            let st = sample_str(sample, "status")
                .and_then(|s| match s.as_str() {
                    "succeeded" => Some(RunStatus::Succeeded),
                    "failed" => Some(RunStatus::Failed),
                    _ => None,
                })
                .or(*status)
                .unwrap_or(RunStatus::Succeeded);
            TriggerPayload::RunFinished {
                workflow_id: *workflow_id,
                run_id,
                status: st,
                outputs: Vec::new(),
                applet_id: *applet_id,
                outside_input: false,
                depth: 1,
            }
        }
        Trigger::AppletSubmission { applet_id } => TriggerPayload::AppletSubmission {
            applet_id: applet_id
                .or_else(|| sample_ulid(sample, "applet_id"))
                .unwrap_or(Ulid::nil()),
            version_id: sample_ulid(sample, "version_id").unwrap_or(Ulid::nil()),
            pseudonym: sample_str(sample, "pseudonym").unwrap_or_else(|| "sample".into()),
            guest: false,
            outside_input: false,
            bucket: sample_str(sample, "bucket").unwrap_or_else(|| "sample".into()),
            key: sample_str(sample, "key").unwrap_or_else(|| "sample".into()),
            files: sample_files(sample)?,
        },
        Trigger::Schedule(_) => TriggerPayload::Schedule {
            at_ms: sample_u64(sample, "at_ms").unwrap_or(0),
        },
    })
}

fn seed_stage_vars(spec: &Spec, vars: &mut BTreeMap<String, Value>) {
    let nil = Ulid::nil().to_string();
    for stage in &spec.stages {
        vars.insert(
            format!("stages.{}.run_id", stage.name),
            Value::String(nil.clone()),
        );
        vars.insert(
            format!("stages.{}.dataset_id", stage.name),
            Value::String(nil.clone()),
        );
        vars.insert(
            format!("stages.{}.first_output", stage.name),
            Value::String("aruna://sample/sample.dat".into()),
        );
    }
}

fn seed_stage_outputs(raw: &str, stages: &BTreeSet<&str>, vars: &mut BTreeMap<String, Value>) {
    let mut rest = raw;
    while let Some(open) = rest.find("{{") {
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            break;
        };
        let name = &after[..close];
        if let Some(tail) = name.strip_prefix("stages.")
            && let Some((stage, output)) = tail.split_once(".output.")
            && stages.contains(stage)
            && !output.is_empty()
            && !output.contains(['{', '}', '"'])
        {
            vars.entry(name.to_string())
                .or_insert_with(|| Value::String("aruna://sample/sample.dat".into()));
        }
        rest = &after[close + 2..];
    }
}

pub(crate) fn render_trigger_stage(
    raw_values: &str,
    raw_inputs: Option<&str>,
    trigger: &Trigger,
    spec: &Spec,
    sample: Option<&Value>,
    actor: UserId,
    local: NodeId,
) -> Result<(Value, bool), AutoError> {
    let payload = sample_trigger(trigger, sample, actor)?;
    let mut vars = trigger_vars(&payload);
    seed_stage_vars(spec, &mut vars);
    let stages: BTreeSet<&str> = spec.stages.iter().map(|s| s.name.as_str()).collect();
    seed_stage_outputs(raw_values, &stages, &mut vars);
    if let Some(inputs) = raw_inputs {
        seed_stage_outputs(inputs, &stages, &mut vars);
    }
    let values = render_json_with(raw_values, &vars)?;
    if !values.is_object() {
        return Err(AutoError::Invalid(
            "run values_json must render to a JSON object".into(),
        ));
    }
    let (files, no_files) = match &payload {
        TriggerPayload::AppletSubmission { files, .. } => (files.as_slice(), files.is_empty()),
        _ => (&[][..], false),
    };
    let (_, rendered_inputs) = parse_inputs_with(raw_inputs, &vars, files, local)?;
    let merged = merge_run_inputs(local, &values.to_string(), rendered_inputs.as_deref())
        .map_err(|err| AutoError::Invalid(err.to_string()))?;
    Ok((merged, no_files))
}

/// Validates an automation's DAG, scope, workflow forms, copy templates and sandbox posture.
#[derive(Debug, PartialEq)]
pub struct CheckAutoOperation {
    auth: AuthContext,
    id: Ulid,
    local: NodeId,
    sample: Option<Value>,
    now_ms: u64,
    auto: Option<Automation>,
    has_wildcard: bool,
    applet_ids: Vec<Ulid>,
    run_checks: Vec<RunCheck>,
    outside_input: bool,
    unsandboxed: bool,
    diagnostics: Vec<String>,
    copy_previews: Vec<CopyPreview>,
    workflows_checked: usize,
    step: CheckStep,
}

impl CheckAutoOperation {
    pub fn new(auth: &AuthContext, id: Ulid, local: NodeId, sample: Option<Value>) -> Self {
        Self {
            auth: auth.clone(),
            id,
            local,
            sample,
            now_ms: 0,
            auto: None,
            has_wildcard: false,
            applet_ids: Vec::new(),
            run_checks: Vec::new(),
            outside_input: false,
            unsandboxed: false,
            diagnostics: Vec::new(),
            copy_previews: Vec::new(),
            workflows_checked: 0,
            step: CheckStep::Access,
        }
    }

    fn prepare(&mut self, auto: &Automation) {
        let spec: &Spec = auto.pending.as_ref().map(|p| &p.spec).unwrap_or(&auto.spec);
        if let Err(err) = validate_spec(spec) {
            self.diagnostics.push(err);
        }
        if let Err(err) = validate_scope(spec, self.auth.realm_id, auto.group_id, self.local) {
            self.diagnostics.push(err);
        }
        let date = format_date(self.now_ms);
        let run_sample = auto.automation_id.to_string();
        let mut seen_applets = BTreeSet::new();
        for trigger in &spec.triggers {
            let applet = match trigger {
                Trigger::AppletSubmission {
                    applet_id: Some(id),
                } => Some(*id),
                Trigger::AppletSubmission { applet_id: None } => {
                    self.has_wildcard = true;
                    None
                }
                Trigger::RunFinished { applet_id, .. } => *applet_id,
                _ => None,
            };
            if let Some(id) = applet.filter(|id| seen_applets.insert(*id)) {
                self.applet_ids.push(id);
            }
        }
        for stage in &spec.stages {
            match &stage.kind {
                StageKind::Run {
                    workflow_id,
                    version,
                    values_json,
                    inputs_json,
                    ..
                } => {
                    for trigger in &spec.triggers {
                        match render_trigger_stage(
                            values_json,
                            inputs_json.as_deref(),
                            trigger,
                            spec,
                            self.sample.as_ref(),
                            self.auth.user_id,
                            self.local,
                        ) {
                            Ok((values, no_files)) => {
                                if let Some(prev) = self.run_checks.iter_mut().find(|c| {
                                    c.stage == stage.name
                                        && c.workflow_id == *workflow_id
                                        && c.version == *version
                                        && c.values == values
                                }) {
                                    prev.no_files |= no_files;
                                } else {
                                    self.run_checks.push(RunCheck {
                                        stage: stage.name.clone(),
                                        workflow_id: *workflow_id,
                                        version: *version,
                                        values,
                                        no_files,
                                    });
                                }
                            }
                            Err(err) => {
                                self.diagnostics
                                    .push(format!("stage `{}`: {err}", stage.name));
                            }
                        }
                    }
                }
                StageKind::Copy {
                    dest_bucket,
                    dest_template,
                    ..
                } => match expand_copy(dest_template, &date, &run_sample, "sample.dat") {
                    Ok(sample_key) => {
                        let path = object_permission_path(
                            self.auth.realm_id,
                            auto.group_id,
                            self.local,
                            dest_bucket,
                            &sample_key,
                        );
                        let within = spec
                            .scope
                            .write
                            .iter()
                            .any(|pat| permission_pattern_matches(pat, &path));
                        if !within {
                            self.diagnostics.push(format!(
                                "copy stage `{}` preview `{path}` is outside scope.write",
                                stage.name
                            ));
                        }
                        self.copy_previews.push(CopyPreview {
                            stage: stage.name.clone(),
                            dest_bucket: dest_bucket.clone(),
                            sample_key,
                            within_scope: within,
                        });
                    }
                    Err(err) => self
                        .diagnostics
                        .push(format!("copy stage `{}`: {err}", stage.name)),
                },
                _ => {}
            }
        }
    }

    fn next_step(&mut self) -> Next<AutoCheckReport> {
        if let Some(applet_id) = self.applet_ids.last().copied() {
            self.step = CheckStep::Applets;
            return Next::Call(IoCall::child(LoadAppletOperation::new(applet_id)));
        }
        if let Some(check) = self.run_checks.pop() {
            self.step = CheckStep::Workflow(check.stage, check.no_files);
            let op = ValidateValuesOperation::new(
                &self.auth,
                check.workflow_id,
                check.version,
                check.values,
            );
            return Next::Call(IoCall::child(op));
        }
        let ok = self.diagnostics.is_empty();
        Next::Done(AutoCheckReport {
            ok,
            diagnostics: std::mem::take(&mut self.diagnostics),
            outside_input: self.outside_input,
            unsandboxed: self.unsandboxed,
            copy_previews: std::mem::take(&mut self.copy_previews),
            workflows_checked: self.workflows_checked,
        })
    }
}

impl Operation for CheckAutoOperation {
    type Output = AutoCheckReport;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<AutoCheckReport>, AutoError> {
        let access = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AutoCheckReport>, AutoError> {
        match std::mem::replace(&mut self.step, CheckStep::Backends) {
            CheckStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                self.prepare(&auto);
                let group_id = auto.group_id;
                self.auto = Some(auto);
                if self.has_wildcard {
                    self.step = CheckStep::GroupApplets;
                    return Ok(Next::Call(IoCall::child(GroupAppletsOperation::new(
                        group_id,
                    ))));
                }
                if self.applet_ids.is_empty() {
                    self.step = CheckStep::Backends;
                    return Ok(Next::Call(IoCall::child(SandboxOperation)));
                }
                Ok(self.next_step())
            }
            CheckStep::GroupApplets => {
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let (auto_id, group_id) = (auto.automation_id, auto.group_id);
                let applets = result
                    .child::<Vec<Applet>, AppletError>()?
                    .map_err(|error| AutoError::Storage(error.to_string()))?;
                for applet in applets {
                    if applet.group_id == group_id
                        && applet.lifecycle != AppletLifecycle::TakenDown
                        && applet.automations.contains(&auto_id)
                    {
                        self.applet_ids.retain(|id| *id != applet.applet_id);
                        if applet.access == Access::Public || applet.guests.is_some() {
                            self.outside_input = true;
                        }
                    }
                }
                if self.applet_ids.is_empty() {
                    self.step = CheckStep::Backends;
                    return Ok(Next::Call(IoCall::child(SandboxOperation)));
                }
                Ok(self.next_step())
            }
            CheckStep::Applets => {
                let Some(applet_id) = self.applet_ids.pop() else {
                    return Err(IoError::Unfinished.into());
                };
                let group_id = self.auto.as_ref().ok_or(IoError::Unfinished)?.group_id;
                match result.child::<Option<Applet>, AppletError>()? {
                    Ok(Some(applet)) if applet.group_id == group_id => {
                        if applet.access == Access::Public || applet.guests.is_some() {
                            self.outside_input = true;
                        }
                    }
                    Ok(_) => self
                        .diagnostics
                        .push(format!("referenced applet `{applet_id}` not found")),
                    Err(error) => return Err(AutoError::Storage(error.to_string())),
                }
                if self.applet_ids.is_empty() {
                    self.step = CheckStep::Backends;
                    return Ok(Next::Call(IoCall::child(SandboxOperation)));
                }
                Ok(self.next_step())
            }
            CheckStep::Backends => {
                let backends = result
                    .child::<Vec<Sandboxing>, AppletError>()?
                    .unwrap_or_default();
                let auto = self.auto.as_ref().ok_or(IoError::Unfinished)?;
                let spec = auto.pending.as_ref().map(|p| &p.spec).unwrap_or(&auto.spec);
                let detected_outside = self.outside_input;
                self.outside_input |= spec.scope.allow_outside_input;
                self.unsandboxed = spec.has_runs() && !public_ready(&backends);
                if detected_outside && !spec.scope.allow_outside_input {
                    self.diagnostics.push(
                        "linked or referenced applet accepts outside input; enable scope.allow_outside_input and approve it".into(),
                    );
                }
                if self.outside_input && self.unsandboxed {
                    self.diagnostics.push(
                        "outside-input workflow runs require a sandboxed compute backend".into(),
                    );
                }
                Ok(self.next_step())
            }
            CheckStep::Workflow(stage, no_files) => {
                self.workflows_checked += 1;
                match result.child::<ValuesReport, CatalogError>()? {
                    Ok(report) if report.ok => {}
                    Ok(report) => {
                        for item in report.diagnostics {
                            let msg = item["message"]
                                .as_str()
                                .unwrap_or("invalid workflow values");
                            let line = match no_files {
                                true => format!(
                                    "stage `{stage}`: {msg} (no sample applet submission files provided in sample_values.files; file inputs could not be verified)"
                                ),
                                false => format!("stage `{stage}`: {msg}"),
                            };
                            self.diagnostics.push(line);
                        }
                    }
                    Err(err) => self.diagnostics.push(format!("stage `{stage}`: {err}")),
                }
                Ok(self.next_step())
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
