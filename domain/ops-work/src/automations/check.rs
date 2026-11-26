//! Side-effect-free dry-run check of an automation DAG, workflow forms and copy destinations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::{Access, Applet};
use aruna_core::automation::dag::{validate_scope, validate_spec};
use aruna_core::automation::glob::{expand_copy, format_date};
use aruna_core::automation::record::{Automation, Spec, StageKind, Trigger};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::permission_path::permission_pattern_matches;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::object_permission_path;
use serde_json::{Value, json};
use ulid::Ulid;

use super::{AutoAccessOperation, AutoError, Need};
use crate::applets::AppletError;
use crate::applets::backends::{SandboxOperation, Sandboxing, public_ready};
use crate::applets::reads::LoadAppletOperation;
use crate::workflows::CatalogError;
use crate::workflows::inspect::{ValidateValuesOperation, ValuesReport};

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
    Applets,
    Backends,
    Workflow(String),
}

fn render_sample(raw: &str, sample: Option<&Value>) -> Value {
    let mut text = raw.to_string();
    if let Some(obj) = sample.and_then(Value::as_object) {
        for (key, value) in obj {
            let needle = format!("{{{{trigger.{key}}}}}");
            let repl = value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string());
            text = text.replace(&needle, &repl);
        }
    }
    let mut cleaned = String::with_capacity(text.len());
    let mut rest = text.as_str();
    while let Some(open) = rest.find("{{") {
        cleaned.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            cleaned.push_str("sample");
            rest = "";
            break;
        };
        cleaned.push_str("sample");
        rest = &after[close + 2..];
    }
    cleaned.push_str(rest);
    serde_json::from_str(&cleaned).unwrap_or_else(|_| json!({}))
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
    applet_ids: Vec<Ulid>,
    run_checks: Vec<(String, Ulid, u32, Value)>,
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
                Trigger::AppletSubmission { applet_id } => Some(*applet_id),
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
                    ..
                } => {
                    let values = render_sample(values_json, self.sample.as_ref());
                    self.run_checks
                        .push((stage.name.clone(), *workflow_id, *version, values));
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
        if let Some((stage, workflow_id, version, values)) = self.run_checks.pop() {
            self.step = CheckStep::Workflow(stage);
            let check = ValidateValuesOperation::new(&self.auth, workflow_id, version, values);
            return Next::Call(IoCall::child(check));
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
                self.auto = Some(auto);
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
                match result.child::<Option<Applet>, AppletError>()? {
                    Ok(Some(applet)) => {
                        if applet.access == Access::Public || applet.guests.is_some() {
                            self.outside_input = true;
                        }
                    }
                    Ok(None) => self
                        .diagnostics
                        .push(format!("referenced applet `{applet_id}` not found")),
                    Err(err) => self.diagnostics.push(err.to_string()),
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
                self.unsandboxed = spec.has_runs() && !public_ready(&backends);
                if self.outside_input && self.unsandboxed {
                    self.diagnostics.push(
                        "outside-input workflow runs require a sandboxed compute backend".into(),
                    );
                }
                Ok(self.next_step())
            }
            CheckStep::Workflow(stage) => {
                self.workflows_checked += 1;
                match result.child::<ValuesReport, CatalogError>()? {
                    Ok(report) if report.ok => {}
                    Ok(report) => {
                        for item in report.diagnostics {
                            let msg = item["message"]
                                .as_str()
                                .unwrap_or("invalid workflow values");
                            self.diagnostics.push(format!("stage `{stage}`: {msg}"));
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
