//! Atomic write transaction committing a workflow version and its paired automation together.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::dag::{validate_scope, validate_spec};
use aruna_core::automation::keys::{record_key, schedule_cursor_key};
use aruna_core::automation::record::{Automation, Lifecycle, Spec, StageKind};
use aruna_core::automation::schedule::ScheduleCursor;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTOMATIONS_KEYSPACE, WORKFLOW_KEYSPACE, WORKFLOW_VERSION_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, WorkflowRecord, WorkflowVisibility, version_key, workflow_key,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{GroupId, Key, KeySpace};
use aruna_core::user::id::UserId;
use aruna_core::workflow::WorkflowError;
use ulid::Ulid;

use super::change::{AutoChange, DraftAutoInput, apply_change, draft_automation, validate_name};
use super::{AutoError, auto_row, plan_schedule_sync};
use crate::workflows::CatalogError;
use crate::workflows::import::{Created, Prepared, SourceInput, version_write_rows};

/// Workflow part of a unified Studio save.
#[derive(Clone, Debug, PartialEq)]
pub struct SaveWorkflow {
    pub workflow_id: Option<Ulid>,
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub language: Option<StoredWorkflowLanguage>,
    pub visibility: Option<WorkflowVisibility>,
    pub message: Option<String>,
    pub source: Option<SourceInput>,
}

/// Automation part of a unified Studio save.
#[derive(Clone, Debug, PartialEq)]
pub struct SaveAutomation {
    pub automation_id: Option<Ulid>,
    pub expected_revision: Option<u64>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub spec: Spec,
    /// When `true` and a new workflow version is stored, pins matching `run` stages to it.
    pub pin_version: bool,
}

/// Complete input for `POST /compute/automations/save`.
#[derive(Clone, Debug, PartialEq)]
pub struct SaveBundle {
    pub group_id: GroupId,
    pub workflow: SaveWorkflow,
    pub automation: SaveAutomation,
}

/// Both records committed by one unified save transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct SavedBundle {
    pub workflow: WorkflowRecord,
    pub created: Option<Created>,
    pub automation: Automation,
    /// Whether the automation's service account role is synchronized with the saved revision.
    pub identity_synced: bool,
}

/// Named parameters for [`CommitBundleOperation`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CommitBundleInput {
    pub group_id: GroupId,
    pub local: NodeId,
    pub realm_id: RealmId,
    pub actor: UserId,
    pub workflow_id: Ulid,
    pub workflow_new: bool,
    pub automation_id: Ulid,
    pub automation_new: bool,
    pub workflow: SaveWorkflow,
    pub automation: SaveAutomation,
    pub prepared: Option<Prepared>,
    pub now_ms: u64,
}

pub(crate) fn map_catalog(error: CatalogError) -> AutoError {
    match error {
        CatalogError::NotFound => AutoError::NotFound,
        CatalogError::Forbidden => AutoError::Forbidden,
        CatalogError::Invalid(message) | CatalogError::Unsupported(message) => {
            AutoError::Invalid(message)
        }
        CatalogError::Conflict(message) => AutoError::Conflict(message),
        CatalogError::Capacity(message) => AutoError::Capacity(message),
        CatalogError::Coordinator(WorkflowError::Refused(reason)) => {
            AutoError::Invalid(reason.to_string())
        }
        CatalogError::Coordinator(error) => AutoError::Unavailable(error.to_string()),
        CatalogError::Storage(message) => AutoError::Storage(message),
    }
}

fn pin_spec(spec: &mut Spec, target_workflow: Ulid, version: u32, pin_existing: bool) {
    let mut pinned_any = false;
    for stage in &mut spec.stages {
        if let StageKind::Run {
            workflow_id,
            version: stage_version,
            ..
        } = &mut stage.kind
            && (*workflow_id == Ulid::nil()
                || (pin_existing && *workflow_id == target_workflow)
                || (*workflow_id == target_workflow && *stage_version == 0))
        {
            *workflow_id = target_workflow;
            *stage_version = version;
            pinned_any = true;
        }
    }
    spec.scope.workflows.retain(|(id, _)| *id != Ulid::nil());
    if pin_existing {
        let kept: Vec<(Ulid, u32)> = spec
            .stages
            .iter()
            .filter_map(|stage| match stage.kind {
                StageKind::Run {
                    workflow_id,
                    version,
                    ..
                } => Some((workflow_id, version)),
                _ => None,
            })
            .collect();
        spec.scope
            .workflows
            .retain(|pair| pair.0 != target_workflow || kept.contains(pair));
    }
    if pinned_any && !spec.scope.workflows.contains(&(target_workflow, version)) {
        spec.scope.workflows.push((target_workflow, version));
    }
}

#[derive(Clone, Debug, PartialEq)]
enum CommitStep {
    Read,
    CheckVersion {
        workflow: WorkflowRecord,
        automation: Option<Automation>,
        cursor: Option<ScheduleCursor>,
    },
    Write {
        saved: Box<SavedBundle>,
        deletes: Vec<(KeySpace, Key)>,
    },
    DropRows(Box<SavedBundle>),
}

/// Atomically writes workflow, version, source, schedule cursor and automation rows in one tx.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CommitBundleOperation {
    input: CommitBundleInput,
    step: CommitStep,
}

impl CommitBundleOperation {
    pub(crate) fn new(input: CommitBundleInput) -> Self {
        Self {
            input,
            step: CommitStep::Read,
        }
    }

    fn write_all(
        &mut self,
        mut workflow: WorkflowRecord,
        existing_automation: Option<Automation>,
        cursor: Option<ScheduleCursor>,
    ) -> Result<Next<SavedBundle>, AutoError> {
        let has_prepared = self.input.prepared.is_some();
        let target_version = if has_prepared {
            workflow.version_count + 1
        } else {
            workflow.version_count
        };
        if let Some(name) = &self.input.workflow.name {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                return Err(AutoError::Invalid("a workflow needs a name".into()));
            }
            workflow.name = trimmed.to_string();
        }
        if let Some(description) = &self.input.workflow.description {
            workflow.description = description.clone();
        }
        if let Some(visibility) = self.input.workflow.visibility {
            workflow.visibility = visibility;
        }
        workflow.automation_id = Some(self.input.automation_id);

        let mut spec = self.input.automation.spec.clone();
        if target_version > 0 {
            pin_spec(
                &mut spec,
                self.input.workflow_id,
                target_version,
                self.input.automation.pin_version && has_prepared,
            );
        }
        validate_spec(&spec).map_err(AutoError::Invalid)?;
        validate_scope(
            &spec,
            self.input.realm_id,
            self.input.group_id,
            self.input.local,
        )
        .map_err(AutoError::Invalid)?;

        let was_active = existing_automation
            .as_ref()
            .is_some_and(|item| item.lifecycle == Lifecycle::Active);
        let automation = match existing_automation {
            Some(mut automation) => {
                if self
                    .input
                    .automation
                    .expected_revision
                    .is_some_and(|expected| expected != automation.revision)
                {
                    return Err(AutoError::Conflict(
                        "automation changed meanwhile; read again".into(),
                    ));
                }
                apply_change(
                    &mut automation,
                    AutoChange::Update {
                        name: self.input.automation.name.clone(),
                        description: self.input.automation.description.clone(),
                        spec: Some(spec),
                        workflow_id: Some(Some(self.input.workflow_id)),
                    },
                    self.input.actor,
                    self.input.realm_id,
                    self.input.now_ms,
                )?;
                automation
            }
            None => {
                let raw_name = self
                    .input
                    .automation
                    .name
                    .as_deref()
                    .unwrap_or(&workflow.name);
                let name = validate_name(raw_name)?;
                draft_automation(DraftAutoInput {
                    automation_id: self.input.automation_id,
                    group_id: self.input.group_id,
                    name,
                    description: self
                        .input
                        .automation
                        .description
                        .as_deref()
                        .unwrap_or_default()
                        .trim()
                        .to_string(),
                    host_node: self.input.local,
                    spec,
                    created_by: self.input.actor,
                    now_ms: self.input.now_ms,
                    workflow_id: Some(self.input.workflow_id),
                })
            }
        };

        let (created, mut writes) = if let Some(prepared) = self.input.prepared.clone() {
            let (created, rows) = version_write_rows(
                self.input.workflow_id,
                &mut workflow,
                prepared,
                self.input.actor,
                self.input.workflow.message.clone(),
                self.input.now_ms,
            )
            .map_err(map_catalog)?;
            (Some(created), rows)
        } else {
            workflow.updated_at_ms = self.input.now_ms;
            let row = (
                WORKFLOW_KEYSPACE.into(),
                workflow_key(self.input.workflow_id),
                workflow.to_bytes()?.into(),
            );
            (None, vec![row])
        };
        writes.push(auto_row(&automation)?);
        let schedule_sync =
            plan_schedule_sync(&automation, cursor.as_ref(), was_active, self.input.now_ms)?;
        writes.extend(schedule_sync.writes);
        let identity_synced = !automation.needs_identity_sync();
        let saved = SavedBundle {
            workflow,
            created,
            automation,
            identity_synced,
        };
        self.step = CommitStep::Write {
            saved: Box::new(saved),
            deletes: schedule_sync.deletes,
        };
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for CommitBundleOperation {
    type Output = SavedBundle;
    type Error = AutoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.input.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<SavedBundle>, AutoError> {
        self.step = CommitStep::Read;
        let mut reads = vec![(
            WORKFLOW_KEYSPACE.to_string(),
            workflow_key(self.input.workflow_id),
        )];
        if !self.input.automation_new {
            reads.push((
                AUTOMATIONS_KEYSPACE.to_string(),
                record_key(self.input.automation_id),
            ));
            reads.push((
                AUTOMATIONS_KEYSPACE.to_string(),
                schedule_cursor_key(self.input.automation_id),
            ));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SavedBundle>, AutoError> {
        match std::mem::replace(&mut self.step, CommitStep::Read) {
            CommitStep::Read => {
                let values = result.values()?;
                let stored_workflow = values.first().and_then(Option::as_deref);
                let workflow = match (stored_workflow, self.input.workflow_new) {
                    (None, true) => {
                        let prepared_language =
                            self.input.prepared.as_ref().and_then(|item| item.language);
                        let language = prepared_language
                            .or(self.input.workflow.language)
                            .ok_or_else(|| {
                                AutoError::Invalid(
                                    "the language of the workflow is not known".into(),
                                )
                            })?;
                        let name = self
                            .input
                            .workflow
                            .name
                            .as_deref()
                            .map(str::trim)
                            .filter(|text| !text.is_empty())
                            .ok_or_else(|| AutoError::Invalid("a workflow needs a name".into()))?;
                        WorkflowRecord {
                            workflow_id: self.input.workflow_id,
                            group_id: self.input.group_id,
                            name: name.to_string(),
                            description: self.input.workflow.description.clone().flatten(),
                            language,
                            visibility: self.input.workflow.visibility.unwrap_or_default(),
                            version_count: 0,
                            edam_formats: Vec::new(),
                            created_by: self.input.actor,
                            created_at_ms: self.input.now_ms,
                            updated_at_ms: self.input.now_ms,
                            automation_id: Some(self.input.automation_id),
                        }
                    }
                    (Some(bytes), false) => {
                        let record = WorkflowRecord::from_bytes(bytes)?;
                        if record.group_id != self.input.group_id {
                            return Err(AutoError::Invalid(
                                "workflow belongs to a different group".into(),
                            ));
                        }
                        if record
                            .automation_id
                            .is_some_and(|existing| existing != self.input.automation_id)
                        {
                            return Err(AutoError::Conflict(
                                "workflow is already linked to another automation".into(),
                            ));
                        }
                        if let Some(prepared_language) =
                            self.input.prepared.as_ref().and_then(|item| item.language)
                            && prepared_language != record.language
                        {
                            return Err(AutoError::Invalid(format!(
                                "the files are {}, the workflow is {}",
                                prepared_language.as_str(),
                                record.language.as_str()
                            )));
                        }
                        record
                    }
                    (None, false) => return Err(AutoError::NotFound),
                    (Some(_), true) => {
                        return Err(AutoError::Conflict("the workflow exists".into()));
                    }
                };
                let (existing_automation, cursor) = if self.input.automation_new {
                    (None, None)
                } else {
                    let bytes = values
                        .get(1)
                        .and_then(Option::as_deref)
                        .ok_or(AutoError::NotFound)?;
                    let automation = Automation::from_bytes(bytes)?;
                    if automation.lifecycle == Lifecycle::Removed {
                        return Err(AutoError::NotFound);
                    }
                    if automation.group_id != self.input.group_id {
                        return Err(AutoError::Invalid(
                            "automation belongs to a different group".into(),
                        ));
                    }
                    if automation
                        .workflow_id
                        .is_some_and(|existing| existing != self.input.workflow_id)
                    {
                        return Err(AutoError::Conflict(
                            "automation is already linked to another workflow".into(),
                        ));
                    }
                    let cursor = values
                        .get(2)
                        .and_then(Option::as_deref)
                        .map(ScheduleCursor::from_bytes)
                        .transpose()?;
                    (Some(automation), cursor)
                };
                if self.input.prepared.is_some() {
                    let next_version = workflow.version_count + 1;
                    self.step = CommitStep::CheckVersion {
                        workflow,
                        automation: existing_automation,
                        cursor,
                    };
                    return Ok(Next::Call(read_row(
                        WORKFLOW_VERSION_KEYSPACE,
                        version_key(self.input.workflow_id, next_version),
                    )));
                }
                self.write_all(workflow, existing_automation, cursor)
            }
            CommitStep::CheckVersion {
                workflow,
                automation,
                cursor,
            } => {
                if result.value()?.is_some() {
                    return Err(AutoError::Conflict(
                        "the workflow version already exists".into(),
                    ));
                }
                self.write_all(workflow, automation, cursor)
            }
            CommitStep::Write { saved, deletes } => {
                result.done()?;
                if !deletes.is_empty() {
                    self.step = CommitStep::DropRows(saved);
                    return Ok(Next::Call(IoCall::Delete(deletes)));
                }
                Ok(Next::Done(*saved))
            }
            CommitStep::DropRows(saved) => {
                result.done()?;
                Ok(Next::Done(*saved))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

#[cfg(test)]
#[path = "commit_bundle_tests.rs"]
mod tests;
