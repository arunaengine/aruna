//! Creates and atomically mutates automation records with revision checks, link consistency
//! and widening rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::dag::{validate_scope, validate_spec};
use aruna_core::automation::keys::{record_key, schedule_cursor_key};
use aruna_core::automation::record::{Automation, Lifecycle, PauseReason, PendingChange, Spec};
use aruna_core::automation::schedule::ScheduleCursor;
use aruna_core::automation::scope::is_widening;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{AUTOMATIONS_KEYSPACE, WORKFLOW_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::workflow::{
    WorkflowRecord, workflow_key, workflow_permission_path,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::{GroupId, Key, KeySpace, RoleId, Value};
use aruna_core::user::id::UserId;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use ulid::Ulid;

use super::firing_reads::decode_ulid;
use super::{
    AutoError, active_page, allowed, authorize_path, auto_row, check_feature, plan_schedule_sync,
};

pub(crate) const MAX_NAME: usize = 120;

/// Trims and validates an automation name length (`1..=120` Unicode scalar values).
pub(crate) fn validate_name(raw: &str) -> Result<String, AutoError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.chars().count() > MAX_NAME {
        return Err(AutoError::Invalid(format!(
            "name needs 1..={MAX_NAME} chars"
        )));
    }
    Ok(trimmed.to_string())
}

/// Inputs required to construct a fresh draft `Automation` record.
pub(crate) struct DraftAutoInput {
    pub automation_id: Ulid,
    pub group_id: GroupId,
    pub name: String,
    pub description: String,
    pub host_node: NodeId,
    pub spec: Spec,
    pub created_by: UserId,
    pub now_ms: u64,
    pub workflow_id: Option<Ulid>,
}

/// Constructs a fresh draft `Automation` record with initial revision and zeroed sync state.
pub(crate) fn draft_automation(input: DraftAutoInput) -> Automation {
    Automation {
        automation_id: input.automation_id,
        group_id: input.group_id,
        name: input.name,
        description: input.description,
        host_node: input.host_node,
        lifecycle: Lifecycle::Draft,
        pause_reason: None,
        spec: input.spec,
        pending: None,
        account: None,
        role_id: None,
        approved_by: None,
        approved_at_ms: None,
        created_by: input.created_by,
        created_at_ms: input.now_ms,
        updated_at_ms: input.now_ms,
        revision: 1,
        identity_synced_revision: 0,
        workflow_id: input.workflow_id,
    }
}

fn workflow_row(record: &WorkflowRecord) -> Result<(KeySpace, Key, Value), AutoError> {
    Ok((
        WORKFLOW_KEYSPACE.to_string(),
        workflow_key(record.workflow_id),
        record.to_bytes()?.into(),
    ))
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewAuto {
    pub group_id: GroupId,
    pub name: String,
    pub description: String,
    pub spec: Spec,
    pub workflow_id: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
enum CreateStep {
    Config,
    AuthGroup,
    AuthWorkflow,
    Commit,
}

/// Creates a new draft automation on `local` after checking feature switches, group WRITE,
/// optional workflow pairing and DAG validation.
#[derive(Debug, PartialEq)]
pub struct CreateAutoOperation {
    auth: AuthContext,
    local: NodeId,
    input: NewAuto,
    disabled_features: BTreeSet<Feature>,
    ids: IdMint,
    now_ms: u64,
    step: CreateStep,
}

impl CreateAutoOperation {
    pub fn new(auth: &AuthContext, local: NodeId, input: NewAuto) -> Self {
        Self {
            auth: auth.clone(),
            local,
            input,
            disabled_features: BTreeSet::new(),
            ids: IdMint::default(),
            now_ms: 0,
            step: CreateStep::Config,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }

    fn start_commit(&mut self) -> Result<Next<Automation>, AutoError> {
        let name = validate_name(&self.input.name)?;
        let record = draft_automation(DraftAutoInput {
            automation_id: self.ids.mint(self.now_ms),
            group_id: self.input.group_id,
            name,
            description: self.input.description.trim().to_string(),
            host_node: self.local,
            spec: self.input.spec.clone(),
            created_by: self.auth.user_id,
            now_ms: self.now_ms,
            workflow_id: self.input.workflow_id,
        });
        let automation_id = record.automation_id;
        let commit = WriteDraftOperation {
            record,
            now_ms: self.now_ms,
            writing: false,
        };
        self.step = CreateStep::Commit;
        Ok(Next::Call(IoCall::retry(commit, &automation_id.to_bytes())))
    }
}

impl Operation for CreateAutoOperation {
    type Output = Automation;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<Automation>, AutoError> {
        validate_name(&self.input.name)?;
        validate_spec(&self.input.spec).map_err(AutoError::Invalid)?;
        validate_scope(
            &self.input.spec,
            self.auth.realm_id,
            self.input.group_id,
            self.local,
        )
        .map_err(AutoError::Invalid)?;
        self.step = CreateStep::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.auth.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Automation>, AutoError> {
        match std::mem::replace(&mut self.step, CreateStep::Commit) {
            CreateStep::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| {
                        AutoError::Unavailable("the realm feature policy cannot be read".into())
                    })?;
                check_feature(
                    &config,
                    &self.disabled_features,
                    Feature::Automations,
                    self.input.group_id,
                )?;
                if self.input.spec.uses_applets() {
                    check_feature(
                        &config,
                        &self.disabled_features,
                        Feature::Applets,
                        self.input.group_id,
                    )?;
                }
                let path =
                    group_permission_path(self.auth.realm_id, self.input.group_id, self.local);
                self.step = CreateStep::AuthGroup;
                Ok(Next::Call(authorize_path(
                    &self.auth,
                    &path,
                    Permission::WRITE,
                )))
            }
            CreateStep::AuthGroup => {
                if !allowed(result)? {
                    return Err(AutoError::Forbidden);
                }
                if self.input.workflow_id.is_some() {
                    let workflow_path =
                        workflow_permission_path(self.auth.realm_id, self.input.group_id);
                    self.step = CreateStep::AuthWorkflow;
                    return Ok(Next::Call(authorize_path(
                        &self.auth,
                        &workflow_path,
                        Permission::WRITE,
                    )));
                }
                self.start_commit()
            }
            CreateStep::AuthWorkflow => {
                if !allowed(result)? {
                    return Err(AutoError::Forbidden);
                }
                self.start_commit()
            }
            CreateStep::Commit => Ok(Next::Done(result.child::<Automation, AutoError>()??)),
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

/// Atomically writes a new draft automation and, when linked, binds the paired workflow row.
#[derive(Clone, Debug, PartialEq)]
struct WriteDraftOperation {
    record: Automation,
    now_ms: u64,
    writing: bool,
}

impl Operation for WriteDraftOperation {
    type Output = Automation;
    type Error = AutoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        self.record.created_at_ms = now_ms;
        self.record.updated_at_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Automation>, AutoError> {
        if let Some(workflow_id) = self.record.workflow_id {
            self.writing = false;
            return Ok(Next::Call(read_row(
                WORKFLOW_KEYSPACE,
                workflow_key(workflow_id),
            )));
        }
        self.writing = true;
        Ok(Next::Call(IoCall::Write(vec![auto_row(&self.record)?])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Automation>, AutoError> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(self.record.clone()));
        }
        let bytes = result.value()?.ok_or(AutoError::NotFound)?;
        let mut workflow = WorkflowRecord::from_bytes(&bytes)?;
        if workflow.group_id != self.record.group_id {
            return Err(AutoError::Invalid(
                "workflow belongs to a different group".into(),
            ));
        }
        if workflow.automation_id.is_some() {
            return Err(AutoError::Conflict(
                "workflow is already linked to another automation".into(),
            ));
        }
        workflow.automation_id = Some(self.record.automation_id);
        workflow.updated_at_ms = self.now_ms;
        self.writing = true;
        Ok(Next::Call(IoCall::Write(vec![
            auto_row(&self.record)?,
            workflow_row(&workflow)?,
        ])))
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

/// One atomic change applied to an automation record.
#[derive(Clone, Debug, PartialEq)]
pub enum AutoChange {
    Update {
        name: Option<String>,
        description: Option<String>,
        spec: Option<Spec>,
        workflow_id: Option<Option<Ulid>>,
    },
    RequestApproval,
    Approve,
    Reject,
    Pause(PauseReason),
    Resume,
    Delete,
    Identity {
        account: Option<UserId>,
        role_id: Option<RoleId>,
    },
}

impl AutoChange {
    pub fn moves_identity(&self) -> bool {
        matches!(
            self,
            Self::Update { .. } | Self::Approve | Self::Pause(_) | Self::Resume | Self::Delete
        )
    }

    pub fn needs_admin(&self) -> bool {
        matches!(self, Self::Approve | Self::Reject)
    }
}

pub(crate) fn apply_change(
    auto: &mut Automation,
    change: AutoChange,
    actor: UserId,
    realm_id: RealmId,
    now_ms: u64,
) -> Result<(), AutoError> {
    if auto.lifecycle == Lifecycle::Removed && !matches!(change, AutoChange::Identity { .. }) {
        return Err(AutoError::Conflict("automation has been removed".into()));
    }
    match change {
        AutoChange::Update {
            name,
            description,
            spec,
            workflow_id,
        } => {
            if let Some(name) = name {
                let trimmed = name.trim();
                if trimmed.is_empty() || trimmed.chars().count() > MAX_NAME {
                    return Err(AutoError::Invalid(format!(
                        "name needs 1..={MAX_NAME} chars"
                    )));
                }
                auto.name = trimmed.to_string();
            }
            if let Some(desc) = description {
                auto.description = desc.trim().to_string();
            }
            if let Some(wf) = workflow_id {
                auto.workflow_id = wf;
            }
            if let Some(next_spec) = spec {
                validate_spec(&next_spec).map_err(AutoError::Invalid)?;
                validate_scope(&next_spec, realm_id, auto.group_id, auto.host_node)
                    .map_err(AutoError::Invalid)?;
                if auto.approved_by.is_some() && is_widening(&auto.spec, &next_spec) {
                    auto.pending = Some(PendingChange {
                        spec: next_spec,
                        requested_by: actor,
                        requested_at_ms: now_ms,
                    });
                } else {
                    auto.spec = next_spec;
                    if auto.lifecycle == Lifecycle::WaitingForApproval {
                        auto.pending = Some(PendingChange {
                            spec: auto.spec.clone(),
                            requested_by: actor,
                            requested_at_ms: now_ms,
                        });
                    } else {
                        auto.pending = None;
                    }
                }
            }
        }
        AutoChange::RequestApproval => {
            let target = auto.pending.as_ref().map(|p| &p.spec).unwrap_or(&auto.spec);
            validate_spec(target).map_err(AutoError::Invalid)?;
            validate_scope(target, realm_id, auto.group_id, auto.host_node)
                .map_err(AutoError::Invalid)?;
            auto.pending = Some(PendingChange {
                spec: target.clone(),
                requested_by: actor,
                requested_at_ms: now_ms,
            });
            if matches!(auto.lifecycle, Lifecycle::Draft | Lifecycle::Paused) {
                auto.lifecycle = Lifecycle::WaitingForApproval;
            }
        }
        AutoChange::Approve => {
            let next_spec = auto
                .pending
                .take()
                .map(|p| p.spec)
                .unwrap_or_else(|| auto.spec.clone());
            validate_spec(&next_spec).map_err(AutoError::Invalid)?;
            validate_scope(&next_spec, realm_id, auto.group_id, auto.host_node)
                .map_err(AutoError::Invalid)?;
            auto.spec = next_spec;
            auto.lifecycle = Lifecycle::Active;
            auto.pause_reason = None;
            auto.approved_by = Some(actor);
            auto.approved_at_ms = Some(now_ms);
        }
        AutoChange::Reject => {
            auto.pending = None;
            if auto.lifecycle == Lifecycle::WaitingForApproval {
                auto.lifecycle = match auto.approved_by.is_some() {
                    true => Lifecycle::Paused,
                    false => Lifecycle::Draft,
                };
            }
        }
        AutoChange::Pause(reason) => {
            if auto.lifecycle != Lifecycle::Active {
                return Err(AutoError::Conflict(
                    "only active automations can be paused".into(),
                ));
            }
            auto.lifecycle = Lifecycle::Paused;
            auto.pause_reason = Some(reason);
        }
        AutoChange::Resume => {
            if auto.lifecycle != Lifecycle::Paused || auto.approved_by.is_none() {
                return Err(AutoError::Conflict(
                    "only approved paused automations can resume".into(),
                ));
            }
            auto.lifecycle = Lifecycle::Active;
            auto.pause_reason = None;
        }
        AutoChange::Delete => {
            auto.lifecycle = Lifecycle::Removed;
            auto.pending = None;
        }
        AutoChange::Identity { account, role_id } => {
            auto.account = account;
            auto.role_id = role_id;
        }
    }
    auto.updated_at_ms = now_ms;
    auto.revision = auto.revision.saturating_add(1);
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
enum WriteStep {
    Read,
    CheckActive(Box<Automation>),
    Write {
        record: Box<Automation>,
        drop_schedule: bool,
    },
    DropSchedule(Box<Automation>),
}

/// Applies one [`AutoChange`] transactionally with optional optimistic `expected` revision check.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteAutoOperation {
    id: Ulid,
    expected: Option<u64>,
    change: AutoChange,
    actor: UserId,
    realm_id: RealmId,
    now_ms: u64,
    step: WriteStep,
}

impl WriteAutoOperation {
    pub fn new(
        (id, expected): (Ulid, Option<u64>),
        change: AutoChange,
        actor: UserId,
        realm_id: RealmId,
    ) -> Self {
        Self {
            id,
            expected,
            change,
            actor,
            realm_id,
            now_ms: 0,
            step: WriteStep::Read,
        }
    }

    pub fn node(id: Ulid, change: AutoChange, actor: UserId, realm_id: RealmId) -> Self {
        Self::new((id, None), change, actor, realm_id)
    }

    fn commit(&mut self, mut auto: Automation) -> Result<Next<Automation>, AutoError> {
        let reset_schedule = matches!(self.change, AutoChange::Approve | AutoChange::Resume);
        let clear_schedule = matches!(
            self.change,
            AutoChange::Pause(_)
                | AutoChange::Delete
                | AutoChange::Approve
                | AutoChange::Update { .. }
        );
        apply_change(
            &mut auto,
            self.change.clone(),
            self.actor,
            self.realm_id,
            self.now_ms,
        )?;
        let mut writes = vec![auto_row(&auto)?];
        let keep_schedule = auto.lifecycle == Lifecycle::Active && auto.spec.has_schedules();
        if reset_schedule && keep_schedule {
            writes.push(schedule_row(auto.automation_id, self.now_ms));
        }
        let drop_schedule = clear_schedule && !keep_schedule;
        self.step = WriteStep::Write {
            record: Box::new(auto),
            drop_schedule,
        };
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for WriteAutoOperation {
    type Output = Automation;
    type Error = AutoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Automation>, AutoError> {
        self.step = WriteStep::Read;
        Ok(Next::Call(read_auto(self.id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Automation>, AutoError> {
        match std::mem::replace(&mut self.step, WriteStep::Read) {
            WriteStep::Read => {
                let auto = auto_answer(result)?.ok_or(AutoError::NotFound)?;
                if self.expected.is_some_and(|exp| exp != auto.revision) {
                    return Err(AutoError::Conflict(
                        "automation changed meanwhile; read again".into(),
                    ));
                }
                if matches!(self.change, AutoChange::Delete) && auto.lifecycle != Lifecycle::Removed
                {
                    self.step = WriteStep::CheckActive(Box::new(auto));
                    return Ok(Next::Call(active_page(None)));
                }
                self.commit(auto)
            }
            WriteStep::CheckActive(auto) => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    if decode_ulid(&value) == Some(self.id) {
                        return Err(AutoError::Conflict(
                            "cannot delete automation while active firings exist".into(),
                        ));
                    }
                }
                if let Some(after) = next {
                    self.step = WriteStep::CheckActive(auto);
                    return Ok(Next::Call(active_page(Some(after))));
                }
                self.commit(*auto)
            }
            WriteStep::Write {
                record,
                drop_schedule,
            } => {
                result.done()?;
                if drop_schedule {
                    let key = schedule_key(record.automation_id);
                    self.step = WriteStep::DropSchedule(record);
                    return Ok(Next::Call(IoCall::Delete(vec![(
                        AUTOMATIONS_KEYSPACE.to_string(),
                        key,
                    )])));
                }
                Ok(Next::Done(*record))
            }
            WriteStep::DropSchedule(record) => {
                result.done()?;
                Ok(Next::Done(*record))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
