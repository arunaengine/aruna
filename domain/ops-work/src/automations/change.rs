//! Creates and atomically mutates automation records with revision checks and widening rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::dag::{validate_scope, validate_spec};
use aruna_core::automation::record::{Automation, Lifecycle, PauseReason, PendingChange, Spec};
use aruna_core::automation::scope::is_widening;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::{GroupId, RoleId};
use aruna_core::user::id::UserId;
use ulid::Ulid;

use super::firing_reads::decode_ulid;
use super::{AutoError, active_page, allowed, authorize_path, auto_answer, auto_row, read_auto};

const MAX_NAME: usize = 120;

#[derive(Clone, Debug, PartialEq)]
pub struct NewAuto {
    pub group_id: GroupId,
    pub name: String,
    pub description: String,
    pub spec: Spec,
}

/// Creates a new draft automation on `local` after checking group WRITE and validating its DAG.
#[derive(Debug, PartialEq)]
pub struct CreateAutoOperation {
    auth: AuthContext,
    local: NodeId,
    input: NewAuto,
    ids: IdMint,
    now_ms: u64,
    record: Option<Automation>,
}

impl CreateAutoOperation {
    pub fn new(auth: &AuthContext, local: NodeId, input: NewAuto) -> Self {
        Self {
            auth: auth.clone(),
            local,
            input,
            ids: IdMint::default(),
            now_ms: 0,
            record: None,
        }
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
        let name = self.input.name.trim();
        if name.is_empty() || name.chars().count() > MAX_NAME {
            return Err(AutoError::Invalid(format!(
                "name needs 1..={MAX_NAME} chars"
            )));
        }
        validate_spec(&self.input.spec).map_err(AutoError::Invalid)?;
        validate_scope(
            &self.input.spec,
            self.auth.realm_id,
            self.input.group_id,
            self.local,
        )
        .map_err(AutoError::Invalid)?;
        let path = group_permission_path(self.auth.realm_id, self.input.group_id, self.local);
        Ok(Next::Call(authorize_path(
            &self.auth,
            &path,
            Permission::WRITE,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Automation>, AutoError> {
        if let Some(record) = self.record.take() {
            result.done()?;
            return Ok(Next::Done(record));
        }
        if !allowed(result)? {
            return Err(AutoError::Forbidden);
        }
        let record = Automation {
            automation_id: self.ids.mint(self.now_ms),
            group_id: self.input.group_id,
            name: self.input.name.trim().to_string(),
            description: self.input.description.trim().to_string(),
            host_node: self.local,
            lifecycle: Lifecycle::Draft,
            pause_reason: None,
            spec: self.input.spec.clone(),
            pending: None,
            account: None,
            role_id: None,
            approved_by: None,
            approved_at_ms: None,
            created_by: self.auth.user_id,
            created_at_ms: self.now_ms,
            updated_at_ms: self.now_ms,
            revision: 1,
        };
        let row = auto_row(&record)?;
        self.record = Some(record);
        Ok(Next::Call(IoCall::Write(vec![row])))
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

fn apply(
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
    Write(Box<Automation>),
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
        apply(
            &mut auto,
            self.change.clone(),
            self.actor,
            self.realm_id,
            self.now_ms,
        )?;
        let row = auto_row(&auto)?;
        self.step = WriteStep::Write(Box::new(auto));
        Ok(Next::Call(IoCall::Write(vec![row])))
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
            WriteStep::Write(record) => {
                result.done()?;
                Ok(Next::Done(*record))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
