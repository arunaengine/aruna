//! Grants or removes the realm administrator role for one user and keeps at least one admin.
//! It writes through the shared admin document path, so every node applies the change.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::UserId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget, roles_narrowed};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::rows::done;
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;

use super::COMMIT_ATTEMPTS;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::epoch_due::DueMarks;
use crate::realm::assign_role::should_seed_role;
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

/// The default realm role that carries WRITE on the whole realm admin tree.
const ADMIN_ROLE: &str = "realm_admin";

#[derive(Debug, Clone, PartialEq)]
pub struct SetAdminConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays restricted.
    pub auth_context: AuthContext,
    pub user_id: UserId,
    /// `true` grants the role, `false` removes it.
    pub admin: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetAdminError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("caller may not change the realm administrators")]
    Unauthorized,
    #[error("the user id is not a user of this realm")]
    InvalidUser,
    #[error("realm config or authorization document missing")]
    DocumentMissing,
    #[error("the realm has no realm_admin role")]
    RoleMissing,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("the realm keeps at least one realm administrator")]
    LastAdmin,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for SetAdminError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetAdmin".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

/// Checks the caller, then grants or removes the role in a write transaction that reruns after a
/// lost conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct SetAdminOperation {
    config: SetAdminConfig,
    authorized: bool,
}

impl SetAdminOperation {
    pub fn new(config: SetAdminConfig) -> Self {
        Self {
            config,
            authorized: false,
        }
    }

    fn change(&self) -> IoCall {
        let config = self.config.clone();
        let seed = self.config.user_id.to_bytes();
        let build = move || SetAdminWrite::new(config.clone());
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetAdminOperation {
    type Output = HashSet<UserId>;
    type Error = SetAdminError;

    fn first(&mut self) -> Result<Next<HashSet<UserId>>, SetAdminError> {
        let realm_id = self.config.actor.realm_id;
        let user_id = self.config.user_id;
        if user_id.is_nil() || user_id.realm_id != realm_id {
            return Err(SetAdminError::InvalidUser);
        }
        if self.config.auth_context.realm_id != realm_id {
            return Err(SetAdminError::Unauthorized);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: format!("/{realm_id}/admin/roles/{user_id}"),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HashSet<UserId>>, SetAdminError> {
        if self.authorized {
            return result
                .child::<HashSet<UserId>, SetAdminError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetAdminError::Unauthorized),
            Err(error) => {
                warn!(error = %error, "Realm admin authorization check failed");
                match error {
                    AuthorizationError::StorageError(error) => Err(error.into()),
                    _ => Err(SetAdminError::Unauthorized),
                }
            }
        }
    }
}

/// What the role change transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
    Marks,
}

/// Reads, validates and applies the role change in one write transaction; a removal marks key
/// epochs due in the same transaction.
#[derive(Debug, PartialEq)]
struct SetAdminWrite {
    config: SetAdminConfig,
    stage: Stage,
    narrowed: bool,
    admins: HashSet<UserId>,
    deletes: Vec<(KeySpace, Key)>,
    marks: DueMarks,
}

impl SetAdminWrite {
    fn new(config: SetAdminConfig) -> Self {
        Self {
            config,
            stage: Stage::Read,
            narrowed: false,
            admins: HashSet::new(),
            deletes: Vec::new(),
            marks: DueMarks::new(None, aruna_core::time::unix_timestamp_millis()),
        }
    }

    fn emit_write(
        &mut self,
        config_value: Option<&Value>,
        auth_value: Option<&Value>,
        state_value: Option<&Value>,
    ) -> Result<Next<HashSet<UserId>>, SetAdminError> {
        let actor = &self.config.actor;
        let (Some(config_value), Some(auth_value)) = (config_value, auth_value) else {
            return Err(SetAdminError::DocumentMissing);
        };
        let config = RealmConfigDocument::from_bytes(config_value)?;
        if !is_management(&config, actor.node_id) {
            return Err(SetAdminError::NotManagementNode);
        }
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(auth_value)?;
        let role_id = auth_doc
            .roles
            .values()
            .find(|role| role.name == ADMIN_ROLE)
            .map(|role| role.role_id)
            .ok_or(SetAdminError::RoleMissing)?;

        let target = AdminDocumentTarget::Realm {
            realm_id: actor.realm_id,
        };
        let previous = state_value
            .map(|value| decode_reducer_state(value.as_ref()).map_err(ConversionError::from))
            .transpose()?;
        if previous
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }
        let mut reducer_state = previous
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let mut admin_events = Vec::new();
        if should_seed_role(&reducer_state, role_id) {
            admin_events.push(
                reducer_state
                    .apply_operation(actor, AdminDocumentOperation::RealmRoleAdded { role_id })?,
            );
        }
        let user_id = self.config.user_id;
        let change = if self.config.admin {
            AdminDocumentOperation::RealmAssignmentAdded { role_id, user_id }
        } else {
            AdminDocumentOperation::RealmAssignmentRemoved { role_id, user_id }
        };
        admin_events.push(reducer_state.apply_operation(actor, change)?);

        let roles_before = auth_doc.roles.clone();
        let assigned = reducer_state
            .materialized_realm_assignments()
            .get(&role_id)
            .is_some_and(|users| users.contains(&user_id));
        let role = auth_doc
            .roles
            .get_mut(&role_id)
            .ok_or(SetAdminError::RoleMissing)?;
        if assigned {
            role.assigned_users.insert(user_id);
        } else {
            role.assigned_users.remove(&user_id);
        }
        if role.assigned_users.is_empty() {
            return Err(SetAdminError::LastAdmin);
        }
        let admins = role.assigned_users.clone();
        self.narrowed = roles_narrowed(&roles_before, &auth_doc.roles);

        let document_target = DocumentTarget::RealmAuthorization {
            realm_id: actor.realm_id,
        };
        let placement = target_placement_ref(&config, &document_target, Default::default());
        let mut writes = vec![
            (
                AUTH_KEYSPACE.to_string(),
                (*actor.realm_id.as_bytes()).into(),
                auth_doc.to_bytes(actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
        ];
        for admin_event in admin_events {
            let record = new_identified_record(
                admin_event.event_id,
                actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(admin_event),
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.admins = admins;
        self.deletes = stale_conflict_deletes(previous.as_ref(), Some(&reducer_state));
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// Marks key epochs due when the change narrowed access, then wakes the outbox drain.
    fn after_writes(&mut self) -> Result<Next<HashSet<UserId>>, SetAdminError> {
        if self.narrowed {
            self.stage = Stage::Marks;
            return Ok(Next::Call(self.marks.first()));
        }
        Ok(self.finish())
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Next<HashSet<UserId>> {
        Next::Then {
            output: std::mem::take(&mut self.admins),
            calls: vec![IoCall::Effect(Box::new(schedule_drain_effect()))],
        }
    }
}

impl Operation for SetAdminWrite {
    type Output = HashSet<UserId>;
    type Error = SetAdminError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> Result<Next<HashSet<UserId>>, SetAdminError> {
        let realm_id = self.config.actor.realm_id;
        let config = DocumentTarget::RealmConfig { realm_id };
        let target = AdminDocumentTarget::Realm { realm_id };
        Ok(Next::Call(IoCall::Read(vec![
            (config.storage_keyspace().to_string(), config.storage_key()),
            (AUTH_KEYSPACE.to_string(), (*realm_id.as_bytes()).into()),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HashSet<UserId>>, SetAdminError> {
        match self.stage {
            Stage::Read => {
                let [config, auth, reducer] = aruna_core::rows::values::<3>(result)?;
                self.emit_write(config.as_ref(), auth.as_ref(), reducer.as_ref())
            }
            Stage::Write if !self.deletes.is_empty() => {
                done(result)?;
                self.stage = Stage::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            Stage::Write | Stage::Delete => {
                done(result)?;
                self.after_writes()
            }
            Stage::Marks => match self.marks.next(result, true) {
                Ok(Some(call)) => Ok(Next::Call(call)),
                Ok(None) => Ok(self.finish()),
                Err(error) => Err(StorageError::WriteError(error.to_string()).into()),
            },
        }
    }
}

#[cfg(test)]
#[path = "set_admin_tests.rs"]
mod tests;
