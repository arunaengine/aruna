//! Grants or removes the realm administrator role for one user and keeps at least one admin.
//! It writes through the shared admin document path, so every node applies the change.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::UserId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget, roles_narrowed};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::realm::assign_role::should_seed_role;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

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

#[derive(Debug, PartialEq)]
pub struct SetAdminOperation {
    config: SetAdminConfig,
    txn_id: Option<TxnId>,
    state: SetAdminState,
    narrowed: bool,
    output: Option<Result<HashSet<UserId>, SetAdminError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetAdminState {
    Init,
    Auth,
    StartTransaction,
    ReadCurrent,
    WriteDocument {
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteConflicts,
    CommitTransaction,
    ScheduleDrain,
    Finish,
    Error,
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
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl SetAdminOperation {
    pub fn new(config: SetAdminConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetAdminState::Init,
            narrowed: false,
            output: None,
        }
    }

    fn emit_read(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = SetAdminState::ReadCurrent;
        let realm_id = self.config.actor.realm_id;
        let config = DocumentTarget::RealmConfig { realm_id };
        let target = AdminDocumentTarget::Realm { realm_id };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (config.storage_keyspace().to_string(), config.storage_key()),
                (AUTH_KEYSPACE.to_string(), (*realm_id.as_bytes()).into()),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn emit_write(
        &mut self,
        config_value: Option<&Value>,
        auth_value: Option<&Value>,
        state_value: Option<&Value>,
    ) -> Result<Effects, SetAdminError> {
        let txn_id = self.txn_id.ok_or(SetAdminError::MissingTransaction)?;
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

        self.output = Some(Ok(admins));
        self.state = SetAdminState::WriteDocument {
            stale_conflict_deletes: stale_conflict_deletes(previous.as_ref(), Some(&reducer_state)),
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    /// A removal narrows access, so key epochs are marked due inside the same transaction.
    fn emit_commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetAdminError::MissingTransaction);
        };
        self.state = SetAdminState::CommitTransaction;
        if self.narrowed {
            return smallvec![crate::abe::mark_due(None, txn_id)];
        }
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetAdminError) -> Effects {
        let cleanup = self.abort();
        self.state = SetAdminState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: &Event) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetAdminError::UnexpectedEvent {
            state,
            expected,
            got: format!("{got:?}"),
        })
    }
}

impl Operation for SetAdminOperation {
    type Output = HashSet<UserId>;
    type Error = SetAdminError;

    fn start(&mut self) -> Effects {
        let realm_id = self.config.actor.realm_id;
        let user_id = self.config.user_id;
        if user_id.is_nil() || user_id.realm_id != realm_id {
            return self.fail(SetAdminError::InvalidUser);
        }
        if self.config.auth_context.realm_id != realm_id {
            return self.fail(SetAdminError::Unauthorized);
        }
        self.state = SetAdminState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: format!("/{realm_id}/admin/roles/{user_id}"),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            if self.state == SetAdminState::CommitTransaction {
                self.txn_id = None;
            }
            return self.fail(error.into());
        }
        if self.state == SetAdminState::CommitTransaction
            && let Some(txn_id) = self.txn_id
            && let Some(next) = crate::abe::marked(&event, txn_id)
        {
            return next.unwrap_or_else(|error| self.fail(error.into()));
        }
        match (self.state.clone(), &event) {
            (
                SetAdminState::Auth,
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
            ) => match allowed {
                Ok(true) => {
                    self.state = SetAdminState::StartTransaction;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Ok(false) => self.fail(SetAdminError::Unauthorized),
                Err(error) => {
                    warn!(error = %error, "Realm admin authorization check failed");
                    match error {
                        AuthorizationError::StorageError(error) => self.fail(error.clone().into()),
                        _ => self.fail(SetAdminError::Unauthorized),
                    }
                }
            },
            (
                SetAdminState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => self.emit_read(*txn_id),
            (
                SetAdminState::ReadCurrent,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                let [(_, config), (_, auth), (_, reducer)] = values.as_slice() else {
                    return self.unexpected_event("three read values", &event);
                };
                match self.emit_write(config.as_ref(), auth.as_ref(), reducer.as_ref()) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            (
                SetAdminState::WriteDocument {
                    stale_conflict_deletes,
                },
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => {
                if stale_conflict_deletes.is_empty() {
                    return self.emit_commit();
                }
                let Some(txn_id) = self.txn_id else {
                    return self.fail(SetAdminError::MissingTransaction);
                };
                self.state = SetAdminState::DeleteConflicts;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: stale_conflict_deletes,
                    txn_id: Some(txn_id),
                })]
            }
            (
                SetAdminState::DeleteConflicts,
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => self.emit_commit(),
            (
                SetAdminState::CommitTransaction,
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.state = SetAdminState::ScheduleDrain;
                smallvec![schedule_drain_effect()]
            }
            (SetAdminState::ScheduleDrain, Event::Task(TaskEvent::TimerScheduled { .. })) => {
                self.state = SetAdminState::Finish;
                smallvec![]
            }
            (SetAdminState::ScheduleDrain, Event::Task(TaskEvent::Error { message, .. })) => {
                warn!(error = %message, "Failed to schedule admin document outbox drain; durable outbox remains retryable");
                self.state = SetAdminState::Finish;
                smallvec![]
            }
            (SetAdminState::Init | SetAdminState::Finish | SetAdminState::Error, _) => smallvec![],
            _ => self.unexpected_event("an event for the current state", &event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SetAdminState::Finish | SetAdminState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !matches!(self.state, SetAdminState::Finish | SetAdminState::Error) {
            return Err(SetAdminError::NotFinished);
        }
        self.output.unwrap_or(Err(SetAdminError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}
