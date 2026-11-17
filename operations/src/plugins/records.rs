//! Writes one plugin install record into the realm config as an admin of the install's group.
//! The record's revision must match what the caller read, so a local race loses no change.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::placement::target_placement_ref;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::task::TaskEvent;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use crate::users::service_account::group_admin_path;

#[derive(Debug, PartialEq)]
pub struct InstallEntryConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays restricted.
    pub auth_context: AuthContext,
    pub install_id: Ulid,
    pub group_id: GroupId,
    /// The revision the current record must have; `None` when it must not exist yet.
    pub expected: Option<u64>,
    /// The record to write, or `None` to delete it.
    pub install: Option<PluginInstall>,
    /// The admin audit row written in the same transaction.
    pub audit: Option<AdminAuditRecord>,
}

#[derive(Debug, Error, PartialEq)]
pub enum RecordError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("caller may not administer this group's plugins")]
    Unauthorized,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("the install changed meanwhile (revision {0:?}); read it again")]
    Stale(Option<u64>),
    #[error("the record does not belong to this install and group")]
    Mismatch,
    #[error("the node that runs an install cannot change")]
    ReceiverChanged,
    #[error(transparent)]
    Call(IoError),
}

/// A lost transaction stays a storage conflict, so callers can tell it from other failures.
impl From<IoError> for RecordError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Call(other),
        }
    }
}

#[derive(Debug, PartialEq)]
enum Phase {
    Auth,
    Read,
    Write { stale: Vec<(KeySpace, Key)> },
    DeleteConflicts,
}

/// Checks the caller's group admin right, then reads, checks and writes the record in one
/// transaction. [`write_entry`] wakes the outbox drain after the commit.
#[derive(Debug, PartialEq)]
pub struct InstallEntryOperation {
    config: InstallEntryConfig,
    phase: Phase,
}

impl InstallEntryOperation {
    pub fn new(config: InstallEntryConfig) -> Self {
        Self {
            config,
            phase: Phase::Auth,
        }
    }

    fn document(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn read(&mut self) -> IoCall {
        self.phase = Phase::Read;
        let document = self.document();
        IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&self.admin_target()),
            ),
        ])
    }

    /// Whether the new record keeps this install's id and group and raises the revision by one.
    fn matches(&self) -> bool {
        let next = self.config.expected.map_or(1, |revision| revision + 1);
        self.config.install.as_ref().is_none_or(|install| {
            install.install_id == self.config.install_id
                && install.group_id == self.config.group_id
                && install.revision == next
        })
    }

    fn authorized(&mut self, result: IoResult) -> Result<IoCall, RecordError> {
        let event = match result {
            IoResult::Event(event) => *event,
            other => return Err(unexpected(other.label())),
        };
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return Err(unexpected(event.label()));
        };
        match allowed {
            Ok(true) => Ok(self.read()),
            Ok(false) => Err(RecordError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(error) => {
                warn!(%error, "Plugin install authorization check failed");
                Err(RecordError::Unauthorized)
            }
        }
    }

    fn write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<IoCall, RecordError> {
        let document_value = document_value.ok_or(RecordError::ConfigMissing)?;
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(RecordError::NotManagementNode);
        }
        let target = self.admin_target();
        let previous = (reducer_state_value.as_ref())
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
        let current = (reducer_state.materialized_installs().into_iter())
            .find(|install| install.install_id == self.config.install_id);
        if current
            .as_ref()
            .is_some_and(|current| current.group_id != self.config.group_id)
        {
            return Err(RecordError::Mismatch);
        }
        let revision = current.as_ref().map(|current| current.revision);
        if revision != self.config.expected {
            return Err(RecordError::Stale(revision));
        }
        let moved = (current.as_ref().zip(self.config.install.as_ref()))
            .is_some_and(|(current, next)| current.receiver != next.receiver);
        if moved {
            return Err(RecordError::ReceiverChanged);
        }
        let operation = AdminDocumentOperation::ConfigInstallSet {
            install_id: self.config.install_id,
            install: self.config.install.clone().map(Box::new),
        };
        let event = reducer_state.apply_operation(&self.config.actor, operation)?;
        document.installs = reducer_state.materialized_installs();
        let document_target = self.document();
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let record = new_identified_record(
            event.event_id,
            self.config.actor.node_id,
            document_target.clone(),
            Vec::new(),
            DocumentOutboxEvent::admin(event),
            placement,
            false,
        );
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
            outbox_write_entry(&record).map_err(ConversionError::from)?,
        ];
        writes.extend(conflict_write_entries(&reducer_state)?);
        if let Some(audit) = &self.config.audit {
            writes.extend(audit.entries()?);
        }
        self.phase = Phase::Write {
            stale: stale_conflict_deletes(previous.as_ref(), Some(&reducer_state)),
        };
        Ok(IoCall::Write(writes))
    }
}

impl Operation for InstallEntryOperation {
    type Output = Option<PluginInstall>;
    type Error = RecordError;

    fn transaction(&self) -> bool {
        true
    }

    fn first(&mut self) -> Result<Next<Self::Output>, RecordError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id || !self.matches() {
            return Err(RecordError::Mismatch);
        }
        let realm_id = self.config.actor.realm_id;
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.config.auth_context.clone(),
            path: group_admin_path(realm_id, self.config.group_id),
            required_permission: Permission::WRITE,
        });
        Ok(Next::Call(IoCall::Child(boxed_suboperation(
            check,
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, RecordError> {
        match std::mem::replace(&mut self.phase, Phase::DeleteConflicts) {
            Phase::Auth => Ok(Next::Call(self.authorized(result)?)),
            Phase::Read => {
                let [document, state] = values::<2>(result)?;
                Ok(Next::Call(self.write(document, state)?))
            }
            Phase::Write { stale } => {
                done(result)?;
                match stale.is_empty() {
                    true => Ok(Next::Done(self.config.install.clone())),
                    false => Ok(Next::Call(IoCall::Delete(stale))),
                }
            }
            Phase::DeleteConflicts => {
                done(result)?;
                Ok(Next::Done(self.config.install.clone()))
            }
        }
    }
}

fn unexpected(got: &'static str) -> RecordError {
    RecordError::Call(IoError::Unexpected {
        expected: "an authorization result",
        got,
    })
}

/// Writes or deletes the record, then wakes the outbox drain; a failed wake only delays the drain.
pub(crate) async fn write_entry(
    ctx: &DriverContext,
    config: InstallEntryConfig,
) -> Result<Option<PluginInstall>, RecordError> {
    let written = drive(InstallEntryOperation::new(config), ctx).await?;
    if let Err(error) = drive(WakeDrain, ctx).await {
        warn!(%error, "Failed to schedule the outbox drain; the durable outbox stays retryable");
    }
    Ok(written)
}

/// Schedules the outbox drain at once.
#[derive(Debug, PartialEq)]
struct WakeDrain;

impl Operation for WakeDrain {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Effect(
            Box::new(schedule_drain_effect()),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let expected = "outbox drain timer schedule";
        let event = match result {
            IoResult::Event(event) => *event,
            other => {
                return Err(IoError::Unexpected {
                    expected,
                    got: other.label(),
                });
            }
        };
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(Next::Done(())),
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(error = %message, "Failed to schedule the outbox drain; the durable outbox stays retryable");
                Ok(Next::Done(()))
            }
            other => Err(IoError::Unexpected {
                expected,
                got: other.label(),
            }),
        }
    }
}

#[cfg(test)]
#[path = "records_tests.rs"]
mod tests;
