//! Writes one plugin install record into the realm config as an admin of the install's group.
//! The record's revision must match what the caller read, so a local race loses no change.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::admin_audit::AdminAuditRecord;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
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
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum RecordState {
    Init,
    Auth,
    StartTransaction,
    ReadCurrent,
    WriteState { stale: Vec<(KeySpace, Key)> },
    DeleteConflicts,
    CommitTransaction,
    ScheduleSyncDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct InstallEntryOperation {
    config: InstallEntryConfig,
    txn_id: Option<TxnId>,
    state: RecordState,
    output: Option<Result<Option<PluginInstall>, RecordError>>,
}

impl InstallEntryOperation {
    pub fn new(config: InstallEntryConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: RecordState::Init,
            output: None,
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

    fn emit_read(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = RecordState::ReadCurrent;
        let document = self.document();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key()
                ),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&self.admin_target())
                ),
            ],
            txn_id: Some(txn_id),
        })]
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

    fn emit_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, RecordError> {
        let txn_id = self.txn_id.ok_or(RecordError::MissingTransaction)?;
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
        self.output = Some(Ok(self.config.install.clone()));
        self.state = RecordState::WriteState {
            stale: stale_conflict_deletes(previous.as_ref(), Some(&reducer_state)),
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(RecordError::MissingTransaction);
        };
        self.state = RecordState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: RecordError) -> Effects {
        let cleanup = self.abort();
        self.state = RecordState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(RecordError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn handle_auth(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected("authorization result", format!("{event:?}"));
        };
        match allowed {
            Ok(true) => {
                self.state = RecordState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            Ok(false) => self.fail(RecordError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => self.fail(error.into()),
            Err(error) => {
                warn!(%error, "Plugin install authorization check failed");
                self.fail(RecordError::Unauthorized)
            }
        }
    }

    fn handle_written(&mut self, event: Event, stale: Vec<(KeySpace, Key)>) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) if stale.is_empty() => {
                self.emit_commit()
            }
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(RecordError::MissingTransaction);
                };
                self.state = RecordState::DeleteConflicts;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: stale,
                    txn_id: Some(txn_id),
                })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.unexpected("storage batch write result", format!("{other:?}")),
        }
    }
}

impl Operation for InstallEntryOperation {
    type Output = Option<PluginInstall>;
    type Error = RecordError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id || !self.matches() {
            return self.fail(RecordError::Mismatch);
        }
        self.state = RecordState::Auth;
        let realm_id = self.config.actor.realm_id;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: group_admin_path(realm_id, self.config.group_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            RecordState::Auth => self.handle_auth(event),
            RecordState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction start result", format!("{other:?}")),
            },
            RecordState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document), (_, state)] = values.as_slice() else {
                        return self
                            .unexpected("realm config and reducer state", format!("{values:?}"));
                    };
                    match self.emit_write(document.clone(), state.clone()) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("storage batch read result", format!("{other:?}")),
            },
            RecordState::WriteState { stale } => self.handle_written(event, stale),
            RecordState::DeleteConflicts => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.emit_commit(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("storage batch delete result", format!("{other:?}")),
            },
            RecordState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = RecordState::ScheduleSyncDrain;
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected("transaction commit result", format!("{other:?}")),
            },
            RecordState::ScheduleSyncDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = RecordState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule the outbox drain; the durable outbox stays retryable");
                    self.state = RecordState::Finish;
                    smallvec![]
                }
                other => self.unexpected("outbox drain timer schedule", format!("{other:?}")),
            },
            RecordState::Finish | RecordState::Error | RecordState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RecordState::Finish | RecordState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(RecordError::NotFinished);
        }
        self.output.unwrap_or(Err(RecordError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}
