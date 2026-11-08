//! Sets or removes the realm email profile and its realm vault password in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::compute::SecretBytes;
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::realm_vault::{
    RealmSecretRef, RealmVaultEntry, RealmVaultError, RealmVaultKey, RealmVaultPurpose,
    RealmVaultRecord,
};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::execution::email::{RealmEmailProfile, SmtpTls};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

/// The profile a realm admin enters. A missing password keeps the stored one for the same username.
#[derive(Debug, PartialEq)]
pub struct EmailInput {
    pub host: String,
    pub port: Option<u16>,
    pub tls: SmtpTls,
    pub username: Option<String>,
    pub password: Option<SecretBytes>,
    pub from: String,
}

#[derive(Debug, PartialEq)]
pub struct SetEmailConfig {
    pub actor: Actor,
    /// The caller's own token context, so a path-restricted credential stays restricted.
    pub auth_context: AuthContext,
    pub vault_key: RealmVaultKey,
    /// `None` removes the profile and its password.
    pub profile: Option<EmailInput>,
}

#[derive(Debug, PartialEq)]
pub struct SetEmailOperation {
    config: SetEmailConfig,
    txn_id: Option<TxnId>,
    state: SetEmailState,
    output: Option<Result<Option<RealmEmailProfile>, SetEmailError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetEmailState {
    Init,
    Auth,
    StartTransaction,
    ReadCurrent,
    WriteDocumentState {
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteAdminConflicts,
    CommitTransaction,
    ScheduleSyncDrain,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetEmailError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error(transparent)]
    Vault(#[from] RealmVaultError),
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("caller may not write the realm configuration")]
    Unauthorized,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("invalid email profile: {reason}")]
    InvalidProfile { reason: String },
    #[error("a new or changed username needs a password")]
    PasswordRequired,
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

impl SetEmailOperation {
    pub fn new(config: SetEmailConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetEmailState::Init,
            output: None,
        }
    }

    fn document_ref(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = SetEmailState::ReadCurrent;
        let document = self.document_ref();
        let target = self.admin_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&target),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    /// The vault writes and the profile write, in that order, so a receiver sees the record first.
    fn email_writes(
        &self,
        state: &AdminDocumentState,
    ) -> Result<(Vec<AdminDocumentOperation>, Option<RealmEmailProfile>), SetEmailError> {
        let (current, live) = (
            state.materialized_realm_email(),
            state.materialized_realm_vault(),
        );
        // Every earlier or competing SMTP password goes, unless the new profile keeps it.
        let mut stale: BTreeSet<RealmVaultEntry> = (state.realm_email_candidates().iter())
            .filter_map(|profile| profile.password.map(|password| password.entry))
            .chain(live.iter().map(|record| record.entry))
            .filter(|entry| entry.purpose == RealmVaultPurpose::SmtpPassword)
            .collect();
        let mut writes = Vec::new();
        let profile = match &self.config.profile {
            None => None,
            Some(input) => {
                let password = self.password(input, current.as_ref(), &mut writes)?;
                if let Some(password) = password {
                    stale.remove(&password.entry);
                }
                let profile = RealmEmailProfile {
                    revision: Ulid::generate(),
                    host: input.host.trim().to_string(),
                    port: input.port,
                    tls: input.tls,
                    username: input.username.clone(),
                    from: input.from.trim().to_string(),
                    password,
                    changed_by: self.config.actor.user_id,
                    changed_at_ms: unix_timestamp_millis(),
                };
                if !profile.valid() {
                    return Err(SetEmailError::InvalidProfile {
                        reason: "host and sender address are required".to_string(),
                    });
                }
                Some(profile)
            }
        };
        let tombstones = stale
            .into_iter()
            .map(|entry| AdminDocumentOperation::ConfigVaultSet {
                entry,
                record: None,
            });
        let mut operations: Vec<_> = tombstones.chain(writes).collect();
        operations.push(AdminDocumentOperation::ConfigEmailSet {
            profile: profile.clone(),
        });
        Ok((operations, profile))
    }

    /// Seals a new password under a new revision, or keeps the stored one for the same username.
    fn password(
        &self,
        input: &EmailInput,
        current: Option<&RealmEmailProfile>,
        writes: &mut Vec<AdminDocumentOperation>,
    ) -> Result<Option<RealmSecretRef>, SetEmailError> {
        match (&input.username, &input.password) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(SetEmailError::InvalidProfile {
                reason: "a password needs a username".to_string(),
            }),
            (Some(username), None) => current
                .filter(|current| current.username.as_ref() == Some(username))
                .and_then(|current| current.password)
                .map(Some)
                .ok_or(SetEmailError::PasswordRequired),
            (Some(_), Some(secret)) => {
                let entry = (current.and_then(|current| current.password))
                    .map(|password| password.entry)
                    .unwrap_or_else(|| {
                        RealmVaultEntry::new(RealmVaultPurpose::SmtpPassword, Ulid::generate())
                    });
                let revision = Ulid::generate();
                let realm_id = self.config.actor.realm_id;
                let sealed = (self.config.vault_key).seal(&realm_id, entry, revision, secret)?;
                writes.push(AdminDocumentOperation::ConfigVaultSet {
                    entry,
                    record: Some(RealmVaultRecord {
                        entry,
                        revision,
                        sealed,
                        changed_by: self.config.actor.user_id,
                        changed_at_ms: unix_timestamp_millis(),
                    }),
                });
                Ok(Some(RealmSecretRef { entry, revision }))
            }
        }
    }

    fn emit_document_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, SetEmailError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetEmailError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(SetEmailError::ConfigMissing);
        };
        let mut document = RealmConfigDocument::from_bytes(&document_value)?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetEmailError::NotManagementNode);
        }
        let target = self.admin_target();
        let previous_reducer_state = reducer_state_value
            .as_ref()
            .map(|value| {
                aruna_core::reducer::decode_reducer_state(value.as_ref())
                    .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous_reducer_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }
        let mut reducer_state = previous_reducer_state
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let (operations, profile) = self.email_writes(&reducer_state)?;
        let mut events = Vec::new();
        for operation in operations {
            if let AdminDocumentOperation::ConfigVaultSet { entry, record } = &operation {
                let revision = record.as_ref().map(|record| record.revision);
                info!(?entry, ?revision, user = %self.config.actor.user_id, "Vault entry set");
            }
            events.push(reducer_state.apply_operation(&self.config.actor, operation)?);
        }
        document.email = reducer_state.materialized_realm_email();
        document.vault = reducer_state.materialized_realm_vault();
        let stale_conflict_deletes =
            stale_conflict_deletes(previous_reducer_state.as_ref(), Some(&reducer_state));
        let document_target = self.document_ref();
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            reducer_state_entry(&reducer_state)?,
        ];
        for event in events {
            let record = new_identified_record(
                event.event_id,
                self.config.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event),
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(profile));
        self.state = SetEmailState::WriteDocumentState {
            stale_conflict_deletes,
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit_transaction(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetEmailError::MissingTransaction);
        };
        self.state = SetEmailState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetEmailError) -> Effects {
        let cleanup = self.abort();
        self.state = SetEmailState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetEmailError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetEmailOperation {
    type Output = Option<RealmEmailProfile>;
    type Error = SetEmailError;

    fn start(&mut self) -> Effects {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(SetEmailError::Unauthorized);
        }
        self.state = SetEmailState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state.clone() {
            SetEmailState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => {
                            self.state = SetEmailState::StartTransaction;
                            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                                read: false
                            })]
                        }
                        Ok(false) => self.fail(SetEmailError::Unauthorized),
                        Err(AuthorizationError::StorageError(error)) => self.fail(error.into()),
                        Err(error) => {
                            warn!(error = %error, "Realm email profile authorization check failed");
                            self.fail(SetEmailError::Unauthorized)
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            SetEmailState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetEmailState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self
                        .emit_document_write(document_value.clone(), reducer_state_value.clone())
                    {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            SetEmailState::WriteDocumentState {
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetEmailError::MissingTransaction);
                    };
                    if !stale_conflict_deletes.is_empty() {
                        self.state = SetEmailState::DeleteAdminConflicts;
                        return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                            deletes: stale_conflict_deletes,
                            txn_id: Some(txn_id),
                        })];
                    }
                    self.emit_commit_transaction()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            SetEmailState::DeleteAdminConflicts => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    self.emit_commit_transaction()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetEmailState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = SetEmailState::ScheduleSyncDrain;
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetEmailState::ScheduleSyncDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetEmailState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetEmailState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetEmailState::Finish | SetEmailState::Error | SetEmailState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SetEmailState::Finish | SetEmailState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(SetEmailError::NotFinished);
        }
        self.output.unwrap_or(Err(SetEmailError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "set_email_tests.rs"]
mod tests;
