//! Sets or removes the realm email profile and its realm vault password in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::realm_vault::{
    RealmSecretRef, RealmVaultEntry, RealmVaultError, RealmVaultKey, RealmVaultPurpose,
    RealmVaultRecord,
};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::execution::email::{RealmEmailProfile, SmtpTls};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, KeySpace, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use super::COMMIT_ATTEMPTS;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

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

/// Checks the caller's config permission, then writes the profile in a write transaction that
/// reruns after a lost conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct SetEmailOperation {
    config: Arc<SetEmailConfig>,
    authorized: bool,
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
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for SetEmailError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SetEmail".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SetEmailOperation {
    pub fn new(config: SetEmailConfig) -> Self {
        Self {
            config: Arc::new(config),
            authorized: false,
        }
    }

    fn change(&self) -> IoCall {
        let config = Arc::clone(&self.config);
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || EmailWrite::new(Arc::clone(&config));
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        IoCall::Child(child)
    }
}

impl Operation for SetEmailOperation {
    type Output = Option<RealmEmailProfile>;
    type Error = SetEmailError;

    fn first(&mut self) -> Result<Next<Self::Output>, SetEmailError> {
        if self.config.auth_context.realm_id != self.config.actor.realm_id {
            return Err(SetEmailError::Unauthorized);
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetEmailError> {
        if self.authorized {
            return result
                .child::<Option<RealmEmailProfile>, SetEmailError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => {
                self.authorized = true;
                Ok(Next::Call(self.change()))
            }
            Ok(false) => Err(SetEmailError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(error) => {
                warn!(error = %error, "Realm email profile authorization check failed");
                Err(SetEmailError::Unauthorized)
            }
        }
    }
}

/// What the email write transaction waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
}

/// Reads the realm config and its admin state, then stores the profile, its vault entries and the
/// queued admin events in one write transaction.
#[derive(Debug, PartialEq)]
struct EmailWrite {
    config: Arc<SetEmailConfig>,
    stage: Stage,
    profile: Option<Option<RealmEmailProfile>>,
    deletes: Vec<(KeySpace, Key)>,
}

impl EmailWrite {
    fn new(config: Arc<SetEmailConfig>) -> Self {
        Self {
            config,
            stage: Stage::Read,
            profile: None,
            deletes: Vec::new(),
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
    ) -> Result<Next<Option<RealmEmailProfile>>, SetEmailError> {
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

        self.profile = Some(profile);
        self.deletes = stale_conflict_deletes;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> Result<Next<Option<RealmEmailProfile>>, SetEmailError> {
        let output = self.profile.take().ok_or(SetEmailError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for EmailWrite {
    type Output = Option<RealmEmailProfile>;
    type Error = SetEmailError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> Result<Next<Self::Output>, SetEmailError> {
        let document = self.document_ref();
        let target = self.admin_target();
        Ok(Next::Call(IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&target),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, SetEmailError> {
        match self.stage {
            Stage::Read => {
                let [document, reducer] = values::<2>(result)?;
                self.emit_document_write(document, reducer)
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
                self.finish()
            }
        }
    }
}

#[cfg(test)]
#[path = "set_email_tests.rs"]
mod tests;
