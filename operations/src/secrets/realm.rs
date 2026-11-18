//! Stores named realm secrets in the realm config: the record by name and the value in the realm
//! vault, written by a management node in one admin document with the audit row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{AUTH_KEYSPACE, DOCUMENT_STATE_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::realm_vault::{RealmSecretRef, RealmVaultError, RealmVaultKey, RealmVaultRecord};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::rows;
use aruna_core::secrets::{SecretRecord, SecretScope, UseRule};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::{Key, KeySpace, Value};
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

use super::{Applied, ChangeError, Managers, SecretChange, apply_change, drains, widening_notices};
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{new_identified_record, outbox_write_entry};
use aruna_core::placement::target_placement_ref;

/// One realm secret change, with the time and write id it uses.
#[derive(Debug, PartialEq)]
pub struct RealmChange {
    pub actor: Actor,
    pub vault_key: RealmVaultKey,
    pub change: SecretChange,
    pub now_ms: u64,
    pub write_id: Ulid,
}

#[derive(Debug, Error, PartialEq)]
pub enum RealmSecretError {
    #[error(transparent)]
    Change(#[from] ChangeError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Document(#[from] AdminDocumentError),
    #[error(transparent)]
    Vault(#[from] RealmVaultError),
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("this node is not a realm management node")]
    NotManagementNode,
}

impl From<IoError> for RealmSecretError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<aruna_core::secrets::SecretError> for RealmSecretError {
    fn from(error: aruna_core::secrets::SecretError) -> Self {
        Self::Change(ChangeError::Invalid(error))
    }
}

/// The call the operation waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    /// The write, then the deletes of the lost conflicts it settles.
    Write(Vec<(KeySpace, Key)>),
    Delete,
}

/// Creates, replaces, rotates or deletes one realm secret.
#[derive(Debug, PartialEq)]
pub struct RealmSecretOperation {
    input: RealmChange,
    stage: Stage,
    notices: bool,
    result: Option<Option<SecretRecord>>,
}

impl RealmSecretOperation {
    pub fn new(input: RealmChange) -> Self {
        Self {
            input,
            stage: Stage::Read,
            notices: false,
            result: None,
        }
    }

    fn document(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.input.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.input.actor.realm_id,
        }
    }

    /// The vault write first, then the record, then retirements, so a receiver sees the value
    /// before the record that points at it.
    fn operations(
        &mut self,
        state: &AdminDocumentState,
    ) -> Result<(Vec<AdminDocumentOperation>, Applied, Vec<UseRule>), RealmSecretError> {
        let name = self.input.change.name().clone();
        let current =
            (state.materialized_realm_secrets().into_iter()).find(|info| info.name == name);
        let old = current
            .as_ref()
            .map(|info| info.uses.clone())
            .unwrap_or_default();
        let applied = apply_change(
            SecretScope::Realm,
            current.as_ref(),
            self.input.change.clone(),
            self.input.actor.user_id,
            self.input.now_ms,
            self.input.write_id,
        )?;
        let mut operations = Vec::new();
        if let (Some(info), Some(fields)) = (&applied.info, &applied.value) {
            let reference = RealmSecretRef::named(info.value_id);
            let realm_id = self.input.actor.realm_id;
            let sealed = (self.input.vault_key).seal(
                &realm_id,
                reference.entry,
                reference.revision,
                &fields.encode()?,
            )?;
            operations.push(AdminDocumentOperation::ConfigVaultSet {
                entry: reference.entry,
                record: Some(RealmVaultRecord {
                    entry: reference.entry,
                    revision: reference.revision,
                    sealed,
                    changed_by: self.input.actor.user_id,
                    changed_at_ms: self.input.now_ms,
                }),
            });
        }
        operations.push(AdminDocumentOperation::ConfigSecretSet {
            name,
            info: applied.info.clone(),
        });
        Ok((operations, applied, old))
    }

    fn write(
        &mut self,
        document: Option<Value>,
        reducer: Option<Value>,
        auth: Option<Value>,
    ) -> Result<Next<Option<SecretRecord>>, RealmSecretError> {
        let mut config =
            RealmConfigDocument::from_bytes(&document.ok_or(RealmSecretError::ConfigMissing)?)?;
        if !is_management(&config, self.input.actor.node_id) {
            return Err(RealmSecretError::NotManagementNode);
        }
        let target = self.admin_target();
        let previous = (reducer.as_ref())
            .map(|value| decode_reducer_state(value.as_ref()).map_err(ConversionError::from))
            .transpose()?;
        if previous
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }
        let name = self.input.change.name().clone();
        let superseded: BTreeSet<_> = (previous.iter())
            .flat_map(|state| state.realm_secret_candidates(&name))
            .map(|info| RealmSecretRef::named(info.value_id).entry)
            .collect();
        let mut state = previous
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let (operations, applied, old) = self.operations(&state)?;
        let actor = self.input.actor.clone();
        let mut events = Vec::new();
        for operation in operations {
            events.push(state.apply_operation(&actor, operation)?);
        }
        // Only values this write supersedes retire: the name's old value and those of its lost
        // conflicts. A value whose record has not arrived yet stays hidden, never retired.
        let referenced: Vec<_> = (state.materialized_realm_secrets().iter())
            .map(|info| RealmSecretRef::named(info.value_id).entry)
            .collect();
        for entry in superseded {
            if !referenced.contains(&entry) {
                let retire = AdminDocumentOperation::ConfigVaultSet {
                    entry,
                    record: None,
                };
                events.push(state.apply_operation(&actor, retire)?);
            }
        }
        config.vault = state.materialized_realm_vault();
        config.secrets = state.materialized_realm_secrets();
        let deletes = stale_conflict_deletes(previous.as_ref(), Some(&state));
        let document = self.document();
        let placement = target_placement_ref(&config, &document, Default::default());
        let mut writes = vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
                config.to_bytes(&actor)?.into(),
            ),
            reducer_state_entry(&state)?,
        ];
        for event in events {
            let record = new_identified_record(
                event.event_id,
                actor.node_id,
                document.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(event),
                placement,
                false,
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&state)?);
        writes.extend(applied.audit.entries()?);
        if let (Some(info), Some(auth)) = (&applied.info, auth.as_deref()) {
            let notices = widening_notices(
                actor.realm_id,
                &info.name,
                (&old, &info.uses),
                Managers::Realm(auth),
                None,
                actor.user_id,
                self.input.now_ms,
            )?;
            self.notices = !notices.is_empty();
            writes.extend(notices);
        }
        self.result = Some(applied.info);
        self.stage = Stage::Write(deletes);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for RealmSecretOperation {
    type Output = Option<SecretRecord>;
    type Error = RealmSecretError;

    fn first(&mut self) -> Result<Next<Self::Output>, RealmSecretError> {
        let document = self.document();
        Ok(Next::Call(IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&self.admin_target()),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                self.input.actor.realm_id.as_bytes().to_vec().into(),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, RealmSecretError> {
        match std::mem::replace(&mut self.stage, Stage::Delete) {
            Stage::Read => {
                let [document, reducer, auth] = rows::values::<3>(result)?;
                self.write(document, reducer, auth)
            }
            Stage::Write(deletes) => {
                rows::done(result)?;
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            Stage::Delete => {
                rows::done(result)?;
                Ok(Next::Then {
                    output: self.result.take().flatten(),
                    calls: drains(self.notices),
                })
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "realm_tests.rs"]
mod tests;
