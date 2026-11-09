//! Stores named realm secrets in the realm config: the record by name and the value in the realm
//! vault, written by a management node in one admin document with the audit row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::realm_vault::{RealmSecretRef, RealmVaultError, RealmVaultKey, RealmVaultRecord};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::secrets::{SecretRecord, SecretScope};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::{Applied, ChangeError, SecretChange, apply_change};
use crate::placement::target_placement_ref;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

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
    #[error("unexpected event in state {state}: {got}")]
    UnexpectedEvent { state: &'static str, got: String },
    #[error("operation did not finish")]
    NotFinished,
}

impl From<aruna_core::secrets::SecretError> for RealmSecretError {
    fn from(error: aruna_core::secrets::SecretError) -> Self {
        Self::Change(ChangeError::Invalid(error))
    }
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    StartTransaction,
    ReadCurrent,
    Write { deletes: Vec<(KeySpace, Key)> },
    DeleteConflicts,
    Commit,
    ScheduleDrain,
    Done,
}

/// Creates, replaces, rotates or deletes one realm secret.
#[derive(Debug, PartialEq)]
pub struct RealmSecretOperation {
    input: RealmChange,
    state: State,
    txn_id: Option<TxnId>,
    result: Option<Option<SecretRecord>>,
    output: Option<Result<Option<SecretRecord>, RealmSecretError>>,
}

impl RealmSecretOperation {
    pub fn new(input: RealmChange) -> Self {
        Self {
            input,
            state: State::Init,
            txn_id: None,
            result: None,
            output: None,
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

    fn fail(&mut self, error: RealmSecretError) -> Effects {
        self.output = Some(Err(error));
        self.state = State::Done;
        self.abort()
    }

    fn unexpected(&mut self, state: &'static str, event: Event) -> Effects {
        let got = format!("{event:?}");
        self.fail(RealmSecretError::UnexpectedEvent { state, got })
    }

    /// The vault write first, then the record, then retirements, so a receiver sees the value
    /// before the record that points at it.
    fn operations(
        &mut self,
        state: &AdminDocumentState,
    ) -> Result<(Vec<AdminDocumentOperation>, Applied), RealmSecretError> {
        let name = self.input.change.name().clone();
        let current =
            (state.materialized_realm_secrets().into_iter()).find(|info| info.name == name);
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
        Ok((operations, applied))
    }

    fn write(
        &mut self,
        document: Option<Value>,
        reducer: Option<Value>,
    ) -> Result<Effects, RealmSecretError> {
        let txn_id = self.txn_id.ok_or(RealmSecretError::NotFinished)?;
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
        let (operations, applied) = self.operations(&state)?;
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
        self.result = Some(applied.info);
        self.state = State::Write { deletes };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(RealmSecretError::NotFinished);
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for RealmSecretOperation {
    type Output = Option<SecretRecord>;
    type Error = RealmSecretError;

    fn start(&mut self) -> Effects {
        self.state = State::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            if self.state == State::Commit {
                self.txn_id = None;
            }
            return self.fail(error.into());
        }
        match (&self.state, event) {
            (
                State::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = State::ReadCurrent;
                let document = self.document();
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            document.storage_keyspace().to_string(),
                            document.storage_key()
                        ),
                        (
                            DOCUMENT_STATE_KEYSPACE.to_string(),
                            reducer_state_key(&self.admin_target()),
                        ),
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            (State::ReadCurrent, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let [(_, document), (_, reducer)] = values.as_slice() else {
                    let got = format!("{} values", values.len());
                    return self.fail(RealmSecretError::UnexpectedEvent {
                        state: "ReadCurrent",
                        got,
                    });
                };
                match self.write(document.clone(), reducer.clone()) {
                    Ok(effects) => effects,
                    Err(error) => self.fail(error),
                }
            }
            (State::Write { deletes }, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                if deletes.is_empty() {
                    return self.commit();
                }
                let deletes = deletes.clone();
                self.state = State::DeleteConflicts;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes,
                    txn_id: self.txn_id,
                })]
            }
            (State::DeleteConflicts, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.commit()
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = State::ScheduleDrain;
                smallvec![schedule_drain_effect()]
            }
            (State::ScheduleDrain, Event::Task(TaskEvent::TimerScheduled { .. })) => {
                self.state = State::Done;
                self.output = Some(Ok(self.result.take().flatten()));
                smallvec![]
            }
            (State::ScheduleDrain, Event::Task(TaskEvent::Error { message, .. })) => {
                warn!(error = %message, "Outbox drain not scheduled; the durable outbox retries");
                self.state = State::Done;
                self.output = Some(Ok(self.result.take().flatten()));
                smallvec![]
            }
            (State::Init | State::Done, _) => smallvec![],
            (_, event) => self.unexpected("RealmSecret", event),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RealmSecretError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}
