//! Publishes one database list change through the realm config's admin document path on a
//! management node: a mark or retarget with its owner consent, or a removal. Each credential
//! the change depends on is checked first; nothing else in the realm config can change here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::admin_documents::{AdminDocumentDot, AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::database::{
    DatabaseConsent, DatabaseEntry, DatabaseError, validate_databases,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
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

/// One change of the database list.
#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseChange {
    /// A mark or retarget. With a consent, the entry's register must still be at the version
    /// the consent names; without one, the actor's own owner right was checked.
    Set {
        entry: DatabaseEntry,
        consent: Option<Box<DatabaseConsent>>,
    },
    Remove {
        entry_id: Ulid,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishDatabaseConfig {
    /// The curator for a mark, the remover for a removal: the K1 actor of the event.
    pub actor: Actor,
    /// Credentials and paths that each need WRITE before anything is written.
    pub checks: Vec<(AuthContext, String)>,
    pub change: DatabaseChange,
}

#[derive(Debug, Error, PartialEq)]
pub enum PublishDatabaseError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    AdminDocument(#[from] AdminDocumentError),
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("caller may not change this database entry")]
    Unauthorized,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("database entry {0} does not exist")]
    NotFound(Ulid),
    #[error("database entry {0} changed since the consent was given; ask for a new consent")]
    PredecessorChanged(Ulid),
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

#[derive(Debug, Clone, PartialEq)]
enum State {
    Auth(usize),
    StartTransaction,
    ReadCurrent,
    Write { stale: Vec<(KeySpace, Key)> },
    DeleteConflicts,
    Commit,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct PublishDatabaseOperation {
    config: PublishDatabaseConfig,
    txn_id: Option<TxnId>,
    state: State,
    output: Option<Result<RealmConfigDocument, PublishDatabaseError>>,
}

impl PublishDatabaseOperation {
    pub fn new(config: PublishDatabaseConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: State::Auth(0),
            output: None,
        }
    }

    fn document_target(&self) -> DocumentTarget {
        DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn check(&mut self, index: usize) -> Effects {
        let Some((auth_context, path)) = self.config.checks.get(index).cloned() else {
            self.state = State::StartTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
        };
        if auth_context.realm_id != self.config.actor.realm_id {
            return self.fail(PublishDatabaseError::Unauthorized);
        }
        self.state = State::Auth(index);
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context,
                path,
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = State::ReadCurrent;
        let document = self.document_target();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key(),
                ),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&self.admin_target()),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn operation(
        &self,
        state: &AdminDocumentState,
        document: &RealmConfigDocument,
    ) -> Result<AdminDocumentOperation, PublishDatabaseError> {
        match &self.config.change {
            DatabaseChange::Set { entry, consent } => {
                entry.validate()?;
                let current: Option<AdminDocumentDot> = state.database_version(entry.entry_id);
                if let Some(consent) = consent {
                    consent
                        .verify(self.config.actor.realm_id, entry, current.as_ref())
                        .map_err(|error| match error {
                            DatabaseError::ConsentMismatch => {
                                PublishDatabaseError::PredecessorChanged(entry.entry_id)
                            }
                            other => other.into(),
                        })?;
                }
                Ok(AdminDocumentOperation::DatabaseSet {
                    entry: entry.clone(),
                    predecessor: current,
                    consent: consent.clone(),
                })
            }
            DatabaseChange::Remove { entry_id } => {
                if !document
                    .databases
                    .iter()
                    .any(|entry| entry.entry_id == *entry_id)
                {
                    return Err(PublishDatabaseError::NotFound(*entry_id));
                }
                Ok(AdminDocumentOperation::DatabaseRemoved {
                    entry_id: *entry_id,
                })
            }
        }
    }

    fn write(
        &mut self,
        document_value: Option<Value>,
        state_value: Option<Value>,
    ) -> Result<Effects, PublishDatabaseError> {
        let txn_id = self
            .txn_id
            .ok_or(PublishDatabaseError::MissingTransaction)?;
        let mut document = RealmConfigDocument::from_bytes(
            &document_value.ok_or(PublishDatabaseError::ConfigMissing)?,
        )?;
        if !is_management(&document, self.config.actor.node_id) {
            return Err(PublishDatabaseError::NotManagementNode);
        }
        let target = self.admin_target();
        let previous = state_value
            .as_ref()
            .map(|value| {
                aruna_core::reducer::decode_reducer_state(value.as_ref())
                    .map_err(ConversionError::from)
            })
            .transpose()?;
        if previous
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }
        let mut state = previous
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let operation = self.operation(&state, &document)?;
        let admin_event = state.apply_operation(&self.config.actor, operation)?;
        validate_databases(&state.database_entries())?;
        document.databases = state.materialized_databases();

        let stale = stale_conflict_deletes(previous.as_ref(), Some(&state));
        let document_target = self.document_target();
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let mut writes = vec![
            (
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                document.to_bytes(&self.config.actor)?.into(),
            ),
            reducer_state_entry(&state)?,
        ];
        let record = new_identified_record(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentOutboxEvent::admin(admin_event),
            placement,
            false,
        );
        writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        writes.extend(conflict_write_entries(&state)?);
        self.output = Some(Ok(document));
        self.state = State::Write { stale };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(PublishDatabaseError::MissingTransaction);
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: PublishDatabaseError) -> Effects {
        let cleanup = self.abort();
        self.state = State::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, got: &Event) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(PublishDatabaseError::UnexpectedEvent {
            state,
            expected,
            got: format!("{got:?}"),
        })
    }
}

impl Operation for PublishDatabaseOperation {
    type Output = RealmConfigDocument;
    type Error = PublishDatabaseError;

    fn start(&mut self) -> Effects {
        self.check(0)
    }

    fn step(&mut self, event: Event) -> Effects {
        if let (Event::Storage(StorageEvent::Error { error }), false) = (
            &event,
            matches!(self.state, State::Auth(_) | State::ScheduleDrain),
        ) {
            if matches!(self.state, State::Commit) {
                self.txn_id = None;
            }
            return self.fail(error.clone().into());
        }
        match self.state.clone() {
            State::Auth(index) => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => self.check(index + 1),
                        Ok(false) => self.fail(PublishDatabaseError::Unauthorized),
                        Err(AuthorizationError::StorageError(error)) => self.fail(error.into()),
                        Err(error) => {
                            warn!(error = %error, "Database change authorization check failed");
                            self.fail(PublishDatabaseError::Unauthorized)
                        }
                    }
                }
                other => self.unexpected("authorization result", &other),
            },
            State::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.read_current(txn_id)
                }
                other => self.unexpected("transaction start", &other),
            },
            State::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document), (_, state)] = values.as_slice() else {
                        return self.unexpected(
                            "realm config and reducer state",
                            &Event::Storage(StorageEvent::BatchReadResult { values }),
                        );
                    };
                    match self.write(document.clone(), state.clone()) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                other => self.unexpected("batch read result", &other),
            },
            State::Write { stale } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) if !stale.is_empty() => {
                    self.state = State::DeleteConflicts;
                    smallvec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: stale,
                        txn_id: self.txn_id,
                    })]
                }
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.commit(),
                other => self.unexpected("batch write result", &other),
            },
            State::DeleteConflicts => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.commit(),
                other => self.unexpected("batch delete result", &other),
            },
            State::Commit => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = State::ScheduleDrain;
                    smallvec![schedule_drain_effect()]
                }
                other => self.unexpected("transaction commit", &other),
            },
            State::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = State::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Outbox drain not scheduled; the durable outbox stays retryable");
                    self.state = State::Finish;
                    smallvec![]
                }
                other => self.unexpected("outbox drain timer", &other),
            },
            State::Finish | State::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finish | State::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(PublishDatabaseError::NotFinished);
        }
        self.output
            .unwrap_or(Err(PublishDatabaseError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "database_publish_tests.rs"]
mod tests;
