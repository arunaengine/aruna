//! Sets, clears and reads the realm-administered display names of realm nodes.
//! A name is a label for people: placement, routing and policy never read it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{
    MAX_NODE_NAME, RealmConfigDocument, RealmId, normalize_node_name,
};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeMap;
use thiserror::Error;
use tracing::warn;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::realm::mutate_placement::is_management;
use crate::sync::document_outbox::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};

/// Whose authority names the node. An owner reaches only its own devices; a
/// realm admin reaches every realm node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameScope {
    Owner,
    RealmAdmin,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetNodeNameConfig {
    pub actor: Actor,
    /// The caller's own token context; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub node_id: NodeId,
    /// `None` clears the name.
    pub display_name: Option<String>,
    pub scope: NameScope,
}

#[derive(Debug, PartialEq)]
pub struct SetNodeNameOperation {
    config: SetNodeNameConfig,
    txn_id: Option<TxnId>,
    state: SetNodeNameState,
    output: Option<Result<Option<String>, SetNodeNameError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SetNodeNameState {
    Init,
    Auth,
    StartTransaction,
    ReadCurrent,
    WriteState {
        stale_conflict_deletes: Vec<(KeySpace, Key)>,
    },
    DeleteConflicts,
    CommitTransaction,
    ScheduleSyncDrain,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetNodeNameError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentError(#[from] AdminDocumentError),
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("caller may not name this node")]
    Unauthorized,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error(
        "display name must be non-empty, at most {MAX_NODE_NAME} characters and free of control characters"
    )]
    InvalidName,
    #[error("node {node_id} is not a node the caller may name")]
    UnknownNode { node_id: NodeId },
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

impl SetNodeNameOperation {
    pub fn new(config: SetNodeNameConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: SetNodeNameState::Init,
            output: None,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn begin_transaction(&mut self) -> Effects {
        self.state = SetNodeNameState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = SetNodeNameState::ReadCurrent;
        let document = DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        };
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

    fn emit_name_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> Result<Effects, SetNodeNameError> {
        let Some(txn_id) = self.txn_id else {
            return Err(SetNodeNameError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(SetNodeNameError::ConfigMissing);
        };
        let document = RealmConfigDocument::from_bytes(&document_value)?;
        // Peers accept realm config events from management nodes only.
        if !is_management(&document, self.config.actor.node_id) {
            return Err(SetNodeNameError::NotManagementNode);
        }
        let node_id = self.config.node_id;
        let node_key = node_id.to_string();
        let node = document.nodes.iter().find(|node| node.node_id == node_key);
        let allowed = node.is_some_and(|node| match self.config.scope {
            NameScope::Owner => node.kind.owner() == Some(self.config.auth_context.user_id),
            NameScope::RealmAdmin => true,
        });
        if !allowed {
            return Err(SetNodeNameError::UnknownNode { node_id });
        }

        let target = self.admin_target();
        let previous_state = reducer_state_value
            .as_ref()
            .map(|value| decode_reducer_state(value.as_ref()).map_err(ConversionError::from))
            .transpose()?;
        if previous_state
            .as_ref()
            .is_some_and(|state| state.target != target)
        {
            return Err(AdminDocumentError::TargetMismatch.into());
        }
        let mut reducer_state = previous_state
            .clone()
            .unwrap_or_else(|| AdminDocumentState::new(target));
        let admin_event = reducer_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::NodeNameSet {
                node_id,
                display_name: self.config.display_name.clone(),
            },
        )?;

        let document_target = DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        };
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let record = new_identified_record(
            admin_event.event_id,
            self.config.actor.node_id,
            document_target,
            Vec::new(),
            DocumentOutboxEvent::admin(admin_event),
            placement,
            false,
        );
        let mut writes = vec![
            reducer_state_entry(&reducer_state)?,
            outbox_write_entry(&record).map_err(ConversionError::from)?,
        ];
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(self.config.display_name.clone()));
        self.state = SetNodeNameState::WriteState {
            stale_conflict_deletes: stale_conflict_deletes(
                previous_state.as_ref(),
                Some(&reducer_state),
            ),
        };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn emit_commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetNodeNameError::MissingTransaction);
        };
        self.state = SetNodeNameState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: SetNodeNameError) -> Effects {
        let cleanup = self.abort();
        self.state = SetNodeNameState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(SetNodeNameError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for SetNodeNameOperation {
    type Output = Option<String>;
    type Error = SetNodeNameError;

    fn start(&mut self) -> Effects {
        let auth = &self.config.auth_context;
        // An owner names its device for itself, so a delegated token cannot.
        let foreign = auth.realm_id != self.config.actor.realm_id
            || (self.config.scope == NameScope::Owner && auth.path_restrictions.is_some());
        if foreign {
            return self.fail(SetNodeNameError::Unauthorized);
        }
        if let Some(raw) = self.config.display_name.take() {
            match normalize_node_name(&raw) {
                Some(name) => self.config.display_name = Some(name),
                None => return self.fail(SetNodeNameError::InvalidName),
            }
        }
        if self.config.scope == NameScope::Owner {
            return self.begin_transaction();
        }
        self.state = SetNodeNameState::Auth;
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
            SetNodeNameState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => self.begin_transaction(),
                        Ok(false) => self.fail(SetNodeNameError::Unauthorized),
                        Err(AuthorizationError::StorageError(error)) => {
                            self.fail(SetNodeNameError::StorageError(error))
                        }
                        Err(error) => {
                            warn!(error = %error, "Node name authorization check failed");
                            self.fail(SetNodeNameError::Unauthorized)
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            SetNodeNameState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            SetNodeNameState::ReadCurrent => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, document_value), (_, reducer_state_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with realm config and reducer state",
                            format!("{values:?}"),
                        );
                    };
                    match self.emit_name_write(document_value.clone(), reducer_state_value.clone())
                    {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
            },
            SetNodeNameState::WriteState {
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(SetNodeNameError::MissingTransaction);
                    };
                    if stale_conflict_deletes.is_empty() {
                        return self.emit_commit();
                    }
                    self.state = SetNodeNameState::DeleteConflicts;
                    smallvec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: stale_conflict_deletes,
                        txn_id: Some(txn_id),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            SetNodeNameState::DeleteConflicts => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.emit_commit(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            SetNodeNameState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = SetNodeNameState::ScheduleSyncDrain;
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            SetNodeNameState::ScheduleSyncDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = SetNodeNameState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = SetNodeNameState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            SetNodeNameState::Finish | SetNodeNameState::Error | SetNodeNameState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SetNodeNameState::Finish | SetNodeNameState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(SetNodeNameError::NotFinished);
        }
        self.output.unwrap_or(Err(SetNodeNameError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Reads the display names of realm nodes from this node's replicated realm config state.
/// A realm without any name reads as an empty map.
#[derive(Debug, PartialEq)]
pub struct ReadNamesOperation {
    realm_id: RealmId,
    done: bool,
    output: Option<Result<BTreeMap<NodeId, String>, SetNodeNameError>>,
}

impl ReadNamesOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            done: false,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<BTreeMap<NodeId, String>, SetNodeNameError>) -> Effects {
        self.done = true;
        self.output = Some(output);
        smallvec![]
    }
}

impl Operation for ReadNamesOperation {
    type Output = BTreeMap<NodeId, String>;
    type Error = SetNodeNameError;

    fn start(&mut self) -> Effects {
        let target = AdminDocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: DOCUMENT_STATE_KEYSPACE.to_string(),
            key: reducer_state_key(&target),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.done {
            return smallvec![];
        }
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                let names = value
                    .map(|bytes| decode_reducer_state(&bytes).map_err(ConversionError::from))
                    .transpose()
                    .map(|state| {
                        state
                            .map(|state| state.materialized_node_names())
                            .unwrap_or_default()
                    })
                    .map_err(SetNodeNameError::from);
                self.finish(names)
            }
            Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
            other => self.finish(Err(SetNodeNameError::UnexpectedEvent {
                state: "ReadState".to_string(),
                expected: "storage read result",
                got: format!("{other:?}"),
            })),
        }
    }

    fn is_complete(&self) -> bool {
        self.done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SetNodeNameError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "node_name_tests.rs"]
mod tests;
