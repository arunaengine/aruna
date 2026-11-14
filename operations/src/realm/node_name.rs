//! Sets, clears and reads the realm-administered display names and map places of realm nodes.
//! Both are for people only: placement, routing and policy never read them.
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
    MAX_NODE_NAME, NodeCoordinates, NodePlace, RealmConfigDocument, RealmId, normalize_node_name,
};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeMap;
use thiserror::Error;
use tracing::warn;

use super::COMMIT_ATTEMPTS;
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

/// The fields one request changes: `None` keeps a field, `Some(None)` clears it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodeChange {
    pub display_name: Option<Option<String>>,
    pub place: Option<Option<String>>,
    pub coordinates: Option<Option<NodeCoordinates>>,
}

/// What a node shows people: its display name and map place.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeDisplay {
    pub name: Option<String>,
    pub place: Option<NodePlace>,
}

/// The display names and map places of realm nodes, by node id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeDisplays {
    pub names: BTreeMap<NodeId, String>,
    pub places: BTreeMap<NodeId, NodePlace>,
}

impl NodeDisplays {
    pub fn of(&self, node_id: &NodeId) -> NodeDisplay {
        NodeDisplay {
            name: self.names.get(node_id).cloned(),
            place: self.places.get(node_id).cloned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeNameConfig {
    pub actor: Actor,
    /// The caller's own token context; it is never derived from `actor`.
    pub auth_context: AuthContext,
    pub node_id: NodeId,
    pub change: NodeChange,
    pub scope: NameScope,
}

#[derive(Debug, PartialEq)]
pub struct NodeNameOperation {
    config: NodeNameConfig,
    txn_id: Option<TxnId>,
    attempts: usize,
    state: NodeNameState,
    output: Option<Result<NodeDisplay, NodeNameError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum NodeNameState {
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
pub enum NodeNameError {
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
        "display name and place must be non-empty, at most {MAX_NODE_NAME} characters and free of control characters"
    )]
    InvalidName,
    #[error("coordinates must lie within latitude -90 to 90 and longitude -180 to 180")]
    InvalidCoordinates,
    #[error("the request changes no field")]
    EmptyChange,
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

impl NodeNameOperation {
    pub fn new(config: NodeNameConfig) -> Self {
        Self {
            config,
            txn_id: None,
            attempts: 0,
            state: NodeNameState::Init,
            output: None,
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn begin_transaction(&mut self) -> Effects {
        self.attempts += 1;
        self.state = NodeNameState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn emit_read_current(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = NodeNameState::ReadCurrent;
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
    ) -> Result<Effects, NodeNameError> {
        let Some(txn_id) = self.txn_id else {
            return Err(NodeNameError::MissingTransaction);
        };
        let Some(document_value) = document_value else {
            return Err(NodeNameError::ConfigMissing);
        };
        let document = RealmConfigDocument::from_bytes(&document_value)?;
        // Peers accept realm config events from management nodes only.
        if !is_management(&document, self.config.actor.node_id) {
            return Err(NodeNameError::NotManagementNode);
        }
        let node_id = self.config.node_id;
        let node_key = node_id.to_string();
        let node = document.nodes.iter().find(|node| node.node_id == node_key);
        let allowed = node.is_some_and(|node| match self.config.scope {
            NameScope::Owner => node.kind.owner() == Some(self.config.auth_context.user_id),
            NameScope::RealmAdmin => true,
        });
        if !allowed {
            return Err(NodeNameError::UnknownNode { node_id });
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
        let change = &self.config.change;
        let mut shown = NodeDisplay {
            name: reducer_state.materialized_node_names().remove(&node_id),
            place: reducer_state.materialized_node_places().remove(&node_id),
        };
        let mut operations = Vec::new();
        if let Some(display_name) = &change.display_name {
            shown.name = display_name.clone();
            operations.push(AdminDocumentOperation::NodeNameSet {
                node_id,
                display_name: display_name.clone(),
            });
        }
        if change.place.is_some() || change.coordinates.is_some() {
            // The place travels whole, so a change of one field keeps the other.
            let current = shown.place.take().unwrap_or_default();
            let place = NodePlace {
                name: change.place.clone().unwrap_or(current.name),
                coordinates: change.coordinates.unwrap_or(current.coordinates),
            };
            shown.place = (place.name.is_some() || place.coordinates.is_some()).then_some(place);
            operations.push(AdminDocumentOperation::NodePlaceSet {
                node_id,
                place: shown.place.clone(),
            });
        }

        let document_target = DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        };
        let placement = target_placement_ref(&document, &document_target, Default::default());
        let mut records = Vec::new();
        for operation in operations {
            let admin_event = reducer_state.apply_operation(&self.config.actor, operation)?;
            records.push(new_identified_record(
                admin_event.event_id,
                self.config.actor.node_id,
                document_target.clone(),
                Vec::new(),
                DocumentOutboxEvent::admin(admin_event),
                placement,
                false,
            ));
        }
        let mut writes = vec![reducer_state_entry(&reducer_state)?];
        for record in &records {
            writes.push(outbox_write_entry(record).map_err(ConversionError::from)?);
        }
        writes.extend(conflict_write_entries(&reducer_state)?);

        self.output = Some(Ok(shown));
        self.state = NodeNameState::WriteState {
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
            return self.fail(NodeNameError::MissingTransaction);
        };
        self.state = NodeNameState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: NodeNameError) -> Effects {
        let cleanup = self.abort();
        self.state = NodeNameState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(NodeNameError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for NodeNameOperation {
    type Output = NodeDisplay;
    type Error = NodeNameError;

    fn start(&mut self) -> Effects {
        let auth = &self.config.auth_context;
        // An owner names its device for itself, so a delegated token cannot.
        let foreign = auth.realm_id != self.config.actor.realm_id
            || (self.config.scope == NameScope::Owner && auth.path_restrictions.is_some());
        if foreign {
            return self.fail(NodeNameError::Unauthorized);
        }
        if let Err(error) = normalize_change(&mut self.config.change) {
            return self.fail(error);
        }
        if self.config.scope == NameScope::Owner {
            return self.begin_transaction();
        }
        self.state = NodeNameState::Auth;
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
            NodeNameState::Auth => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) => {
                    match allowed {
                        Ok(true) => self.begin_transaction(),
                        Ok(false) => self.fail(NodeNameError::Unauthorized),
                        Err(AuthorizationError::StorageError(error)) => {
                            self.fail(NodeNameError::StorageError(error))
                        }
                        Err(error) => {
                            warn!(error = %error, "Node name authorization check failed");
                            self.fail(NodeNameError::Unauthorized)
                        }
                    }
                }
                other => self.unexpected_event("authorization result", format!("{other:?}")),
            },
            NodeNameState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.emit_read_current(txn_id)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            NodeNameState::ReadCurrent => match event {
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
            NodeNameState::WriteState {
                stale_conflict_deletes,
            } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(NodeNameError::MissingTransaction);
                    };
                    if stale_conflict_deletes.is_empty() {
                        return self.emit_commit();
                    }
                    self.state = NodeNameState::DeleteConflicts;
                    smallvec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes: stale_conflict_deletes,
                        txn_id: Some(txn_id),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch write result", format!("{other:?}")),
            },
            NodeNameState::DeleteConflicts => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.emit_commit(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch delete result", format!("{other:?}")),
            },
            NodeNameState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = NodeNameState::ScheduleSyncDrain;
                    smallvec![schedule_drain_effect()]
                }
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }) if self.attempts < COMMIT_ATTEMPTS => {
                    self.txn_id = None;
                    self.begin_transaction()
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            NodeNameState::ScheduleSyncDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = NodeNameState::Finish;
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(error = %message, "Failed to schedule admin document operation outbox drain; durable outbox remains retryable");
                    self.state = NodeNameState::Finish;
                    smallvec![]
                }
                other => self.unexpected_event(
                    "document sync outbox drain timer schedule",
                    format!("{other:?}"),
                ),
            },
            NodeNameState::Finish | NodeNameState::Error | NodeNameState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, NodeNameState::Finish | NodeNameState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(NodeNameError::NotFinished);
        }
        self.output.unwrap_or(Err(NodeNameError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Trims the change's texts and checks every field; an empty change is refused.
fn normalize_change(change: &mut NodeChange) -> Result<(), NodeNameError> {
    if *change == NodeChange::default() {
        return Err(NodeNameError::EmptyChange);
    }
    for text in [&mut change.display_name, &mut change.place] {
        if let Some(Some(raw)) = text {
            *raw = normalize_node_name(raw).ok_or(NodeNameError::InvalidName)?;
        }
    }
    match change.coordinates {
        Some(Some(coordinates)) if !coordinates.valid() => Err(NodeNameError::InvalidCoordinates),
        _ => Ok(()),
    }
}

/// Reads the display names and map places of realm nodes from this node's replicated realm
/// config state. A realm without any reads as empty maps.
#[derive(Debug, PartialEq)]
pub struct ReadNamesOperation {
    realm_id: RealmId,
    done: bool,
    output: Option<Result<NodeDisplays, NodeNameError>>,
}

impl ReadNamesOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            done: false,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<NodeDisplays, NodeNameError>) -> Effects {
        self.done = true;
        self.output = Some(output);
        smallvec![]
    }
}

impl Operation for ReadNamesOperation {
    type Output = NodeDisplays;
    type Error = NodeNameError;

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
                let displays = value
                    .map(|bytes| decode_reducer_state(&bytes).map_err(ConversionError::from))
                    .transpose()
                    .map(|state| {
                        let displays = state.map(|state| NodeDisplays {
                            names: state.materialized_node_names(),
                            places: state.materialized_node_places(),
                        });
                        displays.unwrap_or_default()
                    })
                    .map_err(NodeNameError::from);
                self.finish(displays)
            }
            Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
            other => self.finish(Err(NodeNameError::UnexpectedEvent {
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
        self.output.unwrap_or(Err(NodeNameError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "node_name_tests.rs"]
mod tests;
