//! Sets, clears and reads the realm-administered display names and map places of realm nodes.
//! Both are for people only: placement, routing and policy never read them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode, boxed_retry};
use aruna_core::reducer::{AdminDocumentError, AdminDocumentState, decode_reducer_state};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{
    conflict_write_entries, reducer_state_entry, reducer_state_key, stale_conflict_deletes,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{
    MAX_NODE_NAME, NodeCoordinates, NodePlace, RealmConfigDocument, RealmId, normalize_node_name,
};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::types::{Key, KeySpace, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;

use super::COMMIT_ATTEMPTS;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm::mutate_placement::is_management;
use crate::sync::outbox_record::{
    new_identified_record, outbox_write_entry, schedule_drain_effect,
};
use aruna_core::placement::target_placement_ref;

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

/// Checks who may name the node, then writes the change in a write transaction that reruns
/// after a lost conflict, since a background config write can win its commit.
#[derive(Debug, PartialEq)]
pub struct NodeNameOperation {
    config: NodeNameConfig,
    authorized: bool,
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
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for NodeNameError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "NodeName".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type NameTurn = Result<Next<NodeDisplay>, NodeNameError>;

impl NodeNameOperation {
    pub fn new(config: NodeNameConfig) -> Self {
        Self {
            config,
            authorized: false,
        }
    }

    /// The write transaction of the checked change.
    #[cfg(test)]
    fn write(&self) -> NameWrite {
        NameWrite::new(Arc::new(self.config.clone()))
    }

    fn change(&mut self) -> Next<NodeDisplay> {
        self.authorized = true;
        let config = Arc::new(self.config.clone());
        let seed = self.config.actor.user_id.to_bytes();
        let build = move || NameWrite::new(Arc::clone(&config));
        let child = boxed_retry(build, &seed, |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Next::Call(IoCall::Child(child))
    }
}

impl Operation for NodeNameOperation {
    type Output = NodeDisplay;
    type Error = NodeNameError;

    fn first(&mut self) -> NameTurn {
        let auth = &self.config.auth_context;
        // An owner names its device for itself, so a delegated token cannot.
        let foreign = auth.realm_id != self.config.actor.realm_id
            || (self.config.scope == NameScope::Owner && auth.path_restrictions.is_some());
        if foreign {
            return Err(NodeNameError::Unauthorized);
        }
        normalize_change(&mut self.config.change)?;
        if self.config.scope == NameScope::Owner {
            return Ok(self.change());
        }
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path: policy_admin_path(self.config.actor.realm_id),
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> NameTurn {
        if self.authorized {
            return result
                .child::<NodeDisplay, NodeNameError>()?
                .map(Next::Done);
        }
        match result.child::<bool, AuthorizationError>()? {
            Ok(true) => Ok(self.change()),
            Ok(false) => Err(NodeNameError::Unauthorized),
            Err(AuthorizationError::StorageError(error)) => Err(error.into()),
            Err(error) => {
                warn!(error = %error, "Node name authorization check failed");
                Err(NodeNameError::Unauthorized)
            }
        }
    }
}

/// What the name write transaction waits for.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Read,
    Write,
    Delete,
}

/// Reads the realm config and its admin state, then stores the changed names and places and
/// their queued admin events in one write transaction.
#[derive(Debug, PartialEq)]
struct NameWrite {
    config: Arc<NodeNameConfig>,
    stage: Stage,
    shown: Option<NodeDisplay>,
    deletes: Vec<(KeySpace, Key)>,
}

impl NameWrite {
    fn new(config: Arc<NodeNameConfig>) -> Self {
        Self {
            config,
            stage: Stage::Read,
            shown: None,
            deletes: Vec::new(),
        }
    }

    fn admin_target(&self) -> AdminDocumentTarget {
        AdminDocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        }
    }

    fn emit_name_write(
        &mut self,
        document_value: Option<Value>,
        reducer_state_value: Option<Value>,
    ) -> NameTurn {
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

        self.shown = Some(shown);
        self.deletes = stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    /// A failed drain timer leaves the durable outbox retryable.
    fn finish(&mut self) -> NameTurn {
        let output = self.shown.take().ok_or(NodeNameError::NotFinished)?;
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for NameWrite {
    type Output = NodeDisplay;
    type Error = NodeNameError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        COMMIT_ATTEMPTS as u32 - 1
    }

    fn first(&mut self) -> NameTurn {
        let document = DocumentTarget::RealmConfig {
            realm_id: self.config.actor.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![
            (
                document.storage_keyspace().to_string(),
                document.storage_key(),
            ),
            (
                DOCUMENT_STATE_KEYSPACE.to_string(),
                reducer_state_key(&self.admin_target()),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> NameTurn {
        match self.stage {
            Stage::Read => {
                let [document, reducer] = values::<2>(result)?;
                self.emit_name_write(document, reducer)
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
}

impl ReadNamesOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self { realm_id }
    }
}

impl Operation for ReadNamesOperation {
    type Output = NodeDisplays;
    type Error = NodeNameError;

    fn first(&mut self) -> Result<Next<NodeDisplays>, NodeNameError> {
        let target = AdminDocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![(
            DOCUMENT_STATE_KEYSPACE.to_string(),
            reducer_state_key(&target),
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeDisplays>, NodeNameError> {
        let [value] = values::<1>(result)?;
        let state = value
            .map(|bytes| decode_reducer_state(&bytes).map_err(ConversionError::from))
            .transpose()?;
        let displays = state.map(|state| NodeDisplays {
            names: state.materialized_node_names(),
            places: state.materialized_node_places(),
        });
        Ok(Next::Done(displays.unwrap_or_default()))
    }
}

#[cfg(test)]
#[path = "node_name_tests.rs"]
mod tests;
