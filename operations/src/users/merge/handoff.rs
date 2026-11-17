//! Hands the merge coordinator role to another management node. Only the current coordinator
//! does, so it stops merging in the same commit; a realm without one may name one anywhere.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::stage::{Batch, Change};
use super::{Coordinator, MergeError, coordinator};
use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{AuthorizationError, ConversionError};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::rows::done;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Value;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm::ensure_config::overlay_reducer_state;
use crate::sync::outbox_record::schedule_drain_effect;
use aruna_core::placement::target_placement_ref;

#[derive(Clone, Debug, PartialEq)]
pub struct HandoffConfig {
    /// This node, acting for the realm administrator.
    pub actor: Actor,
    pub auth_context: AuthContext,
    /// The management node that runs merges from now on.
    pub node_id: NodeId,
}

/// Checks the administrator's config permission, then hands the role on in one write.
#[derive(Debug, PartialEq)]
pub struct HandoffOperation {
    config: HandoffConfig,
    authorized: bool,
}

impl HandoffOperation {
    pub fn new(config: HandoffConfig) -> Self {
        Self {
            config,
            authorized: false,
        }
    }
}

impl Operation for HandoffOperation {
    type Output = ();
    type Error = MergeError;

    fn first(&mut self) -> Result<Next<()>, MergeError> {
        let path = format!("/{}/admin/config", self.config.actor.realm_id);
        Ok(Next::Call(IoCall::child(CheckPermissionsOperation::new(
            CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path,
                required_permission: Permission::WRITE,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MergeError> {
        if self.authorized {
            return result.child::<(), MergeError>()?.map(Next::Done);
        }
        if !result.child::<bool, AuthorizationError>()?? {
            return Err(MergeError::Unauthorized);
        }
        self.authorized = true;
        let write = HandoffWrite::new(self.config.clone());
        Ok(Next::Call(IoCall::child(write)))
    }
}

/// Where the handoff write stands.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Read,
    Write,
    DeleteStale,
}

/// The handoff transaction: reads the realm config and its admin state, then records the new
/// coordinator and queues the change.
#[derive(Debug, PartialEq)]
pub(super) struct HandoffWrite {
    config: HandoffConfig,
    stage: Stage,
    batch: Batch,
}

impl HandoffWrite {
    pub(super) fn new(config: HandoffConfig) -> Self {
        Self {
            config,
            stage: Stage::Read,
            batch: Batch::default(),
        }
    }

    fn write(&mut self, values: &[Option<Value>]) -> Result<Next<()>, MergeError> {
        let [config, state] = values else {
            return Err(MergeError::NotFinished);
        };
        let mut config = config
            .as_ref()
            .map(|bytes| RealmConfigDocument::from_bytes(bytes))
            .transpose()?
            .ok_or(MergeError::NotCoordinator)?;
        let decoded = state
            .as_ref()
            .map(|bytes| decode_reducer_state(bytes).map_err(ConversionError::from))
            .transpose()?;
        let node_id = self.config.actor.node_id;
        let management = |node: &NodeId| {
            let name = node.to_string();
            (config.nodes.iter())
                .any(|entry| entry.node_id == name && entry.kind == RealmNodeKind::Management)
        };
        if !management(&self.config.node_id) {
            return Err(MergeError::InvalidCoordinator);
        }
        // The coordinator hands the role on; an administrator names one only while none is
        // recorded or the recorded one has left management, as this node sees it.
        let allowed = match coordinator(&config, decoded.as_ref()) {
            Coordinator::Pinned(current) => current == node_id,
            Coordinator::Unpinned | Coordinator::Left => true,
        };
        if !management(&node_id) || !allowed {
            return Err(MergeError::NotCoordinator);
        }
        let realm_id = self.config.actor.realm_id;
        let mut change = Change::new(
            state.as_ref(),
            AdminDocumentTarget::RealmConfig { realm_id },
        )?;
        change.apply(
            &self.config.actor,
            AdminDocumentOperation::MergeCoordinatorSet {
                node_id: self.config.node_id,
            },
        )?;
        overlay_reducer_state(&mut config, &change.state, unix_timestamp_millis());
        let document = DocumentTarget::RealmConfig { realm_id };
        let placement = target_placement_ref(&config, &document, Default::default());
        self.batch.writes.push((
            document.storage_keyspace().to_string(),
            document.storage_key(),
            config.to_bytes(&self.config.actor)?.into(),
        ));
        self.batch
            .stage(node_id, realm_id, (document, placement), &change)?;
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(std::mem::take(
            &mut self.batch.writes,
        ))))
    }

    /// Wakes the outbox drain after the commit; a failed timer leaves the outbox retryable.
    fn committed() -> Next<()> {
        let calls = vec![IoCall::Effect(Box::new(schedule_drain_effect()))];
        Next::Then { output: (), calls }
    }
}

impl Operation for HandoffWrite {
    type Output = ();
    type Error = MergeError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, MergeError> {
        let realm_id = self.config.actor.realm_id;
        let document = DocumentTarget::RealmConfig { realm_id };
        let target = AdminDocumentTarget::RealmConfig { realm_id };
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

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MergeError> {
        match self.stage {
            Stage::Read => {
                let IoResult::Values(values) = result else {
                    return Err(MergeError::NotFinished);
                };
                self.write(&values)
            }
            Stage::Write if !self.batch.deletes.is_empty() => {
                done(result)?;
                self.stage = Stage::DeleteStale;
                let deletes = std::mem::take(&mut self.batch.deletes);
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            Stage::Write | Stage::DeleteStale => {
                done(result)?;
                Ok(Self::committed())
            }
        }
    }
}
