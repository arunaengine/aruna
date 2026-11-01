//! Hands the merge coordinator role to another management node. Only the current coordinator
//! does, so it stops merging in the same commit; a realm without one may name one anywhere.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::stage::{Batch, Change};
use super::{MergeError, coordinator};
use aruna_core::NodeId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::reducer::decode_reducer_state;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::task::TaskEvent;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::placement::target_placement_ref;
use crate::realm::ensure_config::overlay_reducer_state;
use crate::sync::document_outbox::schedule_drain_effect;

#[derive(Clone, Debug, PartialEq)]
pub struct HandoffConfig {
    /// This node, acting for the realm administrator.
    pub actor: Actor,
    pub auth_context: AuthContext,
    /// The management node that runs merges from now on.
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq)]
enum HandoffState {
    Init,
    Authorize,
    StartTransaction,
    Read { txn_id: TxnId },
    Write { txn_id: TxnId },
    DeleteStale { txn_id: TxnId },
    Commit { txn_id: TxnId },
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct HandoffOperation {
    config: HandoffConfig,
    state: HandoffState,
    batch: Batch,
    output: Option<Result<(), MergeError>>,
}

impl HandoffOperation {
    pub fn new(config: HandoffConfig) -> Self {
        Self {
            config,
            state: HandoffState::Init,
            batch: Batch::default(),
            output: None,
        }
    }

    fn fail(&mut self, error: MergeError) -> Effects {
        let cleanup = self.abort();
        self.state = HandoffState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn read(&mut self, txn_id: TxnId) -> Effects {
        self.state = HandoffState::Read { txn_id };
        let realm_id = self.config.actor.realm_id;
        let document = DocumentTarget::RealmConfig { realm_id };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document.storage_keyspace().to_string(),
                    document.storage_key()
                ),
                (
                    DOCUMENT_STATE_KEYSPACE.to_string(),
                    reducer_state_key(&AdminDocumentTarget::RealmConfig { realm_id }),
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn write(
        &mut self,
        txn_id: TxnId,
        values: &[(ByteView, Option<Value>)],
    ) -> Result<Effects, MergeError> {
        let [(_, config), (_, state)] = values else {
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
        let current = coordinator(&config, decoded.as_ref());
        if !management(&node_id) || current.is_some_and(|current| current != node_id) {
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
        self.state = HandoffState::Write { txn_id };
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: std::mem::take(&mut self.batch.writes),
            txn_id: Some(txn_id),
        })])
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = HandoffState::Commit { txn_id };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn advance(&mut self, event: Event) -> Result<Effects, MergeError> {
        match (self.state.clone(), event) {
            (
                HandoffState::Authorize,
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
            ) => {
                if !allowed? {
                    return Err(MergeError::Unauthorized);
                }
                self.state = HandoffState::StartTransaction;
                Ok(smallvec![Effect::Storage(
                    StorageEffect::StartTransaction { read: false }
                )])
            }
            (
                HandoffState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => Ok(self.read(txn_id)),
            (
                HandoffState::Read { txn_id },
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.write(txn_id, &values),
            (
                HandoffState::Write { txn_id },
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => {
                if self.batch.deletes.is_empty() {
                    return Ok(self.commit(txn_id));
                }
                self.state = HandoffState::DeleteStale { txn_id };
                Ok(smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: std::mem::take(&mut self.batch.deletes),
                    txn_id: Some(txn_id),
                })])
            }
            (
                HandoffState::DeleteStale { txn_id },
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => Ok(self.commit(txn_id)),
            (
                HandoffState::Commit { .. },
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.state = HandoffState::ScheduleDrain;
                Ok(smallvec![schedule_drain_effect()])
            }
            (
                HandoffState::ScheduleDrain,
                Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. }),
            ) => {
                self.state = HandoffState::Finish;
                self.output = Some(Ok(()));
                Ok(smallvec![])
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (state, event) => Err(MergeError::UnexpectedEvent {
                state: format!("{state:?}"),
                expected: "the event of the current handoff step",
                got: format!("{event:?}"),
            }),
        }
    }
}

impl Operation for HandoffOperation {
    type Output = ();
    type Error = MergeError;

    fn start(&mut self) -> Effects {
        self.state = HandoffState::Authorize;
        let path = format!("/{}/admin/config", self.config.actor.realm_id);
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.config.auth_context.clone(),
                path,
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            HandoffState::Init | HandoffState::Finish | HandoffState::Error => smallvec![],
            _ => self.advance(event).unwrap_or_else(|error| self.fail(error)),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, HandoffState::Finish | HandoffState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(MergeError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            HandoffState::Read { txn_id }
            | HandoffState::Write { txn_id }
            | HandoffState::DeleteStale { txn_id }
            | HandoffState::Commit { txn_id } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}
