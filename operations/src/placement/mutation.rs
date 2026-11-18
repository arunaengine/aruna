//! Runs realm placement mutations with conflict re-runs, then kicks the outbox drain when
//! a mutation drains this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::backoff::conflict_backoff;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::StorageError;
use aruna_core::events::Event;
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{event, page};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Key;
use tracing::warn;

use crate::realm::mutate_placement::{
    CONFLICT_ATTEMPTS, MutatePlacementConfig, MutatePlacementError, MutatePlacementOperation,
    RealmPlacementMutation,
};

/// Arms the document outbox drain when records wait, keeping a persisted retry deadline.
#[derive(Debug, PartialEq)]
struct KickDrainOperation {
    scanned: bool,
}

impl Operation for KickDrainOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Page {
            space: SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.scanned {
            if let Event::Task(TaskEvent::Error { message, .. }) = event(result)? {
                warn!(message = %message, "Failed to restore document sync outbox timer");
            }
            return Ok(Next::Done(()));
        }
        if page(result)?.0.is_empty() {
            return Ok(Next::Done(()));
        }
        self.scanned = true;
        let arm = TaskEffect::ArmIdleTimer {
            key: TaskKey::DrainSyncOutbox,
            after: Duration::ZERO,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Task(arm)))))
    }
}

#[derive(Debug, PartialEq)]
enum MutationStage {
    Mutate,
    Pause,
    Kick(Box<RealmConfigDocument>),
}

type MutationTurn = Result<Next<RealmConfigDocument>, MutatePlacementError>;

/// Runs a realm placement mutation, then when it drains the local node kicks the
/// installed outbox drain so pre-holdership-loss records retry. `auth_context` is
/// `None` for local origin; `local` is this node on a node with a net transport.
#[derive(Debug, PartialEq)]
pub struct PlacementMutationOperation {
    config: MutatePlacementConfig,
    auth_context: Option<AuthContext>,
    local: Option<NodeId>,
    attempts: usize,
    stage: MutationStage,
}

impl PlacementMutationOperation {
    pub fn new(
        config: MutatePlacementConfig,
        auth_context: Option<AuthContext>,
        local: Option<NodeId>,
    ) -> Self {
        Self {
            config,
            auth_context,
            local,
            attempts: 0,
            stage: MutationStage::Mutate,
        }
    }

    fn mutate(&mut self) -> MutationTurn {
        self.stage = MutationStage::Mutate;
        let config = self.config.clone();
        let call = match self.auth_context.clone() {
            Some(auth_context) => {
                IoCall::child(MutatePlacementOperation::authorized(config, auth_context))
            }
            None => IoCall::child(MutatePlacementOperation::new(config)),
        };
        Ok(Next::Call(call))
    }

    fn drains_local(&self) -> bool {
        matches!(
            &self.config.mutation,
            RealmPlacementMutation::UpsertNode(entry)
                if entry.draining && self.local == Some(entry.node_id)
        )
    }
}

impl Operation for PlacementMutationOperation {
    type Output = RealmConfigDocument;
    type Error = MutatePlacementError;

    fn first(&mut self) -> MutationTurn {
        self.mutate()
    }

    fn next(&mut self, result: IoResult) -> MutationTurn {
        match std::mem::replace(&mut self.stage, MutationStage::Mutate) {
            MutationStage::Mutate => {
                match result.child::<RealmConfigDocument, MutatePlacementError>()? {
                    Err(MutatePlacementError::StorageError(StorageError::TransactionConflict))
                        if self.attempts < CONFLICT_ATTEMPTS =>
                    {
                        // Retrying with no wait spends every attempt in one contention window.
                        let seed = self.config.actor.node_id;
                        let wait = conflict_backoff(self.attempts, seed.as_bytes());
                        self.attempts += 1;
                        self.stage = MutationStage::Pause;
                        Ok(Next::Call(IoCall::Delay(wait)))
                    }
                    Err(MutatePlacementError::StorageError(StorageError::TransactionConflict)) => {
                        warn!(
                            attempts = self.attempts,
                            "Realm placement mutation kept conflicting"
                        );
                        Err(MutatePlacementError::StorageError(
                            StorageError::TransactionConflict,
                        ))
                    }
                    Ok(config) if self.drains_local() => {
                        self.stage = MutationStage::Kick(Box::new(config));
                        let kick = KickDrainOperation { scanned: false };
                        Ok(Next::Call(IoCall::child(kick)))
                    }
                    outcome => Ok(Next::Done(outcome?)),
                }
            }
            MutationStage::Pause => self.mutate(),
            MutationStage::Kick(config) => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Failed to scan document sync outbox");
                }
                Ok(Next::Done(*config))
            }
        }
    }
}

#[cfg(test)]
#[path = "mutation_tests.rs"]
mod tests;
