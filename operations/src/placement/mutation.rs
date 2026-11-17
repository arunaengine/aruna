//! Drives realm placement mutations with conflict re-drives, then kicks the outbox drain when
//! a mutation drains this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tracing::warn;

use crate::realm::mutate_placement::{
    CONFLICT_ATTEMPTS, MutatePlacementConfig, MutatePlacementError, MutatePlacementOperation,
    RealmPlacementMutation,
};

/// Drives a realm placement mutation, then when it drains the local node kicks the
/// installed outbox drain owner so pre-holdership-loss records retry without a second
/// drainer or replacing a persisted deadline. `auth_context` is `None` for local origin.
pub async fn drive_placement_mutation(
    config: MutatePlacementConfig,
    auth_context: Option<AuthContext>,
    context: &crate::driver::DriverContext,
) -> Result<RealmConfigDocument, MutatePlacementError> {
    let drains_node = matches!(
        &config.mutation,
        RealmPlacementMutation::UpsertNode(entry)
            if entry.draining
                && context.net_handle.as_ref().map(|net| net.node_id()) == Some(entry.node_id)
    );
    let mut attempts = 0;
    let outcome = loop {
        let operation = match auth_context.clone() {
            Some(auth_context) => {
                MutatePlacementOperation::authorized(config.clone(), auth_context)
            }
            None => MutatePlacementOperation::new(config.clone()),
        };
        match crate::driver::drive(operation, context).await {
            Err(MutatePlacementError::StorageError(StorageError::TransactionConflict))
                if attempts < CONFLICT_ATTEMPTS =>
            {
                // Retrying with no wait spends every attempt in one contention window.
                tokio::time::sleep(aruna_core::backoff::conflict_backoff(
                    attempts,
                    config.actor.node_id.as_bytes(),
                ))
                .await;
                attempts += 1;
            }
            Err(MutatePlacementError::StorageError(StorageError::TransactionConflict)) => {
                warn!(attempts, "Realm placement mutation kept conflicting");
                break Err(MutatePlacementError::StorageError(
                    StorageError::TransactionConflict,
                ));
            }
            outcome => break outcome,
        }
    };
    if outcome.is_ok() && drains_node && context.net_handle.is_some() {
        crate::tasks::incoming::drive_sync_drain(std::sync::Arc::new(context.clone())).await;
    }
    outcome
}

#[cfg(test)]
#[path = "mutation_tests.rs"]
mod tests;
