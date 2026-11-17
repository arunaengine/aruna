//! Draws placement handles and provisions placement bindings, one at a time on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::OnceLock;

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::{DocumentClass, PlacementBinding, PlacementScope};
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use super::allocate_handle::{AllocateHandleOperation, AllocatedHandle, HandleAllocationError};
use crate::driver::DriverContext;
use crate::placement::mutation::drive_placement_mutation;
use crate::realm::mutate_placement::{MutatePlacementConfig, RealmPlacementMutation};

/// Draws the next handle for `node_id` from its granted (non-conflicted) ranges,
/// advancing and durably persisting the cursor before returning. Returns
/// `placement_handle_exhausted` once the node's ranges are spent.
pub async fn allocate_handle(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
) -> Result<AllocatedHandle, HandleAllocationError> {
    let _guard = allocation_lock().lock().await;
    let config = crate::driver::drive(
        crate::document_repository::GetConfigOperation::new(realm_id),
        context,
    )
    .await?;
    let ranges = config.handle_range_directory().granted_to(&node_id);
    crate::driver::drive(
        AllocateHandleOperation::new(realm_id, node_id, ranges, unix_timestamp_millis()),
        context,
    )
    .await
}

fn allocation_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Allocates a fresh handle for a `(scope, class, strategy)` and appends its
/// immutable Placement Binding with the three provenance fields set. This is the
/// production path new-scope provisioning uses.
pub async fn allocate_placement_binding(
    context: &DriverContext,
    actor: Actor,
    scope: PlacementScope,
    document_class: DocumentClass,
    strategy_id: Ulid,
) -> Result<PlacementBinding, HandleAllocationError> {
    let allocated = allocate_handle(context, actor.realm_id, actor.node_id).await?;
    let binding = PlacementBinding {
        handle: allocated.handle,
        scope,
        document_class,
        strategy_id,
        allocator_range_id: Some(allocated.allocator_range_id),
        allocated_by: Some(allocated.allocated_by),
        allocated_at_ms: Some(allocated.allocated_at_ms),
    };
    drive_placement_mutation(
        MutatePlacementConfig {
            actor,
            mutation: RealmPlacementMutation::AppendPlacementBinding(binding.clone()),
        },
        None,
        context,
    )
    .await?;
    Ok(binding)
}

/// Idempotently provisions the immutable metadata binding required before a
/// placement strategy can become active.
pub async fn provision_metadata_binding(
    context: &DriverContext,
    actor: Actor,
    scope: PlacementScope,
    strategy_id: Ulid,
) -> Result<PlacementBinding, HandleAllocationError> {
    let _guard = provision_lock().lock().await;
    let config = crate::driver::drive(
        crate::document_repository::GetConfigOperation::new(actor.realm_id),
        context,
    )
    .await?;
    if config.strategy(&strategy_id).is_none() {
        return Err(HandleAllocationError::StrategyNotFound(strategy_id));
    }
    if let Some(handle) =
        config
            .binding_directory()
            .handle_for(scope, DocumentClass::Metadata, strategy_id)
    {
        return config
            .placement_bindings
            .into_iter()
            .find(|binding| binding.handle == handle)
            .ok_or_else(|| {
                HandleAllocationError::UnexpectedStorageEvent(
                    "binding directory returned a missing placement binding".to_string(),
                )
            });
    }
    allocate_placement_binding(context, actor, scope, DocumentClass::Metadata, strategy_id).await
}

fn provision_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
#[path = "provision_tests.rs"]
mod tests;
