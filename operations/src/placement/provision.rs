//! Draws placement handles and provisions placement bindings, one at a time on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::{DocumentClass, PlacementBinding, PlacementScope};
use ulid::Ulid;

use super::allocate_handle::{AllocateHandleOperation, AllocatedHandle, HandleAllocationError};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::placement::mutation::PlacementMutationOperation;
use crate::realm::mutate_placement::{
    MutatePlacementConfig, MutatePlacementError, RealmPlacementMutation,
};

fn config_call(realm_id: RealmId) -> IoCall {
    IoCall::child(GetConfigOperation::new(realm_id))
}

fn read_config(result: IoResult) -> Result<RealmConfigDocument, HandleAllocationError> {
    Ok(result.child::<RealmConfigDocument, GetConfigError>()??)
}

/// Draws the next handle for `node_id` from its granted (non-conflicted) ranges,
/// advancing and durably persisting the cursor before returning. Fails with
/// `placement_handle_exhausted` once the node's ranges are spent.
#[derive(Debug, PartialEq)]
pub struct DrawHandleOperation {
    realm_id: RealmId,
    node_id: NodeId,
    now_ms: u64,
    drawing: bool,
}

impl DrawHandleOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId) -> Self {
        Self {
            realm_id,
            node_id,
            now_ms: 0,
            drawing: false,
        }
    }
}

impl Operation for DrawHandleOperation {
    type Output = AllocatedHandle;
    type Error = HandleAllocationError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PlacementHandles)
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<AllocatedHandle>, HandleAllocationError> {
        Ok(Next::Call(config_call(self.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AllocatedHandle>, HandleAllocationError> {
        if self.drawing {
            let drawn = result.child::<AllocatedHandle, HandleAllocationError>()??;
            return Ok(Next::Done(drawn));
        }
        let config = read_config(result)?;
        let ranges = config.handle_range_directory().granted_to(&self.node_id);
        self.drawing = true;
        let draw = AllocateHandleOperation::new(self.realm_id, self.node_id, ranges, self.now_ms);
        Ok(Next::Call(IoCall::child(draw)))
    }
}

/// Allocates a fresh handle for a `(scope, class, strategy)` and appends its
/// immutable Placement Binding with the three provenance fields set. This is the
/// production path new-scope provisioning uses.
#[derive(Debug, PartialEq)]
pub struct AllocateBindingOperation {
    actor: Actor,
    scope: PlacementScope,
    document_class: DocumentClass,
    strategy_id: Ulid,
    binding: Option<PlacementBinding>,
}

impl AllocateBindingOperation {
    pub fn new(
        actor: Actor,
        scope: PlacementScope,
        document_class: DocumentClass,
        strategy_id: Ulid,
    ) -> Self {
        Self {
            actor,
            scope,
            document_class,
            strategy_id,
            binding: None,
        }
    }
}

impl Operation for AllocateBindingOperation {
    type Output = PlacementBinding;
    type Error = HandleAllocationError;

    fn first(&mut self) -> Result<Next<PlacementBinding>, HandleAllocationError> {
        let draw = DrawHandleOperation::new(self.actor.realm_id, self.actor.node_id);
        Ok(Next::Call(IoCall::child(draw)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PlacementBinding>, HandleAllocationError> {
        if let Some(binding) = self.binding.take() {
            result.child::<RealmConfigDocument, MutatePlacementError>()??;
            return Ok(Next::Done(binding));
        }
        let allocated = result.child::<AllocatedHandle, HandleAllocationError>()??;
        let binding = PlacementBinding {
            handle: allocated.handle,
            scope: self.scope,
            document_class: self.document_class,
            strategy_id: self.strategy_id,
            allocator_range_id: Some(allocated.allocator_range_id),
            allocated_by: Some(allocated.allocated_by),
            allocated_at_ms: Some(allocated.allocated_at_ms),
        };
        let config = MutatePlacementConfig {
            actor: self.actor.clone(),
            mutation: RealmPlacementMutation::AppendPlacementBinding(binding.clone()),
        };
        self.binding = Some(binding);
        let append = PlacementMutationOperation::new(config, None, None);
        Ok(Next::Call(IoCall::child(append)))
    }
}

/// Idempotently provisions the immutable metadata binding required before a
/// placement strategy can become active.
#[derive(Debug, PartialEq)]
pub struct ProvisionBindingOperation {
    actor: Actor,
    scope: PlacementScope,
    strategy_id: Ulid,
    allocating: bool,
}

impl ProvisionBindingOperation {
    pub fn new(actor: Actor, scope: PlacementScope, strategy_id: Ulid) -> Self {
        Self {
            actor,
            scope,
            strategy_id,
            allocating: false,
        }
    }
}

impl Operation for ProvisionBindingOperation {
    type Output = PlacementBinding;
    type Error = HandleAllocationError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PlacementBindings)
    }

    fn first(&mut self) -> Result<Next<PlacementBinding>, HandleAllocationError> {
        Ok(Next::Call(config_call(self.actor.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PlacementBinding>, HandleAllocationError> {
        if self.allocating {
            let binding = result.child::<PlacementBinding, HandleAllocationError>()??;
            return Ok(Next::Done(binding));
        }
        let config = read_config(result)?;
        let strategy_id = self.strategy_id;
        if config.strategy(&strategy_id).is_none() {
            return Err(HandleAllocationError::StrategyNotFound(strategy_id));
        }
        let directory = config.binding_directory();
        let class = DocumentClass::Metadata;
        if let Some(handle) = directory.handle_for(self.scope, class, strategy_id) {
            return (config.placement_bindings.iter())
                .find(|binding| binding.handle == handle)
                .cloned()
                .map(Next::Done)
                .ok_or_else(|| {
                    HandleAllocationError::UnexpectedStorageEvent(
                        "binding directory returned a missing placement binding".to_string(),
                    )
                });
        }
        self.allocating = true;
        let actor = self.actor.clone();
        let allocate = AllocateBindingOperation::new(actor, self.scope, class, strategy_id);
        Ok(Next::Call(IoCall::child(allocate)))
    }
}

#[cfg(test)]
#[path = "provision_tests.rs"]
mod tests;
