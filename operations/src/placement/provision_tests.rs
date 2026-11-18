//! Tests handle allocation: durable cursor, unique handles under concurrency, provenance, exhaustion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::placement::provision::AllocateBindingOperation;
use crate::tests::calls::{child, runs_child};
use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::record::{FIRST_GRANTABLE_HANDLE, HandleRange};
use aruna_core::structured_id::PlacementHandle;
use tempfile::tempdir;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn actor(realm_id: RealmId) -> Actor {
    Actor {
        node_id: node(1),
        user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
        realm_id,
    }
}

fn context(root: &str) -> DriverContext {
    DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        outbound_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

async fn seed_range_config(
    context: &DriverContext,
    actor: &Actor,
    range: HandleRange,
) -> RealmConfigDocument {
    let mut document = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    document.seed_default_placement();
    document.ensure_node(actor.node_id, RealmNodeKind::Management);
    document.placement_handle_ranges.push(range);
    let target = DocumentTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: document.to_bytes(actor).unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    document
}

fn range(owner: NodeId, start: u32, end: u32) -> HandleRange {
    HandleRange {
        range_id: Ulid::from_bytes([9; 16]),
        owner,
        start,
        end,
    }
}

#[tokio::test]
async fn allocation_survives_restart() {
    let temp = tempdir().unwrap();
    let context = context(temp.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([60; 32]);
    let actor = actor(realm_id);
    seed_range_config(
        &context,
        &actor,
        range(
            actor.node_id,
            FIRST_GRANTABLE_HANDLE,
            FIRST_GRANTABLE_HANDLE + 1024,
        ),
    )
    .await;

    let first = drive(DrawHandleOperation::new(realm_id, actor.node_id), &context)
        .await
        .unwrap();
    let second = drive(DrawHandleOperation::new(realm_id, actor.node_id), &context)
        .await
        .unwrap();
    // The range's first handle is the reserved JobControl handle.
    assert_eq!(first.handle.get(), FIRST_GRANTABLE_HANDLE + 1);
    assert_eq!(second.handle.get(), FIRST_GRANTABLE_HANDLE + 2);
    assert_eq!(first.allocator_range_id, Ulid::from_bytes([9; 16]));

    // "Restart": a fresh allocation reads the persisted cursor and does not
    // re-issue a spent handle.
    let third = drive(DrawHandleOperation::new(realm_id, actor.node_id), &context)
        .await
        .unwrap();
    assert_eq!(third.handle.get(), FIRST_GRANTABLE_HANDLE + 3);
}

#[tokio::test]
async fn concurrent_allocation_unique() {
    let temp = tempdir().unwrap();
    let context = context(temp.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([64; 32]);
    let actor = actor(realm_id);
    seed_range_config(
        &context,
        &actor,
        range(
            actor.node_id,
            FIRST_GRANTABLE_HANDLE,
            FIRST_GRANTABLE_HANDLE + 1024,
        ),
    )
    .await;

    let (left, right) = tokio::join!(
        drive(DrawHandleOperation::new(realm_id, actor.node_id), &context),
        drive(DrawHandleOperation::new(realm_id, actor.node_id), &context),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_ne!(left.handle, right.handle);
    let mut handles = [left.handle.get(), right.handle.get()];
    handles.sort_unstable();
    assert_eq!(
        handles,
        [FIRST_GRANTABLE_HANDLE + 1, FIRST_GRANTABLE_HANDLE + 2]
    );
}

#[tokio::test]
async fn allocation_sets_provenance() {
    let temp = tempdir().unwrap();
    let context = context(temp.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([61; 32]);
    let actor = actor(realm_id);
    let document = seed_range_config(
        &context,
        &actor,
        range(
            actor.node_id,
            FIRST_GRANTABLE_HANDLE,
            FIRST_GRANTABLE_HANDLE + 1024,
        ),
    )
    .await;
    let strategy_id = document.default_strategy_id.unwrap();

    let binding = drive(
        AllocateBindingOperation::new(
            actor.clone(),
            PlacementScope::Realm(realm_id),
            DocumentClass::Metadata,
            strategy_id,
        ),
        &context,
    )
    .await
    .unwrap();

    assert_eq!(binding.allocator_range_id, Some(Ulid::from_bytes([9; 16])));
    assert_eq!(binding.allocated_by, Some(actor.node_id));
    assert!(binding.allocated_at_ms.is_some());
    assert_eq!(binding.handle.get(), FIRST_GRANTABLE_HANDLE + 1);
}

#[tokio::test]
async fn exhaustion_returns_error() {
    let temp = tempdir().unwrap();
    let context = context(temp.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([62; 32]);
    let actor = actor(realm_id);
    // Beyond the reserved first handle the range has one allocatable
    // handle: one allocation succeeds, the next is exhausted.
    seed_range_config(
        &context,
        &actor,
        range(
            actor.node_id,
            FIRST_GRANTABLE_HANDLE,
            FIRST_GRANTABLE_HANDLE + 2,
        ),
    )
    .await;

    assert_eq!(
        drive(DrawHandleOperation::new(realm_id, actor.node_id), &context)
            .await
            .unwrap()
            .handle
            .get(),
        FIRST_GRANTABLE_HANDLE + 1
    );
    assert!(matches!(
        drive(DrawHandleOperation::new(realm_id, actor.node_id), &context).await,
        Err(HandleAllocationError::PlacementHandleExhausted { node }) if node == actor.node_id
    ));
}

#[tokio::test]
async fn no_range_exhausted() {
    let temp = tempdir().unwrap();
    let context = context(temp.path().to_str().unwrap());
    let realm_id = RealmId::from_bytes([63; 32]);
    let actor = actor(realm_id);
    // A range owned by a different node grants this node nothing.
    seed_range_config(&context, &actor, range(node(2), 1, 1025)).await;

    assert!(matches!(
        drive(DrawHandleOperation::new(realm_id, actor.node_id), &context).await,
        Err(HandleAllocationError::PlacementHandleExhausted { .. })
    ));
}

fn test_realm() -> RealmId {
    RealmId::from_bytes([31; 32])
}

fn seeded_config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(test_realm(), Vec::new(), 3);
    config.seed_default_placement();
    config
}

fn seeded_strategy(config: &RealmConfigDocument) -> Ulid {
    config.default_strategy_id.expect("a default strategy")
}

fn config_read(config: RealmConfigDocument) -> IoResult {
    child(Ok::<_, GetConfigError>(config))
}

fn drawn() -> AllocatedHandle {
    AllocatedHandle {
        handle: PlacementHandle::new(FIRST_GRANTABLE_HANDLE + 1).unwrap(),
        allocator_range_id: Ulid::from_bytes([9; 16]),
        allocated_by: node(1),
        allocated_at_ms: 5,
    }
}

fn binding(scope: PlacementScope, strategy_id: Ulid) -> PlacementBinding {
    PlacementBinding {
        handle: drawn().handle,
        scope,
        document_class: DocumentClass::Metadata,
        strategy_id,
        allocator_range_id: Some(drawn().allocator_range_id),
        allocated_by: Some(drawn().allocated_by),
        allocated_at_ms: Some(drawn().allocated_at_ms),
    }
}

#[test]
fn allocations_share_hold() {
    let realm_id = test_realm();
    let strategy = Ulid::from_bytes([2; 16]);
    let scope = PlacementScope::Realm(realm_id);
    let draw = DrawHandleOperation::new(realm_id, node(1));
    let allocate =
        AllocateBindingOperation::new(actor(realm_id), scope, DocumentClass::Metadata, strategy);
    let provision = ProvisionBindingOperation::new(actor(realm_id), scope, strategy);
    assert_eq!(draw.hold_scope(), Some(HoldScope::PlacementHandles));
    assert_eq!(allocate.hold_scope(), Some(HoldScope::PlacementHandles));
    assert_eq!(provision.hold_scope(), Some(HoldScope::PlacementHandles));
}

#[test]
fn draw_uses_ranges() {
    let mut operation = DrawHandleOperation::new(test_realm(), node(1));
    operation.stamp(5);
    assert!(runs_child(operation.first()));
    assert!(runs_child(operation.next(config_read(seeded_config()))));
    let answer = child(Ok::<_, HandleAllocationError>(drawn()));
    assert_eq!(operation.next(answer), Ok(Next::Done(drawn())));
}

#[test]
fn draw_needs_config() {
    let mut operation = DrawHandleOperation::new(test_realm(), node(1));
    operation.first().unwrap();
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert_eq!(
        operation.next(missing),
        Err(HandleAllocationError::ReadConfig(
            GetConfigError::DocumentNotFound
        ))
    );
}

fn allocating(scope: PlacementScope, strategy: Ulid) -> AllocateBindingOperation {
    let class = DocumentClass::Metadata;
    let mut operation = AllocateBindingOperation::new(actor(test_realm()), scope, class, strategy);
    assert!(runs_child(operation.first()));
    let answer = child(Ok::<_, HandleAllocationError>(drawn()));
    assert!(runs_child(operation.next(answer)));
    operation
}

#[test]
fn appended_binding_returned() {
    let scope = PlacementScope::Realm(test_realm());
    let strategy = Ulid::from_bytes([2; 16]);
    let appended = child(Ok::<_, MutatePlacementError>(seeded_config()));
    assert_eq!(
        allocating(scope, strategy).next(appended),
        Ok(Next::Done(binding(scope, strategy)))
    );
}

#[test]
fn failed_append_reported() {
    let scope = PlacementScope::Realm(test_realm());
    let failed = child(Err::<RealmConfigDocument, _>(
        MutatePlacementError::ConfigMissing,
    ));
    assert_eq!(
        allocating(scope, Ulid::from_bytes([2; 16])).next(failed),
        Err(HandleAllocationError::Append(
            MutatePlacementError::ConfigMissing
        ))
    );
}

fn provisioning(scope: PlacementScope, strategy: Ulid) -> ProvisionBindingOperation {
    let mut operation = ProvisionBindingOperation::new(actor(test_realm()), scope, strategy);
    assert!(runs_child(operation.first()));
    operation
}

#[test]
fn unknown_strategy_refused() {
    let strategy = Ulid::from_bytes([2; 16]);
    let scope = PlacementScope::Realm(test_realm());
    assert_eq!(
        provisioning(scope, strategy).next(config_read(seeded_config())),
        Err(HandleAllocationError::StrategyNotFound(strategy))
    );
}

#[test]
fn existing_binding_reused() {
    let mut config = seeded_config();
    let strategy = seeded_strategy(&config);
    let scope = PlacementScope::Group(Ulid::from_bytes([3; 16]));
    config.placement_bindings.push(binding(scope, strategy));
    assert_eq!(
        provisioning(scope, strategy).next(config_read(config)),
        Ok(Next::Done(binding(scope, strategy)))
    );
}

#[test]
fn missing_binding_allocated() {
    let config = seeded_config();
    let strategy = seeded_strategy(&config);
    let scope = PlacementScope::Group(Ulid::from_bytes([3; 16]));
    let mut operation = provisioning(scope, strategy);
    assert!(runs_child(operation.next(config_read(config))));
    let answer = child(Ok::<_, HandleAllocationError>(binding(scope, strategy)));
    assert_eq!(
        operation.next(answer),
        Ok(Next::Done(binding(scope, strategy)))
    );
}
