//! Tests handle allocation: durable cursor, unique handles under concurrency, provenance, exhaustion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::placement::record::{FIRST_GRANTABLE_HANDLE, HandleRange};
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

    let first = allocate_handle(&context, realm_id, actor.node_id)
        .await
        .unwrap();
    let second = allocate_handle(&context, realm_id, actor.node_id)
        .await
        .unwrap();
    // The range's first handle is the reserved JobControl handle.
    assert_eq!(first.handle.get(), FIRST_GRANTABLE_HANDLE + 1);
    assert_eq!(second.handle.get(), FIRST_GRANTABLE_HANDLE + 2);
    assert_eq!(first.allocator_range_id, Ulid::from_bytes([9; 16]));

    // "Restart": a fresh allocation reads the persisted cursor and does not
    // re-issue a spent handle.
    let third = allocate_handle(&context, realm_id, actor.node_id)
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
        allocate_handle(&context, realm_id, actor.node_id),
        allocate_handle(&context, realm_id, actor.node_id),
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

    let binding = allocate_placement_binding(
        &context,
        actor.clone(),
        PlacementScope::Realm(realm_id),
        DocumentClass::Metadata,
        strategy_id,
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
        allocate_handle(&context, realm_id, actor.node_id)
            .await
            .unwrap()
            .handle
            .get(),
        FIRST_GRANTABLE_HANDLE + 1
    );
    assert!(matches!(
        allocate_handle(&context, realm_id, actor.node_id).await,
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
        allocate_handle(&context, realm_id, actor.node_id).await,
        Err(HandleAllocationError::PlacementHandleExhausted { .. })
    ));
}
