//! Builds temporary storage, users, seeded records, stored settings and canned management
//! answers for the notification tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::driver::drive;
use crate::notifications::inbox::UpsertInboxOperation;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_resource_path,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_storage::storage::{FjallStorage, StorageHandle};
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

pub fn temp_storage() -> (TempDir, StorageHandle) {
    let directory = tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    (directory, storage)
}

pub fn context(storage: &StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    }
}

pub fn context_with_storage() -> (TempDir, DriverContext) {
    let (directory, storage) = temp_storage();
    (directory, context(&storage))
}

pub fn user(realm: u8, seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId([realm; 32]))
}

pub fn record(
    recipient: UserId,
    class: NotificationClass,
    created_at_ms: u64,
) -> NotificationRecord {
    NotificationRecord::new(
        recipient,
        class,
        NotificationKind::AddedToGroup {
            group_id: Ulid::generate(),
            actor_user_id: user(recipient.realm_id.0[0], 200),
        },
        created_at_ms,
    )
}

pub async fn seed(storage: &StorageHandle, records: &[NotificationRecord]) {
    let upsert = UpsertInboxOperation::new(records.to_vec());
    let outcome = drive(upsert, &context(storage)).await;
    assert_eq!(outcome.map(|outcome| outcome.written), Ok(records.len()));
}

/// Stores one register write of a setting on this node, as a local write or sync would.
pub async fn store_setting(storage: &StorageHandle, target: &DocumentTarget, bytes: Vec<u8>) {
    let write = aruna_core::effects::StorageEffect::Write {
        key_space: target.storage_keyspace().to_string(),
        key: target.storage_key(),
        value: bytes.into(),
        txn_id: None,
    };
    storage.send_storage_effect(write).await;
}

/// Three management nodes, in config order.
pub fn managers() -> [aruna_core::NodeId; 3] {
    [61u8, 62, 63].map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
}

/// Creates a watch on this node as its holder, with the owner's READ check and replication.
pub async fn create_holder_watch(
    context: &DriverContext,
    local_node_id: aruna_core::NodeId,
    owner: UserId,
    path_prefix: String,
    event_mask: aruna_core::structs::execution::notification_watch::WatchEventMask,
    authorization: aruna_core::structs::execution::notification_watch::WatchAuthorizationBinding,
    now_ms: u64,
) -> Result<
    aruna_core::structs::execution::notification_watch::WatchSubscription,
    crate::notifications::watch::records::WatchSubscriptionError,
> {
    let watch = (path_prefix, event_mask);
    let create = crate::notifications::watch::store::CreateWatchOperation::new(
        local_node_id,
        owner,
        watch,
        authorization,
        now_ms,
    )?;
    drive(create, context).await
}

/// Stores a watch of `owner` on this node without authorization or replication.
pub async fn create_local_watch(
    storage: &StorageHandle,
    owner: UserId,
    path_prefix: String,
    event_mask: aruna_core::structs::execution::notification_watch::WatchEventMask,
    now_ms: u64,
) -> Result<
    aruna_core::structs::execution::notification_watch::WatchSubscription,
    crate::notifications::watch::records::WatchSubscriptionError,
> {
    let subscription = crate::notifications::watch::records::validated_subscription(
        owner,
        path_prefix,
        event_mask,
        now_ms,
        Default::default(),
    )?;
    let store =
        crate::notifications::watch::store::StoreWatchOperation::create(subscription.clone(), None);
    drive(store, &context(storage)).await?;
    Ok(subscription)
}

/// Deletes a watch of `owner` on this node without replication.
pub async fn delete_local_watch(
    storage: &StorageHandle,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), crate::notifications::watch::records::WatchSubscriptionError> {
    let store =
        crate::notifications::watch::store::StoreWatchOperation::delete(owner, watch_id, None);
    drive(store, &context(storage)).await.map(|_| ())
}

/// A delivery from `node_id(2)` to `node_id(1)`, which holds the inbox of `user(1, 2)`.
pub fn peer_input() -> crate::notifications::deliver::DeliverInput {
    use crate::tests::locations::node_id;
    crate::notifications::deliver::DeliverInput {
        peer: node_id(2),
        local: node_id(1),
        local_realm: user(1, 2).realm_id,
        now_ms: 1_000,
    }
}

pub fn record(recipient: UserId, seed: u8) -> NotificationRecord {
    NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::AddedToGroup {
            group_id: Ulid::from_bytes([seed; 16]),
            actor_user_id: recipient,
        },
        1_700_000_000_000 + seed as u64,
    )
}

pub fn upload_event(realm_id: RealmId, actor: UserId, path: &str) -> WatchEvent {
    let (_, key) = path.split_once('/').expect("bucket/key fixture");
    WatchEvent {
        event_id: Ulid::from_bytes([7u8; 16]),
        realm_id,
        kind: WatchEventKind::DataUploaded,
        path: data_path(key),
        actor,
        occurred_at_ms: 1_700_000_000_000,
        detail: WatchEventDetail::DataUploaded {
            group_id: data_group_id(),
            node_id: data_node_id(),
            bucket: "bucket".to_string(),
            key: key.to_string(),
            size_bytes: 16,
        },
    }
}

pub fn data_group_id() -> Ulid {
    Ulid::from_bytes([31u8; 16])
}

pub fn data_node_id() -> NodeId {
    iroh::SecretKey::from_bytes(&[31u8; 32]).public()
}

pub fn data_path(key: &str) -> String {
    watch_resource_path(data_group_id(), data_node_id(), "bucket", key)
}
