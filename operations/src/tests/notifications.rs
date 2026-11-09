//! Builds temporary storage, users, seeded records and canned management answers for the
//! notification tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::metadata::protocol::MetadataReadError;
use crate::notifications::inbox::upsert_inbox_records;
use crate::notifications::settings::{SettingAnswer, SettingPeers, StoreAnswer};
use aruna_core::UserId;
use aruna_core::document::{DocumentChange, DocumentTarget};
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::realm::RealmId;
use aruna_storage::storage::{FjallStorage, StorageHandle};
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

pub(crate) fn temp_storage() -> (TempDir, StorageHandle) {
    let directory = tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    (directory, storage)
}

pub(crate) fn context(storage: &StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

pub(crate) fn context_with_storage() -> (TempDir, DriverContext) {
    let (directory, storage) = temp_storage();
    (directory, context(&storage))
}

pub(crate) fn user(realm: u8, seed: u8) -> UserId {
    UserId::new(Ulid::from_bytes([seed; 16]), RealmId([realm; 32]))
}

pub(crate) fn record(
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

pub(crate) async fn seed(storage: &StorageHandle, records: &[NotificationRecord]) {
    assert_eq!(
        upsert_inbox_records(storage, records).await,
        Ok(records.len())
    );
}

/// Management nodes that answer setting reads with fixed answers and count them. A node
/// without an answer is unreachable, and writes always fail.
#[derive(Default)]
pub(crate) struct CannedPeers {
    pub answers: std::collections::HashMap<aruna_core::NodeId, SettingAnswer>,
    pub reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl SettingPeers for CannedPeers {
    async fn read(
        &self,
        node: aruna_core::NodeId,
        _owner: UserId,
        _channel: SettingChannel,
    ) -> Result<SettingAnswer, MetadataReadError> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.answers
            .get(&node)
            .cloned()
            .ok_or(MetadataReadError::Unavailable)
    }

    async fn store(
        &self,
        _node: aruna_core::NodeId,
        _target: DocumentTarget,
        _bytes: Vec<u8>,
        _change: DocumentChange,
    ) -> Result<StoreAnswer, MetadataReadError> {
        Err(MetadataReadError::Unavailable)
    }

    async fn confirm(
        &self,
        _node: aruna_core::NodeId,
        _target: DocumentTarget,
    ) -> Result<(), MetadataReadError> {
        Err(MetadataReadError::Unavailable)
    }
}

/// Three management nodes, in config order.
pub(crate) fn managers() -> [aruna_core::NodeId; 3] {
    [61u8, 62, 63].map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
}
