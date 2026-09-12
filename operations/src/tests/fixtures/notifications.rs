use crate::driver::DriverContext;
use crate::notifications::inbox::upsert_inbox_records;
use aruna_core::structs::{NotificationClass, NotificationKind, NotificationRecord, RealmId};
use aruna_core::types::UserId;
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
