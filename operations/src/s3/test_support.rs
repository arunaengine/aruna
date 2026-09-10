#![cfg(test)]

use crate::driver::DriverContext;
use aruna_storage::storage::{FjallStorage, StorageHandle};
use tempfile::{TempDir, tempdir};

pub(super) fn test_storage() -> (TempDir, StorageHandle) {
    let directory = tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    (directory, storage)
}

pub(super) fn test_context(storage: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}
