#![cfg(test)]

use crate::driver::DriverContext;
use aruna_storage::storage::FjallStorage;
use tempfile::{TempDir, tempdir};

pub(super) async fn context() -> (TempDir, DriverContext) {
    let directory = tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    (
        directory,
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        },
    )
}
