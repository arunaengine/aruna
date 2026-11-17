//! Opens temporary storage and a plain driver context for the S3 tests, with shared seeds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::UPLOAD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::types::Effects;
use aruna_storage::storage::{FjallStorage, StorageHandle};
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

pub(crate) fn test_storage() -> (TempDir, StorageHandle) {
    let directory = tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    (directory, storage)
}

pub(crate) fn test_context(storage: StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

/// Stores a multipart upload record as a test seed.
pub(crate) async fn seed_upload(storage: &StorageHandle, record: &MultipartUpload) {
    let write = StorageEffect::Write {
        key_space: UPLOAD_KEYSPACE.to_string(),
        key: record.upload_id.to_bytes().to_vec().into(),
        value: record.to_bytes().unwrap().into(),
        txn_id: None,
    };
    storage.send_storage_effect(write).await;
}

/// Answers the rows and the commit of a fenced write.
pub(crate) fn fence_done(operation: &mut impl Operation) -> Effects {
    operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: Ulid::from_parts(77, 7),
    }))
}
