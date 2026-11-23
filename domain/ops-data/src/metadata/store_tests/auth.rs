//! Shared fixtures of the metadata handle tests: storage, a realm and node ids.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

pub(super) fn auth_storage() -> (TempDir, StorageHandle) {
    let dir = tempdir().expect("temp dir");
    let storage = MemoryStorage::open();
    (dir, storage)
}

pub(super) fn node_id_seed(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}
