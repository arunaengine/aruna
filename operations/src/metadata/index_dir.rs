//! Places the range search indexes of a metadata node beside its metadata store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;

use aruna_storage::StorageHandle;
use aruna_storage::key_index::RangeIndexes;

/// The directory below the metadata storage path that holds the range search indexes.
pub const RANGE_INDEX_DIR: &str = "range-indexes";

/// Installs the range indexes under `path` into `storage`; only a metadata node keeps them.
pub fn install_indexes(storage: &StorageHandle, path: impl AsRef<Path>) {
    storage.install_range_indexes(RangeIndexes::new(path.as_ref().join(RANGE_INDEX_DIR)));
}
