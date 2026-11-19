//! Declares the storage crate: the fjall storage module, its errors, key indexes, compaction and
//! snapshots.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod compaction;
pub mod errors;
pub mod key_index;
pub mod queue_lag;
pub mod scan;
pub mod snapshot;
pub mod storage;
pub mod task_timers;

pub use storage::{
    CACHE_BYTES, FjallPersistPolicy, FjallStorage, SEALED_KEYSPACES, StorageHandle, row_aad,
};
