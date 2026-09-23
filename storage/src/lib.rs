//! Declares the storage crate: the fjall storage module, its errors and compaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod compaction;
pub mod errors;
pub mod storage;

pub use storage::{FjallPersistPolicy, FjallStorage, SEALED_KEYSPACES, StorageHandle, row_aad};
