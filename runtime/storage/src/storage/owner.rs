//! Lets the storage handle take over transactions their owner gave up, and re-exports the owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub use aruna_core::handle::TransactionOwner;
use aruna_core::handle::TransactionRetainer;
use ulid::Ulid;

use super::handle::StorageHandle;

impl TransactionRetainer for StorageHandle {
    fn retain_transaction(&self, txn_id: Ulid, commit_unknown: bool) -> bool {
        StorageHandle::retain_transaction(self, txn_id, commit_unknown)
    }
}
