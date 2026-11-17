//! Rewrites the subject index entries of one user after its subjects changed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::storage_entries::{stale_subject_deletes, subject_index_writes};
use aruna_core::structs::identity::user::User;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;

pub fn rewrite_subjects(
    previous: Option<&User>,
    current: &User,
    txn_id: TxnId,
) -> Result<Effects, ConversionError> {
    let deletes = stale_subject_deletes(previous, Some(current));
    let writes = subject_index_writes(current);

    let mut effects = smallvec![];
    if !deletes.is_empty() {
        effects.push(Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        }));
    }
    if !writes.is_empty() {
        effects.push(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        }));
    }
    Ok(effects)
}
