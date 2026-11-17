//! Writes reconciled documents into their storage keyspaces, guarded by create fences.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

pub(in crate::document_sync) fn target_write_entry(
    target: DocumentTarget,
    value: Value,
) -> (String, ByteView, Value) {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        value,
    )
}

pub(in crate::document_sync) async fn apply_create_event(
    storage: &StorageHandle,
    event: &MetadataEventRecord,
    target: DocumentTarget,
    bytes: Vec<u8>,
) -> Result<()> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        match create_fence_txn(storage, event, txn_id).await {
            Ok(true) => {
                let _ = abort_txn(storage, txn_id).await;
                return Ok(());
            }
            Ok(false) => {}
            Err(error) => {
                let _ = abort_txn(storage, txn_id).await;
                return Err(error);
            }
        }
        let writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            ByteView::from(bytes.clone()),
        )];
        if commit_batch(storage, txn_id, Vec::new(), writes).await? {
            return Ok(());
        }
    }
    Err(NetError::Dht(
        "metadata create admission conflicted twice".to_string(),
    ))
}
