//! Drops the delete trail of a removed bucket or group, a page at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::jobs::store::iter_prefix_page;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE;
use aruna_core::structs::storage::delete_audit::bucket_prefix;
use aruna_core::types::Key;
use ulid::Ulid;

const TRAIL_PAGE: usize = 500;

/// Deletes the trail rows of one incarnation of `bucket`, or of the whole group without one.
/// A bucket created again under the name has its own rows, which stay. An interrupted pass
/// leaves the rest for the next one.
pub(crate) async fn drop_trail(
    context: &DriverContext,
    group_id: Ulid,
    bucket: Option<&str>,
    incarnation: u128,
) -> Result<(), String> {
    let prefix: Key = match bucket {
        Some(bucket) => bucket_prefix(group_id, bucket, incarnation).into(),
        None => group_id.to_bytes().to_vec().into(),
    };
    loop {
        let storage = &context.storage_handle;
        let page = iter_prefix_page(
            storage,
            DELETE_TRAIL_KEYSPACE,
            Some(prefix.clone()),
            None,
            TRAIL_PAGE,
            None,
        );
        let (rows, next) = page.await?;
        if !rows.is_empty() {
            let space = DELETE_TRAIL_KEYSPACE.to_string();
            let deletes = rows
                .into_iter()
                .map(|(key, _)| (space.clone(), key))
                .collect();
            let effect = StorageEffect::BatchDelete {
                deletes,
                txn_id: None,
            };
            match storage.send_storage_effect(effect).await {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
                other => return Err(format!("could not drop trail rows: {other:?}")),
            }
        }
        if next.is_none() {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::s3::{test_context, test_storage};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::delete_audit::{
        BlobAuditKind, BlobAuditRecord, BlobPurgeKind, trail_key,
    };

    fn record(kind: BlobAuditKind, bucket: &str) -> BlobAuditRecord {
        BlobAuditRecord {
            realm_id: RealmId::from_bytes([1; 32]),
            group_id: Ulid::from_parts(1, 1),
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            user_id: Default::default(),
            kind,
            bucket: bucket.to_string(),
            key: "k".to_string(),
            version_id: None,
            occurred_at_ms: 10,
        }
    }

    #[tokio::test]
    async fn drops_bucket_trail() {
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let purge = BlobAuditKind::Purge(BlobPurgeKind::Bucket);
        let rows = [
            (record(BlobAuditKind::DeleteMarker, "b"), 1),
            (record(purge, "b"), 1),
            (record(purge, "other"), 1),
            // A bucket created again under the name keeps its own rows.
            (record(BlobAuditKind::DeleteMarker, "b"), 2),
        ];
        let mut keys = Vec::new();
        for (row, incarnation) in &rows {
            let key: Key = trail_key(row, *incarnation, Ulid::generate()).into();
            let write = StorageEffect::Write {
                key_space: DELETE_TRAIL_KEYSPACE.to_string(),
                key: key.clone(),
                value: row.to_bytes().unwrap().into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(write).await;
            keys.push(key);
        }
        let group = Ulid::from_parts(1, 1);
        drop_trail(&context, group, Some("b"), 1).await.unwrap();
        let mut kept = Vec::new();
        for key in &keys {
            let read = StorageEffect::Read {
                key_space: DELETE_TRAIL_KEYSPACE.to_string(),
                key: key.clone(),
                txn_id: None,
            };
            let event = context.storage_handle.send_storage_effect(read).await;
            let found = matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
            );
            kept.push(found);
        }
        assert_eq!(kept, [false, false, true, true]);
        // A removed group drops all of its rows.
        drop_trail(&context, group, None, 0).await.unwrap();
        let storage = &context.storage_handle;
        let page = iter_prefix_page(storage, DELETE_TRAIL_KEYSPACE, None, None, 10, None);
        assert!(page.await.unwrap().0.is_empty());
    }
}
