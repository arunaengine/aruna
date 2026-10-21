//! Drops the delete trail of a removed bucket or group, a page at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::jobs::store::iter_prefix_page;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DELETE_TRAIL_KEYSPACE;
use aruna_core::structs::storage::delete_audit::{BlobAuditRecord, bucket_prefix, purge_prefix};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

const TRAIL_PAGE: usize = 500;

/// Deletes the trail rows recorded before `before_ms` of `bucket`, or of the whole group
/// without one. A bucket created again under the name keeps its own later rows. An
/// interrupted pass leaves the rest for the next one.
pub(crate) async fn drop_trail(
    context: &DriverContext,
    group_id: Ulid,
    bucket: Option<&str>,
    before_ms: u64,
) -> Result<(), String> {
    let prefixes: Vec<Key> = match bucket {
        Some(bucket) => vec![
            bucket_prefix(group_id, bucket).into(),
            purge_prefix(group_id).into(),
        ],
        None => vec![group_id.to_bytes().to_vec().into()],
    };
    for prefix in prefixes {
        let mut after = None;
        loop {
            let storage = &context.storage_handle;
            let page = iter_prefix_page(
                storage,
                DELETE_TRAIL_KEYSPACE,
                Some(prefix.clone()),
                after.take(),
                TRAIL_PAGE,
                None,
            );
            let (rows, next) = page.await?;
            let deletes = stale(rows, bucket, before_ms)?;
            if !deletes.is_empty() {
                let space = DELETE_TRAIL_KEYSPACE.to_string();
                let deletes = deletes
                    .into_iter()
                    .map(|key| (space.clone(), key))
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
            match next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
    }
    Ok(())
}

/// The rows of `bucket`, or of any bucket, recorded before `before_ms`.
fn stale(
    rows: Vec<(Key, Value)>,
    bucket: Option<&str>,
    before_ms: u64,
) -> Result<Vec<Key>, String> {
    let mut keys = Vec::new();
    for (key, value) in rows {
        let record = BlobAuditRecord::from_bytes(&value).map_err(|error| error.to_string())?;
        let ours = bucket.is_none_or(|bucket| record.bucket == bucket);
        if ours && record.occurred_at_ms < before_ms {
            keys.push(key);
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::s3::{test_context, test_storage};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::delete_audit::{BlobAuditKind, BlobPurgeKind, trail_key};

    fn record(kind: BlobAuditKind, bucket: &str, at: u64) -> BlobAuditRecord {
        BlobAuditRecord {
            realm_id: RealmId::from_bytes([1; 32]),
            group_id: Ulid::from_parts(1, 1),
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            user_id: Default::default(),
            kind,
            bucket: bucket.to_string(),
            key: "k".to_string(),
            version_id: None,
            occurred_at_ms: at,
        }
    }

    #[tokio::test]
    async fn drops_bucket_trail() {
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let purge = BlobAuditKind::Purge(BlobPurgeKind::Bucket);
        let rows = [
            record(BlobAuditKind::DeleteMarker, "b", 10),
            record(purge, "b", 10),
            record(purge, "other", 10),
            // A bucket created again under the name keeps its later rows.
            record(BlobAuditKind::DeleteMarker, "b", 30),
        ];
        let mut keys = Vec::new();
        for row in &rows {
            let key: Key = trail_key(row, Ulid::generate()).into();
            let write = StorageEffect::Write {
                key_space: DELETE_TRAIL_KEYSPACE.to_string(),
                key: key.clone(),
                value: row.to_bytes().unwrap().into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(write).await;
            keys.push(key);
        }
        drop_trail(&context, Ulid::from_parts(1, 1), Some("b"), 20)
            .await
            .unwrap();
        let mut kept = Vec::new();
        for key in &keys {
            let read = StorageEffect::Read {
                key_space: DELETE_TRAIL_KEYSPACE.to_string(),
                key: key.clone(),
                txn_id: None,
            };
            let event = context.storage_handle.send_storage_effect(read).await;
            kept.push(matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
            ));
        }
        assert_eq!(kept, [false, false, true, true]);
        // A removed group drops all of its rows before the time.
        drop_trail(&context, Ulid::from_parts(1, 1), None, 40)
            .await
            .unwrap();
        let page = iter_prefix_page(
            &context.storage_handle,
            DELETE_TRAIL_KEYSPACE,
            None,
            None,
            10,
            None,
        );
        assert!(page.await.unwrap().0.is_empty());
    }
}
