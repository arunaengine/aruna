//! Purges what a member bucket holds of one range of a distributed bucket: open uploads first,
//! then every key from the ordered inventory with all its versions and kept copies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Pages are bounded, a durable cursor resumes them and nothing is counted first. Each page
// checks the bucket's set, member and incarnation; an absent or replaced bucket ends the purge.

use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::purge_steps::{
    ClearSetOperation, DeleteRowsOperation, PurgeError, PurgeRangeOperation, PurgeRestOperation,
    RangeUploadsOperation, ReadRowOperation,
};
pub use super::purge_steps::{PurgeTarget, Purged};
use crate::driver::{DriverContext, drive};

fn text(error: PurgeError) -> String {
    error.0
}

/// Purges at most `pages` pages of the range. `Done` once the range holds nothing of the set
/// anymore, or the bucket is no longer the bound member.
pub async fn purge_range(
    context: &DriverContext,
    target: &PurgeTarget,
    pages: &mut usize,
) -> Result<Purged, String> {
    let purge = PurgeRangeOperation::new(target.clone(), *pages);
    let (purged, left) = drive(purge, context).await.map_err(text)?;
    *pages = left;
    Ok(purged)
}

/// Whether the range still has an open upload.
pub async fn has_uploads(context: &DriverContext, target: &PurgeTarget) -> Result<bool, String> {
    let look = RangeUploadsOperation::new(target.clone(), false);
    drive(look, context).await.map_err(text)
}

/// Aborts one page of the range's open uploads; `true` while it found any, so the caller
/// looks again until none is left.
pub async fn abort_range_uploads(
    context: &DriverContext,
    target: &PurgeTarget,
) -> Result<bool, String> {
    let abort = RangeUploadsOperation::new(target.clone(), true);
    drive(abort, context).await.map_err(text)
}

/// Deletes one page of versions the inventory does not name, from the bucket's own listing,
/// so the member bucket ends up empty. The listing restarts at the first key each time.
pub async fn purge_rest(context: &DriverContext, target: &PurgeTarget) -> Result<(), String> {
    let purge = PurgeRestOperation::new(target.clone());
    drive(purge, context).await.map_err(text)
}

/// Deletes what this node keeps of a set besides its bucket, at most `pages` pages per call
/// from a durable cursor; the fence row stays with `keep_fence`. Once `bucket` is bound to the
/// set by another binding than `clearing`, the rows are that member's and the clear ends.
pub async fn clear_set_rows(
    context: &DriverContext,
    (set_id, bucket, clearing): (Ulid, &str, Option<&DistributionBinding>),
    keep_fence: bool,
    pages: &mut usize,
) -> Result<Purged, String> {
    let rows = (set_id, bucket.to_string(), clearing.cloned());
    let clear = ClearSetOperation::new(rows, keep_fence, *pages);
    let (purged, left) = drive(clear, context).await.map_err(text)?;
    *pages = left;
    Ok(purged)
}

pub(super) async fn read(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Value>, String> {
    let read = ReadRowOperation {
        key_space: key_space.to_string(),
        key,
    };
    drive(read, context).await.map_err(text)
}

pub(super) async fn batch_delete(
    context: &DriverContext,
    deletes: Vec<(String, Key)>,
) -> Result<(), String> {
    drive(DeleteRowsOperation { deletes }, context)
        .await
        .map_err(text)
}

#[cfg(test)]
mod tests {
    use super::super::purge_steps::{CLEARED, ClearRangeOperation, PurgeKeysOperation};
    use super::*;
    use crate::distribution::test_store::{context, open};
    use crate::s3::object::delete::{DeleteObjectInput, DeleteObjectOperation};
    use aruna_core::UserId;
    use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        CAPACITY_RESERVATION_KEYSPACE, CONTENT_INDEX_KEYSPACE, DISTRIBUTION_FENCE_KEYSPACE,
        DISTRIBUTION_PURGE_KEYSPACE, INVENTORY_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
        RANGE_COUNTER_KEYSPACE, SEARCH_FEED_KEYSPACE, SEARCH_INDEX_KEYSPACE,
    };
    use aruna_core::structs::storage::distribution::accounting::{RangeCounters, account_key};
    use aruna_core::structs::storage::distribution::fence::{
        ClearCursor, LocalFence, PurgeCursor,
    };
    use aruna_core::structs::storage::distribution::inventory::range_interval;

    async fn write(
        context: &DriverContext,
        key_space: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), String> {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(write).await {
            Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
            other => Err(other.label().to_string()),
        }
    }

    /// The rows of the first page of the range in `key_space`.
    async fn page(
        context: &DriverContext,
        key_space: &str,
        target: &PurgeTarget,
    ) -> Vec<(Vec<u8>, Value)> {
        let set_id = target.binding.set_id;
        let (lower, upper) = range_interval(set_id, &target.lower, target.upper.as_deref());
        let read = StorageEffect::IterRange {
            key_space: key_space.to_string(),
            start: IterStart::At(lower.into()),
            end: IterEnd::Before(upper.into()),
            limit: 64,
            max_bytes: 1 << 20,
            txn_id: None,
        };
        let answer = context.storage_handle.send_storage_effect(read).await;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = answer else {
            panic!("unexpected answer {answer:?}");
        };
        (values.into_iter())
            .map(|(key, value)| (key.to_vec(), value))
            .collect()
    }

    async fn clear_page(
        context: &DriverContext,
        key_space: &str,
        prefix: &[u8],
    ) -> Vec<(Key, Value)> {
        let iter = StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(prefix.to_vec().into()),
            start: None,
            limit: 1024,
            txn_id: None,
        };
        let answer = context.storage_handle.send_storage_effect(iter).await;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = answer else {
            panic!("unexpected answer {answer:?}");
        };
        values
    }

    fn row(set_id: Ulid, seed: u8) -> Vec<u8> {
        let mut key = set_id.to_bytes().to_vec();
        key.push(seed);
        key
    }

    #[tokio::test]
    async fn bounded_clearing_resumes() {
        // Each pass spends at most its pages; the next resumes at the cursor until the set is gone.
        use crate::distribution::imports::keep_resume;
        use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
        use aruna_core::structs::storage::distribution::fence::RangeFence;
        let (_dir, handle) = open();
        let context = context(&handle);
        let (set_id, other) = (Ulid::from_bytes([1; 16]), Ulid::from_bytes([2; 16]));
        for seed in 0..4 {
            for id in [set_id, other] {
                write(&context, INVENTORY_KEYSPACE, row(id, seed), vec![seed])
                    .await
                    .unwrap();
            }
        }
        let range_cursor = PurgeCursor::key(set_id, Ulid::from_bytes([3; 16]));
        write(
            &context,
            DISTRIBUTION_PURGE_KEYSPACE,
            range_cursor.clone(),
            vec![],
        )
        .await
        .unwrap();
        let fence = set_id.to_bytes().to_vec();
        let transfer_id = Ulid::from_bytes([4; 16]);
        let mut fenced = LocalFence::default();
        fenced.fence_range(RangeFence {
            range_id: Ulid::from_bytes([3; 16]),
            transfer_id,
            epoch: 1,
            since_ms: 0,
            uploads_open: false,
        });
        let stored = fenced.to_bytes().unwrap();
        write(&context, DISTRIBUTION_FENCE_KEYSPACE, fence.clone(), stored)
            .await
            .unwrap();
        // The imports kept for the set's transfer away leave with the set.
        keep_resume(&context, "spire", transfer_id).await.unwrap();
        let resume = [
            b"import_resume/".as_slice(),
            &transfer_id.to_bytes(),
            &[1; 16],
        ]
        .concat();
        write(&context, NODE_STATE_KEYSPACE, resume, vec![1])
            .await
            .unwrap();
        let reservation = account_key(set_id, Ulid::from_bytes([3; 16]));
        write(
            &context,
            CAPACITY_RESERVATION_KEYSPACE,
            reservation.clone(),
            vec![1],
        )
        .await
        .unwrap();
        let mut passes = 0;
        loop {
            passes += 1;
            let mut pages = 1;
            let cleared = clear_set_rows(&context, (set_id, "spire", None), true, &mut pages).await;
            if cleared.unwrap() == Purged::Done {
                break;
            }
            assert_eq!(pages, 0, "a waiting pass spent its whole budget");
            assert!(passes < 32, "clearing must finish");
        }
        assert!(
            passes > CLEARED.len(),
            "every keyspace takes at least one page"
        );
        for seed in 0..4 {
            let gone = read(&context, INVENTORY_KEYSPACE, row(set_id, seed)).await;
            assert_eq!(gone.unwrap(), None);
            let kept = read(&context, INVENTORY_KEYSPACE, row(other, seed)).await;
            assert!(kept.unwrap().is_some(), "another set's rows stay");
        }
        let purged = read(&context, DISTRIBUTION_PURGE_KEYSPACE, range_cursor).await;
        assert_eq!(purged.unwrap(), None);
        let cursor = read(
            &context,
            DISTRIBUTION_PURGE_KEYSPACE,
            ClearCursor::key(set_id),
        )
        .await;
        assert_eq!(cursor.unwrap(), None, "a finished clear leaves no cursor");
        let kept = read(&context, DISTRIBUTION_FENCE_KEYSPACE, fence).await;
        assert!(kept.unwrap().is_some(), "a deleted set keeps its fence");
        let imports = clear_page(&context, NODE_STATE_KEYSPACE, b"import_").await;
        assert!(imports.is_empty(), "no kept import of the set stays");
        let released = read(&context, CAPACITY_RESERVATION_KEYSPACE, reservation).await;
        assert_eq!(released.unwrap(), None, "the set's capacity is released");
    }

    #[tokio::test]
    async fn split_reclaim_releases() {
        // A split moves the parent's reservation to its children; reclaiming both releases it
        // and the retired parent's rows.
        use crate::distribution::split::{SplitConfig, SplitOperation};
        use crate::distribution::test_store::{apply, install, range, write as update};
        let (_dir, handle) = open();
        let context = context(&handle);
        let set_id = Ulid::from_bytes([5; 16]);
        let ids = [1, 2, 3].map(|seed| Ulid::from_bytes([seed; 16]));
        install(&handle, set_id, &[range(1, "", None)]).await;
        for (key, seed) in [("a", 1), ("c", 2), ("x", 3)] {
            assert!(apply(&handle, update(set_id, key, seed, 1)).await.unwrap());
        }
        let reserved = |id| account_key(set_id, id);
        let charge = 3000u64.to_be_bytes().to_vec();
        write(
            &context,
            CAPACITY_RESERVATION_KEYSPACE,
            reserved(ids[0]),
            charge,
        )
        .await
        .unwrap();
        let split = SplitOperation::new(SplitConfig {
            set_id,
            range_id: ids[0],
            split_key: "m".to_string(),
            left: ids[1],
            right: ids[2],
        });
        drive(split, &context).await.unwrap();
        let mut parts = Vec::new();
        for id in ids {
            let row = read(&context, CAPACITY_RESERVATION_KEYSPACE, reserved(id)).await;
            parts.push(
                row.unwrap()
                    .map(|row| u64::from_be_bytes(row.as_ref().try_into().unwrap())),
            );
        }
        assert_eq!(parts, vec![None, Some(2000), Some(1000)]);
        let clear = |id| ClearRangeOperation::new(set_id, id);
        drive(clear(ids[1]), &context).await.unwrap();
        for space in [RANGE_ACCOUNT_KEYSPACE, RANGE_COUNTER_KEYSPACE] {
            let parent = clear_page(&context, space, &reserved(ids[0])).await;
            assert!(!parent.is_empty(), "the parent stays while a child is held");
        }
        drive(clear(ids[2]), &context).await.unwrap();
        let prefix = set_id.to_bytes();
        let spaces = [
            CAPACITY_RESERVATION_KEYSPACE,
            RANGE_ACCOUNT_KEYSPACE,
            RANGE_COUNTER_KEYSPACE,
        ];
        for space in spaces {
            let left = clear_page(&context, space, &prefix).await;
            assert!(left.is_empty(), "{space} keeps no row of the set");
        }
    }

    #[tokio::test]
    async fn replacement_keeps_rows() {
        // A bucket bound to the set again owns the set's rows; the clear leaves them.
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        use aruna_core::structs::storage::blob::BucketInfo;
        let (_dir, handle) = open();
        let context = context(&handle);
        let set_id = Ulid::from_bytes([1; 16]);
        write(&context, INVENTORY_KEYSPACE, row(set_id, 1), vec![1])
            .await
            .unwrap();
        let info = BucketInfo {
            group_id: Ulid::from_bytes([2; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: Some(DistributionBinding {
                set_id,
                member_id: Ulid::from_bytes([3; 16]),
                incarnation: 9,
                revision: Default::default(),
            }),
        };
        let bucket = b"spire".to_vec();
        write(
            &context,
            S3_BUCKET_KEYSPACE,
            bucket,
            info.to_bytes().unwrap(),
        )
        .await
        .unwrap();
        let mut pages = usize::MAX;
        let cleared = clear_set_rows(&context, (set_id, "spire", None), false, &mut pages).await;
        assert_eq!(cleared.unwrap(), Purged::Done);
        let kept = read(&context, INVENTORY_KEYSPACE, row(set_id, 1))
            .await
            .unwrap();
        assert!(kept.is_some(), "the replacement's inventory stays");
    }

    /// A bound member bucket "spire" whose one range holds keys "a" and "b", 10 bytes each, and
    /// the target purging it with its charged counter row.
    async fn purged_spire(
        handle: &aruna_storage::storage::StorageHandle,
        context: &DriverContext,
    ) -> (PurgeTarget, Vec<u8>) {
        use crate::distribution::test_store::{apply, counters, exact, install, range, write};
        use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
        use aruna_core::structs::storage::blob::BucketInfo;
        use aruna_core::structs::storage::distribution::accounting::generation_key;
        let (snapshot, _) = crate::distribution::test_record::record(true);
        let binding = crate::distribution::test_record::binding(&snapshot, 1, 5);
        let (set_id, range_id) = (binding.set_id, Ulid::from_bytes([1; 16]));
        install(handle, set_id, &[range(1, "", None)]).await;
        for (seed, key) in [(1, "a"), (2, "b")] {
            assert!(apply(handle, write(set_id, key, seed, 10)).await.unwrap());
        }
        let charged = generation_key(set_id, range_id, 1);
        assert_eq!(counters(handle, charged.clone()).await, exact(2, 20));
        let info = BucketInfo {
            group_id: Ulid::from_bytes([2; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: Some(binding.clone()),
        };
        let bucket = info.to_bytes().unwrap();
        self::write(context, S3_BUCKET_KEYSPACE, b"spire".to_vec(), bucket)
            .await
            .unwrap();
        let target = PurgeTarget {
            realm_id: crate::distribution::test_record::realm(),
            node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            bucket: "spire".to_string(),
            group_id: info.group_id,
            binding,
            range_id,
            lower: String::new(),
            upper: None,
        };
        (target, charged)
    }

    fn emptied() -> RangeCounters {
        RangeCounters {
            max_epoch: aruna_core::structs::storage::distribution::OriginalEpoch(1),
            ..RangeCounters::default()
        }
    }

    #[tokio::test]
    async fn purge_debits_counters() {
        // A page takes the rows no version delete took off the range's counters, so what is
        // left of a range being purged stays counted exactly.
        let (_dir, handle) = open();
        let context = context(&handle);
        let (target, charged) = purged_spire(&handle, &context).await;

        let rows = page(&context, INVENTORY_KEYSPACE, &target).await;
        assert_eq!(rows.len(), 4, "two heads and two versions");
        let purge = PurgeKeysOperation::new(target.clone(), rows);
        drive(purge, &context).await.unwrap();

        let counted = crate::distribution::test_store::counters(&handle, charged).await;
        assert_eq!(counted, emptied());
        let rows = page(&context, INVENTORY_KEYSPACE, &target).await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn version_debits_together() {
        // A purged version takes its inventory rows and range counters in its own delete, so a
        // publish never sees the group debit without the range debit.
        use aruna_core::keyspaces::{
            BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, NODE_STATS_KEYSPACE,
        };
        use aruna_core::structs::storage::blob::{
            BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
        };
        use aruna_core::structs::storage::usage::dirty_group_key;
        let (_dir, handle) = open();
        let context = context(&handle);
        let (target, charged) = purged_spire(&handle, &context).await;
        let version_id = Ulid::from_bytes([1; 16]);
        let version = BlobVersion::deleted(
            std::time::SystemTime::UNIX_EPOCH,
            UserId::nil(target.realm_id),
        );
        let row = VersionKey::new("spire", "a", version_id)
            .to_bytes()
            .unwrap();
        self::write(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            row.clone(),
            version.to_bytes().unwrap(),
        )
        .await
        .unwrap();
        let head = BlobHeadKey::new("spire", "a").to_bytes().unwrap();
        let pointer = CurrentVersionPointer::new(version_id).to_bytes().unwrap();
        self::write(&context, BLOB_HEAD_KEYSPACE, head, pointer)
            .await
            .unwrap();

        let input = DeleteObjectInput {
            bucket: target.bucket.clone(),
            key: "a".to_string(),
            version_id: Some(version_id),
            group_id: target.group_id,
            realm_id: target.realm_id,
            node_id: target.node_id,
            deleted_by: UserId::nil(target.realm_id),
        };
        let delete = DeleteObjectOperation::new(input)
            .purging(target.binding.clone())
            .in_inventory();
        drive(delete, &context).await.unwrap();

        assert!(
            read(&context, BLOB_VERSIONS_KEYSPACE, row)
                .await
                .unwrap()
                .is_none()
        );
        let half = crate::distribution::test_store::exact(1, 10);
        let counted = crate::distribution::test_store::counters(&handle, charged).await;
        assert_eq!(
            counted, half,
            "key a left the range with its version and head"
        );
        let rows = page(&context, INVENTORY_KEYSPACE, &target).await;
        assert_eq!(rows.len(), 2, "only key b is left");
        let marker = read(
            &context,
            NODE_STATS_KEYSPACE,
            dirty_group_key(target.group_id),
        );
        assert!(
            marker.await.unwrap().is_some(),
            "the group is published again"
        );
    }

    #[tokio::test]
    async fn clears_search_rows() {
        // Content index, search feed and search index rows leave with the set.
        let (_dir, handle) = open();
        let context = context(&handle);
        let set_id = Ulid::from_bytes([1; 16]);
        let key = account_key(set_id, Ulid::from_bytes([3; 16]));
        let spaces = [
            CONTENT_INDEX_KEYSPACE,
            SEARCH_FEED_KEYSPACE,
            SEARCH_INDEX_KEYSPACE,
        ];
        for space in spaces {
            write(&context, space, key.clone(), vec![1]).await.unwrap();
        }
        let mut pages = usize::MAX;
        let cleared = clear_set_rows(&context, (set_id, "spire", None), false, &mut pages).await;
        assert_eq!(cleared.unwrap(), Purged::Done);
        for space in spaces {
            let gone = read(&context, space, key.clone()).await.unwrap();
            assert_eq!(gone, None, "{space} keeps no row of a cleared set");
        }
    }
}
