//! Tests the storage usage counters, their rebuild and the quota write gate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::bucket::create::CreateBucketOperation;
use crate::s3::realm_usage::{RealmUsage, RealmUsageError, RealmUsageOperation, RealmUsageScope};
use crate::s3::summary_refresh::RefreshUsageOperation;
use crate::s3::usage_rebuild::{RebuildStatsError, RebuildStatsOperation, RebuildStatsState};
use crate::usage_publish::{ClearMarkersOperation, PublishUsageOperation, PublishedUsage};
use aruna_core::calls::ChildOutcome;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
    UPLOAD_KEYSPACE,
};
use aruna_core::operation_check::sample_events;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::storage::blob::{
    BackendRef, BlobHeadKey, BucketInfo, CurrentVersionPointer,
};
use aruna_core::structs::storage::blob::{BlobLocationKey, BlobVersion, VersionKey};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::format::{FrameLayout, StoredFormat, StoredLayout};
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::structs::storage::multipart::MultipartUploadStatus;
use aruna_core::structs::storage::usage::usage_hash_key;
use aruna_core::structs::storage::usage::{BucketCounters, global_shard_index};
use aruna_core::structs::storage::usage::{
    SUMMARY_GLOBAL_KEY, global_shard_keys, usage_global_key, usage_snapshot_key, usage_summary_key,
};
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::testing::memory_context;
use std::time::SystemTime;
use ulid::Ulid;

async fn publish_usage_snapshots(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<PublishedUsage, String> {
    let serving = ctx.node_and_realm().map(|(_, realm_id)| realm_id);
    let publish = PublishUsageOperation::new((node_id, realm_id), serving, full, false);
    drive(publish, ctx).await.map_err(|error| error.to_string())
}

async fn publish_refresh_snapshots(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<PublishedUsage, String> {
    let serving = ctx.node_and_realm().map(|(_, realm_id)| realm_id);
    let publish = PublishUsageOperation::new((node_id, realm_id), serving, full, true);
    drive(publish, ctx).await.map_err(|error| error.to_string())
}
fn row(value: Option<Vec<u8>>) -> IoResult {
    IoResult::Values(vec![value.map(Into::into)])
}

// A rebuild refuses quota writes only while no earlier rebuild completed the counters.
#[test]
fn rebuild_keeps_counters() {
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let mut fresh = QuotaGate::new(10, 1, Ulid::from_bytes([2; 16]), node_id);
    fresh.start_calls();
    assert!(fresh.step_calls(row(Some(Vec::new()))).unwrap().is_some());
    assert_eq!(fresh.step_calls(row(None)).unwrap(), None);
    assert!(fresh.is_rebuilding() && fresh.is_exceeded());

    let mut complete = QuotaGate::new(10, 1, Ulid::from_bytes([2; 16]), node_id);
    complete.start_calls();
    complete.step_calls(row(Some(Vec::new()))).unwrap();
    let next = complete.step_calls(row(Some(Vec::new()))).unwrap();
    assert!(matches!(
        next,
        Some(IoCall::Read(ref reads)) if reads[0].1.as_ref() != USAGE_COMPLETE_KEY
    ));
    assert!(!complete.is_rebuilding());
}

// Every read of the gate goes out as one call, and its answer moves the gate on.
#[test]
fn gate_reads_batches() {
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let group_id = Ulid::from_bytes([2; 16]);
    let mut gate = QuotaGate::new(10, 4, group_id, node_id);
    gate.start_calls();
    // No rebuild runs, so the gate reads the group's counters next.
    gate.step_calls(row(None)).unwrap();
    gate.step_calls(row(Some(logical(3).to_bytes().unwrap())))
        .unwrap();
    let scanned = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(gate.step_calls(scanned), Ok(None));
    assert_eq!(gate.projected_usage(), 7);
    assert!(!gate.is_exceeded());

    // A larger write no longer fits.
    let mut gate = QuotaGate::new(10, 8, group_id, node_id);
    assert!(matches!(gate.start_calls(), IoCall::Read(_)));
    assert!(matches!(
        gate.step_calls(row(None)),
        Ok(Some(IoCall::Read(_)))
    ));
    let counters = row(Some(logical(3).to_bytes().unwrap()));
    assert!(matches!(
        gate.step_calls(counters),
        Ok(Some(IoCall::Page { .. }))
    ));
    let scanned = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(gate.step_calls(scanned), Ok(None));
    assert!(gate.is_exceeded());
}

async fn realm_usage(
    ctx: &DriverContext,
    local: NodeId,
    scope: RealmUsageScope,
) -> Result<RealmUsage, RealmUsageError> {
    let realm_id = ctx.node_and_realm().map(|(_, realm_id)| realm_id);
    let mut totals = drive(RealmUsageOperation::new(local, realm_id, scope), ctx).await?;
    Ok(totals.pop().expect("one scope gives one total"))
}

fn location(blob_size: u64, staging: bool, partial: bool) -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "path".to_string(),
        ulid: Ulid::generate(),
        format: StoredFormat::default(),
        created_at: SystemTime::now(),
        created_by: Default::default(),
        staging,
        partial,
        blob_size,
        hashes: std::collections::HashMap::new(),
    }
}

fn hash_on_shard(shard: usize) -> [u8; 32] {
    let mut blake3 = [0u8; 32];
    blake3[0] = shard as u8;
    blake3
}

#[test]
fn credit_counts_stored() {
    // Backend capacity counts what the backend holds, not the original size.
    let mut framed = location(100, false, false);
    framed.hashes.insert("blake3".to_string(), vec![2u8; 32]);
    framed.format.layout = StoredLayout::Frames(Box::new(FrameLayout {
        level: 3,
        frames: 1,
        stored_size: 40,
        index_hash: [0u8; 32],
    }));

    let delta = StoredDelta::for_location(&framed, true).unwrap();

    assert_eq!((delta.blobs, delta.bytes), (1, 40));
    let adopted = StoredDelta::for_location(&framed, false).unwrap();
    assert_eq!((adopted.blobs, adopted.bytes), (0, 0));
}

#[test]
fn credit_picks_shard() {
    // Stored bytes belong to the hash's shard and the holding backend's row,
    // never to the writing group's shard.
    let group_id = Ulid::from_bytes([1u8; 16]);
    let blake3 = hash_on_shard((global_shard_index(group_id) + 1) % 64);
    let backend = BackendRef::Node("cold".to_string());

    let update = UsageCounterUpdate::with_stored(
        group_id,
        UsageDelta {
            objects: 1,
            logical_bytes: 10,
            ..Default::default()
        },
        StoredDelta::new(blake3, backend.clone(), 1, 10),
    );

    assert_eq!(update.entries.len(), 4);
    let shard = shard_for_hash(&blake3);
    for (key, expected) in [
        (usage_hash_key(&blake3), (1, 10)),
        (usage_backend_key(&backend, shard), (0, 10)),
        (global_group_key(group_id), (0, 0)),
    ] {
        let (_, delta) = update
            .entries
            .iter()
            .find(|(entry, _)| *entry == key)
            .expect("entry present");
        assert_eq!((delta.stored_blobs, delta.stored_bytes), expected);
    }
}

#[test]
fn credit_skips_group() {
    // Reclaim and rebuild account shared copies by hash.
    // The group usage row therefore remains logical-only.
    let group_id = Ulid::from_bytes([2u8; 16]);
    let blake3 = hash_on_shard((global_shard_index(group_id) + 1) % 64);

    let update = UsageCounterUpdate::with_stored(
        group_id,
        UsageDelta {
            objects: 1,
            logical_bytes: 10,
            ..Default::default()
        },
        StoredDelta::new(blake3, BackendRef::node_default(), 1, 10),
    );

    let (_, group) = update
        .entries
        .iter()
        .find(|(key, _)| *key == usage_group_key(group_id))
        .expect("group entry");
    assert_eq!(group.objects, 1);
    assert_eq!((group.stored_blobs, group.stored_bytes), (0, 0));
}

#[test]
fn credit_merges_shard() {
    // One key must carry both halves when the hash shard is the group shard.
    let group_id = Ulid::from_bytes([1u8; 16]);
    let blake3 = hash_on_shard(global_shard_index(group_id));

    let update = UsageCounterUpdate::with_stored(
        group_id,
        UsageDelta {
            objects: 1,
            logical_bytes: 10,
            ..Default::default()
        },
        StoredDelta::new(blake3, BackendRef::node_default(), 1, 10),
    );

    assert_eq!(update.entries.len(), 3);
    let (_, delta) = update
        .entries
        .iter()
        .find(|(key, _)| *key == usage_hash_key(&blake3))
        .expect("global shard entry");
    assert_eq!(delta.objects, 1);
    assert_eq!(delta.logical_bytes, 10);
    assert_eq!(delta.stored_blobs, 1);
    assert_eq!(delta.stored_bytes, 10);
}

#[test]
fn adopted_credits_nothing() {
    let update = UsageCounterUpdate::for_stored(StoredDelta::new(
        [3u8; 32],
        BackendRef::node_default(),
        0,
        0,
    ));

    assert!(update.entries.is_empty());
    assert!(update.is_noop());
}

async fn read_optional_counters(ctx: &DriverContext, key: Vec<u8>) -> Option<UsageCounters> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => Some(UsageCounters::from_bytes(&bytes).unwrap()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => None,
        other => panic!("unexpected event: {other:?}"),
    }
}

async fn read_counters(ctx: &DriverContext, key: Vec<u8>) -> UsageCounters {
    read_optional_counters(ctx, key).await.unwrap_or_default()
}

async fn read_bucket(ctx: &DriverContext, bucket: &str) -> BucketCounters {
    let read = StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: usage_bucket_key(bucket).into(),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => BucketCounters::from_bytes(&bytes).unwrap(),
        other => panic!("expected the bucket row, got {other:?}"),
    }
}

async fn read_global_counters(ctx: &DriverContext) -> UsageCounters {
    let mut total = UsageCounters::default();
    for key in global_shard_keys() {
        total.add(&read_counters(ctx, key).await).unwrap();
    }
    total
}

async fn write_counters(ctx: &DriverContext, key: Vec<u8>, counters: UsageCounters) {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            value: counters.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn update_applies_deltas() {
    let group_id = Ulid::generate();
    let mut update = UsageCounterUpdate::for_group(
        group_id,
        UsageDelta {
            objects: 1,
            logical_bytes: 42,
            ..Default::default()
        },
    );

    let generation = Ulid::from_parts(7, 7);
    let StagedCall::InTransaction(IoCall::AddUsage { deltas, .. }) = update.start_calls(generation)
    else {
        panic!("expected the counter deltas");
    };
    let expected = UsageDelta {
        objects: 1,
        logical_bytes: 42,
        ..Default::default()
    };
    assert_eq!(
        deltas,
        vec![
            (global_group_key(group_id).into(), expected),
            (usage_group_key(group_id).into(), expected),
        ]
    );

    let Some(StagedCall::InTransaction(IoCall::Write(writes))) =
        update.step_calls(IoResult::Done).unwrap()
    else {
        panic!("expected the dirty marker write");
    };
    assert!(
        writes
            .iter()
            .all(|(key_space, ..)| key_space == NODE_STATS_KEYSPACE)
    );
    // Each marker carries the generation the host minted.
    let carried = generation.to_bytes();
    assert!(writes.iter().all(|(.., value)| value.as_ref() == carried));

    let done = update.step_calls(IoResult::Done).unwrap();
    assert!(done.is_none());
    assert!(update.is_done());
}

#[test]
fn bucket_books_delta() {
    let group_id = Ulid::generate();
    let delta = UsageDelta {
        buckets: 1,
        objects: -1,
        logical_bytes: -42,
        ..Default::default()
    };
    let update = UsageCounterUpdate::for_group(group_id, delta);
    let mut update = update.in_bucket("data", RowChange::RemovedVersion);
    let StagedCall::InTransaction(IoCall::AddUsage {
        deltas, buckets, ..
    }) = update.start_calls(Ulid::from_parts(7, 7))
    else {
        panic!("expected the counter deltas");
    };
    assert_eq!(deltas.len(), 2);
    let bucket = BucketDelta {
        objects: -1,
        versions: -1,
        logical_bytes: -42,
        ..Default::default()
    };
    assert_eq!(buckets, vec![(usage_bucket_key("data").into(), bucket)]);

    // A marker alone moves no group counter, yet the update still books it.
    let update = UsageCounterUpdate::for_group(group_id, UsageDelta::default());
    assert!(update.is_noop());
    assert!(!update.in_bucket("data", RowChange::AddedMarker).is_noop());

    // The reclaim sweep's physical debit names no group, so no bucket row moves.
    let stored = StoredDelta::new([1; 32], BackendRef::node_default(), -1, -5);
    let mut update = UsageCounterUpdate::for_stored(stored).in_bucket("data", RowChange::Kept);
    let StagedCall::InTransaction(IoCall::AddUsage { buckets, .. }) =
        update.start_calls(Ulid::from_parts(7, 7))
    else {
        panic!("expected the counter deltas");
    };
    assert!(buckets.is_empty());
}

#[test]
fn rows_follow_replacement() {
    // A replay replaces a row of its own kind; another kind moves one count to the other.
    let cases = [
        (None, false, RowChange::AddedVersion, (1, 0)),
        (None, true, RowChange::AddedMarker, (0, 1)),
        (Some(true), false, RowChange::MarkerToVersion, (1, -1)),
        (Some(false), true, RowChange::VersionToMarker, (-1, 1)),
        (Some(true), true, RowChange::Kept, (0, 0)),
        (Some(false), false, RowChange::Kept, (0, 0)),
    ];
    let group_id = Ulid::generate();
    for (existing, marker, row, (versions, delete_markers)) in cases {
        assert_eq!(RowChange::written(existing, marker), row);
        let update = UsageCounterUpdate::for_group(group_id, UsageDelta::default());
        let mut update = update.in_bucket("data", row);
        let StagedCall::InTransaction(IoCall::AddUsage { buckets, .. }) =
            update.start_calls(Ulid::from_parts(7, 7))
        else {
            panic!("expected the counter deltas");
        };
        let delta = BucketDelta {
            versions,
            delete_markers,
            ..Default::default()
        };
        assert_eq!(buckets, vec![(usage_bucket_key("data").into(), delta)]);
    }
}

#[tokio::test]
async fn bucket_maintains_usage() {
    let ctx = memory_context();
    let group_id = Ulid::generate();

    drive(
        CreateBucketOperation::new(
            "counted".to_string(),
            BucketInfo {
                group_id,
                created_at: SystemTime::now(),
                created_by: Default::default(),
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        ),
        &ctx,
    )
    .await
    .unwrap();

    let global = read_global_counters(&ctx).await;
    assert_eq!(global.buckets, 1);
    let group = read_counters(&ctx, usage_group_key(group_id)).await;
    assert_eq!(group.buckets, 1);
}

#[tokio::test]
async fn load_reads_counters() {
    let ctx = memory_context();
    let group_id = Ulid::generate();
    let group = UsageCounters {
        buckets: 1,
        objects: 2,
        logical_bytes: 3,
        ..Default::default()
    };
    write_counters(&ctx, usage_group_key(group_id), group).await;

    let loaded = drive(LoadCountersOperation::new(usage_group_key(group_id)), &ctx)
        .await
        .unwrap();
    assert_eq!(loaded, group);

    let shard_zero = UsageCounters {
        buckets: 4,
        stored_bytes: 5,
        ..Default::default()
    };
    let shard_one = UsageCounters {
        objects: 6,
        logical_bytes: 7,
        ..Default::default()
    };
    write_counters(&ctx, global_shard_key(0), shard_zero).await;
    write_counters(&ctx, global_shard_key(1), shard_one).await;

    let loaded = drive(LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()), &ctx)
        .await
        .unwrap();
    let mut expected = UsageCounters::default();
    expected.add(&shard_zero).unwrap();
    expected.add(&shard_one).unwrap();
    assert_eq!(loaded, expected);

    let stale_global_ctx = memory_context();
    write_counters(
        &stale_global_ctx,
        USAGE_GLOBAL_KEY.to_vec(),
        UsageCounters {
            buckets: 99,
            ..Default::default()
        },
    )
    .await;
    let loaded = drive(
        LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()),
        &stale_global_ctx,
    )
    .await
    .unwrap();
    assert_eq!(loaded, UsageCounters::default());
}

#[tokio::test]
async fn rebuild_counts_bytes() {
    let ctx = memory_context();
    let group_a = Ulid::generate();
    let group_b = Ulid::generate();

    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        panic!("failed to start transaction");
    };

    for (bucket, group_id) in [("alpha", group_a), ("beta", group_b)] {
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::now(),
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                value: info.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
    }

    // Two completed blobs plus one staging blob that must not count.
    let live = location(100, false, false);
    let shared = location(40, false, false);
    let staged = location(7, true, false);
    let mut hashes = Vec::new();
    for (index, loc) in [&live, &shared, &staged].into_iter().enumerate() {
        let hash = [index as u8 + 1; 32];
        hashes.push(hash);
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::new(hash, EncodingClass::Raw, loc.backend.clone())
                    .to_bytes()
                    .into(),
                value: loc.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
    }

    // Fixtures cover a live object, deletion, reference, and two materialized versions.
    let write_version = async |bucket: &str, key: &str, version: BlobVersion| {
        let version_id = Ulid::generate();
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        version_id
    };
    let write_head = async |bucket: &str, key: &str, version_id: Ulid| {
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
    };

    let now = SystemTime::now();
    let user = Default::default();
    let alpha_live_head = write_version(
        "alpha",
        "live.txt",
        BlobVersion::materialized(
            hashes[0],
            BackendRef::node_default(),
            EncodingClass::Raw,
            now,
            user,
            None,
        ),
    )
    .await;
    write_version(
        "alpha",
        "gone.txt",
        BlobVersion::materialized(
            hashes[1],
            BackendRef::node_default(),
            EncodingClass::Raw,
            now,
            user,
            None,
        ),
    )
    .await;
    let alpha_gone_head = write_version("alpha", "gone.txt", BlobVersion::deleted(now, user)).await;
    write_version(
        "beta",
        "shared.bin",
        BlobVersion::materialized(
            hashes[1],
            BackendRef::node_default(),
            EncodingClass::Raw,
            now,
            user,
            None,
        ),
    )
    .await;
    let beta_head = write_version(
        "beta",
        "shared.bin",
        BlobVersion::materialized(
            hashes[1],
            BackendRef::node_default(),
            EncodingClass::Raw,
            now,
            user,
            None,
        ),
    )
    .await;
    let alpha_ref_head = write_version(
        "alpha",
        "ref.txt",
        BlobVersion::reference(
            VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::Http,
                    public_config: Default::default(),
                    source_path: "ref.txt".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                    namespace: None,
                    root: None,
                },
                connector_id: Some(Ulid::generate()),
                mirrors: Vec::new(),
            },
            SourceMetadata {
                content_length: 30,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
                digests: Default::default(),
            },
            now,
            user,
            now,
        ),
    )
    .await;

    write_head("alpha", "live.txt", alpha_live_head).await;
    write_head("alpha", "gone.txt", alpha_gone_head).await;
    write_head("alpha", "ref.txt", alpha_ref_head).await;
    write_head("beta", "shared.bin", beta_head).await;
    // One upload record in alpha, being completed, and one of a bucket that is gone.
    for (bucket, status) in [
        ("alpha", MultipartUploadStatus::Completing),
        ("gone", MultipartUploadStatus::Open),
    ] {
        let upload = MultipartUpload {
            upload_id: Ulid::generate(),
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: bucket.to_string(),
            key: "part.bin".to_string(),
            group_id: group_a,
            created_by: user,
            created_at: now,
            status,
            checksum_hint: None,
            metadata: Default::default(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
            backend_upload: None,
            encryption: None,
        };
        ctx.storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: UPLOAD_KEYSPACE.to_string(),
                key: upload.upload_id.to_bytes().to_vec().into(),
                value: upload.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
    }

    ctx.storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await;

    let global = drive(RebuildStatsOperation::new(), &ctx).await.unwrap();

    // live.txt, ref.txt, and shared.bin are live; gone.txt ends on a delete marker.
    assert_eq!(global.buckets, 2);
    assert_eq!(global.objects, 3);
    assert_eq!(global.stored_blobs, 2);
    assert_eq!(global.stored_bytes, 140);
    // Referenced source size is reported separately from local logical bytes.
    assert_eq!(global.logical_bytes, 220);
    assert_eq!(global.referenced_bytes, 30);

    let stored_global = read_global_counters(&ctx).await;
    assert_eq!(stored_global, global);

    let alpha = read_counters(&ctx, usage_group_key(group_a)).await;
    assert_eq!(alpha.buckets, 1);
    assert_eq!(alpha.objects, 2);
    assert_eq!(alpha.logical_bytes, 140);
    assert_eq!(alpha.referenced_bytes, 30);

    let beta = read_counters(&ctx, usage_group_key(group_b)).await;
    assert_eq!(beta.buckets, 1);
    assert_eq!(beta.objects, 1);
    assert_eq!(beta.logical_bytes, 80);
    assert_eq!(beta.referenced_bytes, 0);

    // Each bucket row holds its bucket's share and counts its rows.
    let alpha_row = BucketCounters {
        objects: 2,
        versions: 3,
        delete_markers: 1,
        logical_bytes: 140,
        referenced_bytes: 30,
        open_uploads: 1,
    };
    assert_eq!(read_bucket(&ctx, "alpha").await, alpha_row);
    let beta_row = BucketCounters {
        objects: 1,
        versions: 2,
        logical_bytes: 80,
        ..Default::default()
    };
    assert_eq!(read_bucket(&ctx, "beta").await, beta_row);
}

#[tokio::test]
async fn rebuild_removes_stale() {
    let ctx = memory_context();
    let stale_group = Ulid::generate();
    let stale_key = usage_group_key(stale_group);

    ctx.storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes: vec![
                (
                    USAGE_STATS_KEYSPACE.to_string(),
                    stale_key.clone().into(),
                    UsageCounters {
                        buckets: 9,
                        ..Default::default()
                    }
                    .to_bytes()
                    .unwrap()
                    .into(),
                ),
                (
                    USAGE_STATS_KEYSPACE.to_string(),
                    b"global".to_vec().into(),
                    UsageCounters {
                        objects: 9,
                        ..Default::default()
                    }
                    .to_bytes()
                    .unwrap()
                    .into(),
                ),
            ],
            txn_id: None,
        })
        .await;

    drive(RebuildStatsOperation::new(), &ctx).await.unwrap();

    assert!(read_optional_counters(&ctx, stale_key).await.is_none());
    assert!(
        read_optional_counters(&ctx, b"global".to_vec())
            .await
            .is_none()
    );
    assert!(
        read_optional_counters(&ctx, global_shard_key(0))
            .await
            .is_some()
    );
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

async fn read_node_stat(ctx: &DriverContext, key: Vec<u8>) -> Option<Vec<u8>> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|b| b.to_vec()),
        other => panic!("unexpected event: {other:?}"),
    }
}

async fn write_node_stat(ctx: &DriverContext, key: Vec<u8>, value: Vec<u8>) {
    write_stat(ctx, NODE_STATS_KEYSPACE, key, value).await;
}

async fn write_stat(ctx: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    aruna_runner::testing::write_value(&ctx.storage_handle, key_space, key, value).await;
}

fn logical(logical_bytes: u64) -> UsageCounters {
    UsageCounters {
        logical_bytes,
        ..Default::default()
    }
}

/// Drives a gate of a two-node realm: `live` local logical bytes, then each node's snapshot
/// as (counters, replicas), the local one first, and whether a removal shrank the share.
fn run_gate(
    count_replicas: bool,
    limits: (u64, u64),
    live: u64,
    shares: [(u64, u64); 2],
    dirty: bool,
) -> QuotaGate {
    run_gate_at(count_replicas, limits, live, shares, dirty, [1, 1])
}

/// [`run_gate`] with the number of the one set's record each node's share was computed under.
fn run_gate_at(
    count_replicas: bool,
    (ceiling, delta): (u64, u64),
    live: u64,
    shares: [(u64, u64); 2],
    dirty: bool,
    numbers: [u64; 2],
) -> QuotaGate {
    use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};
    let nodes = [node(1), node(2)];
    let group_id = Ulid::from_bytes([2; 16]);
    let realm_id = RealmId([4; 32]);
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.nodes = nodes
        .map(|node_id| RealmNode {
            node_id: node_id.to_string(),
            kind: RealmNodeKind::Management,
        })
        .to_vec();
    config.replica_quota.count = count_replicas;
    let mut gate = QuotaGate::new_for_realm(ceiling, delta, group_id, nodes[0], realm_id);
    gate.start_calls();
    gate.step_calls(row(None)).unwrap();
    gate.step_calls(row(Some(postcard::to_allocvec(&config).unwrap())))
        .unwrap();
    gate.step_calls(row(Some(logical(live).to_bytes().unwrap())))
        .unwrap();
    let set_id = Ulid::from_bytes([3; 16]);
    let values = (nodes.into_iter().zip(shares).zip(numbers))
        .map(|((node_id, (counters, replicas)), number)| {
            let snapshot = NodeUsageSnapshot {
                node_id,
                counters: logical(counters),
                replicas: logical(replicas),
                revisions: BTreeMap::from([(set_id, number)]),
            };
            let key = Key::from(usage_snapshot_key(group_id, node_id));
            (key, Value::from(snapshot.to_bytes().unwrap()))
        })
        .collect();
    let scanned = IoResult::Page {
        rows: values,
        next: None,
    };
    if let Some(call) = gate.step_calls(scanned).unwrap() {
        let IoCall::Read(reads) = &call else {
            panic!("the gate reads one marker: {call:?}");
        };
        assert_eq!(
            reads[0].1.as_ref(),
            dirty_share_key(group_id),
            "the share marker, not the group's"
        );
        let marker = dirty.then(|| vec![1]);
        assert_eq!(gate.step_calls(row(marker)).unwrap(), None);
    }
    gate
}

// Replicas count toward a quota only by the group's setting; the own snapshot adds its
// replica share, never its counters a second time, and no share once a removal shrank it.
#[test]
fn gate_counts_replicas() {
    let cases = [(false, false, 61), (false, true, 101), (true, false, 151)];
    for (count_replicas, dirty, projected) in cases {
        let gate = run_gate(
            count_replicas,
            (1_000, 1),
            100,
            [(100, 40), (50, 50)],
            dirty,
        );
        assert_eq!(gate.projected_usage(), projected);
    }
}

// Another node owns 100 B and this node just purged its 100 B copy: until the next publish
// its stale share must not hide the owner's bytes from a 100 B ceiling.
#[test]
fn stale_share_refused() {
    let gate = run_gate(false, (100, 100), 0, [(100, 100), (100, 0)], true);
    assert!(gate.is_exceeded());
    let published = run_gate(false, (100, 100), 0, [(0, 0), (100, 0)], false);
    assert!(
        published.is_exceeded(),
        "after the publish the owner's bytes still count"
    );
}

// After a promotion the former coordinator publishes its 100 B as replicas; the record marked
// this node's share in its install, so the stale 100 B share is not deducted as well.
#[test]
fn promotion_counts_once() {
    let gate = run_gate(false, (1_000, 1), 120, [(120, 100), (100, 100)], true);
    assert_eq!(gate.projected_usage(), 121);
}

// The former coordinator's snapshot under the promoting record can arrive before that record:
// a share computed under an older record of the set is then not deducted, 1 B counts as 121 B.
#[test]
fn newer_record_counts() {
    let shares = [(120, 100), (100, 100)];
    let early = run_gate_at(false, (1_000, 1), 120, shares, false, [1, 2]);
    assert_eq!(early.projected_usage(), 121);
    let applied = run_gate_at(false, (1_000, 1), 120, shares, false, [2, 2]);
    assert_eq!(
        applied.projected_usage(),
        21,
        "both shares under one record are exact"
    );
}

// The reverse on the former coordinator A: it writes under record 2 while the promoted node's
// snapshot is still from record 1, so that stale share stays charged too: 1 B counts as 121 B.
#[test]
fn older_record_charged() {
    let shares = [(100, 100), (120, 100)];
    let stale = run_gate_at(false, (1_000, 1), 100, shares, false, [2, 1]);
    assert_eq!(stale.projected_usage(), 121);
    let published = run_gate_at(
        false,
        (1_000, 1),
        100,
        [(100, 100), (120, 0)],
        false,
        [2, 2],
    );
    assert_eq!(
        published.projected_usage(),
        121,
        "the republished share counts the same"
    );
}

// Steady additions keep a group dirty but never mark its share, so the gate keeps deducting
// it: with 100 B owned elsewhere and a 100 B local copy, a 150 B quota admits 1 B more.
#[test]
fn additions_keep_share() {
    let group_id = Ulid::from_bytes([2; 16]);
    let markers = |logical_bytes| {
        let delta = UsageDelta {
            logical_bytes,
            ..Default::default()
        };
        let mut update = UsageCounterUpdate::for_group(group_id, delta);
        update.start_calls(Ulid::from_parts(7, 7));
        let Some(StagedCall::InTransaction(IoCall::Write(writes))) =
            update.step_calls(IoResult::Done).unwrap()
        else {
            panic!("the update writes its markers");
        };
        writes
            .iter()
            .map(|(_, key, _)| key.to_vec())
            .collect::<Vec<_>>()
    };
    let share = dirty_share_key(group_id);
    assert!(markers(100).contains(&dirty_group_key(group_id)));
    assert!(
        !markers(100).contains(&share),
        "an addition leaves the share deductible"
    );
    assert!(markers(-100).contains(&share), "a removal may shrink it");

    let gate = run_gate(false, (150, 1), 100, [(100, 100), (100, 0)], false);
    assert_eq!(gate.projected_usage(), 101);
    assert!(!gate.is_exceeded());
}

// Realm totals leave out the copies of distributed ranges, the own node's included, while
// physical stored bytes add up over every node.
#[tokio::test]
async fn totals_count_once() {
    let ctx = memory_context();
    let (local, remote) = (node(1), node(2));
    let group_id = Ulid::from_bytes([5; 16]);
    let counters = |objects, logical_bytes, referenced_bytes, stored_bytes| UsageCounters {
        objects,
        logical_bytes,
        referenced_bytes,
        stored_bytes,
        ..Default::default()
    };
    write_counters(&ctx, global_shard_key(0), counters(3, 30, 9, 50)).await;
    write_counters(&ctx, usage_group_key(group_id), counters(3, 30, 9, 0)).await;
    let snapshot = |node_id, counters, replicas| {
        NodeUsageSnapshot {
            node_id,
            counters,
            replicas,
            revisions: Default::default(),
        }
        .to_bytes()
        .unwrap()
    };
    for (global, key) in [
        (true, usage_global_key(local)),
        (false, usage_snapshot_key(group_id, local)),
    ] {
        let own = counters(3, 30, 9, if global { 50 } else { 0 });
        let value = snapshot(local, own, counters(1, 10, 4, 0));
        write_node_stat(&ctx, key, value).await;
    }
    for (global, key) in [
        (true, usage_global_key(remote)),
        (false, usage_snapshot_key(group_id, remote)),
    ] {
        // The remote node holds copies only.
        let held = counters(2, 20, 6, if global { 25 } else { 0 });
        let value = snapshot(remote, held, counters(2, 20, 6, 0));
        write_node_stat(&ctx, key, value).await;
    }

    let global = realm_usage(&ctx, local, RealmUsageScope::Global)
        .await
        .unwrap();
    assert_eq!(global.counters, counters(2, 20, 5, 75));
    assert_eq!(global.replicas, counters(3, 30, 10, 0));
    let group = realm_usage(&ctx, local, RealmUsageScope::Group(group_id))
        .await
        .unwrap();
    assert_eq!(group.counters, counters(2, 20, 5, 0));
}

fn binding(seed: u8) -> aruna_core::structs::storage::distribution::DistributionBinding {
    aruna_core::structs::storage::distribution::DistributionBinding {
        set_id: Ulid::from_bytes([seed; 16]),
        member_id: Ulid::from_bytes([9; 16]),
        incarnation: 1,
        revision: Default::default(),
    }
}

async fn write_bucket(
    ctx: &DriverContext,
    bucket: &str,
    group_id: GroupId,
    distribution: Option<aruna_core::structs::storage::distribution::DistributionBinding>,
) {
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution,
    };
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        value: info.to_bytes().unwrap().into(),
        txn_id: None,
    };
    ctx.storage_handle.send_storage_effect(write).await;
}

// Distribution records arrive as realm config targets; the groups of local distributed
// buckets are marked so their replica shares are published again.
#[tokio::test]
async fn record_marks_groups() {
    let ctx = memory_context();
    let (bound, plain) = (Ulid::from_bytes([6; 16]), Ulid::from_bytes([7; 16]));
    write_bucket(&ctx, "spire", bound, Some(binding(8))).await;
    write_bucket(&ctx, "plain", plain, None).await;

    let target = DocumentTarget::RealmConfig {
        realm_id: RealmId([4; 32]),
    };
    let refresh = RefreshUsageOperation::new(node(1), None, &[target], Ulid::from_parts(1, 1));
    drive(refresh, &ctx).await.unwrap();

    assert!(read_node_stat(&ctx, dirty_group_key(bound)).await.is_some());
    assert!(read_node_stat(&ctx, dirty_group_key(plain)).await.is_none());
    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_some()
    );
}

// A replica share that cannot be read fails the publish, so its markers stay for the retry.
#[tokio::test]
async fn share_failure_keeps() {
    use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
    let ctx = memory_context();
    let group_id = Ulid::from_bytes([6; 16]);
    write_bucket(&ctx, "spire", group_id, Some(binding(8))).await;
    let record = Ulid::from_bytes([8; 16]).to_bytes().to_vec();
    write_stat(&ctx, DISTRIBUTION_KEYSPACE, record, vec![0xff; 3]).await;
    write_node_stat(&ctx, dirty_group_key(group_id), vec![1]).await;

    let published = publish_usage_snapshots(&ctx, node(1), RealmId([4; 32]), false).await;

    assert!(published.is_err(), "an unreadable record is no zero share");
    assert!(
        read_node_stat(&ctx, dirty_group_key(group_id))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn update_writes_markers() {
    let ctx = memory_context();
    let group_id = Ulid::generate();

    drive(
        CreateBucketOperation::new(
            "counted".to_string(),
            BucketInfo {
                group_id,
                created_at: SystemTime::now(),
                created_by: Default::default(),
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        ),
        &ctx,
    )
    .await
    .unwrap();

    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_some()
    );
    assert!(
        read_node_stat(&ctx, dirty_group_key(group_id))
            .await
            .is_some()
    );
}

// A paged marker scan can pass a group's marker before it is written and then see the
// group's share marker: the group is published in that run, so clearing the marker is safe.
#[tokio::test]
async fn share_marker_publishes() {
    let ctx = memory_context();
    let group_id = Ulid::from_bytes([5; 16]);
    write_counters(&ctx, usage_group_key(group_id), logical(7)).await;
    write_node_stat(&ctx, dirty_share_key(group_id), vec![1]).await;

    let published = publish_usage_snapshots(&ctx, node(1), RealmId([4; 32]), false)
        .await
        .unwrap();

    assert_eq!(published.groups, vec![group_id]);
    let snapshot = read_node_stat(&ctx, usage_snapshot_key(group_id, node(1))).await;
    let snapshot = NodeUsageSnapshot::from_bytes(&snapshot.unwrap()).unwrap();
    assert_eq!(snapshot.counters.logical_bytes, 7);
    assert!(
        read_node_stat(&ctx, dirty_share_key(group_id))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn publish_consumes_markers() {
    let ctx = memory_context();
    let node_id = node(1);
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let group_id = Ulid::generate();

    let counters = UsageCounters {
        buckets: 1,
        logical_bytes: 10,
        ..Default::default()
    };
    write_counters(&ctx, global_group_key(group_id), counters).await;
    write_counters(&ctx, usage_group_key(group_id), counters).await;
    write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), Vec::new()).await;
    write_node_stat(&ctx, dirty_group_key(group_id), Vec::new()).await;

    let published = publish_usage_snapshots(&ctx, node_id, realm_id, false)
        .await
        .unwrap();
    assert!(published.global);
    assert_eq!(published.groups, vec![group_id]);

    // Markers were consumed.
    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_none()
    );
    assert!(
        read_node_stat(&ctx, dirty_group_key(group_id))
            .await
            .is_none()
    );

    // Snapshot documents were written under this node's keys.
    let global = NodeUsageSnapshot::from_bytes(
        &read_node_stat(&ctx, usage_global_key(node_id))
            .await
            .expect("global snapshot written"),
    )
    .unwrap();
    assert_eq!(global.node_id, node_id);
    assert_eq!(global.counters.logical_bytes, 10);
    let group = NodeUsageSnapshot::from_bytes(
        &read_node_stat(&ctx, usage_snapshot_key(group_id, node_id))
            .await
            .expect("group snapshot written"),
    )
    .unwrap();
    assert_eq!(group.counters.logical_bytes, 10);
}

#[tokio::test]
async fn summary_excludes_own() {
    let ctx = memory_context();
    let local = node(1);
    let remote = node(2);
    let group_id = Ulid::generate();

    let local_counters = UsageCounters {
        logical_bytes: 10,
        objects: 1,
        ..Default::default()
    };
    write_counters(&ctx, global_group_key(group_id), local_counters).await;
    write_counters(&ctx, usage_group_key(group_id), local_counters).await;

    let remote_snapshot = |counters| NodeUsageSnapshot {
        node_id: remote,
        counters,
        replicas: Default::default(),
        revisions: Default::default(),
    };
    let remote_counters = UsageCounters {
        logical_bytes: 5,
        objects: 1,
        ..Default::default()
    };
    write_node_stat(
        &ctx,
        usage_global_key(remote),
        remote_snapshot(remote_counters).to_bytes().unwrap(),
    )
    .await;
    write_node_stat(
        &ctx,
        usage_snapshot_key(group_id, remote),
        remote_snapshot(remote_counters).to_bytes().unwrap(),
    )
    .await;
    // This node's own stale snapshot must be ignored in favor of live counters.
    write_node_stat(
        &ctx,
        usage_global_key(local),
        NodeUsageSnapshot {
            node_id: local,
            counters: UsageCounters {
                logical_bytes: 999,
                ..Default::default()
            },
            replicas: Default::default(),
            revisions: Default::default(),
        }
        .to_bytes()
        .unwrap(),
    )
    .await;

    let summary = RealmUsageOperation::summary(local, None, true, vec![group_id]);
    drive(summary, &ctx).await.unwrap();

    let global = realm_usage(&ctx, local, RealmUsageScope::Global)
        .await
        .unwrap()
        .counters;
    assert_eq!(global.logical_bytes, 15);
    assert_eq!(global.objects, 2);
    let group = realm_usage(&ctx, local, RealmUsageScope::Group(group_id))
        .await
        .unwrap()
        .counters;
    assert_eq!(group.logical_bytes, 15);
    assert_eq!(group.objects, 2);
}

#[tokio::test]
async fn load_recomputes_usage() {
    let ctx = memory_context();
    let local = node(1);
    let remote = node(2);

    write_counters(
        &ctx,
        global_shard_key(0),
        UsageCounters {
            logical_bytes: 3,
            ..Default::default()
        },
    )
    .await;
    write_node_stat(
        &ctx,
        usage_global_key(remote),
        NodeUsageSnapshot {
            node_id: remote,
            counters: UsageCounters {
                logical_bytes: 4,
                ..Default::default()
            },
            replicas: Default::default(),
            revisions: Default::default(),
        }
        .to_bytes()
        .unwrap(),
    )
    .await;

    // No summary/global cache entry exists, so the read recomputes on the fly.
    let global = realm_usage(&ctx, local, RealmUsageScope::Global)
        .await
        .unwrap()
        .counters;
    assert_eq!(global.logical_bytes, 7);
}

#[tokio::test]
async fn rebuild_preserves_snapshots() {
    let ctx = memory_context();
    let remote = node(2);
    let snapshot_key = usage_global_key(remote);
    let snapshot = NodeUsageSnapshot {
        node_id: remote,
        counters: UsageCounters {
            logical_bytes: 42,
            ..Default::default()
        },
        replicas: Default::default(),
        revisions: Default::default(),
    };
    write_node_stat(&ctx, snapshot_key.clone(), snapshot.to_bytes().unwrap()).await;
    write_node_stat(&ctx, SUMMARY_GLOBAL_KEY.to_vec(), Vec::new()).await;

    drive(RebuildStatsOperation::new(), &ctx).await.unwrap();

    assert_eq!(
        read_node_stat(&ctx, snapshot_key).await,
        Some(snapshot.to_bytes().unwrap())
    );
    assert!(
        read_node_stat(&ctx, SUMMARY_GLOBAL_KEY.to_vec())
            .await
            .is_some()
    );
}

#[tokio::test]
async fn clear_keeps_regenerated() {
    let ctx = memory_context();
    let generation_one = Ulid::generate().to_bytes().to_vec();

    write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), generation_one.clone()).await;
    let observed = vec![(
        Key::from(DIRTY_GLOBAL_KEY.to_vec()),
        Value::from(generation_one),
    )];

    // A concurrent counter update re-dirties the marker with a new generation
    // after it was observed but before it is cleared.
    let generation_two = Ulid::generate().to_bytes().to_vec();
    write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), generation_two.clone()).await;

    drive(ClearMarkersOperation::new(observed), &ctx)
        .await
        .unwrap();

    // The re-dirtied marker survives, keeping the retry signal for the racing
    // write's counter change.
    assert_eq!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec()).await,
        Some(generation_two.clone())
    );

    // Observing the current generation lets the marker be consumed.
    let observed = vec![(
        Key::from(DIRTY_GLOBAL_KEY.to_vec()),
        Value::from(generation_two),
    )];
    drive(ClearMarkersOperation::new(observed), &ctx)
        .await
        .unwrap();
    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_none()
    );
}

#[tokio::test]
async fn failure_preserves_markers() {
    let ctx = memory_context();
    let node_id = node(1);
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let group_id = Ulid::generate();
    let generation = Ulid::generate().to_bytes().to_vec();

    let counters = UsageCounters {
        buckets: 1,
        ..Default::default()
    };
    write_counters(&ctx, global_group_key(group_id), counters).await;
    write_counters(&ctx, usage_group_key(group_id), counters).await;
    write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), generation.clone()).await;
    write_node_stat(&ctx, dirty_group_key(group_id), generation).await;

    // Corrupt the realm config document so replication fails hard.
    let config_key = DocumentTarget::RealmConfig { realm_id }.storage_key();
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            key: config_key,
            value: Value::from(b"corrupt".to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event: {other:?}"),
    }

    let result = publish_usage_snapshots(&ctx, node_id, realm_id, false).await;
    assert!(
        result.is_err(),
        "replication failure must surface as an error, got {result:?}"
    );

    // Markers survive so the debounced publisher retries the run.
    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_some()
    );
    assert!(
        read_node_stat(&ctx, dirty_group_key(group_id))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn replication_failure_redirties() {
    let ctx = memory_context();
    let node_id = node(1);
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let group_id = Ulid::generate();

    let counters = UsageCounters {
        buckets: 1,
        ..Default::default()
    };
    write_counters(&ctx, global_group_key(group_id), counters).await;
    write_counters(&ctx, usage_group_key(group_id), counters).await;
    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_none()
    );
    assert!(
        read_node_stat(&ctx, dirty_group_key(group_id))
            .await
            .is_none()
    );

    let config_key = DocumentTarget::RealmConfig { realm_id }.storage_key();
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            key: config_key,
            value: Value::from(b"corrupt".to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event: {other:?}"),
    }

    let result = publish_refresh_snapshots(&ctx, node_id, realm_id, true).await;
    assert!(
        result.is_err(),
        "replication failure must surface as an error, got {result:?}"
    );

    assert!(
        read_node_stat(&ctx, usage_global_key(node_id))
            .await
            .is_some()
    );
    assert!(
        read_node_stat(&ctx, usage_snapshot_key(group_id, node_id))
            .await
            .is_some()
    );
    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_some()
    );
    assert!(
        read_node_stat(&ctx, dirty_group_key(group_id))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn summary_failure_preserves() {
    let ctx = memory_context();
    let node_id = node(1);
    let remote = node(2);
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let group_id = Ulid::generate();
    let generation = Ulid::generate().to_bytes().to_vec();

    let counters = UsageCounters {
        buckets: 1,
        ..Default::default()
    };
    write_counters(&ctx, global_group_key(group_id), counters).await;
    write_counters(&ctx, usage_group_key(group_id), counters).await;
    write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), generation.clone()).await;
    write_node_stat(&ctx, dirty_group_key(group_id), generation.clone()).await;
    write_node_stat(&ctx, usage_global_key(remote), b"corrupt".to_vec()).await;

    let result = publish_refresh_snapshots(&ctx, node_id, realm_id, false).await;
    assert!(
        result.is_err(),
        "summary recompute failure must surface as an error, got {result:?}"
    );

    // The snapshots were published, but the markers survive so the debounced
    // publisher retries and refreshes the realm summary cache later.
    assert!(
        read_node_stat(&ctx, usage_global_key(node_id))
            .await
            .is_some()
    );
    assert_eq!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec()).await,
        Some(generation.clone())
    );
    assert_eq!(
        read_node_stat(&ctx, dirty_group_key(group_id)).await,
        Some(generation)
    );
}

#[tokio::test]
async fn summary_failure_redirties() {
    let ctx = memory_context();
    let node_id = node(1);
    let remote = node(2);
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let group_id = Ulid::generate();

    let counters = UsageCounters {
        buckets: 1,
        logical_bytes: 10,
        ..Default::default()
    };
    write_counters(&ctx, global_group_key(group_id), counters).await;
    write_counters(&ctx, usage_group_key(group_id), counters).await;
    write_node_stat(&ctx, usage_global_key(remote), b"corrupt".to_vec()).await;

    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_none()
    );
    assert!(
        read_node_stat(&ctx, dirty_group_key(group_id))
            .await
            .is_none()
    );

    let result = publish_refresh_snapshots(&ctx, node_id, realm_id, true).await;
    assert!(
        result.is_err(),
        "summary recompute failure must surface as an error, got {result:?}"
    );

    // Full startup may publish without markers. A later summary failure must mark every
    // published scope so the debounced publisher retries.
    assert!(
        read_node_stat(&ctx, usage_global_key(node_id))
            .await
            .is_some()
    );
    assert!(
        read_node_stat(&ctx, usage_snapshot_key(group_id, node_id))
            .await
            .is_some()
    );
    assert!(
        read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
            .await
            .is_some()
    );
    assert!(
        read_node_stat(&ctx, dirty_group_key(group_id))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn publish_zeros_stale() {
    let ctx = memory_context();
    let node_id = node(1);
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let live_group = Ulid::generate();
    let stale_group = Ulid::generate();

    // A live group still has a counter key.
    let counters = UsageCounters {
        buckets: 1,
        logical_bytes: 10,
        ..Default::default()
    };
    write_counters(&ctx, usage_group_key(live_group), counters).await;

    // Removing a previously published group's counter leaves a stale snapshot.
    write_node_stat(
        &ctx,
        usage_snapshot_key(stale_group, node_id),
        NodeUsageSnapshot {
            node_id,
            counters: UsageCounters {
                buckets: 5,
                logical_bytes: 500,
                ..Default::default()
            },
            replicas: Default::default(),
            revisions: Default::default(),
        }
        .to_bytes()
        .unwrap(),
    )
    .await;

    let published = publish_usage_snapshots(&ctx, node_id, realm_id, true)
        .await
        .unwrap();
    assert!(published.groups.contains(&stale_group));
    assert!(published.groups.contains(&live_group));

    // The stale snapshot is overwritten with a zero total.
    let stale = NodeUsageSnapshot::from_bytes(
        &read_node_stat(&ctx, usage_snapshot_key(stale_group, node_id))
            .await
            .expect("stale snapshot present"),
    )
    .unwrap();
    assert_eq!(stale.counters, UsageCounters::default());

    // The live group keeps its real total.
    let live = NodeUsageSnapshot::from_bytes(
        &read_node_stat(&ctx, usage_snapshot_key(live_group, node_id))
            .await
            .expect("live snapshot present"),
    )
    .unwrap();
    assert_eq!(live.counters.logical_bytes, 10);
}

#[tokio::test]
async fn refresh_recomputes_targets() {
    let ctx = memory_context();
    let local = node(1);
    let remote = node(2);
    let group_id = Ulid::generate();

    let local_counters = UsageCounters {
        logical_bytes: 10,
        ..Default::default()
    };
    write_counters(&ctx, global_group_key(group_id), local_counters).await;
    write_counters(&ctx, usage_group_key(group_id), local_counters).await;

    let remote_counters = UsageCounters {
        logical_bytes: 5,
        ..Default::default()
    };
    write_node_stat(
        &ctx,
        usage_global_key(remote),
        NodeUsageSnapshot {
            node_id: remote,
            counters: remote_counters,
            replicas: Default::default(),
            revisions: Default::default(),
        }
        .to_bytes()
        .unwrap(),
    )
    .await;
    write_node_stat(
        &ctx,
        usage_snapshot_key(group_id, remote),
        NodeUsageSnapshot {
            node_id: remote,
            counters: remote_counters,
            replicas: Default::default(),
            revisions: Default::default(),
        }
        .to_bytes()
        .unwrap(),
    )
    .await;

    let targets = [
        DocumentTarget::NodeUsage {
            realm_id: RealmId::from_bytes([9u8; 32]),
            node_id: remote,
            group_id: None,
        },
        DocumentTarget::NodeUsage {
            realm_id: RealmId::from_bytes([9u8; 32]),
            node_id: remote,
            group_id: Some(group_id),
        },
    ];
    let refresh = RefreshUsageOperation::new(local, None, &targets, Ulid::from_parts(1, 1));
    drive(refresh, &ctx).await.unwrap();

    // The summed cache was materialized for both touched scopes.
    assert_eq!(
        read_node_stat(&ctx, SUMMARY_GLOBAL_KEY.to_vec())
            .await
            .map(|bytes| UsageCounters::from_bytes(&bytes).unwrap().logical_bytes),
        Some(15)
    );
    assert_eq!(
        read_node_stat(&ctx, usage_summary_key(group_id))
            .await
            .map(|bytes| UsageCounters::from_bytes(&bytes).unwrap().logical_bytes),
        Some(15)
    );
}

// The swap after the walk takes only its write answer; any other event is refused.
#[test]
fn terminal_rejects_event() {
    let finishing = || {
        let mut rebuild = RebuildStatsOperation::new();
        rebuild.first().unwrap();
        let walked = Ok::<_, RebuildStatsError>(UsageCounters::default());
        let swap = rebuild.next(IoResult::Child(ChildOutcome::new(walked)));
        assert!(matches!(swap, Ok(Next::Call(IoCall::Effect(_)))));
        rebuild
    };
    for event in sample_events() {
        if matches!(event, Event::Storage(StorageEvent::BatchWriteResult { .. })) {
            continue;
        }
        let label = event.label();
        let refused = finishing().next(IoResult::Event(Box::new(event)));
        let finish = matches!(
            refused,
            Err(RebuildStatsError::InvalidStateEvent {
                state: RebuildStatsState::Finish,
                ..
            })
        );
        assert!(finish, "{label}: {refused:?}");
    }
}
