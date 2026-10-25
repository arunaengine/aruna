//! Tests the paged usage rebuild against real storage: totals, exactness under writes made
//! during the walk, and quota buckets first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;

use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{
    GroupQuotaOverride, QuotaConfig, RealmConfigDocument, RealmId,
};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::structs::storage::format::{
    Compression, EncodingClass, PithosLayout, StoredFormat,
};
use aruna_core::structs::storage::usage::{
    BucketCounters, BucketDelta, USAGE_REBUILD_KEY, UsageCounters, UsageDelta, usage_bucket_key,
    usage_group_key,
};
use aruna_core::types::{GroupId, Key};
use tempfile::tempdir;
use ulid::Ulid;

use super::{RebuildStatsError, RebuildStatsOperation};
use crate::driver::DriverContext;
use crate::node::rebuild_counts::CounterDeltas;

const GROUP_A: GroupId = Ulid::from_parts(1, 1);
const GROUP_B: GroupId = Ulid::from_parts(2, 2);
/// Delete markers in beta, more than one walk page.
const MARKERS: usize = 300;

fn context(root: &str) -> DriverContext {
    DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

fn location(blob_size: u64) -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "path".to_string(),
        ulid: Ulid::generate(),
        format: StoredFormat::default(),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        staging: false,
        partial: false,
        blob_size,
        hashes: HashMap::new(),
    }
}

async fn write(ctx: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn read<T>(ctx: &DriverContext, key: Vec<u8>, decode: fn(&[u8]) -> T) -> Option<T> {
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("unexpected event {event:?}");
    };
    value.map(|value| decode(value.as_ref()))
}

fn counters(bytes: &[u8]) -> UsageCounters {
    UsageCounters::from_bytes(bytes).unwrap()
}

fn bucket_row(bytes: &[u8]) -> BucketCounters {
    BucketCounters::from_bytes(bytes).unwrap()
}

async fn bucket(ctx: &DriverContext, name: &str, group_id: GroupId) {
    let info = BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let key = name.as_bytes().to_vec();
    write(ctx, S3_BUCKET_KEYSPACE, key, info.to_bytes().unwrap()).await;
}

/// Writes a version and points its key's head at it.
async fn version(ctx: &DriverContext, bucket: &str, key: &str, version: BlobVersion) -> VersionKey {
    let version_key = VersionKey::new(bucket, key, Ulid::generate());
    let row = version_key.to_bytes().unwrap();
    write(
        ctx,
        BLOB_VERSIONS_KEYSPACE,
        row,
        version.to_bytes().unwrap(),
    )
    .await;
    let head = BlobHeadKey::new(bucket, key).to_bytes().unwrap();
    let pointer = CurrentVersionPointer::new(version_key.version_id);
    write(ctx, BLOB_HEAD_KEYSPACE, head, pointer.to_bytes().unwrap()).await;
    version_key
}

fn materialized(hash: [u8; 32]) -> BlobVersion {
    let now = SystemTime::UNIX_EPOCH;
    let backend = BackendRef::node_default();
    BlobVersion::materialized(
        hash,
        backend,
        EncodingClass::Raw,
        now,
        Default::default(),
        None,
    )
}

/// Two buckets: alpha holds a plain object and a sealed object whose Pithos archive is being
/// promoted (a known and a pending row) with its envelope; beta a plain object with a pending
/// sealed copy and enough delete markers for several pages. Returns the plain stored size.
async fn fixture(ctx: &DriverContext) -> u64 {
    bucket(ctx, "alpha", GROUP_A).await;
    bucket(ctx, "beta", GROUP_B).await;
    let plain = location(100);
    let hash = [1u8; 32];
    let key = BlobLocationKey::new(hash, EncodingClass::Raw, plain.backend.clone()).to_bytes();
    write(ctx, BLOB_LOCATIONS_KEYSPACE, key, plain.to_bytes().unwrap()).await;

    let mut sealed = location(100);
    let layout = PithosLayout {
        stored_size: 130,
        metadata_digest: [4u8; 32],
        storage_generation: 0,
    };
    sealed.format = StoredFormat::pithos(layout, BucketKeyRef::new(Ulid::generate(), 1));
    let archive = ArchiveKey::of(&sealed);
    let row = sealed.to_bytes().unwrap();
    write(ctx, PENDING_LOCATION_KEYSPACE, archive.to_bytes(), row).await;
    let mut known = sealed.clone();
    known.hashes.insert("blake3".to_string(), vec![9u8; 32]);
    let key = BlobLocationKey::new([9u8; 32], EncodingClass::Raw, sealed.backend.clone());
    write(
        ctx,
        BLOB_LOCATIONS_KEYSPACE,
        key.to_bytes(),
        known.to_bytes().unwrap(),
    )
    .await;

    version(ctx, "alpha", "live.txt", materialized(hash)).await;
    let now = SystemTime::UNIX_EPOCH;
    let pending = BlobVersion::pending(archive, now, Default::default(), None);
    let sealed_key = version(ctx, "alpha", "sealed.bin", pending).await;
    let envelope = Ulid::generate().to_bytes().to_vec();
    write(ctx, ABE_ENVELOPE_KEYSPACE, envelope.clone(), vec![1; 40]).await;
    write(ctx, ABE_ARCHIVE_KEYSPACE, envelope.clone(), vec![2; 9]).await;
    let row = sealed_key.to_bytes().unwrap();
    write(ctx, ABE_VERSION_KEYSPACE, row, envelope).await;

    let copy_key = version(ctx, "beta", "copy.bin", materialized(hash)).await;
    let copy = crate::abe::copies::copy_row(Ulid::generate(), &copy_key).unwrap();
    write(ctx, ABE_COPY_KEYSPACE, copy, vec![3; 70]).await;
    for index in 0..MARKERS {
        let marker = BlobVersion::deleted(now, Default::default());
        version(ctx, "beta", &format!("marker-{index:04}"), marker).await;
    }
    plain.stored_size()
}

/// Drives the rebuild against storage; `live` commits right after the rebuild began, like a
/// writer racing the walk. Returns the output and the prefixes of version page reads.
async fn run(
    mut operation: RebuildStatsOperation,
    ctx: &DriverContext,
    live: Option<CounterDeltas>,
) -> (Result<UsageCounters, RebuildStatsError>, Vec<Option<Key>>) {
    let mut live = live;
    let mut prefixes = Vec::new();
    let mut queue: VecDeque<Effect> = operation.start().into_iter().collect();
    while let Some(effect) = queue.pop_front() {
        let Effect::Storage(effect) = effect else {
            panic!("unexpected effect {effect:?}");
        };
        if let StorageEffect::Iter {
            key_space, prefix, ..
        } = &effect
            && key_space == BLOB_VERSIONS_KEYSPACE
        {
            prefixes.push(prefix.clone());
        }
        let begin = matches!(effect, StorageEffect::BeginUsageRebuild { .. });
        let event = ctx.storage_handle.send_storage_effect(effect).await;
        if begin && let Some((deltas, buckets)) = live.take() {
            commit_live(ctx, deltas, buckets).await;
        }
        queue.extend(operation.step(event));
    }
    assert!(operation.is_complete());
    (operation.finalize(), prefixes)
}

async fn commit_live(
    ctx: &DriverContext,
    deltas: Vec<(Key, UsageDelta)>,
    buckets: Vec<(Key, BucketDelta)>,
) {
    let storage = &ctx.storage_handle;
    let start = StorageEffect::StartTransaction { read: false };
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) =
        storage.send_storage_effect(start).await
    else {
        panic!("no write transaction");
    };
    let add = StorageEffect::AddUsage {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        deltas,
        buckets,
        txn_id,
    };
    storage.send_storage_effect(add).await;
    let commit = StorageEffect::CommitTransaction { txn_id };
    let event = storage.send_storage_effect(commit).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::TransactionCommitted { .. })
    ));
}

/// The paged walk books what the whole-keyspace rebuild booked: a promoted Pithos archive is
/// one stored blob, a pending version is charged its archive, envelopes and pending copies
/// their rows, and delete markers over several pages count only as markers.
#[tokio::test]
async fn walk_matches_totals() {
    let temp = tempdir().unwrap();
    let ctx = context(temp.path().to_str().unwrap());
    let plain_stored = fixture(&ctx).await;

    let (output, _) = run(RebuildStatsOperation::new(), &ctx, None).await;
    let total = output.unwrap();

    assert_eq!(total.buckets, 2);
    assert_eq!(total.objects, 3);
    assert_eq!(total.stored_blobs, 2);
    assert_eq!(total.stored_bytes, plain_stored + 130);
    assert_eq!(total.logical_bytes, 100 + 100 + 49 + 100 + 70);
    let alpha = read(&ctx, usage_group_key(GROUP_A), counters)
        .await
        .unwrap();
    assert_eq!(
        (alpha.buckets, alpha.objects, alpha.logical_bytes),
        (1, 2, 249)
    );
    let beta = read(&ctx, usage_group_key(GROUP_B), counters)
        .await
        .unwrap();
    assert_eq!(
        (beta.buckets, beta.objects, beta.logical_bytes),
        (1, 1, 170)
    );
    let alpha_row = read(&ctx, usage_bucket_key("alpha"), bucket_row).await;
    let expected = BucketCounters {
        objects: 2,
        versions: 2,
        logical_bytes: 249,
        ..Default::default()
    };
    assert_eq!(alpha_row, Some(expected));
    let beta_row = read(&ctx, usage_bucket_key("beta"), bucket_row)
        .await
        .unwrap();
    assert_eq!(beta_row.delete_markers, MARKERS as u64);
    assert_eq!((beta_row.objects, beta_row.versions), (1, 1));
    assert!(
        read(&ctx, USAGE_REBUILD_KEY.to_vec(), |_| ())
            .await
            .is_none()
    );
}

/// A write committed after the rebuild began is counted once, a removal of an object the
/// snapshot holds never clamps, and drift in the old rows is replaced.
#[tokio::test]
async fn walk_counts_concurrent() {
    let temp = tempdir().unwrap();
    let ctx = context(temp.path().to_str().unwrap());
    bucket(&ctx, "alpha", GROUP_A).await;
    let plain = location(100);
    let hash = [1u8; 32];
    let key = BlobLocationKey::new(hash, EncodingClass::Raw, plain.backend.clone()).to_bytes();
    write(
        &ctx,
        BLOB_LOCATIONS_KEYSPACE,
        key,
        plain.to_bytes().unwrap(),
    )
    .await;
    version(&ctx, "alpha", "live.txt", materialized(hash)).await;
    let drift = UsageCounters {
        objects: 99,
        ..Default::default()
    };
    write(
        &ctx,
        USAGE_STATS_KEYSPACE,
        usage_group_key(GROUP_A),
        drift.to_bytes().unwrap(),
    )
    .await;

    // One new object of 7 bytes, and the snapshot's 100 byte object deleted.
    let delta = UsageDelta {
        objects: 0,
        logical_bytes: 7 - 100,
        ..Default::default()
    };
    let row = BucketDelta {
        versions: 0,
        logical_bytes: 7 - 100,
        ..Default::default()
    };
    let live = (
        vec![(Key::from(usage_group_key(GROUP_A)), delta)],
        vec![(Key::from(usage_bucket_key("alpha")), row)],
    );
    let (output, _) = run(RebuildStatsOperation::new(), &ctx, Some(live)).await;
    output.unwrap();

    let alpha = read(&ctx, usage_group_key(GROUP_A), counters)
        .await
        .unwrap();
    assert_eq!(
        (alpha.buckets, alpha.objects, alpha.logical_bytes),
        (1, 1, 7)
    );
    let alpha_row = read(&ctx, usage_bucket_key("alpha"), bucket_row)
        .await
        .unwrap();
    assert_eq!((alpha_row.objects, alpha_row.versions), (1, 1));
    assert_eq!(alpha_row.logical_bytes, 7);
}

/// With a realm quota for alpha's group only, alpha's versions are walked through its prefix
/// first, and the node-wide walk after it skips alpha without counting it twice.
#[tokio::test]
async fn quota_buckets_first() {
    let temp = tempdir().unwrap();
    let ctx = context(temp.path().to_str().unwrap());
    let plain_stored = fixture(&ctx).await;
    let realm_id = RealmId::from_bytes([5u8; 32]);
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.quota = QuotaConfig {
        default_quota_bytes: Some(1 << 30),
        group_overrides: vec![GroupQuotaOverride {
            group_id: GROUP_B,
            quota_bytes: None,
            grace_factor_percent: None,
        }],
        ..QuotaConfig::default()
    };
    let key = DocumentTarget::RealmConfig { realm_id }
        .storage_key()
        .to_vec();
    write(
        &ctx,
        REALM_CONFIG_KEYSPACE,
        key,
        postcard::to_allocvec(&config).unwrap(),
    )
    .await;

    let operation = RebuildStatsOperation::new().with_realm(realm_id);
    let (output, prefixes) = run(operation, &ctx, None).await;
    let total = output.unwrap();

    let alpha = VersionKey::bucket_prefix("alpha").unwrap();
    assert_eq!(prefixes.first(), Some(&Some(Key::from(alpha))));
    assert!(prefixes[1..].iter().all(Option::is_none));
    assert_eq!(total.objects, 3);
    assert_eq!(total.stored_bytes, plain_stored + 130);
    assert_eq!(total.logical_bytes, 419);
    let alpha = read(&ctx, usage_group_key(GROUP_A), counters)
        .await
        .unwrap();
    assert_eq!((alpha.objects, alpha.logical_bytes), (2, 249));
}
