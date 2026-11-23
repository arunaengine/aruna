//! Tests the paged usage rebuild against real storage: totals, exactness under writes made
//! during the walk, and quota buckets first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE,
};
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
    BucketCounters, BucketDelta, USAGE_PROGRESS_KEY, USAGE_REBUILD_KEY, USAGE_SOURCE_KEYSPACES,
    UsageCounters, UsageDelta, global_group_key, usage_bucket_key, usage_group_key,
};
use aruna_core::types::{GroupId, Key, TxnId};
use tempfile::tempdir;
use ulid::Ulid;

use super::{RebuildStatsError, RebuildStatsOperation};
use crate::s3::rebuild_counts::CounterDeltas;
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::testing::{memory_context, path_context, test_context};
use aruna_storage::storage::handle::StorageHandle;

const GROUP_A: GroupId = Ulid::from_parts(1, 1);
const GROUP_B: GroupId = Ulid::from_parts(2, 2);
/// Delete markers in beta, more than one walk page.
const MARKERS: usize = 300;

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
    let copy = crate::s3::abe_copies::copy_row(Ulid::generate(), &copy_key).unwrap();
    write(ctx, ABE_COPY_KEYSPACE, copy, vec![3; 70]).await;
    for index in 0..MARKERS {
        let marker = BlobVersion::deleted(now, Default::default());
        version(ctx, "beta", &format!("marker-{index:04}"), marker).await;
    }
    plain.stored_size()
}

/// What the watching storage does and saw: live deltas it commits right after the rebuild
/// began, a stop after `pages` page commits, the version pages and reads outside the sources.
#[derive(Default)]
struct Watch {
    live: Option<CounterDeltas>,
    pages: Option<usize>,
    commits: usize,
    view: Option<TxnId>,
    prefixes: Vec<Option<Key>>,
    starts: Vec<bool>,
    foreign: Vec<String>,
}

/// A storage that passes every effect to `real` and lets `watch` see it first.
fn watched(real: StorageHandle, watch: Arc<Mutex<Watch>>) -> StorageHandle {
    let (handle, receivers) = StorageHandle::new();
    for receiver in [receivers.foreground, receivers.bulk] {
        let (real, watch) = (real.clone(), watch.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            while let Ok((effect, response, ..)) = receiver.recv() {
                response.send(runtime.block_on(pass(&real, &watch, effect)));
            }
        });
    }
    handle
}

/// Once the stop is reached, everything but aborts is refused, as a node that went down.
async fn pass(real: &StorageHandle, watch: &Mutex<Watch>, effect: StorageEffect) -> StorageEvent {
    {
        let mut watch = watch.lock().unwrap();
        if watch.pages.is_some_and(|pages| watch.commits >= pages)
            && !matches!(effect, StorageEffect::AbortTransaction { .. })
        {
            let error = StorageError::PersistError("the node stopped".to_string());
            return StorageEvent::Error { error };
        }
        note(&mut watch, &effect);
    }
    let begin = matches!(effect, StorageEffect::BeginUsageRebuild { .. });
    let Event::Storage(event) = real.send_storage_effect(effect).await else {
        panic!("storage answers with storage events");
    };
    let live = {
        let mut watch = watch.lock().unwrap();
        if let (true, StorageEvent::TransactionStarted { txn_id }) = (begin, &event) {
            watch.view = Some(*txn_id);
        }
        let committed = matches!(event, StorageEvent::TransactionCommitted { .. });
        watch.commits += usize::from(committed);
        watch.live.take().filter(|_| begin)
    };
    if let Some((deltas, buckets)) = live {
        commit_live(real, deltas, buckets).await;
    }
    event
}

/// Notes the version pages, and reads in the snapshot of keyspaces the rebuild must not read.
fn note(watch: &mut Watch, effect: &StorageEffect) {
    let spaces: Vec<&str> = match effect {
        StorageEffect::Read {
            key_space, txn_id, ..
        }
        | StorageEffect::Iter {
            key_space, txn_id, ..
        } if *txn_id == watch.view => vec![key_space.as_str()],
        StorageEffect::BatchRead { reads, txn_id } if *txn_id == watch.view => reads
            .iter()
            .map(|(key_space, _)| key_space.as_str())
            .collect(),
        _ => Vec::new(),
    };
    let kept =
        |space: &str| USAGE_SOURCE_KEYSPACES.contains(&space) || space == USAGE_STATS_KEYSPACE;
    let foreign = spaces.into_iter().filter(|space| !kept(space));
    watch.foreign.extend(foreign.map(str::to_string));
    if let StorageEffect::Iter {
        key_space,
        prefix,
        start,
        ..
    } = effect
        && key_space == BLOB_VERSIONS_KEYSPACE
    {
        watch.prefixes.push(prefix.clone());
        watch.starts.push(start.is_some());
    }
}

/// Runs `operation` through the runner on a storage that `watch` watches.
async fn watch_run(
    operation: RebuildStatsOperation,
    ctx: &DriverContext,
    watch: Watch,
) -> (Result<UsageCounters, RebuildStatsError>, Watch) {
    let shared = Arc::new(Mutex::new(watch));
    let storage = watched(ctx.storage_handle.clone(), shared.clone());
    let output = drive(operation, &test_context(storage)).await;
    let watch = std::mem::take(&mut *shared.lock().unwrap());
    assert!(watch.foreign.is_empty(), "read {:?}", watch.foreign);
    (output, watch)
}

/// Runs `operation` to its end; `live` deltas commit right after the rebuild began.
async fn run(
    operation: RebuildStatsOperation,
    ctx: &DriverContext,
    live: Option<CounterDeltas>,
) -> (Result<UsageCounters, RebuildStatsError>, Vec<Option<Key>>) {
    let watch = Watch {
        live,
        ..Watch::default()
    };
    let (output, watch) = watch_run(operation, ctx, watch).await;
    (output, watch.prefixes)
}

async fn commit_live(
    storage: &StorageHandle,
    deltas: Vec<(Key, UsageDelta)>,
    buckets: Vec<(Key, BucketDelta)>,
) {
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
    let ctx = memory_context();
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
    let ctx = memory_context();
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
    let ctx = memory_context();
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

/// Runs a rebuild until it committed `pages` page transactions, then stops it as a crash
/// would: its open transactions end without a finish. Returns its outcome with whether each
/// version page started after a saved position.
async fn interrupt(
    operation: RebuildStatsOperation,
    ctx: &DriverContext,
    pages: usize,
) -> (Result<UsageCounters, RebuildStatsError>, Vec<bool>) {
    let watch = Watch {
        pages: Some(pages),
        ..Watch::default()
    };
    let (output, watch) = watch_run(operation, ctx, watch).await;
    (output, watch.starts)
}

/// Commits version rows with their heads and the deltas a writer books for them at once.
async fn commit_versions(
    ctx: &DriverContext,
    versions: Vec<(&str, &str, BlobVersion)>,
    live: CounterDeltas,
) {
    let storage = &ctx.storage_handle;
    let start = StorageEffect::StartTransaction { read: false };
    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) =
        storage.send_storage_effect(start).await
    else {
        panic!("no write transaction");
    };
    let mut writes = Vec::new();
    for (bucket, key, version) in versions {
        let version_key = VersionKey::new(bucket, key, Ulid::generate());
        let pointer = CurrentVersionPointer::new(version_key.version_id);
        let head = BlobHeadKey::new(bucket, key).to_bytes().unwrap();
        writes.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            Key::from(version_key.to_bytes().unwrap()),
            Key::from(version.to_bytes().unwrap()),
        ));
        writes.push((
            BLOB_HEAD_KEYSPACE.to_string(),
            Key::from(head),
            Key::from(pointer.to_bytes().unwrap()),
        ));
    }
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    };
    storage.send_storage_effect(write).await;
    let (deltas, buckets) = live;
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

fn markers(count: i128) -> BucketDelta {
    BucketDelta {
        delete_markers: count,
        ..Default::default()
    }
}

/// A rebuild stopped after a page resumes there after a restart, without walking from the
/// start, and counts writes made around the stop exactly once: one transaction with rows on
/// both sides of the saved position, and one after the restart before the resume.
#[tokio::test]
async fn resumes_after_restart() {
    let temp = tempdir().unwrap();
    let root = temp.path().to_str().unwrap();
    let ctx = path_context(root);
    fixture(&ctx).await;
    let (stopped, starts) = interrupt(RebuildStatsOperation::new(), &ctx, 2).await;
    assert!(stopped.is_err(), "the stopped rebuild did not finish");
    assert_eq!(starts, vec![false], "stopped after the first version page");
    assert!(
        read(&ctx, USAGE_PROGRESS_KEY.to_vec(), |_| ())
            .await
            .is_some()
    );

    let now = SystemTime::UNIX_EPOCH;
    let marker = || BlobVersion::deleted(now, Default::default());
    // The first and last keys of both buckets: whichever bucket the walk reads first, its rows
    // are behind the saved position and the other bucket's last row is ahead of it.
    let ends = vec![
        ("alpha", "a-1", marker()),
        ("alpha", "zz-1", marker()),
        ("beta", "a-1", marker()),
        ("beta", "zz-1", marker()),
    ];
    let rows = vec![
        (Key::from(usage_bucket_key("alpha")), markers(2)),
        (Key::from(usage_bucket_key("beta")), markers(2)),
    ];
    commit_versions(&ctx, ends, (Vec::new(), rows)).await;
    let DriverContext { storage_handle, .. } = ctx;
    storage_handle.close().await;

    let ctx = path_context(root);
    let object = UsageDelta {
        objects: 1,
        logical_bytes: 100,
        ..Default::default()
    };
    let row = BucketDelta {
        objects: 1,
        versions: 1,
        logical_bytes: 100,
        ..Default::default()
    };
    let live = (
        vec![
            (Key::from(usage_group_key(GROUP_A)), object),
            (Key::from(global_group_key(GROUP_A)), object),
        ],
        vec![(Key::from(usage_bucket_key("alpha")), row)],
    );
    let added = vec![("alpha", "ab-new", materialized([1u8; 32]))];
    commit_versions(&ctx, added, live).await;

    let (resumed, starts) = interrupt(RebuildStatsOperation::new(), &ctx, usize::MAX).await;
    // The walk totals count the rows as the rebuild began; later writes reach the counters.
    assert_eq!(resumed.unwrap().objects, 3);
    assert!(
        starts.iter().all(|start| *start),
        "the walk resumed after the saved page"
    );
    let alpha_row = read(&ctx, usage_bucket_key("alpha"), bucket_row).await;
    let expected = BucketCounters {
        objects: 3,
        versions: 3,
        delete_markers: 2,
        logical_bytes: 349,
        ..Default::default()
    };
    assert_eq!(alpha_row, Some(expected));
    let beta_row = read(&ctx, usage_bucket_key("beta"), bucket_row)
        .await
        .unwrap();
    assert_eq!(beta_row.delete_markers, MARKERS as u64 + 2);
    let alpha = read(&ctx, usage_group_key(GROUP_A), counters)
        .await
        .unwrap();
    assert_eq!((alpha.objects, alpha.logical_bytes), (3, 349));
    assert!(
        read(&ctx, USAGE_PROGRESS_KEY.to_vec(), |_| ())
            .await
            .is_none()
    );

    // A clean rebuild of the same rows books the same counters.
    let (output, _) = run(RebuildStatsOperation::new(), &ctx, None).await;
    assert_eq!(output.unwrap().objects, 4);
    let again = read(&ctx, usage_bucket_key("alpha"), bucket_row).await;
    assert_eq!(again, Some(expected));
    let beta_again = read(&ctx, usage_bucket_key("beta"), bucket_row).await;
    assert_eq!(beta_again, Some(beta_row));
    let alpha_again = read(&ctx, usage_group_key(GROUP_A), counters).await;
    assert_eq!(alpha_again, Some(alpha));
}
