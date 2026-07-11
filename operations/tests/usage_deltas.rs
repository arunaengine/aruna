use std::collections::{BTreeMap, HashMap};

use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{USAGE_NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    Backend, BackendConfig, BucketInfo, GroupQuotaOverride, MultipartChecksumType,
    NodeUsageSnapshot, QuotaConfig, RealmId, UsageCounters, node_usage_group_key,
    usage_global_shard_keys, usage_group_key,
};
use aruna_core::types::NodeId;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::abort_multipart_upload::{
    AbortMultipartUploadInput, AbortMultipartUploadOperation,
};
use aruna_operations::s3::complete_multipart_upload::{
    CompleteMultipartPart, CompleteMultipartUploadError, CompleteMultipartUploadInput,
    CompleteMultipartUploadOperation, CompleteMultipartUploadResult,
};
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::create_multipart_upload::{
    CreateMultipartUploadInput, CreateMultipartUploadOperation,
};
use aruna_operations::s3::delete_bucket::DeleteBucketOperation;
use aruna_operations::s3::delete_object::{
    DeleteObjectInput, DeleteObjectOperation, DeleteObjectResult,
};
use aruna_operations::s3::put_object::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use aruna_operations::s3::upload_part::{UploadPartInput, UploadPartOperation, UploadPartResult};
use aruna_operations::usage_stats::RebuildUsageStatsOperation;
use aruna_storage::storage;
use tempfile::TempDir;
use ulid::Ulid;

struct Harness {
    _temp_dir: TempDir,
    driver: DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    created_by: UserId,
}

async fn setup() -> Harness {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap();
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();

    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root,
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("uploaded-parts".to_string()),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
    )
    .await
    .unwrap();

    let realm_id = RealmId::from_bytes([7u8; 32]);
    let node_id = net_handle.node_id();
    Harness {
        _temp_dir: temp_dir,
        driver: DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
        },
        realm_id,
        node_id,
        created_by: UserId::local(Ulid::r#gen(), realm_id),
    }
}

fn stream_from_bytes(bytes: &[u8]) -> BackendStream<Result<bytes::Bytes, StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes.to_vec(),
    )))
}

async fn create_bucket(h: &Harness, bucket: &str, group_id: Ulid) {
    drive(
        CreateBucketOperation::new(
            bucket.to_string(),
            BucketInfo {
                group_id,
                created_at: std::time::SystemTime::now(),
                created_by: h.created_by,
                cors_configuration: None,
            },
        ),
        &h.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
}

async fn put_object(
    h: &Harness,
    bucket: &str,
    key: &str,
    group_id: Ulid,
    data: &[u8],
) -> PutObjectResult {
    drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: h.created_by,
            group_id,
            realm_id: h.realm_id,
            node_id: h.node_id,
            request: PutObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content_length: Some(data.len() as u64),
                body: Some(stream_from_bytes(data)),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            quota_ceiling: None,
        }),
        &h.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

async fn delete_object(
    h: &Harness,
    bucket: &str,
    key: &str,
    group_id: Ulid,
    version_id: Option<Ulid>,
) -> DeleteObjectResult {
    drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            group_id,
            realm_id: h.realm_id,
            node_id: h.node_id,
            deleted_by: h.created_by,
        }),
        &h.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

async fn create_upload(h: &Harness, bucket: &str, key: &str, group_id: Ulid) -> Ulid {
    drive(
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id,
            created_by: h.created_by,
            checksum_hint: None,
        }),
        &h.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
    .record
    .upload_id
}

async fn upload_part(
    h: &Harness,
    bucket: &str,
    key: &str,
    upload_id: Ulid,
    part_number: u16,
    bytes: &[u8],
) -> UploadPartResult {
    drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id,
            part_number,
            content_length: Some(bytes.len() as u64),
            body: Some(stream_from_bytes(bytes)),
            created_by: h.created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &h.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

async fn complete_upload(
    h: &Harness,
    bucket: &str,
    key: &str,
    upload_id: Ulid,
    parts: &[UploadPartResult],
    object_size: u64,
) -> CompleteMultipartUploadResult {
    drive(
        CompleteMultipartUploadOperation::new(CompleteMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id,
            realm_id: h.realm_id,
            node_id: h.node_id,
            completed_parts: parts
                .iter()
                .enumerate()
                .map(|(idx, part)| CompleteMultipartPart {
                    part_number: (idx + 1) as u16,
                    etag: Some(hex::encode(part.location.hashes.get("md5").unwrap())),
                    expected_checksums: vec![],
                })
                .collect(),
            expected_checksums: vec![],
            checksum_type: MultipartChecksumType::FullObject,
            object_size: Some(object_size),
            created_by: h.created_by,
            quota_ceiling: None,
        }),
        &h.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

async fn read_all_usage_stats(ctx: &DriverContext) -> Vec<(Vec<u8>, UsageCounters)> {
    let Event::Storage(StorageEvent::IterResult { values, .. }) = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 4096,
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected iter event");
    };
    values
        .into_iter()
        .map(|(key, value)| {
            (
                key.to_vec(),
                UsageCounters::from_bytes(value.as_ref()).unwrap(),
            )
        })
        .collect()
}

/// Effective, reader-visible counters: the summed global total across all 64
/// shards, plus every non-zero per-group counter. This is the semantically
/// meaningful projection both `LoadUsageCountersOperation` and the realm summary
/// read; per-shard placement of `stored_*` differs between the incremental write
/// paths (group shard) and a rebuild (shard 0), but the summed total is identical.
async fn effective_usage(ctx: &DriverContext) -> (UsageCounters, BTreeMap<Vec<u8>, UsageCounters>) {
    let mut global = UsageCounters::default();
    let mut groups = BTreeMap::new();
    for (key, counters) in read_all_usage_stats(ctx).await {
        if key.starts_with(b"global/") {
            global.add(&counters).unwrap();
        } else if key.starts_with(b"group/") && counters != UsageCounters::default() {
            groups.insert(key, counters);
        }
    }
    (global, groups)
}

async fn read_group(ctx: &DriverContext, group_id: Ulid) -> UsageCounters {
    for (key, counters) in read_all_usage_stats(ctx).await {
        if key == usage_group_key(group_id) {
            return counters;
        }
    }
    UsageCounters::default()
}

async fn read_global(ctx: &DriverContext) -> UsageCounters {
    let shard_keys: std::collections::HashSet<Vec<u8>> =
        usage_global_shard_keys().into_iter().collect();
    let mut global = UsageCounters::default();
    for (key, counters) in read_all_usage_stats(ctx).await {
        if shard_keys.contains(&key) {
            global.add(&counters).unwrap();
        }
    }
    global
}

/// The rebuild-as-oracle assertion: recomputing all counters from a full keyspace
/// scan must leave the reader-visible counters byte-identical to what the
/// incremental bookkeeping produced.
async fn assert_matches_rebuild(ctx: &DriverContext) {
    let before = effective_usage(ctx).await;
    drive(RebuildUsageStatsOperation::new(), ctx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = effective_usage(ctx).await;
    assert_eq!(
        before, after,
        "incremental counters diverged from rebuilt truth: before={before:?} after={after:?}"
    );
}

#[tokio::test]
async fn create_and_delete_bucket_delta() {
    let h = setup().await;
    let group_id = Ulid::r#gen();

    create_bucket(&h, "counted", group_id).await;
    assert_eq!(read_global(&h.driver).await.buckets, 1);
    assert_eq!(read_group(&h.driver, group_id).await.buckets, 1);
    assert_matches_rebuild(&h.driver).await;

    // delete_bucket refuses non-empty buckets, so only empty deletion is drivable.
    drive(DeleteBucketOperation::new("counted".to_string()), &h.driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(read_global(&h.driver).await.buckets, 0);
    assert_eq!(read_group(&h.driver, group_id).await.buckets, 0);
    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn simple_put_object_delta() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    let data = b"hello world";
    put_object(&h, "bucket", "fresh.txt", group_id, data).await;

    let global = read_global(&h.driver).await;
    assert_eq!(global.buckets, 1);
    assert_eq!(global.objects, 1);
    assert_eq!(global.stored_blobs, 1);
    assert_eq!(global.stored_bytes, data.len() as u64);
    assert_eq!(global.logical_bytes, data.len() as u64);

    let group = read_group(&h.driver, group_id).await;
    assert_eq!(group.buckets, 1);
    assert_eq!(group.objects, 1);
    // Per-group counter tracks logical usage only; stored_* live on the global total.
    assert_eq!(group.stored_blobs, 0);
    assert_eq!(group.stored_bytes, 0);
    assert_eq!(group.logical_bytes, data.len() as u64);

    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn overwrite_put_accumulates_logical_bytes_per_version() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    let first = b"first";
    let second = b"second-longer";
    put_object(&h, "bucket", "same.txt", group_id, first).await;
    put_object(&h, "bucket", "same.txt", group_id, second).await;

    // Overwrite semantics are accumulate-per-version, not replace: the object
    // count stays at 1 (the head moves), but every materialized version's bytes
    // keep counting logically and each distinct blob is stored.
    let global = read_global(&h.driver).await;
    assert_eq!(global.objects, 1);
    assert_eq!(global.stored_blobs, 2);
    assert_eq!(global.stored_bytes, (first.len() + second.len()) as u64);
    assert_eq!(global.logical_bytes, (first.len() + second.len()) as u64);

    let group = read_group(&h.driver, group_id).await;
    assert_eq!(group.objects, 1);
    assert_eq!(group.logical_bytes, (first.len() + second.len()) as u64);

    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn overwrite_put_same_content_double_counts_logical_bytes() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    let data = b"identical";
    put_object(&h, "bucket", "same.txt", group_id, data).await;
    put_object(&h, "bucket", "same.txt", group_id, data).await;

    // The blob deduplicates physically (one stored blob) but each materialized
    // version still counts logically, so logical_bytes is charged twice.
    let global = read_global(&h.driver).await;
    assert_eq!(global.objects, 1);
    assert_eq!(global.stored_blobs, 1);
    assert_eq!(global.stored_bytes, data.len() as u64);
    assert_eq!(global.logical_bytes, 2 * data.len() as u64);

    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn delete_marker_drops_object_but_keeps_logical_bytes() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    let data = b"payload";
    put_object(&h, "bucket", "obj.txt", group_id, data).await;

    // A versioned delete (version_id: None) writes a delete marker over the live
    // head: the object stops being live but its materialized version and stored
    // blob remain, so only the object count drops.
    let result = delete_object(&h, "bucket", "obj.txt", group_id, None).await;
    assert!(result.delete_marker);

    let global = read_global(&h.driver).await;
    assert_eq!(global.objects, 0);
    assert_eq!(global.stored_blobs, 1);
    assert_eq!(global.stored_bytes, data.len() as u64);
    assert_eq!(global.logical_bytes, data.len() as u64);

    let group = read_group(&h.driver, group_id).await;
    assert_eq!(group.objects, 0);
    assert_eq!(group.logical_bytes, data.len() as u64);

    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn put_over_delete_marker_revives_object() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    let first = b"first-body";
    let second = b"second-body-2";
    put_object(&h, "bucket", "obj.txt", group_id, first).await;
    delete_object(&h, "bucket", "obj.txt", group_id, None).await;
    put_object(&h, "bucket", "obj.txt", group_id, second).await;

    // The key is live again. Both materialized versions still count logically and
    // both distinct blobs are stored; the delete marker contributes nothing.
    let global = read_global(&h.driver).await;
    assert_eq!(global.objects, 1);
    assert_eq!(global.stored_blobs, 2);
    assert_eq!(global.stored_bytes, (first.len() + second.len()) as u64);
    assert_eq!(global.logical_bytes, (first.len() + second.len()) as u64);

    let group = read_group(&h.driver, group_id).await;
    assert_eq!(group.objects, 1);
    assert_eq!(group.logical_bytes, (first.len() + second.len()) as u64);

    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn permanent_delete_of_live_version_frees_logical_bytes() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    let data = b"payload";
    let put = put_object(&h, "bucket", "obj.txt", group_id, data).await;

    // A permanent delete by version id removes the only (live) version: the
    // object and its logical bytes are freed. The content-addressed blob is left
    // in place, so stored_* are unchanged and the rebuild still counts them.
    delete_object(&h, "bucket", "obj.txt", group_id, Some(put.version_id)).await;

    let global = read_global(&h.driver).await;
    assert_eq!(global.objects, 0);
    assert_eq!(global.logical_bytes, 0);
    assert_eq!(global.stored_blobs, 1);
    assert_eq!(global.stored_bytes, data.len() as u64);

    let group = read_group(&h.driver, group_id).await;
    assert_eq!(group.objects, 0);
    assert_eq!(group.logical_bytes, 0);

    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn multipart_staging_counts_nothing_until_completion() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    let part1 = b"hello ";
    let part2 = b"world!!";
    let upload_id = create_upload(&h, "bucket", "big.bin", group_id).await;
    upload_part(&h, "bucket", "big.bin", upload_id, 1, part1).await;
    upload_part(&h, "bucket", "big.bin", upload_id, 2, part2).await;

    // Staged parts live outside the content-addressed blob keyspace: an initiated
    // but never-completed upload contributes nothing to the counters, and the
    // rebuild (which scans only completed blobs) agrees.
    let staged = read_global(&h.driver).await;
    assert_eq!(staged.objects, 0);
    assert_eq!(staged.stored_blobs, 0);
    assert_eq!(staged.stored_bytes, 0);
    assert_eq!(staged.logical_bytes, 0);
    assert_matches_rebuild(&h.driver).await;

    // Completion charges the object exactly once, for the assembled blob.
    let part1_result = upload_part(&h, "bucket", "big.bin", upload_id, 1, part1).await;
    let part2_result = upload_part(&h, "bucket", "big.bin", upload_id, 2, part2).await;
    let size = (part1.len() + part2.len()) as u64;
    complete_upload(
        &h,
        "bucket",
        "big.bin",
        upload_id,
        &[part1_result, part2_result],
        size,
    )
    .await;

    let global = read_global(&h.driver).await;
    assert_eq!(global.objects, 1);
    assert_eq!(global.stored_blobs, 1);
    assert_eq!(global.stored_bytes, size);
    assert_eq!(global.logical_bytes, size);

    let group = read_group(&h.driver, group_id).await;
    assert_eq!(group.objects, 1);
    assert_eq!(group.logical_bytes, size);

    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn aborted_multipart_upload_leaves_counters_untouched() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    let upload_id = create_upload(&h, "bucket", "abort.bin", group_id).await;
    upload_part(&h, "bucket", "abort.bin", upload_id, 1, b"discard me").await;

    drive(
        AbortMultipartUploadOperation::new(AbortMultipartUploadInput {
            bucket: "bucket".to_string(),
            key: "abort.bin".to_string(),
            upload_id,
        }),
        &h.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    // Abort touches no counters; only the bucket remains accounted for.
    let global = read_global(&h.driver).await;
    assert_eq!(global.buckets, 1);
    assert_eq!(global.objects, 0);
    assert_eq!(global.stored_blobs, 0);
    assert_eq!(global.stored_bytes, 0);
    assert_eq!(global.logical_bytes, 0);

    assert_matches_rebuild(&h.driver).await;
}

// ---------------------------------------------------------------------------
// Quota gate (finding 3): hard rejection when a positive logical_bytes delta
// would push a group's realm-wide logical_bytes above the resolved ceiling.
// ---------------------------------------------------------------------------

#[allow(clippy::result_large_err)]
async fn try_put_object(
    h: &Harness,
    bucket: &str,
    key: &str,
    group_id: Ulid,
    data: &[u8],
    quota_ceiling: Option<u64>,
) -> Result<PutObjectResult, PutObjectError> {
    drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: h.created_by,
            group_id,
            realm_id: h.realm_id,
            node_id: h.node_id,
            request: PutObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content_length: Some(data.len() as u64),
                body: Some(stream_from_bytes(data)),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            quota_ceiling,
        }),
        &h.driver,
    )
    .await
    .map(|result| {
        result
            .expect("put object output")
            .expect("put object inner")
    })
}

fn remote_node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// Injects a remote node's per-group snapshot so the gate reads a realm-wide
/// total larger than the live local counters.
async fn inject_remote_group_snapshot(
    h: &Harness,
    group_id: Ulid,
    node_id: NodeId,
    logical_bytes: u64,
) {
    let snapshot = NodeUsageSnapshot {
        node_id,
        counters: UsageCounters {
            logical_bytes,
            ..Default::default()
        },
    };
    match h
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
            key: node_usage_group_key(group_id, node_id).into(),
            value: snapshot.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected snapshot write event: {other:?}"),
    }
}

#[tokio::test]
async fn put_object_gate_allows_under_and_at_ceiling_rejects_over() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;
    let ceiling = Some(20);

    // Under the ceiling: 0 + 10 <= 20.
    try_put_object(&h, "bucket", "a.bin", group_id, &[1u8; 10], ceiling)
        .await
        .expect("under ceiling passes");
    // Exactly at the ceiling: 10 + 10 == 20.
    try_put_object(&h, "bucket", "b.bin", group_id, &[2u8; 10], ceiling)
        .await
        .expect("at ceiling passes");
    assert_eq!(read_group(&h.driver, group_id).await.logical_bytes, 20);

    // One byte over the ceiling: 20 + 1 > 20.
    let error = try_put_object(&h, "bucket", "c.bin", group_id, &[3u8; 1], ceiling)
        .await
        .expect_err("one byte over ceiling is rejected");
    assert!(matches!(
        error,
        PutObjectError::QuotaExceeded {
            limit: 20,
            usage: 21
        }
    ));

    // The rejected write left the counters and the blob store untouched.
    assert_eq!(read_group(&h.driver, group_id).await.logical_bytes, 20);
    assert_matches_rebuild(&h.driver).await;
}

#[tokio::test]
async fn put_object_gate_unlimited_when_ceiling_is_none() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    // None => unlimited, no gate regardless of size.
    try_put_object(&h, "bucket", "big.bin", group_id, &[7u8; 4096], None)
        .await
        .expect("unlimited group is never gated");
    assert_eq!(read_group(&h.driver, group_id).await.logical_bytes, 4096,);
}

#[tokio::test]
async fn put_object_gate_honors_grace_headroom() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    // quota 100 with 110% grace => ceiling 110. Writes above the quota but under
    // the ceiling are allowed; the grace band is the budget for remote staleness.
    let quota = QuotaConfig {
        default_group_quota_bytes: Some(100),
        grace_factor_percent: 110,
        ..QuotaConfig::default()
    };
    let ceiling = quota.effective_group_ceiling(&group_id);
    assert_eq!(ceiling, Some(110));

    // 105 bytes: over the 100 quota, still within the 110 ceiling.
    try_put_object(&h, "bucket", "grace.bin", group_id, &[1u8; 105], ceiling)
        .await
        .expect("write inside grace band passes");
    // 6 more bytes: 105 + 6 = 111 > 110 ceiling.
    let error = try_put_object(&h, "bucket", "over.bin", group_id, &[2u8; 6], ceiling)
        .await
        .expect_err("write past the grace ceiling is rejected");
    assert!(matches!(
        error,
        PutObjectError::QuotaExceeded { limit: 110, .. }
    ));
}

#[tokio::test]
async fn put_object_gate_group_override_takes_precedence_over_default() {
    let h = setup().await;
    let overridden = Ulid::r#gen();
    let plain = Ulid::r#gen();
    create_bucket(&h, "over-bucket", overridden).await;
    create_bucket(&h, "plain-bucket", plain).await;

    let quota = QuotaConfig {
        default_group_quota_bytes: Some(50),
        grace_factor_percent: 100,
        group_overrides: vec![GroupQuotaOverride {
            group_id: overridden,
            quota_bytes: Some(200),
            grace_factor_percent: Some(100),
        }],
        ..QuotaConfig::default()
    };

    // Overridden group: 100 bytes fits under its 200 ceiling.
    let over_ceiling = quota.effective_group_ceiling(&overridden);
    assert_eq!(over_ceiling, Some(200));
    try_put_object(
        &h,
        "over-bucket",
        "ok.bin",
        overridden,
        &[1u8; 100],
        over_ceiling,
    )
    .await
    .expect("override quota permits the write");

    // Plain group: the same 100 bytes exceeds the default 50 ceiling.
    let plain_ceiling = quota.effective_group_ceiling(&plain);
    assert_eq!(plain_ceiling, Some(50));
    let error = try_put_object(
        &h,
        "plain-bucket",
        "no.bin",
        plain,
        &[1u8; 100],
        plain_ceiling,
    )
    .await
    .expect_err("default quota rejects the write");
    assert!(matches!(
        error,
        PutObjectError::QuotaExceeded { limit: 50, .. }
    ));
}

#[tokio::test]
async fn put_object_gate_counts_remote_node_snapshots() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;
    let ceiling = Some(100);

    // A remote node already reports 80 logical bytes for this group.
    inject_remote_group_snapshot(&h, group_id, remote_node(9), 80).await;

    // 0 (local) + 80 (remote) + 20 == 100 ceiling: still allowed.
    try_put_object(&h, "bucket", "ok.bin", group_id, &[1u8; 20], ceiling)
        .await
        .expect("realm-wide total at ceiling passes");

    // Now local is 20, remote 80; +1 byte => 101 > 100. The remote snapshot is
    // what tips the group over, proving the gate sums the realm-wide basis.
    let error = try_put_object(&h, "bucket", "over.bin", group_id, &[2u8; 1], ceiling)
        .await
        .expect_err("remote snapshot pushes the group over the ceiling");
    assert!(matches!(
        error,
        PutObjectError::QuotaExceeded {
            limit: 100,
            usage: 101
        }
    ));
}

#[tokio::test]
async fn delete_object_is_never_gated_by_quota() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;

    // Fill the group exactly to a tight ceiling.
    let put = try_put_object(&h, "bucket", "big.bin", group_id, &[1u8; 50], Some(50))
        .await
        .expect("at-ceiling write passes");

    // Deletes carry no ceiling and must always proceed, even at/over quota.
    delete_object(&h, "bucket", "big.bin", group_id, Some(put.version_id)).await;
    assert_eq!(read_group(&h.driver, group_id).await.logical_bytes, 0);
    assert_matches_rebuild(&h.driver).await;
}

#[allow(clippy::result_large_err)]
async fn try_complete_multipart(
    h: &Harness,
    bucket: &str,
    key: &str,
    group_id: Ulid,
    data: &[u8],
    quota_ceiling: Option<u64>,
) -> Result<CompleteMultipartUploadResult, CompleteMultipartUploadError> {
    let upload_id = create_upload(h, bucket, key, group_id).await;
    let part = upload_part(h, bucket, key, upload_id, 1, data).await;
    drive(
        CompleteMultipartUploadOperation::new(CompleteMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id,
            realm_id: h.realm_id,
            node_id: h.node_id,
            completed_parts: vec![CompleteMultipartPart {
                part_number: 1,
                etag: Some(hex::encode(part.location.hashes.get("md5").unwrap())),
                expected_checksums: vec![],
            }],
            expected_checksums: vec![],
            checksum_type: MultipartChecksumType::FullObject,
            object_size: Some(data.len() as u64),
            created_by: h.created_by,
            quota_ceiling,
        }),
        &h.driver,
    )
    .await
    .map(|result| result.expect("complete output").expect("complete inner"))
}

#[tokio::test]
async fn multipart_completion_is_gated_like_put() {
    let h = setup().await;
    let group_id = Ulid::r#gen();
    create_bucket(&h, "bucket", group_id).await;
    let ceiling = Some(30);

    // 25 bytes under the 30 ceiling: completes.
    try_complete_multipart(&h, "bucket", "ok.bin", group_id, &[1u8; 25], ceiling)
        .await
        .expect("multipart under ceiling completes");
    assert_eq!(read_group(&h.driver, group_id).await.logical_bytes, 25);

    // 10 more bytes: 25 + 10 = 35 > 30 ceiling: rejected.
    let error = try_complete_multipart(&h, "bucket", "over.bin", group_id, &[2u8; 10], ceiling)
        .await
        .expect_err("multipart over ceiling is rejected");
    assert!(matches!(
        error,
        CompleteMultipartUploadError::QuotaExceeded {
            limit: 30,
            usage: 35
        }
    ));

    // The rejected completion left the group counter unchanged.
    assert_eq!(read_group(&h.driver, group_id).await.logical_bytes, 25);
    assert_matches_rebuild(&h.driver).await;
}
