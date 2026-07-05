use std::collections::{BTreeMap, HashMap};

use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    Backend, BackendConfig, BucketInfo, RealmId, UsageCounters, usage_global_shard_keys,
    usage_group_key,
};
use aruna_core::types::NodeId;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::delete_bucket::DeleteBucketOperation;
use aruna_operations::s3::put_object::{
    PutObjectConfig, PutObjectInput, PutObjectOperation, PutObjectResult,
};
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
        created_by: UserId::local(Ulid::new(), realm_id),
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
    let group_id = Ulid::new();

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
    let group_id = Ulid::new();
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
    let group_id = Ulid::new();
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
    let group_id = Ulid::new();
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
