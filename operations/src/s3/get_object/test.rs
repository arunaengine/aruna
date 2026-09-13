use crate::driver::{DriverContext, drive};
use crate::node::usage_stats::UsageCounterUpdate;
use crate::replication::bao_read::BaoReadError;
use crate::replication::protocol::{BaoReadRefusal, ReferenceAdvance};
use crate::replication::queue::LiveReplicationObligationRecord;
use crate::s3::get_object::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectState, HolderFailures,
    MAX_AUTO_ADVANCES, MAX_DRIFT_ADVANCE_ATTEMPTS, MIN_ADVANCE_INTERVAL, ObjectRangeRequest,
    RoutedRead, get_object_routed, routed_info,
};
use aruna_blob::blob::BlobHandler;
use aruna_blob::hash::Hasher;
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect, StorageEffect};
use aruna_core::egress::EgressPolicy;
use aruna_core::events::SubOperationEvent;
use aruna_core::events::{Event, StagingSourceEvent, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::{HASH_MD5, HASH_SHA256};
use aruna_core::structs::{
    Backend, BackendConfig, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
    BlobVersionState, CurrentVersionPointer, MultipartChecksumType, MultipartObjectSummary,
    PathRestriction, Permission, PortableSourceDescriptor, RealmId, ResolvedSourceAccess,
    SourceConnectorKind, SourceMetadata, StagingStrategy, UsageDelta, VersionKey,
    VersionSourceBinding, usage_group_key,
};
use aruna_net::{NetConfig, NetHandle};
use aruna_storage::storage;
use axum::{Router, routing::get};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tempfile::tempdir;
use tokio::net::TcpListener;
use ulid::Ulid;

fn test_node_id() -> aruna_core::NodeId {
    iroh::SecretKey::from_bytes(&[7; 32]).public()
}

async fn spawn_reference_server(body: &'static [u8]) -> String {
    let app = Router::new().route(
        "/folder/file.txt",
        get(move || async move { ([("content-type", "text/plain"), ("etag", "etag-123")], body) }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{}", addr)
}

// A holder-backed read with no serving holder keeps infrastructure failure
// and integrity failure distinct from genuine absence.
#[test]
fn failure_causes_preserved() {
    let mut unavailable = HolderFailures::default();
    unavailable.record(BaoReadError::Refused(BaoReadRefusal::BackendFailure));
    assert_eq!(unavailable.into_error(), GetObjectError::HoldersUnavailable);

    let mut integrity = HolderFailures::default();
    integrity.record(BaoReadError::Refused(BaoReadRefusal::HashMismatch));
    assert_eq!(
        integrity.into_error(),
        GetObjectError::HolderIntegrityFailure
    );

    let mut denied = HolderFailures::default();
    denied.record(BaoReadError::Refused(BaoReadRefusal::ReadDenied));
    assert_eq!(denied.into_error(), GetObjectError::HolderAccessDenied);

    let mut governed = HolderFailures::default();
    governed.record(BaoReadError::GovernedUnavailable);
    assert_eq!(governed.into_error(), GetObjectError::GovernedUnavailable);

    let mut absent = HolderFailures::default();
    absent.record(BaoReadError::Refused(BaoReadRefusal::NotFound));
    assert_eq!(absent.into_error(), GetObjectError::NoSuchKey);

    let mut mixed = HolderFailures::default();
    mixed.record(BaoReadError::Refused(BaoReadRefusal::NotFound));
    mixed.record(BaoReadError::Refused(BaoReadRefusal::BackendFailure));
    assert_eq!(mixed.into_error(), GetObjectError::HoldersUnavailable);

    let metadata_only = HolderFailures {
        contacted: true,
        metadata_only: true,
        ..HolderFailures::default()
    };
    assert_eq!(
        metadata_only.into_error(),
        GetObjectError::HoldersUnavailable
    );

    assert_eq!(
        HolderFailures::default().into_error(),
        GetObjectError::HoldersUnavailable
    );
}

#[test]
fn failure_priority_stable() {
    let cases = [
        (
            vec![
                BaoReadError::Refused(BaoReadRefusal::BackendFailure),
                BaoReadError::Refused(BaoReadRefusal::ReadDenied),
            ],
            GetObjectError::HolderAccessDenied,
        ),
        (
            vec![
                BaoReadError::Refused(BaoReadRefusal::ReadDenied),
                BaoReadError::Refused(BaoReadRefusal::HashMismatch),
            ],
            GetObjectError::HolderIntegrityFailure,
        ),
        (
            vec![
                BaoReadError::Refused(BaoReadRefusal::HashMismatch),
                BaoReadError::GovernedUnavailable,
            ],
            GetObjectError::GovernedUnavailable,
        ),
    ];

    for (errors, expected) in cases {
        let mut failures = HolderFailures::default();
        for error in errors {
            failures.record(error);
        }
        assert_eq!(failures.into_error(), expected);
    }
}

#[test]
fn transient_then_success() {
    let mut failures = HolderFailures::default();
    assert!(
        failures
            .consider(Err(BaoReadError::Refused(BaoReadRefusal::BackendFailure)))
            .is_none()
    );
    let served = failures.consider(Ok(crate::replication::bao_read::BaoReadOutput::Stream {
        blob: BackendStream::new(stream::iter(Vec::<Result<Bytes, std::io::Error>>::new())),
        size: 0,
        blake3: [9u8; 32],
        etag: None,
        hashes: HashMap::new(),
    }));

    assert!(matches!(
        served,
        Some(crate::replication::bao_read::BaoReadOutput::Stream { .. })
    ));
}

#[test]
fn routed_facts_preserved() {
    let version_created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(12);
    let read = RoutedRead {
        user_id: UserId::nil(RealmId::from_bytes([3u8; 32])),
        blake3: [9u8; 32],
        version_id: Some(Ulid::from(7u128)),
        metadata: HashMap::new(),
        version_created_at: Some(version_created_at),
        source_policies: Vec::new(),
        restrictions: None,
    };
    let hashes = HashMap::from([
        (HASH_MD5.to_string(), vec![4u8; 16]),
        (HASH_SHA256.to_string(), vec![5u8; 32]),
    ]);
    let composite_hashes = HashMap::from([(HASH_SHA256.to_string(), vec![6u8; 32])]);
    let summary = MultipartObjectSummary {
        checksum_type: MultipartChecksumType::Composite,
        part_count: 3,
        composite_hashes: composite_hashes.clone(),
    };

    let etag = Some(hex::encode(&hashes[HASH_MD5]));
    let info = routed_info(&read, 42, etag.clone(), hashes.clone(), Some(summary));

    assert_eq!(info.size, 42);
    assert_eq!(info.version_created_at, Some(version_created_at));
    assert_eq!(info.etag, etag);
    assert_eq!(info.hashes, hashes);
    assert_eq!(info.checksum_type, MultipartChecksumType::Composite);
    assert_eq!(info.composite_hashes, composite_hashes);
    assert_eq!(info.part_count, Some(3));
}

#[test]
fn routed_empty_preserved() {
    let read = RoutedRead {
        user_id: UserId::nil(RealmId::from_bytes([3u8; 32])),
        blake3: [9u8; 32],
        version_id: Some(Ulid::from(7u128)),
        metadata: HashMap::new(),
        version_created_at: Some(SystemTime::UNIX_EPOCH),
        source_policies: Vec::new(),
        restrictions: None,
    };
    let hashes = HashMap::from([(HASH_MD5.to_string(), vec![0u8; 16])]);

    let etag = Some(hex::encode(&hashes[HASH_MD5]));
    let info = routed_info(&read, 0, etag.clone(), hashes.clone(), None);

    assert_eq!(info.size, 0);
    assert_eq!(info.etag, etag);
    assert_eq!(info.hashes, hashes);
    assert_eq!(info.checksum_type, MultipartChecksumType::FullObject);
    assert_eq!(info.part_count, None);
}

// A version this node knows without its bytes must name the blob a routed
// read would fetch, not read as a corrupt local record.
#[test]
fn reports_missing_blob() {
    let blake3 = [9u8; 32];
    let version_id = Ulid::generate();
    let mut operation = GetObjectOperation::new(GetObjectInput {
        bucket: "bucket".to_string(),
        key: "file.txt".to_string(),
        version_id: Some(version_id),
        range: None,
        group_id: Ulid::generate(),
        user_identity: UserId::nil(RealmId::from_bytes([3u8; 32])),
        node_id: test_node_id(),
    });
    operation.txn_id = Some(Ulid::generate());
    let version = BlobVersion::materialized(
        blake3,
        BackendRef::node_default(),
        SystemTime::UNIX_EPOCH,
        operation.input.user_identity,
        None,
    )
    .with_metadata(HashMap::from([("k".to_string(), "v".to_string())]));
    operation.read_version(version_id, version, true);

    operation.step(Event::Storage(StorageEvent::ReadResult {
        key: Vec::<u8>::new().into(),
        value: None,
    }));

    assert!(matches!(
        operation.output,
        Some(Err(GetObjectError::BlobNotLocal {
            blake3: hash,
            version_id: Some(seen),
            ..
        })) if hash == blake3 && seen == version_id
    ));
}

#[test]
fn explicit_range_resolves() {
    let resolved = ObjectRangeRequest::StartEnd { start: 2, end: 5 }
        .resolve(10)
        .unwrap();

    assert_eq!(resolved.range, 2..6);
    assert_eq!(resolved.content_length, 4);
    assert_eq!(resolved.content_range, "bytes 2-5/10");
}

#[test]
fn suffix_range_resolves() {
    let resolved = ObjectRangeRequest::Suffix { length: 3 }
        .resolve(10)
        .unwrap();

    assert_eq!(resolved.range, 7..10);
    assert_eq!(resolved.content_length, 3);
    assert_eq!(resolved.content_range, "bytes 7-9/10");
}

#[test]
fn open_range_resolves() {
    let resolved = ObjectRangeRequest::Start { start: 4 }.resolve(10).unwrap();

    assert_eq!(resolved.range, 4..10);
    assert_eq!(resolved.content_length, 6);
    assert_eq!(resolved.content_range, "bytes 4-9/10");
}

#[test]
fn invalid_ranges_rejected() {
    assert_eq!(
        ObjectRangeRequest::Suffix { length: 1 }.resolve(0),
        Err(GetObjectError::InvalidRange)
    );
    assert_eq!(
        ObjectRangeRequest::Start { start: 10 }.resolve(10),
        Err(GetObjectError::InvalidRange)
    );
    assert_eq!(
        ObjectRangeRequest::StartEnd { start: 6, end: 5 }.resolve(10),
        Err(GetObjectError::InvalidRange)
    );
}

#[test]
fn materialized_range_reads() {
    let mut operation = GetObjectOperation::new(GetObjectInput {
        bucket: "s3test".to_string(),
        key: "range.txt".to_string(),
        version_id: None,
        range: Some(ObjectRangeRequest::StartEnd { start: 2, end: 4 }),
        group_id: Ulid::generate(),
        user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
        node_id: test_node_id(),
    });
    let txn_id = Ulid::generate();
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/tmp".to_string(),
        storage_bucket: "aruna_test".to_string(),
        backend_path: "s3test/range.txt".to_string(),
        ulid: Ulid::generate(),
        compressed: false,
        encrypted: false,
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 10,
        hashes: HashMap::new(),
    };
    operation.txn_id = Some(txn_id);
    operation.location = Some(location.clone());

    let effects = operation.read_blob();

    assert!(matches!(
        effects.as_slice(),
        [
            Effect::Storage(StorageEffect::CommitTransaction { txn_id: committed_txn_id }),
            Effect::Blob(BlobEffect::ReadRange { location: emitted_location, range })
        ] if *committed_txn_id == txn_id && emitted_location == &location && range == &(2..5)
    ));
}

#[test]
fn reference_range_reads() {
    let mut operation = GetObjectOperation::new(GetObjectInput {
        bucket: "s3test".to_string(),
        key: "range.txt".to_string(),
        version_id: None,
        range: Some(ObjectRangeRequest::Suffix { length: 4 }),
        group_id: Ulid::generate(),
        user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
        node_id: test_node_id(),
    });
    let txn_id = Ulid::generate();
    let access = ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::Http,
        config: HashMap::new(),
        path: "folder/file.txt".to_string(),
        version: None,
    };
    operation.txn_id = Some(txn_id);
    operation.reference_access = Some(access.clone());

    let effects = operation.read_reference();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::CommitTransaction { txn_id: committed_txn_id })]
            if *committed_txn_id == txn_id
    ));

    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    assert!(matches!(
        effects.as_slice(),
        [Effect::StagingSource(StagingSourceEffect::Head { access: emitted_access })]
            if emitted_access == &access
    ));

    let metadata = SourceMetadata {
        content_length: 10,
        content_type: Some("text/plain".to_string()),
        etag: None,
        last_modified: None,
        source_version: None,
    };
    // Baseline matches the live head, so this undrifted read serves directly.
    operation.reference_cached = Some(metadata.clone());
    let effects = operation.step(Event::StagingSource(StagingSourceEvent::HeadResult {
        metadata: metadata.clone(),
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::StagingSource(StagingSourceEffect::Read { access: emitted_access, range })]
            if emitted_access == &access && range == &Some(6..10)
    ));

    let effects = operation.step(Event::StagingSource(StagingSourceEvent::ReadResult {
        metadata,
        stream: aruna_core::stream::BackendStream::new(stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"ence")),
        ])),
    }));
    assert!(effects.is_empty());
    let result = operation.finalize().unwrap();
    let resolved_range = result.resolved_range.unwrap();
    assert_eq!(resolved_range.range, 6..10);
    assert_eq!(resolved_range.content_length, 4);
    assert_eq!(resolved_range.content_range, "bytes 6-9/10");
}

#[test]
fn range_drift_restarts() {
    let mut operation = GetObjectOperation::new(GetObjectInput {
        bucket: "s3test".to_string(),
        key: "range.txt".to_string(),
        version_id: None,
        range: Some(ObjectRangeRequest::StartEnd { start: 1, end: 3 }),
        group_id: Ulid::generate(),
        user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
        node_id: test_node_id(),
    });
    operation.source_metadata = Some(SourceMetadata {
        content_length: 10,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
    });
    operation.resolved_range = Some(
        ObjectRangeRequest::StartEnd { start: 1, end: 3 }
            .resolve(10)
            .unwrap(),
    );
    operation.state = GetObjectState::ReadReferenceSource;

    let effects = operation.step(Event::StagingSource(StagingSourceEvent::ReadResult {
        metadata: SourceMetadata {
            content_length: 11,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
        },
        stream: aruna_core::stream::BackendStream::new(stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"bad")),
        ])),
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    ));
    assert_eq!(operation.state, GetObjectState::StartTransaction);
}

#[tokio::test]
pub async fn test_get_object() {
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::with_egress(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100000),
            multipart_bucket: Some("multipart".to_string()),
            root: temp_root.to_string(),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();
    let content = "Hello, World!";
    let hasher = Hasher::new_with_bytes(content.as_bytes());
    let hashes = hasher.finalize();
    let blake3_hash: [u8; 32] = hashes.blake3.into();

    let bucket = "s3test".to_string();
    let key = "test.txt".to_string();
    let blob_ulid = Ulid::generate();
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: temp_root.to_string(),
        storage_bucket: format!("aruna_{}", Ulid::generate()),
        backend_path: format!("{bucket}/{key}_{blob_ulid}"),
        ulid: blob_ulid,
        compressed: false,
        encrypted: false,
        created_by: Default::default(),
        created_at: SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: content.len() as u64,
        hashes: hasher.to_map(),
    };

    // Write file + db entries
    std::fs::create_dir_all(
        Path::new(&location.get_full_path().unwrap())
            .parent()
            .unwrap(),
    )
    .unwrap();
    std::fs::write(location.get_full_path().unwrap(), content).unwrap();

    if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::new(blake3_hash, location.backend.clone())
                    .to_bytes()
                    .into(),
                value: location.clone().to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;

        let version_id = Ulid::generate();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(&bucket, &key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, &key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::materialized(
                    blake3_hash,
                    BackendRef::node_default(),
                    location.created_at,
                    location.created_by,
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
    } else {
        panic!("Failed to start transaction");
    }

    // Read file with operation
    let driver_ctx = DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let operation = GetObjectOperation::new(GetObjectInput {
        bucket,
        key,
        version_id: None,
        range: None,
        group_id: Ulid::generate(),
        user_identity: Default::default(),
        node_id: test_node_id(),
    });

    let blob_result = drive(operation, &driver_ctx).await.unwrap();
    assert_eq!(
        blob_result.location.as_ref().unwrap().hashes,
        location.hashes
    );
    assert!(blob_result.source_metadata.is_none());
    assert!(blob_result.source_binding.is_none());
    assert!(blob_result.last_refresh.is_none());
    assert_eq!(
        blob_result.info.checksum_type,
        MultipartChecksumType::FullObject
    );
    let info = &blob_result.info;
    assert_eq!(info.size, content.len() as u64);
    assert_eq!(info.etag, location.hashes.get(HASH_MD5).map(hex::encode));
    assert_eq!(info.version_created_at, Some(location.created_at));
    assert_eq!(info.hashes, location.hashes);
    let mut blob_stream = blob_result.blob;
    let mut read_buffer = Vec::new();
    while let Some(Ok(bytes)) = blob_stream.next().await {
        read_buffer.extend_from_slice(&bytes);
    }
    assert_eq!(read_buffer, content.as_bytes());
}

#[tokio::test]
async fn routed_missing_blob() {
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();

    let bucket = "s3test".to_string();
    let key = "missing.txt".to_string();
    let version_id = Ulid::generate();
    let user_identity = UserId::nil(RealmId::from_bytes([3u8; 32]));

    if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(&bucket, &key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, &key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::materialized(
                    [5u8; 32],
                    BackendRef::node_default(),
                    SystemTime::UNIX_EPOCH,
                    user_identity,
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
    } else {
        panic!("Failed to start transaction");
    }

    let driver_ctx = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let read = get_object_routed(
        &driver_ctx,
        GetObjectInput {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: None,
            range: None,
            group_id: Ulid::generate(),
            user_identity,
            node_id: test_node_id(),
        },
        None,
    )
    .await;
    assert!(matches!(read, Err(GetObjectError::GetObjectFailed)));

    let ranged = get_object_routed(
        &driver_ctx,
        GetObjectInput {
            bucket,
            key,
            version_id: None,
            range: Some(ObjectRangeRequest::StartEnd { start: 0, end: 1 }),
            group_id: Ulid::generate(),
            user_identity,
            node_id: test_node_id(),
        },
        None,
    )
    .await;
    assert!(matches!(ranged, Err(GetObjectError::GetObjectFailed)));
}

#[tokio::test]
pub async fn hash_mismatch_rejected() {
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::with_egress(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100000),
            multipart_bucket: Some("multipart".to_string()),
            root: temp_root.to_string(),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();
    let content = "Hello, World!";
    let tampered = "Hallo, World!";
    let hasher = Hasher::new_with_bytes(content.as_bytes());
    let hashes = hasher.finalize();
    let blake3_hash: [u8; 32] = hashes.blake3.into();

    let bucket = "s3test".to_string();
    let key = "test.txt".to_string();
    let blob_ulid = Ulid::generate();
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: temp_root.to_string(),
        storage_bucket: format!("aruna_{}", Ulid::generate()),
        backend_path: format!("{bucket}/{key}_{blob_ulid}"),
        ulid: blob_ulid,
        compressed: false,
        encrypted: false,
        created_by: Default::default(),
        created_at: SystemTime::now(),
        staging: false,
        partial: false,
        blob_size: content.len() as u64,
        hashes: hasher.to_map(),
    };

    std::fs::create_dir_all(
        Path::new(&location.get_full_path().unwrap())
            .parent()
            .unwrap(),
    )
    .unwrap();
    std::fs::write(location.get_full_path().unwrap(), tampered).unwrap();

    if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::new(blake3_hash, location.backend.clone())
                    .to_bytes()
                    .into(),
                value: location.clone().to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;

        let version_id = Ulid::generate();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(&bucket, &key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, &key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::materialized(
                    blake3_hash,
                    BackendRef::node_default(),
                    location.created_at,
                    location.created_by,
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
    } else {
        panic!("Failed to start transaction");
    }

    let driver_ctx = DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let operation = GetObjectOperation::new(GetObjectInput {
        bucket,
        key,
        version_id: None,
        range: None,
        group_id: Ulid::generate(),
        user_identity: Default::default(),
        node_id: test_node_id(),
    });

    let mut blob_stream = drive(operation, &driver_ctx).await.unwrap().blob;
    let mut read_buffer = Vec::new();
    let mut read_error = None;
    while let Some(result) = blob_stream.next().await {
        match result {
            Ok(bytes) => read_buffer.extend_from_slice(&bytes),
            Err(err) => {
                read_error = Some(err.to_string());
                break;
            }
        }
    }

    assert_eq!(read_buffer, tampered.as_bytes());
    assert!(read_error.is_some());
    assert!(read_error.unwrap().contains("Integrity check failed"));
}

#[tokio::test]
async fn bound_connector_used() {
    let endpoint = spawn_reference_server(b"hello reference").await;
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::with_egress(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100000),
            multipart_bucket: Some("multipart".to_string()),
            root: temp_root.to_string(),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();

    let driver_ctx = DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let bucket = "s3test".to_string();
    let key = "test.txt".to_string();
    let version_id = Ulid::generate();
    let connector_id = Ulid::generate();
    let cached_metadata = SourceMetadata {
        content_length: 15,
        content_type: Some("text/plain".to_string()),
        etag: Some("etag-123".to_string()),
        last_modified: Some(SystemTime::UNIX_EPOCH),
        source_version: None,
    };
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
        },
        connector_id: Some(connector_id),
    };

    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        panic!("Failed to start transaction");
    };

    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new(&bucket, &key).to_bytes().unwrap().into(),
            value: CurrentVersionPointer::new(version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: Some(txn_id),
        })
        .await;

    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new(&bucket, &key, version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: BlobVersion::reference(
                source,
                cached_metadata,
                SystemTime::UNIX_EPOCH,
                Default::default(),
                SystemTime::UNIX_EPOCH,
            )
            .to_bytes()
            .unwrap()
            .into(),
            txn_id: Some(txn_id),
        })
        .await;

    let _ = storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await;

    let result = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket,
            key,
            version_id: None,
            range: None,
            group_id: Ulid::generate(),
            user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
            node_id: test_node_id(),
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    assert!(result.location.is_none());
    assert_eq!(
        result
            .source_metadata
            .as_ref()
            .and_then(|m| m.content_type.clone()),
        Some("text/plain".to_string())
    );
    assert_eq!(
        result
            .source_binding
            .as_ref()
            .map(|binding| binding.strategy.clone()),
        Some(StagingStrategy::Reference)
    );
    assert!(result.last_refresh.is_some());
    let info = &result.info;
    assert_eq!(info.size, 15);
    assert_eq!(info.etag.as_deref(), Some("etag-123"));
    assert_eq!(info.version_created_at, None);
    assert!(info.hashes.is_empty());
    let mut stream = result.blob;
    let mut read_buffer = Vec::new();
    while let Some(Ok(bytes)) = stream.next().await {
        read_buffer.extend_from_slice(&bytes);
    }
    assert_eq!(read_buffer, b"hello reference");

    let Event::Storage(StorageEvent::ReadResult { value, .. }) = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new("s3test", "test.txt", version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing version metadata");
    };
    let metadata = BlobVersion::from_bytes(value.unwrap().as_ref()).unwrap();
    let BlobVersionState::Reference {
        cached_metadata,
        last_refresh,
        ..
    } = metadata.state
    else {
        panic!("expected reference metadata");
    };
    assert_eq!(cached_metadata.content_type.as_deref(), Some("text/plain"));
    assert_eq!(last_refresh, SystemTime::UNIX_EPOCH);
}

// A drifted current-version read records a same-binding successor
// (spec REQ-S3-DATA-MODEL-001); the prior version stays immutable.
#[tokio::test]
async fn drift_creates_successor() {
    let endpoint = spawn_reference_server(b"hello reference").await;
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::with_egress(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100000),
            multipart_bucket: Some("multipart".to_string()),
            root: temp_root.to_string(),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();

    let driver_ctx = DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let version_id = Ulid::generate();
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
        },
        connector_id: Some(Ulid::generate()),
    };

    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        panic!("Failed to start transaction");
    };

    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new("s3test", "refresh.txt")
                .to_bytes()
                .unwrap()
                .into(),
            value: CurrentVersionPointer::new(version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: Some(txn_id),
        })
        .await;
    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new("s3test", "refresh.txt", version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: BlobVersion::reference(
                source,
                SourceMetadata {
                    content_length: 1,
                    content_type: Some("application/octet-stream".to_string()),
                    etag: Some("stale-etag".to_string()),
                    last_modified: None,
                    source_version: None,
                },
                SystemTime::UNIX_EPOCH,
                Default::default(),
                SystemTime::UNIX_EPOCH,
            )
            .to_bytes()
            .unwrap()
            .into(),
            txn_id: Some(txn_id),
        })
        .await;
    let _ = storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await;

    let group_id = Ulid::generate();
    let result = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: "s3test".to_string(),
            key: "refresh.txt".to_string(),
            version_id: None,
            range: None,
            group_id,
            user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
            node_id: test_node_id(),
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    let mut stream = result.blob;
    assert!(result.last_refresh.is_some());
    assert_eq!(
        result
            .source_metadata
            .as_ref()
            .map(|metadata| metadata.content_length),
        Some(15)
    );
    assert_eq!(
        result
            .source_metadata
            .as_ref()
            .and_then(|metadata| metadata.content_type.as_deref()),
        Some("text/plain")
    );
    while let Some(Ok(_)) = stream.next().await {}

    // The head now points at a fresh successor, not the drifted version.
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: BlobHeadKey::new("s3test", "refresh.txt")
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing head pointer");
    };
    let successor_id = CurrentVersionPointer::from_bytes(value.unwrap().as_ref())
        .unwrap()
        .version_id;
    assert_ne!(
        successor_id, version_id,
        "a successor must have been created"
    );

    let successor = read_reference_version(&driver_ctx, successor_id).await;
    assert_eq!(successor.content_length, 15);
    assert_eq!(successor.etag.as_deref(), Some("etag-123"));
    assert_eq!(successor.content_type.as_deref(), Some("text/plain"));

    // The superseded version is untouched: successors, never mutation.
    let original = read_reference_version(&driver_ctx, version_id).await;
    assert_eq!(original.content_length, 1);
    assert_eq!(original.etag.as_deref(), Some("stale-etag"));

    // Both versions stay stored, so the successor is charged its full size.
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: aruna_core::keyspaces::USAGE_STATS_KEYSPACE.to_string(),
            key: usage_group_key(group_id).into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing usage counters");
    };
    let counters = aruna_core::structs::UsageCounters::from_bytes(value.unwrap().as_ref()).unwrap();
    assert_eq!(counters.referenced_bytes, 15);

    // The successor's obligation is committed with it, so replication and
    // the repair scanner can both find it.
    let Event::Storage(StorageEvent::IterResult { values, .. }) = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 16,
            txn_id: None,
        })
        .await
    else {
        panic!("missing obligation listing");
    };
    let obligations: Vec<_> = values
        .iter()
        .map(|(_, value)| LiveReplicationObligationRecord::from_bytes(value.as_ref()).unwrap())
        .collect();
    assert_eq!(obligations.len(), 1);
    assert_eq!(obligations[0].version_id, successor_id);
    assert_eq!(obligations[0].bucket, "s3test");
}

fn observation(content_length: u64) -> SourceMetadata {
    SourceMetadata {
        content_length,
        content_type: None,
        etag: Some(format!("etag-{content_length}")),
        last_modified: None,
        source_version: None,
    }
}

fn reference_source() -> VersionSourceBinding {
    VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::new(),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
        },
        connector_id: Some(Ulid::generate()),
    }
}

/// A current-version read whose head observation already drifted.
fn drifted_operation() -> GetObjectOperation {
    let mut operation = GetObjectOperation::new(GetObjectInput {
        bucket: "s3test".to_string(),
        key: "range.txt".to_string(),
        version_id: None,
        range: None,
        group_id: Ulid::generate(),
        user_identity: UserId::local(Ulid::from_parts(2, 2), RealmId([0u8; 32])),
        node_id: test_node_id(),
    });
    let version = BlobVersion::reference(
        reference_source(),
        observation(4),
        SystemTime::UNIX_EPOCH,
        UserId::local(Ulid::from_parts(1, 1), RealmId([0u8; 32])),
        SystemTime::UNIX_EPOCH,
    )
    .with_metadata(HashMap::from([(
        "label".to_string(),
        "preserved".to_string(),
    )]));
    let _ = operation.read_version(Ulid::generate(), version, false);
    operation.advance_observation = Some(observation(9));
    operation.reference_access = Some(ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::Http,
        config: HashMap::new(),
        path: "folder/file.txt".to_string(),
        version: None,
    });
    operation
}

fn head_read(value: Ulid) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: BlobHeadKey::new("s3test", "range.txt")
            .to_bytes()
            .unwrap()
            .into(),
        value: Some(CurrentVersionPointer::new(value).to_bytes().unwrap().into()),
    })
}

/// The stored current reference the advance transaction rereads.
fn current_version(created_at: SystemTime, advance_count: u16) -> Event {
    let version = BlobVersion::reference(
        reference_source(),
        observation(4),
        created_at,
        UserId::local(Ulid::from_parts(1, 1), RealmId([0u8; 32])),
        created_at,
    )
    .with_metadata(HashMap::from([(
        "label".to_string(),
        "preserved".to_string(),
    )]))
    .with_advance_count(advance_count);
    Event::Storage(StorageEvent::ReadResult {
        key: b"version".to_vec().into(),
        value: Some(version.to_bytes().unwrap().into()),
    })
}

/// Drives a drifted read through the head CAS up to the version reread.
fn advance_to_reread(operation: &mut GetObjectOperation, headed: Ulid) -> Ulid {
    let txn_id = Ulid::generate();
    operation.resolved_version_id = Some(headed);
    operation.txn_id = Some(txn_id);
    operation.state = GetObjectState::ReadHeadForAdvance;
    let effects = operation.step(head_read(headed));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Read { .. })]
    ));
    assert_eq!(operation.state, GetObjectState::ReadCurrentForAdvance);
    txn_id
}

fn written_successor(effects: &[Effect]) -> BlobVersion {
    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects else {
        panic!("expected a single batch write, got {effects:?}")
    };
    BlobVersion::from_bytes(writes[0].2.as_ref()).unwrap()
}

fn source_read(metadata: SourceMetadata) -> Event {
    Event::StagingSource(StagingSourceEvent::ReadResult {
        metadata,
        stream: aruna_core::stream::BackendStream::new(stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"body")),
        ])),
    })
}

// CAS: only the reader whose head still matches may write the successor;
// a concurrent winner makes this read abort and restart.
#[test]
fn advance_conflict_restarts() {
    let mut operation = drifted_operation();
    let txn_id = Ulid::generate();
    operation.resolved_version_id = Some(Ulid::generate());
    operation.txn_id = Some(txn_id);
    operation.state = GetObjectState::ReadHeadForAdvance;

    // The head already names a different (winning) successor.
    let effects = operation.step(head_read(Ulid::generate()));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
            if *aborted == txn_id
    ));
    assert_eq!(operation.state, GetObjectState::RestartReference);
}

// The successor's replication obligation must be committed with it, or peers
// never learn of it and the repair scanner cannot discover it.
#[test]
fn advance_writes_obligation() {
    let restrictions = vec![PathRestriction {
        pattern: "/realm/g/group/data/node/s3test/range.txt".to_string(),
        permission: Permission::READ,
    }];
    let mut operation = drifted_operation().with_restrictions(Some(restrictions));
    let creator = operation.reference_creator.unwrap();
    let reader_auth = operation.auth_context();
    let headed = Ulid::generate();
    advance_to_reread(&mut operation, headed);

    let effects = operation.step(current_version(SystemTime::UNIX_EPOCH, 0));

    let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
        panic!("expected a single batch write, got {effects:?}")
    };
    let [
        (_, _, version_value),
        (_, _, head_value),
        (key_space, _, obligation_value),
    ] = writes.as_slice()
    else {
        panic!("expected version, head and obligation writes, got {writes:?}")
    };
    let successor = BlobVersion::from_bytes(version_value.as_ref()).unwrap();
    assert_eq!(successor.created_by, creator);
    assert_ne!(successor.created_by, operation.input.user_identity);
    assert_eq!(successor.metadata, operation.metadata);
    let next_pointer = CurrentVersionPointer::from_bytes(head_value.as_ref()).unwrap();
    assert_eq!(key_space, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE);
    let record = LiveReplicationObligationRecord::from_bytes(obligation_value.as_ref()).unwrap();
    assert_eq!(Some(record.version_id), operation.resolved_version_id);
    assert_ne!(record.version_id, headed);
    assert_eq!(next_pointer.version_id, record.version_id);
    assert_eq!(next_pointer.generation, 2);
    assert_eq!(record.local_node_id, test_node_id());
    assert!(!record.delete_marker);
    assert_eq!(record.auth_context, reader_auth);
    assert_eq!(
        record.reference_advance,
        Some(ReferenceAdvance {
            generation: next_pointer.generation,
            predecessor: headed,
        })
    );
}

// The cooldown is measured against the current version's immutable
// `created_at`, and the exact interval is already old enough to advance.
#[test]
fn advance_at_boundary() {
    let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let mut operation = drifted_operation();
    operation.now_override = Some(created_at + MIN_ADVANCE_INTERVAL);
    advance_to_reread(&mut operation, Ulid::generate());

    let effects = operation.step(current_version(created_at, 7));

    let successor = written_successor(effects.as_slice());
    assert_eq!(successor.created_at, created_at + MIN_ADVANCE_INTERVAL);
    assert_eq!(successor.advance_count(), Some(8));
}

// One second inside the cooldown fails closed and releases the transaction
// instead of minting another durable successor.
#[test]
fn advance_rejects_early() {
    let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let mut operation = drifted_operation();
    operation.now_override = Some(created_at + MIN_ADVANCE_INTERVAL - Duration::from_secs(1));
    let txn_id = advance_to_reread(&mut operation, Ulid::generate());

    let effects = operation.step(current_version(created_at, 0));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
            if *aborted == txn_id
    ));
    assert_eq!(
        operation.finalize(),
        Err(GetObjectError::ReferenceSourceChanged)
    );
}

// A backwards clock must not grant an unbounded advance budget.
#[test]
fn advance_rejects_rollback() {
    let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    let mut operation = drifted_operation();
    operation.now_override = Some(created_at - Duration::from_secs(3_600));
    advance_to_reread(&mut operation, Ulid::generate());

    let effects = operation.step(current_version(created_at, 0));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { .. })]
    ));
    assert_eq!(
        operation.finalize(),
        Err(GetObjectError::ReferenceSourceChanged)
    );
}

// Count 99 may still mint the hundredth successor.
#[test]
fn advance_at_cap() {
    let created_at = SystemTime::UNIX_EPOCH;
    let mut operation = drifted_operation();
    operation.now_override = Some(created_at + MIN_ADVANCE_INTERVAL);
    advance_to_reread(&mut operation, Ulid::generate());

    let effects = operation.step(current_version(created_at, MAX_AUTO_ADVANCES - 1));

    assert_eq!(
        written_successor(effects.as_slice()).advance_count(),
        Some(MAX_AUTO_ADVANCES)
    );
}

// At the cap the read fails immediately with its own variant: no retry of the
// per-request drift attempts, and the cooldown is never consulted.
#[test]
fn advance_rejects_exhausted() {
    let created_at = SystemTime::UNIX_EPOCH;
    let mut operation = drifted_operation();
    operation.now_override = Some(created_at + Duration::from_secs(86_400));
    let txn_id = advance_to_reread(&mut operation, Ulid::generate());
    let attempts = operation.drift_attempts;

    let effects = operation.step(current_version(created_at, MAX_AUTO_ADVANCES));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
            if *aborted == txn_id
    ));
    assert_eq!(operation.drift_attempts, attempts);
    assert_eq!(
        operation.finalize(),
        Err(GetObjectError::ReferenceAdvanceExhausted)
    );
}

// A committed successor is enqueued for live replication before its bytes
// are served, the same way the normal write path enqueues after its commit.
#[test]
fn commit_queues_replication() {
    let mut operation = drifted_operation();
    let txn_id = Ulid::generate();
    operation.resolved_version_id = Some(Ulid::generate());
    operation.reference_advance = Some(ReferenceAdvance {
        generation: 2,
        predecessor: Ulid::generate(),
    });
    operation.txn_id = Some(txn_id);
    operation.state = GetObjectState::CommitAdvance;

    let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));

    assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    assert_eq!(operation.state, GetObjectState::QueueSuccessorReplication);

    // A failed enqueue leaves the durable obligation for repair and serves.
    let effects = operation.step(Event::SubOperation(
        SubOperationEvent::LiveReplicationQueued {
            result: Err("queue unavailable".to_string()),
        },
    ));

    assert!(matches!(
        effects.as_slice(),
        [Effect::StagingSource(StagingSourceEffect::Read { .. })]
    ));
}

// The superseded version stays stored and stays charged, so the successor
// adds its own full size rather than the size difference.
#[test]
fn successor_charges_size() {
    let mut operation = drifted_operation();
    let txn_id = Ulid::generate();
    operation.reference_cached = Some(observation(4));
    operation.txn_id = Some(txn_id);
    operation.state = GetObjectState::WriteSuccessor;

    operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));

    assert_eq!(operation.state, GetObjectState::UpdateReferenceUsage);
    let mut expected = UsageCounterUpdate::for_group(
        operation.input.group_id,
        UsageDelta {
            referenced_bytes: 9,
            ..Default::default()
        },
    );
    let _ = expected.start(txn_id);
    assert_eq!(operation.usage_update, Some(expected));
}

#[test]
fn drift_limit_fails() {
    let mut operation = drifted_operation();
    operation.drift_attempts = MAX_DRIFT_ADVANCE_ATTEMPTS;
    operation.state = GetObjectState::HeadReferenceSource;

    let effects = operation.step(Event::StagingSource(StagingSourceEvent::HeadResult {
        metadata: observation(9),
    }));

    assert!(effects.is_empty());
    assert_eq!(
        operation.finalize(),
        Err(GetObjectError::ReferenceSourceChanged)
    );
}

// The source can drift again between the head and the read; those bytes must
// never be served under the VersionId that promised the head observation.
#[test]
fn read_drift_restarts() {
    let mut operation = drifted_operation();
    let headed = observation(9);
    let mut read = headed.clone();
    read.etag = Some("changed-etag".to_string());
    operation.advance_observation = None;
    operation.source_metadata = Some(headed);
    operation.state = GetObjectState::ReadReferenceSource;

    let effects = operation.step(source_read(read));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    ));
    assert_eq!(operation.state, GetObjectState::StartTransaction);
    assert!(operation.advance_observation.is_none());
}

#[test]
fn historical_read_fails() {
    let mut operation = drifted_operation();
    let headed = observation(9);
    let mut read = headed.clone();
    read.content_type = Some("changed/type".to_string());
    operation.advance_observation = None;
    operation.reference_explicit = true;
    operation.source_metadata = Some(headed);
    operation.state = GetObjectState::ReadReferenceSource;

    let effects = operation.step(source_read(read));

    assert!(effects.is_empty());
    assert_eq!(
        operation.finalize(),
        Err(GetObjectError::HistoricalReferenceUnavailable)
    );
}

// A matching read is served, and the stale promise is cleared with it.
#[test]
fn read_match_serves() {
    let mut operation = drifted_operation();
    operation.source_metadata = Some(observation(9));
    operation.state = GetObjectState::ReadReferenceSource;

    let effects = operation.step(source_read(observation(9)));

    assert!(effects.is_empty());
    assert_eq!(operation.state, GetObjectState::Finish);
    assert!(operation.advance_observation.is_none());
}

// The restart must observe its own abort before re-reading the head.
#[test]
fn restart_needs_abort() {
    let mut operation = drifted_operation();
    operation.state = GetObjectState::RestartReference;

    let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
        key: b"unrelated".to_vec().into(),
    }));

    assert!(effects.is_empty());
    assert!(matches!(
        operation.finalize(),
        Err(GetObjectError::InvalidStateEvent { .. })
    ));

    let mut operation = drifted_operation();
    operation.state = GetObjectState::RestartReference;
    let effects = operation.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: Ulid::generate(),
    }));

    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    ));
}

async fn read_reference_version(ctx: &DriverContext, version_id: Ulid) -> SourceMetadata {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new("s3test", "refresh.txt", version_id)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing version metadata");
    };
    let version = BlobVersion::from_bytes(value.unwrap().as_ref()).unwrap();
    let BlobVersionState::Reference {
        cached_metadata, ..
    } = version.state
    else {
        panic!("expected reference metadata");
    };
    cached_metadata
}

// A pinned historical version whose source drifted has no cached bytes
// (#375 deferred), so it cannot serve current content.
#[tokio::test]
async fn historical_drift_fails() {
    let endpoint = spawn_reference_server(b"hello reference").await;
    let temp_handle = tempdir().unwrap();
    let temp_root = temp_handle.path().to_str().unwrap();
    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::with_egress(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100000),
            multipart_bucket: Some("multipart".to_string()),
            root: temp_root.to_string(),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        },
        storage_handle.clone(),
        net_handle.clone(),
        EgressPolicy::loopback(),
    )
    .await
    .unwrap();

    let driver_ctx = DriverContext {
        storage_handle: storage_handle.clone(),
        net_handle: Some(net_handle),
        blob_handle: Some(blob_handle),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };

    let version_id = Ulid::generate();
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
        },
        connector_id: Some(Ulid::generate()),
    };

    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        panic!("Failed to start transaction");
    };
    let _ = storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: VersionKey::new("s3test", "refresh.txt", version_id)
                .to_bytes()
                .unwrap()
                .into(),
            value: BlobVersion::reference(
                source,
                SourceMetadata {
                    content_length: 1,
                    content_type: Some("application/octet-stream".to_string()),
                    etag: Some("stale-etag".to_string()),
                    last_modified: None,
                    source_version: None,
                },
                SystemTime::UNIX_EPOCH,
                Default::default(),
                SystemTime::UNIX_EPOCH,
            )
            .to_bytes()
            .unwrap()
            .into(),
            txn_id: Some(txn_id),
        })
        .await;
    let _ = storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await;

    let error = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: "s3test".to_string(),
            key: "refresh.txt".to_string(),
            version_id: Some(version_id),
            range: None,
            group_id: Ulid::generate(),
            user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
            node_id: test_node_id(),
        }),
        &driver_ctx,
    )
    .await
    .unwrap_err();

    assert_eq!(error, GetObjectError::HistoricalReferenceUnavailable);
}
