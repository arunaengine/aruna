use aruna_blob::blob::BlobHandler;
use aruna_blob::hash::Hasher;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DHT_KEYSPACE,
    HASH_PATHS_INDEX_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
    S3_MULTIPART_UPLOAD_PART_KEYSPACE,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::{
    Backend, BackendConfig, BlobHeadKey, BlobVersion, CurrentVersionPointer, HashPathIndexKey,
    MultipartChecksumType, MultipartObjectMetadataKey, MultipartObjectPart, MultipartObjectSummary,
    MultipartUploadChecksumHint, MultipartUploadPartKey, RealmId, VersionKey,
};
use aruna_net::dht::storage::decode_entries;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::abort_multipart_upload::{
    AbortMultipartUploadInput, AbortMultipartUploadOperation,
};
use aruna_operations::s3::complete_multipart_upload::{
    CompleteMultipartPart, CompleteMultipartUploadInput, CompleteMultipartUploadOperation,
};
use aruna_operations::s3::create_multipart_upload::{
    CreateMultipartUploadInput, CreateMultipartUploadOperation,
};
use aruna_operations::s3::delete_object::{DeleteObjectInput, DeleteObjectOperation};
use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_operations::s3::upload_part::{UploadPartInput, UploadPartOperation};
use aruna_storage::storage;
use std::collections::HashMap;
use std::fs::{create_dir_all, exists, read_dir, read_to_string};
use std::path::Path;
use tempfile::TempDir;
use ulid::Ulid;

struct TestContext {
    _temp_dir: TempDir,
    blob_root: String,
    driver: DriverContext,
}

async fn setup_context() -> TestContext {
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap();
    let blob_root = format!("{temp_root}/blobstore");
    create_dir_all(&blob_root).unwrap();

    let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
    let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
        .await
        .unwrap();
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root.clone(),
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

    TestContext {
        _temp_dir: temp_dir,
        blob_root,
        driver: DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        },
    }
}

fn stream_from_bytes(
    bytes: &[u8],
) -> BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes.to_vec(),
    )))
}

async fn read_value(
    context: &DriverContext,
    keyspace: &str,
    key: Vec<u8>,
) -> Option<byteview::ByteView> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: keyspace.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage event")
    };

    value
}

fn count_blob_files(root: &str) -> usize {
    fn count_dir(path: &Path) -> usize {
        read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .map(|path| if path.is_dir() { count_dir(&path) } else { 1 })
            .sum()
    }

    count_dir(Path::new(root))
}

async fn create_upload(
    context: &TestContext,
    bucket: &str,
    key: &str,
    group_id: Ulid,
    created_by: UserId,
) -> aruna_core::structs::MultipartUpload {
    drive(
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id,
            created_by,
            checksum_hint: None,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
    .record
}

async fn upload_part_bytes(
    context: &TestContext,
    bucket: &str,
    key: &str,
    upload_id: Ulid,
    part_number: u16,
    bytes: &[u8],
    created_by: UserId,
) -> aruna_operations::s3::upload_part::UploadPartResult {
    drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id,
            part_number,
            content_length: Some(bytes.len() as u64),
            body: Some(stream_from_bytes(bytes)),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn complete_upload(
    context: &TestContext,
    bucket: &str,
    key: &str,
    upload_id: Ulid,
    realm_id: RealmId,
    node_id: aruna_core::types::NodeId,
    uploaded_parts: &[aruna_operations::s3::upload_part::UploadPartResult],
    checksum_type: MultipartChecksumType,
    object_size: Option<u64>,
    created_by: UserId,
) -> aruna_operations::s3::complete_multipart_upload::CompleteMultipartUploadResult {
    drive(
        CompleteMultipartUploadOperation::new(CompleteMultipartUploadInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id,
            realm_id,
            node_id,
            completed_parts: uploaded_parts
                .iter()
                .enumerate()
                .map(|(idx, part)| CompleteMultipartPart {
                    part_number: (idx + 1) as u16,
                    etag: Some(hex::encode(part.location.hashes.get("md5").unwrap())),
                    expected_checksums: vec![],
                })
                .collect(),
            expected_checksums: vec![],
            checksum_type,
            object_size,
            created_by,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
}

fn composite_sha256(parts: &[&[u8]]) -> Vec<u8> {
    let mut combined = Vec::new();
    for part in parts {
        combined.extend_from_slice(&Hasher::new_with_bytes(part).finalize().sha256);
    }
    Hasher::new_with_bytes(&combined).finalize().sha256.to_vec()
}

#[tokio::test]
async fn completes_multipart_upload_and_persists_object_part_metadata() {
    let context = setup_context().await;
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let created_by = UserId::local(Ulid::new(), realm_id);
    let node_id = context.driver.net_handle.as_ref().unwrap().node_id();
    let group_id = Ulid::new();
    let created = drive(
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "big.bin".to_string(),
            group_id,
            created_by,
            checksum_hint: Some(MultipartUploadChecksumHint {
                algorithm: Some(ChecksumAlgorithm::Sha256),
                checksum_type: MultipartChecksumType::Composite,
            }),
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let upload_id = created.record.upload_id;
    let part1 = b"hello ";
    let part2 = b"world";

    let uploaded_part1 = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "big.bin".to_string(),
            upload_id,
            part_number: 1,
            content_length: Some(part1.len() as u64),
            body: Some(stream_from_bytes(part1)),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let uploaded_part2 = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "big.bin".to_string(),
            upload_id,
            part_number: 2,
            content_length: Some(part2.len() as u64),
            body: Some(stream_from_bytes(part2)),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let complete = drive(
        CompleteMultipartUploadOperation::new(CompleteMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "big.bin".to_string(),
            upload_id,
            realm_id,
            node_id,
            completed_parts: vec![
                CompleteMultipartPart {
                    part_number: 1,
                    etag: Some(hex::encode(
                        uploaded_part1.location.hashes.get("md5").unwrap(),
                    )),
                    expected_checksums: vec![],
                },
                CompleteMultipartPart {
                    part_number: 2,
                    etag: Some(hex::encode(
                        uploaded_part2.location.hashes.get("md5").unwrap(),
                    )),
                    expected_checksums: vec![],
                },
            ],
            expected_checksums: vec![ExpectedChecksum {
                algorithm: ChecksumAlgorithm::Sha256,
                digest: composite_sha256(&[part1, part2]),
            }],
            checksum_type: MultipartChecksumType::Composite,
            object_size: Some((part1.len() + part2.len()) as u64),
            created_by,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert!(exists(complete.location.get_full_path().unwrap()).unwrap());
    assert_eq!(
        read_to_string(complete.location.get_full_path().unwrap()).unwrap(),
        "hello world"
    );
    assert_eq!(complete.checksum_type, MultipartChecksumType::Composite);

    let blob_hash: [u8; 32] = complete.location.get_blake3().unwrap().try_into().unwrap();
    let blob_location = read_value(&context.driver, BLOB_LOCATIONS_KEYSPACE, blob_hash.to_vec())
        .await
        .expect("missing blob location entry");
    assert_eq!(
        aruna_core::structs::BackendLocation::from_bytes(blob_location.as_ref()).unwrap(),
        complete.location.clone()
    );

    let blob_head = read_value(
        &context.driver,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new("bucket-a", "big.bin").to_bytes().unwrap(),
    )
    .await
    .expect("missing blob head entry");
    assert_eq!(
        CurrentVersionPointer::from_bytes(blob_head.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(complete.version_id, 1)
    );

    let blob_version = read_value(
        &context.driver,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new("bucket-a", "big.bin", complete.version_id)
            .to_bytes()
            .unwrap(),
    )
    .await
    .expect("missing blob version entry");
    let blob_version = BlobVersion::from_bytes(blob_version.as_ref()).unwrap();
    assert!(blob_version.is_materialized());
    assert_eq!(blob_version.blob_hash(), Some(&blob_hash));

    let hash_path = read_value(
        &context.driver,
        HASH_PATHS_INDEX_KEYSPACE,
        HashPathIndexKey::new(
            blob_hash,
            complete.version_id,
            realm_id,
            group_id,
            node_id,
            "bucket-a",
            "big.bin",
        )
        .to_bytes()
        .unwrap(),
    )
    .await
    .expect("missing hash path index entry");
    assert!(hash_path.is_empty());

    assert!(
        read_value(
            &context.driver,
            S3_MULTIPART_UPLOAD_KEYSPACE,
            upload_id.to_bytes().to_vec(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver,
            S3_MULTIPART_UPLOAD_PART_KEYSPACE,
            MultipartUploadPartKey::new(upload_id, 1)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver,
            S3_MULTIPART_UPLOAD_PART_KEYSPACE,
            MultipartUploadPartKey::new(upload_id, 2)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );

    assert!(!exists(uploaded_part1.location.get_full_path().unwrap()).unwrap());
    assert!(!exists(uploaded_part2.location.get_full_path().unwrap()).unwrap());

    let summary = MultipartObjectSummary::from_bytes(
        read_value(
            &context.driver,
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            MultipartObjectMetadataKey::summary(complete.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .unwrap()
        .as_ref(),
    )
    .unwrap();
    assert_eq!(summary.checksum_type, MultipartChecksumType::Composite);
    assert_eq!(summary.part_count, 2);

    let object_part_1 = MultipartObjectPart::from_bytes(
        read_value(
            &context.driver,
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            MultipartObjectMetadataKey::part(complete.version_id, 1)
                .to_bytes()
                .unwrap(),
        )
        .await
        .unwrap()
        .as_ref(),
    )
    .unwrap();
    assert_eq!(object_part_1.size, part1.len() as u64);

    let object_part_2 = MultipartObjectPart::from_bytes(
        read_value(
            &context.driver,
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            MultipartObjectMetadataKey::part(complete.version_id, 2)
                .to_bytes()
                .unwrap(),
        )
        .await
        .unwrap()
        .as_ref(),
    )
    .unwrap();
    assert_eq!(object_part_2.size, part2.len() as u64);

    let Event::Storage(StorageEvent::ReadResult {
        value: Some(dht_value),
        ..
    }) = context
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DHT_KEYSPACE.to_string(),
            key: complete.location.get_blake3().unwrap().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        panic!("missing DHT blob registration")
    };
    let entries = decode_entries(dht_value.as_ref());
    assert!(entries.iter().any(|entry| {
        entry.realm_id == realm_id
            && entry.value
                == context
                    .driver
                    .net_handle
                    .as_ref()
                    .unwrap()
                    .node_id()
                    .as_bytes()
                    .to_vec()
    }));
}

#[tokio::test]
async fn upload_part_overwrites_existing_part_and_cleans_old_blob() {
    let context = setup_context().await;
    let created_by = UserId::local(Ulid::new(), RealmId::from_bytes([7u8; 32]));
    let upload_id = drive(
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "overwrite.bin".to_string(),
            group_id: Ulid::new(),
            created_by,
            checksum_hint: None,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
    .record
    .upload_id;

    let first = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "overwrite.bin".to_string(),
            upload_id,
            part_number: 1,
            content_length: Some(5),
            body: Some(stream_from_bytes(b"first")),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let second = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "overwrite.bin".to_string(),
            upload_id,
            part_number: 1,
            content_length: Some(6),
            body: Some(stream_from_bytes(b"second")),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert_ne!(first.location.backend_path, second.location.backend_path);
    assert!(!exists(first.location.get_full_path().unwrap()).unwrap());
    assert!(exists(second.location.get_full_path().unwrap()).unwrap());
}

#[tokio::test]
async fn completes_multipart_upload_retains_previous_current_hash_path_index() {
    let context = setup_context().await;
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let created_by = UserId::local(Ulid::new(), realm_id);
    let group_id = Ulid::new();
    let node_id = context.driver.net_handle.as_ref().unwrap().node_id();

    let initial = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: created_by,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: "bucket-a".to_string(),
                key: "replace.bin".to_string(),
                content_length: Some(5),
                body: Some(stream_from_bytes(b"first")),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let created = drive(
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "replace.bin".to_string(),
            group_id,
            created_by,
            checksum_hint: None,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let upload_id = created.record.upload_id;
    let part1 = b"hello ";
    let part2 = b"world";

    let uploaded_part1 = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "replace.bin".to_string(),
            upload_id,
            part_number: 1,
            content_length: Some(part1.len() as u64),
            body: Some(stream_from_bytes(part1)),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let uploaded_part2 = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "replace.bin".to_string(),
            upload_id,
            part_number: 2,
            content_length: Some(part2.len() as u64),
            body: Some(stream_from_bytes(part2)),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let complete = drive(
        CompleteMultipartUploadOperation::new(CompleteMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "replace.bin".to_string(),
            upload_id,
            realm_id,
            node_id,
            completed_parts: vec![
                CompleteMultipartPart {
                    part_number: 1,
                    etag: Some(hex::encode(
                        uploaded_part1.location.hashes.get("md5").unwrap(),
                    )),
                    expected_checksums: vec![],
                },
                CompleteMultipartPart {
                    part_number: 2,
                    etag: Some(hex::encode(
                        uploaded_part2.location.hashes.get("md5").unwrap(),
                    )),
                    expected_checksums: vec![],
                },
            ],
            expected_checksums: vec![],
            checksum_type: MultipartChecksumType::FullObject,
            object_size: Some((part1.len() + part2.len()) as u64),
            created_by,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let initial_hash: [u8; 32] = initial.location.get_blake3().unwrap().try_into().unwrap();
    let complete_hash: [u8; 32] = complete.location.get_blake3().unwrap().try_into().unwrap();

    let blob_head = read_value(
        &context.driver,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new("bucket-a", "replace.bin")
            .to_bytes()
            .unwrap(),
    )
    .await
    .expect("missing blob head entry");
    assert_eq!(
        CurrentVersionPointer::from_bytes(blob_head.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(complete.version_id, 2)
    );

    // Verify that the old hash path entry still exists (historical access preserved)
    let old_hash_path = read_value(
        &context.driver,
        HASH_PATHS_INDEX_KEYSPACE,
        HashPathIndexKey::new(
            initial_hash,
            initial.version_id,
            realm_id,
            group_id,
            node_id,
            "bucket-a",
            "replace.bin",
        )
        .to_bytes()
        .unwrap(),
    )
    .await
    .expect("missing old hash path index entry");
    assert!(old_hash_path.is_empty());

    let new_hash_path = read_value(
        &context.driver,
        HASH_PATHS_INDEX_KEYSPACE,
        HashPathIndexKey::new(
            complete_hash,
            complete.version_id,
            realm_id,
            group_id,
            node_id,
            "bucket-a",
            "replace.bin",
        )
        .to_bytes()
        .unwrap(),
    )
    .await
    .expect("missing replacement hash path index entry");
    assert!(new_hash_path.is_empty());
}

#[tokio::test]
async fn multipart_completion_deduplicates_against_existing_multipart_object() {
    let context = setup_context().await;
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let created_by = UserId::local(Ulid::new(), realm_id);
    let node_id = context.driver.net_handle.as_ref().unwrap().node_id();
    let group_id = Ulid::new();
    let part1 = b"hello ";
    let part2 = b"world";

    let first_upload = create_upload(&context, "bucket-a", "first.bin", group_id, created_by).await;
    let first_part1 = upload_part_bytes(
        &context,
        "bucket-a",
        "first.bin",
        first_upload.upload_id,
        1,
        part1,
        created_by,
    )
    .await;
    let first_part2 = upload_part_bytes(
        &context,
        "bucket-a",
        "first.bin",
        first_upload.upload_id,
        2,
        part2,
        created_by,
    )
    .await;
    let first_complete = complete_upload(
        &context,
        "bucket-a",
        "first.bin",
        first_upload.upload_id,
        realm_id,
        node_id,
        &[first_part1.clone(), first_part2.clone()],
        MultipartChecksumType::FullObject,
        Some((part1.len() + part2.len()) as u64),
        created_by,
    )
    .await;

    let second_upload =
        create_upload(&context, "bucket-a", "second.bin", group_id, created_by).await;
    let second_part1 = upload_part_bytes(
        &context,
        "bucket-a",
        "second.bin",
        second_upload.upload_id,
        1,
        part1,
        created_by,
    )
    .await;
    let second_part2 = upload_part_bytes(
        &context,
        "bucket-a",
        "second.bin",
        second_upload.upload_id,
        2,
        part2,
        created_by,
    )
    .await;
    let second_complete = complete_upload(
        &context,
        "bucket-a",
        "second.bin",
        second_upload.upload_id,
        realm_id,
        node_id,
        &[second_part1.clone(), second_part2.clone()],
        MultipartChecksumType::FullObject,
        Some((part1.len() + part2.len()) as u64),
        created_by,
    )
    .await;

    assert_eq!(second_complete.location, first_complete.location);
    assert_eq!(
        read_to_string(second_complete.location.get_full_path().unwrap()).unwrap(),
        "hello world"
    );
    assert!(!exists(second_part1.location.get_full_path().unwrap()).unwrap());
    assert!(!exists(second_part2.location.get_full_path().unwrap()).unwrap());
    assert_eq!(count_blob_files(&context.blob_root), 1);

    let blob_hash: [u8; 32] = second_complete
        .location
        .get_blake3()
        .unwrap()
        .try_into()
        .unwrap();
    let blob_location = read_value(&context.driver, BLOB_LOCATIONS_KEYSPACE, blob_hash.to_vec())
        .await
        .expect("missing blob location entry");
    assert_eq!(
        aruna_core::structs::BackendLocation::from_bytes(blob_location.as_ref()).unwrap(),
        first_complete.location
    );
}

#[tokio::test]
async fn multipart_completion_deduplicates_against_existing_put_object() {
    let context = setup_context().await;
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let created_by = UserId::local(Ulid::new(), realm_id);
    let node_id = context.driver.net_handle.as_ref().unwrap().node_id();
    let group_id = Ulid::new();
    let content = b"hello world";

    let put = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: created_by,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: "bucket-a".to_string(),
                key: "put.bin".to_string(),
                content_length: Some(content.len() as u64),
                body: Some(stream_from_bytes(content)),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let upload = create_upload(&context, "bucket-a", "multipart.bin", group_id, created_by).await;
    let uploaded_part1 = upload_part_bytes(
        &context,
        "bucket-a",
        "multipart.bin",
        upload.upload_id,
        1,
        b"hello ",
        created_by,
    )
    .await;
    let uploaded_part2 = upload_part_bytes(
        &context,
        "bucket-a",
        "multipart.bin",
        upload.upload_id,
        2,
        b"world",
        created_by,
    )
    .await;
    let complete = complete_upload(
        &context,
        "bucket-a",
        "multipart.bin",
        upload.upload_id,
        realm_id,
        node_id,
        &[uploaded_part1.clone(), uploaded_part2.clone()],
        MultipartChecksumType::FullObject,
        Some(content.len() as u64),
        created_by,
    )
    .await;

    assert_eq!(complete.location, put.location);
    assert!(!exists(uploaded_part1.location.get_full_path().unwrap()).unwrap());
    assert!(!exists(uploaded_part2.location.get_full_path().unwrap()).unwrap());
    assert_eq!(count_blob_files(&context.blob_root), 1);

    let blob_head = read_value(
        &context.driver,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new("bucket-a", "multipart.bin")
            .to_bytes()
            .unwrap(),
    )
    .await
    .expect("missing blob head entry");
    assert_eq!(
        CurrentVersionPointer::from_bytes(blob_head.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(complete.version_id, 1)
    );
}

#[tokio::test]
async fn multipart_completion_same_key_same_content_bumps_generation_and_reuses_location() {
    let context = setup_context().await;
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let created_by = UserId::local(Ulid::new(), realm_id);
    let node_id = context.driver.net_handle.as_ref().unwrap().node_id();
    let group_id = Ulid::new();
    let part1 = b"hello ";
    let part2 = b"world";

    let initial_upload =
        create_upload(&context, "bucket-a", "same.bin", group_id, created_by).await;
    let initial_part1 = upload_part_bytes(
        &context,
        "bucket-a",
        "same.bin",
        initial_upload.upload_id,
        1,
        part1,
        created_by,
    )
    .await;
    let initial_part2 = upload_part_bytes(
        &context,
        "bucket-a",
        "same.bin",
        initial_upload.upload_id,
        2,
        part2,
        created_by,
    )
    .await;
    let initial_complete = complete_upload(
        &context,
        "bucket-a",
        "same.bin",
        initial_upload.upload_id,
        realm_id,
        node_id,
        &[initial_part1.clone(), initial_part2.clone()],
        MultipartChecksumType::FullObject,
        Some((part1.len() + part2.len()) as u64),
        created_by,
    )
    .await;

    let replacement_upload =
        create_upload(&context, "bucket-a", "same.bin", group_id, created_by).await;
    let replacement_part1 = upload_part_bytes(
        &context,
        "bucket-a",
        "same.bin",
        replacement_upload.upload_id,
        1,
        part1,
        created_by,
    )
    .await;
    let replacement_part2 = upload_part_bytes(
        &context,
        "bucket-a",
        "same.bin",
        replacement_upload.upload_id,
        2,
        part2,
        created_by,
    )
    .await;
    let replacement_complete = complete_upload(
        &context,
        "bucket-a",
        "same.bin",
        replacement_upload.upload_id,
        realm_id,
        node_id,
        &[replacement_part1.clone(), replacement_part2.clone()],
        MultipartChecksumType::FullObject,
        Some((part1.len() + part2.len()) as u64),
        created_by,
    )
    .await;

    assert_eq!(replacement_complete.location, initial_complete.location);
    assert_ne!(replacement_complete.version_id, initial_complete.version_id);
    assert!(!exists(replacement_part1.location.get_full_path().unwrap()).unwrap());
    assert!(!exists(replacement_part2.location.get_full_path().unwrap()).unwrap());
    assert_eq!(count_blob_files(&context.blob_root), 1);

    let blob_head = read_value(
        &context.driver,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new("bucket-a", "same.bin").to_bytes().unwrap(),
    )
    .await
    .expect("missing blob head entry");
    assert_eq!(
        CurrentVersionPointer::from_bytes(blob_head.as_ref()).unwrap(),
        CurrentVersionPointer::new_with_generation(replacement_complete.version_id, 2)
    );

    let blob_hash: [u8; 32] = replacement_complete
        .location
        .get_blake3()
        .unwrap()
        .try_into()
        .unwrap();
    let blob_version = read_value(
        &context.driver,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new("bucket-a", "same.bin", replacement_complete.version_id)
            .to_bytes()
            .unwrap(),
    )
    .await
    .expect("missing blob version entry");
    assert_eq!(
        BlobVersion::from_bytes(blob_version.as_ref())
            .unwrap()
            .blob_hash(),
        Some(&blob_hash)
    );
}

#[tokio::test]
async fn abort_multipart_upload_removes_metadata_and_part_blobs() {
    let context = setup_context().await;
    let created_by = UserId::local(Ulid::new(), RealmId::from_bytes([7u8; 32]));
    let created = drive(
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "abort.bin".to_string(),
            group_id: Ulid::new(),
            created_by,
            checksum_hint: None,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let upload_id = created.record.upload_id;
    let uploaded = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "abort.bin".to_string(),
            upload_id,
            part_number: 1,
            content_length: Some(4),
            body: Some(stream_from_bytes(b"part")),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    drive(
        AbortMultipartUploadOperation::new(AbortMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "abort.bin".to_string(),
            upload_id,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert!(
        read_value(
            &context.driver,
            S3_MULTIPART_UPLOAD_KEYSPACE,
            upload_id.to_bytes().to_vec(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver,
            S3_MULTIPART_UPLOAD_PART_KEYSPACE,
            MultipartUploadPartKey::new(upload_id, 1)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );
    assert!(!exists(uploaded.location.get_full_path().unwrap()).unwrap());
}

#[tokio::test]
async fn upload_part_checksum_mismatch_cleans_up_raw_part() {
    let context = setup_context().await;
    let created_by = UserId::local(Ulid::new(), RealmId::from_bytes([7u8; 32]));
    let upload_id = drive(
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "checksum.bin".to_string(),
            group_id: Ulid::new(),
            created_by,
            checksum_hint: None,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap()
    .record
    .upload_id;

    let err = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "checksum.bin".to_string(),
            upload_id,
            part_number: 1,
            content_length: Some(4),
            body: Some(stream_from_bytes(b"part")),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![ExpectedChecksum {
                algorithm: ChecksumAlgorithm::Sha256,
                digest: vec![0; 32],
            }],
        }),
        &context.driver,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        aruna_operations::s3::upload_part::UploadPartError::ChecksumMismatch("SHA256")
    ));

    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
            key: MultipartUploadPartKey::new(upload_id, 1)
                .to_bytes()
                .unwrap()
                .into(),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage event")
    };
    assert!(value.is_none());
}

#[tokio::test]
async fn delete_object_removes_completed_multipart_metadata() {
    let context = setup_context().await;
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let created_by = UserId::local(Ulid::new(), realm_id);
    let node_id = context.driver.net_handle.as_ref().unwrap().node_id();

    let created = drive(
        CreateMultipartUploadOperation::new(CreateMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "delete-me.bin".to_string(),
            group_id: Ulid::new(),
            created_by,
            checksum_hint: None,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let upload_id = created.record.upload_id;
    let part1 = b"hello ";
    let part2 = b"world";

    let uploaded_part1 = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "delete-me.bin".to_string(),
            upload_id,
            part_number: 1,
            content_length: Some(part1.len() as u64),
            body: Some(stream_from_bytes(part1)),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let uploaded_part2 = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: "bucket-a".to_string(),
            key: "delete-me.bin".to_string(),
            upload_id,
            part_number: 2,
            content_length: Some(part2.len() as u64),
            body: Some(stream_from_bytes(part2)),
            created_by,
            compressed: false,
            encrypted: false,
            expected_checksums: vec![],
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let complete = drive(
        CompleteMultipartUploadOperation::new(CompleteMultipartUploadInput {
            bucket: "bucket-a".to_string(),
            key: "delete-me.bin".to_string(),
            upload_id,
            realm_id: RealmId::from_bytes([7u8; 32]),
            node_id: context.driver.net_handle.as_ref().unwrap().node_id(),
            completed_parts: vec![
                CompleteMultipartPart {
                    part_number: 1,
                    etag: Some(hex::encode(
                        uploaded_part1.location.hashes.get("md5").unwrap(),
                    )),
                    expected_checksums: vec![],
                },
                CompleteMultipartPart {
                    part_number: 2,
                    etag: Some(hex::encode(
                        uploaded_part2.location.hashes.get("md5").unwrap(),
                    )),
                    expected_checksums: vec![],
                },
            ],
            expected_checksums: vec![],
            checksum_type: MultipartChecksumType::FullObject,
            object_size: Some((part1.len() + part2.len()) as u64),
            created_by,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    drive(
        DeleteObjectOperation::new(DeleteObjectInput {
            bucket: "bucket-a".to_string(),
            key: "delete-me.bin".to_string(),
            version_id: Some(complete.version_id),
            group_id: created.record.group_id,
            realm_id,
            node_id,
            deleted_by: created_by,
        }),
        &context.driver,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    assert!(
        read_value(
            &context.driver,
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            MultipartObjectMetadataKey::summary(complete.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver,
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            MultipartObjectMetadataKey::part(complete.version_id, 1)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );
    assert!(
        read_value(
            &context.driver,
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            MultipartObjectMetadataKey::part(complete.version_id, 2)
                .to_bytes()
                .unwrap(),
        )
        .await
        .is_none()
    );
}
