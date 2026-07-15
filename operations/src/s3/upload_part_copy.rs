use crate::driver::{DriverContext, drive};
use crate::s3::copy_object::{CopySourceConditions, evaluate_source_conditions};
use crate::s3::get_object::{
    GetObjectError, GetObjectInput, GetObjectOperation, ObjectRangeRequest,
};
use crate::s3::upload_part::{UploadPartError, UploadPartInput, UploadPartOperation};
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_MULTIPART_UPLOAD_KEYSPACE;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::{BackendLocation, MultipartUpload, MultipartUploadStatus};
use aruna_core::types::GroupId;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadPartCopyInput {
    pub source_bucket: String,
    pub source_key: String,
    pub source_version_id: Option<Ulid>,
    pub source_group_id: GroupId,
    pub dest_bucket: String,
    pub dest_key: String,
    pub upload_id: Ulid,
    pub part_number: u16,
    pub range: Option<ObjectRangeRequest>,
    pub user_id: UserId,
    pub conditions: CopySourceConditions,
}

#[derive(Debug, PartialEq)]
pub struct UploadPartCopyResultData {
    pub part_location: BackendLocation,
    pub source_version_id: Option<Ulid>,
    pub source_last_modified: Option<SystemTime>,
}

#[derive(Debug, Error, PartialEq)]
pub enum UploadPartCopyError {
    #[error(transparent)]
    Get(#[from] GetObjectError),
    #[error(transparent)]
    UploadPart(#[from] UploadPartError),
    #[error("At least one of the preconditions you specified did not hold.")]
    PreconditionFailed,
}

pub async fn upload_part_copy(
    context: &DriverContext,
    input: UploadPartCopyInput,
) -> Result<UploadPartCopyResultData, UploadPartCopyError> {
    validate_destination_upload(context, &input).await?;

    let source = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: input.source_bucket,
            key: input.source_key,
            version_id: input.source_version_id,
            range: input.range,
            group_id: input.source_group_id,
            user_identity: input.user_id,
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())?
    .ok_or(UploadPartCopyError::Get(GetObjectError::GetObjectFailed))?;

    let source_version_id = source.version_id;
    let source_last_modified = source
        .version_created_at
        .or_else(|| source.location.as_ref().map(|location| location.created_at))
        .or_else(|| {
            source
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.last_modified)
        });

    // Evaluate preconditions before consuming the (lazy) source stream.
    let source_etag = source
        .location
        .as_ref()
        .and_then(|location| location.hashes.get(HASH_MD5))
        .map(hex::encode)
        .or_else(|| {
            source
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.etag.clone())
        });
    if evaluate_source_conditions(
        &input.conditions,
        source_etag.as_deref(),
        source_last_modified,
        true,
    )
    .is_err()
    {
        return Err(UploadPartCopyError::PreconditionFailed);
    }

    let content_length = source
        .resolved_range
        .as_ref()
        .map(|range| range.content_length as u64)
        .or_else(|| source.location.as_ref().map(|location| location.blob_size));

    let part = drive(
        UploadPartOperation::new(UploadPartInput {
            bucket: input.dest_bucket,
            key: input.dest_key,
            upload_id: input.upload_id,
            part_number: input.part_number,
            content_length,
            body: Some(source.blob),
            created_by: input.user_id,
            compressed: false,
            encrypted: false,
            expected_checksums: Vec::new(),
        }),
        context,
    )
    .await
    .and_then(|result| result.transpose())?;
    let part = part.ok_or(UploadPartError::UploadPartFailed)?;

    Ok(UploadPartCopyResultData {
        part_location: part.location,
        source_version_id,
        source_last_modified,
    })
}

async fn validate_destination_upload(
    context: &DriverContext,
    input: &UploadPartCopyInput,
) -> Result<(), UploadPartCopyError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: input.upload_id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await;
    let value = match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(UploadPartCopyError::UploadPart(
                UploadPartError::StorageError(error),
            ));
        }
        _ => {
            return Err(UploadPartCopyError::UploadPart(
                UploadPartError::InvalidOperationState,
            ));
        }
    };
    let Some(value) = value else {
        return Err(UploadPartCopyError::UploadPart(
            UploadPartError::NoSuchUpload,
        ));
    };
    let record = MultipartUpload::from_bytes(value.as_ref()).map_err(UploadPartError::from)?;
    if record.bucket != input.dest_bucket || record.key != input.dest_key {
        return Err(UploadPartCopyError::UploadPart(
            UploadPartError::UploadTargetMismatch,
        ));
    }
    if record.status != MultipartUploadStatus::Open {
        return Err(UploadPartCopyError::UploadPart(
            UploadPartError::UploadNotOpen,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_blob::hash::Hasher;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE};
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        Backend, BackendConfig, MultipartUpload, MultipartUploadPart, MultipartUploadPartKey,
        MultipartUploadStatus, RealmId,
    };
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::{TempDir, tempdir};

    async fn full_context() -> (TempDir, DriverContext) {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (temp_handle, context)
    }

    async fn put_source(
        context: &DriverContext,
        realm_id: RealmId,
        group_id: GroupId,
        node_id: aruna_core::NodeId,
        bucket: &str,
        key: &str,
        data: &'static [u8],
    ) {
        drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: UserId::local(Ulid::r#gen(), realm_id),
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    content_length: Some(data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(data))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                quota_ceiling: None,
            }),
            context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    }

    async fn seed_multipart_upload(
        context: &DriverContext,
        upload_id: Ulid,
        bucket: &str,
        key: &str,
        group_id: GroupId,
        user_id: UserId,
    ) {
        let record = MultipartUpload {
            upload_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id,
            created_by: user_id,
            created_at: SystemTime::now(),
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
                key: upload_id.to_bytes().to_vec().into(),
                value: record.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn range_copy_writes_part_bytes_and_record() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::r#gen();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::r#gen(), realm_id);
        let upload_id = Ulid::r#gen();

        put_source(
            &context,
            realm_id,
            group_id,
            node_id,
            "bucket",
            "source.txt",
            b"0123456789",
        )
        .await;
        seed_multipart_upload(&context, upload_id, "bucket", "dest.txt", group_id, user_id).await;

        let result = upload_part_copy(
            &context,
            UploadPartCopyInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                upload_id,
                part_number: 1,
                range: Some(ObjectRangeRequest::StartEnd { start: 2, end: 5 }),
                user_id,
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap();

        assert_eq!(result.part_location.blob_size, 4);
        let expected = Hasher::new_with_bytes(b"2345").to_map();
        assert_eq!(
            result.part_location.hashes.get(HASH_MD5),
            expected.get(HASH_MD5)
        );

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
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
            panic!("missing part record");
        };
        let part =
            MultipartUploadPart::from_bytes(value.expect("missing part record").as_ref()).unwrap();
        assert_eq!(part.part_number, 1);
        assert_eq!(part.location.blob_size, 4);
    }

    #[tokio::test]
    async fn missing_destination_upload_fails_before_source_lookup() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::r#gen();
        let user_id = UserId::local(Ulid::r#gen(), realm_id);

        let error = upload_part_copy(
            &context,
            UploadPartCopyInput {
                source_bucket: "missing-source".to_string(),
                source_key: "missing.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "dest".to_string(),
                dest_key: "object.txt".to_string(),
                upload_id: Ulid::r#gen(),
                part_number: 1,
                range: None,
                user_id,
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            UploadPartCopyError::UploadPart(UploadPartError::NoSuchUpload)
        );
    }

    #[tokio::test]
    async fn unsatisfiable_range_errors() {
        let (_temp, context) = full_context().await;
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = Ulid::r#gen();
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let user_id = UserId::local(Ulid::r#gen(), realm_id);
        let upload_id = Ulid::r#gen();

        put_source(
            &context,
            realm_id,
            group_id,
            node_id,
            "bucket",
            "source.txt",
            b"0123456789",
        )
        .await;
        seed_multipart_upload(&context, upload_id, "bucket", "dest.txt", group_id, user_id).await;

        let error = upload_part_copy(
            &context,
            UploadPartCopyInput {
                source_bucket: "bucket".to_string(),
                source_key: "source.txt".to_string(),
                source_version_id: None,
                source_group_id: group_id,
                dest_bucket: "bucket".to_string(),
                dest_key: "dest.txt".to_string(),
                upload_id,
                part_number: 1,
                range: Some(ObjectRangeRequest::StartEnd {
                    start: 100,
                    end: 200,
                }),
                user_id,
                conditions: CopySourceConditions::default(),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(
            error,
            UploadPartCopyError::Get(GetObjectError::InvalidRange)
        );
    }
}
