//! Response metadata and builders for the S3 adapter, kept apart from the
//! trait implementation that maps requests and operations.

use super::ArunaS3Service;
use crate::s3::checksum::{
    ApplyChecksums, ChecksumSelection, UploadChecksumRequest, encode_checksums,
};
use crate::s3::util::map_checksum_type;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::{AuthContext, OBJECT_CONTENT_TYPE_KEY};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::complete_upload::CompleteMultipartUploadResult;
use aruna_operations::s3::delete_object::DeleteObjectResult;
use aruna_operations::s3::get_object::{GetObjectResult, ObjectInfo};
use aruna_operations::s3::put_object::PutObjectResult;
use aruna_operations::s3::refresh_metadata::{
    QueueReferenceMetadataRefreshOperation, ReferenceMetadataRefresh,
};
use s3s::dto::{
    CompleteMultipartUploadOutput, DeleteObjectOutput, ETag, LastModified, PutObjectOutput,
};
use s3s::{S3Response, S3Result, s3_error};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{error, warn};

#[derive(Debug)]
pub(super) struct ObjectResponseFields {
    pub(super) content_length: Option<i64>,
    pub(super) content_type: Option<String>,
    pub(super) e_tag: Option<ETag>,
    pub(super) last_modified: Option<LastModified>,
    pub(super) metadata: Option<std::collections::HashMap<String, String>>,
}

pub(super) fn object_metadata(
    mut metadata: std::collections::HashMap<String, String>,
    content_type: Option<&str>,
) -> std::collections::HashMap<String, String> {
    if let Some(content_type) = content_type {
        metadata.insert(
            OBJECT_CONTENT_TYPE_KEY.to_string(),
            content_type.to_string(),
        );
    }
    metadata
}

pub(super) fn reference_metadata_refresh(
    bucket: String,
    key: String,
    result: &GetObjectResult,
) -> Option<ReferenceMetadataRefresh> {
    if result.location.is_some() {
        return None;
    }

    Some(ReferenceMetadataRefresh {
        bucket,
        key,
        version_id: result.resolved_version_id.or(result.version_id)?,
        metadata: result.source_metadata.clone()?,
        refreshed_at: result.last_refresh?,
    })
}

pub(super) fn attach_reference_refresh<T: 'static>(
    blob: BackendStream<Result<T, StreamError>>,
    context: Arc<DriverContext>,
    refresh: ReferenceMetadataRefresh,
) -> BackendStream<Result<T, StreamError>> {
    let refresh_bucket = refresh.bucket.clone();
    let refresh_key = refresh.key.clone();
    let refresh_version_id = refresh.version_id;

    blob.on_success_async(move || async move {
        match drive(
            QueueReferenceMetadataRefreshOperation::new(refresh),
            context.as_ref(),
        )
        .await
        {
            Ok(queue_result) => {
                if queue_result.queued && !queue_result.scheduled {
                    warn!(
                        bucket = %refresh_bucket,
                        key = %refresh_key,
                        version_id = %refresh_version_id,
                        "Reference metadata refresh job persisted but drain scheduling was not acknowledged"
                    );
                }
            }
            Err(error) => {
                warn!(
                    error = %error,
                    bucket = %refresh_bucket,
                    key = %refresh_key,
                    version_id = %refresh_version_id,
                    "Failed to queue reference metadata refresh after successful stream; refresh is best effort"
                );
            }
        }
        Ok(())
    })
}

impl ArunaS3Service {
    /// Returns the true full-object MD5 hex as the multipart ETag, without the
    /// AWS `-<partCount>` suffix: Aruna composes the parts and hashes the blob,
    /// so the ETag is a content MD5 clients can verify end-to-end.
    pub(super) fn complete_upload_response(
        &self,
        bucket: String,
        key: String,
        checksum_request: &UploadChecksumRequest,
        result: CompleteMultipartUploadResult,
    ) -> S3Response<CompleteMultipartUploadOutput> {
        let mut output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: result
                .location
                .hashes
                .get(HASH_MD5)
                .map(|value| ETag::Strong(hex::encode(value))),
            version_id: Some(result.version_id.to_string()),
            ..Default::default()
        };

        output.apply_checksums(encode_checksums(
            &result.response_hashes,
            ChecksumSelection::Requested(checksum_request.response_algorithm),
            map_checksum_type(result.checksum_type),
            Some(result.part_count),
        ));

        S3Response::new(output)
    }

    pub(super) async fn put_object_response(
        &self,
        checksum_request: &UploadChecksumRequest,
        replication_auth: AuthContext,
        group_id: ulid::Ulid,
        replication_bucket: String,
        replication_key: String,
        result: PutObjectResult,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let mut output = PutObjectOutput {
            e_tag: Some(ETag::Strong(hex::encode(
                result.location.hashes.get(HASH_MD5).ok_or_else(|| {
                    error!(error = "Missing MD5 hash");
                    s3_error!(InternalError, "Missing MD5 hash")
                })?,
            ))),
            size: Some(result.location.blob_size as i64),
            version_id: Some(result.version_id.to_string()),
            ..Default::default()
        };
        output.apply_checksums(encode_checksums(
            &result.location.hashes,
            ChecksumSelection::Requested(checksum_request.response_algorithm),
            checksum_request.checksum_type.clone(),
            None,
        ));
        self.complete_put(
            replication_auth,
            group_id,
            replication_bucket,
            replication_key,
            result.version_id,
            result.location.blob_size,
        )
        .await;

        Ok(S3Response::new(output))
    }

    pub(super) async fn delete_object_response(
        &self,
        replication_auth: AuthContext,
        replication_bucket: String,
        replication_key: String,
        replicate_latest_delete: bool,
        result: DeleteObjectResult,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        if replicate_latest_delete {
            self.queue_live_replication(
                replication_auth,
                replication_bucket,
                replication_key,
                result.version_id,
                result.delete_marker,
            )
            .await;
        }

        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: Some(result.delete_marker),
            version_id: Some(result.version_id.to_string()),
            ..Default::default()
        }))
    }

    fn source_metadata_headers(
        &self,
        metadata: &aruna_core::structs::SourceMetadata,
        last_refresh: Option<SystemTime>,
    ) -> Option<std::collections::HashMap<String, String>> {
        let mut headers = std::collections::HashMap::new();

        if let Some(content_type) = &metadata.content_type {
            headers.insert(
                "aruna-source-content-type".to_string(),
                content_type.clone(),
            );
        }
        if let Some(etag) = &metadata.etag {
            headers.insert("aruna-source-etag".to_string(), etag.clone());
        }
        if let Some(last_modified) = metadata.last_modified {
            headers.insert(
                "aruna-source-last-modified".to_string(),
                chrono::DateTime::<chrono::Utc>::from(last_modified).to_rfc3339(),
            );
        }
        if let Some(last_refresh) = last_refresh {
            headers.insert(
                "aruna-last-refresh".to_string(),
                chrono::DateTime::<chrono::Utc>::from(last_refresh).to_rfc3339(),
            );
        }

        (!headers.is_empty()).then_some(headers)
    }

    pub(super) fn build_response_fields(
        &self,
        location: Option<&aruna_core::structs::BackendLocation>,
        info: Option<&ObjectInfo>,
        metadata: Option<&std::collections::HashMap<String, String>>,
        source_metadata: Option<&aruna_core::structs::SourceMetadata>,
        last_refresh: Option<SystemTime>,
        version_created_at: Option<SystemTime>,
    ) -> ObjectResponseFields {
        let mut response_metadata = metadata.cloned().unwrap_or_default();
        let content_type = response_metadata.remove(OBJECT_CONTENT_TYPE_KEY);
        if let Some(source_headers) = source_metadata
            .and_then(|metadata| self.source_metadata_headers(metadata, last_refresh))
        {
            response_metadata.extend(source_headers);
        }

        ObjectResponseFields {
            content_length: info
                .map(|info| info.size as i64)
                .or_else(|| location.map(|location| location.blob_size as i64))
                .or_else(|| source_metadata.map(|metadata| metadata.content_length as i64)),
            content_type: source_metadata
                .and_then(|metadata| metadata.content_type.clone())
                .or(content_type),
            // Read information carries the backing read's exact ETag.
            e_tag: if let Some(info) = info {
                info.etag
                    .as_deref()
                    .and_then(|etag| ETag::from_str(etag).ok())
            } else {
                location
                    .and_then(|location| {
                        location
                            .hashes
                            .get(HASH_MD5)
                            .map(|value| ETag::Strong(hex::encode(value)))
                    })
                    .or_else(|| {
                        source_metadata.and_then(|metadata| {
                            metadata
                                .etag
                                .as_deref()
                                .and_then(|etag| ETag::from_str(etag).ok())
                        })
                    })
            },
            last_modified: if let Some(info) = info {
                info.version_created_at.map(Into::into)
            } else {
                version_created_at
                    .map(Into::into)
                    .or_else(|| location.map(|location| location.created_at.into()))
                    .or_else(|| {
                        source_metadata.and_then(|metadata| metadata.last_modified.map(Into::into))
                    })
            },
            metadata: (!response_metadata.is_empty()).then_some(response_metadata),
        }
    }
}
