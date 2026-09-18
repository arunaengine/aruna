//! Builds get, put, delete and completion responses and queues reference metadata refresh.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{ArunaS3Service, reference_etag};
use crate::s3::checksum::{
    ApplyChecksums, ChecksumSelection, UploadChecksumRequest, encode_checksums,
};
use crate::s3::util::map_checksum_type;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::CONTENT_TYPE_KEY;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::multipart::complete::CompleteUploadResult;
use aruna_operations::s3::object::delete::bulk::BulkDeleteOutcome;
use aruna_operations::s3::object::delete::{DeleteObjectError, DeleteObjectResult};
use aruna_operations::s3::object::get::{GetObjectResult, ObjectInfo};
use aruna_operations::s3::object::metadata::{QueueRefreshOperation, ReferenceRefresh};
use aruna_operations::s3::object::put::PutObjectResult;
use s3s::dto::{
    CompleteMultipartUploadOutput, DeleteObjectOutput, DeleteObjectsOutput, DeletedObject, ETag,
    Error as S3DeleteError, LastModified, PutObjectOutput,
};
use s3s::{S3Response, S3Result, s3_error};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{error, warn};
use ulid::Ulid;

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
        metadata.insert(CONTENT_TYPE_KEY.to_string(), content_type.to_string());
    }
    metadata
}

pub(super) fn reference_metadata_refresh(
    bucket: String,
    key: String,
    result: &GetObjectResult,
) -> Option<ReferenceRefresh> {
    if result.location.is_some() {
        return None;
    }

    Some(ReferenceRefresh {
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
    refresh: ReferenceRefresh,
) -> BackendStream<Result<T, StreamError>> {
    let refresh_bucket = refresh.bucket.clone();
    let refresh_key = refresh.key.clone();
    let refresh_version_id = refresh.version_id;

    blob.on_success_async(move || async move {
        match drive(
            QueueRefreshOperation::new(refresh),
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
        result: CompleteUploadResult,
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

    /// Maps one bulk delete's per-entry outcomes into the response: policy
    /// refusals keep their place and order, quiet mode suppresses only deleted
    /// entries, and errors are always reported.
    pub(super) async fn delete_objects_response(
        &self,
        quiet: bool,
        bucket: String,
        replication_auth: AuthContext,
        prior_errors: Vec<S3DeleteError>,
        outcomes: Vec<BulkDeleteOutcome>,
    ) -> S3Response<DeleteObjectsOutput> {
        let mut deleted = Vec::new();
        let mut errors = prior_errors;
        for outcome in outcomes {
            match outcome.result {
                Ok(result) => {
                    if outcome.requested_version_id.is_none() {
                        self.queue_live_replication(
                            replication_auth.clone(),
                            bucket.clone(),
                            outcome.key.clone(),
                            result.version_id,
                            result.delete_marker,
                        )
                        .await;
                    }
                    deleted.push(deleted_object(
                        outcome.key,
                        outcome.requested_version_id,
                        &result,
                    ));
                }
                Err(DeleteObjectError::NoSuchVersion) => errors.push(missing_version_error(
                    outcome.key,
                    outcome.requested_version_id,
                )),
                Err(err) => {
                    warn!(error = %err, key = %outcome.key, "DeleteObjects entry failed");
                    errors.push(internal_delete_error(
                        outcome.key,
                        outcome.requested_version_id,
                    ));
                }
            }
        }

        S3Response::new(DeleteObjectsOutput {
            deleted: (!quiet).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..Default::default()
        })
    }

    fn source_metadata_headers(
        &self,
        metadata: &aruna_core::structs::execution::source_access::SourceMetadata,
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
        location: Option<&aruna_core::structs::storage::blob::BackendLocation>,
        info: Option<&ObjectInfo>,
        metadata: Option<&std::collections::HashMap<String, String>>,
        source_metadata: Option<&aruna_core::structs::execution::source_access::SourceMetadata>,
        last_refresh: Option<SystemTime>,
        version_created_at: Option<SystemTime>,
    ) -> ObjectResponseFields {
        let mut response_metadata = metadata.cloned().unwrap_or_default();
        let content_type = response_metadata.remove(CONTENT_TYPE_KEY);
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
            // Read information carries the backing read's exact ETag. A reference
            // source without one keeps the derived ETag its listing shows.
            e_tag: if let Some(info) = info {
                info.etag
                    .as_deref()
                    .and_then(|etag| ETag::from_str(etag).ok())
                    .or_else(|| source_metadata.map(reference_etag))
            } else {
                location
                    .and_then(|location| {
                        location
                            .hashes
                            .get(HASH_MD5)
                            .map(|value| ETag::Strong(hex::encode(value)))
                    })
                    .or_else(|| source_metadata.map(reference_etag))
            },
            last_modified: if let Some(info) = info {
                info.version_created_at
                    .map(Into::into)
                    .or_else(|| last_refresh.map(Into::into))
            } else {
                version_created_at
                    .map(Into::into)
                    .or_else(|| location.map(|location| location.created_at.into()))
                    .or_else(|| {
                        source_metadata.and_then(|metadata| metadata.last_modified.map(Into::into))
                    })
                    // A reference always answers with a date: clients such as
                    // mountpoint refuse a listing entry without one.
                    .or_else(|| last_refresh.map(Into::into))
            },
            metadata: (!response_metadata.is_empty()).then_some(response_metadata),
        }
    }
}

/// The deleted entry of one successful bulk delete: an unversioned delete reports
/// the created marker and its version, a versioned delete reports the version and
/// marks the id as a delete marker only when that version was one.
fn deleted_object(
    key: String,
    requested_version_id: Option<Ulid>,
    result: &DeleteObjectResult,
) -> DeletedObject {
    if requested_version_id.is_none() {
        DeletedObject {
            key: Some(key),
            delete_marker: Some(result.delete_marker),
            delete_marker_version_id: Some(result.version_id.to_string()),
            ..Default::default()
        }
    } else {
        DeletedObject {
            key: Some(key),
            version_id: Some(result.version_id.to_string()),
            delete_marker: Some(result.delete_marker),
            delete_marker_version_id: result.delete_marker.then(|| result.version_id.to_string()),
        }
    }
}

fn missing_version_error(key: String, requested_version_id: Option<Ulid>) -> S3DeleteError {
    S3DeleteError {
        code: Some("NoSuchVersion".to_string()),
        key: Some(key),
        version_id: requested_version_id.map(|id| id.to_string()),
        message: Some("The specified version does not exist.".to_string()),
    }
}

fn internal_delete_error(key: String, requested_version_id: Option<Ulid>) -> S3DeleteError {
    S3DeleteError {
        code: Some("InternalError".to_string()),
        key: Some(key),
        version_id: requested_version_id.map(|id| id.to_string()),
        message: Some("We encountered an internal error. Please try again.".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::structs::identity::realm::RealmId;

    async fn test_service() -> (tempfile::TempDir, ArunaS3Service) {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage =
            aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let node_id = NodeId::from_bytes(&[0u8; 32]).unwrap();
        let service = ArunaS3Service::new(context, RealmId([7u8; 32]), node_id).await;
        (dir, service)
    }

    fn success(key: &str) -> BulkDeleteOutcome {
        let version_id = Ulid::generate();
        BulkDeleteOutcome {
            key: key.to_string(),
            requested_version_id: Some(version_id),
            result: Ok(DeleteObjectResult {
                version_id,
                delete_marker: false,
            }),
        }
    }

    fn missing(key: &str) -> BulkDeleteOutcome {
        BulkDeleteOutcome {
            key: key.to_string(),
            requested_version_id: Some(Ulid::generate()),
            result: Err(DeleteObjectError::NoSuchVersion),
        }
    }

    #[test]
    fn delete_reports_marker() {
        let result = DeleteObjectResult {
            version_id: Ulid::generate(),
            delete_marker: true,
        };
        let deleted = deleted_object("key".to_string(), None, &result);
        assert_eq!(deleted.key.as_deref(), Some("key"));
        assert_eq!(deleted.version_id, None);
        assert_eq!(
            deleted.delete_marker_version_id.as_deref(),
            Some(result.version_id.to_string().as_str())
        );
        assert_eq!(deleted.delete_marker, Some(true));
    }

    #[test]
    fn delete_reports_version() {
        let result = DeleteObjectResult {
            version_id: Ulid::generate(),
            delete_marker: false,
        };
        let deleted = deleted_object("key".to_string(), Some(result.version_id), &result);
        assert_eq!(
            deleted.version_id.as_deref(),
            Some(result.version_id.to_string().as_str())
        );
        assert_eq!(deleted.delete_marker_version_id, None);
    }

    #[test]
    fn delete_error_shapes() {
        let missing = missing_version_error("missing".to_string(), Some(Ulid::generate()));
        assert_eq!(missing.code.as_deref(), Some("NoSuchVersion"));
        assert!(missing.version_id.is_some());
        let internal = internal_delete_error("broken".to_string(), None);
        assert_eq!(internal.code.as_deref(), Some("InternalError"));
        assert_eq!(internal.version_id, None);
    }

    #[tokio::test]
    async fn quiet_delete_errors() {
        let (_dir, service) = test_service().await;
        let outcomes = vec![success("ok"), missing("missing")];
        let response = service
            .delete_objects_response(
                true,
                "bucket".to_string(),
                AuthContext::anonymous(RealmId([7u8; 32])),
                vec![],
                outcomes,
            )
            .await;
        let output = response.output;
        assert_eq!(output.deleted, None);
        let errors = output.errors.expect("partial errors are reported");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].key.as_deref(), Some("missing"));
    }

    #[tokio::test]
    async fn delete_preserves_order() {
        let (_dir, service) = test_service().await;
        let outcomes = vec![success("first"), missing("second"), success("third")];
        let response = service
            .delete_objects_response(
                false,
                "bucket".to_string(),
                AuthContext::anonymous(RealmId([7u8; 32])),
                vec![],
                outcomes,
            )
            .await;
        let output = response.output;
        let deleted: Vec<String> = output
            .deleted
            .expect("deleted list")
            .into_iter()
            .filter_map(|object| object.key)
            .collect();
        assert_eq!(deleted, vec!["first".to_string(), "third".to_string()]);
        let errors = output.errors.expect("errors reported");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].key.as_deref(), Some("second"));
    }

    #[tokio::test]
    async fn policy_errors_first() {
        let (_dir, service) = test_service().await;
        let prior = vec![S3DeleteError {
            code: Some("AccessDenied".to_string()),
            key: Some("denied".to_string()),
            version_id: None,
            message: Some("Access Denied".to_string()),
        }];
        let response = service
            .delete_objects_response(
                false,
                "bucket".to_string(),
                AuthContext::anonymous(RealmId([7u8; 32])),
                prior,
                vec![missing("after")],
            )
            .await;
        let errors = response.output.errors.expect("errors reported");
        let keys: Vec<&str> = errors
            .iter()
            .filter_map(|error| error.key.as_deref())
            .collect();
        assert_eq!(keys, vec!["denied", "after"]);
    }
}
