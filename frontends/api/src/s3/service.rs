//! Implements the S3 trait for Aruna and maps every S3 call onto node operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(clippy::result_large_err)]

mod attributes;
mod bucket;
mod buckets;
mod copy;
mod encryption;
pub(crate) mod forward;
mod link;
mod listing;
mod lists;
mod multipart;
mod object;
mod payload;
mod pinned;
mod reads;
mod response;
mod uploads;
pub(crate) mod view;
mod writes;

use crate::s3::auth::{DelegatedRequest, map_authorize_error};
use crate::s3::browse::{IndexEntry, render_index};
use crate::s3::multipart_join::{CompletionRegistry, completion_registry};
use aruna_core::compute::session::{SessionRequest, TouchedObject};
use aruna_core::id::NodeId;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::execution::job::input::RoCrateLimits;
use aruna_core::structs::execution::job::local::credential_job_id;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, object_permission_path};
use aruna_core::structs::storage::encryption::TokenCredential;
use aruna_core::structs::storage::multipart::COMPLETION_DEADLINE_MS;
use aruna_ops_data::s3::object::list::{ListBucketInput, ListBucketOperation};
use aruna_ops_data::transfer::read::TokenRead;
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use aruna_ops_work::jobs::session::SessionOperation;
use aruna_runner::driver::{DriverContext, drive};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, BucketVersioningStatus,
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput,
    CreateBucketInput, CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
    DeleteBucketCorsInput, DeleteBucketCorsOutput, DeleteBucketEncryptionInput,
    DeleteBucketEncryptionOutput, DeleteBucketInput, DeleteBucketOutput,
    DeleteBucketReplicationInput, DeleteBucketReplicationOutput, DeleteObjectInput,
    DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, ETag, GetBucketCorsInput,
    GetBucketCorsOutput, GetBucketEncryptionInput, GetBucketEncryptionOutput,
    GetBucketLocationInput, GetBucketLocationOutput, GetBucketReplicationInput,
    GetBucketReplicationOutput, GetBucketVersioningInput, GetBucketVersioningOutput,
    GetObjectAttributesInput, GetObjectAttributesOutput, GetObjectInput, GetObjectOutput,
    HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput, ListBucketsInput,
    ListBucketsOutput, ListMultipartUploadsInput, ListMultipartUploadsOutput,
    ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
    ListObjectsV2Input, ListObjectsV2Output, ListPartsInput, ListPartsOutput, PutBucketCorsInput,
    PutBucketCorsOutput, PutBucketEncryptionInput, PutBucketEncryptionOutput,
    PutBucketReplicationInput, PutBucketReplicationOutput, PutBucketVersioningInput,
    PutBucketVersioningOutput, PutObjectInput, PutObjectOutput, StreamingBlob, UploadPartCopyInput,
    UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};

const URL_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// The ETag a reference answers with: the source's when it gave a usable one,
/// otherwise one derived from the observation, so listings and reads agree and
/// clients that require an ETag, such as mountpoint, keep working.
pub(super) fn reference_etag(
    metadata: &aruna_core::structs::execution::source_access::SourceMetadata,
) -> ETag {
    metadata
        .etag
        .as_deref()
        .and_then(|etag| ETag::from_str(etag).ok())
        .unwrap_or_else(|| ETag::Strong(hex::encode(&metadata.observation_fingerprint()[..16])))
}

/// On expiry the record reopens and the composed blob is rolled back. The
/// cleanup sweep waits out the same deadline, so it never aborts a live
/// completion.
const COMPLETION_DEADLINE: Duration = Duration::from_millis(COMPLETION_DEADLINE_MS);

#[derive(Clone)]
pub struct ArunaS3Service {
    state: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
    rocrate_limits: RoCrateLimits,
    /// Detached CompleteMultipartUpload tasks, shared by every clone of the
    /// service so a retry on another connection joins the running completion.
    completions: Arc<CompletionRegistry>,
}

impl Debug for ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(driver_ctx))]
    pub async fn new(driver_ctx: Arc<DriverContext>, realm_id: RealmId, node_id: NodeId) -> Self {
        ArunaS3Service {
            state: driver_ctx,
            realm_id,
            node_id,
            rocrate_limits: RoCrateLimits::default(),
            completions: Arc::new(completion_registry()),
        }
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    /// The token credential the access check admitted for this request, if any.
    fn token_read(&self, extensions: &http::Extensions) -> Option<TokenRead> {
        let credential = extensions.get::<TokenCredential>()?.clone();
        Some(TokenRead {
            credential,
            limits: self.rocrate_limits.clone(),
        })
    }

    /// Attributes one authorized request made with a session's own credential
    /// to its job. Every other credential is ignored.
    async fn record_touch(&self, access_key: &str, bucket: &str, key: &str, operation: &str) {
        let Some(job_id) = credential_job_id(access_key) else {
            return;
        };
        let touched = TouchedObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            operation: operation.to_string(),
        };
        let request = SessionRequest::RecordTouched(touched);
        // Without a live session there is nothing to attribute the request to.
        let _ = drive(
            SessionOperation::new(job_id.to_string(), request),
            &self.state,
        )
        .await;
    }
}

impl ArunaS3Service {
    /// One page directly below a public folder for an unsigned browser request.
    /// The access layer authorized the folder; each entry is checked again for
    /// the Everyone principal, so a narrower rule never lists a private name.
    async fn folder_index(
        &self,
        extensions: &http::Extensions,
        group_id: aruna_core::types::GroupId,
        bucket: &str,
        prefix: &str,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        // A delegated request is never sent on, and an index would list other ranges.
        if extensions.get::<DelegatedRequest>().is_some() {
            return Err(s3_error!(
                AccessDenied,
                "Delegation refused: no folder index"
            ));
        }
        let extras = extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "Missing policy context"))?;
        let input = ListBucketInput {
            bucket: bucket.to_owned(),
            group_id,
            continuation_token: None,
            max_keys: Some(ListBucketOperation::DEFAULT_MAX_KEYS),
            prefix: Some(prefix.to_owned()),
            delimiter: Some("/".to_owned()),
            start_after: None,
        };
        let distributed = (extensions.get::<BucketInfo>())
            .and_then(|info| info.distribution.clone())
            .zip(extensions.get::<UserAccess>());
        // A distributed bucket's folder spans its key ranges, as its S3 listing does.
        let (page, stale, more) = match distributed {
            Some((binding, user)) => {
                let user = forward::reads::anonymous_window(user);
                let shape = (None, false, None);
                let (page, next, stale) =
                    (self.merged_objects(&user, &binding, input, shape, (None, true))).await?;
                (page, stale, next.is_some())
            }
            None => {
                let page = self
                    .run_object_listing(input, (None, None), false, (None, None), (None, None))
                    .await?;
                (page, Vec::new(), false)
            }
        };
        let truncated = more || page.continuation_token.is_some() || page.sealed.is_some();
        let folders = page
            .common_prefixes
            .into_iter()
            .filter_map(|folder| folder.prefix)
            .map(|key| (key, None, None));
        let objects = page.contents.into_iter().filter_map(|object| {
            object
                .key
                .map(|key| (key, object.size, object.last_modified))
        });
        let anonymous = AuthContext::anonymous(self.realm_id);
        let mut entries = Vec::new();
        for (key, size, modified) in folders.chain(objects) {
            let Some(name) = key.strip_prefix(prefix).filter(|name| !name.is_empty()) else {
                continue;
            };
            let path = object_permission_path(self.realm_id, group_id, self.node_id, bucket, &key);
            match drive(
                AuthorizeOperation::new(
                    self.realm_id,
                    &anonymous,
                    &path,
                    &Permission::READ,
                    extras.clone(),
                ),
                &self.state,
            )
            .await
            {
                Ok(_) => entries.push(IndexEntry {
                    name: name.to_owned(),
                    size,
                    modified,
                }),
                Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {}
                Err(error) => return Err(map_authorize_error(error)),
            }
        }

        let html = render_index(bucket, prefix, &entries, truncated);
        let output = GetObjectOutput {
            content_length: i64::try_from(html.len()).ok(),
            content_type: Some("text/html; charset=utf-8".to_owned()),
            body: Some(StreamingBlob::from(bytes::Bytes::from(html))),
            ..Default::default()
        };
        let mut response = S3Response::new(output);
        forward::reads::mark_stale(&mut response.headers, &stale);
        response
            .headers
            .insert(http::header::VARY, http::HeaderValue::from_static("Accept"));
        response.headers.insert(
            http::header::X_CONTENT_TYPE_OPTIONS,
            http::HeaderValue::from_static("nosniff"),
        );
        response.headers.insert(
            http::header::CONTENT_SECURITY_POLICY,
            http::HeaderValue::from_static("default-src 'none'; style-src 'unsafe-inline'"),
        );
        Ok(response)
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        buckets::create_bucket(self, req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        buckets::head_bucket(self, req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        buckets::get_bucket_location(self, req).await
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        debug!(bucket = %req.input.bucket, "Received GET BUCKET VERSIONING Request");

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        Ok(S3Response::new(GetBucketVersioningOutput {
            status: Some(BucketVersioningStatus::from_static(
                BucketVersioningStatus::ENABLED,
            )),
            mfa_delete: None,
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        debug!(bucket = %req.input.bucket, "Received PUT BUCKET VERSIONING Request");

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        match req
            .input
            .versioning_configuration
            .status
            .as_ref()
            .map(BucketVersioningStatus::as_str)
        {
            Some(BucketVersioningStatus::ENABLED) => {
                Ok(S3Response::new(PutBucketVersioningOutput::default()))
            }
            _ => Err(s3_error!(NotImplemented, "Versioning cannot be suspended")),
        }
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        buckets::list_buckets(self, req).await
    }

    async fn put_bucket_cors(
        &self,
        req: S3Request<PutBucketCorsInput>,
    ) -> S3Result<S3Response<PutBucketCorsOutput>> {
        buckets::put_bucket_cors(self, req).await
    }

    async fn get_bucket_cors(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        buckets::get_bucket_cors(self, req).await
    }

    async fn delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        buckets::delete_bucket_cors(self, req).await
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        buckets::get_bucket_encryption(self, req).await
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        buckets::put_bucket_encryption(self, req).await
    }

    #[tracing::instrument(err, skip(self, _req))]
    async fn delete_bucket_encryption(
        &self,
        _req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        Err(s3_error!(
            NotImplemented,
            "Disable bucket encryption through the Aruna REST API"
        ))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        lists::list_objects_v2(self, req).await
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        lists::list_objects(self, req).await
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        writes::put_object(self, req).await
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        writes::copy_object(self, req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        uploads::create_multipart_upload(self, req).await
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        uploads::upload_part(self, req).await
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        uploads::upload_part_copy(self, req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        uploads::complete_multipart_upload(self, req).await
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        uploads::abort_multipart_upload(self, req).await
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        reads::get_object(self, req).await
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        reads::get_object_attributes(self, req).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        reads::head_object(self, req).await
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        uploads::list_parts(self, req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        uploads::list_multipart_uploads(self, req).await
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        lists::list_object_versions(self, req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        writes::delete_object(self, req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        writes::delete_objects(self, req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        buckets::delete_bucket(self, req).await
    }

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        buckets::put_bucket_replication(self, req).await
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        buckets::get_bucket_replication(self, req).await
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        buckets::delete_bucket_replication(self, req).await
    }
}

#[cfg(test)]
mod tests;
