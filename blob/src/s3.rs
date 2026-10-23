//! Creates an aws S3 client from backend config and makes a bucket with the right region.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::egress::EgressGuard;
use crate::opendal::{environment_variable, s3_region};
use aruna_core::errors::BlobError;
use aruna_core::stream::{BackendStream, BoxStream, StreamError};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use aws_sdk_s3::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, CompletedMultipartUpload, CompletedPart, CreateBucketConfiguration,
};
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use http_body::{Frame, SizeHint};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

const DEFAULT_REGION: &str = "eu-central-1";
/// A copy source names a key as a URL path: everything but unreserved characters and `/`.
const COPY_SOURCE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');
// AWS rejects CreateBucket requests that name us-east-1 explicitly.
const IMPLICIT_REGION: &str = "us-east-1";
/// User metadata naming the version whose write an empty reservation holds a path for.
pub(crate) const RESERVATION: &str = "aruna-reservation";

pub async fn create_s3_client(
    endpoint: &str,
    region: Option<String>,
    access_key_id: &str,
    secret_key: &str,
    force_path_style: bool,
) -> Result<Client, BlobError> {
    let creds = Credentials::new(access_key_id, secret_key, None, None, "Aruna_v3");
    // An unpinned region makes the SDK resolve one through the EC2 metadata service.
    let region = Region::new(region.unwrap_or_else(|| DEFAULT_REGION.to_string()));
    let client_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region.clone())
        .credentials_provider(creds)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&client_config)
        .region(region)
        .endpoint_url(endpoint)
        .force_path_style(force_path_style)
        .build();

    Ok(Client::from_conf(s3_config))
}

fn required_key<'a>(config: &'a HashMap<String, String>, key: &str) -> Result<&'a str, BlobError> {
    config.get(key).map(String::as_str).ok_or_else(|| {
        BlobError::OperatorCreationFailed(format!("blob backend config is missing {key}"))
    })
}

pub async fn make_bucket(bucket: &str, config: &HashMap<String, String>) -> Result<(), BlobError> {
    let region = config
        .get("region")
        .cloned()
        .unwrap_or_else(|| DEFAULT_REGION.to_string());
    let s3_client = create_s3_client(
        required_key(config, "endpoint")?,
        Some(region.clone()),
        required_key(config, "access_key_id")?,
        required_key(config, "secret_access_key")?,
        config
            .get("force_path_style")
            .map(|val| val.parse::<bool>().unwrap_or(true))
            .unwrap_or(true),
    )
    .await?;

    if s3_client
        .get_bucket_location()
        .bucket(bucket)
        .send()
        .await
        .is_ok()
    {
        return Ok(());
    }

    let mut request = s3_client.create_bucket().bucket(bucket);
    if let Some(constraint) = location_constraint(&region) {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(constraint)
                .build(),
        );
    }

    match request.send().await {
        Ok(_) => Ok(()),
        Err(err) => match err.as_service_error() {
            // A racing creator won: the bucket is ours, or at least reachable.
            Some(service) if service.is_bucket_already_owned_by_you() => Ok(()),
            Some(service) if service.is_bucket_already_exists() => s3_client
                .get_bucket_location()
                .bucket(bucket)
                .send()
                .await
                .map(|_| ())
                .map_err(|_| BlobError::MakeBucketError(err.to_string())),
            _ => Err(BlobError::MakeBucketError(err.to_string())),
        },
    }
}

/// The provider's own multipart calls for one bucket. OpenDAL picks part boundaries itself and
/// hides the upload id, so in-place multipart uploads and their cleanup use this client.
#[derive(Clone, Debug)]
pub struct NativeMultipart {
    client: Client,
    bucket: String,
    root: String,
}

impl NativeMultipart {
    /// Builds the client from an S3 backend's service config; tenant backends pass their guard.
    pub fn from_config(
        config: &HashMap<String, String>,
        bucket: &str,
        root: &str,
        guard: Option<&EgressGuard>,
    ) -> Result<Self, BlobError> {
        Ok(Self {
            client: config_client(config, guard)?,
            bucket: bucket.to_string(),
            root: root.to_string(),
        })
    }

    #[cfg(test)]
    pub(crate) fn region(&self) -> Option<String> {
        self.client.config().region().map(ToString::to_string)
    }

    /// Number and size of each part of a completed object.
    #[cfg(test)]
    pub(crate) async fn part_sizes(&self, path: &str) -> Vec<(i32, u64)> {
        use aws_sdk_s3::types::ObjectAttributes;
        let request = self.client.get_object_attributes().bucket(&self.bucket);
        let request = request.key(self.key(path)).max_parts(1000);
        let output = request
            .object_attributes(ObjectAttributes::ObjectParts)
            .send()
            .await
            .unwrap();
        let parts = output.object_parts().unwrap().parts();
        let size = |part: &aws_sdk_s3::types::ObjectPart| part.size().unwrap() as u64;
        parts
            .iter()
            .map(|part| (part.part_number().unwrap(), size(part)))
            .collect()
    }

    /// The object key OpenDAL uses for `path` under this backend's root.
    fn key(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        match self.root.trim_matches('/') {
            "" => path.to_string(),
            root => format!("{root}/{path}"),
        }
    }

    pub async fn create(&self, path: &str) -> Result<String, BlobError> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(self.key(path))
            .send()
            .boxed()
            .await
            .map_err(|error| write_error("create", error))?;
        output
            .upload_id()
            .map(str::to_string)
            .ok_or_else(|| BlobError::WriteError("backend returned no multipart upload id".into()))
    }

    /// Streams one part straight to the backend and returns its ETag.
    pub async fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u16,
        size: u64,
        body: BackendStream<Result<Bytes, StreamError>>,
    ) -> Result<String, BlobError> {
        let length = i64::try_from(size)
            .map_err(|_| BlobError::WriteError("part is too large".to_string()))?;
        let body = PartBody {
            stream: body.0,
            size,
        };
        let output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(self.key(path))
            .upload_id(upload_id)
            .part_number(i32::from(part_number))
            .content_length(length)
            .body(ByteStream::from_body_1_x(body))
            .send()
            .boxed()
            .await
            .map_err(|error| write_error("upload part", error))?;
        output
            .e_tag()
            .map(str::to_string)
            .ok_or_else(|| BlobError::WriteError("backend returned no part ETag".to_string()))
    }

    /// Copies a whole object of the same provider into one part, so no byte passes the node.
    pub async fn copy_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: u16,
        source: &NativeMultipart,
        source_path: &str,
    ) -> Result<String, BlobError> {
        let part = (part_number, None);
        self.copy_range(path, upload_id, part, (source, source_path, None, None))
            .await
    }

    /// Copies `range` (inclusive), or all, of one provider `version` of a source object into
    /// one part; without a version only while the source has the ETag `if_match`.
    pub async fn copy_range(
        &self,
        path: &str,
        upload_id: &str,
        (part_number, range): (u16, Option<(u64, u64)>),
        (source, source_path, version, if_match): (
            &NativeMultipart,
            &str,
            Option<&str>,
            Option<&str>,
        ),
    ) -> Result<String, BlobError> {
        let range = range.map(|(first, last)| format!("bytes={first}-{last}"));
        let output = self
            .client
            .upload_part_copy()
            .bucket(&self.bucket)
            .key(self.key(path))
            .upload_id(upload_id)
            .part_number(i32::from(part_number))
            .copy_source(source.copy_source(source_path, version))
            .set_copy_source_range(range)
            .set_copy_source_if_match(
                if_match.map(|etag| format!("\"{}\"", etag.trim_matches('"'))),
            )
            .send()
            .boxed()
            .await
            .map_err(|error| copy_error("copy part", error))?;
        output
            .copy_part_result()
            .and_then(|result| result.e_tag())
            .map(str::to_string)
            .ok_or_else(|| BlobError::WriteError("backend returned no copied part ETag".into()))
    }

    /// Copies one provider `version` of a source object, or its current bytes while it has the
    /// ETag `if_match`, onto `path` in one request; answers the copy's version id and ETag.
    pub async fn copy_object(
        &self,
        path: &str,
        (source, source_path, version, if_match): (
            &NativeMultipart,
            &str,
            Option<&str>,
            Option<&str>,
        ),
    ) -> Result<(Option<String>, Option<String>), BlobError> {
        let output = self
            .client
            .copy_object()
            .bucket(&self.bucket)
            .key(self.key(path))
            .copy_source(source.copy_source(source_path, version))
            .set_copy_source_if_match(
                if_match.map(|etag| format!("\"{}\"", etag.trim_matches('"'))),
            )
            .send()
            .boxed()
            .await
            .map_err(|error| copy_error("copy object", error))?;
        let etag = output
            .copy_object_result()
            .and_then(|result| result.e_tag());
        let version = output.version_id().map(str::to_string);
        Ok((version, etag.map(str::to_string)))
    }

    /// The copy source naming `path` of this bucket, at `version` when one is given.
    fn copy_source(&self, path: &str, version: Option<&str>) -> String {
        let key = self.key(path);
        let encoded = utf8_percent_encode(&key, COPY_SOURCE);
        match version {
            Some(version) => {
                let version = utf8_percent_encode(version, COPY_SOURCE);
                format!("{}/{encoded}?versionId={version}", self.bucket)
            }
            None => format!("{}/{encoded}", self.bucket),
        }
    }

    /// Creates an empty object at `path` for the write of `version` only while the path is
    /// free, the one create-only request every S3 storage honors; completions and copies may
    /// ignore their conditions. This version's own reservation there counts as reserved.
    pub async fn reserve(&self, path: &str, version: &str) -> Result<(), BlobError> {
        let put = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key(path))
            .if_none_match("*")
            .metadata(RESERVATION, version)
            .body(ByteStream::from_static(b""))
            .send()
            .boxed()
            .await;
        let Err(error) = put else {
            return Ok(());
        };
        match status(&error) {
            // 409 answers a conditional write of the same key still under way.
            Some(412 | 409) => match self.reservation(path).await? {
                Some(found) if found.version == version => Ok(()),
                _ => Err(BlobError::PathTaken),
            },
            Some(501) => Err(BlobError::ConditionalUnsupported),
            _ if error.code() == Some("NotImplemented") => Err(BlobError::ConditionalUnsupported),
            _ => Err(BlobError::WriteError(format!(
                "reserve the path: {}",
                DisplayErrorContext(&error)
            ))),
        }
    }

    /// Whether `path` still holds the empty reservation of `version`, checked right before the
    /// request that replaces it.
    pub async fn holds(&self, path: &str, version: &str) -> Result<bool, BlobError> {
        let found = self.reservation(path).await?;
        Ok(found.is_some_and(|found| found.version == version))
    }

    /// The empty reservation `path` holds now, if it holds one.
    async fn reservation(&self, path: &str) -> Result<Option<Reservation>, BlobError> {
        let head = self.client.head_object().bucket(&self.bucket);
        let output = match head.key(self.key(path)).send().boxed().await {
            Ok(output) => output,
            Err(error) if status(&error) == Some(404) => return Ok(None),
            Err(error) => {
                let message = format!("head object: {}", DisplayErrorContext(&error));
                return Err(BlobError::ReadError(message));
            }
        };
        let version = output.metadata().and_then(|found| found.get(RESERVATION));
        let (Some(version), Some(0)) = (version, output.content_length()) else {
            return Ok(None);
        };
        Ok(Some(Reservation {
            version: version.clone(),
            object_version: output
                .version_id()
                .filter(|id| *id != "null")
                .map(Into::into),
            etag: output.e_tag().map(str::to_string),
        }))
    }

    /// Removes the empty reservation of `version` at `path` and never anything else: exactly
    /// its object version where the storage keeps versions, else only with its ETag as the
    /// delete's condition. Where neither holds, the reservation stays.
    pub async fn unreserve(&self, path: &str, version: &str) -> Result<(), BlobError> {
        let found = self.reservation(path).await?;
        let Some(found) = found.filter(|found| found.version == version) else {
            return Ok(());
        };
        let delete = self.client.delete_object().bucket(&self.bucket);
        let delete = delete.key(self.key(path));
        let request = match (found.object_version, found.etag) {
            (Some(id), _) => delete.version_id(id),
            (None, Some(etag)) => delete.if_match(etag),
            (None, None) => return Ok(()),
        };
        let error = match request.send().boxed().await {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
        match status(&error) {
            // Other bytes, or none, are at the path now; they stay. So does a reservation the
            // storage cannot delete by its ETag alone.
            Some(412 | 404 | 501) => Ok(()),
            _ if error.code() == Some("NotImplemented") => Ok(()),
            _ => Err(BlobError::DeleteError(format!(
                "delete reservation: {}",
                DisplayErrorContext(&error)
            ))),
        }
    }

    /// Assembles the listed parts, in order, under the backend ETags recorded for them, and
    /// answers the object's version id and ETag. `if_not_exists` fails with `PathTaken` on a
    /// taken key.
    pub async fn complete(
        &self,
        path: &str,
        upload_id: &str,
        parts: &[(u16, String)],
        if_not_exists: bool,
    ) -> Result<(Option<String>, Option<String>), BlobError> {
        let parts = parts
            .iter()
            .map(|(part_number, etag)| {
                CompletedPart::builder()
                    .part_number(i32::from(*part_number))
                    .e_tag(etag)
                    .build()
            })
            .collect();
        let request = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(self.key(path))
            .upload_id(upload_id)
            .set_if_none_match(if_not_exists.then(|| "*".to_string()))
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            );
        let output = request
            .send()
            .boxed()
            .await
            .map_err(|error| match status(&error) {
                Some(412) => BlobError::PathTaken,
                _ => write_error("complete", error),
            })?;
        let tag = output.e_tag().map(str::to_string);
        Ok((output.version_id().map(str::to_string), tag))
    }

    /// Frees the stored parts of one upload; an upload the backend no longer has is gone already.
    pub async fn abort(&self, path: &str, upload_id: &str) -> Result<(), BlobError> {
        match self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(self.key(path))
            .upload_id(upload_id)
            .send()
            .boxed()
            .await
        {
            Ok(_) => Ok(()),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_no_such_upload()) =>
            {
                Ok(())
            }
            Err(error) => Err(BlobError::DeleteError(format!(
                "abort multipart upload: {}",
                DisplayErrorContext(&error)
            ))),
        }
    }

    /// Aborts every unfinished upload of exactly this path, such as one an abandoned writer left.
    pub async fn abort_path(&self, path: &str) -> Result<usize, BlobError> {
        let key = self.key(path);
        let mut aborted = 0;
        let mut key_marker = None;
        let mut upload_marker = None;
        loop {
            let page = self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket)
                .prefix(&key)
                .set_key_marker(key_marker.take())
                .set_upload_id_marker(upload_marker.take())
                .send()
                .boxed()
                .await
                .map_err(|error| {
                    BlobError::DeleteError(format!(
                        "list multipart uploads: {}",
                        DisplayErrorContext(&error)
                    ))
                })?;
            for upload in page.uploads() {
                if upload.key() != Some(key.as_str()) {
                    continue;
                }
                if let Some(upload_id) = upload.upload_id() {
                    self.abort(path, upload_id).await?;
                    aborted += 1;
                }
            }
            if !page.is_truncated().unwrap_or(false) {
                return Ok(aborted);
            }
            key_marker = page.next_key_marker().map(str::to_string);
            upload_marker = page.next_upload_id_marker().map(str::to_string);
            if key_marker.is_none() && upload_marker.is_none() {
                return Ok(aborted);
            }
        }
    }
}

/// The ETag a provider gives an object assembled from these parts: the MD5 of their binary MD5s
/// and the part count. `None` when a part ETag is not a plain MD5, as with SSE-KMS.
pub fn multipart_etag(etags: &[(u16, String)]) -> Option<String> {
    let mut digests = Vec::with_capacity(etags.len() * 16);
    for (_, etag) in etags {
        let digest = hex::decode(etag.trim_matches('"')).ok()?;
        if digest.len() != 16 {
            return None;
        }
        digests.extend(digest);
    }
    let digest = md5::compute(&digests);
    Some(format!("{}-{}", hex::encode(digest.0), etags.len()))
}

/// A client from an S3 backend's service config; tenant backends pass their guard.
fn config_client(
    config: &HashMap<String, String>,
    guard: Option<&EgressGuard>,
) -> Result<Client, BlobError> {
    let endpoint = required_key(config, "endpoint")?;
    let credentials = Credentials::new(
        required_key(config, "access_key_id")?,
        required_key(config, "secret_access_key")?,
        None,
        None,
        "Aruna_v3",
    );
    // The same region the backend's opendal operator signs with.
    let region = s3_region(config, environment_variable).ok_or_else(|| {
        BlobError::OperatorCreationFailed("the S3 backend names no region".to_string())
    })?;
    let path_style = config
        .get("force_path_style")
        .is_none_or(|value| value.trim().parse::<bool>().unwrap_or(true));
    // Built without the shared config loader, so no environment or profile setting applies.
    let mut builder = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(region))
        .credentials_provider(credentials)
        .endpoint_url(endpoint)
        .force_path_style(path_style)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
    if let Some(guard) = guard {
        let client = guard
            .sdk_client(endpoint)
            .map_err(|error| BlobError::OperatorCreationFailed(error.to_string()))?;
        builder = builder.http_client(client);
    }
    Ok(Client::from_conf(builder.build()))
}

/// Finds `bucket` on a tenant S3 storage or creates it in the storage's region; `true` when
/// Aruna created it.
pub async fn ensure_bucket(
    config: &HashMap<String, String>,
    bucket: &str,
    guard: &EgressGuard,
) -> Result<bool, BlobError> {
    let client = config_client(config, Some(guard))?;
    if head_bucket(&client, bucket).await? {
        return Ok(false);
    }
    let region = s3_region(config, environment_variable).unwrap_or_default();
    let mut request = client.create_bucket().bucket(bucket);
    if let Some(constraint) = location_constraint(&region) {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(constraint)
                .build(),
        );
    }
    match request.send().boxed().await {
        Ok(_) => Ok(true),
        Err(error) => match error.as_service_error() {
            Some(service) if service.is_bucket_already_owned_by_you() => Ok(false),
            Some(service) if service.is_bucket_already_exists() => Err(BlobError::WriteError(
                format!("The bucket name `{bucket}` is taken by another account."),
            )),
            _ => Err(bucket_error(bucket, "create", &error)),
        },
    }
}

/// Whether `bucket` exists; nothing is created.
pub async fn bucket_exists(
    config: &HashMap<String, String>,
    bucket: &str,
    guard: &EgressGuard,
) -> Result<bool, BlobError> {
    head_bucket(&config_client(config, Some(guard))?, bucket).await
}

async fn head_bucket(client: &Client, bucket: &str) -> Result<bool, BlobError> {
    match client.head_bucket().bucket(bucket).send().boxed().await {
        Ok(_) => Ok(true),
        Err(error) if status(&error) == Some(404) => Ok(false),
        Err(error) => Err(bucket_error(bucket, "check", &error)),
    }
}

/// Deletes `bucket` only while it holds no object; answers whether it is gone. A missing
/// bucket counts as deleted, since an earlier attempt may have deleted it already.
pub async fn delete_empty(
    config: &HashMap<String, String>,
    bucket: &str,
    guard: &EgressGuard,
) -> Result<bool, BlobError> {
    let client = config_client(config, Some(guard))?;
    let listed = client.list_objects_v2().bucket(bucket).max_keys(1);
    let listed = listed.send().boxed().await;
    match listed {
        Ok(listed) if !listed.contents().is_empty() => return Ok(false),
        Ok(_) => {}
        Err(error) if status(&error) == Some(404) => return Ok(true),
        Err(error) => return Err(bucket_error(bucket, "list", &error)),
    }
    // A bucket with old object versions answers 409 and stays.
    match client.delete_bucket().bucket(bucket).send().boxed().await {
        Ok(_) => Ok(true),
        Err(error) if status(&error) == Some(404) => Ok(true),
        Err(error) if status(&error) == Some(409) => Ok(false),
        Err(error) => Err(bucket_error(bucket, "delete", &error)),
    }
}

/// Proves the keys of a tenant S3 storage without a bucket. A key may lack the right to
/// list buckets; a wrong key or secret is refused.
pub async fn check_keys(
    config: &HashMap<String, String>,
    guard: &EgressGuard,
) -> Result<(), BlobError> {
    let client = config_client(config, Some(guard))?;
    let Err(error) = client.list_buckets().send().boxed().await else {
        return Ok(());
    };
    tracing::warn!(code = ?error.code(), status = ?status(&error), "tenant storage key check failed");
    let message = match error.code() {
        Some("AccessDenied") => return Ok(()),
        Some("InvalidAccessKeyId") => "The storage does not know this access key.",
        Some("SignatureDoesNotMatch") => "The secret key does not match the access key.",
        Some(_) => "The storage refused the keys. The node log has details.",
        None => "The storage could not be reached. Check the endpoint.",
    };
    Err(BlobError::WriteError(message.to_string()))
}

fn status<E>(error: &aws_sdk_s3::error::SdkError<E>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

/// A short reason for a failed bucket check; the full error goes to the node log.
fn bucket_error<E>(bucket: &str, call: &str, error: &aws_sdk_s3::error::SdkError<E>) -> BlobError
where
    E: std::error::Error + Send + Sync + 'static,
{
    tracing::warn!(bucket, call, status = ?status(error), "tenant bucket check failed");
    let message = match status(error) {
        Some(403) => format!("No access to bucket `{bucket}`. Check that the key may use it."),
        Some(301) => format!("The bucket `{bucket}` is in another region."),
        Some(_) => {
            format!("The storage refused to {call} bucket `{bucket}`. The node log has details.")
        }
        None => "The storage could not be reached. Check the endpoint.".to_string(),
    };
    BlobError::WriteError(message)
}

fn write_error<E>(call: &str, error: aws_sdk_s3::error::SdkError<E>) -> BlobError
where
    E: std::error::Error + Send + Sync + 'static,
{
    BlobError::WriteError(format!(
        "{call} multipart upload: {}",
        DisplayErrorContext(&error)
    ))
}

/// A refused copy: a source that lost its ETag, a storage that cannot copy, or another error.
fn copy_error<E>(call: &str, error: aws_sdk_s3::error::SdkError<E>) -> BlobError
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
{
    match status(&error) {
        Some(412) => BlobError::IntegrityCheckFailed("the source lost its ETag".into()),
        // Only a refusal of the call itself says the storage cannot copy.
        Some(501) => BlobError::CopyUnsupported,
        _ if error.code() == Some("NotImplemented") => BlobError::CopyUnsupported,
        _ => write_error(call, error),
    }
}

/// An empty object holding a path for the write of `version`.
struct Reservation {
    version: String,
    /// The object version it is, where the storage keeps versions.
    object_version: Option<String>,
    etag: Option<String>,
}

/// A part body the SDK sends as it arrives, without buffering it.
struct PartBody {
    stream: BoxStream<'static, Result<Bytes, StreamError>>,
    size: u64,
}

impl http_body::Body for PartBody {
    type Data = Bytes;
    type Error = StreamError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, StreamError>>> {
        self.get_mut()
            .stream
            .poll_next_unpin(context)
            .map(|item| item.map(|chunk| chunk.map(Frame::data)))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.size)
    }
}

fn location_constraint(region: &str) -> Option<BucketLocationConstraint> {
    (region != IMPLICIT_REGION).then(|| BucketLocationConstraint::from(region))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::error::SdkError;
    use tokio::net::TcpListener;

    // The AWS SDK only ships an HTTPS connector with `default-https-client`;
    // a ConstructionFailure here means it was dropped from the build.
    #[tokio::test]
    async fn https_connector_present() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let client = create_s3_client(&format!("https://{addr}"), None, "key", "secret", true)
            .await
            .unwrap();

        let err = client
            .get_bucket_location()
            .bucket("bucket")
            .send()
            .await
            .expect_err("handshake against a closed socket must fail");

        assert!(matches!(err, SdkError::DispatchFailure(_)), "{err:?}");
    }

    /// Answers every request with `status` and an S3 error `code`.
    async fn answering(status: &'static str, code: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buffer = vec![0u8; 8192];
                let _ = stream.read(&mut buffer).await;
                let body = format!("<Error><Code>{code}</Code></Error>");
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn missing_bucket_deleted() {
        let guard = EgressGuard::new(aruna_core::egress::EgressPolicy::loopback()).unwrap();
        let config = |endpoint: String| {
            HashMap::from([
                ("endpoint".to_string(), endpoint),
                ("region".to_string(), "us-east-1".to_string()),
                ("access_key_id".to_string(), "key".to_string()),
                ("secret_access_key".to_string(), "secret".to_string()),
            ])
        };
        // An earlier attempt deleted the bucket, so the cleanup is done.
        let gone = config(answering("404 Not Found", "NoSuchBucket").await);
        assert_eq!(delete_empty(&gone, "team-a-x", &guard).await, Ok(true));
        // Any other refusal stays an error, so the cleanup is tried again.
        let denied = config(answering("403 Forbidden", "AccessDenied").await);
        assert!(delete_empty(&denied, "team-a-x", &guard).await.is_err());
    }

    #[test]
    fn composes_multipart_etag() {
        // Two parts of "a" and "b": the provider ETag is md5(md5(a) ++ md5(b)) with "-2".
        let part = |bytes: &[u8]| format!("\"{}\"", hex::encode(md5::compute(bytes).0));
        let etags = [(1, part(b"a")), (2, part(b"b"))];
        let joined = [md5::compute(b"a").0, md5::compute(b"b").0].concat();
        let expected = format!("{}-2", hex::encode(md5::compute(joined).0));

        assert_eq!(multipart_etag(&etags), Some(expected));
        assert_eq!(multipart_etag(&[(1, "\"kms-tag\"".to_string())]), None);
    }

    #[test]
    fn omits_default_constraint() {
        // us-east-1 must stay implicit; every other region must be named.
        assert!(location_constraint("us-east-1").is_none());
        assert_eq!(
            location_constraint("eu-central-1"),
            Some(BucketLocationConstraint::EuCentral1)
        );
    }
}
