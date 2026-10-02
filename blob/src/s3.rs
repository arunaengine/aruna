//! Creates an aws S3 client from backend config and makes a bucket with the right region.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::egress::EgressGuard;
use aruna_core::errors::BlobError;
use aruna_core::stream::{BackendStream, BoxStream, StreamError};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, CompletedMultipartUpload, CompletedPart, CreateBucketConfiguration,
};
use bytes::Bytes;
use futures::StreamExt;
use http_body::{Frame, SizeHint};
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

const DEFAULT_REGION: &str = "eu-central-1";
// AWS rejects CreateBucket requests that name us-east-1 explicitly.
const IMPLICIT_REGION: &str = "us-east-1";

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
        let endpoint = required_key(config, "endpoint")?;
        let credentials = Credentials::new(
            required_key(config, "access_key_id")?,
            required_key(config, "secret_access_key")?,
            None,
            None,
            "Aruna_v3",
        );
        let region = config
            .get("region")
            .cloned()
            .unwrap_or_else(|| DEFAULT_REGION.to_string());
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
        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: bucket.to_string(),
            root: root.to_string(),
        })
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
            .await
            .map_err(|error| write_error("upload part", error))?;
        output
            .e_tag()
            .map(str::to_string)
            .ok_or_else(|| BlobError::WriteError("backend returned no part ETag".to_string()))
    }

    /// Assembles the listed parts, in order, under the backend ETags recorded for them.
    pub async fn complete(
        &self,
        path: &str,
        upload_id: &str,
        parts: &[(u16, String)],
    ) -> Result<(), BlobError> {
        let parts = parts
            .iter()
            .map(|(part_number, etag)| {
                CompletedPart::builder()
                    .part_number(i32::from(*part_number))
                    .e_tag(etag)
                    .build()
            })
            .collect();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(self.key(path))
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map_err(|error| write_error("complete", error))?;
        Ok(())
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

fn write_error<E>(call: &str, error: aws_sdk_s3::error::SdkError<E>) -> BlobError
where
    E: std::error::Error + Send + Sync + 'static,
{
    BlobError::WriteError(format!(
        "{call} multipart upload: {}",
        DisplayErrorContext(&error)
    ))
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
