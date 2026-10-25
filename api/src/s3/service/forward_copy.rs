//! Copies into a distributed bucket across ranges and coordinators: a member that keeps the
//! copy source streams it to the destination's coordinator as a forwarded write; otherwise the
//! coordinator copies itself when it keeps the source's records (D33). Nothing is queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{BucketInfo, CONTENT_TYPE_KEY, UserAccess};
use aruna_core::structs::storage::distribution::{ReadRoute, RouteError};
use aruna_operations::distribution::guard::{record_holders, route_read};
use aruna_operations::s3::object::copy::{CopySourceConditions, read_copy_source};
use aruna_operations::s3::object::get::{GetObjectInput, GetObjectResult, ObjectRangeRequest};
use futures_util::TryStreamExt;
use http::Method;
use s3s::dto::{
    CopyObjectOutput, CopyObjectResult, CopyPartResult, ETag, StreamingBlob, Timestamp,
    TimestampFormat, UploadPartCopyOutput,
};
use s3s::{S3Error, S3ErrorCode, S3Response, S3Result};

use super::ArunaS3Service;
use super::forward::{Forward, ForwardBody, ForwardRequest, answer_text, header, xml_field};
use crate::s3::error::{IntoS3Error, guard_error};

/// How a copy reaches the coordinator of its destination key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum CopyPath {
    /// This member keeps the source and streams it as a forwarded write.
    Stream,
    /// The coordinator keeps the source's records and copies itself.
    Delegate,
    /// Only another member keeps the source: this member reads it through that holder.
    Relay(Forward),
}

/// The copy conditions of the source as the holder's own read sees them.
const COPY_CONDITIONS: &[(&str, &str)] = &[
    ("x-amz-copy-source-if-match", "if-match"),
    ("x-amz-copy-source-if-none-match", "if-none-match"),
    ("x-amz-copy-source-if-modified-since", "if-modified-since"),
    (
        "x-amz-copy-source-if-unmodified-since",
        "if-unmodified-since",
    ),
    ("x-amz-copy-source-range", "range"),
];

/// The holder's read of a copy source, as a copy request names it.
fn source_read(
    (bucket, key, version_id): (&str, &str, Option<ulid::Ulid>),
    headers: &http::HeaderMap,
) -> ForwardRequest {
    let headers = (COPY_CONDITIONS.iter())
        .filter_map(|(from, to)| {
            let value = headers.get(*from)?.to_str().ok()?;
            Some((to.to_string(), value.to_string()))
        })
        .collect();
    ForwardRequest {
        method: Method::GET,
        operation: "GetObject",
        bucket: bucket.to_string(),
        key: key.to_string(),
        query: version_id.map(|version_id| format!("versionId={version_id}")),
        headers,
        body: ForwardBody::Empty,
    }
}

/// The metadata a holder answered with, as a copy keeps it.
fn answered_metadata(response: &reqwest::Response) -> HashMap<String, String> {
    let mut metadata: HashMap<String, String> = (response.headers().iter())
        .filter_map(|(name, value)| {
            let name = name.as_str().strip_prefix("x-amz-meta-")?;
            Some((name.to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    if let Some(content_type) = header(response, "content-type") {
        metadata.insert(CONTENT_TYPE_KEY.to_string(), content_type);
    }
    metadata
}

/// The source a member streams: one whole version, or one byte range for a part.
pub(super) struct CopySource<'a> {
    pub bucket: &'a BucketInfo,
    pub name: String,
    pub key: String,
    pub version_id: Option<ulid::Ulid>,
    pub range: Option<ObjectRangeRequest>,
    pub auth: &'a AuthContext,
    pub conditions: &'a CopySourceConditions,
}

/// Neither this member nor the coordinator keeps the source; the hint names who does.
fn source_elsewhere(set_id: ulid::Ulid, holders: &[NodeId]) -> S3Error {
    let holders: Vec<String> = holders.iter().map(ToString::to_string).collect();
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("CopySourceNotHeld".into()),
        format!(
            "Neither this node nor the coordinator of the destination keeps the records of the \
             copy source's range in distributed bucket {set_id}. Send the copy to a node that \
             keeps them: {}.",
            holders.join(", ")
        ),
    );
    error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
    error
}

/// The headers that store a streamed copy with the metadata it keeps.
fn copy_headers(metadata: &HashMap<String, String>, length: u64) -> Vec<(String, String)> {
    let mut headers = vec![("content-length".to_string(), length.to_string())];
    for (name, value) in metadata {
        match name.as_str() {
            CONTENT_TYPE_KEY => headers.push(("content-type".to_string(), value.clone())),
            // Internal tags of the source stay with the source.
            name if name.starts_with("aruna") => {}
            name => headers.push((format!("x-amz-meta-{name}"), value.clone())),
        }
    }
    headers
}

fn copied_at(text: Option<String>) -> Timestamp {
    text.and_then(|text| Timestamp::parse(TimestampFormat::DateTime, &text).ok())
        .unwrap_or_else(|| SystemTime::now().into())
}

fn e_tag(text: Option<String>) -> Option<ETag> {
    text.and_then(|value| ETag::parse_http_header(value.as_bytes()).ok())
}

impl ArunaS3Service {
    /// Whether this node answers reads of `key` itself: an ordinary bucket, or a caught-up
    /// member that keeps the records of the key's range.
    async fn keeps_source(&self, bucket: &BucketInfo, key: &str) -> S3Result<bool> {
        let Some(binding) = bucket.distribution.as_ref() else {
            return Ok(true);
        };
        match route_read(self.state.as_ref(), binding, key, false).await {
            Ok(ReadRoute::Local { catching_up }) => Ok(!catching_up),
            Ok(ReadRoute::Holder { .. }) => Ok(false),
            Err(error) => Err(guard_error(&error)),
        }
    }

    /// Refuses a copy this node writes itself when it does not keep the source.
    pub(super) async fn ensure_source(&self, bucket: &BucketInfo, key: &str) -> S3Result<()> {
        if self.keeps_source(bucket, key).await? {
            return Ok(());
        }
        let Some(binding) = bucket.distribution.as_ref() else {
            return Ok(());
        };
        let holders = record_holders(self.state.as_ref(), binding, key)
            .await
            .map_err(|error| guard_error(&error))?;
        Err(source_elsewhere(binding.set_id, &holders))
    }

    /// How a copy whose destination `forward` coordinates reaches it.
    pub(super) async fn copy_path(
        &self,
        bucket: &BucketInfo,
        key: &str,
        forward: &Forward,
    ) -> S3Result<CopyPath> {
        if self.keeps_source(bucket, key).await? {
            return Ok(CopyPath::Stream);
        }
        let Some(binding) = bucket.distribution.as_ref() else {
            return Ok(CopyPath::Stream);
        };
        let snapshot = self.snapshot(binding).await?;
        let range = (snapshot.range_for(key))
            .ok_or_else(|| guard_error(&RouteError::NoRange(binding.set_id).into()))?;
        let holders: Vec<NodeId> = (snapshot.record_holders(range).into_iter())
            .map(|member| member.node_id)
            .collect();
        if holders.contains(&forward.node_id) {
            return Ok(CopyPath::Delegate);
        }
        match holders.iter().find(|node_id| **node_id != self.node_id) {
            Some(node_id) => Ok(CopyPath::Relay(Forward {
                set_id: binding.set_id,
                range_id: range.range_id,
                node_id: *node_id,
            })),
            None => Err(source_elsewhere(binding.set_id, &holders)),
        }
    }

    /// Reads a copy source through the member that keeps it, under the caller's authority
    /// there; a failed condition fails the copy.
    pub(super) async fn read_through(
        &self,
        user: &UserAccess,
        holder: &Forward,
        source: (&str, &str, Option<ulid::Ulid>),
        headers: &http::HeaderMap,
    ) -> S3Result<reqwest::Response> {
        match self
            .forward_read(user, holder, source_read(source, headers))
            .await
        {
            Err(error) if error.status_code() == Some(http::StatusCode::NOT_MODIFIED) => Err(
                s3s::s3_error!(PreconditionFailed, "The copy source changed"),
            ),
            answer => answer,
        }
    }

    /// Streams a source read through its holder to the destination's coordinator.
    pub(super) async fn relay_copy(
        &self,
        user: &UserAccess,
        forward: &Forward,
        target: (String, String),
        read: reqwest::Response,
        metadata: Option<HashMap<String, String>>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let version = header(&read, "x-amz-version-id");
        let length = (header(&read, "content-length").and_then(|value| value.parse().ok()))
            .ok_or_else(|| s3s::s3_error!(ServiceUnavailable, "The holder named no length"))?;
        let metadata = metadata.unwrap_or_else(|| answered_metadata(&read));
        let headers = copy_headers(&metadata, length);
        let body = StreamingBlob::wrap(read.bytes_stream().map_err(std::io::Error::other));
        self.send_copy(user, forward, target, (headers, body), version)
            .await
    }

    /// Streams a source range read through its holder to the upload's coordinator.
    pub(super) async fn relay_part(
        &self,
        user: &UserAccess,
        forward: &Forward,
        target: (String, String, String),
        read: reqwest::Response,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let version = header(&read, "x-amz-version-id");
        let length = header(&read, "content-length")
            .ok_or_else(|| s3s::s3_error!(ServiceUnavailable, "The holder named no length"))?;
        let body = StreamingBlob::wrap(read.bytes_stream().map_err(std::io::Error::other));
        let headers = vec![("content-length".to_string(), length)];
        self.send_part(user, forward, target, (headers, body), version)
            .await
    }

    /// Reads the source here, its conditions checked first, through a holder when this member
    /// keeps only its record.
    async fn read_copy(
        &self,
        source: CopySource<'_>,
        token: Option<&aruna_operations::s3::object::get::TokenRead>,
    ) -> S3Result<GetObjectResult> {
        let input = GetObjectInput {
            bucket: source.name,
            key: source.key,
            version_id: source.version_id,
            range: source.range,
            group_id: source.bucket.group_id,
            user_identity: source.auth.user_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
        };
        let restrictions = source.auth.path_restrictions.clone();
        read_copy_source(&self.state, input, restrictions, source.conditions, token)
            .await
            .map_err(IntoS3Error::into_s3_error)
    }

    /// Streams a source this member keeps to the destination's coordinator as its write.
    pub(super) async fn stream_copy(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key): (String, String),
        source: CopySource<'_>,
        metadata: Option<HashMap<String, String>>,
        token: Option<&aruna_operations::s3::object::get::TokenRead>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let read = self.read_copy(source, token).await?;
        let version = read.resolved_version_id.or(read.version_id);
        let headers = copy_headers(metadata.as_ref().unwrap_or(&read.metadata), read.info.size);
        let body = StreamingBlob::wrap(read.blob);
        let version = version.map(|version| version.to_string());
        self.send_copy(user, forward, (bucket, key), (headers, body), version)
            .await
    }

    /// Writes a copy's bytes at the destination's coordinator.
    async fn send_copy(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key): (String, String),
        (headers, body): (Vec<(String, String)>, StreamingBlob),
        version: Option<String>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let request = ForwardRequest {
            method: Method::PUT,
            operation: "PutObject",
            bucket,
            key,
            query: None,
            headers,
            body: ForwardBody::Stream(body, None),
        };
        let response = self.forward_write(user, forward, request).await?;
        let put = Self::forwarded_put(&response).output;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: put.e_tag,
                last_modified: Some(SystemTime::now().into()),
                checksum_crc32: put.checksum_crc32,
                checksum_crc32c: put.checksum_crc32c,
                checksum_crc64nvme: put.checksum_crc64nvme,
                checksum_sha1: put.checksum_sha1,
                checksum_sha256: put.checksum_sha256,
                ..Default::default()
            }),
            version_id: put.version_id,
            copy_source_version_id: version,
            ..Default::default()
        }))
    }

    /// Streams a source range this member keeps to the upload's coordinator as one part.
    pub(super) async fn stream_part(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key, query): (String, String, String),
        source: CopySource<'_>,
        token: Option<&aruna_operations::s3::object::get::TokenRead>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let read = self.read_copy(source, token).await?;
        let version = read.resolved_version_id.or(read.version_id);
        let length = (read.resolved_range.as_ref())
            .map_or(read.info.size, |range| range.range.end - range.range.start);
        let headers = vec![("content-length".to_string(), length.to_string())];
        let body = StreamingBlob::wrap(read.blob);
        let version = version.map(|version| version.to_string());
        self.send_part(
            user,
            forward,
            (bucket, key, query),
            (headers, body),
            version,
        )
        .await
    }

    /// Writes one part's bytes at the upload's coordinator.
    async fn send_part(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key, query): (String, String, String),
        (headers, body): (Vec<(String, String)>, StreamingBlob),
        version: Option<String>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let request = ForwardRequest {
            method: Method::PUT,
            operation: "UploadPart",
            bucket,
            key,
            query: Some(query),
            headers,
            body: ForwardBody::Stream(body, None),
        };
        let response = self.forward_write(user, forward, request).await?;
        let part = Self::forwarded_part(&response).output;
        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: part.e_tag,
                last_modified: Some(SystemTime::now().into()),
                checksum_crc32: part.checksum_crc32,
                checksum_crc32c: part.checksum_crc32c,
                checksum_crc64nvme: part.checksum_crc64nvme,
                checksum_sha1: part.checksum_sha1,
                checksum_sha256: part.checksum_sha256,
                ..Default::default()
            }),
            copy_source_version_id: version,
            ..Default::default()
        }))
    }

    /// The coordinator's answer to a delegated CopyObject.
    pub(super) async fn delegated_copy(
        response: reqwest::Response,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let version_id = header(&response, "x-amz-version-id");
        let source_version = header(&response, "x-amz-copy-source-version-id");
        let body = answer_text(response).await?;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: e_tag(xml_field(&body, "ETag")),
                last_modified: Some(copied_at(xml_field(&body, "LastModified"))),
                checksum_crc32: xml_field(&body, "ChecksumCRC32"),
                checksum_crc32c: xml_field(&body, "ChecksumCRC32C"),
                checksum_crc64nvme: xml_field(&body, "ChecksumCRC64NVME"),
                checksum_sha1: xml_field(&body, "ChecksumSHA1"),
                checksum_sha256: xml_field(&body, "ChecksumSHA256"),
                ..Default::default()
            }),
            version_id,
            copy_source_version_id: source_version,
            ..Default::default()
        }))
    }

    /// The coordinator's answer to a delegated UploadPartCopy.
    pub(super) async fn delegated_part(
        response: reqwest::Response,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let source_version = header(&response, "x-amz-copy-source-version-id");
        let body = answer_text(response).await?;
        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: e_tag(xml_field(&body, "ETag")),
                last_modified: Some(copied_at(xml_field(&body, "LastModified"))),
                checksum_crc32: xml_field(&body, "ChecksumCRC32"),
                checksum_crc32c: xml_field(&body, "ChecksumCRC32C"),
                checksum_crc64nvme: xml_field(&body, "ChecksumCRC64NVME"),
                checksum_sha1: xml_field(&body, "ChecksumSHA1"),
                checksum_sha256: xml_field(&body, "ChecksumSHA256"),
                ..Default::default()
            }),
            copy_source_version_id: source_version,
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_keeps_metadata() {
        let metadata = HashMap::from([
            (CONTENT_TYPE_KEY.to_string(), "text/csv".to_string()),
            ("owner".to_string(), "lab".to_string()),
            ("aruna.sync.version".to_string(), "x".to_string()),
        ]);
        let mut headers = copy_headers(&metadata, 12);
        headers.sort();
        assert_eq!(
            headers,
            vec![
                ("content-length".to_string(), "12".to_string()),
                ("content-type".to_string(), "text/csv".to_string()),
                ("x-amz-meta-owner".to_string(), "lab".to_string()),
            ]
        );
    }

    #[test]
    fn read_keeps_conditions() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-amz-copy-source-if-match", "\"abc\"".parse().unwrap());
        headers.insert("x-amz-copy-source-range", "bytes=0-9".parse().unwrap());
        headers.insert("authorization", "AWS4-HMAC-SHA256 secret".parse().unwrap());
        let version = ulid::Ulid::from_bytes([5; 16]);
        let read = source_read(("spire", "a/b", Some(version)), &headers);
        assert_eq!(read.operation, "GetObject");
        assert_eq!(read.query, Some(format!("versionId={version}")));
        let mut passed = read.headers;
        passed.sort();
        assert_eq!(
            passed,
            vec![
                ("if-match".to_string(), "\"abc\"".to_string()),
                ("range".to_string(), "bytes=0-9".to_string()),
            ]
        );
    }

    #[test]
    fn refusal_names_holders() {
        let node = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let error = source_elsewhere(ulid::Ulid::from_bytes([1; 16]), &[node]);
        assert_eq!(
            error.status_code(),
            Some(http::StatusCode::SERVICE_UNAVAILABLE)
        );
        assert!(
            error
                .message()
                .unwrap_or_default()
                .contains(&node.to_string())
        );
    }
}
