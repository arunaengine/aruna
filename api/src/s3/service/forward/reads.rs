//! Sends a GET or HEAD for a key range this member keeps no records of to a holder, chosen by
//! free download slots, with a request-bound delegation (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The holder checks the caller on its own path; a holder that fails is skipped for a while.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::ReadRoute;
use aruna_operations::distribution::guard::route_read;
use aruna_operations::distribution::resolve::key_holders;
use aruna_operations::node::holder_choice::{avoid_holder, choose_holder};
use futures_util::TryStreamExt;
use http::{HeaderMap, Method};
use s3s::dto::{
    ChecksumType, ETag, GetObjectOutput, HeadObjectOutput, StreamingBlob, Timestamp,
    TimestampFormat,
};
use s3s::{S3Response, S3Result};

use super::{Forward, ForwardBody, ForwardRequest, header, passed_headers, passed_query};
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::guard_error;
use crate::s3::service::ArunaS3Service;

/// How long an anonymous caller's stand-in credential lasts; the delegation is shorter.
const ANONYMOUS_SECS: u64 = 60;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// The read of one object the holder receives instead of this node.
pub(in crate::s3::service) fn read_request(
    method: Method,
    operation: &'static str,
    bucket: &str,
    key: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> ForwardRequest {
    ForwardRequest {
        method,
        operation,
        bucket: bucket.to_string(),
        key: key.to_string(),
        query: passed_query(query),
        headers: passed_headers(headers, None),
        body: ForwardBody::Empty,
    }
}

fn last_modified(response: &reqwest::Response) -> Option<Timestamp> {
    header(response, "last-modified")
        .and_then(|value| Timestamp::parse(TimestampFormat::HttpDate, &value).ok())
}

fn e_tag(response: &reqwest::Response) -> Option<ETag> {
    header(response, "etag").and_then(|value| ETag::parse_http_header(value.as_bytes()).ok())
}

/// The user metadata the holder answered with.
fn user_metadata(response: &reqwest::Response) -> Option<HashMap<String, String>> {
    let metadata: HashMap<String, String> = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str().strip_prefix("x-amz-meta-")?;
            Some((name.to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    (!metadata.is_empty()).then_some(metadata)
}

fn checksum_type(response: &reqwest::Response) -> Option<ChecksumType> {
    header(response, "x-amz-checksum-type").map(ChecksumType::from)
}

fn content_length(response: &reqwest::Response) -> Option<i64> {
    header(response, "content-length").and_then(|value| value.parse().ok())
}

/// The holder's GET answer as this node's, its body streamed through.
pub(in crate::s3::service) fn forwarded_get(
    response: reqwest::Response,
) -> S3Response<GetObjectOutput> {
    let status = response.status();
    let output = GetObjectOutput {
        accept_ranges: header(&response, "accept-ranges"),
        checksum_crc32: header(&response, "x-amz-checksum-crc32"),
        checksum_crc32c: header(&response, "x-amz-checksum-crc32c"),
        checksum_crc64nvme: header(&response, "x-amz-checksum-crc64nvme"),
        checksum_sha1: header(&response, "x-amz-checksum-sha1"),
        checksum_sha256: header(&response, "x-amz-checksum-sha256"),
        checksum_type: checksum_type(&response),
        content_length: content_length(&response),
        content_range: header(&response, "content-range"),
        content_type: header(&response, "content-type"),
        e_tag: e_tag(&response),
        last_modified: last_modified(&response),
        metadata: user_metadata(&response),
        version_id: header(&response, "x-amz-version-id"),
        body: None,
        ..Default::default()
    };
    let body = StreamingBlob::wrap(response.bytes_stream().map_err(std::io::Error::other));
    let output = GetObjectOutput {
        body: Some(body),
        ..output
    };
    match status {
        http::StatusCode::PARTIAL_CONTENT => S3Response::with_status(output, status),
        _ => S3Response::new(output),
    }
}

/// The holder's HEAD answer as this node's.
pub(in crate::s3::service) fn forwarded_head(
    response: &reqwest::Response,
) -> S3Response<HeadObjectOutput> {
    S3Response::new(HeadObjectOutput {
        accept_ranges: header(response, "accept-ranges"),
        checksum_crc32: header(response, "x-amz-checksum-crc32"),
        checksum_crc32c: header(response, "x-amz-checksum-crc32c"),
        checksum_crc64nvme: header(response, "x-amz-checksum-crc64nvme"),
        checksum_sha1: header(response, "x-amz-checksum-sha1"),
        checksum_sha256: header(response, "x-amz-checksum-sha256"),
        checksum_type: checksum_type(response),
        content_length: content_length(response),
        content_type: header(response, "content-type"),
        e_tag: e_tag(response),
        last_modified: last_modified(response),
        metadata: user_metadata(response),
        version_id: header(response, "x-amz-version-id"),
        ..Default::default()
    })
}

/// The caller a delegation carries: the Everyone principal has no credential, so the
/// delegation's own short window bounds it.
pub(in crate::s3::service) fn anonymous_window(user: &UserAccess) -> UserAccess {
    let mut user = user.clone();
    if user.user_identity.is_nil() {
        user.expiry = SystemTime::now() + Duration::from_secs(ANONYMOUS_SECS);
    }
    user
}

impl ArunaS3Service {
    /// The holder a GET or HEAD of `key` goes to when this member keeps no records of the
    /// key's range, or `None` when this member answers it. A delegated read is never sent on.
    pub(in crate::s3::service) async fn read_holder(
        &self,
        extensions: &http::Extensions,
        bucket: Option<&BucketInfo>,
        key: &str,
    ) -> S3Result<Option<Forward>> {
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution.as_ref()) else {
            return Ok(None);
        };
        if extensions.get::<DelegatedRequest>().is_some() {
            return Ok(None);
        }
        let (range_id, coordinator) =
            match route_read(self.state.as_ref(), binding, key, false).await {
                Ok(ReadRoute::Holder { range_id, node_id }) => (range_id, node_id),
                Ok(ReadRoute::Local { .. }) => return Ok(None),
                Err(error) => return Err(guard_error(&error)),
            };
        let holders = key_holders(self.state.as_ref(), binding.set_id, key).await;
        let node_id = choose_holder(self.state.as_ref(), &holders, now_ms())
            .await
            .map_or(coordinator, |chosen| chosen.node_id);
        Ok(Some(Forward {
            set_id: binding.set_id,
            range_id,
            node_id,
        }))
    }

    /// Sends one read to the chosen holder; a holder that fails is skipped for a while.
    pub(in crate::s3::service) async fn forward_read(
        &self,
        user: &UserAccess,
        forward: &Forward,
        request: ForwardRequest,
    ) -> S3Result<reqwest::Response> {
        let user = anonymous_window(user);
        let result = self.forward_write(&user, forward, request).await;
        if let Err(error) = &result
            && error
                .status_code()
                .is_none_or(|status| status.is_server_error())
        {
            avoid_holder(self.state.as_ref(), forward.node_id, now_ms());
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder_answer(status: u16) -> reqwest::Response {
        let response = http::Response::builder()
            .status(status)
            .header("content-length", "3")
            .header("content-range", "bytes 0-2/10")
            .header("etag", "\"abc\"")
            .header("last-modified", "Sat, 24 Oct 2026 10:00:00 GMT")
            .header("x-amz-version-id", "01JMETADATA0123456789ABCDE")
            .header("x-amz-meta-origin", "spire")
            .header(
                "x-amz-checksum-sha256",
                "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=",
            )
            .header("x-amz-checksum-type", "FULL_OBJECT")
            .body("abc")
            .unwrap();
        reqwest::Response::from(response)
    }

    #[test]
    fn request_binds_range() {
        let mut headers = HeaderMap::new();
        headers.insert("range", "bytes=0-2".parse().unwrap());
        headers.insert("authorization", "AWS4-HMAC-SHA256 secret".parse().unwrap());
        let query = Some("versionId=01JMETADATA0123456789ABCDE&X-Amz-Signature=secret");
        let request = read_request(Method::GET, "GetObject", "spire", "a.fa", query, &headers);
        assert_eq!(
            request.query.as_deref(),
            Some("versionId=01JMETADATA0123456789ABCDE")
        );
        assert_eq!(
            request.headers,
            vec![("range".to_string(), "bytes=0-2".to_string())]
        );
        assert!(matches!(request.body, ForwardBody::Empty));
    }

    #[test]
    fn partial_answer_kept() {
        let response = forwarded_get(holder_answer(206));
        assert_eq!(response.status, Some(http::StatusCode::PARTIAL_CONTENT));
        let output = response.output;
        assert_eq!(output.content_length, Some(3));
        assert_eq!(output.content_range.as_deref(), Some("bytes 0-2/10"));
        assert_eq!(
            output.version_id.as_deref(),
            Some("01JMETADATA0123456789ABCDE")
        );
        assert!(output.last_modified.is_some());
        assert!(output.e_tag.is_some());
        assert_eq!(
            output.metadata.unwrap().get("origin").map(String::as_str),
            Some("spire")
        );
        assert!(output.checksum_sha256.is_some());
        assert_eq!(
            output.checksum_type.as_ref().map(ChecksumType::as_str),
            Some("FULL_OBJECT")
        );
    }

    #[test]
    fn head_answer_mapped() {
        let output = forwarded_head(&holder_answer(200)).output;
        assert_eq!(output.content_length, Some(3));
        assert!(output.checksum_sha256.is_some());
        assert_eq!(
            output.version_id.as_deref(),
            Some("01JMETADATA0123456789ABCDE")
        );
    }
}
