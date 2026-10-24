//! Forwards an S3 write that reached a member of a distributed bucket to the coordinator of the
//! key's range, with a request-bound delegation in place of the caller's signature (D33). An
//! unreachable coordinator fails the write at once; nothing is queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::WriteRoute;
use aruna_core::structs::storage::distribution::delegation::{DELEGATION_HEADER, request_digest};
use aruna_operations::distribution::delegate::{DelegationInput, issue_delegation, node_urls};
use aruna_operations::distribution::guard::route_write;
use http::{HeaderMap, Method};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use s3s::dto::{DeleteObjectOutput, ETag, PutObjectOutput, StreamingBlob};
use s3s::{S3Error, S3ErrorCode, S3Response, S3Result, s3_error};
use tracing::warn;
use ulid::Ulid;

use super::ArunaS3Service;
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::guard_error;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Headers that describe the body; the delegation binds their values.
const DIGEST_HEADERS: &[&str] = &[
    "content-length",
    "content-md5",
    "content-type",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
];

/// Further headers the coordinator needs to store the same object.
const PASSED_HEADERS: &[&str] = &[
    "cache-control",
    "content-disposition",
    "content-language",
    "expires",
    "if-match",
    "if-none-match",
    "x-amz-checksum-algorithm",
    "x-amz-checksum-type",
    "x-amz-mp-object-size",
    "x-amz-tagging",
];

/// Characters a key keeps unescaped in a forwarded path; `/` separates its segments.
const KEY_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .unwrap_or_default()
});

/// The coordinator a write goes to when this node does not coordinate the key's range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Forward {
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub node_id: NodeId,
}

/// One forwarded request: what the coordinator receives besides the delegation.
pub(super) struct ForwardRequest {
    pub method: Method,
    pub operation: &'static str,
    pub bucket: String,
    pub key: String,
    pub query: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Option<StreamingBlob>,
}

/// The digest of a forwarded request as its target sees it.
pub(crate) fn forwarded_digest(
    method: &Method,
    bucket: &str,
    key: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> [u8; 32] {
    let pairs: Vec<(&str, &str)> = DIGEST_HEADERS
        .iter()
        .filter_map(|name| Some((*name, headers.get(*name)?.to_str().ok()?)))
        .collect();
    request_digest(method.as_str(), bucket, key, query, &pairs)
}

/// The headers of the original request that travel with it, lowercase.
pub(super) fn passed_headers(
    headers: &HeaderMap,
    content_length: Option<i64>,
) -> Vec<(String, String)> {
    let mut passed: Vec<(String, String)> = headers
        .iter()
        .filter(|(name, _)| {
            let name = name.as_str();
            (DIGEST_HEADERS.contains(&name) && name != "content-length")
                || PASSED_HEADERS.contains(&name)
                || name.starts_with("x-amz-meta-")
        })
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    if let Some(length) = content_length {
        passed.push(("content-length".to_string(), length.to_string()));
    }
    passed
}

/// The query of a forwarded request without presigning parameters.
pub(super) fn passed_query(query: Option<&str>) -> Option<String> {
    let kept: Vec<&str> = query?
        .split('&')
        .filter(|pair| !pair.is_empty() && !pair.to_ascii_lowercase().starts_with("x-amz-"))
        .collect();
    (!kept.is_empty()).then(|| kept.join("&"))
}

fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

fn unreachable_error(forward: &Forward) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("CoordinatorUnreachable".into()),
        format!(
            "Range {} of distributed bucket {} is coordinated by node {}, which is unreachable. \
             The bucket is degraded and writes to this range fail until the coordinator returns.",
            forward.range_id, forward.set_id, forward.node_id
        ),
    );
    error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
    error
}

/// The text between `<tag>` and `</tag>` of an S3 error body, with basic entities decoded.
pub(super) fn xml_field(body: &str, tag: &str) -> Option<String> {
    let start = body.find(&format!("<{tag}>"))? + tag.len() + 2;
    let end = start + body.get(start..)?.find(&format!("</{tag}>"))?;
    let text = body.get(start..end)?;
    Some(
        text.replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&"),
    )
}

/// The coordinator's refusal as this node's answer; a denial says why node globs help.
fn coordinator_error(forward: &Forward, status: http::StatusCode, body: &str) -> S3Error {
    let code = xml_field(body, "Code")
        .map(|code| {
            S3ErrorCode::from_bytes(code.as_bytes())
                .unwrap_or_else(|| S3ErrorCode::Custom(code.into()))
        })
        .unwrap_or(S3ErrorCode::InternalError);
    let message = xml_field(body, "Message").unwrap_or_default();
    let message = match status {
        http::StatusCode::FORBIDDEN => format!(
            "The coordinator node {} denied the write: {message} Every member checks the \
             caller on its own path; grant the bucket with a node glob such as \
             /{{realm}}/g/{{group}}/data/*/{{bucket}}/**.",
            forward.node_id
        ),
        _ => message,
    };
    let mut error = S3Error::with_message(code, message);
    error.set_status_code(status);
    error
}

pub(super) fn header(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

impl ArunaS3Service {
    /// The coordinator a write to `key` goes to, or `None` when this node writes it. A request
    /// that arrived with a delegation is never forwarded again.
    pub(super) async fn forward_target(
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
        match route_write(self.state.as_ref(), binding, key).await {
            Ok(WriteRoute::Local { .. }) => Ok(None),
            Ok(WriteRoute::Forward {
                range_id, node_id, ..
            }) => Ok(Some(Forward {
                set_id: binding.set_id,
                range_id,
                node_id,
            })),
            Err(error) => Err(guard_error(&error)),
        }
    }

    /// Sends one request to the coordinator and returns its successful answer.
    pub(super) async fn forward_write(
        &self,
        user: &UserAccess,
        forward: &Forward,
        request: ForwardRequest,
    ) -> S3Result<reqwest::Response> {
        let urls = node_urls(self.state.as_ref(), forward.node_id).await;
        let Some(base) = urls.and_then(|urls| urls.s3) else {
            return Err(unreachable_error(forward));
        };
        let mut headers = HeaderMap::new();
        for (name, value) in &request.headers {
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(name.as_bytes()),
                http::HeaderValue::from_str(value),
            ) {
                headers.insert(name, value);
            }
        }
        let query = request.query.as_deref();
        let digest = forwarded_digest(
            &request.method,
            &request.bucket,
            &request.key,
            query,
            &headers,
        );
        let input = DelegationInput {
            set_id: forward.set_id,
            range_id: forward.range_id,
            target: forward.node_id,
            user_id: user.user_identity,
            group_id: user.group_id,
            credential_id: user.access_key.clone(),
            restrictions: user.path_restrictions.clone(),
            credential_expiry: unix_secs(user.expiry),
            operation: request.operation.to_string(),
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            copy_source: None,
            request_digest: digest,
        };
        let delegation = issue_delegation(self.state.as_ref(), input, unix_secs(SystemTime::now()))
            .and_then(|signed| Ok(signed.encode()?))
            .map_err(|error| s3_error!(InternalError, "Could not delegate the write: {error}"))?;
        let path = utf8_percent_encode(&request.key, KEY_SET);
        let mut url = format!("{}/{}/{path}", base.trim_end_matches('/'), request.bucket);
        if let Some(query) = query {
            url = format!("{url}?{query}");
        }
        let mut outgoing = CLIENT
            .request(request.method.clone(), &url)
            .headers(headers)
            .header(DELEGATION_HEADER, delegation);
        if let Some(body) = request.body {
            outgoing = outgoing.body(reqwest::Body::wrap_stream(body));
        }
        let response = match outgoing.send().await {
            Ok(response) => response,
            Err(error) => {
                warn!(
                    node_id = %forward.node_id,
                    connect = error.is_connect(),
                    error = %error,
                    "A distributed bucket coordinator did not take a forwarded write"
                );
                return Err(unreachable_error(forward));
            }
        };
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        Err(coordinator_error(forward, status, &body))
    }

    pub(super) fn forwarded_put(response: &reqwest::Response) -> S3Response<PutObjectOutput> {
        let e_tag = header(response, "etag")
            .and_then(|value| ETag::parse_http_header(value.as_bytes()).ok());
        S3Response::new(PutObjectOutput {
            e_tag,
            version_id: header(response, "x-amz-version-id"),
            checksum_crc32: header(response, "x-amz-checksum-crc32"),
            checksum_crc32c: header(response, "x-amz-checksum-crc32c"),
            checksum_crc64nvme: header(response, "x-amz-checksum-crc64nvme"),
            checksum_sha1: header(response, "x-amz-checksum-sha1"),
            checksum_sha256: header(response, "x-amz-checksum-sha256"),
            ..Default::default()
        })
    }

    pub(super) fn forwarded_delete(response: &reqwest::Response) -> S3Response<DeleteObjectOutput> {
        S3Response::new(DeleteObjectOutput {
            delete_marker: header(response, "x-amz-delete-marker").map(|value| value == "true"),
            version_id: header(response, "x-amz-version-id"),
            ..Default::default()
        })
    }
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
