//! Forwards an S3 write that reached a member of a distributed bucket to the coordinator of the
//! key's range, with a request-bound delegation in place of the caller's signature (D33). An
//! unreachable coordinator fails the write at once; nothing is queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub(super) mod copy;
pub(super) mod list;
pub(super) mod parts;
pub(super) mod reads;
mod uploads;
mod versions;

use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::RouteError;
use aruna_core::structs::storage::distribution::delegation::{
    DELEGATION_HEADER, Payload, TRAILER_BYTES, body_digest, request_digest,
};
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{ReadRoute, WriteRoute};
use aruna_operations::distribution::delegate::{DelegationInput, issue_delegation, node_urls};
use aruna_operations::distribution::guard::{
    GuardError, route_read, route_upload, route_write, upload_route, upload_tag, upload_tags,
};
use aruna_operations::distribution::status::node_reach;
use aruna_operations::s3::object::delete::bulk::BulkDeleteEntry;
use bytes::Bytes;
use http::{HeaderMap, Method};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use s3s::dto::{
    DeleteObjectOutput, DeletedObject, ETag, Error as S3DeleteError, PutObjectOutput, StreamingBlob,
};
use s3s::{S3Error, S3ErrorCode, S3Response, S3Result, s3_error};
use tracing::warn;
use ulid::Ulid;

use super::ArunaS3Service;
use super::payload::{ChecksumValue, frame_body};
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::guard_error;
use crate::s3::util::{map_checksum_algorithm, parse_checksum_algorithm, parse_upload_id};
use aruna_core::structs::checksum::ChecksumAlgorithm;
use s3s::TrailingHeaders;
use s3s::dto::ChecksumAlgorithm as S3ChecksumAlgorithm;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Headers that describe the body.
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
    "if-modified-since",
    "if-none-match",
    "if-unmodified-since",
    "range",
    "x-amz-checksum-algorithm",
    "x-amz-checksum-type",
    "x-amz-mp-object-size",
    "x-amz-tagging",
    "x-amz-copy-source",
    "x-amz-copy-source-range",
    "x-amz-copy-source-if-match",
    "x-amz-copy-source-if-none-match",
    "x-amz-copy-source-if-modified-since",
    "x-amz-copy-source-if-unmodified-since",
    "x-amz-metadata-directive",
    "x-amz-tagging-directive",
    "x-amz-acl",
    "x-amz-checksum-mode",
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
    pub body: ForwardBody,
}

/// The body of a forwarded request and how its delegation binds it.
pub(super) enum ForwardBody {
    Empty,
    /// Streams with a trailer the issuer signs over its length and digest.
    Stream(StreamingBlob, Option<TrailerChecksum>),
    /// A small body known in advance, bound by its digest.
    Known(Bytes),
}

/// The client's trailing checksum a forwarded body carries in its signed trailer: the S3
/// algorithm name and the value the client's trailer named once the body ended.
pub(super) struct TrailerChecksum {
    pub algorithm: String,
    pub value: ChecksumValue,
}

/// The client's trailing checksum to forward, read from its verified trailer once the body
/// ended; `None` without a trailer.
pub(super) fn trailer_checksum(
    algorithm: Option<ChecksumAlgorithm>,
    trailing: Option<TrailingHeaders>,
) -> Option<TrailerChecksum> {
    let (algorithm, trailing) = (algorithm?, trailing?);
    let name = map_checksum_algorithm(algorithm)?.as_str().to_string();
    let header = format!("x-amz-checksum-{}", name.to_ascii_lowercase());
    let value: ChecksumValue = Arc::new(move || {
        trailing
            .read(|headers| {
                let value = headers.get(&header)?.to_str().ok()?;
                Some(value.to_string())
            })
            .flatten()
    });
    Some(TrailerChecksum {
        algorithm: name,
        value,
    })
}

/// The trailing checksum algorithm a delegated request's framed body carries.
pub(super) fn delegated_trailer(
    extensions: &http::Extensions,
) -> S3Result<Option<ChecksumAlgorithm>> {
    let payload = extensions
        .get::<DelegatedRequest>()
        .map(|delegated| &delegated.payload);
    match payload {
        Some(Payload::Framed {
            checksum: Some(name),
        }) => parse_checksum_algorithm(&S3ChecksumAlgorithm::from(name.clone())).map(Some),
        _ => Ok(None),
    }
}

/// Whether a header carries request semantics the delegation must bind; `accept` selects a
/// folder index, which a forwarded read never asks for.
fn semantic(name: &str) -> bool {
    DIGEST_HEADERS.contains(&name)
        || PASSED_HEADERS.contains(&name)
        || name == "accept"
        || name.starts_with("x-amz-meta-")
        || name.starts_with("x-amz-checksum-")
}

/// The digest of a forwarded request as its target sees it.
pub(crate) fn forwarded_digest(
    method: &Method,
    bucket: &str,
    key: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> [u8; 32] {
    // Every semantic header the target sees counts, so none can be added or changed.
    let pairs: Vec<(&str, &str)> = headers
        .iter()
        .filter(|(name, _)| semantic(name.as_str()))
        .map(|(name, value)| {
            let value = value.to_str().unwrap_or("\u{fffd}");
            (name.as_str(), value)
        })
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

/// Query parameters of a presigned SigV2 request; SigV4 ones all start with `X-Amz-`.
const PRESIGN_V2: &[&str] = &["AWSAccessKeyId", "Signature", "Expires"];

/// The query of a forwarded request without presigning parameters.
pub(super) fn passed_query(query: Option<&str>) -> Option<String> {
    let kept: Vec<&str> = query?
        .split('&')
        .filter(|pair| {
            let name = pair.split('=').next().unwrap_or_default();
            !name.is_empty()
                && !name.to_ascii_lowercase().starts_with("x-amz-")
                && !PRESIGN_V2.contains(&name)
        })
        .collect();
    (!kept.is_empty()).then(|| kept.join("&"))
}

fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

/// A catching-up member's view is incomplete: a listing or a miss proves nothing yet.
fn catching_up_error(set_id: Ulid) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("DistributedBucketCatchingUp".into()),
        format!(
            "This member of distributed bucket {set_id} is still catching up; its view is \
             incomplete until then. Retry later or read from a ready member."
        ),
    );
    error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
    error
}

/// A miss of a catching-up member becomes the catch-up hint instead of a missing key.
pub(super) fn catch_up_miss(
    error: S3Error,
    catching_up: bool,
    bucket: Option<&BucketInfo>,
) -> S3Error {
    let missing = matches!(
        error.code(),
        S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion
    );
    match bucket.and_then(|bucket| bucket.distribution.as_ref()) {
        Some(binding) if catching_up && missing => catching_up_error(binding.set_id),
        _ => error,
    }
}

/// `reach` says since when this node has not heard from the coordinator.
fn unreachable_error(forward: &Forward, reach: &str) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("CoordinatorUnreachable".into()),
        format!(
            "Range {} of distributed bucket {} is coordinated by node {}, which is {reach}. \
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
            "The node {} denied the request: {message} Every member checks the \
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

/// The body of a node's answer; a broken or oversized body is an error, never an empty answer.
pub(super) async fn answer_text(response: reqwest::Response) -> S3Result<String> {
    match crate::routes::storage::relay::capped_body(response).await {
        Ok(body) => String::from_utf8(body)
            .map_err(|_| s3_error!(ServiceUnavailable, "A member answered with invalid text")),
        Err(error) => Err(s3_error!(
            ServiceUnavailable,
            "A member's answer broke off: {error}"
        )),
    }
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
        self.forward_for(extensions, bucket, key, false).await
    }

    /// The coordinator a request on an open upload of `key` goes to; a range fence in its
    /// upload grace period keeps the request here, so the upload can still complete.
    pub(super) async fn forward_upload(
        &self,
        extensions: &http::Extensions,
        bucket: Option<&BucketInfo>,
        key: &str,
    ) -> S3Result<Option<Forward>> {
        self.forward_for(extensions, bucket, key, true).await
    }

    async fn forward_for(
        &self,
        extensions: &http::Extensions,
        bucket: Option<&BucketInfo>,
        key: &str,
        upload: bool,
    ) -> S3Result<Option<Forward>> {
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution.as_ref()) else {
            return Ok(None);
        };
        if extensions.get::<DelegatedRequest>().is_some() {
            return Ok(None);
        }
        let route = match upload {
            true => route_upload(self.state.as_ref(), binding, key).await,
            false => route_write(self.state.as_ref(), binding, key).await,
        };
        match route {
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

    /// Whether this member answers a read of `key`, or with `listing` of every key under it,
    /// from its own view; `true` while it still catches up. A listing that reaches a range it
    /// keeps no records of is refused with its coordinator named; GET and HEAD go to a holder.
    pub(super) async fn read_scope(
        &self,
        bucket: Option<&BucketInfo>,
        key: &str,
        listing: bool,
    ) -> S3Result<bool> {
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution.as_ref()) else {
            return Ok(false);
        };
        match route_read(self.state.as_ref(), binding, key, listing).await {
            Ok(ReadRoute::Local { catching_up }) if listing && catching_up => {
                Err(catching_up_error(binding.set_id))
            }
            Ok(ReadRoute::Local { catching_up }) => Ok(catching_up),
            Ok(ReadRoute::Holder { range_id, node_id }) => {
                let mut error = S3Error::with_message(
                    S3ErrorCode::Custom("RangeNotHeld".into()),
                    format!(
                        "This member of distributed bucket {} keeps no records of range {range_id}; \
                         node {node_id} coordinates it and serves the read.",
                        binding.set_id
                    ),
                );
                error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
                Err(error)
            }
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
            return Err(unreachable_error(
                forward,
                &node_reach(self.state.as_ref(), forward.node_id).hint(),
            ));
        };
        let (payload, extra) = match &request.body {
            ForwardBody::Empty => (Payload::Empty, 0),
            ForwardBody::Stream(_, checksum) => {
                let checksum = checksum.as_ref().map(|checksum| checksum.algorithm.clone());
                (Payload::Framed { checksum }, TRAILER_BYTES as i64)
            }
            ForwardBody::Known(bytes) => (Payload::Digest(body_digest(bytes)), 0),
        };
        let mut headers = HeaderMap::new();
        for (name, value) in &request.headers {
            // A framed body is longer by its trailer.
            let value = match (name.as_str(), value.parse::<i64>()) {
                ("content-length", Ok(length)) => (length + extra).to_string(),
                _ => value.clone(),
            };
            let value = &value;
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::from_bytes(name.as_bytes()),
                http::HeaderValue::from_str(value),
            ) {
                headers.insert(name, value);
            }
        }
        // Set here so the delegation binds it; a forwarded read never asks for a folder index.
        (headers.entry(http::header::ACCEPT)).or_insert(http::HeaderValue::from_static("*/*"));
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
            copy_source: (request.headers.iter())
                .find(|(name, _)| name == "x-amz-copy-source")
                .map(|(_, value)| value.clone()),
            request_digest: digest,
            payload,
        };
        let refused = |error: &dyn std::fmt::Display| {
            s3_error!(InternalError, "Could not delegate the write: {error}")
        };
        let signed = issue_delegation(self.state.as_ref(), input, unix_secs(SystemTime::now()))
            .map_err(|error| refused(&error))?;
        let delegation = signed.encode().map_err(|error| refused(&error))?;
        let nonce = signed.delegation.nonce;
        let path = utf8_percent_encode(&request.key, KEY_SET);
        let mut url = format!("{}/{}/{path}", base.trim_end_matches('/'), request.bucket);
        if let Some(query) = query {
            url = format!("{url}?{query}");
        }
        let mut outgoing = CLIENT
            .request(request.method.clone(), &url)
            .headers(headers)
            .header(DELEGATION_HEADER, delegation);
        match request.body {
            ForwardBody::Empty => {}
            ForwardBody::Stream(body, checksum) => {
                let checksum = checksum.map(|checksum| checksum.value);
                let framed = frame_body(body, self.state.clone(), nonce, checksum);
                outgoing = outgoing.body(reqwest::Body::wrap_stream(framed));
            }
            ForwardBody::Known(bytes) => outgoing = outgoing.body(bytes),
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
                return Err(unreachable_error(
                    forward,
                    &node_reach(self.state.as_ref(), forward.node_id).hint(),
                ));
            }
        };
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        Err(coordinator_error(forward, status, &body))
    }

    /// The stored upload a request names. A distributed bucket's upload id names its set, range
    /// and epoch, and only the coordinator still holding that authority takes it.
    pub(super) async fn upload_id(
        &self,
        bucket: Option<&BucketInfo>,
        key: &str,
        value: &str,
    ) -> S3Result<Ulid> {
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution.as_ref()) else {
            return parse_upload_id(value);
        };
        let missing = || s3_error!(NoSuchUpload, "The specified upload does not exist.");
        let tag = UploadTag::parse(value).ok_or_else(missing)?;
        match upload_route(self.state.as_ref(), binding, key, &tag).await {
            Ok(()) => Ok(tag.upload_id),
            Err(GuardError::Route(RouteError::StaleRange(_))) => Err(s3_error!(
                NoSuchUpload,
                "The upload was opened under another coordinator of the key's range."
            )),
            Err(error) => Err(guard_error(&error)),
        }
    }

    /// The upload id a client sees for a new upload of `key`.
    pub(super) async fn shown_upload(
        &self,
        bucket: Option<&BucketInfo>,
        key: &str,
        upload_id: Ulid,
    ) -> S3Result<String> {
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution.as_ref()) else {
            return Ok(upload_id.to_string());
        };
        upload_tag(self.state.as_ref(), binding, key, upload_id)
            .await
            .map(|tag| tag.encode())
            .map_err(|error| guard_error(&error))
    }

    /// The upload ids a listing shows, by stored upload id.
    pub(super) async fn shown_uploads(
        &self,
        bucket: Option<&BucketInfo>,
        uploads: &[(String, Ulid)],
    ) -> S3Result<std::collections::HashMap<String, String>> {
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution.as_ref()) else {
            return Ok(std::collections::HashMap::new());
        };
        let tags = upload_tags(self.state.as_ref(), binding, uploads)
            .await
            .map_err(|error| guard_error(&error))?;
        Ok(tags
            .into_iter()
            .map(|tag| (tag.upload_id.to_string(), tag.encode()))
            .collect())
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

    /// Forwards each delete marker whose key another member coordinates, one delegated
    /// request per key, and keeps the other entries for this node.
    pub(super) async fn forward_deletes(
        &self,
        extensions: &http::Extensions,
        bucket: (&str, Option<&BucketInfo>),
        user: &UserAccess,
        entries: Vec<BulkDeleteEntry>,
    ) -> (Vec<BulkDeleteEntry>, Vec<DeletedObject>, Vec<S3DeleteError>) {
        let (name, info) = bucket;
        let (mut local, mut deleted, mut errors) = (Vec::new(), Vec::new(), Vec::new());
        for entry in entries {
            if entry.version_id.is_some() {
                local.push(entry);
                continue;
            }
            let forwarded = match self.forward_target(extensions, info, &entry.key).await {
                Ok(None) => {
                    local.push(entry);
                    continue;
                }
                Ok(Some(forward)) => {
                    let request = ForwardRequest {
                        method: Method::DELETE,
                        operation: "DeleteObject",
                        bucket: name.to_string(),
                        key: entry.key.clone(),
                        query: None,
                        headers: Vec::new(),
                        body: ForwardBody::Empty,
                    };
                    self.forward_write(user, &forward, request).await
                }
                Err(error) => Err(error),
            };
            match forwarded {
                Ok(response) => deleted.push(DeletedObject {
                    key: Some(entry.key),
                    delete_marker: header(&response, "x-amz-delete-marker").map(|v| v == "true"),
                    delete_marker_version_id: header(&response, "x-amz-version-id"),
                    ..Default::default()
                }),
                Err(error) => errors.push(S3DeleteError {
                    code: Some(error.code().as_str().to_string()),
                    key: Some(entry.key),
                    version_id: None,
                    message: error.message().map(str::to_string),
                }),
            }
        }
        (local, deleted, errors)
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
mod tests;
