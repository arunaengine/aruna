//! Sends a GET or HEAD for a key range this member keeps no records of to a holder, chosen by
//! free download slots, with a request-bound delegation (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The holder checks the caller on its own path; a holder that fails is skipped for a while.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::delegation::COPY_READ_HEADER;
use aruna_core::structs::storage::distribution::{DistributionBinding, ReadRoute};
use aruna_operations::distribution::guard::{
    GuardError, copy_readers, read_coordinator, route_read,
};
use aruna_operations::distribution::resolve::key_holders;
use aruna_operations::node::holder_choice::{avoid_holder, choose_holder};
use futures_util::TryStreamExt;
use http::{HeaderMap, Method};
use s3s::dto::{
    ChecksumType, ETag, GetObjectOutput, HeadObjectOutput, StreamingBlob, Timestamp,
    TimestampFormat,
};
use s3s::{S3Error, S3ErrorCode, S3Response, S3Result};
use ulid::Ulid;

use super::list::unreachable;
use super::{Forward, ForwardBody, ForwardRequest, header, passed_headers, passed_query};
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::guard_error;
use crate::s3::service::ArunaS3Service;

/// How long an anonymous caller's stand-in credential lasts; the delegation is shorter.
const ANONYMOUS_SECS: u64 = 60;
/// How long a holder may take to start its answer; the body then streams without a bound.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// Marks an answer a copy gave for ranges whose coordinator could not answer; it may be behind.
pub(in crate::s3::service) const STALE_HEADER: &str = "x-aruna-stale";

/// Where a GET or HEAD is answered; `Some` names the range a copy answered for.
pub(crate) enum ReadAnswer {
    Forwarded(reqwest::Response, Option<Ulid>),
    Local(Option<Ulid>),
}

/// Adds the stale mark of `ranges` to an answer's headers.
pub(in crate::s3::service) fn mark_stale(headers: &mut HeaderMap, ranges: &[Ulid]) {
    let value = (ranges.iter())
        .map(|range_id| format!("range={range_id}"))
        .collect::<Vec<_>>()
        .join(", ");
    if !ranges.is_empty()
        && let Ok(value) = http::HeaderValue::from_str(&value)
    {
        headers.insert(STALE_HEADER, value);
    }
}

/// A copy's error, marked; its miss says the key may still exist at the coordinator.
pub(in crate::s3::service) fn stale_error(mut error: S3Error, range_id: Ulid) -> S3Error {
    if matches!(
        error.code(),
        S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion
    ) {
        error.set_message(format!(
            "Not found on a copy of range {range_id}, which answered because the range's \
             coordinator could not; the copy may be behind, so the object may exist."
        ));
    }
    let mut headers = error.headers().cloned().unwrap_or_default();
    mark_stale(&mut headers, &[range_id]);
    error.set_headers(headers);
    error
}

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
    // A ranged answer carries no full-object checksum, as in S3.
    let partial = status == http::StatusCode::PARTIAL_CONTENT;
    let checksum = |name| header(&response, name).filter(|_| !partial);
    let output = GetObjectOutput {
        accept_ranges: header(&response, "accept-ranges"),
        checksum_crc32: checksum("x-amz-checksum-crc32"),
        checksum_crc32c: checksum("x-amz-checksum-crc32c"),
        checksum_crc64nvme: checksum("x-amz-checksum-crc64nvme"),
        checksum_sha1: checksum("x-amz-checksum-sha1"),
        checksum_sha256: checksum("x-amz-checksum-sha256"),
        checksum_type: checksum_type(&response).filter(|_| !partial),
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

/// Whether a holder other than the coordinator missed the object: a replica may not have the
/// key's newest versions yet, while the coordinator owns its head.
fn replica_miss(error: &S3Error, holder: &Forward, coordinator: NodeId) -> bool {
    holder.node_id != coordinator && error.status_code() == Some(http::StatusCode::NOT_FOUND)
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

/// The holder's answer, or the holder as unreachable when it does not start answering in time.
async fn answer_within(
    forward: &Forward,
    answer: impl Future<Output = S3Result<reqwest::Response>>,
) -> S3Result<reqwest::Response> {
    match tokio::time::timeout(ANSWER_TIMEOUT, answer).await {
        Ok(answer) => answer,
        Err(_) => Err(super::unreachable_error(forward, "not answering in time")),
    }
}

impl ArunaS3Service {
    /// The holder a GET or HEAD of `key` goes to, with the range's coordinator, or `None` here.
    /// Only a `pinned` version may come from a lagging copy. A delegated read is never sent on;
    /// a current one is refused unless this node coordinates the range or a copy was asked.
    pub(in crate::s3::service) async fn read_holder(
        &self,
        extensions: &http::Extensions,
        bucket: Option<&BucketInfo>,
        (key, pinned): (&str, bool),
    ) -> S3Result<Option<(Forward, NodeId)>> {
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution.as_ref()) else {
            return Ok(None);
        };
        if let Some(delegated) = extensions.get::<DelegatedRequest>() {
            if !pinned
                && !delegated.copy_read
                && let Some(coordinator) = self.range_coordinator(binding, key).await?
            {
                return Err(guard_error(&GuardError::NotCoordinator {
                    set_id: binding.set_id,
                    range_id: coordinator.range_id,
                    node_id: coordinator.node_id,
                }));
            }
            return Ok(None);
        }
        if !pinned {
            let coordinator = self.range_coordinator(binding, key).await?;
            return Ok(coordinator.map(|forward| {
                let node_id = forward.node_id;
                (forward, node_id)
            }));
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
        let holder = Forward {
            set_id: binding.set_id,
            range_id,
            node_id,
        };
        Ok(Some((holder, coordinator)))
    }

    /// The coordinator of `key`'s range when another member coordinates it, or `None` when
    /// this member does; a range this node fenced for a transfer is refused.
    async fn range_coordinator(
        &self,
        binding: &DistributionBinding,
        key: &str,
    ) -> S3Result<Option<Forward>> {
        let coordinator = read_coordinator(self.state.as_ref(), binding, key)
            .await
            .map_err(|error| guard_error(&error))?;
        Ok(coordinator.map(|(range_id, node_id)| Forward {
            set_id: binding.set_id,
            range_id,
            node_id,
        }))
    }

    /// The coordinator a local miss goes on to, since a copy may lag behind it; a miss on a
    /// range this node fenced is refused. A delegated read is never sent on.
    pub(in crate::s3::service) async fn copy_miss(
        &self,
        extensions: &http::Extensions,
        bucket: Option<&BucketInfo>,
        key: &str,
        error: &S3Error,
    ) -> S3Result<Option<Forward>> {
        let missing = matches!(
            error.code(),
            S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion
        );
        let binding = bucket.and_then(|bucket| bucket.distribution.as_ref());
        let Some(binding) = binding.filter(|_| missing) else {
            return Ok(None);
        };
        let coordinator = self.range_coordinator(binding, key).await?;
        match extensions.get::<DelegatedRequest>() {
            Some(_) => Ok(None),
            None => Ok(coordinator),
        }
    }

    /// Answers a GET or HEAD of `key` at its holder, coordinator first. While the coordinator
    /// or this node's fenced view cannot answer a current read, a copy answers, marked stale.
    pub(in crate::s3::service) async fn answer_read(
        &self,
        extensions: &http::Extensions,
        user: &UserAccess,
        bucket: Option<&BucketInfo>,
        (key, pinned): (&str, bool),
        request: impl Fn() -> ForwardRequest,
    ) -> S3Result<ReadAnswer> {
        let error = match self.read_holder(extensions, bucket, (key, pinned)).await {
            Ok(None) => return Ok(ReadAnswer::Local(None)),
            Ok(Some(holder)) => match self.read_object(user, &holder, &request).await {
                Ok(response) => return Ok(ReadAnswer::Forwarded(response, None)),
                Err(error) => error,
            },
            Err(error) => error,
        };
        // A delegated read's refusal goes back to its sender, which asks the copies itself.
        let delegated = extensions.get::<DelegatedRequest>().is_some();
        let binding = bucket.and_then(|bucket| bucket.distribution.as_ref());
        let Some(binding) = binding.filter(|_| !pinned && !delegated && unreachable(&error)) else {
            return Err(error);
        };
        let readers = copy_readers(self.state.as_ref(), binding, key).await;
        let (range_id, here, copies) = readers.map_err(|error| guard_error(&error))?;
        if here {
            return Ok(ReadAnswer::Local(Some(range_id)));
        }
        for node_id in copies {
            let forward = Forward {
                set_id: binding.set_id,
                range_id,
                node_id,
            };
            let mut copy_read = request();
            (copy_read.headers).push((COPY_READ_HEADER.to_string(), "1".to_string()));
            match self.forward_read(user, &forward, copy_read).await {
                Ok(response) => return Ok(ReadAnswer::Forwarded(response, Some(range_id))),
                Err(failed) if unreachable(&failed) => {}
                Err(failed) => return Err(stale_error(failed, range_id)),
            }
        }
        Err(error)
    }

    /// A GET of `key` for a caller another surface verified, answered as an S3 GET would be: a
    /// current version by the range's coordinator, a pinned one by any holder of its records.
    pub(crate) async fn member_get(
        &self,
        user: &UserAccess,
        (bucket, info): (&str, &BucketInfo),
        key: &str,
        version: Option<Ulid>,
    ) -> S3Result<ReadAnswer> {
        let query = version.map(|version| format!("versionId={version}"));
        let headers = HeaderMap::new();
        let request = || {
            read_request(
                Method::GET,
                "GetObject",
                bucket,
                key,
                query.as_deref(),
                &headers,
            )
        };
        let pinned = (key, version.is_some());
        (self.answer_read(&http::Extensions::new(), user, Some(info), pinned, request)).await
    }

    /// A HEAD of `key` answered as `member_get` answers a GET. A holder's HEAD miss has no
    /// body to name its code, so it becomes the missing key a local miss is.
    pub(crate) async fn member_head(
        &self,
        user: &UserAccess,
        (bucket, info): (&str, &BucketInfo),
        key: &str,
    ) -> S3Result<ReadAnswer> {
        let headers = HeaderMap::new();
        let request = || read_request(Method::HEAD, "HeadObject", bucket, key, None, &headers);
        let extensions = http::Extensions::new();
        let answer = self.answer_read(&extensions, user, Some(info), (key, false), request);
        answer.await.map_err(|error| match error.status_code() {
            Some(http::StatusCode::NOT_FOUND) => s3s::s3_error!(NoSuchKey),
            _ => error,
        })
    }

    /// Sends a GET or HEAD to the chosen holder; a replica's miss goes on to the coordinator.
    pub(in crate::s3::service) async fn read_object(
        &self,
        user: &UserAccess,
        (holder, coordinator): &(Forward, NodeId),
        request: impl Fn() -> ForwardRequest,
    ) -> S3Result<reqwest::Response> {
        match self.forward_read(user, holder, request()).await {
            Err(error) if replica_miss(&error, holder, *coordinator) => {
                let coordinator = Forward {
                    node_id: *coordinator,
                    ..holder.clone()
                };
                self.forward_read(user, &coordinator, request()).await
            }
            answer => answer,
        }
    }

    /// Sends one read to the chosen holder; a holder that fails is skipped for a while.
    pub(in crate::s3::service) async fn forward_read(
        &self,
        user: &UserAccess,
        forward: &Forward,
        request: ForwardRequest,
    ) -> S3Result<reqwest::Response> {
        let user = anonymous_window(user);
        let result = answer_within(forward, self.forward_write(&user, forward, request)).await;
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

    #[tokio::test(start_paused = true)]
    async fn silent_holder_unreachable() {
        let forward = Forward {
            set_id: Ulid::nil(),
            range_id: Ulid::nil(),
            node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
        };
        let error = answer_within(&forward, std::future::pending())
            .await
            .unwrap_err();
        assert!(unreachable(&error));
        let answer = answer_within(&forward, async { Ok(holder_answer(200)) }).await;
        assert_eq!(answer.unwrap().status(), 200);
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
        assert!(output.checksum_sha256.is_none());
        assert!(output.checksum_type.is_none());
    }

    #[test]
    fn full_answer_checksummed() {
        let output = forwarded_get(holder_answer(200)).output;
        assert!(output.checksum_sha256.is_some());
        assert_eq!(
            output.checksum_type.as_ref().map(ChecksumType::as_str),
            Some("FULL_OBJECT")
        );
    }

    #[test]
    fn miss_asks_coordinator() {
        let node = |seed| iroh::SecretKey::from_bytes(&[seed; 32]).public();
        let holder = Forward {
            set_id: ulid::Ulid::from_bytes([1; 16]),
            range_id: ulid::Ulid::from_bytes([2; 16]),
            node_id: node(3),
        };
        let missing = s3s::s3_error!(NoSuchKey);
        assert!(replica_miss(&missing, &holder, node(4)));
        // The coordinator's own miss is final, and other errors are not a lagging copy.
        assert!(!replica_miss(&missing, &holder, node(3)));
        let denied = s3s::s3_error!(AccessDenied);
        assert!(!replica_miss(&denied, &holder, node(4)));
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
