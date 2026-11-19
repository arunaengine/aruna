//! Runs the member reads of S3 GET and HEAD requests and turns their answers into this node's,
//! and sends the reads listings and copies address to one chosen member (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use aruna_core::member_read::MemberBody;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_operations::distribution::status::node_reach;
use aruna_operations::driver::drive;
use aruna_operations::s3::object::member_read::{
    MemberCallOperation, MemberFailure, MemberKey, MemberRead, MemberReadOperation, MemberRefusal,
    MemberTextOperation,
};
use aruna_operations::s3::object::range_list::DelegatedCaller;
use http::HeaderMap;
use s3s::dto::{
    ChecksumType, ETag, GetObjectOutput, HeadObjectOutput, StreamingBlob, Timestamp,
    TimestampFormat,
};
use s3s::{S3Error, S3ErrorCode, S3Response, S3Result, s3_error};
use ulid::Ulid;

use super::{Forward, ForwardRequest, passed_headers, passed_query, refused_error, unix_secs};
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::guard_error;
use crate::s3::service::ArunaS3Service;

/// How long an anonymous caller's stand-in credential lasts; the delegation is shorter.
const ANONYMOUS_SECS: u64 = 60;

/// Marks an answer a copy gave for ranges whose coordinator could not answer; it may be behind.
pub(in crate::s3::service) const STALE_HEADER: &str = "x-aruna-stale";

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

/// The read of one object another member receives instead of this node.
pub(in crate::s3::service) fn member_key(
    head: bool,
    (bucket, key, version): (&str, &str, Option<Ulid>),
    query: Option<&str>,
    headers: &HeaderMap,
) -> MemberKey {
    MemberKey {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version,
        head,
        query: passed_query(query),
        headers: passed_headers(headers, None),
    }
}

/// The caller a read is delegated for.
pub(crate) fn delegated_caller(user: &UserAccess) -> DelegatedCaller {
    DelegatedCaller {
        user_id: user.user_identity,
        group_id: user.group_id,
        credential_id: user.access_key.clone(),
        restrictions: user.path_restrictions.clone(),
        credential_expiry: unix_secs(user.expiry),
    }
}

fn last_modified(body: &MemberBody) -> Option<Timestamp> {
    (body.header("last-modified"))
        .and_then(|value| Timestamp::parse(TimestampFormat::HttpDate, value).ok())
}

fn e_tag(body: &MemberBody) -> Option<ETag> {
    body.header("etag")
        .and_then(|value| ETag::parse_http_header(value.as_bytes()).ok())
}

/// The user metadata the holder answered with.
fn user_metadata(body: &MemberBody) -> Option<HashMap<String, String>> {
    let metadata: HashMap<String, String> = (body.headers.iter())
        .filter_map(|(name, value)| {
            let name = name.strip_prefix("x-amz-meta-")?;
            Some((name.to_string(), value.clone()))
        })
        .collect();
    (!metadata.is_empty()).then_some(metadata)
}

fn text(body: &MemberBody, name: &str) -> Option<String> {
    body.header(name).map(str::to_string)
}

fn checksum_type(body: &MemberBody) -> Option<ChecksumType> {
    text(body, "x-amz-checksum-type").map(ChecksumType::from)
}

fn content_length(body: &MemberBody) -> Option<i64> {
    body.header("content-length")
        .and_then(|value| value.parse().ok())
}

/// The holder's GET answer as this node's, its body streamed through.
pub(in crate::s3::service) fn forwarded_get(body: MemberBody) -> S3Response<GetObjectOutput> {
    // A ranged answer carries no full-object checksum, as in S3.
    let partial = body.status == http::StatusCode::PARTIAL_CONTENT.as_u16();
    let checksum = |name| text(&body, name).filter(|_| !partial);
    let output = GetObjectOutput {
        accept_ranges: text(&body, "accept-ranges"),
        checksum_crc32: checksum("x-amz-checksum-crc32"),
        checksum_crc32c: checksum("x-amz-checksum-crc32c"),
        checksum_crc64nvme: checksum("x-amz-checksum-crc64nvme"),
        checksum_sha1: checksum("x-amz-checksum-sha1"),
        checksum_sha256: checksum("x-amz-checksum-sha256"),
        checksum_type: checksum_type(&body).filter(|_| !partial),
        content_length: content_length(&body),
        content_range: text(&body, "content-range"),
        content_type: text(&body, "content-type"),
        e_tag: e_tag(&body),
        last_modified: last_modified(&body),
        metadata: user_metadata(&body),
        version_id: text(&body, "x-amz-version-id"),
        body: None,
        ..Default::default()
    };
    let output = GetObjectOutput {
        body: Some(StreamingBlob::wrap(body.stream)),
        ..output
    };
    match partial {
        true => S3Response::with_status(output, http::StatusCode::PARTIAL_CONTENT),
        false => S3Response::new(output),
    }
}

/// The holder's HEAD answer as this node's.
pub(in crate::s3::service) fn forwarded_head(body: &MemberBody) -> S3Response<HeadObjectOutput> {
    S3Response::new(HeadObjectOutput {
        accept_ranges: text(body, "accept-ranges"),
        checksum_crc32: text(body, "x-amz-checksum-crc32"),
        checksum_crc32c: text(body, "x-amz-checksum-crc32c"),
        checksum_crc64nvme: text(body, "x-amz-checksum-crc64nvme"),
        checksum_sha1: text(body, "x-amz-checksum-sha1"),
        checksum_sha256: text(body, "x-amz-checksum-sha256"),
        checksum_type: checksum_type(body),
        content_length: content_length(body),
        content_type: text(body, "content-type"),
        e_tag: e_tag(body),
        last_modified: last_modified(body),
        metadata: user_metadata(body),
        version_id: text(body, "x-amz-version-id"),
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
    fn member_operation(
        &self,
        user: &UserAccess,
        binding: &DistributionBinding,
        key: MemberKey,
    ) -> MemberReadOperation {
        let ids = self.state.node_and_realm();
        MemberReadOperation::new(ids, delegated_caller(user), binding.clone(), key)
    }

    /// A member's failure as this node's answer; a copy's refusal is marked.
    pub(super) fn member_error(&self, failure: MemberFailure) -> S3Error {
        let error = match failure.refusal {
            MemberRefusal::Refused {
                node_id,
                status,
                code,
                message,
            } => {
                let status =
                    (http::StatusCode::from_u16(status)).unwrap_or(http::StatusCode::BAD_GATEWAY);
                refused_error(node_id, status, code, message)
            }
            MemberRefusal::Unreachable {
                set_id,
                range_id,
                node_id,
                late,
            } => {
                let reach = match late {
                    true => "not answering in time".to_string(),
                    false => node_reach(&self.state.heard(), node_id).hint(),
                };
                let forward = Forward {
                    set_id,
                    range_id,
                    node_id,
                };
                super::unreachable_error(&forward, &reach)
            }
            MemberRefusal::NoHolder { set_id, range_id } => {
                super::list::incomplete(set_id, range_id)
            }
            MemberRefusal::Broken(reason) => s3_error!(ServiceUnavailable, "{reason}"),
            MemberRefusal::Route(error) => guard_error(&error),
            MemberRefusal::Failed(reason) => {
                s3_error!(InternalError, "Could not delegate the request: {reason}")
            }
        };
        match failure.stale {
            Some(range_id) => stale_error(error, range_id),
            None => error,
        }
    }

    /// Where a GET or HEAD of `key` is answered, read there when that is another member.
    pub(in crate::s3::service) async fn member_read(
        &self,
        extensions: &http::Extensions,
        user: &UserAccess,
        bucket: Option<&BucketInfo>,
        key: MemberKey,
    ) -> S3Result<MemberRead> {
        let Some(binding) = bucket.and_then(|bucket| bucket.distribution.as_ref()) else {
            return Ok(MemberRead::Local(None));
        };
        let read = self.member_operation(user, binding, key);
        let read = match extensions.get::<DelegatedRequest>() {
            Some(delegated) => read.delegated(delegated.copy_read),
            None => read,
        };
        let answer = drive(read, self.state.as_ref()).await;
        answer.map_err(|failure| self.member_error(failure))
    }

    /// What a local `error` for `key` leaves: a missing key goes on to its coordinator, since
    /// this copy may lag behind it. See [`MemberReadOperation::after_miss`].
    pub(in crate::s3::service) async fn missed_read(
        &self,
        extensions: &http::Extensions,
        (user, bucket): (&UserAccess, Option<&BucketInfo>),
        key: MemberKey,
        error: &S3Error,
    ) -> S3Result<MemberRead> {
        let missing = matches!(
            error.code(),
            S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion
        );
        let binding = bucket.and_then(|bucket| bucket.distribution.as_ref());
        let Some(binding) = binding.filter(|_| missing) else {
            return Ok(MemberRead::Local(None));
        };
        let delegated = extensions.get::<DelegatedRequest>().is_some();
        let read = self
            .member_operation(user, binding, key)
            .after_miss(delegated);
        let answer = drive(read, self.state.as_ref()).await;
        answer.map_err(|failure| self.member_error(failure))
    }

    /// Sends one request to the coordinator and returns its successful answer.
    pub(super) async fn forward_write(
        &self,
        user: &UserAccess,
        forward: &Forward,
        request: ForwardRequest,
    ) -> S3Result<MemberBody> {
        let ids = self.state.node_and_realm();
        let caller = delegated_caller(user);
        let call = MemberCallOperation::write(ids, caller, forward.clone(), request);
        (drive(call, self.state.as_ref()).await)
            .map_err(|refusal| self.member_error(refusal.into()))
    }

    /// Sends one read to the chosen holder; a holder that fails is skipped for a while.
    pub(in crate::s3::service) async fn forward_read(
        &self,
        user: &UserAccess,
        forward: &Forward,
        request: ForwardRequest,
    ) -> S3Result<MemberBody> {
        let ids = self.state.node_and_realm();
        let caller = delegated_caller(user);
        let call = MemberCallOperation::read(ids, caller, forward.clone(), request);
        (drive(call, self.state.as_ref()).await)
            .map_err(|refusal| self.member_error(refusal.into()))
    }

    /// The text of a member's answer; a broken or oversized one is an error.
    pub(super) async fn answer_text(&self, body: MemberBody) -> S3Result<String> {
        (drive(MemberTextOperation::of_body(body), self.state.as_ref()).await)
            .map_err(|refusal| self.member_error(refusal.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holder_answer(status: u16) -> MemberBody {
        let headers = [
            ("content-length", "3"),
            ("content-range", "bytes 0-2/10"),
            ("etag", "\"abc\""),
            ("last-modified", "Sat, 24 Oct 2026 10:00:00 GMT"),
            ("x-amz-version-id", "01JMETADATA0123456789ABCDE"),
            ("x-amz-meta-origin", "spire"),
            (
                "x-amz-checksum-sha256",
                "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=",
            ),
            ("x-amz-checksum-type", "FULL_OBJECT"),
        ];
        let bytes = bytes::Bytes::from_static(b"abc");
        let chunks = futures_util::stream::iter([Ok::<_, std::io::Error>(bytes)]);
        MemberBody {
            status,
            headers: (headers.into_iter())
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            stream: aruna_core::stream::BackendStream::new(chunks),
        }
    }

    #[test]
    fn request_binds_range() {
        let mut headers = HeaderMap::new();
        headers.insert("range", "bytes=0-2".parse().unwrap());
        headers.insert("authorization", "AWS4-HMAC-SHA256 secret".parse().unwrap());
        let query = Some("versionId=01JMETADATA0123456789ABCDE&X-Amz-Signature=secret");
        let request = member_key(false, ("spire", "a.fa", None), query, &headers);
        assert_eq!(
            request.query.as_deref(),
            Some("versionId=01JMETADATA0123456789ABCDE")
        );
        assert_eq!(
            request.headers,
            vec![("range".to_string(), "bytes=0-2".to_string())]
        );
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
