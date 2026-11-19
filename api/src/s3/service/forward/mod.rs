//! Forwards an S3 write on a distributed bucket member to the key range's coordinator, with a
//! request-bound delegation in place of the caller's signature (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// An unreachable coordinator fails the write at once; nothing is queued.

pub(super) mod copy;
pub(super) mod list;
pub(super) mod parts;
pub(super) mod reads;
mod uploads;
mod versions;

use aruna_operations::driver::drive;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::member_read::{MemberBody, MemberMethod, TrailerValue};
use aruna_core::outbound::xml_field;
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::delegation::{
    COPY_CONTEXT_HEADER, CopyContext, DIGEST_HEADERS, PASSED_HEADERS, Payload, bound_header,
    request_digest,
};
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{ReadRoute, RouteError};
use aruna_operations::distribution::guard::GuardError;
use aruna_operations::distribution::routing::route_read;
use aruna_operations::distribution::routing::{upload_route, upload_tag, upload_tags};
use aruna_operations::s3::object::delete::bulk::BulkDeleteEntry;
pub(crate) use aruna_operations::s3::object::member_read::MemberTarget as Forward;
pub(super) use aruna_operations::s3::object::member_read::{
    MemberPayload as ForwardBody, MemberRequest as ForwardRequest, TrailerChecksum,
};
use aruna_operations::s3::object::member_write::{ForwardDeletesOperation, WriteTargetOperation};
use http::{HeaderMap, Method};
use s3s::dto::{
    DeleteObjectOutput, DeletedObject, ETag, Error as S3DeleteError, PutObjectOutput, StreamingBlob,
};
use s3s::{S3Error, S3ErrorCode, S3Response, S3Result, s3_error};
use ulid::Ulid;

use super::ArunaS3Service;
use crate::s3::auth::DelegatedRequest;
use crate::s3::error::guard_error;
use crate::s3::util::{map_checksum_algorithm, parse_checksum_algorithm, parse_upload_id};
use aruna_core::structs::checksum::ChecksumAlgorithm;
use s3s::TrailingHeaders;
use s3s::dto::ChecksumAlgorithm as S3ChecksumAlgorithm;

/// A client's body as a forwarded one, ending with the trailer this node signs.
pub(super) fn stream_body(body: StreamingBlob, checksum: Option<TrailerChecksum>) -> ForwardBody {
    ForwardBody::Stream(BackendStream::new_from_boxed(body), checksum)
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
    let value = TrailerValue(Arc::new(move || {
        trailing
            .read(|headers| {
                let value = headers.get(&header)?.to_str().ok()?;
                Some(value.to_string())
            })
            .flatten()
    }));
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

/// What a delegated write keeps of the source it copies; `None` for any other request.
pub(super) fn copy_context(
    extensions: &http::Extensions,
    headers: &HeaderMap,
) -> S3Result<Option<CopyContext>> {
    if extensions.get::<DelegatedRequest>().is_none() {
        return Ok(None);
    }
    let Some(value) = headers.get(COPY_CONTEXT_HEADER) else {
        return Ok(None);
    };
    let value = value.to_str().ok();
    match value.map(CopyContext::decode) {
        Some(Ok(context)) => Ok(Some(context)),
        _ => Err(s3_error!(
            InvalidArgument,
            "The copy context does not decode"
        )),
    }
}

/// The header a holder answers a delegated read with when the version carries placement refs.
pub(super) fn copy_policies(
    extensions: &http::Extensions,
    policies: &[aruna_core::structs::placement::policy::PlacementPolicyRef],
) -> Option<http::HeaderValue> {
    extensions.get::<DelegatedRequest>()?;
    if policies.is_empty() {
        return None;
    }
    let context = CopyContext {
        policies: policies.to_vec(),
        origin: None,
    };
    http::HeaderValue::from_str(&context.encode().ok()?).ok()
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
        .filter(|(name, _)| bound_header(name.as_str()))
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

/// A member's refusal, given its status and S3 error fields, as this node's answer.
fn refused_error(
    node_id: NodeId,
    status: http::StatusCode,
    code: Option<String>,
    message: String,
) -> S3Error {
    let code = code
        .map(|code| {
            S3ErrorCode::from_bytes(code.as_bytes())
                .unwrap_or_else(|| S3ErrorCode::Custom(code.into()))
        })
        .unwrap_or(S3ErrorCode::InternalError);
    let message = match status {
        http::StatusCode::FORBIDDEN => format!(
            "The node {node_id} denied the request: {message} Every member checks the \
             caller on its own path; grant the bucket with a node glob such as \
             /{{realm}}/g/{{group}}/data/*/{{bucket}}/**."
        ),
        _ => message,
    };
    let mut error = S3Error::with_message(code, message);
    error.set_status_code(status);
    error
}

pub(super) fn header(response: &MemberBody, name: &str) -> Option<String> {
    response.header(name).map(str::to_string)
}

impl ArunaS3Service {
    /// The coordinator a write to `key` goes to, or `None` when this node writes it. A request
    /// that arrived with a delegation is never forwarded again.
    pub(crate) async fn forward_target(
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
        let delegated = extensions.get::<DelegatedRequest>().is_some();
        let target = WriteTargetOperation::new(binding, key, upload, delegated);
        (drive(target, self.state.as_ref()).await).map_err(|error| guard_error(&error))
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
        match drive(route_read(binding, key, listing), self.state.as_ref()).await {
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

    /// Sends a new object `key` to its coordinator for a caller another surface verified;
    /// `If-None-Match: *` makes the coordinator keep a live key as it is.
    pub(crate) async fn member_put(
        &self,
        user: &UserAccess,
        forward: &Forward,
        (bucket, key): (&str, &str),
        (body, content_length, content_type): (StreamingBlob, Option<u64>, &str),
    ) -> S3Result<MemberBody> {
        let mut headers = vec![
            ("content-type".to_string(), content_type.to_string()),
            ("if-none-match".to_string(), "*".to_string()),
        ];
        headers.extend(
            content_length.map(|length| ("content-length".to_string(), length.to_string())),
        );
        let request = ForwardRequest {
            method: MemberMethod::Put,
            operation: "PutObject",
            bucket: bucket.to_string(),
            key: key.to_string(),
            query: None,
            headers,
            body: stream_body(body, None),
        };
        self.forward_write(user, forward, request).await
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
        match drive(upload_route(binding, key, &tag), self.state.as_ref()).await {
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
        drive(upload_tag(binding, key, upload_id), self.state.as_ref())
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
        let tags = drive(upload_tags(binding, uploads), self.state.as_ref())
            .await
            .map_err(|error| guard_error(&error))?;
        Ok(tags
            .into_iter()
            .map(|tag| (tag.upload_id.to_string(), tag.encode()))
            .collect())
    }

    pub(super) fn forwarded_put(response: &MemberBody) -> S3Response<PutObjectOutput> {
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
        let binding = info.and_then(|info| info.distribution.as_ref());
        let delegated = extensions.get::<DelegatedRequest>().is_some();
        let forward = ForwardDeletesOperation::new(
            self.state.node_and_realm(),
            reads::delegated_caller(user),
            (name, binding, delegated),
            entries.clone(),
        );
        let forwarded = match drive(forward, self.state.as_ref()).await {
            Ok(forwarded) => forwarded,
            Err(error) => {
                let message = error.to_string();
                let failed = entries.into_iter().map(|entry| S3DeleteError {
                    code: Some("InternalError".to_string()),
                    key: Some(entry.key),
                    version_id: None,
                    message: Some(message.clone()),
                });
                return (Vec::new(), Vec::new(), failed.collect());
            }
        };
        let (mut deleted, mut errors) = (Vec::new(), Vec::new());
        for (key, answer) in forwarded.answers {
            match answer {
                Ok(response) => deleted.push(DeletedObject {
                    key: Some(key),
                    delete_marker: header(&response, "x-amz-delete-marker").map(|v| v == "true"),
                    delete_marker_version_id: header(&response, "x-amz-version-id"),
                    ..Default::default()
                }),
                Err(failure) => {
                    let error = self.member_error(failure);
                    errors.push(S3DeleteError {
                        code: Some(error.code().as_str().to_string()),
                        key: Some(key),
                        version_id: None,
                        message: error.message().map(str::to_string),
                    });
                }
            }
        }
        (forwarded.local, deleted, errors)
    }

    pub(super) fn forwarded_delete(response: &MemberBody) -> S3Response<DeleteObjectOutput> {
        S3Response::new(DeleteObjectOutput {
            delete_marker: header(response, "x-amz-delete-marker").map(|value| value == "true"),
            version_id: header(response, "x-amz-version-id"),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod delegated_tests;
#[cfg(test)]
mod members_tests;
#[cfg(test)]
mod tests;
