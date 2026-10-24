//! Forwards an S3 write that reached a member of a distributed bucket to the coordinator of the
//! key's range, with a request-bound delegation in place of the caller's signature (D33). An
//! unreachable coordinator fails the write at once; nothing is queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::distribution::delegation::request_digest;
use http::{HeaderMap, Method};

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
