//! Tests forwarded S3 writes: kept-back signatures, digests, denial hints and bound headers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (name, value) in pairs {
        map.insert(
            http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            http::HeaderValue::from_str(value).unwrap(),
        );
    }
    map
}

fn forward() -> Forward {
    Forward {
        set_id: Ulid::from_bytes([1; 16]),
        range_id: Ulid::from_bytes([2; 16]),
        node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
    }
}

#[test]
fn signatures_stay_behind() {
    let original = headers(&[
        ("authorization", "AWS4-HMAC-SHA256 Credential=secret"),
        ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
        ("x-amz-security-token", "token"),
        ("content-type", "text/csv"),
        ("x-amz-meta-origin", "lab"),
        ("x-amz-checksum-sha256", "abc"),
        ("content-length", "999"),
    ]);
    let passed = passed_headers(&original, Some(7));
    let names: Vec<&str> = passed.iter().map(|(name, _)| name.as_str()).collect();
    assert!(names.contains(&"content-type"));
    assert!(names.contains(&"x-amz-meta-origin"));
    assert!(names.contains(&"x-amz-checksum-sha256"));
    assert!(!names.contains(&"authorization"));
    assert!(!names.contains(&"x-amz-content-sha256"));
    assert!(!names.contains(&"x-amz-security-token"));
    // The decoded body length replaces the length of the signed chunk encoding.
    assert!(passed.contains(&("content-length".to_string(), "7".to_string())));
    assert_eq!(
        passed_query(Some("X-Amz-Signature=s&X-Amz-Credential=c&tagging")),
        Some("tagging".to_string())
    );
    assert_eq!(passed_query(Some("X-Amz-Date=1")), None);
    assert_eq!(
        passed_query(Some("AWSAccessKeyId=a&Signature=s&Expires=9&partNumber=2")),
        Some("partNumber=2".to_string())
    );
}

#[test]
fn digest_matches_target() {
    // The target recomputes the digest from the headers it receives.
    let sent = headers(&[("content-length", "7"), ("content-type", "text/csv")]);
    let issuer = forwarded_digest(&Method::PUT, "spire", "a/b", None, &sent);
    let target = forwarded_digest(&Method::PUT, "spire", "a/b", None, &sent);
    assert_eq!(issuer, target);
    let changed = headers(&[("content-length", "8"), ("content-type", "text/csv")]);
    assert_ne!(
        issuer,
        forwarded_digest(&Method::PUT, "spire", "a/b", None, &changed)
    );
}

#[test]
fn denial_hints_globs() {
    let body = "<Error><Code>AccessDenied</Code><Message>Permission denied</Message></Error>";
    let error = coordinator_error(&forward(), http::StatusCode::FORBIDDEN, body);
    assert_eq!(*error.code(), S3ErrorCode::AccessDenied);
    assert_eq!(error.status_code(), Some(http::StatusCode::FORBIDDEN));
    let message = error.message().unwrap_or_default();
    assert!(message.contains("node glob"));
    assert!(message.contains(&forward().node_id.to_string()));
    let busy = coordinator_error(
        &forward(),
        http::StatusCode::SERVICE_UNAVAILABLE,
        "<Error><Code>SlowDown</Code><Message>a &amp; b</Message></Error>",
    );
    assert_eq!(busy.message(), Some("a & b"));
}

#[test]
fn unreachable_names_range() {
    let error = unreachable_error(&forward(), "unreachable since its last heartbeat at 7 ms");
    assert_eq!(
        error.status_code(),
        Some(http::StatusCode::SERVICE_UNAVAILABLE)
    );
    let message = error.message().unwrap_or_default();
    assert!(message.contains("degraded"));
    assert!(message.contains(&forward().range_id.to_string()));
    assert!(message.contains("since its last heartbeat at 7 ms"));
}

#[test]
fn semantic_headers_bound() {
    let sent = headers(&[("content-length", "7"), ("x-amz-meta-origin", "lab")]);
    let issued = forwarded_digest(&Method::PUT, "spire", "a/b", None, &sent);
    for (name, value) in [
        ("x-amz-meta-origin", "other"),
        ("if-match", "\"abc\""),
        ("cache-control", "no-store"),
        ("x-amz-checksum-crc32", "AAAAAA=="),
    ] {
        let mut changed = sent.clone();
        changed.insert(
            http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            http::HeaderValue::from_str(value).unwrap(),
        );
        let digest = forwarded_digest(&Method::PUT, "spire", "a/b", None, &changed);
        assert_ne!(digest, issued, "{name} must be bound");
    }
    // Headers without request semantics may differ in transit.
    let mut transit = sent.clone();
    transit.insert("user-agent", http::HeaderValue::from_static("proxy"));
    assert_eq!(
        forwarded_digest(&Method::PUT, "spire", "a/b", None, &transit),
        issued
    );
}

#[test]
fn consumed_headers_bound() {
    let sent = headers(&[("accept", "*/*"), ("x-amz-acl", "private")]);
    let issuer = forwarded_digest(&Method::GET, "spire", "a/", None, &sent);
    // A changed `accept` could turn a read into a folder index, a changed ACL another policy.
    for changed in [
        headers(&[("accept", "text/html"), ("x-amz-acl", "private")]),
        headers(&[("accept", "*/*"), ("x-amz-acl", "public-read")]),
    ] {
        assert_ne!(
            issuer,
            forwarded_digest(&Method::GET, "spire", "a/", None, &changed)
        );
    }
    let original = headers(&[("x-amz-acl", "private"), ("x-amz-checksum-mode", "ENABLED")]);
    let passed = passed_headers(&original, None);
    assert!(passed.contains(&("x-amz-acl".to_string(), "private".to_string())));
    assert!(passed.contains(&("x-amz-checksum-mode".to_string(), "ENABLED".to_string())));
}
