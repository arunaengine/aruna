use super::*;

fn body(result: CallToolResult) -> serde_json::Value {
    assert_eq!(result.is_error, Some(true));
    result
        .structured_content
        .expect("a tool error carries the structured body")
}

#[test]
fn key_rejects_traversal() {
    let text = body(validate_key("../secret").unwrap_err());
    assert_eq!(text["code"], "Bad request");
    assert!(
        text["error"]
            .as_str()
            .unwrap_or_default()
            .contains("relative key")
    );
    assert!(validate_key("reads/sample.fastq.gz").is_ok());
}

#[test]
fn bounded_bytes_range() {
    assert_eq!(bounded_bytes(None).unwrap(), MAX_TEXT_BYTES);
    assert_eq!(bounded_bytes(Some(1024)).unwrap(), 1024);
    assert!(bounded_bytes(Some(0)).is_err());
    assert!(bounded_bytes(Some(MAX_TEXT_BYTES + 1)).is_err());
}

#[test]
fn cursor_rejects_garbage() {
    // Non base64 and well-formed base64 that is not a token both refuse.
    let text = body(decode_cursor("!not base64!").unwrap_err());
    assert!(
        text["error"]
            .as_str()
            .unwrap_or_default()
            .contains("next_cursor")
    );
    assert!(decode_cursor("Zm9v").is_err());
}

#[test]
fn object_error_forbidden() {
    let forbidden = body(object_error(crate::error::ServerError::Forbidden, "write"));
    assert!(
        forbidden["error"]
            .as_str()
            .unwrap_or_default()
            .contains("write permission")
    );
    assert_eq!(
        body(object_error(crate::error::ServerError::NotFound, "read"))["code"],
        "Not found"
    );
}

#[test]
fn bucket_error_maps() {
    assert!(
        body(map_bucket_error(GetBucketError::NotFound))["error"]
            .as_str()
            .unwrap_or_default()
            .contains("list_buckets")
    );
    assert_eq!(
        body(map_bucket_error(GetBucketError::Incomplete))["code"],
        "Internal error"
    );
}

#[test]
fn get_error_categories() {
    assert_eq!(
        body(map_get_error(GetObjectError::NoSuchKey))["code"],
        "Not found"
    );
    assert_eq!(
        body(map_get_error(GetObjectError::InvalidRange))["code"],
        "Bad request"
    );
    assert_eq!(
        body(map_get_error(GetObjectError::GovernedUnavailable))["code"],
        "Forbidden"
    );
    assert_eq!(
        body(map_get_error(GetObjectError::ReferenceSourceChanged))["code"],
        "Conflict"
    );
}

#[test]
fn put_error_categories() {
    assert_eq!(
        body(map_put_error(PutObjectError::MissingBody))["code"],
        "Bad request"
    );
    assert_eq!(
        body(map_put_error(PutObjectError::QuotaExceeded {
            limit: 10,
            usage: 20
        }))["code"],
        "Conflict"
    );
    assert_eq!(
        body(map_put_error(PutObjectError::PutObjectFailed))["code"],
        "Internal error"
    );
}

fn sample(at: &str, bytes: u64) -> ObjectSample {
    ObjectSample {
        at: chrono::DateTime::parse_from_rfc3339(at)
            .expect("fixture timestamp")
            .with_timezone(&chrono::Utc),
        bytes,
    }
}

#[test]
fn weeks_start_monday() {
    // A Sunday belongs to the week that began on the preceding Monday.
    let samples = [
        sample("2026-01-04T23:59:59Z", 10),
        sample("2026-01-05T00:00:00Z", 20),
        sample("2026-01-11T12:00:00Z", 30),
    ];
    let folded = fold_buckets(&samples, BucketUnit::Week, 10);
    assert_eq!(
        folded.buckets,
        vec![
            TimeBucketOutput {
                start: "2025-12-29T00:00:00+00:00".to_string(),
                count: 1,
                bytes: 10,
            },
            TimeBucketOutput {
                start: "2026-01-05T00:00:00+00:00".to_string(),
                count: 2,
                bytes: 50,
            },
        ]
    );
    assert_eq!(folded.total_count, 3);
    assert_eq!(folded.total_bytes, 60);
    assert!(!folded.truncated);
}

#[test]
fn months_and_days() {
    let samples = [
        sample("2026-01-31T23:00:00Z", 1),
        sample("2026-02-01T00:00:00Z", 2),
    ];
    let months = fold_buckets(&samples, BucketUnit::Month, 10);
    assert_eq!(months.buckets.len(), 2);
    assert_eq!(months.buckets[0].start, "2026-01-01T00:00:00+00:00");
    assert_eq!(months.buckets[1].start, "2026-02-01T00:00:00+00:00");
    let days = fold_buckets(&samples, BucketUnit::Day, 10);
    assert_eq!(days.buckets[0].start, "2026-01-31T00:00:00+00:00");
}

#[test]
fn folds_empty_window() {
    let folded = fold_buckets(&[], BucketUnit::Day, 1);
    assert!(folded.buckets.is_empty());
    assert_eq!(folded.total_count, 0);
    assert!(!folded.truncated);
}

#[test]
fn caps_bucket_count() {
    // Beyond the cap the series is cut, but the totals still cover it.
    let samples = [
        sample("2026-01-01T00:00:00Z", 1),
        sample("2026-01-02T00:00:00Z", 2),
        sample("2026-01-03T00:00:00Z", 4),
    ];
    let folded = fold_buckets(&samples, BucketUnit::Day, 2);
    assert_eq!(folded.buckets.len(), 2);
    assert!(folded.truncated);
    assert_eq!(folded.total_count, 3);
    assert_eq!(folded.total_bytes, 7);
}

#[test]
fn bound_rejects_garbage() {
    assert!(parse_bound("since", Some("yesterday")).is_err());
    assert!(parse_bound("since", None).unwrap().is_none());
    assert!(
        parse_bound("until", Some("2026-01-01T00:00:00Z"))
            .unwrap()
            .is_some()
    );
}

#[test]
fn filenames_drop_prefix() {
    assert_eq!(filename_of("results/run-1/chart.png"), "chart.png");
    assert_eq!(filename_of("chart.png"), "chart.png");
    assert_eq!(filename_of("results/"), "results");
}

#[test]
fn head_error_categories() {
    assert_eq!(
        body(map_head_error(HeadObjectError::NoSuchKey))["code"],
        "Not found"
    );
    assert!(
        body(map_head_error(HeadObjectError::NoSuchVersion))["error"]
            .as_str()
            .unwrap_or_default()
            .contains("version_id")
    );
    assert_eq!(
        body(map_head_error(HeadObjectError::HeadObjectFailed))["code"],
        "Internal error"
    );
}

#[test]
fn search_kind_names() {
    assert_eq!(SearchKind::Documents.as_str(), "documents");
    assert_eq!(SearchKind::Buckets.as_str(), "buckets");
    assert_eq!(SearchKind::Groups.as_str(), "groups");
    assert_eq!(SearchKind::Users.as_str(), "users");
}
