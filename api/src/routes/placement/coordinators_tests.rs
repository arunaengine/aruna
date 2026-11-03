//! Tests the cursor and the joined answer of a pass run on every coordinator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::placement::BlockedGapBody;

fn answer(status: &str, minted: usize, cursor: Option<&str>) -> BulkRunResponse {
    BulkRunResponse {
        operation_id: "op".to_string(),
        status: status.to_string(),
        generation: 4,
        target_policies: Vec::new(),
        observed: minted + 1,
        covered: 1,
        minted,
        replanned: 0,
        blocked: vec![BlockedGapBody {
            key: format!("k{minted}"),
            reason: "source_unavailable".to_string(),
        }],
        cursor: cursor.map(str::to_string),
        complete: cursor.is_none(),
        unreached: Vec::new(),
    }
}

fn request() -> BulkRunRequest {
    BulkRunRequest {
        operation_id: "op".to_string(),
        limit: None,
        cursor: None,
    }
}

#[test]
fn cursor_round_trips() {
    let mut cursor = SpreadCursor::default();
    cursor.next.insert("a".to_string(), "6b0d".to_string());
    cursor.done.insert("b".to_string());
    let encoded = cursor.encode().unwrap();
    assert_eq!(SpreadCursor::decode(Some(&encoded)).unwrap(), cursor);
    assert!(matches!(
        SpreadCursor::decode(Some("not hex")),
        Err(ServerError::BadRequest)
    ));
}

#[test]
fn joins_coordinator_answers() {
    let mut cursor = SpreadCursor::default();
    cursor.next.insert("a".to_string(), "6b0d".to_string());
    let joined = join(
        &request(),
        vec![
            answer("active", 2, Some("6b0d")),
            answer("completed", 3, None),
        ],
        cursor,
        Vec::new(),
    )
    .unwrap();
    assert_eq!(
        (joined.minted, joined.observed, joined.blocked.len()),
        (5, 7, 2)
    );
    assert_eq!(joined.status, "active");
    assert!(!joined.complete && joined.cursor.is_some());

    let done = join(
        &request(),
        vec![answer("completed", 0, None)],
        SpreadCursor::default(),
        Vec::new(),
    )
    .unwrap();
    assert_eq!(done.status, "completed");
    assert!(done.complete && done.cursor.is_none());

    // A coordinator that did not answer keeps the run open, and a moved default ends it.
    let open = join(
        &request(),
        vec![answer("completed", 0, None)],
        SpreadCursor::default(),
        vec!["c".to_string()],
    )
    .unwrap();
    assert_eq!(open.status, "active");
    assert!(!open.complete && open.cursor.is_some());
    let moved = join(
        &request(),
        vec![answer("superseded", 0, None)],
        SpreadCursor::default(),
        Vec::new(),
    )
    .unwrap();
    assert_eq!(moved.status, "superseded");
}

fn coverage(observed: usize, cursor: Option<&str>, limits: &[&str]) -> CoverageResponse {
    CoverageResponse {
        bucket: "spire".to_string(),
        scope: "current".to_string(),
        generation: 4,
        target_policies: Vec::new(),
        observed,
        deleted: 1,
        gaps: Vec::new(),
        registered: observed,
        quarantined: 0,
        absent: 0,
        reference_only: 0,
        cursor: cursor.map(str::to_string),
        complete: cursor.is_none(),
        limits: limits.iter().map(|limit| limit.to_string()).collect(),
        unreached: Vec::new(),
    }
}

#[test]
fn joins_coverage_pages() {
    let local = [
        "responder_local",
        "coordinated_ranges",
        "historical_excluded",
    ];
    let mut cursor = SpreadCursor::default();
    cursor.next.insert("a".to_string(), "6b0d".to_string());
    let spread = Spread {
        answers: vec![coverage(3, Some("6b0d"), &local), coverage(5, None, &local)],
        cursor,
        unreached: Vec::new(),
    };
    let joined = join_coverage("spire", spread).unwrap();
    assert_eq!(
        (joined.observed, joined.deleted, joined.registered),
        (8, 2, 8)
    );
    assert_eq!(joined.limits, vec!["bounded_page", "historical_excluded"]);
    assert!(!joined.complete && joined.cursor.is_some());

    // Every coordinator done: the report covers the whole bucket, nothing is left open.
    let spread = Spread {
        answers: vec![coverage(2, None, &local)],
        cursor: SpreadCursor::default(),
        unreached: Vec::new(),
    };
    let whole = join_coverage("spire", spread).unwrap();
    assert_eq!(whole.limits, vec!["historical_excluded"]);
    assert!(whole.complete && whole.unreached.is_empty());
}
