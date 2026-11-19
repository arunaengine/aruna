//! Tests object search planning and the assembly of its pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

use super::super::search::{
    ObjectSearchPartitions, ObjectSearchPlan, assemble_object_execution, plan_object_search,
};

use super::super::search_types::RangeLayout;

fn object_search_request(realm_id: RealmId, query: &str, limit: usize) -> SearchQueryRequest {
    SearchQueryRequest {
        auth: AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        bearer_token: None,
        query: query.to_string(),
        key_match: ObjectKeyMatch::Substring,
        bucket: None,
        limit,
        cursor: None,
        mode: ObjectQueryMode::Local,
        target_nodes: None,
    }
}

#[test]
fn search_plan_limits() {
    let realm_id = RealmId::from_bytes([70u8; 32]);
    assert!(matches!(
        plan_object_search(realm_id, object_search_request(realm_id, "", 10)),
        Err(MetadataApiError::BadRequest)
    ));
    assert!(matches!(
        plan_object_search(
            realm_id,
            object_search_request(RealmId::from_bytes([71u8; 32]), "query", 10)
        ),
        Err(MetadataApiError::Forbidden)
    ));

    let plan = plan_object_search(realm_id, object_search_request(realm_id, "query", 0))
        .expect("zero limit clamps");
    assert_eq!(plan.limit, 1);
    let plan = plan_object_search(
        realm_id,
        object_search_request(realm_id, "query", usize::MAX),
    )
    .expect("large limit clamps");
    assert_eq!(plan.limit, crate::s3::object::search::SEARCH_MAX_LIMIT);
}

#[test]
fn search_assembly_order() {
    let realm_id = RealmId::from_bytes([72u8; 32]);
    let first = iroh::SecretKey::from_bytes(&[73u8; 32]).public();
    let second = iroh::SecretKey::from_bytes(&[74u8; 32]).public();
    let plan = ObjectSearchPlan {
        auth: AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        bearer_token: None,
        query: "query".to_string(),
        key_match: ObjectKeyMatch::Substring,
        bucket: None,
        limit: 10,
        cursor: None,
        mode: ObjectQueryMode::DistributedBestEffort,
        target_nodes: None,
        fingerprint: [7u8; 32],
    };
    let partitions = || ObjectSearchPartitions {
        as_of: SystemTime::UNIX_EPOCH,
        partitions: vec![
            ObjectPartitionState {
                node_id: first,
                start_after: None,
                exhausted: false,
                observed_at: None,
            },
            ObjectPartitionState {
                node_id: second,
                start_after: None,
                exhausted: false,
                observed_at: None,
            },
        ],
        failed_partitions: Vec::new(),
        discovery_failed: false,
        omitted_partitions: 0,
        pending: Vec::new(),
        changed_keys: Some(0),
        layout: RangeLayout::default(),
    };
    let page = |node_id, key: Option<&str>| SearchNodePage {
        hits: key
            .map(|key| {
                vec![crate::s3::object::search::SearchNodeHit {
                    hit: ObjectInventoryHit {
                        node_id,
                        group_id: Ulid::nil(),
                        bucket: "bucket".to_string(),
                        key: key.to_string(),
                        content_w3id: None,
                        checksum: None,
                        size: None,
                        updated_at: None,
                        metadata: Default::default(),
                    },
                    cursor_key: key.as_bytes().to_vec(),
                }]
            })
            .unwrap_or_default(),
        next_start_after: None,
        observed_at: SystemTime::UNIX_EPOCH,
        incomplete_ranges: 0,
        remote_ranges: 0,
        index_progress: Vec::new(),
        partial_ranges: Vec::new(),
        skipped_buckets: Vec::new(),
        changed: Some(Vec::new()),
        layout: None,
    };
    let stats = || MetadataFanoutStats {
        nodes_queried: 2,
        nodes_failed: 0,
        failed_partitions: Vec::new(),
        discovery_failed: false,
        omitted: Vec::new(),
    };
    // A strict search fails rather than answer with a range nobody searched.
    let strict = ObjectSearchPlan {
        mode: ObjectQueryMode::DistributedStrict,
        auth: plan.auth.clone(),
        query: plan.query.clone(),
        ..plan
    };
    let progress = crate::distribution::feed_walks::IndexProgress {
        ready: false,
        indexed: 10,
        keys: 50,
    };
    let unsearched = SearchNodePage {
        incomplete_ranges: 1,
        index_progress: vec![(Ulid::nil(), progress)],
        partial_ranges: vec![(Ulid::nil(), Some("m".to_string()))],
        ..page(second, None)
    };
    let parts = vec![(first, page(first, Some("a"))), (second, unsearched)];
    let refused = assemble_object_execution((&strict, first), partitions(), parts, stats());
    let Err(MetadataApiError::SearchIncomplete(reason)) = refused else {
        panic!("a strict search with an unsearched range is refused");
    };
    assert!(
        reason.starts_with("1 distributed bucket ranges"),
        "{reason}"
    );
    assert!(
        reason.contains("1 of them are indexed only up to"),
        "{reason}"
    );
    assert!(
        reason.ends_with("about 40 keys are still to be indexed"),
        "{reason}"
    );
    let plan = ObjectSearchPlan {
        mode: ObjectQueryMode::DistributedBestEffort,
        ..strict
    };
    let (execution, cursor) = assemble_object_execution(
        (&plan, first),
        partitions(),
        vec![
            (first, page(first, Some("a"))),
            (second, page(second, None)),
        ],
        stats(),
    )
    .expect("exhausted partitions assemble");

    assert_eq!(
        execution
            .hits
            .iter()
            .map(|hit| hit.key.as_str())
            .collect::<Vec<_>>(),
        vec!["a"]
    );
    assert_eq!(execution.partitions.len(), 2);
    assert!(
        execution
            .partitions
            .iter()
            .all(|partition| !partition.truncated)
    );
    assert!(execution.complete);
    assert!(cursor.is_none());

    // Changed keys of finished pages add to the total the earlier pages carried.
    let changed = |node_id, key: Option<&str>, changed: &str| SearchNodePage {
        changed: Some(vec![changed.as_bytes().to_vec()]),
        ..page(node_id, key)
    };
    let carried = ObjectSearchPartitions {
        changed_keys: Some(3),
        ..partitions()
    };
    let parts = vec![
        (first, changed(first, Some("b"), "a")),
        (second, changed(second, None, "z")),
    ];
    let (execution, _) = assemble_object_execution((&plan, first), carried, parts, stats())
        .expect("exhausted partitions assemble");
    assert_eq!(execution.page_changed, 2);
    assert_eq!(execution.changed_keys, Some(5));

    // A range that moved between nodes or pages may hold keys no member searched.
    let laid = |node_id, set: u8, layout: u8| SearchNodePage {
        layout: Some((Ulid::from_bytes([set; 16]), [layout; 32])),
        ..page(node_id, None)
    };
    let seen = |set: u8, layout: u8, changed: bool| ObjectSearchPartitions {
        layout: RangeLayout {
            seen: vec![(Ulid::from_bytes([set; 16]), [layout; 32])],
            changed,
        },
        ..partitions()
    };
    let cases = [
        (partitions(), [laid(first, 1, 1), laid(second, 1, 2)], false),
        // Two sets of one bucket name on other members are independent.
        (partitions(), [laid(first, 1, 1), laid(second, 2, 2)], true),
        (
            seen(1, 1, false),
            [laid(first, 1, 2), page(second, None)],
            false,
        ),
        (
            seen(1, 1, false),
            [laid(first, 1, 1), laid(second, 1, 1)],
            true,
        ),
        (
            seen(1, 1, true),
            [laid(first, 1, 1), page(second, None)],
            false,
        ),
    ];
    for (carried, [left, right], kept) in cases {
        let parts = vec![(first, left), (second, right)];
        let (execution, _) = assemble_object_execution((&plan, first), carried, parts, stats())
            .expect("exhausted partitions assemble");
        assert_eq!(execution.complete, kept);
        assert_eq!(execution.layout_changed, !kept);
        assert_eq!(execution.changed_keys, kept.then_some(0));
    }
    let strict = ObjectSearchPlan {
        mode: ObjectQueryMode::DistributedStrict,
        ..plan
    };
    let parts = vec![(first, laid(first, 1, 2)), (second, page(second, None))];
    let refused = assemble_object_execution((&strict, first), seen(1, 1, false), parts, stats());
    assert!(matches!(
        refused,
        Err(MetadataApiError::SearchIncomplete(_))
    ));
}
