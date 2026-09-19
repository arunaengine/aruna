//! Tests bucket and object search fan-out: partial results, denials, deadlines and caps.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

use super::super::search::{
    ObjectSearchPartitions, ObjectSearchPlan, assemble_object_execution, plan_object_search,
};

#[tokio::test]
async fn bucket_fanout_partial() {
    let directory = tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let local = iroh::SecretKey::from_bytes(&[41u8; 32]).public();
    let healthy = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
    let failed = iroh::SecretKey::from_bytes(&[43u8; 32]).public();
    let local_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(1) });
    let remote_call: MetadataNodeCall<usize> =
        metadata_node_call(failed, |failed, node_id| async move {
            if node_id == failed {
                Err(MetadataReadError::Unavailable)
            } else {
                Ok(2)
            }
        });

    let (parts, stats) = run_metadata_fanout(
        &context,
        RealmId::from_bytes([9u8; 32]),
        local,
        MetadataFanoutScope::new(
            Some(ApiQueryMode::Distributed),
            Some(vec![local, healthy, failed]),
            true,
        ),
        MetadataFanoutOperation::BucketSearch,
        local_call,
        remote_call,
        |_, _| {},
        map_read_error,
    )
    .await
    .unwrap();

    assert_eq!(parts, vec![(local, 1), (healthy, 2)]);
    assert_eq!(stats.nodes_queried, 3);
    assert_eq!(stats.nodes_failed, 1);
    assert_eq!(stats.failed_partitions, vec![failed]);
}

#[tokio::test]
async fn object_fanout_reports() {
    let directory = tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let local = iroh::SecretKey::from_bytes(&[81u8; 32]).public();
    let healthy = iroh::SecretKey::from_bytes(&[82u8; 32]).public();
    let failed = iroh::SecretKey::from_bytes(&[83u8; 32]).public();
    let local_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(1) });
    let remote_call: MetadataNodeCall<usize> =
        metadata_node_call(failed, |failed, node_id| async move {
            if node_id == failed {
                Err(MetadataReadError::Unavailable)
            } else {
                Ok(2)
            }
        });

    let (parts, stats) = run_metadata_fanout(
        &context,
        RealmId::from_bytes([19u8; 32]),
        local,
        MetadataFanoutScope::new(
            Some(ApiQueryMode::Distributed),
            Some(vec![local, healthy, failed]),
            ObjectQueryMode::DistributedBestEffort.allow_partial(),
        ),
        MetadataFanoutOperation::ObjectSearch,
        local_call,
        remote_call,
        |_, _| {},
        map_read_error,
    )
    .await
    .unwrap();

    assert_eq!(parts, vec![(local, 1), (healthy, 2)]);
    assert_eq!(stats.nodes_queried, 3);
    assert_eq!(stats.nodes_failed, 1);
    assert_eq!(stats.failed_partitions, vec![failed]);
}

#[tokio::test]
async fn object_fanout_strict() {
    let directory = tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let local = iroh::SecretKey::from_bytes(&[84u8; 32]).public();
    let failed = iroh::SecretKey::from_bytes(&[85u8; 32]).public();
    let local_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(1) });
    let remote_call: MetadataNodeCall<usize> =
        metadata_node_call(
            (),
            |(), _| async move { Err(MetadataReadError::Unavailable) },
        );

    let result = run_metadata_fanout(
        &context,
        RealmId::from_bytes([20u8; 32]),
        local,
        MetadataFanoutScope::new(
            Some(ApiQueryMode::Distributed),
            Some(vec![local, failed]),
            ObjectQueryMode::DistributedStrict.allow_partial(),
        ),
        MetadataFanoutOperation::ObjectSearch,
        local_call,
        remote_call,
        |_, _| {},
        map_read_error,
    )
    .await;

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
}

#[tokio::test]
async fn bucket_denial_wins() {
    let directory = tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let local = iroh::SecretKey::from_bytes(&[49u8; 32]).public();
    let denied = iroh::SecretKey::from_bytes(&[50u8; 32]).public();
    let local_call: MetadataNodeCall<Vec<BucketSearchHit>> =
        metadata_node_call((), |(), _| async move { Ok(Vec::new()) });
    let remote_call: MetadataNodeCall<Vec<BucketSearchHit>> =
        metadata_node_call(denied, |denied, node_id| async move {
            if node_id == denied {
                Err(MetadataReadError::Forbidden)
            } else {
                Ok(Vec::new())
            }
        });

    let result = run_metadata_fanout(
        &context,
        RealmId::from_bytes([12u8; 32]),
        local,
        MetadataFanoutScope::new(
            Some(ApiQueryMode::Distributed),
            Some(vec![local, denied]),
            true,
        ),
        MetadataFanoutOperation::BucketSearch,
        local_call,
        remote_call,
        |_, _| {},
        map_read_error,
    )
    .await;

    assert!(matches!(result, Err(MetadataApiError::Forbidden)));
}

#[tokio::test]
async fn fanout_missing_fails() {
    let directory = tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let local = iroh::SecretKey::from_bytes(&[47u8; 32]).public();
    let stale = iroh::SecretKey::from_bytes(&[48u8; 32]).public();
    let local_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(1) });
    let remote_call: MetadataNodeCall<usize> =
        metadata_node_call(stale, |stale, node_id| async move {
            if node_id == stale {
                Err(MetadataReadError::NotFound)
            } else {
                Ok(2)
            }
        });

    let result = run_metadata_fanout(
        &context,
        RealmId::from_bytes([11u8; 32]),
        local,
        MetadataFanoutScope::new(
            Some(ApiQueryMode::Distributed),
            Some(vec![local, stale]),
            true,
        ),
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        |_, _| {},
        map_read_error,
    )
    .await;

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
}

#[tokio::test(start_paused = true)]
async fn search_deadline_partial() {
    // A node that never answers must not stall search fanout: the overall
    // deadline fails its partition and the reachable nodes still answer.
    let directory = tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let local = iroh::SecretKey::from_bytes(&[44u8; 32]).public();
    let healthy = iroh::SecretKey::from_bytes(&[45u8; 32]).public();
    let hanging = iroh::SecretKey::from_bytes(&[46u8; 32]).public();
    let local_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(1) });
    let remote_call: MetadataNodeCall<usize> =
        metadata_node_call(hanging, |hanging, node_id| async move {
            if node_id == hanging {
                std::future::pending::<Result<usize, MetadataReadError>>().await
            } else {
                Ok(2)
            }
        });

    let (parts, stats) = run_metadata_fanout(
        &context,
        RealmId::from_bytes([10u8; 32]),
        local,
        MetadataFanoutScope::new(
            Some(ApiQueryMode::Distributed),
            Some(vec![local, healthy, hanging]),
            true,
        ),
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        |_, _| {},
        map_read_error,
    )
    .await
    .unwrap();

    assert_eq!(parts, vec![(local, 1), (healthy, 2)]);
    assert_eq!(stats.nodes_queried, 3);
    assert_eq!(stats.nodes_failed, 1);
    assert_eq!(stats.failed_partitions, vec![hanging]);
}

#[tokio::test]
async fn capped_fanout_incomplete() {
    // More realm nodes than the fanout cap truncates the node set, which a
    // caller that refused partial results must not receive as complete.
    let directory = tempdir().unwrap();
    let context = DriverContext {
        storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let nodes = (0..=QUERY_MAX_NODES)
        .map(|index| iroh::SecretKey::from_bytes(&[60 + index as u8; 32]).public())
        .collect::<Vec<_>>();
    let local = nodes[0];
    let local_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(1) });
    let remote_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(2) });

    let result = run_metadata_fanout(
        &context,
        RealmId::from_bytes([13u8; 32]),
        local,
        MetadataFanoutScope::new(Some(ApiQueryMode::Distributed), Some(nodes), false),
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        |_, _| {},
        map_read_error,
    )
    .await;

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
}

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
    let test = metadata_test();
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
    let partitions = ObjectSearchPartitions {
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
                    },
                    cursor_key: key.as_bytes().to_vec(),
                }]
            })
            .unwrap_or_default(),
        next_start_after: None,
        observed_at: SystemTime::UNIX_EPOCH,
    };
    let execution = assemble_object_execution(
        &test.context,
        &plan,
        partitions,
        vec![
            (first, page(first, Some("a"))),
            (second, page(second, None)),
        ],
        MetadataFanoutStats {
            nodes_queried: 2,
            nodes_failed: 0,
            failed_partitions: Vec::new(),
            discovery_failed: false,
        },
    )
    .expect("exhausted partitions assemble without a net handle");

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
    assert!(execution.next_cursor.is_none());
}
