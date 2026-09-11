use super::*;

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
            Some(MetadataApiQueryMode::Distributed),
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
async fn object_fanout_reports_partial_partitions() {
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
            Some(MetadataApiQueryMode::Distributed),
            Some(vec![local, healthy, failed]),
            ObjectSearchQueryMode::DistributedBestEffort.allow_partial(),
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
async fn object_fanout_strict_fails_instead_of_downgrading() {
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
            Some(MetadataApiQueryMode::Distributed),
            Some(vec![local, failed]),
            ObjectSearchQueryMode::DistributedStrict.allow_partial(),
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
            Some(MetadataApiQueryMode::Distributed),
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
            Some(MetadataApiQueryMode::Distributed),
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
            Some(MetadataApiQueryMode::Distributed),
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
    let nodes = (0..=METADATA_DISTRIBUTED_QUERY_MAX_NODES)
        .map(|index| iroh::SecretKey::from_bytes(&[60 + index as u8; 32]).public())
        .collect::<Vec<_>>();
    let local = nodes[0];
    let local_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(1) });
    let remote_call: MetadataNodeCall<usize> = metadata_node_call((), |(), _| async move { Ok(2) });

    let result = run_metadata_fanout(
        &context,
        RealmId::from_bytes([13u8; 32]),
        local,
        MetadataFanoutScope::new(Some(MetadataApiQueryMode::Distributed), Some(nodes), false),
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        |_, _| {},
        map_read_error,
    )
    .await;

    assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
}
