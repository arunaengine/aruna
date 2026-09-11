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
