use super::*;

#[tokio::test]
async fn metadata_registry_upsert_skips_stale_local_record() {
    let (_dir, storage) = test_storage();
    let group_id = Ulid::from_parts(1, 1);
    let document_id = Ulid::from_parts(2, 2);
    let local = registry_record(
        group_id,
        document_id,
        "datasets/fresh",
        200,
        Ulid::from_parts(200, 2),
    );
    write_registry_record(&storage, &local).await;

    let mut stale = registry_record(
        group_id,
        document_id,
        "datasets/stale",
        100,
        Ulid::from_parts(100, 1),
    );
    stale.public = false;
    stale.holder_node_ids = vec![node(2)];
    let stale_bytes = postcard::to_allocvec(&stale).expect("stale registry serializes");

    apply_metadata_registry_upsert_to_storage(&storage, stale, stale_bytes)
        .await
        .expect("stale registry upsert succeeds idempotently");

    let primary = read_registry_record(
        &storage,
        METADATA_INDEX_KEYSPACE,
        metadata_registry_key(group_id, document_id),
    )
    .await;
    let document_index = read_registry_record(
        &storage,
        METADATA_DOCUMENT_INDEX_KEYSPACE,
        metadata_document_key(document_id),
    )
    .await;
    let holder_value = read_storage_value(
        &storage,
        METADATA_HOLDERS_KEYSPACE,
        metadata_registry_key(group_id, document_id),
    )
    .await
    .expect("holder index exists");
    let holders: Vec<NodeId> = postcard::from_bytes(&holder_value).expect("holders decode");

    assert_eq!(primary, local);
    assert_eq!(document_index, local);
    assert_eq!(holders, local.holder_node_ids);
}

#[tokio::test]
async fn registry_replay_repairs() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([42; 32]);
    let group_id = Ulid::from_parts(2_110, 1);
    let actor = test_actor(1, UserId::nil(realm_id), realm_id);
    let strategy_id = Ulid::from_parts(2_113, 1);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.default_strategy_id = Some(strategy_id);
    config.strategies.push(PlacementStrategy {
        strategy_id,
        name: "test".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    });
    config.placement_bindings.push(PlacementBinding {
        handle: PlacementHandle::new(METADATA_HANDLE).unwrap(),
        scope: PlacementScope::Realm(realm_id),
        document_class: DocumentClass::Metadata,
        strategy_id,
        allocator_range_id: None,
        allocated_by: None,
        allocated_at_ms: None,
    });
    let placement = PlacementRef {
        strategy_id,
        shard: 4,
    };
    let document_id = MetaResourceId::from_parts(
        2_111,
        PlacementHandle::new(METADATA_HANDLE).unwrap(),
        BucketId::new(placement.shard as u16).unwrap(),
        1,
    )
    .unwrap()
    .as_ulid();
    let mut record = registry_record(
        group_id,
        document_id,
        "datasets/replay-repair",
        100,
        Ulid::from_parts(2_112, 1),
    );
    record.placement = placement;
    storage_batch_write_to(
        &storage,
        vec![(
            DocumentSyncTarget::RealmConfig { realm_id }
                .storage_keyspace()
                .to_string(),
            DocumentSyncTarget::RealmConfig { realm_id }.storage_key(),
            config
                .to_bytes(&actor)
                .expect("realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("realm config writes");
    let mut entries = metadata_registry_write_entries(&record).expect("registry entries build");
    let primary = entries.remove(0);
    storage_batch_write_to(&storage, vec![primary])
        .await
        .expect("registry primary writes");

    apply_metadata_registry_upsert_to_storage(
        &storage,
        record.clone(),
        postcard::to_allocvec(&record).expect("registry serializes"),
    )
    .await
    .expect("equal registry replay repairs sidecars");

    assert_registry_record_present(&storage, &record).await;
}

#[tokio::test]
async fn registry_fence_ulid() {
    let (_dir, storage) = test_storage();
    let group_id = Ulid::from_parts(2_120, 1);
    let document_id = Ulid::from_parts(2_121, 1);
    let boundary = Ulid::from_parts(2_122, 1);
    let delete = metadata_delete_lifecycle(
        group_id,
        document_id,
        200,
        Ulid::from_parts(2_123, 1),
        boundary,
    );
    write_document_lifecycle_record(&storage, &delete).await;
    let record = registry_record(
        group_id,
        document_id,
        "datasets/post-delete",
        100,
        Ulid::from_parts(2_124, 1),
    );
    let txn_id = start_storage_transaction(&storage)
        .await
        .expect("fence transaction starts");
    assert!(
        record_fenced_txn(&storage, &record, txn_id)
            .await
            .expect("newer ULID fence checks")
    );
    match storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionAborted { .. }) => {}
        other => panic!("unexpected fence transaction result: {other:?}"),
    }

    let live = registry_record(group_id, document_id, "datasets/post-delete", 300, boundary);
    write_registry_record(&storage, &live).await;
    let stale = registry_record(
        group_id,
        document_id,
        "datasets/post-delete",
        100,
        Ulid::from_parts(2_124, 1),
    );
    let txn_id = start_storage_transaction(&storage)
        .await
        .expect("stale fence transaction starts");
    assert!(
        !record_fenced_txn(&storage, &stale, txn_id)
            .await
            .expect("stale registry fence checks live primary")
    );
    match storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionAborted { .. }) => {}
        other => panic!("unexpected stale fence transaction result: {other:?}"),
    }
}
