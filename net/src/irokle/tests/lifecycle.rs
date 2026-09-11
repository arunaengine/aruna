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

#[tokio::test]
async fn registry_strategy_fenced() {
    let (_dir, storage) = test_storage();
    let group_id = Ulid::from_parts(2_100, 1);
    let document_id = Ulid::from_parts(2_101, 1);
    let mut record = registry_record(
        group_id,
        document_id,
        "datasets/missing-strategy",
        100,
        Ulid::from_parts(2_102, 1),
    );
    record.placement = PlacementRef {
        strategy_id: Ulid::from_parts(2_103, 1),
        shard: 1,
    };
    let mut config = RealmConfigDocument::default_for_realm(record.realm_id, Vec::new());
    config.seed_default_placement();
    let config_target = DocumentSyncTarget::RealmConfig {
        realm_id: record.realm_id,
    };
    storage_batch_write_to(
        &storage,
        vec![(
            config_target.storage_keyspace().to_string(),
            config_target.storage_key(),
            config
                .to_bytes(&test_actor(
                    1,
                    UserId::nil(record.realm_id),
                    record.realm_id,
                ))
                .expect("realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("realm config writes");

    apply_metadata_registry_upsert_to_storage(
        &storage,
        record.clone(),
        postcard::to_allocvec(&record).expect("registry serializes"),
    )
    .await
    .expect("invalid strategy is rejected without wedging reconciliation");

    assert!(
        read_storage_value(
            &storage,
            METADATA_INDEX_KEYSPACE,
            metadata_registry_key(group_id, document_id),
        )
        .await
        .is_none()
    );
}

#[tokio::test]
async fn upsert_keeps_config() {
    // The placement fence reads the realm config inside its transaction and
    // must not write it back: an identical write only conflicts the config's
    // readers, which is how inbound sync used to stall concurrent creates.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([42; 32]);
    let group_id = Ulid::from_parts(2_130, 1);
    let actor = test_actor(1, UserId::nil(realm_id), realm_id);
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    let placement = PlacementRef {
        strategy_id: config.default_strategy_id.unwrap(),
        shard: 4,
    };
    let document_id = MetaResourceId::from_parts(
        2_131,
        PlacementHandle::new(METADATA_HANDLE).unwrap(),
        BucketId::new(placement.shard as u16).unwrap(),
        1,
    )
    .unwrap()
    .as_ulid();
    let mut record = registry_record(
        group_id,
        document_id,
        "datasets/config-untouched",
        100,
        Ulid::from_parts(2_132, 1),
    );
    record.placement = placement;
    let config_target = DocumentSyncTarget::RealmConfig {
        realm_id: record.realm_id,
    };
    storage_batch_write_to(
        &storage,
        vec![(
            config_target.storage_keyspace().to_string(),
            config_target.storage_key(),
            config
                .to_bytes(&actor)
                .expect("realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("realm config writes");
    let before = storage.snapshot_metrics().requests_total;

    apply_metadata_registry_upsert_to_storage(
        &storage,
        record.clone(),
        postcard::to_allocvec(&record).expect("registry serializes"),
    )
    .await
    .expect("registry upsert applies");

    assert!(
        read_storage_value(
            &storage,
            METADATA_INDEX_KEYSPACE,
            metadata_registry_key(group_id, document_id),
        )
        .await
        .is_some(),
        "the upsert still lands"
    );
    assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
    assert!(storage.snapshot_metrics().requests_total > before);
}

#[tokio::test]
async fn capacity_retains_cursors() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([42; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(77).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");
    let local_node = service.local_node_id().expect("local node id");
    let actor = test_actor(
        77,
        UserId::local(Ulid::from_parts(2_120, 1), realm_id),
        realm_id,
    );
    assert_eq!(actor.node_id, local_node);

    let strategy_id = Ulid::from_parts(2_121, 1);
    let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(local_node, RealmNodeKind::Management);
    config.placement_bindings.push(PlacementBinding {
        handle,
        scope: PlacementScope::Realm(realm_id),
        document_class: DocumentClass::Metadata,
        strategy_id,
        allocator_range_id: None,
        allocated_by: None,
        allocated_at_ms: None,
    });
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentSyncTarget::RealmConfig { realm_id },
            config
                .to_bytes(&actor)
                .expect("realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("realm config writes");

    assert!(config.strategy(&strategy_id).is_none());
    let registry_placement = PlacementRef {
        strategy_id,
        shard: 4,
    };
    let create_placement = PlacementRef {
        strategy_id,
        shard: 5,
    };
    let registry_group_id = Ulid::from_parts(2_122, 1);
    let registry_document_id = MetaResourceId::from_parts(
        2_123,
        handle,
        BucketId::new(registry_placement.shard as u16).unwrap(),
        1,
    )
    .unwrap()
    .as_ulid();
    let registry_event_id = Ulid::from_parts(2_124, 1);
    let mut registry = registry_record(
        registry_group_id,
        registry_document_id,
        "datasets/capacity-registry",
        100,
        registry_event_id,
    );
    registry.placement = registry_placement;
    let registry_target = DocumentSyncTarget::MetadataRegistry {
        group_id: registry_group_id,
        document_id: registry_document_id,
    };

    let create_group_id = Ulid::from_parts(2_125, 1);
    let create_document_id = MetaResourceId::from_parts(
        2_126,
        handle,
        BucketId::new(create_placement.shard as u16).unwrap(),
        1,
    )
    .unwrap()
    .as_ulid();
    let create_event_id = Ulid::from_parts(2_127, 1);
    let mut create = metadata_create_event(
        create_group_id,
        create_document_id,
        100,
        create_event_id,
        77,
    );
    create.record.placement = create_placement;
    let create_target = DocumentSyncTarget::MetadataCreateEvent {
        document_id: create_document_id,
        event_id: create_event_id,
    };
    let registry_topic = registry_target.sync_topic_id(realm_id, &registry_placement);
    let create_topic = create_target.sync_topic_id(realm_id, &create_placement);
    service
        .ensure_document_sync_topics(&[registry_topic, create_topic], Vec::new())
        .expect("metadata shard topic genesis");
    let change = |event_id, placement| DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id,
            actor: local_node,
            updated_at_ms: 100,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement,
    };
    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::Upsert {
                    event_id: registry_event_id,
                    target: registry_target,
                    bytes: postcard::to_allocvec(&registry).expect("registry serializes"),
                    change: change(registry_event_id, registry_placement),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: create_event_id,
                    target: create_target,
                    bytes: postcard::to_allocvec(&create).expect("create serializes"),
                    change: change(create_event_id, create_placement),
                    allow_genesis: true,
                },
            ],
            Vec::new(),
        )
        .await;
    assert!(
        matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
        "metadata publish failed: {published:?}"
    );
    for topic_id in [registry_topic, create_topic] {
        assert_ne!(
            service
                .node()
                .storage()
                .actor_clock(&topic_id)
                .expect("metadata topic clock"),
            irokle_crate::ActorClock::default(),
            "metadata topic must contain its published event"
        );
        reset_test_cursor(&service, topic_id).await;
    }

    let dependency = DocumentSyncDependency::PlacementStrategy {
        realm_id,
        strategy_id,
    };
    assert!(
        !document_sync_dependency_available(&storage, dependency)
            .await
            .expect("placement dependency checks")
    );
    let mut filler_topics = BTreeSet::new();
    for index in 0..MAX_DEFERRED_TOPICS_PER_DEPENDENCY {
        let mut filler_realm = [0xA5; 32];
        filler_realm[..8].copy_from_slice(&(index as u64).to_be_bytes());
        let filler_realm = RealmId::from_bytes(filler_realm);
        filler_topics.insert(
            DocumentSyncTarget::RealmConfig {
                realm_id: filler_realm,
            }
            .sync_topic_id(filler_realm, &PlacementRef::NIL),
        );
    }
    assert_eq!(filler_topics.len(), MAX_DEFERRED_TOPICS_PER_DEPENDENCY);
    assert!(!filler_topics.contains(&registry_topic));
    assert!(!filler_topics.contains(&create_topic));
    let deferred_topics = BTreeMap::from([(dependency, filler_topics)]);
    let mut capacity_probe = deferred_topics.clone();
    let existing_topic = *capacity_probe
        .get(&dependency)
        .and_then(BTreeSet::first)
        .expect("full dependency has a topic");
    assert_eq!(
        register_deferred_topic(&mut capacity_probe, dependency, existing_topic),
        DeferredTopicRegistrationOutcome::AlreadyRegistered
    );
    assert_eq!(
        register_deferred_topic(&mut capacity_probe, dependency, registry_topic),
        DeferredTopicRegistrationOutcome::CapacityExceeded
    );
    service
        .storage_write(
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
            deferred_topics_key(),
            postcard::to_allocvec(&deferred_topics)
                .expect("deferred topics serialize")
                .into(),
        )
        .await
        .expect("full deferred topic registry writes");

    service
        .reconcile_document_topics([registry_topic, create_topic])
        .await
        .expect("metadata reconciliation remains retryable at capacity");
    let deferred_topics: BTreeMap<DocumentSyncDependency, BTreeSet<irokle_crate::TopicId>> =
        postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                deferred_topics_key(),
            )
            .await
            .expect("deferred topic registry remains stored"),
        )
        .expect("deferred topic registry decodes");
    assert_eq!(
        deferred_topics.get(&dependency).map(BTreeSet::len),
        Some(MAX_DEFERRED_TOPICS_PER_DEPENDENCY)
    );
    let registered_dependencies = deferred_topics
        .iter()
        .filter(|(_, topics)| topics.contains(&registry_topic) || topics.contains(&create_topic))
        .map(|(dependency, _)| *dependency)
        .collect::<Vec<_>>();
    assert!(
        registered_dependencies.is_empty(),
        "expected full {dependency:?}; topics registered under {registered_dependencies:?}"
    );
    for topic_id in [registry_topic, create_topic] {
        let cursor = read_test_cursor(&storage, topic_id)
            .await
            .unwrap_or_default();
        assert_eq!(
            cursor,
            irokle_crate::ActorClock::default(),
            "capacity-blocked metadata dependency changed cursor for {topic_id}"
        );
        let topic_clock = service
            .node()
            .storage()
            .actor_clock(&topic_id)
            .expect("metadata topic clock");
        assert!(
            !cursor.dominates(&topic_clock),
            "capacity-blocked metadata dependency advanced cursor for {topic_id}"
        );
    }

    service.shutdown().await;
}
