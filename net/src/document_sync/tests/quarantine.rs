use super::*;

#[tokio::test]
async fn quarantine_keeps_placement() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([72u8; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(72).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");
    let local_node = service.local_node_id().expect("local node id");

    let placement = PlacementRef {
        strategy_id: Ulid::from_parts(7_200, 1),
        shard: 5,
    };
    let target = DocumentSyncTarget::PersistentIdMapping {
        document_id: Ulid::from_parts(7_201, 1),
    };
    let topic_id = target.sync_topic_id(realm_id, &placement);
    service
        .ensure_sync_topics(&[topic_id], Vec::new())
        .expect("mapping shard topic genesis");
    let event_id = Ulid::generate();
    let published = service
        .publish_documents(
            vec![DocumentSyncPublish::Delete {
                event_id,
                target: target.clone(),
                change: DocumentSyncChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: 1,
                        event_id: Ulid::generate(),
                        actor: local_node,
                        updated_at_ms: 1,
                    },
                    kind: DocumentSyncChangeKind::Delete,
                    placement,
                },
                allow_genesis: true,
            }],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));

    reset_test_cursor(&service, topic_id).await;
    service
        .reconcile_document_topics([topic_id])
        .await
        .expect("unsupported mapping delete is quarantined");

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].reason,
        "unsupported non-upsert persistent id mapping event"
    );
    assert_eq!(records[0].target(), Some(&target));
    // The stopgap re-wrapped rejects with a NIL placement; the real one rides along.
    assert_eq!(
        records[0]
            .decoded_event()
            .expect("event decodes")
            .placement(),
        placement
    );
    assert!(cursor_advanced(&service, &storage, topic_id).await);

    service.shutdown().await;
}

#[tokio::test]
async fn quarantine_retains_families() {
    use aruna_core::structs::{WatchEventKind, WatchEventMask};

    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([70u8; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(70).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");
    let local_node = service.local_node_id().expect("local node id");
    let forged_node = node(71);
    let owner = UserId::local(Ulid::from_bytes([3; 16]), realm_id);

    let change = |actor, generation, kind| DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation,
            event_id: Ulid::generate(),
            actor,
            updated_at_ms: 1,
        },
        kind,
        placement: PlacementRef::NIL,
    };

    // Same family, invalid then valid: an unusable path prefix, a delete
    // signed by a node that is not the revision's actor, then a good upsert.
    let invalid_watch = WatchSubscription::new(
        owner,
        "/leading-slash".to_string(),
        WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        1,
    );
    let deleted_watch = WatchSubscription::new(
        owner,
        "deleted".to_string(),
        WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        1,
    );
    let valid_watch = WatchSubscription::new(
        owner,
        "documents".to_string(),
        WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        1,
    );
    let watch_target = |watch: &WatchSubscription| DocumentSyncTarget::WatchSubscription {
        owner,
        watch_id: watch.watch_id,
    };
    let watch_topic = watch_target(&valid_watch).sync_topic_id(realm_id, &PlacementRef::NIL);

    let info_target = DocumentSyncTarget::NodeInfo {
        realm_id,
        node_id: local_node,
    };
    let info_topic = info_target.sync_topic_id(realm_id, &PlacementRef::NIL);

    let invalid_watch_event = Ulid::generate();
    let forged_delete_event = Ulid::generate();
    let valid_watch_event = Ulid::generate();
    let invalid_info_event = Ulid::generate();
    let valid_info_event = Ulid::generate();
    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::Upsert {
                    event_id: invalid_watch_event,
                    target: watch_target(&invalid_watch),
                    bytes: invalid_watch.to_bytes().expect("subscription serializes"),
                    change: change(local_node, 1, DocumentSyncChangeKind::Upsert),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Delete {
                    event_id: forged_delete_event,
                    target: watch_target(&deleted_watch),
                    change: change(forged_node, 2, DocumentSyncChangeKind::Delete),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: valid_watch_event,
                    target: watch_target(&valid_watch),
                    bytes: valid_watch.to_bytes().expect("subscription serializes"),
                    change: change(local_node, 1, DocumentSyncChangeKind::Upsert),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: invalid_info_event,
                    target: info_target.clone(),
                    bytes: node_info_bytes(forged_node, 5),
                    change: change(local_node, 1, DocumentSyncChangeKind::Upsert),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: valid_info_event,
                    target: info_target.clone(),
                    bytes: node_info_bytes(local_node, 6),
                    change: change(local_node, 2, DocumentSyncChangeKind::Upsert),
                    allow_genesis: true,
                },
            ],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));

    reset_test_cursor(&service, watch_topic).await;
    reset_test_cursor(&service, info_topic).await;
    service
        .reconcile_document_topics([watch_topic, info_topic])
        .await
        .expect("rejected events are quarantined");

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 3, "{records:?}");
    assert!(
        quarantined_reason(&records, invalid_watch_event)
            .starts_with("invalid watch subscription:")
    );
    assert_eq!(
        quarantined_reason(&records, forged_delete_event),
        "watch subscription delete actor is not its publisher"
    );
    assert!(
        quarantined_reason(&records, invalid_info_event).starts_with("invalid node info document:")
    );
    // The complete received envelope survives, target included.
    let invalid_watch_record = records
        .iter()
        .find(|record| record.event_id() == Some(invalid_watch_event))
        .expect("watch evidence");
    assert_eq!(
        invalid_watch_record.family(),
        Some(SyncQuarantineFamily::Upsert)
    );
    assert_eq!(
        invalid_watch_record.target(),
        Some(&watch_target(&invalid_watch)),
        "evidence keeps the real target, not a placeholder"
    );
    assert_eq!(invalid_watch_record.origin(), Some(local_node));
    assert_eq!(invalid_watch_record.identity.topic, watch_topic);
    match invalid_watch_record.decoded_event().expect("event decodes") {
        DocumentSyncEvent::Upsert {
            event_id, bytes, ..
        } => {
            assert_eq!(event_id, invalid_watch_event);
            assert_eq!(
                WatchSubscription::from_bytes(&bytes).expect("subscription decodes"),
                invalid_watch
            );
        }
        other => panic!("unexpected quarantined event: {other:?}"),
    }
    let forged_delete_record = records
        .iter()
        .find(|record| record.event_id() == Some(forged_delete_event))
        .expect("delete evidence");
    assert_eq!(
        forged_delete_record.family(),
        Some(SyncQuarantineFamily::Delete)
    );
    assert_eq!(forged_delete_record.origin(), Some(forged_node));

    // The valid successors of both families applied.
    assert!(
        read_storage_value(
            &storage,
            watch_target(&valid_watch).storage_keyspace(),
            watch_target(&valid_watch).storage_key(),
        )
        .await
        .is_some()
    );
    assert_eq!(
        read_storage_value(
            &storage,
            info_target.storage_keyspace(),
            info_target.storage_key(),
        )
        .await
        .expect("node info applied")
        .as_ref(),
        node_info_bytes(local_node, 6).as_slice()
    );
    assert!(cursor_advanced(&service, &storage, watch_topic).await);
    assert!(cursor_advanced(&service, &storage, info_topic).await);

    let usage = quarantine_usage(&storage).await;
    assert_eq!(usage.records, 3);
    assert_eq!(
        usage.bytes,
        records
            .iter()
            .map(|record| record.to_bytes().expect("record serializes").len() as u64)
            .sum::<u64>()
    );

    // Replay: the same evidence, no duplicate rows and no usage growth.
    reset_test_cursor(&service, watch_topic).await;
    reset_test_cursor(&service, info_topic).await;
    service
        .reconcile_document_topics([watch_topic, info_topic])
        .await
        .expect("replay reconciles");
    let replayed = quarantine_rows(&storage).await;
    assert_eq!(replayed.len(), 3);
    assert_eq!(quarantine_usage(&storage).await, usage);

    service.shutdown().await;
}

#[tokio::test]
async fn quarantine_rejects_placement() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([42; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(75).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");
    let local_node = service.local_node_id().expect("local node id");
    let actor = test_actor(
        75,
        UserId::local(Ulid::from_parts(2_150, 1), realm_id),
        realm_id,
    );
    assert_eq!(actor.node_id, local_node);

    let strategy_id = Ulid::from_parts(2_151, 1);
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
    config.strategies.push(PlacementStrategy {
        strategy_id,
        name: "placed".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    });
    batch_write_to(
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

    // A document whose stamped shard is not the one its id decodes to fails
    // the placement fence permanently, on both the registry and create paths.
    let document = |bucket: u16, seed: u64| {
        MetaResourceId::from_parts(seed, handle, BucketId::new(bucket).unwrap(), 1)
            .unwrap()
            .as_ulid()
    };
    let group_id = Ulid::from_parts(2_152, 1);
    let mismatched = PlacementRef {
        strategy_id,
        shard: 9,
    };
    let matching = PlacementRef {
        strategy_id,
        shard: 4,
    };
    let bad_document = document(4, 2_153);
    let good_document = document(matching.shard as u16, 2_154);
    let bad_event_id = Ulid::from_parts(2_155, 1);
    let good_event_id = Ulid::from_parts(2_156, 1);

    let mut bad_record = registry_record(
        group_id,
        bad_document,
        "datasets/mismatched",
        100,
        bad_event_id,
    );
    bad_record.placement = mismatched;
    let mut bad_create = metadata_create_event(group_id, bad_document, 100, bad_event_id, 75);
    bad_create.record = bad_record.clone();
    let mut good_record = registry_record(
        group_id,
        good_document,
        "datasets/placed",
        100,
        good_event_id,
    );
    good_record.placement = matching;
    let mut good_create = metadata_create_event(group_id, good_document, 100, good_event_id, 75);
    good_create.record = good_record.clone();

    let registry_target = |document_id| DocumentSyncTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    let create_target = |document_id, event_id| DocumentSyncTarget::MetadataCreateEvent {
        document_id,
        event_id,
    };
    let bad_topic = registry_target(bad_document).sync_topic_id(realm_id, &mismatched);
    let good_topic = registry_target(good_document).sync_topic_id(realm_id, &matching);
    service
        .ensure_sync_topics(&[bad_topic, good_topic], Vec::new())
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
    let bad_registry_event = Ulid::from_parts(2_157, 1);
    let good_registry_event = Ulid::from_parts(2_158, 1);
    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::Upsert {
                    event_id: bad_registry_event,
                    target: registry_target(bad_document),
                    bytes: postcard::to_allocvec(&bad_record).expect("registry serializes"),
                    change: change(bad_registry_event, mismatched),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: bad_event_id,
                    target: create_target(bad_document, bad_event_id),
                    bytes: postcard::to_allocvec(&bad_create).expect("create serializes"),
                    change: change(bad_event_id, mismatched),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: good_registry_event,
                    target: registry_target(good_document),
                    bytes: postcard::to_allocvec(&good_record).expect("registry serializes"),
                    change: change(good_registry_event, matching),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: good_event_id,
                    target: create_target(good_document, good_event_id),
                    bytes: postcard::to_allocvec(&good_create).expect("create serializes"),
                    change: change(good_event_id, matching),
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

    reset_test_cursor(&service, bad_topic).await;
    reset_test_cursor(&service, good_topic).await;
    let applied = service
        .reconcile_document_topics([bad_topic, good_topic])
        .await
        .expect("mismatched placements are quarantined");

    assert!(applied.targets.contains(&registry_target(good_document)));
    assert!(
        applied
            .targets
            .contains(&create_target(good_document, good_event_id))
    );
    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 2, "{records:?}");
    assert_eq!(
        quarantined_reason(&records, bad_registry_event),
        "metadata registry record has a mismatched placement configuration"
    );
    assert_eq!(
        quarantined_reason(&records, bad_event_id),
        "replicated metadata create has a mismatched placement configuration"
    );
    // The create-batch reject rides that function's own transaction.
    assert!(cursor_advanced(&service, &storage, bad_topic).await);
    assert!(cursor_advanced(&service, &storage, good_topic).await);
    assert_eq!(quarantine_usage(&storage).await.records, 2);

    service.shutdown().await;
}

/// `event_id` is payload-controlled, so it may repeat across publishers,
/// operations, and batches. Rows and usage follow transport identity: one
/// row per identity, counted exactly once however often it is rejected.
#[tokio::test]
async fn quarantine_batch_accounting() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([79; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(79).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let topic_id = topic(79);
    let shared_event_id = Ulid::from_parts(3_300, 1);
    let event = || DocumentSyncEvent::Upsert {
        event_id: shared_event_id,
        target: DocumentSyncTarget::RealmConfig { realm_id },
        bytes: vec![3; 8],
        change: DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: shared_event_id,
                actor: node(79),
                updated_at_ms: 1,
            },
            kind: DocumentSyncChangeKind::Upsert,
            placement: PlacementRef::NIL,
        },
    };
    let identity = |actor: u8, actor_seq: u64| SyncQuarantineIdentity {
        topic: topic_id,
        actor: ::irokle::ActorId::from_bytes([actor; 32]),
        actor_seq,
    };
    let commit = |rejections: Vec<SyncRejection>| {
        let service = service.clone();
        async move {
            let txn_id = start_storage_transaction(&service.storage)
                .await
                .expect("transaction starts");
            let entries = service
                .quarantine_entries(&rejections, txn_id)
                .await
                .expect("evidence builds")
                .expect("capacity is available");
            replace_batch_in(&service.storage, txn_id, Vec::new(), entries)
                .await
                .expect("evidence commits");
        }
    };

    // One batch: one publisher's two operations, a second publisher reusing
    // the same event id, and a repeat of the first key.
    commit(vec![
        SyncRejection::new(identity(1, 1), event(), "first"),
        SyncRejection::new(identity(1, 2), event(), "second"),
        SyncRejection::new(identity(2, 1), event(), "other publisher"),
        SyncRejection::new(identity(1, 1), event(), "repeat in batch"),
    ])
    .await;
    let stored_bytes = |records: &[SyncQuarantineRecord]| {
        records
            .iter()
            .map(|record| record.to_bytes().expect("record serializes").len() as u64)
            .sum::<u64>()
    };
    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 3, "{records:?}");
    let usage = quarantine_usage(&storage).await;
    assert_eq!(usage.records, 3);
    assert_eq!(usage.bytes, stored_bytes(&records));
    assert_eq!(
        records
            .iter()
            .find(|record| record.identity == identity(1, 1))
            .expect("first identity")
            .reason,
        "repeat in batch",
        "the batch's last write for a key is the stored one"
    );

    // Several batches: the same three identities redeliver, and a fourth
    // operation reusing the same event id is the only new row.
    commit(vec![
        SyncRejection::new(identity(1, 1), event(), "redelivered"),
        SyncRejection::new(identity(2, 1), event(), "redelivered"),
    ])
    .await;
    commit(vec![
        SyncRejection::new(identity(1, 2), event(), "redelivered"),
        SyncRejection::new(identity(2, 2), event(), "new operation"),
    ])
    .await;
    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 4, "{records:?}");
    let usage = quarantine_usage(&storage).await;
    assert_eq!(usage.records, 4);
    assert_eq!(usage.bytes, stored_bytes(&records));

    service.shutdown().await;
}
