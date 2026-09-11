use super::*;

#[tokio::test]
async fn quarantine_keeps_placement() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([72u8; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(72).await,
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

    let placement = PlacementRef {
        strategy_id: Ulid::from_parts(7_200, 1),
        shard: 5,
    };
    let target = DocumentSyncTarget::PersistentIdMapping {
        document_id: Ulid::from_parts(7_201, 1),
    };
    let topic_id = target.sync_topic_id(realm_id, &placement);
    service
        .ensure_document_sync_topics(&[topic_id], Vec::new())
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
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(70).await,
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
