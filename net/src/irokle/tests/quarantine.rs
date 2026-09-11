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
