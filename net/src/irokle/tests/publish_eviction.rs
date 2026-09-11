use super::*;

#[tokio::test]
async fn missing_topic_publish_requires_allow_genesis() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([61u8; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(61).await,
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
    let target = DocumentSyncTarget::NodeInfo {
        realm_id,
        node_id: local_node,
    };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let change = DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::generate(),
            actor: local_node,
            updated_at_ms: 1,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement: aruna_core::structs::PlacementRef::NIL,
    };

    let blocked = service
        .publish_documents(
            vec![DocumentSyncPublish::Upsert {
                event_id: Ulid::generate(),
                target: target.clone(),
                bytes: b"blocked".to_vec(),
                change,
                allow_genesis: false,
            }],
            Vec::new(),
        )
        .await;
    assert!(
        matches!(blocked, DocumentSyncNetEvent::Error { .. }),
        "non-origin publish must fail retryably: {blocked:?}"
    );
    assert!(
        !service.has_topic(topic_id).expect("topic lookup"),
        "no genesis may be minted without allow_genesis"
    );

    let published = service
        .publish_documents(
            vec![DocumentSyncPublish::Upsert {
                event_id: Ulid::generate(),
                target: target.clone(),
                bytes: b"origin".to_vec(),
                change,
                allow_genesis: true,
            }],
            Vec::new(),
        )
        .await;
    assert!(
        matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
        "origin publish must succeed: {published:?}"
    );
    assert!(
        service.has_topic(topic_id).expect("topic lookup"),
        "origin publish must create the topic genesis"
    );
}

#[tokio::test]
async fn topic_not_ready_publish_does_not_block_ready_batch_record() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([62; 32]);
    let placement = aruna_core::structs::PlacementRef::NIL;
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(62).await,
        storage,
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let local_node = service.local_node_id().expect("local node id");
    let blocked_target = DocumentSyncTarget::NodeUsage {
        realm_id,
        node_id: local_node,
        group_id: None,
    };
    let ready_target = DocumentSyncTarget::WatchInterest {
        realm_id,
        node_id: local_node,
    };
    let blocked_topic = blocked_target.sync_topic_id(realm_id, &placement);
    let ready_topic = ready_target.sync_topic_id(realm_id, &placement);
    let change = DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_parts(62, 3),
            actor: local_node,
            updated_at_ms: 1,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement,
    };

    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::Upsert {
                    event_id: Ulid::from_parts(62, 4),
                    target: blocked_target.clone(),
                    bytes: b"blocked".to_vec(),
                    change,
                    allow_genesis: false,
                },
                DocumentSyncPublish::Upsert {
                    event_id: Ulid::from_parts(62, 5),
                    target: ready_target.clone(),
                    bytes: b"ready".to_vec(),
                    change,
                    allow_genesis: true,
                },
            ],
            Vec::new(),
        )
        .await;

    match published {
        DocumentSyncNetEvent::DocumentsPartiallyPublished {
            published_indices,
            retry_indices,
            error,
        } => {
            assert_eq!(published_indices, vec![1]);
            assert_eq!(retry_indices, vec![0]);
            assert!(error.contains(&blocked_topic.to_string()));
        }
        other => panic!("expected partial publish, got {other:?}"),
    }
    assert!(
        !service
            .has_topic(blocked_topic)
            .expect("blocked topic lookup"),
        "not-ready record must not mint genesis"
    );
    assert!(
        service.has_topic(ready_topic).expect("ready topic lookup"),
        "ready record behind not-ready record must publish"
    );
}

// Two services fork one admin topic (each mints its own genesis carrying a
// unique admin event). The genesis tie-break resets exactly the losing side,
// whose admin event is evicted and decodes back into a re-emittable outbox
// publish that preserves the original embedded event id (the applier dedup
// key) and refuses to mint a rival genesis.
