use super::*;

/// Only a correctly placed, structurally consistent, publisher-bound mapping
/// reaches the mapping row or the shard manifest: a holder of one shard may
/// not stamp a document that decodes to another, even when both shards share
/// this holder.
#[tokio::test]
async fn pid_placement_fence() {
    use aruna_core::keyspaces::{PERSISTENT_ID_MAPPING_KEYSPACE, SHARD_MANIFEST_KEYSPACE};
    use aruna_core::storage_entries::shard_manifest_key;
    use aruna_core::structs::PersistentIdRevision;

    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([76; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(76).await,
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
    let minted_by = UserId::local(Ulid::from_parts(3_100, 1), realm_id);
    let actor = test_actor(76, minted_by, realm_id);
    assert_eq!(actor.node_id, local_node);

    let strategy_id = Ulid::from_parts(3_101, 1);
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

    let document = |bucket: u16, seed: u64| {
        MetaResourceId::from_parts(seed, handle, BucketId::new(bucket).unwrap(), 1)
            .unwrap()
            .as_ulid()
    };
    let placed = |shard: u32| PlacementRef { strategy_id, shard };
    let mapping = |document_id, actor, seed: u64| {
        let revision = PersistentIdRevision {
            event_id: Ulid::from_parts(seed, 1),
            actor,
            occurred_at_ms: 100 + seed,
        };
        let mut mapping = PersistentIdMapping::requested(
            document_id,
            false,
            minted_by,
            JobId::from_bytes([7; 16]),
            true,
            "/documents/test".to_string(),
            revision,
        );
        assert!(mapping.activate(minted_by, revision));
        mapping
    };

    let valid_document = document(4, 3_110);
    let forged_document = document(4, 3_111);
    let uncanonical_document = document(6, 3_112);
    let forged_actor_document = document(6, 3_113);
    let valid = mapping(valid_document, local_node, 3_120);
    let forged = mapping(forged_document, local_node, 3_121);
    let mut uncanonical = mapping(uncanonical_document, local_node, 3_122);
    uncanonical.pid = "https://w3id.org/aruna/not-this-document".to_string();
    let forged_actor = mapping(forged_actor_document, node(77), 3_123);

    let valid_topic = persistent_id_target(valid_document).sync_topic_id(realm_id, &placed(4));
    let forged_topic = persistent_id_target(forged_document).sync_topic_id(realm_id, &placed(9));
    let other_topic =
        persistent_id_target(uncanonical_document).sync_topic_id(realm_id, &placed(6));
    service
        .ensure_document_sync_topics(&[valid_topic, forged_topic, other_topic], Vec::new())
        .expect("mapping shard topic genesis");

    let publish =
        |mapping: &PersistentIdMapping, placement, event_id: u64| DocumentSyncPublish::Upsert {
            event_id: Ulid::from_parts(event_id, 1),
            target: persistent_id_target(mapping.target),
            bytes: mapping.to_bytes().expect("mapping serializes"),
            change: persistent_id_change(mapping, placement),
            allow_genesis: true,
        };
    let published = service
        .publish_documents(
            vec![
                publish(&valid, placed(4), 3_130),
                publish(&forged, placed(9), 3_131),
                publish(&uncanonical, placed(6), 3_132),
                publish(&forged_actor, placed(6), 3_133),
            ],
            Vec::new(),
        )
        .await;
    assert!(
        matches!(published, DocumentSyncNetEvent::DocumentsPublished { .. }),
        "mapping publish failed: {published:?}"
    );

    for topic_id in [valid_topic, forged_topic, other_topic] {
        reset_test_cursor(&service, topic_id).await;
    }
    let applied = service
        .reconcile_document_topics([valid_topic, forged_topic, other_topic])
        .await
        .expect("forged mappings are quarantined");

    assert_eq!(
        applied.targets,
        vec![persistent_id_target(valid_document)],
        "only the valid mapping applies"
    );
    assert!(
        read_storage_value(
            &storage,
            PERSISTENT_ID_MAPPING_KEYSPACE,
            ByteView::from(persistent_id_key(valid_document)),
        )
        .await
        .is_some()
    );
    assert!(
        read_storage_value(
            &storage,
            SHARD_MANIFEST_KEYSPACE,
            shard_manifest_key(&placed(4), &persistent_id_target(valid_document)),
        )
        .await
        .is_some()
    );
    for (document_id, placement) in [
        (forged_document, placed(9)),
        (uncanonical_document, placed(6)),
        (forged_actor_document, placed(6)),
    ] {
        assert!(
            read_storage_value(
                &storage,
                PERSISTENT_ID_MAPPING_KEYSPACE,
                ByteView::from(persistent_id_key(document_id)),
            )
            .await
            .is_none(),
            "rejected mapping {document_id} reached storage"
        );
        assert!(
            read_storage_value(
                &storage,
                SHARD_MANIFEST_KEYSPACE,
                shard_manifest_key(&placement, &persistent_id_target(document_id)),
            )
            .await
            .is_none(),
            "rejected mapping {document_id} reached the shard manifest"
        );
    }

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 3, "{records:?}");
    assert_eq!(
        quarantined_reason(&records, Ulid::from_parts(3_131, 1)),
        "persistent id mapping has a mismatched placement configuration"
    );
    assert_eq!(
        quarantined_reason(&records, Ulid::from_parts(3_132, 1)),
        "invalid persistent id mapping: mapping pid \
         `https://w3id.org/aruna/not-this-document` is not canonical"
    );
    assert_eq!(
        quarantined_reason(&records, Ulid::from_parts(3_133, 1)),
        "persistent id mapping revision actor is not its publisher"
    );
    for topic_id in [valid_topic, forged_topic, other_topic] {
        assert!(cursor_advanced(&service, &storage, topic_id).await);
    }

    service.shutdown().await;
}
