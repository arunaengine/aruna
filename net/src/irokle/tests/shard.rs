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

#[tokio::test]
async fn capacity_holds_cursor() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([73u8; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(73).await,
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
    let published = service
        .publish_documents(
            vec![DocumentSyncPublish::Upsert {
                event_id: Ulid::generate(),
                target: target.clone(),
                bytes: node_info_bytes(node(74), 5),
                change: DocumentSyncChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: 1,
                        event_id: Ulid::generate(),
                        actor: local_node,
                        updated_at_ms: 1,
                    },
                    kind: DocumentSyncChangeKind::Upsert,
                    placement: PlacementRef::NIL,
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

    // A full store fails the write closed: no evidence, no cursor movement.
    let full = SyncQuarantineUsage {
        records: SYNC_QUARANTINE_MAX_RECORDS,
        bytes: 0,
    };
    write_usage(&storage, full).await;
    reset_test_cursor(&service, topic_id).await;
    service
        .reconcile_document_topics([topic_id])
        .await
        .expect("a full quarantine store is not a reconcile failure");
    assert!(quarantine_rows(&storage).await.is_empty());
    assert_eq!(quarantine_usage(&storage).await, full);
    assert!(!cursor_advanced(&service, &storage, topic_id).await);

    // Reclaimed capacity lets the redelivered event persist and advance.
    write_usage(&storage, SyncQuarantineUsage::default()).await;
    service
        .reconcile_document_topics([topic_id])
        .await
        .expect("the redelivered event is quarantined");
    assert_eq!(quarantine_rows(&storage).await.len(), 1);
    assert_eq!(quarantine_usage(&storage).await.records, 1);
    assert!(cursor_advanced(&service, &storage, topic_id).await);

    service.shutdown().await;
}

#[tokio::test]
async fn shard_membership_exact() {
    let (_dir, storage) = test_storage();
    let doc = tempfile::tempdir().expect("document sync dir");
    let realm_id = restart_realm();
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(80).await,
        storage,
        doc.path().join("document-sync"),
        &[node(81), node(82)],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");
    let local_node = service.local_node_id().expect("local node id");
    let current_node = node(81);
    let stale_node = node(82);
    let shard_topic = restart_topic();
    let shared_topic =
        DocumentSyncTarget::RealmConfig { realm_id }.sync_topic_id(realm_id, &PlacementRef::NIL);

    service
        .ensure_document_sync_topics(&[shard_topic], vec![current_node, stale_node])
        .expect("shard topic exists");
    service
        .ensure_document_sync_topics(&[shared_topic], vec![stale_node])
        .expect("shared topic exists");

    service
        .reconcile_shard_membership(
            &[shard_topic],
            vec![local_node, current_node],
            vec![local_node, current_node],
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .await
        .expect("shard membership reconciles");

    let shard_state = service
        .node()
        .storage()
        .topic_state(&shard_topic)
        .expect("shard state reads")
        .expect("shard state exists");
    assert!(
        shard_state
            .members
            .contains(&node_id_to_peer_id(&local_node))
    );
    assert!(
        shard_state
            .members
            .contains(&node_id_to_peer_id(&current_node))
    );
    assert!(
        !shard_state
            .members
            .contains(&node_id_to_peer_id(&stale_node))
    );

    let shared_state = service
        .node()
        .storage()
        .topic_state(&shared_topic)
        .expect("shared state reads")
        .expect("shared state exists");
    assert!(
        shared_state
            .members
            .contains(&node_id_to_peer_id(&stale_node)),
        "exact shard reconciliation must not alter shared topics"
    );
    assert!(
        service
            .default_peers
            .read()
            .contains(&node_id_to_peer_id(&stale_node)),
        "former shard holders remain available as default network peers"
    );

    service.shutdown().await;
}

#[tokio::test]
async fn document_events_after_keeps_unapplied_dependency_of_covered_head() {
    use irokle_crate::{Ed25519Signer, Signer as _};

    let root = tempfile::tempdir().expect("temp dir");
    let service = open_restart_service(root.path(), "causal-cursor-storage").await;
    let local_node = service.local_node_id().expect("local node id");
    let remote_node = node(88);
    let topic_id = restart_topic();
    service
        .ensure_document_sync_topics(&[topic_id], vec![remote_node])
        .expect("shard topic exists");
    let oplog = Oplog::with_storage(service.node().storage().clone());
    let remote_signer = Ed25519Signer::from_bytes(&[88; 32]);
    let remote_event_id = Ulid::from_parts(1_727_000_000_000, 43);
    let local_event_id = Ulid::from_parts(1_727_000_000_000, 44);
    let change = |event_id, actor| DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id,
            actor,
            updated_at_ms: 1_727_000_000_101,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement: restart_placement(),
    };
    let publish = |event_id, actor| DocumentSyncEvent::Upsert {
        event_id,
        target: restart_target(),
        bytes: restart_payload(),
        change: change(event_id, actor),
    };
    let remote_actor = irokle_crate::actor_id_for(topic_id, remote_signer.peer_id());
    oplog
        .create_event_op(
            topic_id,
            remote_actor,
            EventEnvelope::encode_event(&publish(remote_event_id, remote_node))
                .expect("remote event encodes"),
            &remote_signer,
        )
        .expect("remote event publishes");
    let local_op = oplog
        .create_event_op(
            topic_id,
            irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&local_node)),
            EventEnvelope::encode_event(&publish(local_event_id, local_node))
                .expect("local event encodes"),
            service.node().signer(),
        )
        .expect("local event publishes above remote head");
    let mut cursor = irokle_crate::ActorClock::default();
    cursor.observe(
        local_op.signed.body.actor_id,
        local_op.signed.body.actor_seq,
    );

    let events = service
        .document_events_after(topic_id, &cursor)
        .expect("unapplied document events read");
    let event_ids = events
        .into_iter()
        .filter_map(|(event, _, _)| match event {
            DocumentSyncEvent::Upsert { event_id, .. } => Some(event_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(event_ids, vec![remote_event_id]);

    service.shutdown().await;
}

#[tokio::test]
async fn replay_backlog() {
    let root = tempfile::tempdir().expect("temp dir");
    let service = open_restart_service(root.path(), "replay-batch-storage").await;
    let topic_id = restart_topic();
    let target = restart_target();
    service
        .ensure_document_sync_topics(&[topic_id], Vec::new())
        .expect("shard topic exists");

    let documents = (0..(DOCUMENT_SYNC_REPLAY_BATCH_LIMIT + 1))
        .map(|index| {
            let event_id = Ulid::from_parts(1_800_000_000_000 + index as u64, 1);
            DocumentSyncPublish::Upsert {
                event_id,
                target: target.clone(),
                bytes: restart_payload(),
                change: DocumentSyncChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: 1,
                        event_id,
                        actor: service.local_node_id().expect("local node id"),
                        updated_at_ms: 1_800_000_000_000 + index as u64,
                    },
                    kind: DocumentSyncChangeKind::Upsert,
                    placement: restart_placement(),
                },
                allow_genesis: false,
            }
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        service.publish_documents(documents, Vec::new()).await,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));

    let cursor = irokle_crate::ActorClock::default();
    service
        .storage_write(
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
            topic_cursor_key(topic_id),
            postcard::to_allocvec(&cursor)
                .expect("cursor serializes")
                .into(),
        )
        .await
        .expect("cursor resets");

    let first = service
        .document_event_batch(topic_id, &cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)
        .expect("first replay batch");
    assert_eq!(
        first.events.len(),
        DOCUMENT_SYNC_REPLAY_BATCH_LIMIT - 1,
        "genesis consumes one bounded replay slot"
    );
    let actor = irokle_crate::actor_id_for(topic_id, service.node().peer_id());
    assert_eq!(
        first.cursor.get(&actor),
        DOCUMENT_SYNC_REPLAY_BATCH_LIMIT as u64
    );
    let topic_clock = service
        .node()
        .storage()
        .actor_clock(&topic_id)
        .expect("topic clock");
    assert!(!first.cursor.dominates(&topic_clock));

    // An interrupted run before the cursor write retries the same batch.
    let retry = service
        .document_event_batch(topic_id, &cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)
        .expect("retry replay batch");
    assert_eq!(retry.cursor, first.cursor);
    let remaining = service
        .document_event_batch(topic_id, &first.cursor, DOCUMENT_SYNC_FRAME_LEN_LIMIT)
        .expect("remaining replay batch");
    assert!(remaining.cursor.dominates(&topic_clock));

    let byte_first = service
        .document_event_batch(topic_id, &cursor, 0)
        .expect("single operation byte batch");
    assert_eq!(byte_first.cursor.get(&actor), 1);
    let byte_second = service
        .document_event_batch(topic_id, &byte_first.cursor, 0)
        .expect("second operation byte batch");
    assert_eq!(byte_second.cursor.get(&actor), 2);
    assert!(!byte_second.cursor.dominates(&topic_clock));

    service.shutdown().await;
}

#[tokio::test]
async fn stale_publisher_rejected() {
    let (_receiver_dir, receiver_storage) = test_storage();
    let (_publisher_dir, publisher_storage) = test_storage();
    let receiver_doc = tempfile::tempdir().expect("receiver document sync dir");
    let publisher_doc = tempfile::tempdir().expect("publisher document sync dir");
    let realm_id = restart_realm();
    let receiver = DocumentSyncService::open_with_persist_policy(
        test_endpoint(83).await,
        receiver_storage.clone(),
        receiver_doc.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("receiver opens");
    let publisher = DocumentSyncService::open_with_persist_policy(
        test_endpoint(84).await,
        publisher_storage,
        publisher_doc.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("publisher opens");
    let receiver_node = receiver.local_node_id().expect("receiver node id");
    let publisher_node = publisher.local_node_id().expect("publisher node id");
    let topic_id = restart_topic();

    receiver
        .ensure_document_sync_topics(&[topic_id], vec![publisher_node])
        .expect("receiver creates shard topic");
    let receiver_ops = irokle_crate::oplog::topological(receiver.node().storage(), &topic_id)
        .expect("receiver history reads");
    publisher
        .node()
        .receive_sync_data_from_evicting(
            node_id_to_peer_id(&receiver_node),
            SyncData {
                topic_id,
                ops: receiver_ops,
            },
        )
        .expect("publisher adopts shard genesis");

    let published = publisher
        .publish_documents(
            vec![DocumentSyncPublish::Upsert {
                event_id: restart_event_id(),
                target: restart_target(),
                bytes: restart_payload(),
                change: revision_change(),
                allow_genesis: false,
            }],
            vec![receiver_node],
        )
        .await;
    assert!(matches!(
        published,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));
    let publisher_ops = irokle_crate::oplog::topological(publisher.node().storage(), &topic_id)
        .expect("publisher history reads");
    receiver
        .reconcile_shard_membership(
            &[topic_id],
            vec![receiver_node],
            vec![receiver_node],
            &BTreeSet::new(),
            &BTreeSet::from([topic_id]),
        )
        .await
        .expect("former holder is removed");
    receiver
        .node()
        .receive_sync_data_from_evicting(
            node_id_to_peer_id(&publisher_node),
            SyncData {
                topic_id,
                ops: publisher_ops,
            },
        )
        .expect("receiver admits the publisher's pre-removal causal history");
    let result = receiver
        .reconcile_document_topics([topic_id])
        .await
        .expect("stale publisher event is skipped");
    assert!(
        result.targets.is_empty(),
        "a former holder's shard event must not apply"
    );
    assert!(
        read_storage_value(
            &receiver_storage,
            restart_target().storage_keyspace(),
            restart_target().storage_key(),
        )
        .await
        .is_none(),
        "rejected event must not mutate document storage"
    );

    let cursor = read_test_cursor(&receiver_storage, topic_id)
        .await
        .expect("cursor persisted");
    let topic_clock = receiver
        .node()
        .storage()
        .actor_clock(&topic_id)
        .expect("topic clock reads");
    assert!(
        cursor.dominates(&topic_clock),
        "rejected stale-holder event must not wedge reconciliation"
    );
    let retained_events = irokle_crate::oplog::topological(receiver.node().storage(), &topic_id)
        .expect("retained history reads")
        .into_iter()
        .filter(|op| matches!(&op.signed.body.payload, TopicPayload::Event(_)))
        .count();
    assert_eq!(
        retained_events, 1,
        "membership changes retain event history"
    );

    let records = quarantine_rows(&receiver_storage).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].reason,
        "shard publisher is outside the current holder set"
    );
    assert_eq!(records[0].target(), Some(&restart_target()));
    assert_eq!(
        records[0]
            .decoded_event()
            .expect("event decodes")
            .placement(),
        restart_placement()
    );
    assert_eq!(quarantine_usage(&receiver_storage).await.records, 1);

    publisher.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn cutoff_gated_by_verification() {
    // An unverified shard freezes no former-holder history cutoff; a verified
    // shard does. The local clock is only a trustworthy cutover boundary once
    // the shard is durably verified.
    let (_dir, storage) = test_storage();
    let doc = tempfile::tempdir().expect("document sync dir");
    let realm_id = restart_realm();
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(85).await,
        storage,
        doc.path().join("document-sync"),
        &[node(86)],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("service opens");
    let local_node = service.local_node_id().expect("local node id");
    let co_holder = node(86);
    let topic = restart_topic();
    service
        .ensure_document_sync_topics(&[topic], vec![co_holder])
        .expect("shard topic exists");

    service
        .reconcile_shard_membership(
            &[topic],
            vec![local_node, co_holder],
            vec![local_node, co_holder],
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .await
        .expect("membership reconciles");
    assert!(
        service
            .shard_publishers
            .read()
            .get(&topic)
            .expect("publisher policy installed")
            .history_cutoff
            .is_none(),
        "an unverified shard must not freeze a history cutoff"
    );

    service
        .reconcile_shard_membership(
            &[topic],
            vec![local_node, co_holder],
            vec![local_node, co_holder],
            &BTreeSet::new(),
            &BTreeSet::from([topic]),
        )
        .await
        .expect("membership reconciles");
    assert!(
        service
            .shard_publishers
            .read()
            .get(&topic)
            .expect("publisher policy installed")
            .history_cutoff
            .is_some(),
        "a verified shard must freeze a history cutoff"
    );

    service.shutdown().await;
}
