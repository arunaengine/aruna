//! Tests publish genesis rules and that an evicted event replays after a lost tie-break.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

#[tokio::test]
async fn missing_requires_genesis() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([61u8; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(61).await,
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
    let target = DocumentTarget::NodeInfo {
        realm_id,
        node_id: local_node,
    };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let change = DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::generate(),
            actor: local_node,
            updated_at_ms: 1,
        },
        kind: DocumentChangeKind::Upsert,
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
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
        matches!(blocked, DocumentNetEvent::Error { .. }),
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
        matches!(published, DocumentNetEvent::DocumentsPublished { .. }),
        "origin publish must succeed: {published:?}"
    );
    assert!(
        service.has_topic(topic_id).expect("topic lookup"),
        "origin publish must create the topic genesis"
    );
}

#[tokio::test]
async fn blocked_allows_ready() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([62; 32]);
    let placement = aruna_core::structs::placement::record::PlacementRef::NIL;
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(62).await,
        storage,
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let local_node = service.local_node_id().expect("local node id");
    let blocked_target = DocumentTarget::NodeUsage {
        realm_id,
        node_id: local_node,
        group_id: None,
    };
    let ready_target = DocumentTarget::WatchInterest {
        realm_id,
        node_id: local_node,
    };
    let blocked_topic = blocked_target.sync_topic_id(realm_id, &placement);
    let ready_topic = ready_target.sync_topic_id(realm_id, &placement);
    let change = DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::from_parts(62, 3),
            actor: local_node,
            updated_at_ms: 1,
        },
        kind: DocumentChangeKind::Upsert,
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
        DocumentNetEvent::DocumentsPartiallyPublished {
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

#[tokio::test]
async fn lost_eviction_replays() {
    let (_dir_a, storage_a) = test_storage();
    let (_dir_b, storage_b) = test_storage();
    let doc_a = tempfile::tempdir().expect("doc a");
    let doc_b = tempfile::tempdir().expect("doc b");
    let realm_id = RealmId::from_bytes([79; 32]);
    let service_a = DocumentSyncService::open_with_policy(
        test_endpoint(79).await,
        storage_a,
        doc_a.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("service a opens");
    let service_b = DocumentSyncService::open_with_policy(
        test_endpoint(80).await,
        storage_b.clone(),
        doc_b.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("service b opens");

    let node_a = service_a.local_node_id().expect("node a id");
    let node_b = service_b.local_node_id().expect("node b id");
    let user_id = UserId::local(Ulid::from_parts(79, 1), realm_id);
    let target = DocumentTarget::User { user_id };
    let admin_target = AdminDocumentTarget::User { user_id };
    let placement = PlacementRef {
        strategy_id: Ulid::from_parts(79, 7),
        shard: 1,
    };
    let topic_id = target.sync_topic_id(realm_id, &placement);

    service_a
        .ensure_sync_topics(&[topic_id], vec![node_b])
        .expect("service a topic genesis");
    service_b
        .ensure_sync_topics(&[topic_id], vec![node_a])
        .expect("service b topic genesis");
    for (service, seed, name) in [(&service_a, 1u8, "from-a"), (&service_b, 2u8, "from-b")] {
        let event = test_admin_event(
            Ulid::from_parts(79, seed as u128),
            admin_target.clone(),
            &test_actor(seed, user_id, realm_id),
            1,
            AdminDocumentOperation::UserNameSet {
                name: name.to_string(),
            },
        );
        // Neither service is the origin here, so each relays the origin's
        // own signature instead of substituting its own.
        let origin_signature = sign_as_origin(&event, &placement);
        assert!(matches!(
            service
                .publish_documents(
                    vec![DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(event),
                        placement,
                        allow_genesis: true,
                        origin_signature: Some(origin_signature),
                    }],
                    Vec::new(),
                )
                .await,
            DocumentNetEvent::DocumentsPublished { .. }
        ));
    }

    let node_a_handle = service_a.node();
    let node_b_handle = service_b.node();
    let genesis = |handle: &::irokle::Irokle<::irokle::FjallStorage>| {
        handle
            .storage()
            .topic_state(&topic_id)
            .expect("topic state")
            .expect("topic exists")
            .genesis
    };
    assert_ne!(genesis(&node_a_handle), genesis(&node_b_handle));
    let (loser, loser_node, winner_node, loser_peer, winner_peer) =
        if genesis(&node_a_handle) > genesis(&node_b_handle) {
            (
                &service_a,
                &node_a_handle,
                &node_b_handle,
                node_to_peer(&node_a),
                node_to_peer(&node_b),
            )
        } else {
            (
                &service_b,
                &node_b_handle,
                &node_a_handle,
                node_to_peer(&node_b),
                node_to_peer(&node_a),
            )
        };
    let loser_actor = ::irokle::actor_id_for(topic_id, loser_peer);
    let stale = applied_cursor_clock(
        loser_node.storage(),
        topic_id,
        genesis(loser_node),
        loser
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), topic_cursor_key(topic_id))
            .await
            .expect("cursor read"),
    )
    .expect("cursor decodes");
    assert!(
        stale.get(&loser_actor) > 0,
        "the loser's publish must leave a cursor covering its own chain"
    );

    let winner_ops =
        ::irokle::oplog::topological(winner_node.storage(), &topic_id).expect("winner ops");
    let (_ack, evictions) = loser_node
        .receive_sync_data_from_evicting(
            winner_peer,
            SyncData {
                topic_id,
                ops: winner_ops,
            },
        )
        .expect("loser admits the winning chain");
    assert_eq!(evictions.len(), 1);
    // `decode_eviction` is `consume_eviction` without the cursor delete:
    // exactly the state a crash or a failed delete leaves behind.
    loser.decode_eviction(evictions.into_iter().next().expect("one eviction"));
    let winning_genesis = genesis(loser_node);
    assert!(
        loser
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), topic_cursor_key(topic_id),)
            .await
            .expect("cursor read")
            .is_some(),
        "this test must leave the stale cursor in place"
    );

    loser
        .reconcile_document_topics([topic_id])
        .await
        .expect("the winning chain reconciles");
    let stored: AppliedCursor = postcard::from_bytes(
        &loser
            .storage_read(APPLIED_OPS_KEYSPACE.to_string(), topic_cursor_key(topic_id))
            .await
            .expect("cursor read")
            .expect("reconcile rewrites the cursor"),
    )
    .expect("cursor decodes");
    assert_eq!(stored.lineage, winning_genesis);
    assert_eq!(
        stored.clock.get(&loser_actor),
        0,
        "the replaced chain's sequences must not mask the loser's re-emissions"
    );

    // A reconcile that changes no lineage must not replay the topic again.
    assert!(
        loser
            .reconcile_document_topics([topic_id])
            .await
            .expect("second reconcile")
            .targets
            .is_empty(),
        "an unchanged genesis must keep its cursor instead of reapplying"
    );
}

/// Opens a document sync service of node `seed` on `doc_dir` with fresh node storage.
async fn open_at(
    seed: u8,
    doc_dir: &std::path::Path,
    realm_id: RealmId,
) -> (tempfile::TempDir, DocumentSyncService) {
    let (dir, storage) = test_storage();
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(seed).await,
        storage,
        doc_dir.join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("service opens");
    (dir, service)
}

#[tokio::test]
async fn eviction_survives_restart() {
    // An eviction whose hand-off never committed is still journalled after a restart, with the
    // same documents and event ids; once released, a later restart replays nothing.
    let realm_id = RealmId::from_bytes([81; 32]);
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let (_storage_a, service_a) = open_at(81, dirs[0].path(), realm_id).await;
    let (_storage_b, service_b) = open_at(82, dirs[1].path(), realm_id).await;
    let services = [service_a, service_b];
    let nodes = services
        .each_ref()
        .map(|service| service.local_node_id().unwrap());
    let user_id = UserId::local(Ulid::from_parts(81, 1), realm_id);
    let target = DocumentTarget::User { user_id };
    let placement = PlacementRef {
        strategy_id: Ulid::from_parts(81, 7),
        shard: 1,
    };
    let topic_id = target.sync_topic_id(realm_id, &placement);
    for (index, service) in services.iter().enumerate() {
        service
            .ensure_sync_topics(&[topic_id], vec![nodes[1 - index]])
            .expect("topic genesis");
        let seed = index as u8 + 1;
        let event = test_admin_event(
            Ulid::from_parts(81, u128::from(seed)),
            AdminDocumentTarget::User { user_id },
            &test_actor(seed, user_id, realm_id),
            1,
            AdminDocumentOperation::UserNameSet {
                name: format!("name-{seed}"),
            },
        );
        let origin_signature = sign_as_origin(&event, &placement);
        let published = service
            .publish_documents(
                vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(event),
                    placement,
                    allow_genesis: true,
                    origin_signature: Some(origin_signature),
                }],
                Vec::new(),
            )
            .await;
        assert!(matches!(
            published,
            DocumentNetEvent::DocumentsPublished { .. }
        ));
    }
    let genesis = |service: &DocumentSyncService| {
        let state = service.node().storage().topic_state(&topic_id).unwrap();
        state.expect("topic exists").genesis
    };
    let loser = usize::from(genesis(&services[0]) < genesis(&services[1]));
    let winner = 1 - loser;
    let winner_ops =
        ::irokle::oplog::topological(services[winner].node().storage(), &topic_id).unwrap();
    let (_ack, evictions) = services[loser]
        .node()
        .receive_sync_data_from_evicting(
            node_to_peer(&nodes[winner]),
            SyncData {
                topic_id,
                ops: winner_ops,
            },
        )
        .expect("the loser admits the winning chain");
    let eviction = evictions.into_iter().next().expect("one eviction");
    let entry = services[loser]
        .consume_eviction(eviction)
        .await
        .expect("the eviction is journalled");
    let [first, second] = services;
    for service in [first, second] {
        service.shutdown().await;
    }

    let seed = 81 + loser as u8;
    let (_storage, restarted) = open_at(seed, dirs[loser].path(), realm_id).await;
    let pending = restarted.pending_evictions().await.expect("journal reads");
    assert_eq!(pending, vec![entry.clone()]);
    assert!(restarted.eviction_pending(&placement));
    restarted
        .clear_eviction(entry.key)
        .await
        .expect("entry releases");
    restarted.shutdown().await;
    drop(restarted);

    let (_storage, released) = open_at(seed, dirs[loser].path(), realm_id).await;
    let pending = released.pending_evictions().await.expect("journal reads");
    assert!(pending.is_empty(), "a released entry must not replay");
    released.shutdown().await;
}
