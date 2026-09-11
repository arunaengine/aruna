use super::*;

// The eager cursor delete is an optimization, not the invariant. A crash or
// a failed delete leaves the replaced chain's cursor behind, so reconcile
// must detect the lineage change itself and replay the winner from one.
#[tokio::test]
async fn applied_cursor_lineage() {
    // A cursor is trusted only while it still describes the topic's current
    // history: another genesis, a position rebuilt under the same genesis
    // (an orphan quarantine), and any unreadable value all restart replay.
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([81; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(81).await,
        storage,
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let local_actor = test_actor(81, UserId::nil(realm_id), realm_id);
    let event = test_admin_event(
        Ulid::from_parts(1_810, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &local_actor,
        1,
        AdminDocumentOperation::RealmConfigDescriptionSet {
            description: "cursor lineage".to_string(),
        },
    );
    assert!(matches!(
        service
            .publish_documents(
                vec![DocumentSyncPublish::AdminOperation {
                    target,
                    event: Box::new(event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                    origin_signature: None,
                }],
                Vec::new(),
            )
            .await,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));

    let node = service.node();
    let store = node.storage();
    let genesis = service
        .topic_genesis(topic_id)
        .expect("topic genesis")
        .expect("topic exists");
    let clock = store.actor_clock(&topic_id).expect("topic clock");
    let encoded = applied_cursor_value(store, topic_id, genesis, &clock).expect("cursor encodes");
    let read = |value: Option<Value>, genesis| {
        applied_cursor_clock(store, topic_id, genesis, value).expect("cursor reads")
    };

    assert_eq!(read(Some(encoded.clone()), genesis), clock);
    assert_eq!(
        read(Some(encoded.clone()), test_genesis(2)),
        irokle_crate::ActorClock::default(),
        "another genesis is another history"
    );
    assert_eq!(
        read(
            Some(
                postcard::to_allocvec(&clock)
                    .expect("bare clock serializes")
                    .into()
            ),
            genesis
        ),
        irokle_crate::ActorClock::default(),
        "a cursor without a lineage is untrusted"
    );
    assert_eq!(
        read(None, genesis),
        irokle_crate::ActorClock::default(),
        "an absent cursor replays from one"
    );

    // An orphan quarantine keeps the genesis and rebuilds the chain, so the
    // recorded positions hold different ops than the cursor remembers.
    let mut rebuilt: AppliedCursor =
        postcard::from_bytes(encoded.as_ref()).expect("cursor decodes");
    for mark in rebuilt.marks.values_mut() {
        *mark = test_genesis(9);
    }
    assert_eq!(
        read(
            Some(
                postcard::to_allocvec(&rebuilt)
                    .expect("rebuilt cursor serializes")
                    .into()
            ),
            genesis
        ),
        irokle_crate::ActorClock::default(),
        "a rebuilt position must not count as applied"
    );

    service.shutdown().await;
}

// Whole-document admin sync is refused by apply_upsert/apply_delete. If reconcile
// `?`-propagated that refusal the applied-ops cursor would never advance, so each
// reconcile would re-materialize the whole post-cursor history. The upsert/delete
// must be skipped while the admin operation on the same topic still applies.
#[tokio::test]
async fn reconcile_skips_whole_document_admin_sync_events() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([54u8; 32]);
    let service = DocumentSyncService::open_with_persist_policy(
        test_endpoint(54).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let user_id = UserId::local(Ulid::from_parts(1_400, 1), realm_id);
    let target = DocumentSyncTarget::User { user_id };
    let placement = PlacementRef {
        strategy_id: Ulid::from_parts(54, 7),
        shard: 1,
    };
    let topic_id = target.sync_topic_id(realm_id, &placement);
    service
        .ensure_document_sync_topics(&[topic_id], Vec::new())
        .expect("admin shard topic genesis");

    let change = |kind| DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::generate(),
            actor: service.local_node_id().expect("local node id"),
            updated_at_ms: 1,
        },
        kind,
        placement,
    };
    let actor = test_actor(54, user_id, realm_id);
    assert_eq!(
        actor.node_id,
        service.local_node_id().expect("local node id")
    );
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentSyncTarget::RealmConfig { realm_id },
            config.to_bytes(&actor).expect("config serializes").into(),
        )],
    )
    .await
    .expect("config writes");
    let admin_event = test_admin_event(
        Ulid::from_parts(1_401, 1),
        AdminDocumentTarget::User { user_id },
        &actor,
        1,
        AdminDocumentOperation::UserNameSet {
            name: "Skip Survivor".to_string(),
        },
    );

    // Two hostile whole-document ops (upsert then delete) precede a legitimate
    // owner-authored admin operation on the same topic.
    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::Upsert {
                    event_id: Ulid::generate(),
                    target: target.clone(),
                    bytes: b"whole-document-admin-upsert".to_vec(),
                    change: change(DocumentSyncChangeKind::Upsert),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Delete {
                    event_id: Ulid::generate(),
                    target: target.clone(),
                    change: change(DocumentSyncChangeKind::Delete),
                    allow_genesis: true,
                },
                DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(admin_event),
                    placement,
                    allow_genesis: true,
                    origin_signature: None,
                },
            ],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));

    // Reset the cursor so reconcile reprocesses every op as a fresh peer would.
    reset_test_cursor(&service, topic_id).await;

    // Reconcile completes despite the hostile whole-document ops.
    let result = service
        .reconcile_document_topics([topic_id])
        .await
        .expect("reconcile skips whole-document admin sync instead of wedging");

    // The admin operation on the same topic still applied.
    assert!(result.targets.contains(&target));
    let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
        .await
        .expect("user materialized by the admin operation");
    assert_eq!(
        User::from_bytes(&stored_user).expect("user decodes").name,
        "Skip Survivor"
    );

    // The cursor advanced past the hostile ops.
    let cursor = read_test_cursor(&storage, topic_id)
        .await
        .expect("cursor persisted");
    let topic_clock = service
        .node()
        .storage()
        .actor_clock(&topic_id)
        .expect("topic clock");
    assert!(
        cursor.dominates(&topic_clock),
        "cursor must advance past the hostile ops"
    );

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 2, "{records:?}");
    for record in &records {
        assert_eq!(record.reason, "unsupported whole-document admin sync event");
        assert_eq!(record.target(), Some(&target));
        assert_eq!(
            record.decoded_event().expect("event decodes").placement(),
            placement
        );
    }

    service.shutdown().await;
}

#[tokio::test]
async fn inbound_admin_validation_rejects_publisher_impersonation_for_every_family() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([63; 32]);
    let user_id = UserId::local(Ulid::from_parts(1_600, 1), realm_id);
    let actor = test_actor(63, user_id, realm_id);
    let group_id = Ulid::from_parts(1_601, 1);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Management);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentSyncTarget::RealmConfig { realm_id },
            config.to_bytes(&actor).expect("config serializes").into(),
        )],
    )
    .await
    .expect("config writes");

    let cases = [
        (
            DocumentSyncTarget::RealmConfig { realm_id },
            test_admin_event(
                Ulid::from_parts(1_602, 1),
                AdminDocumentTarget::RealmConfig { realm_id },
                &actor,
                1,
                AdminDocumentOperation::RealmConfigDescriptionSet {
                    description: "forged".to_string(),
                },
            ),
        ),
        (
            DocumentSyncTarget::RealmAuthorization { realm_id },
            test_admin_event(
                Ulid::from_parts(1_603, 1),
                AdminDocumentTarget::Realm { realm_id },
                &actor,
                1,
                AdminDocumentOperation::RealmRoleAdded {
                    role_id: Ulid::from_parts(1_604, 1),
                },
            ),
        ),
        (
            DocumentSyncTarget::User { user_id },
            test_admin_event(
                Ulid::from_parts(1_605, 1),
                AdminDocumentTarget::User { user_id },
                &actor,
                1,
                AdminDocumentOperation::UserNameSet {
                    name: "forged".to_string(),
                },
            ),
        ),
        (
            DocumentSyncTarget::GroupAuthorization { group_id },
            test_admin_event(
                Ulid::from_parts(1_606, 1),
                AdminDocumentTarget::Group { group_id },
                &actor,
                1,
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name: "forged".to_string(),
                    owner: user_id,
                },
            ),
        ),
    ];

    for (target, event) in cases {
        let placement = admin_test_placement();
        let topic_id = target.sync_topic_id(realm_id, &placement);
        let impersonator = irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&node(64)));
        assert!(
            matches!(
                validate_replicated_admin_event(
                    &storage,
                    topic_id,
                    impersonator,
                    &target,
                    &event,
                    realm_id,
                    &placement,
                    &sign_as_origin(&event, &placement),
                    &mut ConfigValidationCache::default(),
                )
                .await
                .expect("storage succeeds"),
                AdminEventValidation::Rejected(_)
            ),
            "publisher impersonation must be rejected for {target:?}"
        );
    }
}

#[tokio::test]
async fn inbound_admin_validation_enforces_management_and_preserves_genesis() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([65; 32]);
    let nil_actor = test_actor(65, UserId::nil(realm_id), realm_id);
    let config_target = DocumentSyncTarget::RealmConfig { realm_id };
    let config_topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let publisher =
        irokle_crate::actor_id_for(config_topic, node_id_to_peer_id(&nil_actor.node_id));
    let ensure = test_admin_event(
        Ulid::from_parts(1_610, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &nil_actor,
        1,
        AdminDocumentOperation::RealmConfigNodeEnsured {
            node_id: nil_actor.node_id,
            kind: RealmNodeKind::Management,
        },
    );
    assert_eq!(
        validate_replicated_admin_event(
            &storage,
            config_topic,
            publisher,
            &config_target,
            &ensure,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&ensure, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("storage succeeds"),
        AdminEventValidation::Accepted
    );
    apply_admin_document_operation_to_storage(&storage, config_target.clone(), ensure)
        .await
        .expect("bootstrap node applies");

    let mut description = test_admin_event(
        Ulid::from_parts(1_611, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &nil_actor,
        2,
        AdminDocumentOperation::RealmConfigDescriptionSet {
            description: "genesis".to_string(),
        },
    );
    description.observed.advance(nil_actor.node_id, 1);
    assert_eq!(
        validate_replicated_admin_event(
            &storage,
            config_topic,
            publisher,
            &config_target,
            &description,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&description, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("storage succeeds"),
        AdminEventValidation::Accepted,
        "the management self-ensure must authorize the rest of config genesis"
    );

    let (_auth_dir, auth_storage) = test_storage();
    let auth_target = DocumentSyncTarget::RealmAuthorization { realm_id };
    let auth_topic = auth_target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let auth_publisher =
        irokle_crate::actor_id_for(auth_topic, node_id_to_peer_id(&nil_actor.node_id));
    let genesis_role = test_admin_event(
        Ulid::from_parts(1_612, 1),
        AdminDocumentTarget::Realm { realm_id },
        &nil_actor,
        1,
        AdminDocumentOperation::RealmRoleCreated {
            role: test_admin_role_definition(
                Ulid::from_parts(1_613, 1),
                "realm_admin",
                &format!("/{realm_id}/admin/**"),
                Permission::WRITE,
            ),
        },
    );
    assert_eq!(
        validate_replicated_admin_event(
            &auth_storage,
            auth_topic,
            auth_publisher,
            &auth_target,
            &genesis_role,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&genesis_role, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("storage succeeds"),
        AdminEventValidation::Accepted
    );

    let server_actor = test_actor(66, UserId::local(Ulid::generate(), realm_id), realm_id);
    let mut server_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    server_config.ensure_node(server_actor.node_id, RealmNodeKind::Server);
    storage_batch_write_to(
        &auth_storage,
        vec![target_write_entry(
            DocumentSyncTarget::RealmConfig { realm_id },
            server_config
                .to_bytes(&server_actor)
                .expect("config serializes")
                .into(),
        )],
    )
    .await
    .expect("config writes");
    for (target, event) in [
        (
            DocumentSyncTarget::RealmConfig { realm_id },
            test_admin_event(
                Ulid::from_parts(1_614, 1),
                AdminDocumentTarget::RealmConfig { realm_id },
                &server_actor,
                1,
                AdminDocumentOperation::RealmConfigQuotaSet {
                    quota: QuotaConfig::default(),
                },
            ),
        ),
        (
            DocumentSyncTarget::RealmAuthorization { realm_id },
            test_admin_event(
                Ulid::from_parts(1_615, 1),
                AdminDocumentTarget::Realm { realm_id },
                &server_actor,
                1,
                AdminDocumentOperation::RealmRoleAdded {
                    role_id: Ulid::generate(),
                },
            ),
        ),
    ] {
        let placement = admin_test_placement();
        let topic_id = target.sync_topic_id(realm_id, &placement);
        let publisher =
            irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&server_actor.node_id));
        assert!(matches!(
            validate_replicated_admin_event(
                &auth_storage,
                topic_id,
                publisher,
                &target,
                &event,
                realm_id,
                &placement,
                &sign_as_origin(&event, &placement),
                &mut ConfigValidationCache::default(),
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Rejected(_)
        ));
    }
}

#[tokio::test]
async fn inbound_rejects_pool() {
    // A child band pool whose issuer does not own its parent is rejected;
    // one whose parent has not replicated yet is deferred.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([73; 32]);
    let coordinator = test_actor(73, UserId::local(Ulid::generate(), realm_id), realm_id);
    let attacker = test_actor(74, UserId::local(Ulid::generate(), realm_id), realm_id);
    let config_target = DocumentSyncTarget::RealmConfig { realm_id };
    let admin_target = AdminDocumentTarget::RealmConfig { realm_id };
    let topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);

    let root = BandPool {
        pool_id: Ulid::from_bytes([90; 16]),
        parent: None,
        issuer: coordinator.node_id,
        owner: coordinator.node_id,
        start: FIRST_GRANTABLE_HANDLE,
        end: band_start(HANDLE_BANDS),
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(coordinator.node_id, RealmNodeKind::Management);
    config.band_pools.push(root);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            config_target.clone(),
            config
                .to_bytes(&coordinator)
                .expect("config serializes")
                .into(),
        )],
    )
    .await
    .expect("config writes");

    let forged = BandPool {
        pool_id: Ulid::from_bytes([91; 16]),
        parent: Some(root.pool_id),
        issuer: attacker.node_id,
        owner: attacker.node_id,
        start: band_start(1),
        end: band_start(2),
    };
    let forged_event = test_admin_event(
        Ulid::from_parts(1_702, 1),
        admin_target.clone(),
        &attacker,
        1,
        AdminDocumentOperation::RealmConfigBandPoolAssigned { pool: forged },
    );
    let forged_publisher = irokle_crate::actor_id_for(topic, node_id_to_peer_id(&attacker.node_id));
    assert!(matches!(
        validate_replicated_admin_event(
            &storage,
            topic,
            forged_publisher,
            &config_target,
            &forged_event,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&forged_event, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("storage succeeds"),
        AdminEventValidation::Rejected(_)
    ));

    let orphan = BandPool {
        pool_id: Ulid::from_bytes([92; 16]),
        parent: Some(Ulid::from_bytes([99; 16])),
        issuer: coordinator.node_id,
        owner: coordinator.node_id,
        start: band_start(1),
        end: band_start(2),
    };
    let orphan_event = test_admin_event(
        Ulid::from_parts(1_703, 1),
        admin_target,
        &coordinator,
        1,
        AdminDocumentOperation::RealmConfigBandPoolAssigned { pool: orphan },
    );
    let publisher = irokle_crate::actor_id_for(topic, node_id_to_peer_id(&coordinator.node_id));
    assert!(matches!(
        validate_replicated_admin_event(
            &storage,
            topic,
            publisher,
            &config_target,
            &orphan_event,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&orphan_event, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("storage succeeds"),
        AdminEventValidation::Deferred { .. }
    ));
}

#[tokio::test]
async fn inbound_checks_grants() {
    // Replicated grants outside or misaligned with an issuer pool are
    // rejected; a canonical grant waits until its pool replicates.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([75; 32]);
    let coordinator = test_actor(75, UserId::local(Ulid::generate(), realm_id), realm_id);
    let config_target = DocumentSyncTarget::RealmConfig { realm_id };
    let admin_target = AdminDocumentTarget::RealmConfig { realm_id };
    let topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let publisher = irokle_crate::actor_id_for(topic, node_id_to_peer_id(&coordinator.node_id));

    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(coordinator.node_id, RealmNodeKind::Management);
    config.band_pools.push(BandPool {
        pool_id: Ulid::from_bytes([93; 16]),
        parent: None,
        issuer: coordinator.node_id,
        owner: coordinator.node_id,
        start: band_start(0),
        end: band_start(2),
    });
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            config_target.clone(),
            config
                .to_bytes(&coordinator)
                .expect("config serializes")
                .into(),
        )],
    )
    .await
    .expect("config writes");

    for (event_id, range, expected) in [
        (
            Ulid::from_parts(1_704, 1),
            HandleRange {
                range_id: Ulid::from_bytes([94; 16]),
                owner: node(76),
                start: band_start(3),
                end: band_start(4),
            },
            "handle grant lies outside the coordinator band pool",
        ),
        (
            Ulid::from_parts(1_705, 1),
            HandleRange {
                range_id: Ulid::from_bytes([95; 16]),
                owner: node(76),
                start: band_start(0) + 1,
                end: band_start(0) + 1 + HANDLE_RANGE_SIZE,
            },
            "handle grant is not one canonical band",
        ),
    ] {
        let event = test_admin_event(
            event_id,
            admin_target.clone(),
            &coordinator,
            1,
            AdminDocumentOperation::RealmConfigHandleRangeGranted { range },
        );
        assert_eq!(
            validate_replicated_admin_event(
                &storage,
                topic,
                publisher,
                &config_target,
                &event,
                realm_id,
                &PlacementRef::NIL,
                &sign_as_origin(&event, &PlacementRef::NIL),
                &mut ConfigValidationCache::default(),
            )
            .await
            .expect("storage succeeds"),
            AdminEventValidation::Rejected(expected.to_string())
        );
    }

    config.band_pools.clear();
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            config_target.clone(),
            config
                .to_bytes(&coordinator)
                .expect("config serializes")
                .into(),
        )],
    )
    .await
    .expect("config writes");
    let waiting = test_admin_event(
        Ulid::from_parts(1_706, 1),
        admin_target,
        &coordinator,
        1,
        AdminDocumentOperation::RealmConfigHandleRangeGranted {
            range: HandleRange {
                range_id: Ulid::from_bytes([96; 16]),
                owner: node(76),
                start: band_start(0),
                end: band_start(1),
            },
        },
    );
    assert_eq!(
        validate_replicated_admin_event(
            &storage,
            topic,
            publisher,
            &config_target,
            &waiting,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&waiting, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("storage succeeds"),
        AdminEventValidation::Deferred {
            dependency: None,
            reason: "coordinator band pool is not yet replicated".to_string(),
        }
    );
}
