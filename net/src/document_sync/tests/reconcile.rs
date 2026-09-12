use super::*;

// The eager cursor delete is an optimization, not the invariant: a crash leaves
// the replaced cursor, so reconcile detects the lineage change and replays.
#[tokio::test]
async fn applied_cursor_lineage() {
    // A cursor is trusted only while it describes the topic's current history:
    // another genesis, a rebuilt position, or unreadable bytes restart replay.
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([81; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(81).await,
        storage,
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
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
        ::irokle::ActorClock::default(),
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
        ::irokle::ActorClock::default(),
        "a cursor without a lineage is untrusted"
    );
    assert_eq!(
        read(None, genesis),
        ::irokle::ActorClock::default(),
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
        ::irokle::ActorClock::default(),
        "a rebuilt position must not count as applied"
    );

    service.shutdown().await;
}

// Whole-document admin sync is refused by apply_upsert/apply_delete; `?`-ing that
// refusal would freeze the cursor, so the op is skipped while admin still applies.
#[tokio::test]
async fn whole_admin_skipped() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([54u8; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(54).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
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
        .ensure_sync_topics(&[topic_id], Vec::new())
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
    batch_write_to(
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
async fn publisher_impersonation_rejected() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([63; 32]);
    let user_id = UserId::local(Ulid::from_parts(1_600, 1), realm_id);
    let actor = test_actor(63, user_id, realm_id);
    let group_id = Ulid::from_parts(1_601, 1);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Management);
    batch_write_to(
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
        let impersonator = ::irokle::actor_id_for(topic_id, node_to_peer(&node(64)));
        assert!(
            matches!(
                validate_admin_event(
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
async fn management_preserves_genesis() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([65; 32]);
    let nil_actor = test_actor(65, UserId::nil(realm_id), realm_id);
    let config_target = DocumentSyncTarget::RealmConfig { realm_id };
    let config_topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let publisher = ::irokle::actor_id_for(config_topic, node_to_peer(&nil_actor.node_id));
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
        validate_admin_event(
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
    apply_admin_operation(&storage, config_target.clone(), ensure)
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
        validate_admin_event(
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
    let auth_publisher = ::irokle::actor_id_for(auth_topic, node_to_peer(&nil_actor.node_id));
    let genesis_role = test_admin_event(
        Ulid::from_parts(1_612, 1),
        AdminDocumentTarget::Realm { realm_id },
        &nil_actor,
        1,
        AdminDocumentOperation::RealmRoleCreated {
            role: admin_role(
                Ulid::from_parts(1_613, 1),
                "realm_admin",
                &format!("/{realm_id}/admin/**"),
                Permission::WRITE,
            ),
        },
    );
    assert_eq!(
        validate_admin_event(
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
    batch_write_to(
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
        let publisher = ::irokle::actor_id_for(topic_id, node_to_peer(&server_actor.node_id));
        assert!(matches!(
            validate_admin_event(
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
    batch_write_to(
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
    let forged_publisher = ::irokle::actor_id_for(topic, node_to_peer(&attacker.node_id));
    assert!(matches!(
        validate_admin_event(
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
    let publisher = ::irokle::actor_id_for(topic, node_to_peer(&coordinator.node_id));
    assert!(matches!(
        validate_admin_event(
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
    let publisher = ::irokle::actor_id_for(topic, node_to_peer(&coordinator.node_id));

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
    batch_write_to(
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
            validate_admin_event(
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
    batch_write_to(
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
        validate_admin_event(
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

#[tokio::test]
async fn malformed_targets_rejected() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([67; 32]);
    let actor = test_actor(67, UserId::local(Ulid::generate(), realm_id), realm_id);
    let user_id = actor.user_id;
    let other_user = UserId::local(Ulid::generate(), realm_id);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentSyncTarget::RealmConfig { realm_id },
            config.to_bytes(&actor).expect("config serializes").into(),
        )],
    )
    .await
    .expect("config writes");
    let target = DocumentSyncTarget::User { user_id };
    let placement = admin_test_placement();
    let topic_id = target.sync_topic_id(realm_id, &placement);
    let publisher = ::irokle::actor_id_for(topic_id, node_to_peer(&actor.node_id));

    let wrong_target = test_admin_event(
        Ulid::from_parts(1_620, 1),
        AdminDocumentTarget::User {
            user_id: other_user,
        },
        &actor,
        1,
        AdminDocumentOperation::UserNameSet {
            name: "wrong".to_string(),
        },
    );
    assert!(matches!(
        validate_admin_event(
            &storage,
            topic_id,
            publisher,
            &target,
            &wrong_target,
            realm_id,
            &placement,
            &sign_as_origin(&wrong_target, &placement),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("storage succeeds"),
        AdminEventValidation::Rejected(_)
    ));

    let malformed = test_admin_event(
        Ulid::from_parts(1_621, 1),
        AdminDocumentTarget::User { user_id },
        &actor,
        1,
        AdminDocumentOperation::UserAttributeSet {
            key: "display name".to_string(),
            value: "invalid".to_string(),
        },
    );
    assert!(matches!(
        validate_admin_event(
            &storage,
            topic_id,
            publisher,
            &target,
            &malformed,
            realm_id,
            &placement,
            &sign_as_origin(&malformed, &placement),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("storage succeeds"),
        AdminEventValidation::Rejected(_)
    ));
    assert_eq!(
        read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into()).await,
        None,
        "rejected validation must not mutate storage"
    );
}

#[tokio::test]
async fn rejection_advances_cursor() {
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([63; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(63).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let other_realm_id = RealmId::from_bytes([64; 32]);
    let user_id = UserId::local(Ulid::from_parts(1_630, 1), realm_id);
    let local_actor = test_actor(63, user_id, realm_id);
    let claimed_actor = test_actor(64, user_id, realm_id);
    assert_eq!(
        service.local_node_id().expect("local node id"),
        local_actor.node_id
    );

    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(local_actor.node_id, RealmNodeKind::Management);
    config.ensure_node(claimed_actor.node_id, RealmNodeKind::Management);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            target.clone(),
            config
                .to_bytes(&local_actor)
                .expect("realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("realm config writes");

    let placement_entry = |node_id| NodePlacementEntry {
        node_id,
        location: "eu".to_string(),
        weight: 250,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    };
    let wrong_realm_event = test_admin_event(
        Ulid::from_parts(1_631, 1),
        AdminDocumentTarget::RealmConfig {
            realm_id: other_realm_id,
        },
        &local_actor,
        1,
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: placement_entry(node(90)),
        },
    );
    let impersonated_event = test_admin_event(
        Ulid::from_parts(1_632, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &claimed_actor,
        1,
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: placement_entry(node(91)),
        },
    );
    let mut reducer_invalid_event = test_admin_event(
        Ulid::from_parts(1_633, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &local_actor,
        2,
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
            strategy: PlacementStrategy {
                strategy_id: Ulid::from_parts(1_633, 2),
                name: "invalid".to_string(),
                replica_count: Some(0),
                distinct_locations: false,
                affinity: Vec::new(),
                shard_count: 64,
            },
        },
    );
    reducer_invalid_event
        .observed
        .advance(local_actor.node_id, 1);
    let mut valid_event = test_admin_event(
        Ulid::from_parts(1_634, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &local_actor,
        3,
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: placement_entry(local_actor.node_id),
        },
    );
    valid_event.observed.advance(local_actor.node_id, 2);
    // Non-placement admin operations use the same publisher binding and must
    // not retain the old generic bypass.
    let unrelated_event = test_admin_event(
        Ulid::from_parts(1_635, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &claimed_actor,
        2,
        AdminDocumentOperation::RealmConfigDescriptionSet {
            description: "unrelated operation applied".to_string(),
        },
    );
    // Impersonation is a relay signing another origin's envelope with its own
    // key: the local node may carry the bytes but never authorize them.
    let forge = |event: &AdminDocumentEvent| {
        iroh::SecretKey::from_bytes(&[63; 32]).sign(
            &event
                .signing_bytes(&PlacementRef::NIL)
                .expect("event serializes"),
        )
    };
    let impersonated_signature = forge(&impersonated_event);
    let unrelated_signature = forge(&unrelated_event);

    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(wrong_realm_event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                    origin_signature: None,
                },
                DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(impersonated_event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                    origin_signature: Some(impersonated_signature),
                },
                DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(reducer_invalid_event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                    origin_signature: None,
                },
                DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(valid_event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                    origin_signature: None,
                },
                DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(unrelated_event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                    origin_signature: Some(unrelated_signature),
                },
            ],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));

    reset_test_cursor(&service, topic_id).await;

    let result = service
        .reconcile_document_topics([topic_id])
        .await
        .expect("unauthorized placement events are skipped");
    assert!(result.targets.contains(&target));

    let config = stored_realm_config(&storage, realm_id).await;
    assert_eq!(config.description, "");
    assert_eq!(
        config.placement_map,
        vec![placement_entry(local_actor.node_id)]
    );

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
        "cursor must advance past rejected admin events"
    );

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 4, "{records:?}");
    for record in &records {
        assert_eq!(record.family(), Some(SyncQuarantineFamily::AdminOperation));
        assert_eq!(record.target(), Some(&target));
        assert!(!record.reason.is_empty());
        assert!(matches!(
            record.decoded_event().expect("event decodes"),
            DocumentSyncEvent::AdminOperation { .. }
        ));
    }
    assert_eq!(quarantine_usage(&storage).await.records, 4);

    service.shutdown().await;
}

#[tokio::test]
async fn unknown_report_quarantined() {
    // An invented transition report must not park realm-config replay: it is
    // quarantined after same-topic retry and the valid mutation still applies.
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([74; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(74).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens");

    let local_actor = test_actor(74, UserId::nil(realm_id), realm_id);
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(local_actor.node_id, RealmNodeKind::Management);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            target.clone(),
            config
                .to_bytes(&local_actor)
                .expect("realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("realm config writes");

    let invented = test_admin_event(
        Ulid::from_parts(1_740, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &local_actor,
        1,
        AdminDocumentOperation::RealmConfigTransitionStallReported {
            transition_id: Ulid::from_parts(1_741, 1),
            bucket: 0,
            reported_by: local_actor.node_id,
            reason: "invented".to_string(),
        },
    );
    let mut later = test_admin_event(
        Ulid::from_parts(1_742, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &local_actor,
        2,
        AdminDocumentOperation::RealmConfigDescriptionSet {
            description: "progress after the invented report".to_string(),
        },
    );
    later.observed.advance(local_actor.node_id, 1);

    assert!(matches!(
        service
            .publish_documents(
                vec![
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(invented),
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                        origin_signature: None,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(later),
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                        origin_signature: None,
                    },
                ],
                Vec::new(),
            )
            .await,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));
    reset_test_cursor(&service, topic_id).await;

    service
        .reconcile_document_topics([topic_id])
        .await
        .expect("an unknown transition report never blocks reconciliation");

    assert_eq!(
        stored_realm_config(&storage, realm_id).await.description,
        "progress after the invented report",
        "a valid mutation behind an invented report must still apply"
    );
    let topic_clock = service
        .node()
        .storage()
        .actor_clock(&topic_id)
        .expect("topic clock");
    assert!(
        read_test_cursor(&storage, topic_id)
            .await
            .is_some_and(|cursor| cursor.dominates(&topic_clock)),
        "the cursor must advance past a quarantined report, not park on it"
    );
    let deferred: BTreeMap<DocumentSyncDependency, BTreeSet<::irokle::TopicId>> =
        postcard::from_bytes(
            &read_storage_value(
                &storage,
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
                deferred_topics_key(),
            )
            .await
            .expect("deferred registry persists"),
        )
        .expect("deferred registry decodes");
    assert!(
        !deferred
            .get(&DocumentSyncDependency::RealmConfig(realm_id))
            .is_some_and(|topics| topics.contains(&topic_id)),
        "a report must never register the config topic against its own realm config"
    );
    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(records[0].reason.contains("transition plan"));

    service.shutdown().await;
}

#[tokio::test]
async fn plan_report_coalesce() {
    // Plan and report share one coalesced run, so the report is unknown on
    // the first pass and must be accepted by the post-flush retry.
    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([75; 32]);
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

    let local_actor = test_actor(75, UserId::nil(realm_id), realm_id);
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(local_actor.node_id, RealmNodeKind::Management);
    config.ensure_node(node(76), RealmNodeKind::Management);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            target.clone(),
            config
                .to_bytes(&local_actor)
                .expect("realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("realm config writes");

    let transition_id = Ulid::from_parts(1_750, 1);
    let plan = aruna_core::structs::TransitionPlan {
        transition_id,
        strategy_id: Ulid::from_parts(1_751, 1),
        buckets: vec![aruna_core::structs::BucketPlan {
            bucket: 0,
            old_holders: vec![local_actor.node_id],
            target_holders: vec![node(76)],
            predecessor_epoch: 1,
        }],
        target_map_epoch: 2,
        limits: Default::default(),
        created_by: local_actor.node_id,
        created_at_ms: 1,
    };
    let started = test_admin_event(
        Ulid::from_parts(1_752, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &local_actor,
        1,
        AdminDocumentOperation::RealmConfigTransitionStarted { plan },
    );
    let mut barrier = test_admin_event(
        Ulid::from_parts(1_753, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &local_actor,
        2,
        AdminDocumentOperation::RealmConfigTransitionBarrierReported {
            transition_id,
            bucket: 0,
            reported_by: local_actor.node_id,
            frontier: vec![7],
        },
    );
    barrier.observed.advance(local_actor.node_id, 1);

    assert!(matches!(
        service
            .publish_documents(
                vec![
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(started),
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                        origin_signature: None,
                    },
                    DocumentSyncPublish::AdminOperation {
                        target: target.clone(),
                        event: Box::new(barrier),
                        placement: PlacementRef::NIL,
                        allow_genesis: true,
                        origin_signature: None,
                    },
                ],
                Vec::new(),
            )
            .await,
        DocumentSyncNetEvent::DocumentsPublished { .. }
    ));
    reset_test_cursor(&service, topic_id).await;

    service
        .reconcile_document_topics([topic_id])
        .await
        .expect("a plan and its report reconcile in one batch");

    let stored = stored_realm_config(&storage, realm_id).await;
    let transition = stored
        .transition(&transition_id)
        .expect("the plan materialized");
    assert!(
        transition
            .barriers
            .iter()
            .any(|reported| reported.reported_by == local_actor.node_id),
        "the participant's barrier must survive the same-batch retry"
    );
    assert!(quarantine_rows(&storage).await.is_empty());

    service.shutdown().await;
}
