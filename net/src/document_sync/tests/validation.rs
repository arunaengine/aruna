//! Tests validation of usage, watch, node and policy documents, including forged publishers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

#[test]
fn usage_owner_validation() {
    use aruna_core::structs::storage::usage::UsageCounters;

    let node_id = node(7);
    let realm_id = RealmId::from_bytes([2u8; 32]);
    let group_id = Ulid::from_bytes([4u8; 16]);
    let global = DocumentTarget::NodeUsage {
        realm_id,
        node_id,
        group_id: None,
    };
    let group = DocumentTarget::NodeUsage {
        realm_id,
        node_id,
        group_id: Some(group_id),
    };

    // The owning node's own snapshot validates as global and per-group.
    let owned = NodeUsageSnapshot {
        node_id,
        counters: UsageCounters {
            buckets: 3,
            ..Default::default()
        },
        replicas: Default::default(),
        revisions: Default::default(),
    };
    let owned_bytes = owned.to_bytes().unwrap();
    assert!(validate_usage_upsert(&global, &owned_bytes).is_ok());
    assert!(validate_usage_upsert(&group, &owned_bytes).is_ok());

    // Zero-counter snapshots (stale-group cleanup) are legitimate upserts.
    let zero = NodeUsageSnapshot {
        node_id,
        counters: UsageCounters::default(),
        replicas: Default::default(),
        revisions: Default::default(),
    };
    assert!(validate_usage_upsert(&global, &zero.to_bytes().unwrap()).is_ok());

    // A snapshot whose embedded node id is a different node is rejected.
    let misattributed = NodeUsageSnapshot {
        node_id: node(9),
        counters: UsageCounters {
            buckets: 99,
            ..Default::default()
        },
        replicas: Default::default(),
        revisions: Default::default(),
    };
    assert!(validate_usage_upsert(&global, &misattributed.to_bytes().unwrap()).is_err());

    // Undecodable payloads and non node-usage targets are rejected.
    assert!(validate_usage_upsert(&global, b"not-a-snapshot").is_err());
    assert!(
        validate_usage_upsert(&DocumentTarget::RealmConfig { realm_id }, &owned_bytes).is_err()
    );
}

#[test]
fn watch_interest_validation() {
    use aruna_core::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};

    let node_id = node(7);
    let realm_id = RealmId::from_bytes([12u8; 32]);
    let target = DocumentTarget::WatchInterest { realm_id, node_id };

    // The owning node's own digest validates.
    let owned = WatchInterestDigest::from_subscriptions(
        node_id,
        [(
            "/owned/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        )],
    );
    let owned_bytes = owned.to_bytes().unwrap();
    assert!(validate_watch_interest(&target, &owned_bytes).is_ok());

    // Empty digests are legitimate upserts that clear a node's interest.
    let empty = WatchInterestDigest {
        node_id,
        entries: Vec::new(),
    };
    assert!(validate_watch_interest(&target, &empty.to_bytes().unwrap()).is_ok());

    let too_many = WatchInterestDigest::from_subscriptions(
        node_id,
        (0..=INTEREST_ENTRY_CAP).map(|index| {
            (
                format!("/entry/{index}"),
                WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            )
        }),
    );
    assert!(validate_watch_interest(&target, &too_many.to_bytes().unwrap()).is_err());
    assert!(validate_watch_interest(&target, &vec![0; INTEREST_BYTES_CAP + 1],).is_err());

    // A digest whose embedded node id is a different node is rejected.
    let misattributed = WatchInterestDigest::from_subscriptions(
        node(9),
        [(
            "/forged/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        )],
    );
    assert!(validate_watch_interest(&target, &misattributed.to_bytes().unwrap()).is_err());

    // Undecodable payloads and non watch-interest targets are rejected.
    assert!(validate_watch_interest(&target, b"not-a-digest").is_err());
    assert!(
        validate_watch_interest(&DocumentTarget::RealmConfig { realm_id }, &owned_bytes,).is_err()
    );
}

#[tokio::test]
async fn watch_origins_converge() {
    // Independent origins must not lose a replica to arrival-order admission.
    use aruna_core::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};

    let (_left_dir, left) = test_storage();
    let (_right_dir, right) = test_storage();
    let realm_id = RealmId::from_bytes([57u8; 32]);
    let make = |seed: u8, event_mask: WatchEventMask| {
        let owner = UserId::local(Ulid::from_parts(seed as u64, 1), realm_id);
        let watch_id = Ulid::from_parts(seed as u64, 2);
        let mut subscription =
            WatchSubscription::new(owner, format!("watch/{seed}"), event_mask, 1);
        subscription.watch_id = watch_id;
        let change = DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id: Ulid::from_parts(seed as u64, 3),
                actor: node(seed),
                updated_at_ms: 1,
            },
            kind: DocumentChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };
        (
            DocumentTarget::WatchSubscription { owner, watch_id },
            subscription.to_bytes().expect("subscription serializes"),
            change,
        )
    };
    let first = make(
        1,
        WatchEventMask::from_kinds([WatchEventKind::SyncCompleted]),
    );
    let second = make(2, WatchEventMask::from_kinds([WatchEventKind::SyncFailed]));

    assert!(validate_watch_upsert(&first.0, &first.1, &first.2).is_ok());
    assert!(
        store_watch_change(&left, first.0.clone(), Some(first.1.clone()), first.2,)
            .await
            .expect("first origin applies")
    );
    assert!(validate_watch_upsert(&second.0, &second.1, &second.2).is_ok());
    assert!(
        store_watch_change(&left, second.0.clone(), Some(second.1.clone()), second.2,)
            .await
            .expect("second origin applies")
    );
    assert!(validate_watch_upsert(&second.0, &second.1, &second.2).is_ok());
    assert!(
        store_watch_change(&right, second.0.clone(), Some(second.1.clone()), second.2,)
            .await
            .expect("second origin applies in reverse order")
    );
    assert!(validate_watch_upsert(&first.0, &first.1, &first.2).is_ok());
    assert!(
        store_watch_change(&right, first.0.clone(), Some(first.1.clone()), first.2,)
            .await
            .expect("first origin applies in reverse order")
    );

    for storage in [&left, &right] {
        assert!(
            read_storage_value(storage, first.0.storage_keyspace(), first.0.storage_key(),)
                .await
                .is_some()
        );
        assert!(
            read_storage_value(storage, second.0.storage_keyspace(), second.0.storage_key(),)
                .await
                .is_some()
        );
    }
    let unknown_mask: WatchEventMask =
        postcard::from_bytes(&postcard::to_allocvec(&16u32).expect("unknown mask serializes"))
            .expect("unknown mask decodes");
    let (target, bytes, change) = make(3, unknown_mask);
    assert!(validate_watch_upsert(&target, &bytes, &change).is_err());
}

#[test]
fn stale_advertisement_skipped() {
    // A delayed advertisement from an older rejoin epoch must not replace
    // the newer one this node already stored.
    use aruna_core::structs::storage::node_info::{
        AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization,
    };

    let document = |membership: u64, publisher: u64| NodeInfoDocument {
        node_id: node(7),
        executors: Vec::new(),
        labels: std::collections::BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 1,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 5,
            download_slots: None,
            storage_capacity_bytes: None,
        },
        updated_at_ms: 5,
        epoch: AdvertisementEpoch {
            membership_generation: membership,
            publisher_generation: publisher,
            observed_at_ms: 5,
        },
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    };
    let current = document(7, 1).to_bytes().expect("document serializes");

    assert!(node_info_supersedes(&document(7, 2), Some(&current)));
    assert!(!node_info_supersedes(&document(6, 900), Some(&current)));
    assert!(!node_info_supersedes(&document(7, 1), Some(&current)));
    assert!(node_info_supersedes(&document(1, 1), None));
    assert!(node_info_supersedes(&document(1, 1), Some(b"corrupt")));

    // An unbounded or self-contradicting snapshot never reaches storage.
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let target = DocumentTarget::NodeInfo {
        realm_id,
        node_id: node(7),
    };
    let mut ahead = document(7, 1);
    ahead.demand.epoch.membership_generation = 8;
    assert!(validate_node_upsert(&target, &postcard::to_allocvec(&ahead).unwrap()).is_err());
}

#[tokio::test]
async fn admits_authentic_policy() {
    let realm_id = RealmId::from_bytes([21u8; 32]);
    let (dir, storage) = test_storage();
    let (config, auth) = policy_realm_view(realm_id);
    write_realm_view(&storage, &config, &auth).await;
    let service = policy_service(realm_id, storage.clone(), dir.path()).await;

    let policy = policy_fixture(Ulid::from_bytes([8u8; 16]));
    let document = signed_policy_document(realm_id, &policy, 1);
    let placement = config
        .policy_placement(policy.policy().policy_id)
        .expect("policy bucket resolves");
    assert!(matches!(
        service
            .apply_policy_document(&document, placement)
            .await
            .expect("apply runs"),
        MetadataPlacementOutcome::Accepted(true)
    ));

    let target = placement_policy_target(policy.policy().policy_id);
    assert!(
        read_storage_value(&storage, target.storage_keyspace(), target.storage_key())
            .await
            .is_some(),
        "an authentic publication must be stored"
    );
}

#[tokio::test]
async fn rejects_forged_publication() {
    // A self-authoring ordinary node and a relay that substitutes itself as
    // origin both lack realm-admin publication authority.
    let realm_id = RealmId::from_bytes([22u8; 32]);
    let (dir, storage) = test_storage();
    let (config, auth) = policy_realm_view(realm_id);
    write_realm_view(&storage, &config, &auth).await;
    let service = policy_service(realm_id, storage.clone(), dir.path()).await;

    let policy = policy_fixture(Ulid::from_bytes([9u8; 16]));
    let placement = config
        .policy_placement(policy.policy().policy_id)
        .expect("policy bucket resolves");
    let authentic = signed_policy_document(realm_id, &policy, 1);

    let secret = iroh::SecretKey::from_bytes(&[2u8; 32]);
    let self_authored = PlacementPolicyDocument::new(
        realm_id,
        &policy,
        aruna_core::structs::placement::policy::document::PolicyPublicationClaim::new(
            realm_id,
            &policy,
            secret.public(),
            UserId::local(Ulid::from_bytes([7u8; 16]), realm_id),
            Ulid::from_bytes([5u8; 16]),
            9,
            [0u8; 32],
        )
        .sign(&secret),
    );
    let mut relayed = authentic.clone();
    relayed.publication.publisher = node(2);

    for forged in [self_authored, relayed] {
        assert!(matches!(
            service
                .apply_policy_document(&forged, placement)
                .await
                .expect("apply runs"),
            MetadataPlacementOutcome::Rejected
        ));
    }

    let target = placement_policy_target(policy.policy().policy_id);
    assert!(
        read_storage_value(&storage, target.storage_keyspace(), target.storage_key())
            .await
            .is_none(),
        "a forged publication must leave no row"
    );
}

#[tokio::test]
async fn defers_unknown_authority() {
    // Without the replicated authorization document the publication cannot
    // be verified yet, so it waits instead of being accepted or dropped.
    let realm_id = RealmId::from_bytes([23u8; 32]);
    let (dir, storage) = test_storage();
    let (config, _) = policy_realm_view(realm_id);
    let actor = aruna_core::structs::identity::auth::Actor {
        node_id: node(1),
        user_id: policy_admin(realm_id),
        realm_id,
    };
    let target = DocumentTarget::RealmConfig { realm_id };
    batch_write_to(
        &storage,
        vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            Value::from(config.to_bytes(&actor).expect("config encodes")),
        )],
    )
    .await
    .expect("config is stored");
    let service = policy_service(realm_id, storage.clone(), dir.path()).await;

    let policy = policy_fixture(Ulid::from_bytes([10u8; 16]));
    let document = signed_policy_document(realm_id, &policy, 1);
    let placement = config
        .policy_placement(policy.policy().policy_id)
        .expect("policy bucket resolves");
    assert!(matches!(
        service
            .apply_policy_document(&document, placement)
            .await
            .expect("apply runs"),
        MetadataPlacementOutcome::Deferred(DocumentSyncDependency::RealmAuthorization(deferred))
            if deferred == realm_id
    ));
}

#[test]
fn binds_policy_target() {
    use aruna_core::structs::placement::policy::document::placement_policy_change;
    use aruna_core::structs::placement::policy::{
        PlacementPolicy, PlacementSelector, VerifiedPolicy,
    };

    let realm_id = RealmId::from_bytes([2u8; 32]);
    let policy_id = Ulid::from_bytes([8u8; 16]);
    let selector = |location: &str| PlacementSelector {
        node_id: None,
        location: Some(location.to_string()),
        labels: Vec::new(),
        executor_kind: None,
    };
    let policy = VerifiedPolicy::verify(
        PlacementPolicy::new(
            policy_id,
            "residency".to_string(),
            vec![selector("eu-west")],
        )
        .expect("policy is valid"),
    )
    .expect("policy verifies");
    let document = signed_policy_document(realm_id, &policy, 7);
    let placement = PlacementRef {
        strategy_id: Ulid::from_bytes([6u8; 16]),
        shard: 3,
    };
    let change = placement_policy_change(&document, placement);
    assert!(validate_policy_document(policy_id, realm_id, &document, &change).is_ok());

    // Another target, another realm, or a restated revision is refused.
    assert!(
        validate_policy_document(Ulid::from_bytes([9u8; 16]), realm_id, &document, &change)
            .is_err()
    );
    assert!(
        validate_policy_document(
            policy_id,
            RealmId::from_bytes([3u8; 32]),
            &document,
            &change
        )
        .is_err()
    );
    let mut restated = change;
    restated.current.actor = node(8);
    assert!(validate_policy_document(policy_id, realm_id, &document, &restated).is_err());
}

#[tokio::test]
async fn forged_upsert_skipped() {
    use aruna_core::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};

    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([55u8; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(55).await,
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
    let forged_node = node(88);
    assert_ne!(local_node, forged_node);
    let target = DocumentTarget::WatchInterest {
        realm_id,
        node_id: local_node,
    };
    let forged_target = DocumentTarget::WatchInterest {
        realm_id,
        node_id: forged_node,
    };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    assert_eq!(
        topic_id,
        forged_target.sync_topic_id(realm_id, &PlacementRef::NIL)
    );

    let change = || DocumentChange {
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
    let forged_digest = WatchInterestDigest::from_subscriptions(
        forged_node,
        [(
            "/forged/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        )],
    );
    let owned_digest = WatchInterestDigest::from_subscriptions(
        local_node,
        [(
            "/owned/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        )],
    );
    let owned_bytes = owned_digest.to_bytes().expect("digest serializes");

    // The forged upsert claims another node's key but is signed by this local
    // node; the legitimate one is signed by its owner on the shared topic.
    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::Upsert {
                    event_id: Ulid::generate(),
                    target: forged_target.clone(),
                    bytes: forged_digest.to_bytes().expect("digest serializes"),
                    change: change(),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: Ulid::generate(),
                    target: target.clone(),
                    bytes: owned_bytes.clone(),
                    change: change(),
                    allow_genesis: true,
                },
            ],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentNetEvent::DocumentsPublished { .. }
    ));

    reset_test_cursor(&service, topic_id).await;

    let result = service
        .reconcile_document_topics([topic_id])
        .await
        .expect("reconcile skips forged watch-interest upsert");

    assert!(result.targets.contains(&target));
    assert!(!result.targets.contains(&forged_target));
    let stored = read_storage_value(&storage, target.storage_keyspace(), target.storage_key())
        .await
        .expect("watch interest digest applied");
    assert_eq!(
        WatchInterestDigest::from_bytes(&stored).expect("digest decodes"),
        owned_digest
    );
    assert!(
        read_storage_value(
            &storage,
            forged_target.storage_keyspace(),
            forged_target.storage_key(),
        )
        .await
        .is_none()
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
        "cursor must advance past the forged upsert"
    );

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].reason,
        "watch interest publisher is not the owning node"
    );
    assert_eq!(records[0].target(), Some(&forged_target));

    service.shutdown().await;
}

#[tokio::test]
async fn forged_watch_skipped() {
    use aruna_core::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};

    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let realm_id = RealmId::from_bytes([56u8; 32]);
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(56).await,
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
    let forged_node = node(89);
    assert_ne!(local_node, forged_node);
    let target = DocumentTarget::WatchInterest {
        realm_id,
        node_id: local_node,
    };
    let forged_target = DocumentTarget::WatchInterest {
        realm_id,
        node_id: forged_node,
    };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    assert_eq!(
        topic_id,
        forged_target.sync_topic_id(realm_id, &PlacementRef::NIL)
    );

    let change = |kind| DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::generate(),
            actor: local_node,
            updated_at_ms: 1,
        },
        kind,
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
    };
    let digest = WatchInterestDigest::from_subscriptions(
        local_node,
        [(
            "/owned/**".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        )],
    );
    let digest_bytes = digest.to_bytes().expect("digest serializes");
    // The local node originates the hostile op, so it clears the origin binding:
    // a realm-config operation has no business on a watch-interest target.
    let actor = Actor {
        node_id: local_node,
        user_id: UserId::local(Ulid::from_parts(1_560, 1), realm_id),
        realm_id,
    };
    let admin_event = test_admin_event(
        Ulid::from_parts(1_561, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &actor,
        1,
        AdminDocumentOperation::ConfigDescriptionSet {
            description: "forged watch-interest admin op".to_string(),
        },
    );

    // Hostile non-upserts precede a legitimate owner-signed digest on the shared
    // topic; reconcile must skip both and continue to the valid upsert.
    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::Delete {
                    event_id: Ulid::generate(),
                    target: forged_target.clone(),
                    change: change(DocumentChangeKind::Delete),
                    allow_genesis: true,
                },
                DocumentSyncPublish::AdminOperation {
                    target: forged_target.clone(),
                    event: Box::new(admin_event),
                    placement: PlacementRef::NIL,
                    allow_genesis: true,
                    origin_signature: None,
                },
                DocumentSyncPublish::Upsert {
                    event_id: Ulid::generate(),
                    target: target.clone(),
                    bytes: digest_bytes.clone(),
                    change: change(DocumentChangeKind::Upsert),
                    allow_genesis: true,
                },
            ],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentNetEvent::DocumentsPublished { .. }
    ));

    reset_test_cursor(&service, topic_id).await;

    let result = service
        .reconcile_document_topics([topic_id])
        .await
        .expect("reconcile skips forged watch-interest non-upserts");

    assert!(result.targets.contains(&target));
    assert!(!result.targets.contains(&forged_target));
    let stored = read_storage_value(&storage, target.storage_keyspace(), target.storage_key())
        .await
        .expect("watch interest digest applied");
    assert_eq!(
        WatchInterestDigest::from_bytes(&stored).expect("digest decodes"),
        digest
    );
    assert!(
        read_storage_value(
            &storage,
            forged_target.storage_keyspace(),
            forged_target.storage_key(),
        )
        .await
        .is_none()
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
        "cursor must advance past the forged non-upserts"
    );

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 2, "{records:?}");
    for record in &records {
        assert_eq!(
            record.reason,
            "unsupported non-upsert shared realm document event"
        );
    }

    service.shutdown().await;
}

// A forged non-upsert node-usage event on the shared realm topic must be skipped,
// not `?`-propagated, or every peer's reconcile errors at the op forever.
#[tokio::test]
async fn forged_usage_skipped() {
    use aruna_core::structs::storage::usage::UsageCounters;

    let (_storage_dir, storage) = test_storage();
    let doc_dir = tempfile::tempdir().expect("doc dir");
    let service = DocumentSyncService::open_with_policy(
        test_endpoint(53).await,
        storage.clone(),
        doc_dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        RealmId::from_bytes([53u8; 32]),
    )
    .expect("document sync service opens");

    let local_node = service.local_node_id().expect("local node id");
    let realm_id = RealmId::from_bytes([53u8; 32]);
    let target = DocumentTarget::NodeUsage {
        realm_id,
        node_id: local_node,
        group_id: None,
    };
    let topic_id = target.sync_topic_id(realm_id, &PlacementRef::NIL);

    let change = |kind| DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::generate(),
            actor: local_node,
            updated_at_ms: 1,
        },
        kind,
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
    };

    let snapshot = NodeUsageSnapshot {
        node_id: local_node,
        counters: UsageCounters {
            buckets: 5,
            ..Default::default()
        },
        replicas: Default::default(),
        revisions: Default::default(),
    };
    let snapshot_bytes = snapshot.to_bytes().expect("snapshot serializes");

    // Hostile Delete first, then a legitimate owner-signed Upsert on the same
    // topic. Publishing appends both ops and advances the local cursor.
    let published = service
        .publish_documents(
            vec![
                DocumentSyncPublish::Delete {
                    event_id: Ulid::generate(),
                    target: target.clone(),
                    change: change(DocumentChangeKind::Delete),
                    allow_genesis: true,
                },
                DocumentSyncPublish::Upsert {
                    event_id: Ulid::generate(),
                    target: target.clone(),
                    bytes: snapshot_bytes.clone(),
                    change: change(DocumentChangeKind::Upsert),
                    allow_genesis: true,
                },
            ],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentNetEvent::DocumentsPublished { .. }
    ));

    // Reset the cursor so reconcile reprocesses both ops exactly as a fresh
    // peer receiving them via sync would (its cursor has not yet advanced).
    reset_test_cursor(&service, topic_id).await;

    // (a) Reconcile completes without error despite the hostile Delete.
    let result = service
        .reconcile_document_topics([topic_id])
        .await
        .expect("reconcile skips the forged delete instead of wedging");

    // (c) The legitimate upsert on the same topic still applied.
    assert!(result.targets.contains(&target));
    let stored = read_storage_value(&storage, target.storage_keyspace(), target.storage_key())
        .await
        .expect("node usage snapshot applied");
    assert_eq!(
        NodeUsageSnapshot::from_bytes(&stored).expect("snapshot decodes"),
        snapshot
    );

    // (b) The cursor advanced past the hostile op.
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
        "cursor must advance past the hostile op"
    );

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].reason,
        "unsupported non-upsert shared realm document event"
    );
    assert_eq!(records[0].family(), Some(SyncQuarantineFamily::Delete));

    service.shutdown().await;
}

#[test]
fn node_owner_validation() {
    use aruna_core::structs::storage::node_info::{
        AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization,
    };

    let node_id = node(7);
    let realm_id = RealmId::from_bytes([2u8; 32]);
    let target = DocumentTarget::NodeInfo { realm_id, node_id };

    let owned = NodeInfoDocument {
        node_id,
        executors: Vec::new(),
        labels: std::collections::BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 1,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 5,
            download_slots: None,
            storage_capacity_bytes: None,
        },
        updated_at_ms: 5,
        epoch: AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation: 1,
            observed_at_ms: 5,
        },
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    };
    assert!(validate_node_upsert(&target, &owned.to_bytes().unwrap()).is_ok());

    // A document whose embedded node id is a different node is rejected.
    let misattributed = NodeInfoDocument {
        node_id: node(9),
        ..owned.clone()
    };
    assert!(validate_node_upsert(&target, &misattributed.to_bytes().unwrap()).is_err());

    // Undecodable payloads and non node-info targets are rejected.
    assert!(validate_node_upsert(&target, b"not-a-document").is_err());
    assert!(
        validate_node_upsert(
            &DocumentTarget::RealmConfig { realm_id },
            &owned.to_bytes().unwrap()
        )
        .is_err()
    );
}

fn setting_change(actor: NodeId, generation: u64, kind: DocumentChangeKind) -> DocumentChange {
    DocumentChange {
        base: None,
        current: DocumentSyncRevision {
            generation,
            event_id: Ulid::generate(),
            actor,
            updated_at_ms: 1,
        },
        kind,
        placement: PlacementRef::NIL,
    }
}

fn email_setting(owner: UserId, seed: u8, on: bool) -> (DocumentTarget, Vec<u8>) {
    use aruna_core::structs::execution::email::EmailPreference;
    use aruna_core::structs::execution::setting::SettingChannel;
    let id = Ulid::from_parts(seed as u64, 1);
    let preference = EmailPreference {
        preference_id: id,
        user_id: owner,
        version: 1,
        categories: match on {
            true => ["share.upload".to_string()].into(),
            false => Default::default(),
        },
        sealed: on.then(|| vec![7; 48]),
        created_at_ms: 1,
    };
    let target = SettingChannel::Email.target(owner, id);
    (target, preference.to_bytes().expect("setting serializes"))
}

#[test]
fn email_upsert_validated() {
    use aruna_core::structs::execution::email::EmailPreference;
    let owner = UserId::local(Ulid::from_parts(1, 1), RealmId::from_bytes([2u8; 32]));
    let upsert = setting_change(node(7), 1, DocumentChangeKind::Upsert);
    for on in [true, false] {
        let (target, bytes) = email_setting(owner, 1, on);
        assert!(validate_setting_upsert(&target, &bytes, &upsert).is_ok());
        let later = setting_change(node(7), 2, DocumentChangeKind::Upsert);
        assert!(validate_setting_upsert(&target, &bytes, &later).is_err());
    }
    let (target, bytes) = email_setting(owner, 1, true);
    let mut preference = EmailPreference::from_bytes(&bytes).expect("setting decodes");
    let other = UserId::local(Ulid::from_parts(9, 9), owner.realm_id);
    let mut broken = Vec::new();
    let changes: [fn(&mut EmailPreference); 5] = [
        |setting: &mut EmailPreference| setting.categories.clear(),
        |setting: &mut EmailPreference| setting.sealed = None,
        |setting: &mut EmailPreference| setting.version = 0,
        |setting: &mut EmailPreference| setting.categories = ["resource.watch".to_string()].into(),
        |setting: &mut EmailPreference| setting.sealed = Some(vec![0; 4096]),
    ];
    for change in changes {
        let mut changed = preference.clone();
        change(&mut changed);
        broken.push(changed);
    }
    preference.user_id = other;
    broken.push(preference);
    for setting in broken {
        let bytes = setting.to_bytes().expect("setting serializes");
        assert!(
            validate_setting_upsert(&target, &bytes, &upsert).is_err(),
            "{setting:?}"
        );
    }
}

#[test]
fn webhook_upsert_validated() {
    use aruna_core::structs::execution::setting::SettingChannel;
    use aruna_core::structs::execution::webhook::WebhookSubscription;
    let owner = UserId::local(Ulid::from_parts(1, 1), RealmId::from_bytes([2u8; 32]));
    let id = Ulid::from_parts(3, 1);
    let target = SettingChannel::Webhook.target(owner, id);
    let upsert = setting_change(node(7), 1, DocumentChangeKind::Upsert);
    let setting = |url: Option<&str>, sealed: Option<Vec<u8>>| {
        let subscription = WebhookSubscription {
            subscription_id: id,
            user_id: owner,
            version: 1,
            url: url.map(str::to_string),
            sealed,
            created_at_ms: 1,
        };
        subscription.to_bytes().expect("setting serializes")
    };
    let url = Some("https://hooks.example.org/aruna");
    assert!(validate_setting_upsert(&target, &setting(url, Some(vec![1; 80])), &upsert).is_ok());
    assert!(validate_setting_upsert(&target, &setting(None, None), &upsert).is_ok());
    for bytes in [
        setting(url, None),
        setting(None, Some(vec![1; 80])),
        setting(Some("ftp://example.org/x"), Some(vec![1; 80])),
        setting(Some("https://user:pw@example.org/x"), Some(vec![1; 80])),
    ] {
        assert!(validate_setting_upsert(&target, &bytes, &upsert).is_err());
    }
    let (email_target, _) = email_setting(owner, 3, true);
    let mismatched = setting(url, Some(vec![1; 80]));
    assert!(validate_setting_upsert(&email_target, &mismatched, &upsert).is_err());
}

#[tokio::test]
async fn foreign_actor_rejected() {
    // A setting whose revision actor is not its publisher, an invalid one and any delete are
    // quarantined; the valid setting applies.
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
    let local = service.local_node_id().expect("local node id");
    let owner = UserId::local(Ulid::from_bytes([3; 16]), realm_id);
    let (forged, forged_bytes) = email_setting(owner, 1, true);
    let (valid, valid_bytes) = email_setting(owner, 2, false);
    let (removed, _) = email_setting(owner, 3, true);
    let (invalid, _) = email_setting(owner, 4, true);
    let topic = valid.sync_topic_id(realm_id, &PlacementRef::NIL);
    let events = [
        Ulid::generate(),
        Ulid::generate(),
        Ulid::generate(),
        Ulid::generate(),
    ];
    let upsert =
        |event_id, target: &DocumentTarget, bytes: &[u8], actor| DocumentSyncPublish::Upsert {
            event_id,
            target: target.clone(),
            bytes: bytes.to_vec(),
            change: setting_change(actor, 1, DocumentChangeKind::Upsert),
            allow_genesis: true,
        };
    let published = service
        .publish_documents(
            vec![
                upsert(events[0], &forged, &forged_bytes, node(71)),
                upsert(events[1], &invalid, &valid_bytes, local),
                DocumentSyncPublish::Delete {
                    event_id: events[2],
                    target: removed,
                    change: setting_change(local, 2, DocumentChangeKind::Delete),
                    allow_genesis: true,
                },
                upsert(events[3], &valid, &valid_bytes, local),
            ],
            Vec::new(),
        )
        .await;
    assert!(matches!(
        published,
        DocumentNetEvent::DocumentsPublished { .. }
    ));
    reset_test_cursor(&service, topic).await;
    service
        .reconcile_document_topics([topic])
        .await
        .expect("rejected settings are quarantined");

    let records = quarantine_rows(&storage).await;
    assert_eq!(records.len(), 3, "{records:?}");
    assert_eq!(
        quarantined_reason(&records, events[0]),
        "notification setting revision actor is not its publisher"
    );
    assert!(quarantined_reason(&records, events[1]).starts_with("invalid notification setting:"));
    assert_eq!(
        quarantined_reason(&records, events[2]),
        "notification settings are never deleted"
    );
    let applied = read_storage_value(&storage, valid.storage_keyspace(), valid.storage_key()).await;
    assert_eq!(applied.as_deref(), Some(valid_bytes.as_slice()));
    let skipped = read_storage_value(&storage, forged.storage_keyspace(), forged.storage_key());
    assert!(skipped.await.is_none());
    service.shutdown().await;
}
