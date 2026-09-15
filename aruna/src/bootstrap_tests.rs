use super::{
    backoff, node_is_ready, prepare_core_documents, publish_core_documents, sync_peer_topic,
    sync_with_retry, unique_user_topic, watch_target_needed,
};
use crate::identity::PersistedNodeIdentity;
use aruna_core::NodeId;
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NOTIFICATION_WATCH_INTEREST_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::{
    Actor, NodePlacementEntry, PlacementRef, RealmConfigDocument, RealmId, RealmNodeKind,
    WatchEventKind, WatchEventMask, WatchInterestDigest, WatchInterestEntry, interest_dirty_key,
    interest_node_key,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::notifications::watch::interest::publish_watch_interest;
use aruna_operations::sync::document_outbox::read_outbox_records;
use aruna_operations::sync::incoming::initialize_incoming_fixture;
use aruna_operations::tasks::incoming::OutboxDrainer;
use aruna_storage::FjallStorage;
use byteview::ByteView;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn user_topics_deduplicate() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let first = DocumentTarget::User {
        user_id: aruna_core::UserId::local(ulid::Ulid::from_bytes([2u8; 16]), realm_id),
    };
    let second = DocumentTarget::User {
        user_id: aruna_core::UserId::local(ulid::Ulid::from_bytes([3u8; 16]), realm_id),
    };
    let first_shard = aruna_core::structs::PlacementRef {
        strategy_id: ulid::Ulid::from_bytes([4u8; 16]),
        shard: 7,
    };
    let second_shard = aruna_core::structs::PlacementRef {
        shard: 8,
        ..first_shard
    };
    let mut synced_topics = std::collections::HashSet::new();

    assert!(unique_user_topic(&mut synced_topics, realm_id, &first_shard, &first).is_some());
    assert_eq!(
        unique_user_topic(&mut synced_topics, realm_id, &first_shard, &second),
        None
    );
    assert!(unique_user_topic(&mut synced_topics, realm_id, &second_shard, &second).is_some());
    assert_eq!(synced_topics.len(), 2);
}

// The retries must survive a full sync budget without flooding the log, and
// must keep asking at a usable pace once they reach the ceiling.
#[test]
fn backoff_grows_capped() {
    assert_eq!(backoff(0), Duration::from_millis(100));
    assert_eq!(backoff(1), Duration::from_millis(200));
    assert_eq!(backoff(4), Duration::from_millis(1_600));
    assert_eq!(backoff(6), Duration::from_secs(5));
    assert_eq!(backoff(u32::MAX), Duration::from_secs(5));
}

// Only a device takes the metadata route, and it is the persisted owner that
// selects it: infrastructure keeps fetching its documents over sync.
#[test]
fn device_identity_routes() {
    let owner = aruna_core::UserId::nil(RealmId::from_bytes([2u8; 32]));
    assert_eq!(PersistedNodeIdentity::User { owner }.owner(), Some(owner));
    assert_eq!(
        PersistedNodeIdentity::Server {
            issuer_private_key_pem: String::new(),
            delegation_signature: String::new(),
        }
        .owner(),
        None
    );
    assert_eq!(
        PersistedNodeIdentity::Management {
            realm_private_key_pem: String::new(),
        }
        .owner(),
        None
    );
}

#[test]
fn watch_target_cases() {
    assert!(watch_target_needed(true, true, false));
    assert!(watch_target_needed(true, false, true));
    assert!(!watch_target_needed(false, true, true));
    assert!(watch_target_needed(false, true, false));
    assert!(!watch_target_needed(false, false, true));
    assert!(!watch_target_needed(false, false, false));
}

fn context(storage_handle: aruna_storage::StorageHandle) -> DriverContext {
    DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

fn context_net(
    storage_handle: aruna_storage::StorageHandle,
    net_handle: NetHandle,
) -> DriverContext {
    DriverContext {
        storage_handle,
        net_handle: Some(net_handle),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    }
}

async fn net_context(realm_id: RealmId, seed: u8) -> (tempfile::TempDir, DriverContext, NetHandle) {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            secret_key: Some(iroh::SecretKey::from_bytes(&[seed; 32])),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .unwrap();
    let context = context_net(storage, net.clone());
    (dir, context, net)
}

async fn write_digest(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    digest: &WatchInterestDigest,
) {
    assert!(matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: interest_node_key(realm_id, node_id).into(),
                value: ByteView::from(digest.to_bytes().unwrap()),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn read_marker(context: &DriverContext, realm_id: RealmId) -> Option<ByteView> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            key: interest_dirty_key(realm_id).into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => panic!("unexpected marker read result: {other:?}"),
    }
}

async fn write_config(context: &DriverContext, realm_id: RealmId, node_id: NodeId) {
    write_config_nodes(context, realm_id, node_id, &[node_id]).await;
}

async fn write_config_nodes(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    nodes: &[NodeId],
) {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    for node in nodes {
        config.ensure_node(*node, RealmNodeKind::Server);
    }
    config.seed_default_placement();
    config.placement_map = nodes
        .iter()
        .map(|node| NodePlacementEntry {
            node_id: *node,
            location: String::new(),
            weight: 100,
            full: false,
            draining: false,
            labels: Default::default(),
        })
        .collect();
    let actor = Actor {
        node_id,
        user_id: aruna_core::UserId::nil(realm_id),
        realm_id,
    };
    assert!(matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: ByteView::from(config.to_bytes(&actor).unwrap()),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn repair_topic(
    context: &Arc<DriverContext>,
    node_id: NodeId,
    realm_id: RealmId,
    target: &DocumentTarget,
) {
    for _ in 0..2 {
        let targets = prepare_core_documents(context, node_id, realm_id, true, false)
            .await
            .unwrap();
        assert!(targets.contains(target));
    }
    publish_core_documents(context, node_id, realm_id, true, vec![target.clone()])
        .await
        .unwrap();
    OutboxDrainer::new(context.clone()).run_once().await;
    let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    assert!(
        context
            .net_handle
            .as_ref()
            .unwrap()
            .sync_topic_exists(topic)
            .unwrap()
    );
}

async fn seed_topic(
    context: &Arc<DriverContext>,
    net: &NetHandle,
    realm_id: RealmId,
    node_id: NodeId,
    peer_id: NodeId,
) -> ::irokle::TopicId {
    let target = DocumentTarget::WatchInterest { realm_id, node_id };
    prepare_core_documents(context, node_id, realm_id, true, false)
        .await
        .unwrap();
    publish_core_documents(context, node_id, realm_id, true, vec![target.clone()])
        .await
        .unwrap();
    OutboxDrainer::new(context.clone()).run_once().await;
    let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    assert!(net.sync_topic_exists(topic).unwrap());
    assert_eq!(net.realm_peers().await, vec![peer_id]);
    net.reconcile_sync_topics(vec![topic]).await.unwrap();
    topic
}

#[tokio::test]
async fn first_boot_watch() {
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let (_dir, context, net) = net_context(realm_id, 7).await;
    let node_id = net.node_id();
    write_config(&context, realm_id, node_id).await;
    let targets = prepare_core_documents(&context, node_id, realm_id, true, false)
        .await
        .unwrap();
    let target = DocumentTarget::WatchInterest { realm_id, node_id };

    assert!(targets.contains(&target));
    publish_core_documents(&context, node_id, realm_id, true, vec![target.clone()])
        .await
        .unwrap();
    let batch = read_outbox_records(&context.storage_handle, &[], None, 8)
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert!(batch.records[0].1.allow_genesis);
    OutboxDrainer::new(Arc::new(context)).run_once().await;
    let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    assert!(net.sync_topic_exists(topic).unwrap());
    net.shutdown().await;
}

#[tokio::test]
async fn missing_topic_repair() {
    let realm_id = RealmId::from_bytes([8u8; 32]);
    let (_dir, context, net) = net_context(realm_id, 8).await;
    let node_id = net.node_id();
    write_config(&context, realm_id, node_id).await;
    write_digest(
        &context,
        realm_id,
        node_id,
        &WatchInterestDigest {
            node_id,
            entries: Vec::new(),
        },
    )
    .await;
    let target = DocumentTarget::WatchInterest { realm_id, node_id };
    let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    assert!(!net.sync_topic_exists(topic).unwrap());

    let targets = prepare_core_documents(&context, node_id, realm_id, true, false)
        .await
        .unwrap();

    assert!(targets.contains(&target));
    publish_core_documents(&context, node_id, realm_id, true, vec![target.clone()])
        .await
        .unwrap();
    let batch = read_outbox_records(&context.storage_handle, &[], None, 8)
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert!(batch.records[0].1.allow_genesis);
    OutboxDrainer::new(Arc::new(context)).run_once().await;
    assert!(net.sync_topic_exists(topic).unwrap());
    net.shutdown().await;
}

#[tokio::test]
async fn restart_stays_quiet() {
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let (_dir, context, net) = net_context(realm_id, 9).await;
    let node_id = net.node_id();
    let target = DocumentTarget::WatchInterest { realm_id, node_id };
    write_config(&context, realm_id, node_id).await;
    write_digest(
        &context,
        realm_id,
        node_id,
        &WatchInterestDigest {
            node_id,
            entries: Vec::new(),
        },
    )
    .await;
    let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    net.ensure_sync_topics(&[topic], Vec::new()).unwrap();

    for _ in 0..2 {
        let targets = prepare_core_documents(&context, node_id, realm_id, true, false)
            .await
            .unwrap();
        assert!(!targets.contains(&target));
        assert!(!publish_watch_interest(&context, node_id).await.unwrap());
        assert!(read_marker(&context, realm_id).await.is_none());
        let batch = read_outbox_records(&context.storage_handle, &[], None, 8)
            .await
            .unwrap();
        assert!(batch.records.is_empty());
    }
    net.shutdown().await;
}

#[tokio::test]
async fn joiner_announces_watch() {
    let realm_id = RealmId::from_bytes([10u8; 32]);
    let (_bootstrap_dir, bootstrap_context, bootstrap_net) = net_context(realm_id, 10).await;
    let (_joiner_dir, joiner_context, joiner_net) = net_context(realm_id, 11).await;
    let bootstrap_context = Arc::new(bootstrap_context);
    let joiner_context = Arc::new(joiner_context);
    initialize_incoming_fixture(bootstrap_context.clone());
    initialize_incoming_fixture(joiner_context.clone());
    let bootstrap_id = bootstrap_net.node_id();
    let joiner_id = joiner_net.node_id();
    write_config_nodes(
        &bootstrap_context,
        realm_id,
        bootstrap_id,
        &[bootstrap_id, joiner_id],
    )
    .await;
    write_config_nodes(
        &joiner_context,
        realm_id,
        joiner_id,
        &[bootstrap_id, joiner_id],
    )
    .await;
    bootstrap_net.reload_realm_peers().await.unwrap();
    joiner_net.reload_realm_peers().await.unwrap();
    bootstrap_net
        .add_peer_addr(joiner_net.endpoint_addr())
        .await;
    joiner_net
        .add_peer_addr(bootstrap_net.endpoint_addr())
        .await;

    let bootstrap_target = DocumentTarget::WatchInterest {
        realm_id,
        node_id: bootstrap_id,
    };
    let topic = seed_topic(
        &bootstrap_context,
        &bootstrap_net,
        realm_id,
        bootstrap_id,
        joiner_id,
    )
    .await;
    sync_peer_topic(
        &joiner_net,
        topic,
        bootstrap_id,
        &bootstrap_target,
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert!(joiner_net.sync_topic_exists(topic).unwrap());

    let target = DocumentTarget::WatchInterest {
        realm_id,
        node_id: joiner_id,
    };
    let targets = prepare_core_documents(&joiner_context, joiner_id, realm_id, false, false)
        .await
        .unwrap();

    assert!(targets.contains(&target));
    publish_core_documents(
        &joiner_context,
        joiner_id,
        realm_id,
        false,
        vec![target.clone()],
    )
    .await
    .unwrap();
    let batch = read_outbox_records(&joiner_context.storage_handle, &[], None, 8)
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert!(!batch.records[0].1.allow_genesis);
    OutboxDrainer::new(joiner_context.clone()).run_once().await;
    assert!(joiner_net.sync_topic_exists(topic).unwrap());
    bootstrap_net.shutdown().await;
    joiner_net.shutdown().await;
}

#[tokio::test]
async fn retries_until_seeded() {
    // Onboarding must survive the window before the peer serves the topic.
    let realm_id = RealmId::from_bytes([12u8; 32]);
    let (_bootstrap_dir, bootstrap_context, bootstrap_net) = net_context(realm_id, 12).await;
    let (_joiner_dir, joiner_context, joiner_net) = net_context(realm_id, 13).await;
    let bootstrap_context = Arc::new(bootstrap_context);
    let joiner_context = Arc::new(joiner_context);
    initialize_incoming_fixture(bootstrap_context.clone());
    initialize_incoming_fixture(joiner_context.clone());
    let bootstrap_id = bootstrap_net.node_id();
    let joiner_id = joiner_net.node_id();
    write_config_nodes(
        &bootstrap_context,
        realm_id,
        bootstrap_id,
        &[bootstrap_id, joiner_id],
    )
    .await;
    write_config_nodes(
        &joiner_context,
        realm_id,
        joiner_id,
        &[bootstrap_id, joiner_id],
    )
    .await;
    bootstrap_net.reload_realm_peers().await.unwrap();
    joiner_net.reload_realm_peers().await.unwrap();
    bootstrap_net
        .add_peer_addr(joiner_net.endpoint_addr())
        .await;
    joiner_net
        .add_peer_addr(bootstrap_net.endpoint_addr())
        .await;

    let target = DocumentTarget::WatchInterest {
        realm_id,
        node_id: bootstrap_id,
    };
    let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let (sync, seeded) = tokio::join!(
        sync_with_retry(
            &joiner_net,
            topic,
            bootstrap_id,
            &target,
            Duration::from_secs(60),
        ),
        seed_topic(
            &bootstrap_context,
            &bootstrap_net,
            realm_id,
            bootstrap_id,
            joiner_id,
        ),
    );
    assert_eq!(seeded, topic);
    sync.unwrap();
    assert!(joiner_net.sync_topic_exists(topic).unwrap());
    bootstrap_net.shutdown().await;
    joiner_net.shutdown().await;
}

#[tokio::test]
async fn publication_retries() {
    let realm_id = RealmId::from_bytes([11u8; 32]);
    let (_dir, context, net) = net_context(realm_id, 11).await;
    let context = Arc::new(context);
    let node_id = net.node_id();
    write_config(&context, realm_id, node_id).await;
    let target = DocumentTarget::WatchInterest { realm_id, node_id };
    let targets = prepare_core_documents(&context, node_id, realm_id, true, false)
        .await
        .unwrap();
    assert!(targets.contains(&target));
    assert!(read_marker(&context, realm_id).await.is_some());

    // A restart after the digest write must repair the missing shared topic.
    repair_topic(&context, node_id, realm_id, &target).await;
    let topic = target.sync_topic_id(realm_id, &PlacementRef::NIL);
    assert!(net.sync_topic_exists(topic).unwrap());

    write_digest(
        &context,
        realm_id,
        node_id,
        &WatchInterestDigest {
            node_id,
            entries: vec![WatchInterestEntry {
                path_prefix: "bucket/".to_string(),
                event_mask: WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            }],
        },
    )
    .await;
    assert!(read_marker(&context, realm_id).await.is_some());
    assert!(matches!(
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: ByteView::from(b"corrupt".to_vec()),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    assert!(publish_watch_interest(&context, node_id).await.is_err());
    assert!(read_marker(&context, realm_id).await.is_some());

    write_config(&context, realm_id, node_id).await;
    let restarted = Arc::new(context_net(context.storage_handle.clone(), net.clone()));
    let targets = prepare_core_documents(&restarted, node_id, realm_id, true, false)
        .await
        .unwrap();
    assert!(!targets.contains(&target));
    assert!(read_marker(&restarted, realm_id).await.is_some());
    publish_core_documents(&restarted, node_id, realm_id, false, vec![target.clone()])
        .await
        .unwrap();
    let batch = read_outbox_records(&restarted.storage_handle, &[], None, 8)
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert!(!batch.records[0].1.allow_genesis);
    let DocumentOutboxEvent::Upsert { bytes, .. } = &batch.records[0].1.event else {
        panic!("watch digest publication must enqueue an upsert")
    };
    assert_eq!(
        WatchInterestDigest::from_bytes(bytes)
            .unwrap()
            .entries
            .len(),
        1
    );
    OutboxDrainer::new(restarted.clone()).run_once().await;
    assert!(net.sync_topic_exists(topic).unwrap());
    assert!(publish_watch_interest(&restarted, node_id).await.unwrap());
    assert!(read_marker(&restarted, realm_id).await.is_none());

    net.shutdown().await;
}

#[test]
fn readiness_requires_all() {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());

    assert!(!node_is_ready(&config, node_id));
    config.ensure_node(node_id, RealmNodeKind::Server);
    assert!(!node_is_ready(&config, node_id));
    config.placement_map.push(NodePlacementEntry {
        node_id,
        location: String::new(),
        weight: 100,
        full: false,
        draining: false,
        labels: Default::default(),
    });
    config.seed_default_placement();
    config
        .placement_handle_ranges
        .push(aruna_core::structs::HandleRange {
            range_id: ulid::Ulid::from_bytes([3; 16]),
            owner: node_id,
            start: aruna_core::structs::FIRST_GRANTABLE_HANDLE,
            end: aruna_core::structs::FIRST_GRANTABLE_HANDLE
                + aruna_core::structs::HANDLE_RANGE_SIZE,
        });
    // A grant without its JobControl binding is not ready yet.
    assert!(!node_is_ready(&config, node_id));
    config
        .placement_bindings
        .push(aruna_core::structs::PlacementBinding {
            handle: aruna_core::structured_id::PlacementHandle::new(
                aruna_core::structs::FIRST_GRANTABLE_HANDLE,
            )
            .unwrap(),
            scope: aruna_core::structs::PlacementScope::Realm(realm_id),
            document_class: aruna_core::structs::DocumentClass::JobControl,
            strategy_id: config.default_strategy_id.unwrap(),
            allocator_range_id: Some(ulid::Ulid::from_bytes([3; 16])),
            allocated_by: Some(node_id),
            allocated_at_ms: Some(1),
        });
    assert!(node_is_ready(&config, node_id));
}

async fn read_digest(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: aruna_core::NodeId,
) -> WatchInterestDigest {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            key: interest_node_key(realm_id, node_id).into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => WatchInterestDigest::from_bytes(&bytes).unwrap(),
        other => panic!("unexpected digest read result: {other:?}"),
    }
}

#[tokio::test]
async fn initial_watch_digest() {
    let realm_id = RealmId::from_bytes([3u8; 32]);
    let (_dir, context, net) = net_context(realm_id, 4).await;
    let node_id = net.node_id();

    let targets = prepare_core_documents(&context, node_id, realm_id, true, true)
        .await
        .unwrap();

    assert!(targets.contains(&DocumentTarget::WatchInterest { realm_id, node_id }));
    assert_eq!(
        read_digest(&context, realm_id, node_id).await,
        WatchInterestDigest {
            node_id,
            entries: Vec::new(),
        }
    );
    net.shutdown().await;
}

#[tokio::test]
async fn existing_watch_digest() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = context(storage);
    let realm_id = RealmId::from_bytes([5u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
    let digest = WatchInterestDigest {
        node_id,
        entries: vec![WatchInterestEntry {
            path_prefix: "bucket/".to_string(),
            event_mask: WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        }],
    };
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            key: interest_node_key(realm_id, node_id).into(),
            value: ByteView::from(digest.to_bytes().unwrap()),
            txn_id: None,
        })
        .await;

    prepare_core_documents(&context, node_id, realm_id, false, true)
        .await
        .unwrap();

    assert_eq!(read_digest(&context, realm_id, node_id).await, digest);
}
