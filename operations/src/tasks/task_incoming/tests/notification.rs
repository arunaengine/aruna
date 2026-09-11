use super::harness::*;
use super::super::*;
use super::*;

#[tokio::test]
async fn notification_drain_delivers_locally_when_self_is_holder() {
    let realm_id = RealmId::from_bytes([5u8; 32]);
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net_handle = make_net_handle(realm_id, &storage, [21u8; 32]).await;

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(net_handle.node_id(), RealmNodeKind::Server);
    write_realm_config(&storage, realm_id, &config, net_handle.node_id()).await;

    let record = notification_record(realm_id, 1_700_000_000_000);
    let outbox = new_notification_outbox_record(record.clone());
    write_notification_outbox(&storage, &outbox).await;

    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net_handle),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
    handler.drain_notification_outbox().await;

    let inbox = read_inbox_records(&storage).await;
    assert_eq!(inbox, vec![record]);
    let remaining = read_notification_outbox_batch(&storage, None, 1024, None)
        .await
        .expect("outbox read");
    assert!(remaining.records.is_empty());
}

#[tokio::test]
async fn notification_drain_retries_when_holder_unresolvable() {
    let realm_id = RealmId::from_bytes([6u8; 32]);
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net_handle = make_net_handle(realm_id, &storage, [22u8; 32]).await;

    let record = notification_record(realm_id, 1_700_000_000_000);
    let outbox = new_notification_outbox_record(record);
    write_notification_outbox(&storage, &outbox).await;

    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net_handle),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
    handler.drain_notification_outbox().await;

    let remaining = read_notification_outbox_batch(&storage, None, 1024, None)
        .await
        .expect("outbox read");
    assert_eq!(remaining.records.len(), 1);
    assert!(read_inbox_records(&storage).await.is_empty());

    let Event::Task(TaskEvent::TimerScheduled { after, .. }) = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DrainNotificationOutbox,
            after: Duration::from_secs(10_000),
        }))
        .await
    else {
        panic!("expected timer scheduled");
    };
    assert!(
        after <= NOTIFICATION_DELIVERY_RETRY_AFTER,
        "an unresolvable holder must re-arm the retry timer"
    );
}

#[tokio::test]
async fn notification_drain_delivers_to_remote_holder() {
    let realm_id = RealmId::from_bytes([8u8; 32]);

    let dir_a = tempdir().expect("temp dir");
    let storage_a =
        FjallStorage::open(dir_a.path().to_str().expect("temp path")).expect("storage opens");
    let net_a = make_net_handle(realm_id, &storage_a, [24u8; 32]).await;

    let dir_b = tempdir().expect("temp dir");
    let storage_b =
        FjallStorage::open(dir_b.path().to_str().expect("temp path")).expect("storage opens");
    let net_b = make_net_handle(realm_id, &storage_b, [25u8; 32]).await;

    net_a.add_peer_addr(net_b.endpoint_addr()).await;

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(net_a.node_id(), RealmNodeKind::Server);
    config.ensure_node(net_b.node_id(), RealmNodeKind::Server);
    write_realm_config(&storage_a, realm_id, &config, net_a.node_id()).await;
    write_realm_config(&storage_b, realm_id, &config, net_b.node_id()).await;

    let b_id = net_b.node_id();
    let recipient = loop {
        let candidate = UserId::new(Ulid::generate(), realm_id);
        if resolve_inbox_holder(&candidate, &config).expect("resolve holder") == Some(b_id) {
            break candidate;
        }
    };

    let record = NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::AddedToGroup {
            group_id: Ulid::from_bytes([1u8; 16]),
            actor_user_id: recipient,
        },
        1_700_000_000_000,
    );
    let outbox = new_notification_outbox_record(record.clone());
    write_notification_outbox(&storage_a, &outbox).await;

    let context_b = Arc::new(DriverContext {
        storage_handle: storage_b.clone(),
        net_handle: Some(net_b),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    crate::sync::incoming::initialize_net_incoming(context_b.clone());

    let context_a = Arc::new(DriverContext {
        storage_handle: storage_a.clone(),
        net_handle: Some(net_a),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context_a, JobsRuntime::new());
    handler.drain_notification_outbox().await;

    assert_eq!(read_inbox_records(&storage_b).await, vec![record]);
    let remaining = read_notification_outbox_batch(&storage_a, None, 1024, None)
        .await
        .expect("outbox read");
    assert!(remaining.records.is_empty());
}

#[tokio::test]
async fn notification_drain_drops_expired_records_with_warn() {
    let realm_id = RealmId::from_bytes([7u8; 32]);
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net_handle = make_net_handle(realm_id, &storage, [23u8; 32]).await;

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(net_handle.node_id(), RealmNodeKind::Server);
    write_realm_config(&storage, realm_id, &config, net_handle.node_id()).await;

    let outbox = NotificationOutboxRecord {
        outbox_id: Ulid::from_parts(1, 0),
        record: notification_record(realm_id, 1_000),
    };
    write_notification_outbox(&storage, &outbox).await;

    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net_handle),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let handler = OperationsTaskHandler::new(context, JobsRuntime::new());
    handler.drain_notification_outbox().await;

    assert!(read_inbox_records(&storage).await.is_empty());
    let remaining = read_notification_outbox_batch(&storage, None, 1024, None)
        .await
        .expect("outbox read");
    assert!(remaining.records.is_empty());
}

async fn write_notification_outbox(
    storage: &aruna_storage::StorageHandle,
    record: &NotificationOutboxRecord,
) {
    let (key_space, key, value) = notification_outbox_write_entry(record).expect("outbox entry");
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected outbox write event: {other:?}"),
    }
}

async fn read_inbox_records(storage: &aruna_storage::StorageHandle) -> Vec<NotificationRecord> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1024,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| NotificationRecord::from_bytes(&value).expect("record decodes"))
            .collect(),
        other => panic!("unexpected inbox iter event: {other:?}"),
    }
}

fn notification_recipient(realm_id: RealmId) -> UserId {
    UserId::new(Ulid::from_bytes([9u8; 16]), realm_id)
}

fn notification_record(realm_id: RealmId, created_at_ms: u64) -> NotificationRecord {
    let recipient = notification_recipient(realm_id);
    NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::AddedToGroup {
            group_id: Ulid::from_bytes([1u8; 16]),
            actor_user_id: recipient,
        },
        created_at_ms,
    )
}

#[tokio::test]
async fn device_skips_holders() {
    // A device publishes no holder record, so the refresh does not even arm
    // its own timer.
    let realm_id = RealmId::from_bytes([57u8; 32]);
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(
        net.node_id(),
        RealmNodeKind::User {
            owner: UserId::nil(realm_id),
        },
    );
    let actor = Actor {
        node_id: net.node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    match storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*realm_id.as_bytes()).into(),
            value: config.to_bytes(&actor).expect("config bytes").into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected realm config write: {other:?}"),
    }

    OperationsTaskHandler::new(context, JobsRuntime::new())
        .refresh_blob_holders()
        .await;

    let timer = storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: ByteView::from(
                postcard::to_allocvec(&TaskKey::RefreshBlobHolders).expect("timer key"),
            ),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        timer,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ));

    net.shutdown().await;
}
