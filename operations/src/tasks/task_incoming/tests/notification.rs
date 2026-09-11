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
