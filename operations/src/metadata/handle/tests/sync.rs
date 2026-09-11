use super::auth::{auth_storage, node_id_from_seed, realm_fixture};
use super::*;
#[test]
fn workspace_delete_allowed() {
    let (_, realm_id, user_id) = realm_fixture();
    let relationship = SyncRelationship {
        id: Ulid::generate(),
        source: ArunaArn::s3_bucket(realm_id, node_id_from_seed(1), "ws-temporary").unwrap(),
        target: ArunaArn::s3_bucket(realm_id, node_id_from_seed(2), "target").unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: user_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    };

    assert!(valid_sync_request(
        &relationship,
        "ws-temporary",
        "target",
        realm_id,
        user_id,
        true,
    ));
    assert!(!valid_sync_request(
        &relationship,
        "ws-temporary",
        "target",
        realm_id,
        user_id,
        false,
    ));
    assert!(!valid_sync_request(
        &relationship,
        "ws-temporary",
        "target",
        realm_id,
        UserId::local(Ulid::generate(), realm_id),
        true,
    ));
}
#[tokio::test]
async fn sync_creates_bucket() {
    let tempdir = tempdir().unwrap();
    let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    let (_, realm_id, user_id) = realm_fixture();
    let group_id = Ulid::generate();
    let relationship = SyncRelationship {
        id: Ulid::generate(),
        source: ArunaArn::s3_bucket(realm_id, node_id_from_seed(1), "source").unwrap(),
        target: ArunaArn::s3_bucket(realm_id, node_id_from_seed(2), "foobar").unwrap(),
        mode: SyncMode::Once,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: false,
        created_by: user_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    };

    create_sync_bucket(&context, "foobar", group_id, &relationship)
        .await
        .unwrap();
    let bucket = drive(GetBucketInfoOperation::new("foobar".to_string()), &context)
        .await
        .unwrap()
        .transpose()
        .unwrap()
        .unwrap();

    assert_eq!(bucket.group_id, group_id);
    assert_eq!(bucket.created_by, user_id);
}
#[tokio::test(start_paused = true)]
async fn sync_timeout_fires() {
    assert!(SYNC_MIRROR_REQUEST_TIMEOUT < RECONCILE_GRACE);
    let result =
        with_sync_timeout(std::future::pending::<Result<(), MetadataRequestError>>()).await;

    assert!(matches!(
        result,
        Err(MetadataError::Backend(message))
            if message == "sync mirror request timed out"
    ));
}
async fn assert_timeout(frame: &[u8], held: bool) {
    let (mut writer, reader) = tokio::io::duplex(16);
    writer.write_all(frame).await.unwrap();
    let budget = Arc::new(tokio::sync::Semaphore::new(8));
    let task_budget = budget.clone();
    let task = tokio::spawn(async move {
        let mut reader = reader;
        read_budget(&mut reader, &task_budget).await
    });
    tokio::task::yield_now().await;
    assert_eq!(budget.available_permits(), if held { 0 } else { 8 });

    tokio::time::advance(METADATA_IO_TIMEOUT).await;
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        MetadataError::Backend(message)
            if message == "timed out waiting for metadata message"
    ));
    assert_eq!(budget.available_permits(), 8);
}
#[tokio::test(start_paused = true)]
async fn header_timeout() {
    assert_timeout(&[0], false).await;
}
#[tokio::test(start_paused = true)]
async fn body_timeout() {
    assert_timeout(&[0, 0, 0, 0, 8], true).await;
}
#[test]
fn metadata_handle_options_default_to_buffered_document_sync_persist() {
    let options = MetadataHandleOptions::default();

    assert_eq!(
        options.document_sync_persist_policy,
        FjallPersistPolicy::Buffer
    );
}
#[test]
fn metadata_handle_options_can_set_document_sync_persist_policy() {
    let options = MetadataHandleOptions::default()
        .with_search_storage(MetadataSearchStorage::Memory)
        .with_document_sync_persist_policy(FjallPersistPolicy::SyncAll);

    assert_eq!(options.search_storage, MetadataSearchStorage::Memory);
    assert_eq!(
        options.document_sync_persist_policy,
        FjallPersistPolicy::SyncAll
    );
}
#[tokio::test]
async fn flush_persistence_succeeds_without_document_sync_database() {
    let (_storage_dir, storage) = auth_storage();
    let metadata_dir = tempdir().expect("metadata dir");
    let metadata_handle = MetadataHandle::new_with_options(
        metadata_dir.path(),
        node_id_from_seed(1),
        storage,
        None,
        None,
        None,
        MetadataHandleOptions::default().with_search_storage(MetadataSearchStorage::Memory),
    )
    .expect("metadata handle opens");

    assert_eq!(
        metadata_handle.inner.node.graph_store_persist_mode(),
        CraqleFjallPersistMode::Buffer
    );

    metadata_handle
        .flush_persistence()
        .await
        .expect("metadata persistence flushes");
}
