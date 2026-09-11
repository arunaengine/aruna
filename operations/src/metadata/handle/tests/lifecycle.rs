use super::auth::{auth_storage, node_id_from_seed};
use super::effect::memory_handle;
use super::visibility::{group_record, registry_record};
use super::*;
async fn store_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected batch write result: {other:?}"),
    }
}
fn registry_entries(record: &MetadataRegistryRecord) -> Vec<(String, ByteView, ByteView)> {
    aruna_core::storage_entries::metadata_registry_write_entries(record)
        .expect("registry entries encode")
}
#[tokio::test]
async fn group_records_live() {
    let (_storage_dir, storage) = auth_storage();
    let group_id = Ulid::generate();
    let live = group_record(group_id, "datasets/live");
    let gone = group_record(group_id, "datasets/gone");
    let tombstone = MetadataGraphLifecycleRecord::deleted(
        gone.graph_iri.clone(),
        gone.realm_id,
        gone.group_id,
        gone.document_id,
        2,
    );
    let mut writes = registry_entries(&live);
    writes.extend(registry_entries(&gone));
    writes.push(
        aruna_core::storage_entries::metadata_graph_lifecycle_write_entry(&tombstone)
            .expect("tombstone encodes"),
    );
    store_entries(&storage, writes).await;
    let (_metadata_dir, handle) = memory_handle(storage);

    let records = handle
        .list_group_records(group_id, 16)
        .await
        .expect("group listing succeeds");

    assert_eq!(
        records
            .iter()
            .map(|record| record.document_id)
            .collect::<Vec<_>>(),
        vec![live.document_id]
    );
}
#[tokio::test]
async fn group_records_capped() {
    // The scan must refuse to answer past its candidate budget instead of
    // returning a silently truncated group listing.
    let (_storage_dir, storage) = auth_storage();
    let group_id = Ulid::generate();
    let mut writes = registry_entries(&group_record(group_id, "datasets/one"));
    writes.extend(registry_entries(&group_record(group_id, "datasets/two")));
    store_entries(&storage, writes).await;
    let (_metadata_dir, handle) = memory_handle(storage);

    let within = handle
        .list_group_records(group_id, 2)
        .await
        .expect("group listing succeeds");
    let over = handle.list_group_records(group_id, 1).await;

    assert_eq!(within.len(), 2);
    assert!(matches!(
        over,
        Err(MetadataError::Backend(message)) if message.contains("candidate limit exceeded")
    ));
}
#[tokio::test]
async fn tombstone_blocks_apply() {
    let (_storage_dir, storage) = auth_storage();
    let metadata_dir = tempdir().expect("metadata dir");
    let metadata_handle = MetadataHandle::new_with_options(
        metadata_dir.path(),
        node_id_from_seed(3),
        storage.clone(),
        None,
        None,
        None,
        MetadataHandleOptions::default().with_search_storage(MetadataSearchStorage::Memory),
    )
    .expect("metadata handle opens");
    let record = registry_record("datasets/fenced");
    let held = metadata_graph_fence(&record.graph_iri)
        .acquire()
        .await
        .expect("graph fence remains open");
    let graph_iri = record.graph_iri.clone();
    let task_handle = metadata_handle.clone();
    let task = tokio::spawn(async move {
        task_handle
            .send_metadata_effect(MetadataEffect::CreateCrate {
                request: MetadataCreateCrateRequest {
                    graph_iri,
                    name: "fenced".to_string(),
                    description: "fenced".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: None,
                    policy: MetadataGraphPolicy {
                        public: true,
                        permission_paths: Vec::new(),
                    },
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor: None,
                },
            })
            .await
    });
    tokio::task::yield_now().await;

    let tombstone = MetadataGraphLifecycleRecord::deleted(
        record.graph_iri.clone(),
        record.realm_id,
        record.group_id,
        record.document_id,
        2,
    );
    let bytes = postcard::to_allocvec(&tombstone).expect("tombstone serializes");
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_graph_lifecycle_key(&record.graph_iri),
            value: ByteView::from(bytes),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected lifecycle write result: {other:?}"),
    }
    drop(held);

    let event = task.await.expect("materialization task joins");
    assert!(matches!(
        event,
        Event::Metadata(MetadataEvent::Error {
            error: MetadataError::InvalidInput(message),
            ..
        }) if message.contains("deleted")
    ));
    assert!(
        !metadata_handle
            .inner
            .node
            .contains_graph(&GraphId::new(&record.graph_iri))
            .expect("graph probe succeeds")
    );
}
#[tokio::test]
async fn flush_persistence_succeeds_with_configured_document_sync_database() {
    let (_storage_dir, storage) = auth_storage();
    let metadata_dir = tempdir().expect("metadata dir");
    let document_sync_dir = tempdir().expect("document sync dir");
    let document_sync_db = fjall::OptimisticTxDatabase::builder(
        document_sync_dir
            .path()
            .to_str()
            .expect("document sync path"),
    )
    .manual_journal_persist(true)
    .open()
    .expect("document sync db opens");
    let metadata_handle = MetadataHandle::new_with_options(
        metadata_dir.path(),
        node_id_from_seed(2),
        storage,
        None,
        None,
        Some(document_sync_db),
        MetadataHandleOptions::default()
            .with_search_storage(MetadataSearchStorage::Memory)
            .with_document_sync_persist_policy(FjallPersistPolicy::SyncAll),
    )
    .expect("metadata handle opens");

    assert_eq!(
        metadata_handle.inner.node.graph_store_persist_mode(),
        CraqleFjallPersistMode::SyncAll
    );

    metadata_handle
        .flush_persistence()
        .await
        .expect("metadata and document sync persistence flush");
}
