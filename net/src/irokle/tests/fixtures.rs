use super::*;

pub(super) fn topic(seed: u8) -> irokle_crate::TopicId {
    DocumentSyncTarget::RealmConfig {
        realm_id: RealmId::from_bytes([seed; 32]),
    }
    .sync_topic_id(RealmId::from_bytes([seed; 32]), &PlacementRef::NIL)
}

pub(super) fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(super) fn test_genesis(seed: u8) -> irokle_crate::OpId {
    irokle_crate::OpId::from_bytes([seed; 32])
}

pub(super) fn test_storage() -> (TempDir, StorageHandle) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().expect("temp path"))
        .expect("storage opens");
    (dir, storage)
}

pub(super) fn storage_at(path: &Path) -> StorageHandle {
    aruna_storage::FjallStorage::open(path.to_str().expect("utf-8 storage path"))
        .expect("storage opens")
}

pub(super) fn restart_target() -> DocumentSyncTarget {
    DocumentSyncTarget::MetadataGraphLifecycle {
        graph_iri: "urn:aruna:restart-contract".to_string(),
    }
}

pub(super) fn restart_realm() -> RealmId {
    RealmId::from_bytes([99; 32])
}

pub(super) fn restart_placement() -> PlacementRef {
    PlacementRef {
        strategy_id: Ulid::from_parts(99, 7),
        shard: 11,
    }
}

pub(super) fn restart_topic() -> irokle_crate::TopicId {
    restart_target().sync_topic_id(restart_realm(), &restart_placement())
}

pub(super) fn restart_event_id() -> Ulid {
    Ulid::from_parts(1_727_000_000_000, 42)
}

pub(super) fn restart_payload() -> Vec<u8> {
    postcard::to_allocvec(&MetadataGraphLifecycleRecord::deleted(
        "urn:aruna:restart-contract".to_string(),
        RealmId::from_bytes([99; 32]),
        Ulid::from_parts(99, 1),
        Ulid::from_parts(99, 2),
        1,
    ))
    .expect("restart payload serializes")
}

pub(super) fn revision_change() -> DocumentSyncChange {
    DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: restart_event_id(),
            actor: node(43),
            updated_at_ms: 1_727_000_000_101,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement: restart_placement(),
    }
}

pub(super) async fn restart_endpoint() -> iroh::Endpoint {
    test_endpoint(91).await
}

pub(super) async fn open_restart_service(root: &Path, storage_name: &str) -> DocumentSyncService {
    DocumentSyncService::open_with_persist_policy(
        restart_endpoint().await,
        storage_at(&root.join(storage_name)),
        root.join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        restart_realm(),
    )
    .expect("document sync service opens")
}

pub(super) fn run_document_sync_restart_child(root: &Path) {
    let status = Command::new(env::current_exe().expect("test binary path"))
        .arg(DOCUMENT_SYNC_RESTART_CHILD_TEST)
        .arg("--exact")
        .arg("--nocapture")
        .env(DOCUMENT_SYNC_RESTART_CHILD_PATH_ENV, root)
        .status()
        .expect("restart child process should run");

    assert!(status.success(), "restart child process failed: {status}");
}
