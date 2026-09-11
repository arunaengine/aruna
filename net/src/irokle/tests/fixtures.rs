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

pub(super) async fn write_registry_record(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
) {
    let event = storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes: metadata_registry_write_entries(record).expect("registry entries build"),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

pub(super) async fn read_storage_value(
    storage: &StorageHandle,
    key_space: &str,
    key: ByteView,
) -> Option<Value> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => panic!("unexpected storage read event: {other:?}"),
    }
}

/// Drops a topic's applied-ops cursor so the next reconcile replays it from
/// the start, whatever lineage the stored cursor carried.
pub(super) async fn reset_test_cursor(
    service: &DocumentSyncService,
    topic_id: irokle_crate::TopicId,
) {
    match service
        .storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
            key: topic_cursor_key(topic_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => {}
        other => panic!("unexpected cursor delete event: {other:?}"),
    }
}

pub(super) async fn read_test_cursor(
    storage: &StorageHandle,
    topic_id: irokle_crate::TopicId,
) -> Option<irokle_crate::ActorClock> {
    let bytes = read_storage_value(
        storage,
        DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
        topic_cursor_key(topic_id),
    )
    .await?;
    Some(
        postcard::from_bytes::<AppliedCursor>(&bytes)
            .expect("cursor decodes")
            .clock,
    )
}

pub(super) fn test_actor(seed: u8, user_id: UserId, realm_id: RealmId) -> Actor {
    Actor {
        node_id: node(seed),
        user_id,
        realm_id,
    }
}

pub(super) fn test_role(role_id: Ulid, assigned_users: impl IntoIterator<Item = UserId>) -> Role {
    Role {
        role_id,
        name: "member".to_string(),
        permissions: HashMap::from([("/datasets".to_string(), Permission::READ)]),
        assigned_users: assigned_users.into_iter().collect(),
    }
}

pub(super) fn test_admin_role_definition(
    role_id: Ulid,
    name: &str,
    path: &str,
    permission: Permission,
) -> AdminDocumentRoleDefinition {
    AdminDocumentRoleDefinition {
        role_id,
        name: name.to_string(),
        permissions: BTreeMap::from([(path.to_string(), permission)]),
    }
}

pub(super) fn admin_test_placement() -> PlacementRef {
    PlacementRef {
        strategy_id: Ulid::from_parts(9_990, 1),
        shard: 0,
    }
}

/// Signs an event as its origin. Test node keys are `[seed; 32]`, so the
/// origin's secret is recoverable from its public id.
pub(super) fn sign_as_origin(
    event: &AdminDocumentEvent,
    placement: &PlacementRef,
) -> iroh::Signature {
    (0u8..=255)
        .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]))
        .find(|key| key.public() == event.origin_node_id)
        .expect("test origin key")
        .sign(&event.signing_bytes(placement).expect("event serializes"))
}

pub(super) fn test_admin_event(
    event_id: Ulid,
    target: AdminDocumentTarget,
    actor: &Actor,
    origin_seq: u64,
    op: AdminDocumentOperation,
) -> AdminDocumentEvent {
    AdminDocumentEvent {
        event_id,
        target,
        origin_node_id: actor.node_id,
        origin_seq,
        observed: AdminDocumentClock::default(),
        actor: actor.clone(),
        op,
    }
}

pub(super) async fn read_user_doc(storage: &StorageHandle, user_id: UserId) -> User {
    let target = DocumentSyncTarget::User { user_id };
    let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
        .await
        .expect("user exists");
    User::from_bytes(&value).expect("user decodes")
}

pub(super) async fn read_group_doc(storage: &StorageHandle, group_id: Ulid) -> Group {
    let target = DocumentSyncTarget::Group { group_id };
    let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
        .await
        .expect("group exists");
    Group::from_bytes(&value).expect("group decodes")
}

pub(super) async fn read_group_auth_doc(
    storage: &StorageHandle,
    group_id: Ulid,
) -> GroupAuthorizationDocument {
    let target = DocumentSyncTarget::GroupAuthorization { group_id };
    let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
        .await
        .expect("group auth doc exists");
    GroupAuthorizationDocument::from_bytes(&value).expect("group auth doc decodes")
}

pub(super) async fn read_realm_auth_doc(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> RealmAuthorizationDocument {
    let target = DocumentSyncTarget::RealmAuthorization { realm_id };
    let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
        .await
        .expect("realm auth doc exists");
    RealmAuthorizationDocument::from_bytes(&value).expect("realm auth doc decodes")
}

pub(super) async fn read_realm_config_doc(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> RealmConfigDocument {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let value = read_storage_value(storage, target.storage_keyspace(), target.storage_key())
        .await
        .expect("realm config doc exists");
    RealmConfigDocument::from_bytes(&value).expect("realm config doc decodes")
}

pub(super) fn realm_config_nodes(config: &RealmConfigDocument) -> BTreeMap<String, RealmNodeKind> {
    config
        .nodes
        .iter()
        .map(|node| (node.node_id.clone(), node.kind.clone()))
        .collect()
}

pub(super) fn realm_config_oidc_providers(
    config: &RealmConfigDocument,
) -> BTreeMap<String, OidcProviderConfig> {
    config
        .oidc_providers
        .iter()
        .map(|provider| (provider.id.clone(), provider.clone()))
        .collect()
}

pub(super) fn test_oidc_provider(id: &str, issuer_suffix: &str) -> OidcProviderConfig {
    OidcProviderConfig {
        id: id.to_string(),
        issuer: format!("https://issuer.example/{issuer_suffix}"),
        audience: "aruna".to_string(),
        discovery_url: format!(
            "https://issuer.example/{issuer_suffix}/.well-known/openid-configuration"
        ),
    }
}

pub(super) fn test_discovery(node_seed: u8, endpoint_addr: &str) -> RealmDiscoveryConfig {
    RealmDiscoveryConfig::Static {
        endpoints: vec![StaticRealmEndpoint {
            node_id: node(node_seed).to_string(),
            endpoint_addr: endpoint_addr.to_string(),
        }],
    }
}

pub(super) async fn read_registry_record(
    storage: &StorageHandle,
    key_space: &str,
    key: ByteView,
) -> MetadataRegistryRecord {
    let value = read_storage_value(storage, key_space, key)
        .await
        .expect("registry record exists");
    postcard::from_bytes(&value).expect("registry record decodes")
}

pub(super) async fn read_graph_lifecycle_record(
    storage: &StorageHandle,
    graph_iri: &str,
) -> Option<MetadataGraphLifecycleRecord> {
    read_storage_value(
        storage,
        METADATA_GRAPH_LIFECYCLE_KEYSPACE,
        metadata_graph_lifecycle_key(graph_iri),
    )
    .await
    .map(|value| postcard::from_bytes(&value).expect("graph lifecycle record decodes"))
}

pub(super) async fn write_document_lifecycle_record(
    storage: &StorageHandle,
    lifecycle: &MetadataDocumentLifecycleRecord,
) {
    storage_batch_write_to(
        storage,
        vec![
            metadata_document_lifecycle_write_entry(lifecycle)
                .expect("document lifecycle entry builds"),
        ],
    )
    .await
    .expect("document lifecycle writes");
}

pub(super) async fn assert_registry_record_present(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
) {
    let primary = read_registry_record(
        storage,
        METADATA_INDEX_KEYSPACE,
        metadata_registry_key(record.group_id, record.document_id),
    )
    .await;
    let document_index = read_registry_record(
        storage,
        METADATA_DOCUMENT_INDEX_KEYSPACE,
        metadata_document_key(record.document_id),
    )
    .await;
    let holder_value = read_storage_value(
        storage,
        METADATA_HOLDERS_KEYSPACE,
        metadata_registry_key(record.group_id, record.document_id),
    )
    .await
    .expect("holder index exists");
    let holders: Vec<NodeId> = postcard::from_bytes(&holder_value).expect("holders decode");

    assert_eq!(primary, *record);
    assert_eq!(document_index, *record);
    assert_eq!(holders, record.holder_node_ids);
}
