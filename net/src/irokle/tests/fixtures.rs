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

pub(super) async fn assert_registry_record_deleted(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
) {
    assert!(
        read_storage_value(
            storage,
            METADATA_INDEX_KEYSPACE,
            metadata_registry_key(group_id, document_id),
        )
        .await
        .is_none()
    );
    assert!(
        read_storage_value(
            storage,
            METADATA_DOCUMENT_INDEX_KEYSPACE,
            metadata_document_key(document_id),
        )
        .await
        .is_none()
    );
    assert!(
        read_storage_value(
            storage,
            METADATA_HOLDERS_KEYSPACE,
            metadata_registry_key(group_id, document_id),
        )
        .await
        .is_none()
    );
}

pub(super) fn metadata_create_event(
    group_id: Ulid,
    document_id: Ulid,
    updated_at_ms: u64,
    event_id: Ulid,
    actor_seed: u8,
) -> MetadataCreateEventRecord {
    let realm_id = RealmId::from_bytes([42; 32]);
    MetadataCreateEventRecord {
        event_id,
        record: registry_record(
            group_id,
            document_id,
            "datasets/lifecycle",
            updated_at_ms,
            event_id,
        ),
        user_id: UserId::local(Ulid::from_parts(90, 1), realm_id),
        node_id: node(actor_seed),
        payload: MetadataCreateEventPayload::Scaffold {
            name: "Lifecycle".to_string(),
            description: "Lifecycle event".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
        },
        occurred_at_ms: updated_at_ms,
    }
}

pub(super) fn metadata_delete_lifecycle(
    group_id: Ulid,
    document_id: Ulid,
    updated_at_ms: u64,
    event_id: Ulid,
    deleted_after_event_id: Ulid,
) -> MetadataDocumentLifecycleRecord {
    let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
    MetadataDocumentLifecycleRecord::Delete {
        event: MetadataDocumentDeleteRecord {
            event_id,
            tombstone: MetadataGraphLifecycleRecord::deleted(
                graph_iri,
                RealmId::from_bytes([42; 32]),
                group_id,
                document_id,
                updated_at_ms,
            ),
            deleted_after_event_id,
        },
    }
}

pub(super) fn metadata_lifecycle_change(
    lifecycle: &MetadataDocumentLifecycleRecord,
    actor: NodeId,
) -> DocumentSyncChange {
    aruna_core::storage_entries::metadata_document_lifecycle_revision_change(
        lifecycle,
        actor,
        aruna_core::structs::PlacementRef::NIL,
    )
}

pub(super) fn registry_record(
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
    updated_at_ms: u64,
    last_event_id: Ulid,
) -> MetadataRegistryRecord {
    let realm_id = RealmId::from_bytes([42; 32]);
    MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: document_path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            document_path,
            document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: vec![node(1)],
        created_at_ms: 1,
        updated_at_ms,
        establishing_event_id: last_event_id,
        last_event_id,
    }
}

pub(super) fn peer(seed: u8) -> PeerId {
    node_id_to_peer_id(&iroh::SecretKey::from_bytes(&[seed; 32]).public())
}

pub(super) async fn apply_conflicting_user_name_and_attribute(
    storage: &StorageHandle,
    user_id: UserId,
    realm_id: RealmId,
) -> Actor {
    let actor_a = test_actor(8, user_id, realm_id);
    let actor_b = test_actor(9, user_id, realm_id);
    let target = AdminDocumentTarget::User { user_id };
    for (seq, actor, origin_seq, op) in [
        (
            1,
            &actor_a,
            1,
            AdminDocumentOperation::UserNameSet {
                name: "Alice".to_string(),
            },
        ),
        (
            2,
            &actor_b,
            1,
            AdminDocumentOperation::UserNameSet {
                name: "Mallory".to_string(),
            },
        ),
        (
            3,
            &actor_a,
            2,
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "physics".to_string(),
            },
        ),
        (
            4,
            &actor_b,
            2,
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "malware".to_string(),
            },
        ),
    ] {
        apply_admin_document_operation_to_storage(
            storage,
            DocumentSyncTarget::User { user_id },
            test_admin_event(
                Ulid::from_parts(2_500 + seq, 1),
                target.clone(),
                actor,
                origin_seq,
                op,
            ),
        )
        .await
        .expect("conflicting user admin operation applies");
    }
    actor_a
}

pub(super) async fn read_document_lifecycle_record(
    storage: &StorageHandle,
    document_id: Ulid,
) -> MetadataDocumentLifecycleRecord {
    let value = read_storage_value(
        storage,
        METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
        metadata_document_lifecycle_key(document_id),
    )
    .await
    .expect("lifecycle record exists");
    postcard::from_bytes(&value).expect("lifecycle record decodes")
}

pub(super) async fn read_lifecycle_revision(
    storage: &StorageHandle,
    document_id: Ulid,
) -> DocumentSyncChange {
    let target = DocumentSyncTarget::MetadataDocumentLifecycle { document_id };
    let value = read_storage_value(
        storage,
        DOCUMENT_SYNC_REVISION_KEYSPACE,
        document_sync_revision_key(&target),
    )
    .await
    .expect("lifecycle revision exists");
    postcard::from_bytes(&value).expect("lifecycle revision decodes")
}

/// The user every policy fixture publishes under.
pub(super) fn policy_admin(realm_id: RealmId) -> UserId {
    UserId::local(Ulid::from_bytes([4u8; 16]), realm_id)
}

/// One authentic publication of `policy` by node `seed`.
pub(super) fn signed_policy_document(
    realm_id: RealmId,
    policy: &aruna_core::structs::VerifiedPolicy,
    seed: u8,
) -> PlacementPolicyDocument {
    let secret = iroh::SecretKey::from_bytes(&[seed; 32]);
    let publication = aruna_core::structs::PolicyPublicationClaim::new(
        realm_id,
        policy,
        secret.public(),
        policy_admin(realm_id),
        Ulid::from_bytes([5u8; 16]),
        9,
        [0u8; 32],
    )
    .sign(&secret);
    PlacementPolicyDocument::new(realm_id, policy, publication)
}

/// Realm view a policy publication is verified against: the publisher is a
/// server node and the admin user holds realm-configuration write.
pub(super) fn policy_realm_view(
    realm_id: RealmId,
) -> (RealmConfigDocument, RealmAuthorizationDocument) {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    config.seed_default_placement();
    for seed in 1..=4u8 {
        config.ensure_node(node(seed), RealmNodeKind::Server);
    }
    let role = Role {
        role_id: Ulid::from_bytes([1u8; 16]),
        name: "realm_admin".to_string(),
        permissions: HashMap::from([(
            format!("/{realm_id}/admin/**"),
            aruna_core::structs::Permission::WRITE,
        )]),
        assigned_users: HashSet::from([policy_admin(realm_id)]),
    };
    let auth = RealmAuthorizationDocument {
        realm_id,
        roles: HashMap::from([(role.role_id, role)]),
        operation_restrictions: HashMap::new(),
    };
    (config, auth)
}

pub(super) async fn write_realm_view(
    storage: &StorageHandle,
    config: &RealmConfigDocument,
    auth: &RealmAuthorizationDocument,
) {
    let actor = aruna_core::structs::Actor {
        node_id: node(1),
        user_id: policy_admin(config.realm_id),
        realm_id: config.realm_id,
    };
    let config_target = DocumentSyncTarget::RealmConfig {
        realm_id: config.realm_id,
    };
    let auth_target = DocumentSyncTarget::RealmAuthorization {
        realm_id: config.realm_id,
    };
    let writes = vec![
        (
            config_target.storage_keyspace().to_string(),
            config_target.storage_key(),
            Value::from(config.to_bytes(&actor).expect("config encodes")),
        ),
        (
            auth_target.storage_keyspace().to_string(),
            auth_target.storage_key(),
            Value::from(auth.to_bytes(&actor).expect("authorization encodes")),
        ),
    ];
    storage_batch_write_to(storage, writes)
        .await
        .expect("realm view is stored");
}

pub(super) fn policy_fixture(policy_id: Ulid) -> aruna_core::structs::VerifiedPolicy {
    use aruna_core::structs::{PlacementPolicy, PlacementSelector, VerifiedPolicy};

    let policy = PlacementPolicy::new(
        policy_id,
        "residency".to_string(),
        vec![PlacementSelector {
            node_id: None,
            location: Some("eu-west".to_string()),
            labels: Vec::new(),
            executor_kind: None,
        }],
    )
    .expect("policy is valid");
    VerifiedPolicy::verify(policy).expect("policy verifies")
}

pub(super) async fn policy_service(
    realm_id: RealmId,
    storage: StorageHandle,
    root: &Path,
) -> DocumentSyncService {
    DocumentSyncService::open_with_persist_policy(
        test_endpoint(31).await,
        storage,
        root.join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        irokle_crate::net::IrohRuntimeConfig::default(),
        FjallPersistPolicy::Buffer,
        realm_id,
    )
    .expect("document sync service opens")
}

pub(super) async fn quarantine_rows(storage: &StorageHandle) -> Vec<SyncQuarantineRecord> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: SYNC_QUARANTINE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 256,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| {
                SyncQuarantineRecord::from_bytes(value.as_ref()).expect("record decodes")
            })
            .collect(),
        other => panic!("unexpected storage iteration event: {other:?}"),
    }
}

pub(super) async fn quarantine_usage(storage: &StorageHandle) -> SyncQuarantineUsage {
    match read_storage_value(
        storage,
        SYNC_QUARANTINE_USAGE_KEYSPACE,
        ByteView::from(SYNC_QUARANTINE_USAGE_KEY),
    )
    .await
    {
        Some(bytes) => SyncQuarantineUsage::from_bytes(bytes.as_ref()).expect("usage decodes"),
        None => SyncQuarantineUsage::default(),
    }
}

pub(super) async fn write_usage(storage: &StorageHandle, usage: SyncQuarantineUsage) {
    storage_batch_write_to(
        storage,
        vec![(
            SYNC_QUARANTINE_USAGE_KEYSPACE.to_string(),
            ByteView::from(SYNC_QUARANTINE_USAGE_KEY),
            ByteView::from(usage.to_bytes().expect("usage serializes")),
        )],
    )
    .await
    .expect("usage row writes");
}

pub(super) async fn cursor_advanced(
    service: &DocumentSyncService,
    storage: &StorageHandle,
    topic_id: irokle_crate::TopicId,
) -> bool {
    let Some(cursor) = read_test_cursor(storage, topic_id).await else {
        return false;
    };
    let topic_clock = service
        .node()
        .storage()
        .actor_clock(&topic_id)
        .expect("topic clock");
    cursor.dominates(&topic_clock)
}

pub(super) fn quarantined_reason(records: &[SyncQuarantineRecord], event_id: Ulid) -> String {
    records
        .iter()
        .find(|record| record.event_id() == Some(event_id))
        .unwrap_or_else(|| panic!("event {event_id} is quarantined"))
        .reason
        .clone()
}

pub(super) fn node_info_bytes(node_id: NodeId, updated_at_ms: u64) -> Vec<u8> {
    use aruna_core::structs::{AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization};

    NodeInfoDocument {
        node_id,
        executors: Vec::new(),
        labels: BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 1,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: updated_at_ms,
        },
        updated_at_ms,
        epoch: AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation: updated_at_ms,
            observed_at_ms: updated_at_ms,
        },
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    }
    .to_bytes()
    .expect("node info serializes")
}
