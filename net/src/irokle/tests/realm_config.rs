use super::*;

#[tokio::test]
async fn realm_config_node_op_alone_stores_reducer_state_without_config_doc() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([40; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_290, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let reducer_node = node(10);

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_291, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: reducer_node,
                kind: RealmNodeKind::Management,
            },
        ),
    )
    .await
    .expect("realm config node ensure applies without config doc");

    assert!(
        read_storage_value(
            &storage,
            document_target.storage_keyspace(),
            document_target.storage_key(),
        )
        .await
        .is_none()
    );
    let state_value = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&target),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&state_value).expect("reducer state decodes");
    assert_eq!(
        reducer_state.materialized_realm_config_nodes()[&reducer_node],
        RealmNodeKind::Management
    );
}

#[tokio::test]
async fn realm_config_settings_admin_op_materializes_existing_config() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([48; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_370, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let existing_provider = test_oidc_provider("existing", "existing-settings");
    let seed_node = node(20);
    let mut seed_config = RealmConfigDocument::new(realm_id, vec![existing_provider.clone()], 3);
    seed_config.discovery = test_discovery(21, "https://existing-settings.example:443");
    seed_config.ensure_node(seed_node, RealmNodeKind::Server);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            document_target.clone(),
            seed_config
                .to_bytes(&actor)
                .expect("seed realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("seed realm config writes");

    let metadata_replication = MetadataReplicationConfig::new(9);
    let discovery = test_discovery(22, "https://reducer-settings.example:443");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_371, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: metadata_replication.clone(),
                discovery: discovery.clone(),
            },
        ),
    )
    .await
    .expect("realm config settings apply");

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(config.metadata_replication, metadata_replication);
    assert_eq!(config.discovery, discovery);
    assert_eq!(config.oidc_providers, vec![existing_provider]);
    assert_eq!(
        realm_config_nodes(&config),
        realm_config_nodes(&seed_config)
    );
    let state_value = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&target),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&state_value).expect("reducer state decodes");
    assert_eq!(
        reducer_state.materialized_realm_config_metadata_replication(),
        Some(metadata_replication)
    );
    assert_eq!(
        reducer_state.materialized_realm_config_discovery(),
        Some(discovery)
    );
}

#[tokio::test]
async fn realm_config_description_admin_op_materializes_existing_config() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([53; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_415, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let mut seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    seed_config.description = "Old Realm".to_string();
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            document_target.clone(),
            seed_config
                .to_bytes(&actor)
                .expect("seed realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("seed realm config writes");

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_416, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "Replicated Realm".to_string(),
            },
        ),
    )
    .await
    .expect("realm config description applies");

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(config.description, "Replicated Realm");
    let state_value = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&target),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&state_value).expect("reducer state decodes");
    assert_eq!(
        reducer_state
            .materialized_realm_config_description()
            .as_deref(),
        Some("Replicated Realm")
    );
}
