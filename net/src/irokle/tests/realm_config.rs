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

#[tokio::test]
async fn realm_config_placement_admin_ops_materialize_existing_config() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([71; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_500, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
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

    let entry = NodePlacementEntry {
        node_id: actor.node_id,
        location: "eu-west".to_string(),
        weight: 250,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    };
    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_parts(1_501, 1),
        name: "default".to_string(),
        replica_count: Some(3),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    };
    let binding = StrategyBinding {
        scope: BindingScope::Class(DocumentClass::MetadataRegistry),
        strategy_id: strategy.strategy_id,
    };
    let record = PlacementOverride {
        subject: b"document-subject".to_vec(),
        pinned: vec![actor.node_id],
        excluded: Vec::new(),
        strategy_id: Some(strategy.strategy_id),
    };

    for (index, op) in [
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: entry.clone(),
        },
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
            strategy: strategy.clone(),
        },
        AdminDocumentOperation::RealmConfigDefaultStrategySet {
            strategy_id: strategy.strategy_id,
        },
        AdminDocumentOperation::RealmConfigStrategyBindingSet {
            binding: binding.clone(),
        },
        AdminDocumentOperation::RealmConfigPlacementOverrideSet {
            record: record.clone(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let seq = index as u64 + 1;
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_502 + seq, 1),
                target.clone(),
                &actor,
                seq,
                op,
            ),
        )
        .await
        .expect("placement op applies");
    }

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(config.placement_map, vec![entry]);
    assert_eq!(config.strategies, vec![strategy.clone()]);
    assert_eq!(config.default_strategy_id, Some(strategy.strategy_id));
    assert_eq!(config.strategy_bindings, vec![binding]);
    assert_eq!(config.placement_overrides, vec![record]);

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
        reducer_state.materialized_realm_config_default_strategy(),
        Some(strategy.strategy_id)
    );
}

#[test]
fn realm_config_overlay_clears_prior_default_strategy_on_reducer_conflict() {
    let realm_id = RealmId::from_bytes([73; 32]);
    let user_id = UserId::local(Ulid::from_parts(1_520, 1), realm_id);
    let actor_a = test_actor(35, user_id, realm_id);
    let actor_b = test_actor(36, user_id, realm_id);
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let mut state = AdminDocumentReducerState::new(target.clone());
    let prior_default = Ulid::from_parts(1_521, 1);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.default_strategy_id = Some(prior_default);

    let now = unix_timestamp_secs();
    let index = state.revocation_index(now);
    overlay_realm_config_reducer_materialization(
        &mut config,
        &state,
        now,
        unix_timestamp_millis(),
        Some(&index),
    );
    assert_eq!(config.default_strategy_id, None);

    for (event_id, actor, strategy_id) in [
        (
            Ulid::from_parts(1_522, 1),
            &actor_a,
            Ulid::from_parts(1_523, 1),
        ),
        (
            Ulid::from_parts(1_524, 1),
            &actor_b,
            Ulid::from_parts(1_525, 1),
        ),
    ] {
        state
            .apply(&test_admin_event(
                event_id,
                target.clone(),
                actor,
                1,
                AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
            ))
            .unwrap();
    }

    assert!(
        state
            .conflicts
            .contains_key(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
    );
    assert_eq!(state.materialized_realm_config_default_strategy(), None);

    let index = state.revocation_index(now);
    overlay_realm_config_reducer_materialization(
        &mut config,
        &state,
        now,
        unix_timestamp_millis(),
        Some(&index),
    );
    assert_eq!(config.default_strategy_id, None);
}

#[tokio::test]
async fn dangling_strategy_references_materialize_through_storage() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([72; 32]);
    let user_id = UserId::local(Ulid::from_parts(1_510, 1), realm_id);
    let strategy_actor = test_actor(30, user_id, realm_id);
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            document_target.clone(),
            seed_config
                .to_bytes(&strategy_actor)
                .expect("seed realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("seed realm config writes");

    let strategy = PlacementStrategy {
        strategy_id: Ulid::from_parts(1_511, 1),
        name: "removed".to_string(),
        replica_count: Some(3),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: 64,
    };
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_512, 1),
            target.clone(),
            &strategy_actor,
            1,
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: strategy.clone(),
            },
        ),
    )
    .await
    .expect("placement strategy applies");

    let binding = StrategyBinding {
        scope: BindingScope::Class(DocumentClass::MetadataRegistry),
        strategy_id: strategy.strategy_id,
    };
    let record = PlacementOverride {
        subject: b"dangling-document-subject".to_vec(),
        pinned: vec![strategy_actor.node_id],
        excluded: Vec::new(),
        strategy_id: Some(strategy.strategy_id),
    };
    let observed_strategy = AdminDocumentClock::default().with_observed(strategy_actor.node_id, 1);
    for (index, (actor, op)) in [
        (
            test_actor(31, user_id, realm_id),
            AdminDocumentOperation::RealmConfigPlacementStrategyRemoved {
                strategy_id: strategy.strategy_id,
            },
        ),
        (
            test_actor(32, user_id, realm_id),
            AdminDocumentOperation::RealmConfigDefaultStrategySet {
                strategy_id: strategy.strategy_id,
            },
        ),
        (
            test_actor(33, user_id, realm_id),
            AdminDocumentOperation::RealmConfigStrategyBindingSet {
                binding: binding.clone(),
            },
        ),
        (
            test_actor(34, user_id, realm_id),
            AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                record: record.clone(),
            },
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut event = test_admin_event(
            Ulid::from_parts(1_513 + index as u64, 1),
            target.clone(),
            &actor,
            1,
            op,
        );
        event.observed = observed_strategy.clone();
        apply_admin_document_operation_to_storage(&storage, document_target.clone(), event)
            .await
            .expect("concurrent placement operation applies");
    }

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert!(config.strategies.is_empty());
    assert_eq!(config.default_strategy_id, None);
    assert!(config.strategy_bindings.is_empty());
    assert_eq!(config.placement_overrides.len(), 1);
    assert_eq!(config.placement_overrides[0].subject, record.subject);
    assert_eq!(config.placement_overrides[0].pinned, record.pinned);
    assert_eq!(config.placement_overrides[0].excluded, record.excluded);
    assert_eq!(config.placement_overrides[0].strategy_id, None);

    let state_value = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&target),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&state_value).expect("reducer state decodes");
    assert!(
        reducer_state
            .materialized_realm_config_placement_strategies()
            .is_empty()
    );
    assert_eq!(
        reducer_state.materialized_realm_config_default_strategy(),
        Some(strategy.strategy_id)
    );
}

#[tokio::test]
async fn realm_config_settings_op_alone_creates_config_doc() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([49; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_380, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let metadata_replication = MetadataReplicationConfig::new(5);
    let discovery = test_discovery(23, "https://missing-settings.example:443");

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_382, 1),
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
    .expect("realm config settings op bootstraps config doc");

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(config.metadata_replication, metadata_replication);
    assert_eq!(config.discovery, discovery);
    assert!(config.nodes.is_empty());
    assert!(config.oidc_providers.is_empty());
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
