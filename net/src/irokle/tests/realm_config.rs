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

#[tokio::test]
async fn realm_policies_replicate() {
    // S2: a policy event replicated from another node must pass the
    // realm-config storage-apply whitelist and materialize here.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([57; 32]);
    let actor = test_actor(
        9,
        UserId::local(Ulid::from_parts(1_610, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_611, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: test_discovery(24, "https://policies.example:443"),
            },
        ),
    )
    .await
    .expect("settings bootstrap the config doc");

    let policies = vec![aruna_core::request_policy::RequestPolicy {
        policy_id: Ulid::from_bytes([2; 16]),
        name: "no-writes".to_string(),
        kind: aruna_core::request_policy::PolicyKind::Deny,
        when: None,
        expression: "permission == 'write'".to_string(),
        enabled: true,
    }];
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_612, 1),
            target.clone(),
            &actor,
            2,
            AdminDocumentOperation::RealmConfigPoliciesSet {
                policies: policies.clone(),
            },
        ),
    )
    .await
    .expect("policy event replicates and applies");

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(config.request_policies, policies);
}

#[tokio::test]
async fn replicated_revocation_applies() {
    // A revocation replicated from another node must pass the realm-config
    // storage-apply whitelist and deny the token on this node.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([59; 32]);
    let actor = test_actor(
        11,
        UserId::local(Ulid::from_parts(1_630, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_631, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: test_discovery(25, "https://revocation.example:443"),
            },
        ),
    )
    .await
    .expect("settings bootstrap the config doc");

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_631, 2),
            target.clone(),
            &actor,
            2,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: actor.node_id,
                kind: RealmNodeKind::Server,
            },
        ),
    )
    .await
    .expect("realm node bootstraps revocation authority");

    let token_hash = aruna_core::auth::bearer_token_hash("replicated-token");
    let expires_at = unix_timestamp_secs() + 600;
    for (index, seq) in [(1_632u64, 3u64), (1_633, 4)] {
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(index, 1),
                target.clone(),
                &actor,
                seq,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: token_hash.clone(),
                    expires_at,
                    token_owner: actor.user_id,
                },
            ),
        )
        .await
        .expect("revocation replicates and applies");
    }

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert!(config.token_revoked(&token_hash, unix_timestamp_secs()));
    assert_eq!(config.revoked_tokens.len(), 1);
}

#[tokio::test]
async fn accepts_onboarded_origin() {
    // Onboarded node kind changes must not make event arrival order diverge.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([61; 32]);
    let attacker = test_actor(
        13,
        UserId::local(Ulid::from_parts(1_650, 1), realm_id),
        realm_id,
    );
    let token_owner = UserId::local(Ulid::from_parts(1_651, 1), realm_id);
    let config_target = DocumentSyncTarget::RealmConfig { realm_id };
    let admin_target = AdminDocumentTarget::RealmConfig { realm_id };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(attacker.node_id, RealmNodeKind::Server);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            config_target.clone(),
            config
                .to_bytes(&attacker)
                .expect("config serializes")
                .into(),
        )],
    )
    .await
    .expect("config writes");

    let topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let event = test_admin_event(
        Ulid::from_parts(1_652, 1),
        admin_target,
        &attacker,
        1,
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: aruna_core::auth::bearer_token_hash("observed-token"),
            expires_at: unix_timestamp_secs() + 600,
            token_owner,
        },
    );
    let publisher = irokle_crate::actor_id_for(topic, node_id_to_peer_id(&attacker.node_id));

    assert_eq!(
        validate_replicated_admin_event(
            &storage,
            topic,
            publisher,
            &config_target,
            &event,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&event, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("validation runs"),
        AdminEventValidation::Accepted
    );

    config.nodes.clear();
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            config_target.clone(),
            config
                .to_bytes(&attacker)
                .expect("unonboarded config serializes")
                .into(),
        )],
    )
    .await
    .expect("unonboarded config writes");
    assert!(matches!(
        validate_replicated_admin_event(
            &storage,
            topic,
            publisher,
            &config_target,
            &event,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&event, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("unonboarded origin validation runs"),
        AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmConfig(id)),
            ..
        } if id == realm_id
    ));

    config.ensure_node(attacker.node_id, RealmNodeKind::Server);
    let long_event = test_admin_event(
        Ulid::from_parts(1_654, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &attacker,
        1,
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: aruna_core::auth::bearer_token_hash("long-token"),
            expires_at: unix_timestamp_secs()
                + MAX_BEARER_TOKEN_LIFETIME_SECS
                + REVOCATION_GRACE_SECS
                + 1,
            token_owner: attacker.user_id,
        },
    );
    assert!(matches!(
        validate_replicated_admin_event(
            &storage,
            topic,
            publisher,
            &config_target,
            &long_event,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&long_event, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("long expiry validation runs"),
        AdminEventValidation::Rejected(reason)
            if reason == "revoked bearer token expiry exceeds the admission window"
    ));

    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            config_target.clone(),
            config
                .to_bytes(&attacker)
                .expect("config serializes")
                .into(),
        )],
    )
    .await
    .expect("updated config writes");
    let user_event = test_admin_event(
        Ulid::from_parts(1_653, 1),
        AdminDocumentTarget::RealmConfig { realm_id },
        &attacker,
        1,
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: aruna_core::auth::bearer_token_hash("owned-token"),
            expires_at: unix_timestamp_secs() + 600,
            token_owner: attacker.user_id,
        },
    );
    assert_eq!(
        validate_replicated_admin_event(
            &storage,
            topic,
            publisher,
            &config_target,
            &user_event,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&user_event, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("onboarded origin validation runs"),
        AdminEventValidation::Accepted
    );
    apply_admin_document_operation_to_storage(&storage, config_target.clone(), user_event)
        .await
        .expect("onboarded revocation applies");
    let config = read_realm_config_doc(&storage, realm_id).await;
    assert!(config.token_revoked(
        &aruna_core::auth::bearer_token_hash("owned-token"),
        unix_timestamp_secs()
    ));
}

/// Fixture for the relay tests: a realm with one Server origin, one Server
/// relay, and one User device, plus a group-create event from the origin.
struct RelayFixture {
    storage: StorageHandle,
    realm_id: RealmId,
    topic: irokle_crate::TopicId,
    target: DocumentSyncTarget,
    event: AdminDocumentEvent,
    placement: PlacementRef,
    origin: Actor,
    relay: NodeId,
    device: NodeId,
}

async fn relay_fixture(dir: &TempDir) -> RelayFixture {
    let storage = storage_at(dir.path());
    let realm_id = RealmId::from_bytes([71; 32]);
    let origin = test_actor(
        31,
        UserId::local(Ulid::from_parts(1_700, 1), realm_id),
        realm_id,
    );
    let relay = node(32);
    let device = node(33);
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(origin.node_id, RealmNodeKind::Server);
    config.ensure_node(relay, RealmNodeKind::Server);
    config.ensure_node(
        device,
        RealmNodeKind::User {
            owner: origin.user_id,
        },
    );
    let config_target = DocumentSyncTarget::RealmConfig { realm_id };
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            config_target,
            config.to_bytes(&origin).expect("config serializes").into(),
        )],
    )
    .await
    .expect("config writes");

    let group_id = Ulid::from_parts(1_701, 1);
    let target = DocumentSyncTarget::GroupAuthorization { group_id };
    let placement = PlacementRef {
        strategy_id: Ulid::from_parts(1_705, 1),
        shard: 1,
    };
    let event = test_admin_event(
        Ulid::from_parts(1_702, 1),
        AdminDocumentTarget::Group { group_id },
        &origin,
        1,
        AdminDocumentOperation::GroupCreated {
            realm_id,
            display_name: "Engineering".to_string(),
            owner: origin.user_id,
        },
    );
    RelayFixture {
        storage,
        realm_id,
        topic: target.sync_topic_id(realm_id, &placement),
        target,
        event,
        placement,
        origin,
        relay,
        device,
    }
}

async fn validate_relayed(fixture: &RelayFixture, publisher: NodeId) -> AdminEventValidation {
    validate_replicated_admin_event(
        &fixture.storage,
        fixture.topic,
        irokle_crate::actor_id_for(fixture.topic, node_id_to_peer_id(&publisher)),
        &fixture.target,
        &fixture.event,
        fixture.realm_id,
        &fixture.placement,
        &sign_as_origin(&fixture.event, &fixture.placement),
        &mut ConfigValidationCache::default(),
    )
    .await
    .expect("validation runs")
}

#[tokio::test]
async fn relay_preserves_origin() {
    // A Server that is not the origin may carry the origin-signed envelope.
    let dir = tempfile::tempdir().expect("temp dir");
    let fixture = relay_fixture(&dir).await;
    assert_eq!(
        validate_relayed(&fixture, fixture.origin.node_id).await,
        AdminEventValidation::Accepted
    );
    assert_eq!(
        validate_relayed(&fixture, fixture.relay).await,
        AdminEventValidation::Accepted
    );
}

#[tokio::test]
async fn rejects_user_relay() {
    let dir = tempfile::tempdir().expect("temp dir");
    let fixture = relay_fixture(&dir).await;
    assert!(matches!(
        validate_relayed(&fixture, fixture.device).await,
        AdminEventValidation::Rejected(reason)
            if reason == "relayed admin event publisher is not a realm relay node"
    ));
}

#[tokio::test]
async fn rejects_user_origin() {
    // A device never publishes a realm administrative event, relayed or not.
    let dir = tempfile::tempdir().expect("temp dir");
    let mut fixture = relay_fixture(&dir).await;
    let device_actor = test_actor(33, fixture.origin.user_id, fixture.realm_id);
    fixture.event = test_admin_event(
        Ulid::from_parts(1_703, 1),
        fixture.event.target.clone(),
        &device_actor,
        1,
        fixture.event.op.clone(),
    );
    assert!(matches!(
        validate_relayed(&fixture, fixture.relay).await,
        AdminEventValidation::Rejected(reason)
            if reason == "group admin event origin is not a publisher-capable realm node"
    ));
}

#[tokio::test]
async fn rejects_forged_relay() {
    // A relay that rewrites the actor invalidates the origin signature.
    let dir = tempfile::tempdir().expect("temp dir");
    let fixture = relay_fixture(&dir).await;
    let signature = sign_as_origin(&fixture.event, &fixture.placement);
    let mut forged = fixture.event.clone();
    forged.actor.user_id = UserId::local(Ulid::from_parts(1_704, 1), fixture.realm_id);
    assert!(matches!(
        validate_replicated_admin_event(
            &fixture.storage,
            fixture.topic,
            irokle_crate::actor_id_for(fixture.topic, node_id_to_peer_id(&fixture.relay)),
            &fixture.target,
            &forged,
            fixture.realm_id,
            &fixture.placement,
            &signature,
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("validation runs"),
        AdminEventValidation::Rejected(reason)
            if reason == "admin event is not signed by its origin node"
    ));
}

#[tokio::test]
async fn rejects_reshard_relay() {
    // The signature covers the placement, so a relay cannot re-route the
    // envelope onto another shard's topic.
    let dir = tempfile::tempdir().expect("temp dir");
    let fixture = relay_fixture(&dir).await;
    let elsewhere = PlacementRef {
        shard: fixture.placement.shard + 1,
        ..fixture.placement
    };
    assert!(matches!(
        validate_replicated_admin_event(
            &fixture.storage,
            fixture.target.sync_topic_id(fixture.realm_id, &elsewhere),
            irokle_crate::actor_id_for(fixture.topic, node_id_to_peer_id(&fixture.relay)),
            &fixture.target,
            &fixture.event,
            fixture.realm_id,
            &elsewhere,
            &sign_as_origin(&fixture.event, &fixture.placement),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("validation runs"),
        AdminEventValidation::Rejected(reason)
            if reason == "admin event is not signed by its origin node"
    ));
}

#[tokio::test]
async fn caps_flooding_origin() {
    // Past its per-origin bound the flooding node is rejected, while another
    // origin's revocation still applies instead of being trimmed away.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([65; 32]);
    let flooder = test_actor(
        22,
        UserId::local(Ulid::from_parts(1_670, 1), realm_id),
        realm_id,
    );
    let neighbour = test_actor(
        23,
        UserId::local(Ulid::from_parts(1_671, 1), realm_id),
        realm_id,
    );
    let config_target = DocumentSyncTarget::RealmConfig { realm_id };
    let admin_target = AdminDocumentTarget::RealmConfig { realm_id };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(flooder.node_id, RealmNodeKind::Server);
    config.ensure_node(neighbour.node_id, RealmNodeKind::Server);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            config_target.clone(),
            config.to_bytes(&flooder).expect("config serializes").into(),
        )],
    )
    .await
    .expect("config writes");

    let expires_at = unix_timestamp_secs() + 600;
    let mut state = AdminDocumentReducerState::new(admin_target.clone());
    let mut index = state.revocation_index(expires_at);
    for seed in 0..MAX_LIVE_REVOCATIONS_PER_ORIGIN {
        state
            .apply_revocation_operation(
                &flooder,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: aruna_core::auth::bearer_token_hash(&format!("flood-{seed}")),
                    expires_at,
                    token_owner: flooder.user_id,
                },
                &mut index,
            )
            .expect("seeded revocation applies");
    }
    storage_batch_write_to(
        &storage,
        vec![admin_document_reducer_state_write_entry(&state).expect("state serializes")],
    )
    .await
    .expect("reducer state writes");

    let topic = config_target.sync_topic_id(realm_id, &PlacementRef::NIL);
    let flood_event = test_admin_event(
        Ulid::from_parts(1_672, 1),
        admin_target.clone(),
        &flooder,
        1,
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: aruna_core::auth::bearer_token_hash("flood-extra"),
            expires_at,
            token_owner: flooder.user_id,
        },
    );
    assert!(matches!(
        validate_replicated_admin_event(
            &storage,
            topic,
            irokle_crate::actor_id_for(topic, node_id_to_peer_id(&flooder.node_id)),
            &config_target,
            &flood_event,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&flood_event, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("flood validation runs"),
        AdminEventValidation::Rejected(reason)
            if reason == "revocation origin reached its live revocation cap"
    ));

    let neighbour_event = test_admin_event(
        Ulid::from_parts(1_673, 1),
        admin_target,
        &neighbour,
        1,
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: aruna_core::auth::bearer_token_hash("neighbour-token"),
            expires_at,
            token_owner: neighbour.user_id,
        },
    );
    assert_eq!(
        validate_replicated_admin_event(
            &storage,
            topic,
            irokle_crate::actor_id_for(topic, node_id_to_peer_id(&neighbour.node_id)),
            &config_target,
            &neighbour_event,
            realm_id,
            &PlacementRef::NIL,
            &sign_as_origin(&neighbour_event, &PlacementRef::NIL),
            &mut ConfigValidationCache::default(),
        )
        .await
        .expect("neighbour validation runs"),
        AdminEventValidation::Accepted
    );
}

#[test]
fn accepts_historical_origin() {
    let realm_id = RealmId::from_bytes([62; 32]);
    let origin = test_actor(
        14,
        UserId::local(Ulid::from_parts(1_660, 1), realm_id),
        realm_id,
    );
    let other = test_actor(
        15,
        UserId::local(Ulid::from_parts(1_661, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let ensure = test_admin_event(
        Ulid::from_parts(1_662, 1),
        target.clone(),
        &origin,
        1,
        AdminDocumentOperation::RealmConfigNodeEnsured {
            node_id: origin.node_id,
            kind: RealmNodeKind::Server,
        },
    );
    let conflict = test_admin_event(
        Ulid::from_parts(1_663, 1),
        target.clone(),
        &other,
        1,
        AdminDocumentOperation::RealmConfigNodeEnsured {
            node_id: origin.node_id,
            kind: RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        },
    );
    let mut state = AdminDocumentReducerState::new(target.clone());
    state.apply(&ensure).expect("onboarding applies");
    state
        .apply(&conflict)
        .expect("conflicting onboarding applies");
    assert!(
        state
            .conflicts
            .contains_key(&realm_config_node_path(&origin.node_id))
    );

    let event = AdminDocumentEvent {
        event_id: Ulid::from_parts(1_664, 1),
        target,
        origin_node_id: origin.node_id,
        origin_seq: 2,
        observed: AdminDocumentClock::default().with_observed(origin.node_id, 1),
        actor: origin,
        op: AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: aruna_core::auth::bearer_token_hash("historical-token"),
            expires_at: unix_timestamp_secs() + 600,
            token_owner: other.user_id,
        },
    };
    assert!(revocation_origin_known(
        None,
        Some(&state),
        &event,
        realm_id
    ));
}

#[tokio::test]
async fn replicated_revocation_compacts() {
    // A replicated revocation whose token has expired must leave no entry
    // behind in the receiving node's reducer state.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([60; 32]);
    let actor = test_actor(
        12,
        UserId::local(Ulid::from_parts(1_640, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_641, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: test_discovery(26, "https://compaction.example:443"),
            },
        ),
    )
    .await
    .expect("settings bootstrap the config doc");

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_641, 2),
            target.clone(),
            &actor,
            2,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: actor.node_id,
                kind: RealmNodeKind::Server,
            },
        ),
    )
    .await
    .expect("realm node bootstraps revocation authority");

    let expired = aruna_core::auth::bearer_token_hash("expired-token");
    let live = aruna_core::auth::bearer_token_hash("live-token");
    let now = unix_timestamp_secs();
    for (index, seq, token_hash, expires_at) in [
        (1_642u64, 3u64, expired.clone(), now - 1),
        (1_643, 4, live.clone(), now + 600),
    ] {
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(index, 1),
                target.clone(),
                &actor,
                seq,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash,
                    expires_at,
                    token_owner: actor.user_id,
                },
            ),
        )
        .await
        .expect("revocation replicates and applies");
    }

    let mut state = read_admin_reducer_state(&storage, &target)
        .await
        .expect("reducer state reads")
        .expect("reducer state exists");
    assert!(state.materialized_revoked_tokens().contains_key(&live));
    assert!(
        state
            .user_subject_ids
            .keys()
            .any(|path| path.contains(&expired))
    );
    let config = read_realm_config_doc(&storage, realm_id).await;
    assert!(!config.token_revoked(&expired, now));
    assert_eq!(config.revoked_tokens.len(), 1);

    let future = now + REVOCATION_GRACE_SECS + 1;
    state.compact_revocations(future);
    assert!(
        !state
            .user_subject_ids
            .keys()
            .any(|path| path.contains(&expired))
    );
    let mut future_config = config;
    let index = state.revocation_index(future);
    overlay_realm_config_reducer_materialization(
        &mut future_config,
        &state,
        future,
        unix_timestamp_millis(),
        Some(&index),
    );
    assert_eq!(future_config.revoked_tokens.len(), 1);
}

#[tokio::test]
async fn redundant_persists_clock() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([64; 32]);
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let assert_stable = |before: &[u8], after: &[u8]| {
        let mut before = RealmConfigDocument::from_bytes(before).expect("decode prior config");
        let after = RealmConfigDocument::from_bytes(after).expect("decode current config");
        assert!(after.revocation_floor >= before.revocation_floor);
        before.revocation_floor = after.revocation_floor;
        assert_eq!(before, after);
    };
    let actor_a = test_actor(
        18,
        UserId::local(Ulid::from_parts(1_690, 1), realm_id),
        realm_id,
    );
    let actor_b = test_actor(
        19,
        UserId::local(Ulid::from_parts(1_691, 1), realm_id),
        realm_id,
    );
    let token_hash = aruna_core::auth::bearer_token_hash("redundant-token");
    let expires_at = unix_timestamp_secs() + 600;
    let mut seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    seed_config.ensure_node(actor_a.node_id, RealmNodeKind::Server);
    seed_config.ensure_node(actor_b.node_id, RealmNodeKind::Server);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            document_target.clone(),
            seed_config
                .to_bytes(&actor_a)
                .expect("seed config serializes")
                .into(),
        )],
    )
    .await
    .expect("seed config writes");
    let first = test_admin_event(
        Ulid::from_parts(1_692, 1),
        target.clone(),
        &actor_a,
        1,
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: token_hash.clone(),
            expires_at,
            token_owner: actor_a.user_id,
        },
    );
    apply_admin_document_operation_to_storage(&storage, document_target.clone(), first.clone())
        .await
        .expect("first revocation applies");
    let config_key = document_target.storage_key();
    let before = read_storage_value(
        &storage,
        document_target.storage_keyspace(),
        config_key.clone(),
    )
    .await
    .expect("config exists after first revocation");
    apply_admin_document_operation_to_storage(&storage, document_target.clone(), first)
        .await
        .expect("duplicate revocation applies");
    let duplicate = read_storage_value(
        &storage,
        document_target.storage_keyspace(),
        document_target.storage_key(),
    )
    .await
    .expect("config exists after duplicate revocation");
    assert_stable(&before, &duplicate);

    let second = test_admin_event(
        Ulid::from_parts(1_691, 1),
        target.clone(),
        &actor_b,
        1,
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: token_hash.clone(),
            expires_at,
            token_owner: actor_b.user_id,
        },
    );
    apply_admin_document_operation_to_storage(&storage, document_target.clone(), second)
        .await
        .expect("redundant revocation applies");

    let after = read_storage_value(&storage, document_target.storage_keyspace(), config_key)
        .await
        .expect("config remains after redundant revocation");
    assert_stable(&before, &after);
    let state = read_admin_reducer_state(&storage, &target)
        .await
        .expect("reducer state reads")
        .expect("reducer state exists");
    assert_eq!(state.clock.sequence_for(&actor_a.node_id), 1);
    assert_eq!(state.clock.sequence_for(&actor_b.node_id), 1);
    assert_eq!(state.applied_event_ids.len(), 1);
    assert_eq!(
        state.materialized_revoked_tokens(),
        BTreeMap::from([(token_hash, expires_at)])
    );
}

#[tokio::test]
async fn group_policies_replicate() {
    // GroupPoliciesSet must pass the group storage-apply whitelist and land
    // on the receiving node's group authorization document.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([58; 32]);
    let group_id = Ulid::from_parts(1_620, 1);
    let owner = UserId::local(Ulid::from_parts(1_621, 1), realm_id);
    let actor = test_actor(10, owner, realm_id);
    let target = AdminDocumentTarget::Group { group_id };
    let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_622, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name: "Engineering".to_string(),
                owner,
            },
        ),
    )
    .await
    .expect("group creation bootstraps the auth doc");

    let policies = vec![aruna_core::request_policy::RequestPolicy {
        policy_id: Ulid::from_bytes([3; 16]),
        name: "no-writes".to_string(),
        kind: aruna_core::request_policy::PolicyKind::Deny,
        when: None,
        expression: "permission == 'write'".to_string(),
        enabled: true,
    }];
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_623, 1),
            target.clone(),
            &actor,
            2,
            AdminDocumentOperation::GroupPoliciesSet {
                policies: policies.clone(),
            },
        ),
    )
    .await
    .expect("group policy event replicates and applies");

    let auth_doc = read_group_auth_doc(&storage, group_id).await;
    assert_eq!(auth_doc.policies, policies);
}

#[tokio::test]
async fn join_authority_replication() {
    use aruna_core::join_request::{JoinDecision, JoinDecisionKind, JoinRequest};
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([59; 32]);
    let group_id = Ulid::from_bytes([3; 16]);
    let owner = test_actor(
        11,
        UserId::local(Ulid::from_bytes([1; 16]), realm_id),
        realm_id,
    );
    let member = test_actor(
        11,
        UserId::local(Ulid::from_bytes([2; 16]), realm_id),
        realm_id,
    );
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(owner.node_id, RealmNodeKind::Management);
    let auth = GroupAuthorizationDocument::new_default_group_doc(owner.user_id, realm_id, group_id);
    let role_id = auth
        .roles
        .values()
        .find(|role| role.name == "user")
        .unwrap()
        .role_id;
    let group = Group {
        display_name: "Group".into(),
        group_id,
        realm_id,
        owner: owner.user_id,
        roles: auth.roles.keys().copied().collect(),
    };
    let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
    let target = AdminDocumentTarget::Group { group_id };
    let document = DocumentSyncTarget::GroupAuthorization { group_id };
    let mut reducer = AdminDocumentReducerState::new(target.clone());
    storage_batch_write_to(
        &storage,
        vec![
            target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config.to_bytes(&owner).unwrap().into(),
            ),
            target_write_entry(
                DocumentSyncTarget::RealmAuthorization { realm_id },
                realm_auth.to_bytes(&owner).unwrap().into(),
            ),
            target_write_entry(document.clone(), auth.to_bytes(&owner).unwrap().into()),
            (
                GROUP_KEYSPACE.into(),
                group_id.to_bytes().into(),
                group.to_bytes(&owner).unwrap().into(),
            ),
        ],
    )
    .await
    .unwrap();
    let request = JoinRequest {
        request_id: Ulid::from_bytes([5; 16]),
        group_id,
        user_id: member.user_id,
        message: None,
        created_at: 1,
    };
    let requested = reducer
        .apply_operation(
            &member,
            AdminDocumentOperation::GroupJoinRequested {
                request: request.clone(),
            },
        )
        .unwrap();
    assert!(matches!(
        validate_group_admin_authority(&storage, &requested, None)
            .await
            .unwrap(),
        AdminEventValidation::Accepted
    ));
    apply_admin_document_operation_to_storage(&storage, document.clone(), requested)
        .await
        .unwrap();
    let denied = test_admin_event(
        Ulid::from_bytes([6; 16]),
        target,
        &member,
        2,
        AdminDocumentOperation::GroupJoinDecided {
            decision: JoinDecision {
                request_id: request.request_id,
                user_id: member.user_id,
                kind: JoinDecisionKind::Approved,
                decided_by: member.user_id,
                reason: None,
                decided_at: 2,
                role_ids: BTreeSet::from([role_id]),
            },
        },
    );
    assert!(matches!(
        validate_group_admin_authority(&storage, &denied, Some(&reducer))
            .await
            .unwrap(),
        AdminEventValidation::Rejected(_)
    ));
    let approved = reducer
        .apply_operation(
            &owner,
            AdminDocumentOperation::GroupJoinDecided {
                decision: JoinDecision {
                    request_id: request.request_id,
                    user_id: member.user_id,
                    kind: JoinDecisionKind::Approved,
                    decided_by: owner.user_id,
                    reason: None,
                    decided_at: 2,
                    role_ids: BTreeSet::from([role_id]),
                },
            },
        )
        .unwrap();
    let previous = read_admin_reducer_state(&storage, &reducer.target)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        validate_group_admin_authority(&storage, &approved, Some(&previous))
            .await
            .unwrap(),
        AdminEventValidation::Accepted
    ));
    apply_admin_document_operation_to_storage(&storage, document, approved)
        .await
        .unwrap();
    assert!(
        read_group_auth_doc(&storage, group_id).await.roles[&role_id]
            .assigned_users
            .contains(&member.user_id)
    );
    let saved = read_admin_reducer_state(&storage, &reducer.target)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        saved.join_requests()[0].decision.as_ref().unwrap().kind,
        JoinDecisionKind::Approved
    );
}

#[tokio::test]
async fn policies_authority_gate() {
    // GroupPoliciesSet must reach the config-path check, not the unreachable
    // arm: a non-owner config admin is accepted, one without config write is
    // rejected, and neither path panics.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([59; 32]);
    let group_id = Ulid::from_parts(1_630, 1);
    let owner = UserId::local(Ulid::from_parts(1_631, 1), realm_id);
    let admin_user = UserId::local(Ulid::from_parts(1_632, 1), realm_id);
    let role_id = Ulid::from_parts(1_633, 1);
    let actor = test_actor(11, admin_user, realm_id);

    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Management);
    let group = Group {
        display_name: "Engineering".to_string(),
        group_id,
        realm_id,
        owner,
        roles: HashSet::from([role_id]),
    };
    let realm_auth = RealmAuthorizationDocument {
        realm_id,
        roles: HashMap::new(),
        operation_restrictions: Default::default(),
    };
    storage_batch_write_to(
        &storage,
        vec![
            target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config.to_bytes(&actor).expect("config serializes").into(),
            ),
            target_write_entry(
                DocumentSyncTarget::RealmAuthorization { realm_id },
                realm_auth
                    .to_bytes(&actor)
                    .expect("realm auth serializes")
                    .into(),
            ),
            (
                GROUP_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                group.to_bytes(&actor).expect("group serializes").into(),
            ),
        ],
    )
    .await
    .expect("realm and group state writes");

    let config_path = format!("/{realm_id}/g/{group_id}/admin/config");
    let policies = vec![aruna_core::request_policy::RequestPolicy {
        policy_id: Ulid::from_bytes([7; 16]),
        name: "deny-writes".to_string(),
        kind: aruna_core::request_policy::PolicyKind::Deny,
        when: None,
        expression: "permission == 'write'".to_string(),
        enabled: true,
    }];
    let event = test_admin_event(
        Ulid::from_parts(1_634, 1),
        AdminDocumentTarget::Group { group_id },
        &actor,
        1,
        AdminDocumentOperation::GroupPoliciesSet { policies },
    );

    for (permission, expect_accept) in [(Permission::WRITE, true), (Permission::READ, false)] {
        let auth_doc = GroupAuthorizationDocument {
            group_id,
            policies: Vec::new(),
            roles: HashMap::from([(
                role_id,
                Role {
                    role_id,
                    name: "config_admin".to_string(),
                    permissions: HashMap::from([(config_path.clone(), permission)]),
                    assigned_users: HashSet::from([admin_user]),
                },
            )]),
        };
        storage_batch_write_to(
            &storage,
            vec![target_write_entry(
                DocumentSyncTarget::GroupAuthorization { group_id },
                auth_doc
                    .to_bytes(&actor)
                    .expect("auth doc serializes")
                    .into(),
            )],
        )
        .await
        .expect("group auth doc writes");

        let validation = validate_group_admin_authority(&storage, &event, None)
            .await
            .expect("validation runs without panic");
        assert_eq!(
            matches!(validation, AdminEventValidation::Accepted),
            expect_accept
        );
    }
}

#[tokio::test]
async fn quota_survives_materialization() {
    // Quota remains when the reducer materializes without a stored config.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([61; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_384, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let quota = QuotaConfig {
        default_group_quota_bytes: Some(9_000),
        grace_factor_percent: 130,
        warn_threshold_percent: 70,
        group_overrides: vec![GroupQuotaOverride {
            group_id: Ulid::from_parts(1_385, 1),
            quota_bytes: Some(4_500),
            grace_factor_percent: Some(140),
        }],
        max_groups_per_user: Some(7),
        user_group_cap_overrides: vec![UserGroupCapOverride {
            user_id: UserId::local(Ulid::from_parts(1_386, 1), realm_id),
            max_groups: Some(2),
        }],
        max_devices_per_user: Some(6),
        ..QuotaConfig::default()
    };

    // Quota lands before any config doc exists; it must be recorded in the
    // reducer and later carried through realm_config_from_reducer_materialization.
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_387, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigQuotaSet {
                quota: quota.clone(),
            },
        ),
    )
    .await
    .expect("realm config quota op applies");

    let metadata_replication = MetadataReplicationConfig::new(5);
    let discovery = test_discovery(23, "https://quota-materialization.example:443");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_388, 1),
            target.clone(),
            &actor,
            2,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: metadata_replication.clone(),
                discovery: discovery.clone(),
            },
        ),
    )
    .await
    .expect("realm config settings op bootstraps config doc");

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(config.quota, quota);
    assert_eq!(config.metadata_replication, metadata_replication);
}

#[tokio::test]
async fn realm_config_settings_after_node_and_oidc_bootstraps_full_config() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([52; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_410, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let reducer_node = node(28);
    let provider = test_oidc_provider("default", "bootstrap-after-reducer");
    let metadata_replication = MetadataReplicationConfig::new(8);
    let discovery = test_discovery(29, "https://bootstrap-settings.example:443");

    for (seq, op) in [
        (
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: reducer_node,
                kind: RealmNodeKind::Management,
            },
        ),
        (
            2,
            AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                provider: provider.clone(),
            },
        ),
    ] {
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_410 + seq, 1),
                target.clone(),
                &actor,
                seq,
                op,
            ),
        )
        .await
        .expect("pre-settings realm config reducer op applies");
    }
    assert!(
        read_storage_value(
            &storage,
            document_target.storage_keyspace(),
            document_target.storage_key(),
        )
        .await
        .is_none()
    );

    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(1_413, 1),
            target,
            &actor,
            3,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: metadata_replication.clone(),
                discovery: discovery.clone(),
            },
        ),
    )
    .await
    .expect("realm config settings op bootstraps full config doc");

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(config.metadata_replication, metadata_replication);
    assert_eq!(config.discovery, discovery);
    assert_eq!(
        realm_config_nodes(&config),
        BTreeMap::from([(reducer_node.to_string(), RealmNodeKind::Management)])
    );
    assert_eq!(
        realm_config_oidc_providers(&config),
        BTreeMap::from([("default".to_string(), provider)])
    );
}

#[tokio::test]
async fn drops_evicted_node() {
    // Removal must reach the stored document: an evicted device otherwise
    // stays an admitted peer wherever that document is read.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([53; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_460, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let device = node(31);

    for (seq, op) in [
        (
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: device,
                kind: RealmNodeKind::User {
                    owner: actor.user_id,
                },
            },
        ),
        (
            2,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: test_discovery(31, "https://eviction.example:443"),
            },
        ),
    ] {
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_460 + seq, 1),
                target.clone(),
                &actor,
                seq,
                op,
            ),
        )
        .await
        .expect("device enrollment applies");
    }
    assert!(
        realm_config_nodes(&read_realm_config_doc(&storage, realm_id).await)
            .contains_key(&device.to_string())
    );

    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(1_463, 1),
            target,
            &actor,
            3,
            AdminDocumentOperation::RealmConfigNodeRemoved { node_id: device },
        ),
    )
    .await
    .expect("device removal applies");

    assert!(realm_config_nodes(&read_realm_config_doc(&storage, realm_id).await).is_empty());
}

#[tokio::test]
async fn replicates_compute_config() {
    // Planners on receiving nodes only see operator compute knowledge if
    // the realm config apply path admits the replicated operation.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([54; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_470, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let compute = aruna_core::structs::RealmComputeConfig {
        witness_base_delay_ms: 4_200,
        catch_up_after_ms: 61_000,
        ..Default::default()
    };

    for (seq, op) in [
        (
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: node(8),
                kind: RealmNodeKind::Management,
            },
        ),
        (
            2,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: test_discovery(32, "https://compute.example:443"),
            },
        ),
    ] {
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_470 + seq, 1),
                target.clone(),
                &actor,
                seq,
                op,
            ),
        )
        .await
        .expect("realm config bootstrap applies");
    }

    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(1_473, 1),
            target,
            &actor,
            3,
            AdminDocumentOperation::RealmConfigComputeSet {
                compute: compute.clone(),
            },
        ),
    )
    .await
    .expect("replicated compute config applies");

    assert_eq!(
        read_realm_config_doc(&storage, realm_id).await.compute,
        compute
    );
}

#[tokio::test]
async fn concurrent_realm_config_settings_conflict_withholds_conflicted_metadata() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([51; 32]);
    let actor_a = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_400, 1), realm_id),
        realm_id,
    );
    let actor_b = test_actor(
        9,
        UserId::local(Ulid::from_parts(1_401, 1), realm_id),
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
                .to_bytes(&actor_a)
                .expect("seed realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("seed realm config writes");

    let first_metadata = MetadataReplicationConfig::new(5);
    let second_metadata = MetadataReplicationConfig::new(7);
    let discovery = test_discovery(27, "https://conflict-settings.example:443");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_402, 1),
            target.clone(),
            &actor_a,
            1,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: first_metadata.clone(),
                discovery: discovery.clone(),
            },
        ),
    )
    .await
    .expect("first realm config settings op applies");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(1_403, 1),
            target.clone(),
            &actor_b,
            1,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: second_metadata.clone(),
                discovery: discovery.clone(),
            },
        ),
    )
    .await
    .expect("conflicting realm config settings op applies");

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(config.metadata_replication, first_metadata);
    assert_ne!(config.metadata_replication, second_metadata);
    assert_eq!(config.discovery, discovery);
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
        None
    );
    assert!(
        read_storage_value(
            &storage,
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
            admin_document_reducer_conflict_key(&target, REALM_CONFIG_METADATA_REPLICATION_PATH,),
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn retries_config_conflict() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([52; 32]);
    let actor_a = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_410, 1), realm_id),
        realm_id,
    );
    let actor_b = test_actor(
        9,
        UserId::local(Ulid::from_parts(1_411, 1), realm_id),
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
                .to_bytes(&actor_a)
                .expect("seed realm config serializes")
                .into(),
        )],
    )
    .await
    .expect("seed realm config writes");

    let first = test_admin_event(
        Ulid::from_parts(1_412, 1),
        target.clone(),
        &actor_a,
        1,
        AdminDocumentOperation::RealmConfigDescriptionSet {
            description: "first".to_string(),
        },
    );
    let second = test_admin_event(
        Ulid::from_parts(1_413, 1),
        target.clone(),
        &actor_b,
        1,
        AdminDocumentOperation::RealmConfigDescriptionSet {
            description: "second".to_string(),
        },
    );
    let (first_result, second_result) = tokio::join!(
        apply_admin_document_operation_to_storage(&storage, document_target.clone(), first),
        apply_admin_document_operation_to_storage(&storage, document_target.clone(), second),
    );
    first_result.expect("first concurrent config operation applies");
    second_result.expect("second concurrent config operation retries");

    let config = read_realm_config_doc(&storage, realm_id).await;
    let state = read_admin_reducer_state(&storage, &target)
        .await
        .expect("reducer state reads")
        .expect("reducer state exists");
    assert!(matches!(config.description.as_str(), "first" | "second"));
    let path = REALM_CONFIG_DESCRIPTION_PATH;
    assert!(state.conflicts.contains_key(path));
    assert_eq!(
        state
            .conflicts
            .get(path)
            .expect("description conflict exists")
            .values
            .len(),
        2
    );
}

#[tokio::test]
async fn keeps_stale_config() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([53; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(1_420, 1), realm_id),
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

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_421, 1),
            target.clone(),
            &actor,
            2,
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "new".to_string(),
            },
        ),
    )
    .await
    .expect("new config operation applies");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_422, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: "stale".to_string(),
            },
        ),
    )
    .await
    .expect("stale config operation persists clock");

    assert_eq!(
        read_realm_config_doc(&storage, realm_id).await.description,
        "new"
    );
    let state = read_admin_reducer_state(&storage, &target)
        .await
        .expect("reducer state reads")
        .expect("reducer state exists");
    assert_eq!(state.clock.sequence_for(&actor.node_id), 2);
    assert_eq!(
        state.materialized_realm_config_description().as_deref(),
        Some("new")
    );
}

#[tokio::test]
async fn realm_config_node_ensure_admin_ops_merge_nodes() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([41; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(300, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let first_node = node(11);
    let second_node = node(12);

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

    for (seq, node_id, kind) in [
        (1, first_node, RealmNodeKind::Management),
        (2, second_node, RealmNodeKind::Server),
    ] {
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_300 + seq, 1),
                target.clone(),
                &actor,
                seq,
                AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind },
            ),
        )
        .await
        .expect("realm config node ensure applies");
    }

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(
        realm_config_nodes(&config),
        BTreeMap::from([
            (first_node.to_string(), RealmNodeKind::Management),
            (second_node.to_string(), RealmNodeKind::Server),
        ])
    );
}

#[tokio::test]
async fn realm_config_oidc_provider_admin_ops_merge_disjoint_updates() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([44; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(330, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let existing = test_oidc_provider("existing", "existing");
    let removed = test_oidc_provider("removed", "removed");
    let first = test_oidc_provider("default", "one");
    let second = test_oidc_provider("partner", "two");
    let seed_node = node(16);
    let mut seed_config =
        RealmConfigDocument::new(realm_id, vec![existing.clone(), removed.clone()], 5);
    seed_config.discovery = RealmDiscoveryConfig::Static {
        endpoints: vec![StaticRealmEndpoint {
            node_id: seed_node.to_string(),
            endpoint_addr: "https://seed.example:443".to_string(),
        }],
    };
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

    for (seq, op) in [
        (
            1,
            AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                provider: first.clone(),
            },
        ),
        (
            2,
            AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                provider: second.clone(),
            },
        ),
        (
            3,
            AdminDocumentOperation::RealmConfigOidcProviderRemoved {
                provider_id: removed.id.clone(),
            },
        ),
    ] {
        apply_admin_document_operation_to_storage(
            &storage,
            document_target.clone(),
            test_admin_event(
                Ulid::from_parts(1_330 + seq, 1),
                target.clone(),
                &actor,
                seq,
                op,
            ),
        )
        .await
        .expect("realm config OIDC provider op applies");
    }

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert_eq!(
        config.metadata_replication,
        seed_config.metadata_replication
    );
    assert_eq!(config.discovery, seed_config.discovery);
    assert_eq!(
        realm_config_nodes(&config),
        realm_config_nodes(&seed_config)
    );
    assert_eq!(
        realm_config_oidc_providers(&config),
        BTreeMap::from([
            ("default".to_string(), first),
            ("existing".to_string(), existing),
            ("partner".to_string(), second),
        ])
    );
}

#[tokio::test]
async fn concurrent_realm_config_oidc_provider_conflict_withholds_provider() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([45; 32]);
    let actor_a = test_actor(
        8,
        UserId::local(Ulid::from_parts(340, 1), realm_id),
        realm_id,
    );
    let actor_b = test_actor(
        9,
        UserId::local(Ulid::from_parts(341, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let untouched = test_oidc_provider("untouched", "untouched");
    let first = test_oidc_provider("default", "one");
    let second = test_oidc_provider("default", "two");
    let seed_config = RealmConfigDocument::new(
        realm_id,
        vec![test_oidc_provider("default", "seed"), untouched.clone()],
        3,
    );
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            document_target.clone(),
            seed_config
                .to_bytes(&actor_a)
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
            Ulid::from_parts(1_340, 1),
            target.clone(),
            &actor_a,
            1,
            AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider: first },
        ),
    )
    .await
    .expect("first realm config OIDC provider upsert applies");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(1_341, 1),
            target.clone(),
            &actor_b,
            1,
            AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider: second },
        ),
    )
    .await
    .expect("conflicting realm config OIDC provider upsert applies");

    let config = read_realm_config_doc(&storage, realm_id).await;
    let providers = realm_config_oidc_providers(&config);
    assert!(!providers.contains_key("default"));
    assert_eq!(providers.get("untouched"), Some(&untouched));
    let path = aruna_core::reducer::realm_config_oidc_provider_path("default");
    assert!(
        read_storage_value(
            &storage,
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
            admin_document_reducer_conflict_key(&target, &path),
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn missing_realm_config_oidc_provider_op_stores_state_without_config_doc() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([47; 32]);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(360, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let provider = test_oidc_provider("default", "missing");

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(1_360, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                provider: provider.clone(),
            },
        ),
    )
    .await
    .expect("realm config OIDC provider upsert applies without config doc");

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
        reducer_state.materialized_realm_config_oidc_providers(),
        BTreeMap::from([("default".to_string(), provider)])
    );
}

#[tokio::test]
async fn concurrent_realm_config_node_kind_conflict_withholds_node() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([43; 32]);
    let actor_a = test_actor(
        8,
        UserId::local(Ulid::from_parts(320, 1), realm_id),
        realm_id,
    );
    let actor_b = test_actor(
        9,
        UserId::local(Ulid::from_parts(321, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::RealmConfig { realm_id };
    let document_target = DocumentSyncTarget::RealmConfig { realm_id };
    let conflicted_node = node(15);

    let seed_config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            document_target.clone(),
            seed_config
                .to_bytes(&actor_a)
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
            Ulid::from_parts(1_500, 1),
            target.clone(),
            &actor_a,
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: conflicted_node,
                kind: RealmNodeKind::Management,
            },
        ),
    )
    .await
    .expect("first realm config node ensure applies");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(1_501, 1),
            target.clone(),
            &actor_b,
            1,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: conflicted_node,
                kind: RealmNodeKind::Server,
            },
        ),
    )
    .await
    .expect("conflicting realm config node ensure applies");

    let config = read_realm_config_doc(&storage, realm_id).await;
    assert!(!realm_config_nodes(&config).contains_key(&conflicted_node.to_string()));
    let path = realm_config_node_path(&conflicted_node);
    assert!(
        read_storage_value(
            &storage,
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
            admin_document_reducer_conflict_key(&target, &path),
        )
        .await
        .is_some()
    );
}
