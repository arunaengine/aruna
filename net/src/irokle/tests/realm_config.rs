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
