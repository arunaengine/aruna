use super::*;

#[test]
fn realm_config_disjoint_nodes_merge_deterministically() {
    let mut state = realm_config_state();
    let first_node = node(11);
    let second_node = node(12);

    state
        .apply(&ensure_realm_config_node(
            1,
            1,
            first_node,
            RealmNodeKind::Management,
        ))
        .unwrap();
    state
        .apply(&ensure_realm_config_node(
            2,
            2,
            second_node,
            RealmNodeKind::Server,
        ))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_nodes(),
        BTreeMap::from([
            (first_node, RealmNodeKind::Management),
            (second_node, RealmNodeKind::Server),
        ])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn concurrent_realm_config_same_node_different_kind_conflicts_fail_closed() {
    let mut state = realm_config_state();
    let config_node = node(11);

    state
        .apply(&ensure_realm_config_node(
            1,
            1,
            config_node,
            RealmNodeKind::Management,
        ))
        .unwrap();
    state
        .apply(&ensure_realm_config_node(
            2,
            2,
            config_node,
            RealmNodeKind::Server,
        ))
        .unwrap();

    assert!(
        !state
            .materialized_realm_config_nodes()
            .contains_key(&config_node)
    );
    let conflict = state
        .conflicts
        .get(&format!("realm_config.nodes.{config_node}"))
        .expect("conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    for kind in [RealmNodeKind::Management, RealmNodeKind::Server] {
        let encoded = realm_node_kind_value(&kind);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(encoded.as_str()))
        );
    }
}

#[test]
fn removes_config_node() {
    // Ensured then removed: the node leaves the materialization and is
    // reported as removed, which is what the overlays subtract.
    let mut state = realm_config_state();
    let config_node = node(11);
    let origin = node(1);
    let ensured = ensure_realm_config_node(
        1,
        1,
        config_node,
        RealmNodeKind::User {
            owner: UserId::nil(realm_id()),
        },
    );
    let removed = realm_config_event(
        2,
        origin,
        2,
        AdminDocumentClock::default().with_observed(origin, 1),
        AdminDocumentOperation::RealmConfigNodeRemoved {
            node_id: config_node,
        },
    );

    state.apply(&ensured).unwrap();
    state.apply(&removed).unwrap();

    assert!(state.materialized_realm_config_nodes().is_empty());
    assert_eq!(state.removed_config_nodes(), BTreeSet::from([config_node]));
    assert!(state.conflicts.is_empty());
}

#[test]
fn observed_realm_config_node_update_replaces_conflict() {
    let mut state = realm_config_state();
    let config_node = node(11);
    let first_origin = node(1);
    let second_origin = node(2);
    let first = realm_config_event(
        1,
        first_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigNodeEnsured {
            node_id: config_node,
            kind: RealmNodeKind::Management,
        },
    );
    let second = realm_config_event(
        2,
        second_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigNodeEnsured {
            node_id: config_node,
            kind: RealmNodeKind::Server,
        },
    );
    let replacement = realm_config_event(
        3,
        node(3),
        1,
        AdminDocumentClock::default()
            .with_observed(first_origin, 1)
            .with_observed(second_origin, 1),
        AdminDocumentOperation::RealmConfigNodeEnsured {
            node_id: config_node,
            kind: RealmNodeKind::User {
                owner: UserId::nil(realm_id()),
            },
        },
    );

    state.apply(&first).unwrap();
    state.apply(&second).unwrap();
    state.apply(&replacement).unwrap();

    assert_eq!(
        state.materialized_realm_config_nodes(),
        BTreeMap::from([(
            config_node,
            RealmNodeKind::User {
                owner: UserId::nil(realm_id()),
            }
        )])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn node_kinds_roundtrip() {
    // The materialized attribute value is the only carrier of the owner a
    // User node is bound to, so decode must return it unchanged.
    let owner = UserId::new(Ulid::from_bytes([5u8; 16]), realm_id());
    for kind in [
        RealmNodeKind::Management,
        RealmNodeKind::Server,
        RealmNodeKind::User { owner },
        RealmNodeKind::User {
            owner: UserId::nil(realm_id()),
        },
    ] {
        let encoded = realm_node_kind_value(&kind);
        assert_eq!(realm_node_kind_from_value(&encoded), Some(kind));
    }
    assert_eq!(realm_node_kind_from_value("not-a-kind"), None);
}

#[test]
fn realm_config_disjoint_oidc_providers_merge_deterministically() {
    let mut state = realm_config_state();
    let first = oidc_provider("default", "one");
    let second = oidc_provider("partner", "two");

    state
        .apply(&upsert_oidc_provider(1, 1, first.clone()))
        .unwrap();
    state
        .apply(&upsert_oidc_provider(2, 2, second.clone()))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_oidc_providers(),
        BTreeMap::from([
            ("default".to_string(), first),
            ("partner".to_string(), second),
        ])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn concurrent_realm_config_node_provider_and_settings_ops_converge_independent_of_order() {
    let config_node = node(11);
    let provider = oidc_provider("default", "one");
    let metadata_replication = MetadataReplicationConfig::new(5);
    let discovery = RealmDiscoveryConfig::Static {
        endpoints: Vec::new(),
    };
    let node_event = ensure_realm_config_node(1, 1, config_node, RealmNodeKind::Management);
    let provider_event = upsert_oidc_provider(2, 2, provider.clone());
    let settings_event =
        set_realm_config_settings(3, 3, metadata_replication.clone(), discovery.clone());

    let mut left = realm_config_state();
    left.apply(&node_event).unwrap();
    left.apply(&provider_event).unwrap();
    left.apply(&settings_event).unwrap();

    let mut right = realm_config_state();
    right.apply(&settings_event).unwrap();
    right.apply(&provider_event).unwrap();
    right.apply(&node_event).unwrap();

    assert_eq!(left.user_subject_ids, right.user_subject_ids);
    assert_eq!(
        left.materialized_realm_config_nodes(),
        BTreeMap::from([(config_node, RealmNodeKind::Management)])
    );
    assert_eq!(
        left.materialized_realm_config_oidc_providers(),
        BTreeMap::from([("default".to_string(), provider)])
    );
    assert_eq!(
        left.materialized_realm_config_metadata_replication(),
        Some(metadata_replication)
    );
    assert_eq!(left.materialized_realm_config_discovery(), Some(discovery));
    assert!(left.conflicts.is_empty());
    assert!(right.conflicts.is_empty());
}

#[test]
fn concurrent_realm_config_settings_conflict_is_order_independent() {
    let first_metadata = MetadataReplicationConfig::new(3);
    let second_metadata = MetadataReplicationConfig::new(5);
    let discovery = RealmDiscoveryConfig::Dynamic {
        methods: Vec::new(),
    };
    let first = set_realm_config_settings(1, 1, first_metadata, discovery.clone());
    let second = set_realm_config_settings(2, 2, second_metadata, discovery.clone());

    let mut left = realm_config_state();
    left.apply(&first).unwrap();
    left.apply(&second).unwrap();

    let mut right = realm_config_state();
    right.apply(&second).unwrap();
    right.apply(&first).unwrap();

    assert_eq!(left.conflicts, right.conflicts);
    assert_eq!(left.materialized_realm_config_metadata_replication(), None);
    assert_eq!(right.materialized_realm_config_metadata_replication(), None);
    assert_eq!(
        left.materialized_realm_config_discovery(),
        Some(discovery.clone())
    );
    assert_eq!(right.materialized_realm_config_discovery(), Some(discovery));
}

#[test]
fn concurrent_realm_config_same_oidc_provider_different_body_conflicts_fail_closed() {
    let mut state = realm_config_state();
    let first = oidc_provider("default", "one");
    let second = oidc_provider("default", "two");
    let first_value = oidc_provider_value(&first);
    let second_value = oidc_provider_value(&second);

    state.apply(&upsert_oidc_provider(1, 1, first)).unwrap();
    state.apply(&upsert_oidc_provider(2, 2, second)).unwrap();

    assert!(
        !state
            .materialized_realm_config_oidc_providers()
            .contains_key("default")
    );
    let conflict = state
        .conflicts
        .get("realm_config.oidc_providers.default")
        .expect("conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(first_value.as_str()))
    );
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(second_value.as_str()))
    );
}

#[test]
fn observed_realm_config_oidc_provider_remove_removes_provider() {
    let mut state = realm_config_state();
    let provider = oidc_provider("default", "one");
    let upsert_origin = node(1);
    let upsert = realm_config_event(
        1,
        upsert_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider },
    );
    let removal = realm_config_event(
        2,
        node(2),
        1,
        AdminDocumentClock::default().with_observed(upsert_origin, 1),
        AdminDocumentOperation::RealmConfigOidcProviderRemoved {
            provider_id: "default".to_string(),
        },
    );

    state.apply(&upsert).unwrap();
    state.apply(&removal).unwrap();

    assert!(state.materialized_realm_config_oidc_providers().is_empty());
    assert!(state.conflicts.is_empty());
}

#[test]
fn realm_config_settings_materialize_metadata_replication_and_discovery() {
    let mut state = realm_config_state();
    let metadata_replication = MetadataReplicationConfig::new(5);
    let discovery = RealmDiscoveryConfig::Static {
        endpoints: Vec::new(),
    };

    state
        .apply(&set_realm_config_settings(
            1,
            1,
            metadata_replication.clone(),
            discovery.clone(),
        ))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_metadata_replication(),
        Some(metadata_replication)
    );
    assert_eq!(state.materialized_realm_config_discovery(), Some(discovery));
    assert!(state.conflicts.is_empty());
}

#[test]
fn realm_config_description_materializes() {
    let mut state = realm_config_state();

    state
        .apply(&set_realm_config_description(1, 1, "Demo Realm"))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_description().as_deref(),
        Some("Demo Realm")
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn keeps_device_cap() {
    // The device cap and the per-device limits materialize like any other
    // quota field.
    let mut state = realm_config_state();
    let quota = QuotaConfig {
        default_group_quota_bytes: Some(1_000),
        max_devices_per_user: Some(6),
        device_requests_per_minute: Some(120),
        device_concurrent_pulls: Some(4),
        ..QuotaConfig::default()
    };
    let expected = quota.clone();

    state
        .apply(&realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigQuotaSet {
                quota: quota.clone(),
            },
        ))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_quota(),
        Some(expected.clone())
    );

    let stored_value = state
        .user_subject_ids
        .get(REALM_CONFIG_QUOTA_PATH)
        .and_then(|version| version.value.as_deref())
        .expect("quota reducer value exists")
        .to_string();
    let stored_quota: QuotaConfig = serde_json::from_str(&stored_value).unwrap();
    assert_eq!(stored_quota, expected);

    state
        .user_subject_ids
        .get_mut(REALM_CONFIG_QUOTA_PATH)
        .expect("quota reducer value exists")
        .value = Some(serde_json::to_string(&quota).unwrap());
    assert_eq!(state.materialized_realm_config_quota(), Some(expected));
}

#[test]
fn realm_config_quota_override_order_is_canonical_for_conflict_detection() {
    let group_a = Ulid::from_bytes([1; 16]);
    let group_b = Ulid::from_bytes([2; 16]);
    let user_a = user_id_with_seed(3);
    let user_b = user_id_with_seed(4);
    let expected = QuotaConfig {
        default_group_quota_bytes: Some(1_000),
        grace_factor_percent: 125,
        warn_threshold_percent: 80,
        group_overrides: vec![
            GroupQuotaOverride {
                group_id: group_a,
                quota_bytes: Some(500),
                grace_factor_percent: None,
            },
            GroupQuotaOverride {
                group_id: group_b,
                quota_bytes: Some(750),
                grace_factor_percent: Some(150),
            },
        ],
        max_groups_per_user: Some(4),
        user_group_cap_overrides: vec![
            UserGroupCapOverride {
                user_id: user_a,
                max_groups: Some(2),
            },
            UserGroupCapOverride {
                user_id: user_b,
                max_groups: Some(3),
            },
        ],
        max_devices_per_user: Some(6),
        ..QuotaConfig::default()
    };
    let reordered = QuotaConfig {
        group_overrides: expected.group_overrides.iter().cloned().rev().collect(),
        user_group_cap_overrides: expected
            .user_group_cap_overrides
            .iter()
            .cloned()
            .rev()
            .collect(),
        max_devices_per_user: Some(6),
        ..expected.clone()
    };

    let first = realm_config_event(
        1,
        node(1),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigQuotaSet {
            quota: expected.clone(),
        },
    );
    let second = realm_config_event(
        2,
        node(2),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigQuotaSet { quota: reordered },
    );

    let mut state = realm_config_state();
    state.apply(&first).unwrap();
    state.apply(&second).unwrap();

    assert!(state.conflicts.is_empty());
    assert_eq!(
        state.materialized_realm_config_quota(),
        Some(expected.clone())
    );

    let stored_value = state
        .user_subject_ids
        .get(REALM_CONFIG_QUOTA_PATH)
        .and_then(|version| version.value.as_deref())
        .expect("quota reducer value exists");
    let stored_quota: QuotaConfig = serde_json::from_str(stored_value).unwrap();
    assert_eq!(stored_quota, expected);
}

#[test]
fn realm_config_settings_metadata_conflict_withholds_only_metadata_replication() {
    let mut state = realm_config_state();
    let first_metadata = MetadataReplicationConfig::new(3);
    let second_metadata = MetadataReplicationConfig::new(5);
    let discovery = RealmDiscoveryConfig::Dynamic {
        methods: Vec::new(),
    };
    let first_value = metadata_replication_value(&first_metadata);
    let second_value = metadata_replication_value(&second_metadata);

    state
        .apply(&set_realm_config_settings(
            1,
            1,
            first_metadata,
            discovery.clone(),
        ))
        .unwrap();
    state
        .apply(&set_realm_config_settings(
            2,
            2,
            second_metadata,
            discovery.clone(),
        ))
        .unwrap();

    assert_eq!(state.materialized_realm_config_metadata_replication(), None);
    assert_eq!(state.materialized_realm_config_discovery(), Some(discovery));
    assert!(!state.conflicts.contains_key(REALM_CONFIG_DISCOVERY_PATH));
    let conflict = state
        .conflicts
        .get(REALM_CONFIG_METADATA_REPLICATION_PATH)
        .expect("conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(first_value.as_str()))
    );
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(second_value.as_str()))
    );
}

#[test]
fn realm_config_settings_discovery_conflict_withholds_only_discovery() {
    let mut state = realm_config_state();
    let metadata_replication = MetadataReplicationConfig::new(3);
    let first_discovery = RealmDiscoveryConfig::Static {
        endpoints: Vec::new(),
    };
    let second_discovery = RealmDiscoveryConfig::Dynamic {
        methods: Vec::new(),
    };
    let first_value = realm_discovery_value(&first_discovery);
    let second_value = realm_discovery_value(&second_discovery);

    state
        .apply(&set_realm_config_settings(
            1,
            1,
            metadata_replication.clone(),
            first_discovery,
        ))
        .unwrap();
    state
        .apply(&set_realm_config_settings(
            2,
            2,
            metadata_replication.clone(),
            second_discovery,
        ))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_metadata_replication(),
        Some(metadata_replication)
    );
    assert_eq!(state.materialized_realm_config_discovery(), None);
    assert!(
        !state
            .conflicts
            .contains_key(REALM_CONFIG_METADATA_REPLICATION_PATH)
    );
    let conflict = state
        .conflicts
        .get(REALM_CONFIG_DISCOVERY_PATH)
        .expect("conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(first_value.as_str()))
    );
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(second_value.as_str()))
    );
}
