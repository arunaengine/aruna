use super::*;

#[test]
fn apply_operation_event() {
    let mut state = user_state();
    let actor = actor(node(1));

    let first = state
        .apply_operation(
            &actor,
            AdminDocumentOperation::UserNameSet {
                name: "Alice".to_string(),
            },
        )
        .unwrap();
    let second = state
        .apply_operation(
            &actor,
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "biology".to_string(),
            },
        )
        .unwrap();

    assert_eq!(first.origin_seq, 1);
    assert_eq!(first.observed.sequence_for(&actor.node_id), 0);
    assert_eq!(second.origin_seq, 2);
    assert_eq!(second.observed.sequence_for(&actor.node_id), 1);
    assert!(second.observed.observes(&first.dot()));
    assert_eq!(state.clock.sequence_for(&actor.node_id), 2);
    assert_eq!(state.materialized_user_name().as_deref(), Some("Alice"));
    assert_eq!(
        state.materialized_user_attributes().get("department"),
        Some(&"biology".to_string())
    );
}

#[test]
fn admin_document_trip() {
    let role_id = role_id(4);
    let user_id = user_id_seed(5);
    let node_id = node(6);

    assert_eq!(USER_NAME_PATH, "user.name");
    assert_eq!(GROUP_DISPLAY_NAME_PATH, "group.display_name");
    assert_eq!(GROUP_REALM_ID_PATH, "group.realm_id");
    assert_eq!(
        REALM_CONFIG_METADATA_REPLICATION_PATH,
        "realm_config.settings.metadata_replication"
    );
    assert_eq!(
        REALM_CONFIG_DISCOVERY_PATH,
        "realm_config.settings.discovery"
    );
    assert_eq!(REALM_CONFIG_DESCRIPTION_PATH, "realm_config.description");
    assert_eq!(
        user_attribute_path("department"),
        "user.attributes.department"
    );
    assert_eq!(user_subject_path("subject-1"), "user.subject_ids.subject-1");

    let group_role = group_role_path(&role_id);
    let group_assignment = group_user_path(&role_id, &user_id);
    assert_eq!(group_role, format!("group.roles.{role_id}"));
    assert_eq!(
        group_assignment,
        format!("group.roles.{role_id}.assigned_users.{user_id}")
    );
    assert_eq!(parse_group_role(&group_role), Some(role_id));
    assert_eq!(parse_group_role(&group_assignment), None);
    assert_eq!(
        parse_group_assignment(&group_assignment),
        Some((role_id, user_id))
    );

    let realm_role = realm_role_path(&role_id);
    let realm_assignment = realm_user_path(&role_id, &user_id);
    assert_eq!(realm_role, format!("realm.roles.{role_id}"));
    assert_eq!(
        realm_assignment,
        format!("realm.roles.{role_id}.assigned_users.{user_id}")
    );
    assert_eq!(parse_realm_role(&realm_role), Some(role_id));
    assert_eq!(parse_realm_role(&realm_assignment), None);
    assert_eq!(
        parse_realm_assignment(&realm_assignment),
        Some((role_id, user_id))
    );

    let node_path = config_node_path(&node_id);
    assert_eq!(node_path, format!("realm_config.nodes.{node_id}"));
    assert_eq!(parse_config_node(&node_path), Some(node_id));
    assert_eq!(
        config_oidc_path("default"),
        "realm_config.oidc_providers.default"
    );
    assert_eq!(
        parse_config_oidc("realm_config.oidc_providers.default"),
        Some("default")
    );

    assert_eq!(parse_group_assignment("group.roles.invalid"), None);
    assert_eq!(parse_realm_assignment("realm.roles.invalid"), None);
    assert_eq!(parse_config_node("realm_config.nodes.invalid"), None);
    assert_eq!(parse_config_oidc("unknown.path"), None);
}

#[test]
fn user_disjoint_merge() {
    let mut state = user_state();

    assert_eq!(
        state.apply(&set_attr(1, 1, "orcid", "0000-0002-1825-0097")),
        Ok(AdminDocumentApplyStatus::Applied)
    );
    assert_eq!(
        state.apply(&set_attr(2, 2, "department", "biology")),
        Ok(AdminDocumentApplyStatus::Applied)
    );

    assert_eq!(
        state.materialized_user_attributes(),
        BTreeMap::from([
            ("department".to_string(), "biology".to_string()),
            ("orcid".to_string(), "0000-0002-1825-0097".to_string()),
        ])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn invalid_user_change() {
    let mut state = user_state();
    let before = state.clone();

    assert_eq!(
        state.apply(&set_attr(1, 1, "display name", "biology")),
        Err(AdminDocumentReducerError::InvalidUserAttribute(
            UserAttributeValidationError::InvalidKey("display name".to_string())
        ))
    );
    assert_eq!(state, before);
}

#[test]
fn invalid_attribute_change() {
    let mut state = user_state();
    let before = state.clone();

    assert_eq!(
        state.apply(&set_attr(1, 1, "department", "bio\nmedicine")),
        Err(AdminDocumentReducerError::InvalidUserAttribute(
            UserAttributeValidationError::InvalidValue("department".to_string())
        ))
    );
    assert_eq!(state, before);
}

#[test]
fn same_user_recorded() {
    let mut state = user_state();

    state
        .apply(&set_attr(1, 1, "department", "physics"))
        .unwrap();
    state
        .apply(&set_attr(2, 2, "department", "biology"))
        .unwrap();

    assert!(
        !state
            .materialized_user_attributes()
            .contains_key("department")
    );
    let conflict = state
        .conflicts
        .get("user.attributes.department")
        .expect("conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some("physics"))
    );
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some("biology"))
    );
}

#[test]
fn disjoint_subject_merge() {
    let mut state = user_state();

    state.apply(&add_subject(1, 1, "subject-1")).unwrap();
    state.apply(&add_subject(2, 2, "subject-2")).unwrap();

    assert_eq!(
        state.materialized_subject_ids(),
        BTreeSet::from(["subject-1".to_string(), "subject-2".to_string()])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn subject_add_absent() {
    let mut state = user_state();

    state.apply(&add_subject(1, 1, "subject-1")).unwrap();
    state.apply(&remove_subject(2, 2, "subject-1")).unwrap();

    assert!(!state.materialized_subject_ids().contains("subject-1"));
    let conflict = state
        .conflicts
        .get("user.subject_ids.subject-1")
        .expect("conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some("subject-1"))
    );
    assert!(conflict.values.iter().any(|value| value.value.is_none()));
}

#[test]
fn duplicate_event_idempotent() {
    let mut state = user_state();
    let event = set_attr(1, 1, "department", "biology");

    assert_eq!(state.apply(&event), Ok(AdminDocumentApplyStatus::Applied));
    let applied_once = state.clone();

    assert_eq!(state.apply(&event), Ok(AdminDocumentApplyStatus::Duplicate));
    assert_eq!(state, applied_once);
}

#[test]
fn same_origin_converge() {
    let origin = node(1);
    let newer = event(
        2,
        origin,
        2,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: "department".to_string(),
            value: "biology".to_string(),
        },
    );
    let stale = event(
        1,
        origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: "orcid".to_string(),
            value: "0000-0002-1825-0097".to_string(),
        },
    );

    let mut newer_first = user_state();
    assert_eq!(
        newer_first.apply(&newer),
        Ok(AdminDocumentApplyStatus::Applied)
    );
    assert_eq!(
        newer_first.apply(&stale),
        Ok(AdminDocumentApplyStatus::Applied)
    );

    let mut older_first = user_state();
    older_first.apply(&stale).unwrap();
    older_first.apply(&newer).unwrap();

    assert_eq!(newer_first, older_first);
    assert_eq!(
        newer_first.materialized_user_attributes(),
        BTreeMap::from([
            ("department".to_string(), "biology".to_string()),
            ("orcid".to_string(), "0000-0002-1825-0097".to_string()),
        ])
    );
    assert_eq!(newer_first.clock.sequence_for(&origin), 2);
}

#[test]
fn same_origin_idempotent() {
    let origin = node(1);
    let older = event(
        1,
        origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: "department".to_string(),
            value: "physics".to_string(),
        },
    );
    let newer = event(
        2,
        origin,
        2,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: "department".to_string(),
            value: "biology".to_string(),
        },
    );

    let mut newer_first = user_state();
    assert_eq!(
        newer_first.apply(&newer),
        Ok(AdminDocumentApplyStatus::Applied)
    );
    let before_stale = newer_first.clone();
    assert_eq!(
        newer_first.apply(&older),
        Ok(AdminDocumentApplyStatus::StaleOriginSequence)
    );
    assert_eq!(
        newer_first.materialized_user_attributes(),
        before_stale.materialized_user_attributes()
    );
    assert!(newer_first.applied_event_ids.contains(&older.event_id));
    assert_eq!(
        newer_first.apply(&newer),
        Ok(AdminDocumentApplyStatus::Duplicate)
    );

    let mut older_first = user_state();
    older_first.apply(&older).unwrap();
    older_first.apply(&newer).unwrap();
    assert_eq!(newer_first, older_first);
    assert_eq!(
        older_first.apply(&older),
        Ok(AdminDocumentApplyStatus::Duplicate)
    );
    assert_eq!(
        newer_first
            .materialized_user_attributes()
            .get("department")
            .map(String::as_str),
        Some("biology")
    );
    assert!(newer_first.conflicts.is_empty());
}

#[test]
fn newer_same_order() {
    let first_origin = node(1);
    let concurrent_origin = node(2);
    let older = event(
        1,
        first_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: "department".to_string(),
            value: "physics".to_string(),
        },
    );
    let concurrent = event(
        2,
        concurrent_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: "department".to_string(),
            value: "chemistry".to_string(),
        },
    );
    let newer = event(
        3,
        first_origin,
        2,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: "department".to_string(),
            value: "biology".to_string(),
        },
    );

    let mut conflict_first = user_state();
    conflict_first.apply(&older).unwrap();
    conflict_first.apply(&concurrent).unwrap();
    conflict_first.apply(&newer).unwrap();

    let mut newer_first = user_state();
    newer_first.apply(&older).unwrap();
    newer_first.apply(&newer).unwrap();
    newer_first.apply(&concurrent).unwrap();

    assert_eq!(conflict_first, newer_first);
    let conflict = conflict_first
        .conflicts
        .get("user.attributes.department")
        .expect("newer and concurrent values conflict");
    assert_eq!(conflict.values.len(), 2);
    assert!(conflict.values.iter().all(|value| value.dot != older.dot()));
}

#[test]
fn same_origin_stale() {
    let origin = node(1);
    let older = realm_config_event(
        1,
        origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigSettingsSet {
            metadata_replication: MetadataReplicationConfig::new(3),
            discovery: RealmDiscoveryConfig::Static {
                endpoints: Vec::new(),
            },
        },
    );
    let newer_metadata = MetadataReplicationConfig::new(5);
    let newer_discovery = RealmDiscoveryConfig::Dynamic {
        methods: Vec::new(),
    };
    let newer = realm_config_event(
        2,
        origin,
        2,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigSettingsSet {
            metadata_replication: newer_metadata.clone(),
            discovery: newer_discovery.clone(),
        },
    );

    let mut newer_first = realm_config_state();
    newer_first.apply(&newer).unwrap();
    assert_eq!(
        newer_first.apply(&older),
        Ok(AdminDocumentApplyStatus::StaleOriginSequence)
    );

    let mut older_first = realm_config_state();
    older_first.apply(&older).unwrap();
    older_first.apply(&newer).unwrap();

    assert_eq!(newer_first, older_first);
    assert_eq!(
        newer_first.materialized_metadata_replication(),
        Some(newer_metadata)
    );
    assert_eq!(
        newer_first.materialized_realm_discovery(),
        Some(newer_discovery)
    );
    assert!(newer_first.conflicts.is_empty());
}

#[test]
fn observed_sequential_value() {
    let mut state = user_state();
    let first_origin = node(1);
    let first = event(
        1,
        first_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: "department".to_string(),
            value: "physics".to_string(),
        },
    );
    let second = event(
        2,
        node(2),
        1,
        AdminDocumentClock::default().with_observed(first_origin, 1),
        AdminDocumentOperation::UserAttributeSet {
            key: "department".to_string(),
            value: "biology".to_string(),
        },
    );

    state.apply(&first).unwrap();
    state.apply(&second).unwrap();

    assert_eq!(
        state
            .materialized_user_attributes()
            .get("department")
            .map(String::as_str),
        Some("biology")
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn observed_name_name() {
    let mut state = user_state();
    let first_origin = node(1);
    let first = event(
        1,
        first_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserNameSet {
            name: "Alice".to_string(),
        },
    );
    let second = event(
        2,
        node(2),
        1,
        AdminDocumentClock::default().with_observed(first_origin, 1),
        AdminDocumentOperation::UserNameSet {
            name: "Bob".to_string(),
        },
    );

    state.apply(&first).unwrap();
    state.apply(&second).unwrap();

    assert_eq!(state.materialized_user_name().as_deref(), Some("Bob"));
    assert!(state.conflicts.is_empty());
}

#[test]
fn concurrent_name_recorded() {
    let mut state = user_state();

    state.apply(&set_name(1, 1, "Alice")).unwrap();
    state.apply(&set_name(2, 2, "Bob")).unwrap();

    assert_eq!(state.materialized_user_name(), None);
    let conflict = state
        .conflicts
        .get(USER_NAME_PATH)
        .expect("conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some("Alice"))
    );
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some("Bob"))
    );
}
