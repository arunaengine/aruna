use super::*;

#[test]
fn group_policies_materialize() {
    let mut state = group_state();
    let policies = vec![crate::request_policy::RequestPolicy {
        policy_id: Ulid::from_bytes([2; 16]),
        name: "no-writes".to_string(),
        kind: crate::request_policy::PolicyKind::Deny,
        when: None,
        expression: "permission == 'write'".to_string(),
        enabled: true,
    }];
    state
        .apply_operation(
            &actor(node(1)),
            AdminDocumentOperation::GroupPoliciesSet {
                policies: policies.clone(),
            },
        )
        .unwrap();
    assert_eq!(state.materialized_group_policies(), Some(policies));
}

#[test]
fn group_created_materializes_display_name_realm_id_and_owner() {
    let mut state = group_state();
    let realm_id = realm_id();
    let owner = user_id_with_seed(5);

    state
        .apply(&create_group(1, 1, "Engineering", realm_id))
        .unwrap();

    assert_eq!(
        state.materialized_group_display_name().as_deref(),
        Some("Engineering")
    );
    assert_eq!(state.materialized_group_realm_id(), Some(realm_id));
    assert_eq!(state.materialized_group_owner(), Some(owner));
    assert!(state.conflicts.is_empty());
}

#[test]
fn group_created_display_name_conflict_withholds_only_display_name() {
    let mut state = group_state();
    let realm_id = realm_id();

    state
        .apply(&create_group(1, 1, "Engineering", realm_id))
        .unwrap();
    state
        .apply(&create_group(2, 2, "Research", realm_id))
        .unwrap();

    assert_eq!(state.materialized_group_display_name(), None);
    assert_eq!(state.materialized_group_realm_id(), Some(realm_id));
    assert!(!state.conflicts.contains_key(GROUP_REALM_ID_PATH));

    let conflict = state
        .conflicts
        .get(GROUP_DISPLAY_NAME_PATH)
        .expect("display name conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some("Engineering"))
    );
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some("Research"))
    );
}

#[test]
fn rename_replaces_name() {
    let mut state = group_state();
    state
        .apply(&create_group(1, 1, "Engineering", realm_id()))
        .unwrap();
    state.apply(&rename_group(2, 1, 2, "Platform")).unwrap();

    assert_eq!(
        state.materialized_group_display_name().as_deref(),
        Some("Platform")
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn older_rename_stale() {
    // The second rename observes the first, so replaying the first loses.
    let mut state = group_state();
    state
        .apply(&create_group(1, 1, "Engineering", realm_id()))
        .unwrap();
    let first = rename_group(2, 1, 2, "Platform");
    let second = rename_group(3, 1, 3, "Infrastructure");
    state.apply(&second).unwrap();
    state.apply(&first).unwrap();

    assert_eq!(
        state.materialized_group_display_name().as_deref(),
        Some("Infrastructure")
    );
}

#[test]
fn concurrent_renames_conflict() {
    let mut state = group_state();
    state
        .apply(&create_group(1, 1, "Engineering", realm_id()))
        .unwrap();
    state.apply(&rename_group(2, 2, 1, "Platform")).unwrap();
    state.apply(&rename_group(3, 3, 1, "Research")).unwrap();

    assert_eq!(state.materialized_group_display_name(), None);
    let conflict = state
        .conflicts
        .get(GROUP_DISPLAY_NAME_PATH)
        .expect("display name conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
}

#[test]
fn group_created_realm_id_conflict_withholds_only_realm_id() {
    let mut state = group_state();
    let first_realm_id = realm_id_with_seed(9);
    let second_realm_id = realm_id_with_seed(10);
    let first_realm_value = first_realm_id.to_string();
    let second_realm_value = second_realm_id.to_string();

    state
        .apply(&create_group(1, 1, "Engineering", first_realm_id))
        .unwrap();
    state
        .apply(&create_group(2, 2, "Engineering", second_realm_id))
        .unwrap();

    assert_eq!(
        state.materialized_group_display_name().as_deref(),
        Some("Engineering")
    );
    assert_eq!(state.materialized_group_realm_id(), None);
    assert!(!state.conflicts.contains_key(GROUP_DISPLAY_NAME_PATH));

    let conflict = state
        .conflicts
        .get(GROUP_REALM_ID_PATH)
        .expect("realm id conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(first_realm_value.as_str()))
    );
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(second_realm_value.as_str()))
    );
}

#[test]
fn group_created_operation_is_rejected_for_non_group_target_without_state_change() {
    let mut state = user_state();
    let before = state.clone();
    let event = event(
        1,
        node(1),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::GroupCreated {
            realm_id: realm_id(),
            display_name: "Engineering".to_string(),
            owner: user_id_with_seed(5),
        },
    );

    assert_eq!(
        state.apply(&event),
        Err(AdminDocumentReducerError::UnsupportedTarget)
    );
    assert_eq!(state, before);
}
