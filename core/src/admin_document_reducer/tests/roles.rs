use super::*;

#[test]
fn group_role_and_user_assignment_materialize() {
    let mut state = group_state();
    let role_id = role_id(3);
    let user_id = user_id_with_seed(4);

    state.apply(&add_group_role(1, 1, role_id)).unwrap();
    state
        .apply(&assign_group_role_user(2, 2, role_id, user_id))
        .unwrap();

    assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
    assert_eq!(
        state.materialized_group_role_user_assignments(),
        BTreeMap::from([(role_id, BTreeSet::from([user_id]))])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn group_role_user_assignment_is_hidden_until_role_exists() {
    let mut state = group_state();
    let role_id = role_id(3);
    let user_id = user_id_with_seed(4);

    state
        .apply(&assign_group_role_user(1, 1, role_id, user_id))
        .unwrap();

    assert!(state.materialized_group_roles().is_empty());
    assert!(state.materialized_group_role_user_assignments().is_empty());

    state.apply(&add_group_role(2, 2, role_id)).unwrap();

    assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
    assert_eq!(
        state.materialized_group_role_user_assignments(),
        BTreeMap::from([(role_id, BTreeSet::from([user_id]))])
    );
}

#[test]
fn group_role_body_creation_materializes_role_id_and_records_body() {
    let mut state = group_state();
    let role_id = role_id(3);
    let role = role_definition(role_id, "Group admin");
    let expected_value = role_definition_value(&role);

    state.apply(&create_group_role(1, 1, role)).unwrap();

    assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
    assert_eq!(
        state
            .user_subject_ids
            .get(&format!("group.roles.{role_id}"))
            .and_then(|version| version.value.as_deref()),
        Some(expected_value.as_str())
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn observed_group_role_removal_clears_role_and_assignments() {
    let mut state = group_state();
    let role_id = role_id(3);
    let user_id = user_id_with_seed(4);

    state.apply(&add_group_role(1, 1, role_id)).unwrap();
    state
        .apply(&assign_group_role_user(2, 2, role_id, user_id))
        .unwrap();
    state
        .apply_operation(
            &actor(node(3)),
            AdminDocumentOperation::GroupRoleRemoved { role_id },
        )
        .unwrap();

    assert!(state.materialized_group_roles().is_empty());
    assert!(state.materialized_group_role_user_assignments().is_empty());
    assert!(state.conflicts.is_empty());
}

#[test]
fn realm_role_body_creation_materializes_role_id_and_records_body() {
    let mut state = realm_state();
    let role_id = role_id(3);
    let role = role_definition(role_id, "Realm admin");
    let expected_value = role_definition_value(&role);

    state.apply(&create_realm_role(1, 1, role)).unwrap();

    assert_eq!(state.materialized_realm_roles(), BTreeSet::from([role_id]));
    assert_eq!(
        state
            .user_subject_ids
            .get(&format!("realm.roles.{role_id}"))
            .and_then(|version| version.value.as_deref()),
        Some(expected_value.as_str())
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn same_role_conflicting_body_recording() {
    let mut state = group_state();
    let role_id = role_id(3);
    let first = role_definition(role_id, "Group reader");
    let second = role_definition(role_id, "Group writer");
    let first_value = role_definition_value(&first);
    let second_value = role_definition_value(&second);

    state.apply(&create_group_role(1, 1, first)).unwrap();
    state.apply(&create_group_role(2, 2, second)).unwrap();

    assert!(state.materialized_group_roles().is_empty());
    let conflict = state
        .conflicts
        .get(&format!("group.roles.{role_id}"))
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
fn concurrent_group_role_create_remove_conflict_fails_closed() {
    let mut state = group_state();
    let role_id = role_id(3);
    let role = role_definition(role_id, "Group reader");

    state.apply(&create_group_role(1, 1, role.clone())).unwrap();
    state.apply(&remove_group_role(2, 2, role_id)).unwrap();

    assert!(state.materialized_group_roles().is_empty());
    let conflict = state
        .conflicts
        .get(&format!("group.roles.{role_id}"))
        .expect("conflict is recorded");
    let role_value = role_definition_value(&role);
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(role_value.as_str()))
    );
    assert!(conflict.values.iter().any(|value| value.value.is_none()));
}

#[test]
fn observed_group_role_user_assignment_removal_clears_assignment() {
    let mut state = group_state();
    let role_id = role_id(3);
    let user_id = user_id_with_seed(4);
    let assignment_origin = node(2);
    let assignment = group_event(
        2,
        assignment_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id },
    );
    let removal = group_event(
        3,
        node(3),
        1,
        AdminDocumentClock::default().with_observed(assignment_origin, 1),
        AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id },
    );

    state.apply(&add_group_role(1, 1, role_id)).unwrap();
    state.apply(&assignment).unwrap();
    state.apply(&removal).unwrap();

    assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
    assert!(state.materialized_group_role_user_assignments().is_empty());
    assert!(state.conflicts.is_empty());
}

#[test]
fn concurrent_group_role_user_assignments_converge_independent_of_order() {
    let role_id = role_id(3);
    let first_user_id = user_id_with_seed(4);
    let second_user_id = user_id_with_seed(5);
    let role_seed = add_group_role(1, 1, role_id);
    let first_assignment = assign_group_role_user(2, 2, role_id, first_user_id);
    let second_assignment = assign_group_role_user(3, 3, role_id, second_user_id);

    let mut left = group_state();
    left.apply(&role_seed).unwrap();
    left.apply(&first_assignment).unwrap();
    left.apply(&second_assignment).unwrap();

    let mut right = group_state();
    right.apply(&role_seed).unwrap();
    right.apply(&second_assignment).unwrap();
    right.apply(&first_assignment).unwrap();

    let expected = BTreeMap::from([(role_id, BTreeSet::from([first_user_id, second_user_id]))]);
    assert_eq!(left.materialized_group_role_user_assignments(), expected);
    assert_eq!(right.materialized_group_role_user_assignments(), expected);
    assert_eq!(left.conflicts, right.conflicts);
    assert!(left.conflicts.is_empty());
}

#[test]
fn concurrent_group_role_additions_converge_independent_of_order() {
    let first_role = role_definition(role_id(3), "Group reader");
    let second_role = role_definition(role_id(4), "Group writer");
    let first = create_group_role(1, 1, first_role.clone());
    let second = create_group_role(2, 2, second_role.clone());

    let mut left = group_state();
    left.apply(&first).unwrap();
    left.apply(&second).unwrap();

    let mut right = group_state();
    right.apply(&second).unwrap();
    right.apply(&first).unwrap();

    let expected_roles = BTreeSet::from([first_role.role_id, second_role.role_id]);
    assert_eq!(left.materialized_group_roles(), expected_roles);
    assert_eq!(right.materialized_group_roles(), expected_roles);
    assert_eq!(left.user_subject_ids, right.user_subject_ids);
    assert!(left.conflicts.is_empty());
    assert!(right.conflicts.is_empty());
}

#[test]
fn concurrent_group_role_user_assignment_add_remove_conflict_fails_closed() {
    let mut state = group_state();
    let role_id = role_id(3);
    let user_id = user_id_with_seed(4);

    state.apply(&add_group_role(1, 1, role_id)).unwrap();
    state
        .apply(&assign_group_role_user(2, 2, role_id, user_id))
        .unwrap();
    state
        .apply(&remove_group_role_user_assignment(3, 3, role_id, user_id))
        .unwrap();

    assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
    assert!(state.materialized_group_role_user_assignments().is_empty());
    let conflict = state
        .conflicts
        .get(&format!("group.roles.{role_id}.assigned_users.{user_id}"))
        .expect("conflict is recorded");
    let expected_user_id = user_id.to_string();
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(expected_user_id.as_str()))
    );
    assert!(conflict.values.iter().any(|value| value.value.is_none()));
}

#[test]
fn realm_role_and_user_assignment_materialize() {
    let mut state = realm_state();
    let role_id = role_id(3);
    let user_id = user_id_with_seed(4);

    state.apply(&add_realm_role(1, 1, role_id)).unwrap();
    state
        .apply(&assign_realm_role_user(2, 2, role_id, user_id))
        .unwrap();

    assert_eq!(state.materialized_realm_roles(), BTreeSet::from([role_id]));
    assert_eq!(
        state.materialized_realm_role_user_assignments(),
        BTreeMap::from([(role_id, BTreeSet::from([user_id]))])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn concurrent_realm_role_user_assignment_add_remove_conflict_fails_closed() {
    let mut state = realm_state();
    let role_id = role_id(3);
    let user_id = user_id_with_seed(4);

    state.apply(&add_realm_role(1, 1, role_id)).unwrap();
    state
        .apply(&assign_realm_role_user(2, 2, role_id, user_id))
        .unwrap();
    state
        .apply(&remove_realm_role_user_assignment(3, 3, role_id, user_id))
        .unwrap();

    assert_eq!(state.materialized_realm_roles(), BTreeSet::from([role_id]));
    assert!(state.materialized_realm_role_user_assignments().is_empty());
    let conflict = state
        .conflicts
        .get(&format!("realm.roles.{role_id}.assigned_users.{user_id}"))
        .expect("conflict is recorded");
    let expected_user_id = user_id.to_string();
    assert_eq!(conflict.values.len(), 2);
    assert!(
        conflict
            .values
            .iter()
            .any(|value| value.value.as_deref() == Some(expected_user_id.as_str()))
    );
    assert!(conflict.values.iter().any(|value| value.value.is_none()));
}
