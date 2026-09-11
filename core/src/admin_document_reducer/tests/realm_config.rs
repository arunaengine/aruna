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
