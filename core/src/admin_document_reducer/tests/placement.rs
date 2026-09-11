use super::*;

pub(super) fn placement_entry(node_id: NodeId, weight: u32) -> NodePlacementEntry {
    NodePlacementEntry {
        node_id,
        location: "eu-west".to_string(),
        weight,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    }
}

pub(super) fn placement_strategy(
    strategy_id: Ulid,
    replica_count: Option<u32>,
) -> PlacementStrategy {
    PlacementStrategy {
        strategy_id,
        name: "default".to_string(),
        replica_count,
        distinct_locations: false,
        affinity: vec![AffinityRule {
            matcher: LabelMatch {
                key: "tier".to_string(),
                value: "hot".to_string(),
            },
            effect: AffinityEffect::Filter,
        }],
        shard_count: 64,
    }
}

pub(super) fn set_placement_entry(
    event_seed: u8,
    origin_seed: u8,
    entry: NodePlacementEntry,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigNodePlacementSet { entry },
    )
}

pub(super) fn upsert_placement_strategy(
    state: &mut AdminDocumentReducerState,
    event_seed: u8,
    origin_seed: u8,
    strategy_id: Ulid,
) {
    state
        .apply(&realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: placement_strategy(strategy_id, Some(3)),
            },
        ))
        .unwrap();
}

#[test]
pub(super) fn admin_document_placement_paths_preserve_strings_and_round_trip() {
    let node_id = node(6);
    let strategy_id = Ulid::from_bytes([4; 16]);

    let node_path = realm_config_placement_node_path(&node_id);
    assert_eq!(node_path, format!("realm_config.placement.nodes.{node_id}"));
    assert_eq!(
        realm_config_placement_node_id_from_path(&node_path),
        Some(node_id)
    );

    let strategy_path = realm_config_placement_strategy_path(&strategy_id);
    assert_eq!(
        strategy_path,
        format!("realm_config.placement.strategies.{strategy_id}")
    );
    assert_eq!(
        realm_config_placement_strategy_id_from_path(&strategy_path),
        Some(strategy_id)
    );

    assert_eq!(
        REALM_CONFIG_DEFAULT_STRATEGY_PATH,
        "realm_config.placement.default_strategy"
    );
    assert_eq!(
        realm_config_placement_node_id_from_path("realm_config.placement.nodes.invalid"),
        None
    );
}

#[test]
pub(super) fn binding_scope_keys_use_stable_canonical_text_and_parse_from_paths() {
    let group_id = group_id();
    let cases = [
        (BindingScope::Realm, "realm".to_string()),
        (BindingScope::Group(group_id), format!("group:{group_id}")),
        (
            BindingScope::Class(DocumentClass::Admin),
            "class:admin".to_string(),
        ),
        (
            BindingScope::Class(DocumentClass::Group),
            "class:group".to_string(),
        ),
        (
            BindingScope::Class(DocumentClass::User),
            "class:user".to_string(),
        ),
        (
            BindingScope::Class(DocumentClass::Metadata),
            "class:metadata".to_string(),
        ),
        (
            BindingScope::Class(DocumentClass::MetadataRegistry),
            "class:metadata_registry".to_string(),
        ),
        (
            BindingScope::Class(DocumentClass::JobControl),
            "class:job_control".to_string(),
        ),
        (
            BindingScope::MetadataPathPrefix(" /datasets/important/ ".to_string()),
            "metadata_path_prefix:datasets/important".to_string(),
        ),
    ];

    for (scope, expected_key) in cases {
        assert_eq!(binding_scope_key(&scope), expected_key);

        let path = realm_config_strategy_binding_path(&scope);
        assert_eq!(
            path,
            format!("realm_config.placement.bindings.{expected_key}")
        );
        assert_eq!(
            realm_config_strategy_binding_scope_key_from_path(&path),
            Some(expected_key.as_str())
        );
    }
}

#[test]
fn realm_config_placement_overlay_replaces_owned_paths_deterministically() {
    let owned_node = node(11);
    let unowned_node = node(12);
    let owned_entry = placement_entry(owned_node, 250);
    let unowned_entry = placement_entry(unowned_node, 100);
    let owned_strategy = placement_strategy(Ulid::from_bytes([4; 16]), Some(3));
    let unowned_strategy = placement_strategy(Ulid::from_bytes([5; 16]), None);
    let owned_binding = StrategyBinding {
        scope: BindingScope::Class(DocumentClass::MetadataRegistry),
        strategy_id: owned_strategy.strategy_id,
    };
    let unowned_binding = StrategyBinding {
        scope: BindingScope::Realm,
        strategy_id: unowned_strategy.strategy_id,
    };
    let owned_override = PlacementOverride {
        subject: b"owned".to_vec(),
        pinned: vec![owned_node],
        excluded: Vec::new(),
        strategy_id: Some(owned_strategy.strategy_id),
    };
    let unowned_override = PlacementOverride {
        subject: b"unowned".to_vec(),
        pinned: vec![unowned_node],
        excluded: Vec::new(),
        strategy_id: Some(unowned_strategy.strategy_id),
    };

    let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
    config.placement_map = vec![unowned_entry.clone(), placement_entry(owned_node, 1)];
    config.strategies = vec![
        unowned_strategy.clone(),
        placement_strategy(owned_strategy.strategy_id, Some(1)),
    ];
    config.default_strategy_id = Some(unowned_strategy.strategy_id);
    config.strategy_bindings = vec![
        unowned_binding.clone(),
        StrategyBinding {
            scope: owned_binding.scope.clone(),
            strategy_id: unowned_strategy.strategy_id,
        },
    ];
    config.placement_overrides = vec![
        unowned_override.clone(),
        PlacementOverride {
            subject: owned_override.subject.clone(),
            pinned: Vec::new(),
            excluded: vec![owned_node],
            strategy_id: None,
        },
    ];

    let untouched = config.clone();
    overlay_realm_config_placement_reducer_materialization(&mut config, &realm_config_state(), 0);
    assert_eq!(config, untouched);

    let mut state = realm_config_state();
    let actor = actor(node(1));
    for op in [
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: owned_entry.clone(),
        },
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
            strategy: owned_strategy.clone(),
        },
        AdminDocumentOperation::RealmConfigDefaultStrategySet {
            strategy_id: owned_strategy.strategy_id,
        },
        AdminDocumentOperation::RealmConfigStrategyBindingSet {
            binding: owned_binding.clone(),
        },
        AdminDocumentOperation::RealmConfigPlacementOverrideSet {
            record: owned_override.clone(),
        },
    ] {
        state.apply_operation(&actor, op).unwrap();
    }

    overlay_realm_config_placement_reducer_materialization(&mut config, &state, 0);
    assert_eq!(config.placement_map, vec![unowned_entry, owned_entry]);
    assert_eq!(
        config.strategies,
        vec![unowned_strategy.clone(), owned_strategy.clone()]
    );
    assert_eq!(config.default_strategy_id, Some(owned_strategy.strategy_id));
    assert_eq!(
        config.strategy_bindings,
        vec![unowned_binding, owned_binding]
    );
    assert_eq!(
        config.placement_overrides,
        vec![unowned_override, owned_override]
    );

    let materialized = config.clone();
    overlay_realm_config_placement_reducer_materialization(&mut config, &state, 0);
    assert_eq!(config, materialized);
}

#[test]
fn realm_config_placement_repair_clears_refs_but_preserves_override_without_live_strategy() {
    let missing_strategy_id = Ulid::from_bytes([8; 16]);
    let pinned = node(11);
    let excluded = node(12);
    let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
    config.default_strategy_id = Some(missing_strategy_id);
    config.strategy_bindings = vec![StrategyBinding {
        scope: BindingScope::Realm,
        strategy_id: missing_strategy_id,
    }];
    config.placement_overrides = vec![PlacementOverride {
        subject: b"document-subject".to_vec(),
        pinned: vec![pinned],
        excluded: vec![excluded],
        strategy_id: Some(missing_strategy_id),
    }];

    overlay_realm_config_placement_reducer_materialization(&mut config, &realm_config_state(), 0);

    assert_eq!(config.default_strategy_id, None);
    assert!(config.strategy_bindings.is_empty());
    assert_eq!(config.placement_overrides.len(), 1);
    assert_eq!(config.placement_overrides[0].strategy_id, None);
    assert_eq!(config.placement_overrides[0].pinned, vec![pinned]);
    assert_eq!(config.placement_overrides[0].excluded, vec![excluded]);
}

#[test]
fn realm_config_placement_entry_materializes() {
    let mut state = realm_config_state();
    let config_node = node(11);
    let entry = placement_entry(config_node, 250);

    state
        .apply(&set_placement_entry(1, 1, entry.clone()))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_placement_map(),
        BTreeMap::from([(config_node, entry)])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn realm_config_disjoint_placement_entries_merge_deterministically() {
    let first_node = node(11);
    let second_node = node(12);
    let first = placement_entry(first_node, 100);
    let second = placement_entry(second_node, 200);
    let first_event = set_placement_entry(1, 1, first.clone());
    let second_event = set_placement_entry(2, 2, second.clone());

    let mut left = realm_config_state();
    left.apply(&first_event).unwrap();
    left.apply(&second_event).unwrap();

    let mut right = realm_config_state();
    right.apply(&second_event).unwrap();
    right.apply(&first_event).unwrap();

    assert_eq!(left.user_subject_ids, right.user_subject_ids);
    assert_eq!(
        left.materialized_realm_config_placement_map(),
        BTreeMap::from([(first_node, first), (second_node, second)])
    );
    assert!(left.conflicts.is_empty());
}

#[test]
fn concurrent_realm_config_same_placement_node_conflicts_fail_closed() {
    let mut state = realm_config_state();
    let config_node = node(11);

    state
        .apply(&set_placement_entry(
            1,
            1,
            placement_entry(config_node, 100),
        ))
        .unwrap();
    state
        .apply(&set_placement_entry(
            2,
            2,
            placement_entry(config_node, 250),
        ))
        .unwrap();

    assert!(
        !state
            .materialized_realm_config_placement_map()
            .contains_key(&config_node)
    );
    let conflict = state
        .conflicts
        .get(&realm_config_placement_node_path(&config_node))
        .expect("conflict is recorded");
    assert_eq!(conflict.values.len(), 2);
}

#[test]
fn equal_concurrent_placement_writes_preserve_causal_frontier() {
    let config_node = node(11);
    let origin_a = node(1);
    let origin_b = node(2);
    let first_a = realm_config_event(
        1,
        origin_a,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: placement_entry(config_node, 100),
        },
    );
    let first_b = realm_config_event(
        2,
        origin_b,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: placement_entry(config_node, 100),
        },
    );
    let second_a = realm_config_event(
        3,
        origin_a,
        2,
        AdminDocumentClock::default().with_observed(origin_a, 1),
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: placement_entry(config_node, 250),
        },
    );
    let events = [first_a, first_b, second_a];
    let mut states = Vec::new();
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let mut state = realm_config_state();
        for index in order {
            state.apply(&events[index]).unwrap();
        }
        states.push(state);
    }

    for state in &states[1..] {
        assert_eq!(state, &states[0]);
    }
    let state = &states[0];
    assert!(
        !state
            .materialized_realm_config_placement_map()
            .contains_key(&config_node)
    );
    let conflict = state
        .conflicts
        .get(&realm_config_placement_node_path(&config_node))
        .expect("causally concurrent values conflict");
    assert_eq!(
        conflict
            .values
            .iter()
            .map(|candidate| candidate.dot)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([events[1].dot(), events[2].dot()])
    );
}

#[test]
fn observed_realm_config_placement_entry_remove_removes_entry() {
    let mut state = realm_config_state();
    let config_node = node(11);
    let set_origin = node(1);
    let set = realm_config_event(
        1,
        set_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: placement_entry(config_node, 100),
        },
    );
    let removal = realm_config_event(
        2,
        node(2),
        1,
        AdminDocumentClock::default().with_observed(set_origin, 1),
        AdminDocumentOperation::RealmConfigNodePlacementRemoved {
            node_id: config_node,
        },
    );

    state.apply(&set).unwrap();
    state.apply(&removal).unwrap();

    assert!(state.materialized_realm_config_placement_map().is_empty());
    assert!(state.conflicts.is_empty());
}

#[test]
fn rejects_derived_labels() {
    // Both the kind label and any storage class are stamped by the node.
    for key in [
        KIND_LABEL_KEY.to_string(),
        format!("{STORAGE_CLASS_LABEL_PREFIX}cold"),
    ] {
        let mut state = realm_config_state();
        let before = state.clone();
        let mut entry = placement_entry(node(11), 100);
        entry.labels.insert(key.clone(), "Server".to_string());

        assert_eq!(
            state.apply(&set_placement_entry(1, 1, entry)),
            Err(AdminDocumentReducerError::ReservedPlacementLabel(key))
        );
        assert_eq!(state, before);
    }
}

#[test]
fn realm_config_placement_strategy_materializes() {
    let mut state = realm_config_state();
    let strategy_id = Ulid::from_bytes([4; 16]);
    let strategy = placement_strategy(strategy_id, Some(3));

    state
        .apply(&realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: strategy.clone(),
            },
        ))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_placement_strategies(),
        BTreeMap::from([(strategy_id, strategy)])
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn strategy_shards_immutable() {
    let mut state = realm_config_state();
    let origin = node(1);
    let strategy_id = Ulid::from_bytes([4; 16]);
    let initial = placement_strategy(strategy_id, Some(3));
    let mut renamed = initial.clone();
    renamed.name = "renamed".to_string();

    assert_eq!(
        state.apply(&realm_config_event(
            1,
            origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy: initial },
        )),
        Ok(AdminDocumentApplyStatus::Applied)
    );
    assert_eq!(
        state.apply(&realm_config_event(
            2,
            origin,
            2,
            AdminDocumentClock::default().with_observed(origin, 1),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: renamed.clone(),
            },
        )),
        Ok(AdminDocumentApplyStatus::Applied)
    );

    let before = state.clone();
    let mut changed = renamed;
    changed.shard_count *= 2;
    assert_eq!(
        state.apply(&realm_config_event(
            3,
            origin,
            3,
            AdminDocumentClock::default().with_observed(origin, 2),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy: changed },
        )),
        Err(AdminDocumentReducerError::PlacementShardCountChanged)
    );
    assert_eq!(state, before);
}

#[test]
fn realm_config_placement_strategy_rejects_zero_replica_count() {
    let mut state = realm_config_state();
    let before = state.clone();

    assert_eq!(
        state.apply(&realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: placement_strategy(Ulid::from_bytes([4; 16]), Some(0)),
            },
        )),
        Err(AdminDocumentReducerError::ZeroPlacementReplicaCount)
    );
    assert_eq!(state, before);
}
