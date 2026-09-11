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

#[test]
fn realm_config_placement_strategy_accepts_max_shard_count() {
    let mut state = realm_config_state();
    let strategy_id = Ulid::from_bytes([4; 16]);
    let mut strategy = placement_strategy(strategy_id, Some(3));
    strategy.shard_count = MAX_PLACEMENT_SHARD_COUNT;

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
fn realm_config_placement_strategy_rejects_shard_count_above_max() {
    let mut state = realm_config_state();
    let before = state.clone();
    let mut strategy = placement_strategy(Ulid::from_bytes([4; 16]), Some(3));
    strategy.shard_count = MAX_PLACEMENT_SHARD_COUNT * 2;

    assert_eq!(
        state.apply(&realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy },
        )),
        Err(AdminDocumentReducerError::InvalidPlacementShardCount)
    );
    assert_eq!(state, before);
}

#[test]
fn realm_config_placement_strategy_rejects_zero_and_non_power_of_two_shard_count() {
    let mut state = realm_config_state();
    let before = state.clone();

    for bad in [0u32, 3, 63] {
        let mut strategy = placement_strategy(Ulid::from_bytes([4; 16]), Some(3));
        strategy.shard_count = bad;
        assert_eq!(
            state.apply(&realm_config_event(
                1,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy },
            )),
            Err(AdminDocumentReducerError::InvalidPlacementShardCount),
            "shard_count {bad} must be rejected"
        );
        assert_eq!(state, before);
    }
}

#[test]
fn realm_config_default_strategy_materializes() {
    let mut state = realm_config_state();
    let strategy_id = Ulid::from_bytes([4; 16]);
    upsert_placement_strategy(&mut state, 9, 9, strategy_id);

    state
        .apply(&realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
        ))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_default_strategy(),
        Some(strategy_id)
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn family_survives_rebuild() {
    // A reducer-only rebuild must reproduce the stored family strategy
    // instead of resetting it to the nil placeholder.
    let mut state = realm_config_state();
    let strategy_id = Ulid::from_bytes([4; 16]);
    upsert_placement_strategy(&mut state, 9, 9, strategy_id);
    state
        .apply(&realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigJobFamilySet { strategy_id },
        ))
        .unwrap();

    assert_eq!(state.materialized_family_strategy(), Some(strategy_id));
    let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
    overlay_realm_config_placement_reducer_materialization(&mut config, &state, 0);
    assert_eq!(config.job_family_strategy_id, strategy_id);
    assert!(config.strategy(&strategy_id).is_some());
}

#[test]
fn rejects_family_mutation() {
    let mut state = realm_config_state();
    let strategy_id = Ulid::from_bytes([4; 16]);
    upsert_placement_strategy(&mut state, 9, 9, strategy_id);
    assert_eq!(
        state.apply(&realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigJobFamilySet {
                strategy_id: Ulid::nil()
            },
        )),
        Err(AdminDocumentReducerError::NilJobFamily)
    );
    state
        .apply(&realm_config_event(
            2,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigJobFamilySet { strategy_id },
        ))
        .unwrap();
    let stored = state.clone();

    assert_eq!(
        state.apply(&realm_config_event(
            3,
            node(1),
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigJobFamilySet {
                strategy_id: Ulid::from_bytes([5; 16])
            },
        )),
        Err(AdminDocumentReducerError::JobFamilyChanged)
    );
    assert_eq!(
        state.apply(&realm_config_event(
            4,
            node(1),
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { strategy_id },
        )),
        Err(AdminDocumentReducerError::JobFamilyRemoved)
    );
    assert_eq!(state, stored);
}

#[test]
fn concurrent_realm_config_strategy_remove_and_references_are_replay_order_independent() {
    let strategy_id = Ulid::from_bytes([4; 16]);
    let fallback_strategy_id = Ulid::from_bytes([3; 16]);
    let subject = b"document-subject".to_vec();
    let reference_ops = vec![
        AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
        AdminDocumentOperation::RealmConfigStrategyBindingSet {
            binding: StrategyBinding {
                scope: BindingScope::MetadataPathPrefix("datasets".to_string()),
                strategy_id,
            },
        },
        AdminDocumentOperation::RealmConfigPlacementOverrideSet {
            record: PlacementOverride {
                subject,
                pinned: vec![node(4)],
                excluded: Vec::new(),
                strategy_id: Some(strategy_id),
            },
        },
    ];

    for (index, reference_op) in reference_ops.into_iter().enumerate() {
        let seed = 40 + index as u8 * 10;
        let strategy_origin = node(seed);
        let mut initial = realm_config_state();
        upsert_placement_strategy(&mut initial, seed, seed, strategy_id);
        let observed_strategy = AdminDocumentClock::default().with_observed(strategy_origin, 1);
        let removal = realm_config_event(
            seed + 1,
            node(seed + 1),
            1,
            observed_strategy.clone(),
            AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { strategy_id },
        );
        let reference = realm_config_event(
            seed + 2,
            node(seed + 2),
            1,
            observed_strategy,
            reference_op.clone(),
        );

        let mut remove_first = initial.clone();
        assert_eq!(
            remove_first.apply(&removal),
            Ok(AdminDocumentApplyStatus::Applied)
        );
        assert_eq!(
            remove_first.apply(&reference),
            Ok(AdminDocumentApplyStatus::Applied)
        );

        let mut reference_first = initial;
        assert_eq!(
            reference_first.apply(&reference),
            Ok(AdminDocumentApplyStatus::Applied)
        );
        assert_eq!(
            reference_first.apply(&removal),
            Ok(AdminDocumentApplyStatus::Applied)
        );

        assert_eq!(remove_first, reference_first);
        assert!(
            remove_first
                .materialized_realm_config_placement_strategies()
                .is_empty()
        );
        assert!(remove_first.conflicts.is_empty());

        let mut base_config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        base_config.strategies = vec![
            placement_strategy(fallback_strategy_id, Some(1)),
            placement_strategy(strategy_id, Some(3)),
        ];
        base_config.default_strategy_id = Some(fallback_strategy_id);

        let mut remove_first_config = base_config.clone();
        overlay_realm_config_placement_reducer_materialization(
            &mut remove_first_config,
            &remove_first,
            0,
        );
        let mut reference_first_config = base_config;
        overlay_realm_config_placement_reducer_materialization(
            &mut reference_first_config,
            &reference_first,
            0,
        );
        assert_eq!(remove_first_config, reference_first_config);
        assert_eq!(
            remove_first_config.default_strategy_id,
            Some(fallback_strategy_id)
        );
        assert!(remove_first_config.strategy(&strategy_id).is_none());
        assert_realm_config_strategy_references_are_live(&remove_first_config);

        match &reference_op {
            AdminDocumentOperation::RealmConfigDefaultStrategySet { .. } => {}
            AdminDocumentOperation::RealmConfigStrategyBindingSet { binding } => assert!(
                remove_first_config
                    .strategy_bindings
                    .iter()
                    .any(|materialized| {
                        materialized.scope == binding.scope
                            && materialized.strategy_id == fallback_strategy_id
                    })
            ),
            AdminDocumentOperation::RealmConfigPlacementOverrideSet { record } => {
                let materialized = remove_first_config
                    .placement_overrides
                    .iter()
                    .find(|materialized| materialized.subject == record.subject)
                    .expect("override remains materialized");
                assert_eq!(materialized.strategy_id, Some(fallback_strategy_id));
                assert_eq!(materialized.pinned, record.pinned);
                assert_eq!(materialized.excluded, record.excluded);
            }
            _ => unreachable!("test only contains strategy reference operations"),
        }

        let restoration = realm_config_event(
            seed + 3,
            node(seed + 3),
            1,
            AdminDocumentClock::default().with_observed(removal.origin_node_id, 1),
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: placement_strategy(strategy_id, Some(3)),
            },
        );
        remove_first.apply(&restoration).unwrap();
        overlay_realm_config_placement_reducer_materialization(
            &mut remove_first_config,
            &remove_first,
            0,
        );
        assert!(remove_first_config.strategy(&strategy_id).is_some());
        assert_realm_config_strategy_references_are_live(&remove_first_config);
        match reference_op {
            AdminDocumentOperation::RealmConfigDefaultStrategySet { .. } => {
                assert_eq!(remove_first_config.default_strategy_id, Some(strategy_id))
            }
            AdminDocumentOperation::RealmConfigStrategyBindingSet { binding } => assert!(
                remove_first_config
                    .strategy_bindings
                    .iter()
                    .any(|materialized| {
                        materialized.scope == binding.scope
                            && materialized.strategy_id == strategy_id
                    })
            ),
            AdminDocumentOperation::RealmConfigPlacementOverrideSet { record } => assert!(
                remove_first_config
                    .placement_overrides
                    .iter()
                    .any(|materialized| {
                        materialized.subject == record.subject
                            && materialized.strategy_id == Some(strategy_id)
                            && materialized.pinned == record.pinned
                            && materialized.excluded == record.excluded
                    })
            ),
            _ => unreachable!("test only contains strategy reference operations"),
        }
    }
}

fn assert_realm_config_strategy_references_are_live(config: &RealmConfigDocument) {
    assert!(
        config
            .default_strategy_id
            .is_none_or(|strategy_id| config.strategy(&strategy_id).is_some())
    );
    assert!(
        config
            .strategy_bindings
            .iter()
            .all(|binding| config.strategy(&binding.strategy_id).is_some())
    );
    assert!(config.placement_overrides.iter().all(|record| {
        record
            .strategy_id
            .is_none_or(|strategy_id| config.strategy(&strategy_id).is_some())
    }));
}

#[test]
fn realm_config_strategy_binding_materializes_and_removes() {
    let mut state = realm_config_state();
    let scope = BindingScope::Class(DocumentClass::MetadataRegistry);
    let binding = StrategyBinding {
        scope: scope.clone(),
        strategy_id: Ulid::from_bytes([4; 16]),
    };
    upsert_placement_strategy(&mut state, 9, 9, binding.strategy_id);
    let scope_key = binding_scope_key(&scope);
    let set_origin = node(1);
    let set = realm_config_event(
        1,
        set_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigStrategyBindingSet {
            binding: binding.clone(),
        },
    );

    state.apply(&set).unwrap();
    assert_eq!(
        state.materialized_realm_config_strategy_bindings(),
        BTreeMap::from([(scope_key, binding)])
    );
    assert!(
        !state
            .conflicts
            .contains_key(&realm_config_strategy_binding_path(&scope))
    );

    let removal = realm_config_event(
        2,
        node(2),
        1,
        AdminDocumentClock::default().with_observed(set_origin, 1),
        AdminDocumentOperation::RealmConfigStrategyBindingRemoved { scope },
    );
    state.apply(&removal).unwrap();
    assert!(
        state
            .materialized_realm_config_strategy_bindings()
            .is_empty()
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn realm_config_metadata_path_prefix_binding_remove_uses_normalized_key() {
    let mut state = realm_config_state();
    let raw_scope = BindingScope::MetadataPathPrefix("/datasets/".to_string());
    let canonical_scope = BindingScope::MetadataPathPrefix("datasets".to_string());
    let binding = StrategyBinding {
        scope: raw_scope.clone(),
        strategy_id: Ulid::from_bytes([4; 16]),
    };
    upsert_placement_strategy(&mut state, 9, 9, binding.strategy_id);
    let canonical_binding = StrategyBinding {
        scope: canonical_scope.clone(),
        strategy_id: binding.strategy_id,
    };
    let canonical_scope_key = binding_scope_key(&canonical_scope);
    let unnormalized_path = "realm_config.placement.bindings.metadata_path_prefix:/datasets/";
    let set_origin = node(1);
    let set = realm_config_event(
        1,
        set_origin,
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigStrategyBindingSet { binding },
    );

    state.apply(&set).unwrap();
    assert_eq!(
        state.materialized_realm_config_strategy_bindings(),
        BTreeMap::from([(canonical_scope_key, canonical_binding)])
    );
    assert!(
        state
            .user_subject_ids
            .contains_key(&realm_config_strategy_binding_path(&canonical_scope))
    );
    assert!(!state.user_subject_ids.contains_key(unnormalized_path));

    let removal = realm_config_event(
        2,
        node(2),
        1,
        AdminDocumentClock::default().with_observed(set_origin, 1),
        AdminDocumentOperation::RealmConfigStrategyBindingRemoved {
            scope: BindingScope::MetadataPathPrefix(" datasets/ ".to_string()),
        },
    );

    state.apply(&removal).unwrap();
    assert!(
        state
            .materialized_realm_config_strategy_bindings()
            .is_empty()
    );
    assert!(state.conflicts.is_empty());
}

#[test]
fn realm_config_placement_override_materializes() {
    let mut state = realm_config_state();
    let subject = b"document-subject".to_vec();
    let strategy_id = Ulid::from_bytes([4; 16]);
    upsert_placement_strategy(&mut state, 9, 9, strategy_id);
    let record = PlacementOverride {
        subject: subject.clone(),
        pinned: vec![node(4)],
        excluded: vec![node(5)],
        strategy_id: Some(strategy_id),
    };
    let subject_key = hex::encode(&subject);

    state
        .apply(&realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                record: record.clone(),
            },
        ))
        .unwrap();

    assert_eq!(
        state.materialized_realm_config_placement_overrides(),
        BTreeMap::from([(subject_key, record)])
    );
    assert_eq!(
        state.apply(&realm_config_placement_override_removed(2, subject)),
        Ok(AdminDocumentApplyStatus::Applied)
    );
}

fn realm_config_placement_override_removed(event_seed: u8, subject: Vec<u8>) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(2),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { subject },
    )
}

#[test]
fn placement_op_is_rejected_for_non_realm_config_target() {
    let mut state = user_state();
    let before = state.clone();
    let event = event(
        1,
        node(1),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigDefaultStrategySet {
            strategy_id: Ulid::from_bytes([4; 16]),
        },
    );

    assert_eq!(
        state.apply(&event),
        Err(AdminDocumentReducerError::UnsupportedTarget)
    );
    assert_eq!(state, before);
}

pub(super) fn placement_binding(handle: u32, strategy_seed: u8) -> PlacementBinding {
    PlacementBinding {
        handle: PlacementHandle::new(handle).unwrap(),
        scope: PlacementScope::Realm(realm_id()),
        document_class: DocumentClass::MetadataRegistry,
        strategy_id: Ulid::from_bytes([strategy_seed; 16]),
        allocator_range_id: None,
        allocated_by: None,
        allocated_at_ms: None,
    }
}

pub(super) fn append_placement_binding(
    event_seed: u8,
    origin_seed: u8,
    binding: PlacementBinding,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding },
    )
}

pub(super) fn handle_range(range_seed: u8, owner: NodeId, start: u32, end: u32) -> HandleRange {
    HandleRange {
        range_id: Ulid::from_bytes([range_seed; 16]),
        owner,
        start,
        end,
    }
}

pub(super) fn grant_handle_range(
    event_seed: u8,
    origin_seed: u8,
    range: HandleRange,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigHandleRangeGranted { range },
    )
}

#[test]
fn grants_replicate_usable() {
    let owner = node(3);
    let first = grant_handle_range(1, 1, handle_range(10, owner, FIRST_GRANTABLE_HANDLE, 1027));
    let second = grant_handle_range(2, 2, handle_range(20, owner, 1027, 2051));

    let mut state = realm_config_state();
    state.apply(&first).unwrap();
    state.apply(&second).unwrap();
    assert!(state.conflicts.is_empty());
    assert_eq!(state.materialized_handle_ranges().len(), 2);

    let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
    overlay_realm_config_placement_reducer_materialization(&mut config, &state, 0);
    let directory = config.handle_range_directory();
    assert_eq!(directory.conflicts(), 0);
    assert_eq!(directory.granted_to(&owner).len(), 2);
    assert_eq!(
        directory.free_band_in(&[(
            FIRST_GRANTABLE_HANDLE,
            crate::structs::band_start(crate::structs::HANDLE_BANDS)
        )]),
        Some((2051, 3075))
    );
}

#[test]
fn malformed_range_rejected() {
    let event = grant_handle_range(1, 1, handle_range(10, node(3), 1, 1025));
    let mut state = realm_config_state();

    assert_eq!(
        state.apply(&event),
        Err(AdminDocumentReducerError::InvalidHandleRange)
    );
}

#[test]
fn overlap_conflicts_converge() {
    let owner = node(3);
    let first = grant_handle_range(1, 1, handle_range(10, owner, FIRST_GRANTABLE_HANDLE, 1027));
    let second = grant_handle_range(2, 2, handle_range(20, owner, 512, 2049));

    let mut left = realm_config_state();
    left.apply(&first).unwrap();
    left.apply(&second).unwrap();

    let mut right = realm_config_state();
    right.apply(&second).unwrap();
    right.apply(&first).unwrap();

    // Distinct ids retain separate paths; the derived directory catches overlap.
    assert!(left.conflicts.is_empty());
    assert_eq!(left.conflicts, right.conflicts);
    for range_seed in [10u8, 20] {
        let path = handle_range_path(Ulid::from_bytes([range_seed; 16]));
        assert!(left.user_subject_ids.contains_key(&path));
    }
    assert_eq!(left.materialized_handle_ranges().len(), 2);

    for state in [&left, &right] {
        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        overlay_realm_config_placement_reducer_materialization(&mut config, state, 0);
        assert_eq!(config.placement_handle_ranges.len(), 2);
        let directory = config.handle_range_directory();
        assert_eq!(directory.conflicts(), 2);
        assert!(directory.granted_to(&owner).is_empty());
    }
}

#[test]
fn binding_paths_disjoint() {
    let handle = PlacementHandle::new(42).unwrap();
    let path = placement_binding_path(handle);
    assert_eq!(path, "realm_config.placement.placement_bindings.42");
    assert_eq!(placement_binding_handle(&path), Some(handle));
    // The two placement-binding namespaces must not parse each other's paths.
    assert_eq!(
        realm_config_strategy_binding_scope_key_from_path(&path),
        None
    );
    let strategy_path = realm_config_strategy_binding_path(&BindingScope::Realm);
    assert_eq!(placement_binding_handle(&strategy_path), None);
}
