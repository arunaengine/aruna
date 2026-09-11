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
