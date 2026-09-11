use super::*;

/// Overlays the realm-config placement paths owned by `reducer_state` onto `config`.
/// Paths absent from both the reducer values and conflicts remain untouched.
///
/// The final repair uses the live strategy with the lowest id as the deterministic
/// fallback for missing defaults, bindings, and explicit override strategy ids. If
/// no strategy is live, references are cleared while override pins and exclusions
/// are retained. Reducer values are not changed, so a later strategy upsert can
/// restore an assignment that was only dangling in the materialized snapshot.
/// Materializes every placement structure the reducer owns into `config`.
///
/// `now_ms` decides only which terminal transitions have outlived their grace
/// and are dropped from the document; it never reaches an activation, so two
/// replicas reading different clocks still route identically.
pub fn overlay_realm_config_placement_reducer_materialization(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
    now_ms: u64,
) {
    if reducer_state
        .user_subject_ids
        .contains_key(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
        || reducer_state
            .conflicts
            .contains_key(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
    {
        config.default_strategy_id = reducer_state.materialized_realm_config_default_strategy();
    }

    // The stored family strategy is immutable, so a materialized value always
    // wins and an absent one never clears what the document already carries.
    if let Some(strategy_id) = reducer_state.materialized_family_strategy() {
        config.job_family_strategy_id = strategy_id;
    }

    let materialized_placement_map = reducer_state.materialized_realm_config_placement_map();
    for path in reducer_state.conflicts.keys() {
        if let Some(node_id) = realm_config_placement_node_id_from_path(path) {
            config
                .placement_map
                .retain(|entry| entry.node_id != node_id);
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(node_id) = realm_config_placement_node_id_from_path(path) else {
            continue;
        };
        config
            .placement_map
            .retain(|entry| entry.node_id != node_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(entry) = materialized_placement_map.get(&node_id) {
            config.placement_map.push(entry.clone());
        }
    }

    let materialized_strategies = reducer_state.materialized_realm_config_placement_strategies();
    for path in reducer_state.conflicts.keys() {
        if let Some(strategy_id) = realm_config_placement_strategy_id_from_path(path) {
            config
                .strategies
                .retain(|strategy| strategy.strategy_id != strategy_id);
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(strategy_id) = realm_config_placement_strategy_id_from_path(path) else {
            continue;
        };
        config
            .strategies
            .retain(|strategy| strategy.strategy_id != strategy_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(strategy) = materialized_strategies.get(&strategy_id) {
            config.strategies.push(strategy.clone());
        }
    }

    let materialized_bindings = reducer_state.materialized_realm_config_strategy_bindings();
    for path in reducer_state.conflicts.keys() {
        if let Some(scope_key) = realm_config_strategy_binding_scope_key_from_path(path) {
            config
                .strategy_bindings
                .retain(|binding| binding_scope_key(&binding.scope) != scope_key);
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(scope_key) = realm_config_strategy_binding_scope_key_from_path(path) else {
            continue;
        };
        config
            .strategy_bindings
            .retain(|binding| binding_scope_key(&binding.scope) != scope_key);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(binding) = materialized_bindings.get(scope_key) {
            config.strategy_bindings.push(binding.clone());
        }
    }

    let materialized_overrides = reducer_state.materialized_realm_config_placement_overrides();
    for path in reducer_state.conflicts.keys() {
        if let Some(subject_key) = realm_config_placement_override_subject_key_from_path(path) {
            config
                .placement_overrides
                .retain(|record| hex::encode(&record.subject) != subject_key);
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(subject_key) = realm_config_placement_override_subject_key_from_path(path) else {
            continue;
        };
        config
            .placement_overrides
            .retain(|record| hex::encode(&record.subject) != subject_key);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(record) = materialized_overrides.get(subject_key) {
            config.placement_overrides.push(record.clone());
        }
    }

    // Placement bindings are immutable and fail closed: unlike strategy bindings
    // (which drop a conflicted scope), every divergent value for a conflicted
    // handle is retained so the derived binding directory reports a conflict.
    let materialized_bindings = reducer_state.materialized_placement_bindings();
    for (path, conflict) in &reducer_state.conflicts {
        let Some(handle) = placement_binding_handle(path) else {
            continue;
        };
        config
            .placement_bindings
            .retain(|binding| binding.handle != handle);
        for value in &conflict.values {
            if let Some(binding) = value.value.as_deref().and_then(parse_placement_binding) {
                config.placement_bindings.push(binding);
            }
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(handle) = placement_binding_handle(path) else {
            continue;
        };
        config
            .placement_bindings
            .retain(|binding| binding.handle != handle);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(binding) = materialized_bindings.get(&handle) {
            config.placement_bindings.push(binding.clone());
        }
    }

    // Same-id grant conflicts retain every value; the range directory derives
    // distinct-id overlap conflicts.
    let materialized_ranges = reducer_state.materialized_handle_ranges();
    for (path, conflict) in &reducer_state.conflicts {
        let Some(range_id) = handle_range_id(path) else {
            continue;
        };
        config
            .placement_handle_ranges
            .retain(|range| range.range_id != range_id);
        for value in &conflict.values {
            if let Some(range) = value.value.as_deref().and_then(parse_handle_range) {
                config.placement_handle_ranges.push(range);
            }
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(range_id) = handle_range_id(path) else {
            continue;
        };
        config
            .placement_handle_ranges
            .retain(|range| range.range_id != range_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(range) = materialized_ranges.get(&range_id) {
            config.placement_handle_ranges.push(*range);
        }
    }

    // Band pools mirror handle ranges: same-id divergence retains every value.
    let materialized_pools = reducer_state.materialized_band_pools();
    for (path, conflict) in &reducer_state.conflicts {
        let Some(pool_id) = band_pool_id(path) else {
            continue;
        };
        config.band_pools.retain(|pool| pool.pool_id != pool_id);
        for value in &conflict.values {
            if let Some(pool) = value.value.as_deref().and_then(parse_band_pool) {
                config.band_pools.push(pool);
            }
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(pool_id) = band_pool_id(path) else {
            continue;
        };
        config.band_pools.retain(|pool| pool.pool_id != pool_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(pool) = materialized_pools.get(&pool_id) {
            config.band_pools.push(*pool);
        }
    }

    overlay_placement_transitions(config, reducer_state, now_ms);
    repair_realm_config_placement_references(config);
}
