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

/// Overlays candidate maps and transitions, then re-derives every activation
/// they govern. A conflicted map epoch keeps all its divergent values (the
/// epoch stays unusable); a conflicted plan or activation drops the record
/// entirely, so the affected buckets resolve nothing.
fn overlay_placement_transitions(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
    now_ms: u64,
) {
    let materialized_maps = reducer_state.materialized_candidate_maps();
    for (path, conflict) in &reducer_state.conflicts {
        let Some(epoch) = candidate_map_epoch(path) else {
            continue;
        };
        config.candidate_maps.retain(|map| map.epoch != epoch);
        for value in &conflict.values {
            if let Some(map) = value.value.as_deref().and_then(candidate_map_from_value) {
                config.candidate_maps.push(map);
            }
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(epoch) = candidate_map_epoch(path) else {
            continue;
        };
        config.candidate_maps.retain(|map| map.epoch != epoch);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(map) = materialized_maps.get(&epoch) {
            config.candidate_maps.push(map.clone());
        }
    }

    let mut transitions = reducer_state.materialized_transitions();
    for path in reducer_state.conflicts.keys() {
        if let Some((transition_id, TransitionPart::Plan)) = transition_part(path) {
            transitions.retain(|transition| transition.plan.transition_id != transition_id);
        }
    }
    for transition in &transitions {
        config
            .placement_transitions
            .retain(|existing| existing.plan.transition_id != transition.plan.transition_id);
    }
    // A released record is dropped from the document but never from the fold
    // below: activations are replayed from the whole reduced chain, so pruning
    // a cut-over out of that chain would silently regress its buckets.
    config.placement_transitions.extend(
        transitions
            .iter()
            .filter(|transition| !transition.released(now_ms))
            .cloned(),
    );

    let epochs = reducer_state.materialized_activation_epochs();
    let mut initialized: Vec<(Ulid, u32, u64)> = Vec::new();
    for path in reducer_state
        .user_subject_ids
        .keys()
        .chain(reducer_state.conflicts.keys())
    {
        let Some(strategy_id) = activation_strategy(path) else {
            continue;
        };
        config
            .placement_activations
            .retain(|activation| activation.strategy_id != strategy_id);
        if let (Some(epoch), Some(strategy)) =
            (epochs.get(&strategy_id), config.strategy(&strategy_id))
        {
            initialized.push((strategy_id, strategy.shard_count, *epoch));
        }
    }
    // Transition order is the id order: a successor is admitted only once its
    // predecessor is terminal, so the chain replays the same way everywhere.
    transitions.sort_by_key(|transition| transition.plan.transition_id);
    for (strategy_id, shard_count, epoch) in initialized {
        for shard in 0..shard_count {
            let mut activation = PlacementActivation {
                strategy_id,
                shard,
                activation_epoch: 1,
                candidate_map_epoch: epoch,
                transition_id: None,
            };
            for transition in transitions
                .iter()
                .filter(|transition| transition.plan.strategy_id == strategy_id)
            {
                let Some(bucket_plan) = transition.plan.bucket_plan(shard) else {
                    continue;
                };
                // Predecessor gate: a plan derived from another activation
                // epoch never applies to this bucket, in any replay order, so
                // concurrent same-base plans cannot chain (see BucketPlan).
                if bucket_plan.predecessor_epoch != activation.activation_epoch {
                    continue;
                }
                if transition.bucket_ready(shard) {
                    activation.candidate_map_epoch = transition.plan.target_map_epoch;
                    activation.activation_epoch += 1;
                    activation.transition_id = None;
                } else if matches!(transition.status, TransitionStatus::Active) {
                    activation.transition_id = Some(transition.plan.transition_id);
                }
            }
            config.placement_activations.push(activation);
        }
    }
    retain_referenced_maps(config);
}

/// Drops maps nothing can select from any more: no activation names them, no
/// retained transition targets them, and they are not the newest - which the
/// next transition would target.
fn retain_referenced_maps(config: &mut RealmConfigDocument) {
    let Some(newest) = config.newest_map_epoch() else {
        return;
    };
    let referenced: BTreeSet<u64> = config
        .placement_activations
        .iter()
        .map(|activation| activation.candidate_map_epoch)
        .chain(
            config
                .placement_transitions
                .iter()
                .map(|transition| transition.plan.target_map_epoch),
        )
        .chain(std::iter::once(newest))
        .collect();
    config
        .candidate_maps
        .retain(|map| referenced.contains(&map.epoch));
}

fn order_by_bucket_and_node(left: &BucketBarrier, right: &BucketBarrier) -> Ordering {
    left.bucket.cmp(&right.bucket).then_with(|| {
        left.reported_by
            .as_bytes()
            .cmp(right.reported_by.as_bytes())
    })
}

fn order_proofs(left: &CompletionProof, right: &CompletionProof) -> Ordering {
    left.bucket
        .cmp(&right.bucket)
        .then_with(|| left.holder.as_bytes().cmp(right.holder.as_bytes()))
}

fn order_stalls(left: &StallReport, right: &StallReport) -> Ordering {
    left.bucket.cmp(&right.bucket).then_with(|| {
        left.reported_by
            .as_bytes()
            .cmp(right.reported_by.as_bytes())
    })
}

fn repair_realm_config_placement_references(config: &mut RealmConfigDocument) {
    // `placement_bindings` are intentionally exempt: they are immutable, so a
    // binding naming a removed strategy fails closed at resolve rather than
    // being repaired here.
    let live_strategy_ids: BTreeSet<_> = config
        .strategies
        .iter()
        .map(|strategy| strategy.strategy_id)
        .collect();
    let fallback_strategy_id = live_strategy_ids.first().copied();

    let Some(fallback_strategy_id) = fallback_strategy_id else {
        config.default_strategy_id = None;
        config.strategy_bindings.clear();
        for record in &mut config.placement_overrides {
            record.strategy_id = None;
        }
        return;
    };

    if config
        .default_strategy_id
        .is_none_or(|strategy_id| !live_strategy_ids.contains(&strategy_id))
    {
        config.default_strategy_id = Some(fallback_strategy_id);
    }
    for binding in &mut config.strategy_bindings {
        if !live_strategy_ids.contains(&binding.strategy_id) {
            binding.strategy_id = fallback_strategy_id;
        }
    }
    for record in &mut config.placement_overrides {
        if record
            .strategy_id
            .is_some_and(|strategy_id| !live_strategy_ids.contains(&strategy_id))
        {
            record.strategy_id = Some(fallback_strategy_id);
        }
    }
}

impl AdminDocumentReducerState {
    pub fn materialized_realm_config_nodes(&self) -> BTreeMap<NodeId, RealmNodeKind> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let node_id = realm_config_node_id_from_path(path)?;
                let kind = version
                    .value
                    .as_deref()
                    .and_then(realm_node_kind_from_value)?;
                Some((node_id, kind))
            })
            .collect()
    }

    /// Nodes whose membership path reduced to no value. The overlays need them
    /// because a stored configuration still carries the earlier membership.
    pub fn removed_config_nodes(&self) -> BTreeSet<NodeId> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeSet::new();
        }

        self.user_subject_ids
            .iter()
            .filter(|(_, version)| version.value.is_none())
            .filter_map(|(path, _)| realm_config_node_id_from_path(path))
            .collect()
    }
    pub fn materialized_realm_config_placement_map(&self) -> BTreeMap<NodeId, NodePlacementEntry> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let node_id = realm_config_placement_node_id_from_path(path)?;
                let entry = version
                    .value
                    .as_deref()
                    .and_then(placement_entry_from_value)?;

                (entry.node_id == node_id).then_some((node_id, entry))
            })
            .collect()
    }

    pub fn materialized_realm_config_placement_strategies(
        &self,
    ) -> BTreeMap<Ulid, PlacementStrategy> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let strategy_id = realm_config_placement_strategy_id_from_path(path)?;
                let strategy = version
                    .value
                    .as_deref()
                    .and_then(placement_strategy_from_value)?;

                (strategy.strategy_id == strategy_id).then_some((strategy_id, strategy))
            })
            .collect()
    }

    pub fn materialized_realm_config_default_strategy(&self) -> Option<Ulid> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| Ulid::from_string(value).ok())
    }

    /// The stored submission-family strategy. A conflicted or nil value
    /// materializes as `None`, which every derivation refuses.
    pub fn materialized_family_strategy(&self) -> Option<Ulid> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_JOB_FAMILY_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| Ulid::from_string(value).ok())
            .filter(|strategy_id| !strategy_id.is_nil())
    }

    pub fn materialized_realm_config_strategy_bindings(&self) -> BTreeMap<String, StrategyBinding> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let scope_key = realm_config_strategy_binding_scope_key_from_path(path)?;
                let binding = version
                    .value
                    .as_deref()
                    .and_then(strategy_binding_from_value)
                    .map(|binding| normalized_strategy_binding(&binding))?;

                let canonical_scope_key = binding_scope_key(&binding.scope);
                (canonical_scope_key == scope_key).then_some((canonical_scope_key, binding))
            })
            .collect()
    }

    pub fn materialized_realm_config_placement_overrides(
        &self,
    ) -> BTreeMap<String, PlacementOverride> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let subject_key = realm_config_placement_override_subject_key_from_path(path)?;
                let record = version
                    .value
                    .as_deref()
                    .and_then(placement_override_from_value)?;

                (hex::encode(&record.subject) == subject_key)
                    .then(|| (subject_key.to_string(), record))
            })
            .collect()
    }

    pub fn materialized_placement_bindings(&self) -> BTreeMap<PlacementHandle, PlacementBinding> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let handle = placement_binding_handle(path)?;
                let binding = version.value.as_deref().and_then(parse_placement_binding)?;

                (binding.handle == handle).then_some((handle, binding))
            })
            .collect()
    }

    pub fn materialized_handle_ranges(&self) -> BTreeMap<Ulid, HandleRange> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let range_id = handle_range_id(path)?;
                let range = version.value.as_deref().and_then(parse_handle_range)?;

                (range.range_id == range_id).then_some((range_id, range))
            })
            .collect()
    }
}

impl AdminDocumentReducerState {
    pub fn materialized_candidate_maps(&self) -> BTreeMap<u64, CandidatePlacementMap> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let epoch = candidate_map_epoch(path)?;
                let map = version
                    .value
                    .as_deref()
                    .and_then(candidate_map_from_value)?;

                (map.epoch == epoch).then_some((epoch, map))
            })
            .collect()
    }

    /// Map epoch each strategy's buckets were activated at. Per-bucket
    /// activations are derived from it plus the reduced transitions.
    pub fn materialized_activation_epochs(&self) -> BTreeMap<Ulid, u64> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let strategy_id = activation_strategy(path)?;
                let epoch = version.value.as_deref()?.parse().ok()?;

                Some((strategy_id, epoch))
            })
            .collect()
    }

    pub fn materialized_transition_plans(&self) -> BTreeMap<Ulid, TransitionPlan> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let (transition_id, TransitionPart::Plan) = transition_part(path)? else {
                    return None;
                };
                let plan = version
                    .value
                    .as_deref()
                    .and_then(transition_plan_from_value)?;

                (plan.transition_id == transition_id).then_some((transition_id, plan))
            })
            .collect()
    }

    /// Assembles each transition from its plan plus the reduced barrier, proof,
    /// force, and stall sets. Entries that do not match their own path, the
    /// plan's strategy, or a planned bucket are dropped.
    pub fn materialized_transitions(&self) -> Vec<PlacementTransition> {
        let mut transitions: BTreeMap<Ulid, PlacementTransition> = self
            .materialized_transition_plans()
            .into_iter()
            .map(|(transition_id, plan)| (transition_id, PlacementTransition::new(plan)))
            .collect();

        for (path, version) in &self.user_subject_ids {
            let Some((transition_id, part)) = transition_part(path) else {
                continue;
            };
            let Some(transition) = transitions.get_mut(&transition_id) else {
                continue;
            };
            let Some(value) = version.value.as_deref() else {
                continue;
            };
            match part {
                TransitionPart::Plan => {}
                TransitionPart::Aborted => transition.status = TransitionStatus::Aborted,
                TransitionPart::Barrier(bucket, reported_by) => {
                    // Only planned old holders fence; a foreign report never
                    // enters the record or the digest proofs commit to.
                    if let Ok(frontier) = hex::decode(value)
                        && transition
                            .plan
                            .bucket_plan(bucket)
                            .is_some_and(|plan| plan.old_holders.contains(&reported_by))
                    {
                        transition.barriers.push(BucketBarrier {
                            bucket,
                            reported_by,
                            frontier,
                        });
                    }
                }
                TransitionPart::Proof(bucket, holder) => {
                    let Some((strategy_id, proof)) = transition_proof_from_value(value) else {
                        continue;
                    };
                    // A proof from outside the planned target set never enters
                    // the record; the tuple predicate rejects the rest lazily.
                    if strategy_id == transition.plan.strategy_id
                        && proof.bucket == bucket
                        && proof.holder == holder
                        && transition
                            .plan
                            .bucket_plan(bucket)
                            .is_some_and(|plan| plan.target_holders.contains(&holder))
                    {
                        transition.proofs.push(proof);
                    }
                }
                TransitionPart::Forced(bucket) => {
                    if transition.plan.covers(bucket) {
                        transition.forced.push(BucketForceFinalize {
                            bucket,
                            at_risk_report: value.to_string(),
                        });
                    }
                }
                TransitionPart::Stall(bucket, reported_by) => {
                    if transition.plan.bucket_plan(bucket).is_some_and(|plan| {
                        plan.old_holders.contains(&reported_by)
                            || plan.target_holders.contains(&reported_by)
                    }) {
                        transition.stalls.push(StallReport {
                            bucket,
                            reported_by,
                            reason: value.to_string(),
                        });
                    }
                }
                TransitionPart::Drain(bucket, reported_by) => {
                    // Only a departing old holder owes (or may end) retention.
                    if transition.plan.bucket_plan(bucket).is_some_and(|plan| {
                        plan.old_holders.contains(&reported_by)
                            && !plan.target_holders.contains(&reported_by)
                    }) {
                        transition.drained.push(crate::structs::BucketDrain {
                            bucket,
                            reported_by,
                        });
                    }
                }
            }
        }

        let mut assembled: Vec<PlacementTransition> = transitions.into_values().collect();
        for transition in assembled.iter_mut() {
            transition.barriers.sort_by(order_by_bucket_and_node);
            transition.proofs.sort_by(order_proofs);
            transition.forced.sort_by_key(|entry| entry.bucket);
            transition.stalls.sort_by(order_stalls);
            transition.drained.sort_by(|left, right| {
                left.bucket.cmp(&right.bucket).then_with(|| {
                    left.reported_by
                        .as_bytes()
                        .cmp(right.reported_by.as_bytes())
                })
            });
            transition.completed = transition
                .plan
                .buckets
                .iter()
                .filter(|plan| transition.bucket_ready(plan.bucket))
                .map(|plan| BucketCompletion {
                    bucket: plan.bucket,
                    completed_at_ms: self.proof_timestamp(transition, plan.bucket),
                })
                .collect();
        }
        assembled
    }

    /// Completion time of a bucket: the newest carried event timestamp among
    /// its proofs. Carried data, so every replica derives the same instant
    /// without consulting a local clock.
    fn proof_timestamp(&self, transition: &PlacementTransition, bucket: u32) -> u64 {
        transition
            .plan
            .bucket_plan(bucket)
            .into_iter()
            .flat_map(|plan| plan.target_holders.iter())
            .map(|holder| transition_proof_path(&transition.plan.transition_id, bucket, holder))
            .filter_map(|path| self.path_timestamp(&path))
            .max()
            .unwrap_or_default()
    }

    /// Newest event timestamp recorded for a path, across its version dot and
    /// any equivalent-value dots.
    fn path_timestamp(&self, path: &str) -> Option<u64> {
        let version = self
            .user_subject_ids
            .get(path)
            .map(|version| version.dot.event_id.timestamp_ms());
        let equivalent = self
            .equivalent_value_dots
            .get(path)
            .and_then(|dots| dots.iter().map(|dot| dot.event_id.timestamp_ms()).max());
        version.max(equivalent).or(equivalent)
    }


    pub fn materialized_band_pools(&self) -> BTreeMap<Ulid, BandPool> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let pool_id = band_pool_id(path)?;
                let pool = version.value.as_deref().and_then(parse_band_pool)?;

                (pool.pool_id == pool_id).then_some((pool_id, pool))
            })
            .collect()
    }
}

impl AdminDocumentReducerState {
    pub(super) fn apply_realm_config_placement_field(
        &mut self,
        event: &AdminDocumentEvent,
        path: String,
        value: Option<String>,
    ) {
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_value(event, &path, current, value) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    pub(super) fn apply_placement_binding(&mut self, event: &AdminDocumentEvent, binding: &PlacementBinding) {
        self.apply_immutable_value(
            event,
            placement_binding_path(binding.handle),
            placement_binding_value(binding),
        );
    }

    pub(super) fn apply_handle_range(&mut self, event: &AdminDocumentEvent, range: &HandleRange) {
        // Like bindings, divergent values for one id fail closed. Distinct-id
        // overlap is derived later by `HandleRangeDirectory::from_ranges`.
        self.apply_immutable_value(
            event,
            handle_range_path(range.range_id),
            handle_range_value(range),
        );
    }

    pub(super) fn apply_band_pool(&mut self, event: &AdminDocumentEvent, pool: &BandPool) {
        self.apply_immutable_value(event, band_pool_path(pool.pool_id), band_pool_value(pool));
    }

    /// One actor's report about one bucket: a retry is not divergence.
    ///
    /// A holder re-runs the transition step until it observes its own report, so
    /// it can legitimately submit twice with a moved frontier or a re-signed
    /// proof. Treating that as a conflict would strand the bucket forever, so
    /// the earliest event wins - deterministic on every replica, whatever order
    /// the two arrived in.
    pub(super) fn apply_transition_report(&mut self, event: &AdminDocumentEvent, path: String, value: String) {
        let dot = event.dot();
        if let Some(current) = self.user_subject_ids.get(&path)
            && current.dot <= dot
        {
            return;
        }
        self.user_subject_ids.insert(
            path,
            AdminDocumentAttributeVersion {
                value: Some(value),
                dot,
            },
        );
    }
}
