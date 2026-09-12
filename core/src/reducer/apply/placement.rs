use super::*;

impl AdminDocumentReducerState {
    pub(super) fn apply_placement(
        &mut self,
        event: &AdminDocumentEvent,
        realm_id: &RealmId,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        match &event.op {
            AdminDocumentOperation::RealmConfigNodePlacementSet { entry } => {
                if let Some(label) = reserved_label(&entry.labels) {
                    return Err(AdminDocumentReducerError::ReservedPlacementLabel(
                        label.to_string(),
                    ));
                }
                self.apply_placement_field(
                    event,
                    placement_node_path(&entry.node_id),
                    Some(placement_entry_value(entry)),
                );
            }
            AdminDocumentOperation::RealmConfigNodePlacementRemoved { node_id } => {
                self.apply_placement_field(event, placement_node_path(node_id), None);
            }
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy } => {
                if strategy.replica_count == Some(0) {
                    return Err(AdminDocumentReducerError::ZeroPlacementReplicaCount);
                }
                if strategy.shard_count == 0
                    || !strategy.shard_count.is_power_of_two()
                    || strategy.shard_count > MAX_PLACEMENT_SHARD_COUNT
                {
                    return Err(AdminDocumentReducerError::InvalidPlacementShardCount);
                }
                if self
                    .materialized_strategies()
                    .get(&strategy.strategy_id)
                    .is_some_and(|current| current.shard_count != strategy.shard_count)
                {
                    return Err(AdminDocumentReducerError::PlacementShardCountChanged);
                }
                self.apply_placement_field(
                    event,
                    placement_strategy_path(&strategy.strategy_id),
                    Some(placement_strategy_value(strategy)),
                );
            }
            AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { strategy_id } => {
                if self.materialized_family_strategy() == Some(*strategy_id) {
                    return Err(AdminDocumentReducerError::JobFamilyRemoved);
                }
                self.apply_placement_field(event, placement_strategy_path(strategy_id), None);
            }
            AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id } => {
                self.apply_config_setting(
                    event,
                    REALM_CONFIG_DEFAULT_STRATEGY_PATH,
                    strategy_id.to_string(),
                );
            }
            AdminDocumentOperation::RealmConfigJobFamilySet { strategy_id } => {
                if strategy_id.is_nil() {
                    return Err(AdminDocumentReducerError::NilJobFamily);
                }
                if self
                    .materialized_family_strategy()
                    .is_some_and(|current| current != *strategy_id)
                {
                    return Err(AdminDocumentReducerError::JobFamilyChanged);
                }
                self.apply_config_setting(
                    event,
                    REALM_CONFIG_JOB_FAMILY_PATH,
                    strategy_id.to_string(),
                );
            }
            AdminDocumentOperation::RealmConfigStrategyBindingSet { binding } => {
                self.apply_placement_field(
                    event,
                    strategy_binding_path(&binding.scope),
                    Some(strategy_binding_value(binding)),
                );
            }
            AdminDocumentOperation::RealmConfigStrategyBindingRemoved { scope } => {
                self.apply_placement_field(event, strategy_binding_path(scope), None);
            }
            AdminDocumentOperation::RealmConfigPlacementOverrideSet { record } => {
                self.apply_placement_field(
                    event,
                    placement_override_path(&record.subject),
                    Some(placement_override_value(record)),
                );
            }
            AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { subject } => {
                self.apply_placement_field(event, placement_override_path(subject), None);
            }
            AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding } => {
                self.apply_placement_binding(event, binding);
            }
            AdminDocumentOperation::RealmConfigCandidateMapPublished { map } => {
                // Epoch zero is reserved for "no map", and a map naming a node
                // twice would make its selection weight ambiguous.
                let mut seen = BTreeSet::new();
                if map.epoch == 0 || !map.nodes.iter().all(|node| seen.insert(node.node_id)) {
                    return Err(AdminDocumentReducerError::InvalidCandidateMap);
                }
                self.apply_immutable_value(
                    event,
                    candidate_map_path(map.epoch),
                    candidate_map_value(map),
                );
            }
            AdminDocumentOperation::RealmConfigActivationsInitialized {
                strategy_id,
                candidate_map_epoch,
            } => {
                if *candidate_map_epoch == 0 {
                    return Err(AdminDocumentReducerError::InvalidCandidateMap);
                }
                self.apply_immutable_value(
                    event,
                    activation_path(strategy_id),
                    candidate_map_epoch.to_string(),
                );
            }
            _ => return self.apply_transition(event, realm_id),
        }
        Ok(AdminDocumentApplyStatus::Applied)
    }
}
