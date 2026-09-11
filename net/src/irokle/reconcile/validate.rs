use super::*;

impl ConfigValidationCache {
    pub(in crate::document_sync) fn invalidate(&mut self) {
        self.entry = None;
    }

    pub(in crate::document_sync) async fn load(
        &mut self,
        storage: &StorageHandle,
        realm_id: RealmId,
    ) -> Result<(
        Option<&RealmConfigDocument>,
        Option<&AdminDocumentReducerState>,
    )> {
        if self
            .entry
            .as_ref()
            .is_none_or(|(cached, ..)| *cached != realm_id)
        {
            let config = read_admin_realm_config(storage, realm_id).await?;
            let state =
                read_admin_reducer_state(storage, &AdminDocumentTarget::RealmConfig { realm_id })
                    .await?;
            self.entry = Some((realm_id, config, state));
        }
        match &self.entry {
            Some((_, config, state)) => Ok((config.as_ref(), state.as_ref())),
            None => Ok((None, None)),
        }
    }
}

pub(in crate::document_sync) async fn read_admin_reducer_state(
    storage: &StorageHandle,
    target: &AdminDocumentTarget,
) -> Result<Option<AdminDocumentReducerState>> {
    storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(target),
    )
    .await?
    .map(|bytes| decode_admin_document_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

pub(in crate::document_sync) async fn read_admin_realm_config(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<Option<RealmConfigDocument>> {
    storage_read_from(
        storage,
        DocumentSyncTarget::RealmConfig { realm_id }
            .storage_keyspace()
            .to_string(),
        DocumentSyncTarget::RealmConfig { realm_id }.storage_key(),
    )
    .await?
    .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

pub(in crate::document_sync) async fn read_admin_realm_authorization(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<Option<RealmAuthorizationDocument>> {
    storage_read_from(
        storage,
        DocumentSyncTarget::RealmAuthorization { realm_id }
            .storage_keyspace()
            .to_string(),
        DocumentSyncTarget::RealmAuthorization { realm_id }.storage_key(),
    )
    .await?
    .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

/// Whether this origin already holds the per-origin bound a local mint obeys.
/// Replacing its own entry stays allowed; the flooding origin is rejected rather
/// than trimmed, so a valid revocation is never discarded to make room.
pub(in crate::document_sync) fn revocation_origin_full(
    state: Option<&AdminDocumentReducerState>,
    event: &AdminDocumentEvent,
    token_hash: &str,
) -> bool {
    let Some(state) = state else {
        return false;
    };
    let index = state.revocation_index(state.revocation_floor.max(unix_timestamp_secs()));
    index.origin(token_hash) != Some(event.origin_node_id)
        && index.count(&event.origin_node_id) >= MAX_LIVE_REVOCATIONS_PER_ORIGIN
}

pub(in crate::document_sync) fn revocation_origin_known(
    config: Option<&RealmConfigDocument>,
    state: Option<&AdminDocumentReducerState>,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
) -> bool {
    if config.is_some_and(|config| {
        config.realm_id == realm_id && origin_may_publish(config, &event.origin_node_id)
    }) {
        return true;
    }

    let Some(state) = state else {
        return false;
    };
    let path = realm_config_node_path(&event.origin_node_id);
    state
        .user_subject_ids
        .get(&path)
        .is_some_and(|version| event.observed.observes(&version.dot))
        || state.conflicts.get(&path).is_some_and(|conflict| {
            conflict
                .values
                .iter()
                .any(|value| event.observed.observes(&value.dot))
        })
}

/// Whether the transport publisher of a relayed admin event is a realm node
/// allowed to relay. User nodes are never relays, so they never appear here.
pub(in crate::document_sync) async fn relay_publisher_allowed(
    storage: &StorageHandle,
    topic_id: irokle_crate::TopicId,
    publisher: irokle_crate::ActorId,
    realm_id: RealmId,
) -> Result<bool> {
    let Some(config) = read_admin_realm_config(storage, realm_id).await? else {
        return Ok(false);
    };
    Ok(config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .any(|node_id| {
            publisher == irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&node_id))
        }))
}

/// Publisher capability by node kind: a User node never originates a realm
/// administrative event, whichever node relayed it.
pub(in crate::document_sync) fn origin_may_publish(
    config: &RealmConfigDocument,
    origin_node_id: &NodeId,
) -> bool {
    configured_node_kind(config, origin_node_id).is_some_and(RealmNodeKind::is_sync_eligible)
}

pub(in crate::document_sync) fn configured_node_kind<'a>(
    config: &'a RealmConfigDocument,
    node_id: &NodeId,
) -> Option<&'a RealmNodeKind> {
    let node_id = node_id.to_string();
    config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .map(|node| &node.kind)
}

/// Resolves the plan a report names (stored config first, reduced state as
/// the fallback) and checks the reporter holds the role the report claims:
/// barriers from old holders, proofs from targets, stalls from the union.
pub(in crate::document_sync) fn report_participation(
    op: &AdminDocumentOperation,
    current_config: Option<&RealmConfigDocument>,
    previous_state: Option<&AdminDocumentReducerState>,
) -> ReportParticipation {
    enum Role {
        Old,
        Target,
        Union,
        Departing,
    }
    let (transition_id, bucket, reporter, role) = match op {
        AdminDocumentOperation::RealmConfigTransitionBarrierReported {
            transition_id,
            bucket,
            reported_by,
            ..
        } => (*transition_id, *bucket, *reported_by, Role::Old),
        AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
            transition_id,
            proof,
            ..
        } => (*transition_id, proof.bucket, proof.holder, Role::Target),
        AdminDocumentOperation::RealmConfigTransitionStallReported {
            transition_id,
            bucket,
            reported_by,
            ..
        } => (*transition_id, *bucket, *reported_by, Role::Union),
        AdminDocumentOperation::RealmConfigTransitionDrainReported {
            transition_id,
            bucket,
            reported_by,
        } => (*transition_id, *bucket, *reported_by, Role::Departing),
        _ => return ReportParticipation::NotReport,
    };
    let from_config = current_config.and_then(|config| {
        config
            .placement_transitions
            .iter()
            .find(|transition| transition.plan.transition_id == transition_id)
            .map(|transition| transition.plan.clone())
    });
    let plan = match from_config {
        Some(plan) => plan,
        None => {
            let Some(plan) = previous_state
                .map(|state| state.materialized_transition_plans())
                .unwrap_or_default()
                .remove(&transition_id)
            else {
                return ReportParticipation::UnknownPlan;
            };
            plan
        }
    };
    let Some(bucket_plan) = plan.bucket_plan(bucket) else {
        return ReportParticipation::Foreign;
    };
    let allowed = match role {
        Role::Old => bucket_plan.old_holders.contains(&reporter),
        Role::Target => bucket_plan.target_holders.contains(&reporter),
        Role::Union => {
            bucket_plan.old_holders.contains(&reporter)
                || bucket_plan.target_holders.contains(&reporter)
        }
        Role::Departing => {
            bucket_plan.old_holders.contains(&reporter)
                && !bucket_plan.target_holders.contains(&reporter)
        }
    };
    if allowed {
        ReportParticipation::Participant
    } else {
        ReportParticipation::Foreign
    }
}

pub(in crate::document_sync) fn validate_config_authority(
    current_config: Option<&RealmConfigDocument>,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentReducerState>,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::RealmConfig { realm_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a realm config".to_string(),
        ));
    };
    if current_config.is_some_and(|config| config.realm_id != realm_id) {
        return Ok(AdminEventValidation::Rejected(
            "stored realm config has the wrong realm".to_string(),
        ));
    }
    // Reports are gated on participation whatever the origin's kind: a plan
    // names its finite holder sets, so nothing outside them may grow state.
    match report_participation(&event.op, current_config, previous_state) {
        ReportParticipation::NotReport | ReportParticipation::Participant => {}
        ReportParticipation::UnknownPlan => {
            // Same-topic retry only. A participant can report only after
            // observing the plan, so a plan absent after the buffered run
            // flushes has no causal evidence and must not park the cursor.
            return Ok(AdminEventValidation::Deferred {
                dependency: None,
                reason: "transition plan is not yet materialized".to_string(),
            });
        }
        ReportParticipation::Foreign => {
            return Ok(AdminEventValidation::Rejected(
                "transition report does not come from a planned participant".to_string(),
            ));
        }
    }
    if matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigTokenRevoked { .. }
    ) {
        if !revocation_origin_known(current_config, previous_state, event, realm_id) {
            return Ok(AdminEventValidation::Deferred {
                dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
                reason: if current_config.is_some() {
                    "revocation event origin onboarding is not yet materialized"
                } else {
                    "current realm config is unavailable"
                }
                .to_string(),
            });
        }
        return Ok(AdminEventValidation::Accepted);
    }
    // Placement reducer state precedes full config materialization at
    // bootstrap. Only band and handle checks consult it, so the full-state
    // overlay is not paid for the other, far more frequent operations.
    let placement_config = matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
            | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
    )
    .then(|| {
        let mut placement_config = current_config
            .cloned()
            .unwrap_or_else(|| RealmConfigDocument::default_for_realm(realm_id, Vec::new()));
        if let Some(state) = previous_state {
            overlay_realm_config_placement_reducer_materialization(&mut placement_config, state, 0);
        }
        placement_config
    });
    // Band pools form a causal delegation tree; reject a forged or
    // non-owning issuer, and defer a child until its parent replicates.
    if let (AdminDocumentOperation::RealmConfigBandPoolAssigned { pool }, Some(placement_config)) =
        (&event.op, placement_config.as_ref())
    {
        match admit_band_pool(&placement_config.band_pools, pool, &event.origin_node_id) {
            PoolAdmission::Reject => {
                return Ok(AdminEventValidation::Rejected(
                    "band pool lineage is invalid".to_string(),
                ));
            }
            PoolAdmission::MissingParent => {
                return Ok(AdminEventValidation::Deferred {
                    dependency: None,
                    reason: "band pool parent is not yet replicated".to_string(),
                });
            }
            PoolAdmission::Accept => {}
        }
    }
    if let (
        AdminDocumentOperation::RealmConfigHandleRangeGranted { range },
        Some(placement_config),
    ) = (&event.op, placement_config.as_ref())
    {
        let canonical = range.len() == HANDLE_RANGE_SIZE
            && range
                .start
                .checked_sub(FIRST_GRANTABLE_HANDLE)
                .is_some_and(|offset| offset % HANDLE_RANGE_SIZE == 0)
            && range.start.checked_add(HANDLE_RANGE_SIZE) == Some(range.end);
        if !canonical {
            return Ok(AdminEventValidation::Rejected(
                "handle grant is not one canonical band".to_string(),
            ));
        }
        let spans = coordinator_spans(&placement_config.band_pools, &event.origin_node_id);
        if spans.is_empty() {
            return Ok(AdminEventValidation::Deferred {
                dependency: None,
                reason: "coordinator band pool is not yet replicated".to_string(),
            });
        }
        if !spans
            .iter()
            .any(|(start, end)| *start <= range.start && range.end <= *end)
        {
            return Ok(AdminEventValidation::Rejected(
                "handle grant lies outside the coordinator band pool".to_string(),
            ));
        }
    }
    if let Some(config) = current_config {
        let server_binding = match (
            configured_node_kind(config, &event.origin_node_id),
            &event.op,
        ) {
            (
                Some(RealmNodeKind::Server),
                AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding },
            ) => {
                binding.allocated_by == Some(event.origin_node_id)
                    && binding.has_valid_provenance(&config.handle_range_directory())
            }
            _ => false,
        };
        // D9: every holder acts. A barrier, proof, or stall names its own origin
        // and moves no authority on its own, so a holder may emit it.
        let self_report = matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigTransitionBarrierReported { reported_by, .. }
            | AdminDocumentOperation::RealmConfigTransitionStallReported { reported_by, .. }
            | AdminDocumentOperation::RealmConfigTransitionDrainReported { reported_by, .. }
                if *reported_by == event.origin_node_id
        ) || matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigTransitionProofSubmitted { proof, .. }
                if proof.holder == event.origin_node_id
        );
        if matches!(
            configured_node_kind(config, &event.origin_node_id),
            Some(RealmNodeKind::Management)
        ) && matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding }
                if !binding.has_valid_provenance(&config.handle_range_directory())
        ) {
            // The granting range event may still be in flight in the same batch
            // (onboarding writes grant + JobControl binding back to back).
            return Ok(AdminEventValidation::Deferred {
                dependency: None,
                reason: "placement binding provenance is not yet valid".to_string(),
            });
        }
        return Ok(
            if matches!(
                configured_node_kind(config, &event.origin_node_id),
                Some(RealmNodeKind::Management)
            ) || server_binding
                || (self_report && origin_may_publish(config, &event.origin_node_id))
            {
                AdminEventValidation::Accepted
            } else {
                AdminEventValidation::Rejected(
                    "event origin is not a current management node".to_string(),
                )
            },
        );
    }

    let bootstrap = previous_state.is_none()
        && matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id,
                kind: RealmNodeKind::Management,
            } if *node_id == event.origin_node_id
        );
    let continuing_bootstrap = previous_state.is_some_and(|state| {
        state
            .materialized_realm_config_nodes()
            .get(&event.origin_node_id)
            .is_some_and(|kind| matches!(kind, RealmNodeKind::Management))
    });
    Ok(if bootstrap || continuing_bootstrap {
        AdminEventValidation::Accepted
    } else {
        AdminEventValidation::Deferred {
            dependency: None,
            reason: "realm config bootstrap must begin with a management node ensuring itself"
                .to_string(),
        }
    })
}
