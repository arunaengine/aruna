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

pub(in crate::document_sync) async fn validate_realm_authorization_admin_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentReducerState>,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::Realm { realm_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a realm authorization document".to_string(),
        ));
    };
    let current_config = read_admin_realm_config(storage, realm_id).await?;
    if let Some(config) = current_config.as_ref() {
        if config.realm_id != realm_id {
            return Ok(AdminEventValidation::Rejected(
                "stored realm config has the wrong realm".to_string(),
            ));
        }
        return Ok(
            if matches!(
                configured_node_kind(config, &event.origin_node_id),
                Some(RealmNodeKind::Management)
            ) {
                AdminEventValidation::Accepted
            } else {
                AdminEventValidation::Rejected(
                    "event origin is not a current management node".to_string(),
                )
            },
        );
    }

    let current_auth = read_admin_realm_authorization(storage, realm_id).await?;
    let bootstrap = previous_state.is_none()
        && current_auth.is_none()
        && event.actor.user_id.is_nil_in(realm_id)
        && matches!(
            &event.op,
            AdminDocumentOperation::RealmRoleCreated { role }
                if !role.role_id.is_nil()
                    && role.name == "realm_admin"
                    && role.permissions == BTreeMap::from([(
                        format!("/{realm_id}/admin/**"),
                        aruna_core::structs::Permission::WRITE,
                    )])
        );
    Ok(if bootstrap {
        AdminEventValidation::Accepted
    } else {
        AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
            reason: "current realm config is unavailable".to_string(),
        }
    })
}

pub(in crate::document_sync) async fn validate_user_admin_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentReducerState>,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::User { user_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a user".to_string(),
        ));
    };
    let realm_id = user_id.realm_id;
    let Some(config) = read_admin_realm_config(storage, realm_id).await? else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
            reason: "current realm config is unavailable".to_string(),
        });
    };
    if config.realm_id != realm_id {
        return Ok(AdminEventValidation::Rejected(
            "stored realm config has the wrong realm".to_string(),
        ));
    }
    let Some(origin_kind) =
        configured_node_kind(&config, &event.origin_node_id).filter(|kind| kind.is_sync_eligible())
    else {
        return Ok(AdminEventValidation::Rejected(
            "user admin event origin is not a publisher-capable realm node".to_string(),
        ));
    };

    let current_user = storage_read_from(
        storage,
        DocumentSyncTarget::User { user_id }
            .storage_keyspace()
            .to_string(),
        DocumentSyncTarget::User { user_id }.storage_key(),
    )
    .await?
    .map(|bytes| User::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if current_user
        .as_ref()
        .is_some_and(|user| user.user_id != user_id)
    {
        return Ok(AdminEventValidation::Rejected(
            "stored user identity does not match the event".to_string(),
        ));
    }

    let self_service = event.actor.user_id == user_id;
    let management_bootstrap = event.actor.user_id.is_nil_in(realm_id)
        && matches!(origin_kind, RealmNodeKind::Management)
        && event.origin_seq <= 2
        && previous_state.is_none_or(|state| {
            state
                .clock
                .origins
                .keys()
                .all(|origin| *origin == event.origin_node_id)
        });
    let realm_admin = if self_service || management_bootstrap {
        false
    } else {
        let Some(auth) = read_admin_realm_authorization(storage, realm_id).await? else {
            return Ok(AdminEventValidation::Deferred {
                dependency: Some(DocumentSyncDependency::RealmAuthorization(realm_id)),
                reason: "realm authorization state is unavailable".to_string(),
            });
        };
        if auth.realm_id != realm_id {
            return Ok(AdminEventValidation::Rejected(
                "stored realm authorization has the wrong realm".to_string(),
            ));
        }
        has_current_write_permission(
            event.actor.user_id,
            &format!("/{realm_id}/admin/u/{user_id}"),
            auth.roles.values(),
        )
    };
    if !self_service && !management_bootstrap && !realm_admin {
        return Ok(AdminEventValidation::Rejected(
            "actor lacks current user write authority".to_string(),
        ));
    }

    Ok(AdminEventValidation::Accepted)
}

pub(in crate::document_sync) async fn validate_group_admin_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentReducerState>,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::Group { group_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a group".to_string(),
        ));
    };
    let realm_id = event.actor.realm_id;
    let Some(config) = read_admin_realm_config(storage, realm_id).await? else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
            reason: "current realm config is unavailable".to_string(),
        });
    };
    if config.realm_id != realm_id || !origin_may_publish(&config, &event.origin_node_id) {
        return Ok(AdminEventValidation::Rejected(
            "group admin event origin is not a publisher-capable realm node".to_string(),
        ));
    }

    let group_value = storage_read_from(
        storage,
        GROUP_KEYSPACE.to_string(),
        group_id.to_bytes().into(),
    )
    .await?;
    let group = group_value
        .map(|bytes| Group::from_bytes(&bytes))
        .transpose()
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;

    if let AdminDocumentOperation::GroupCreated {
        realm_id: event_realm_id,
        display_name,
        owner,
    } = &event.op
    {
        return Ok(match group {
            None => AdminEventValidation::Accepted,
            Some(group)
                if group.group_id == group_id
                    && group.realm_id == *event_realm_id
                    && group.display_name == *display_name
                    && group.owner == *owner =>
            {
                AdminEventValidation::Accepted
            }
            Some(_) => AdminEventValidation::Rejected(
                "group creation conflicts with the current group".to_string(),
            ),
        });
    }

    let Some(group) = group else {
        return Ok(AdminEventValidation::Deferred {
            dependency: None,
            reason: "group does not exist".to_string(),
        });
    };
    if group.group_id != group_id || group.realm_id != realm_id || group.owner.realm_id != realm_id
    {
        return Ok(AdminEventValidation::Rejected(
            "stored group identity does not match the event".to_string(),
        ));
    }
    if let AdminDocumentOperation::GroupJoinRequested { request } = &event.op {
        if request.user_id != event.actor.user_id || request.group_id != group_id {
            return Ok(AdminEventValidation::Rejected(
                "requester does not match actor or group".into(),
            ));
        }
        if let Some(existing) = previous_state
            .into_iter()
            .flat_map(|state| state.join_requests())
            .find(|entry| entry.request.request_id == request.request_id)
            && existing.request != *request
        {
            return Ok(AdminEventValidation::Rejected(
                "membership request identity is immutable".into(),
            ));
        }
        return Ok(AdminEventValidation::Accepted);
    }
    if let AdminDocumentOperation::GroupJoinDecided { decision } = &event.op {
        let Some(previous) = previous_state else {
            return Ok(AdminEventValidation::Deferred {
                dependency: None,
                reason: "membership request is unavailable".into(),
            });
        };
        let Some(request) = previous
            .join_requests()
            .into_iter()
            .find(|entry| entry.request.request_id == decision.request_id)
        else {
            return Ok(AdminEventValidation::Deferred {
                dependency: None,
                reason: "membership request is unavailable".into(),
            });
        };
        if request.request.user_id != decision.user_id {
            return Ok(AdminEventValidation::Rejected(
                "decision requester does not match the request".into(),
            ));
        }
        if !previous.applied_event_ids.contains(&event.event_id) {
            let path = aruna_core::join_request::decision_path(decision.request_id);
            let observed = previous
                .user_subject_ids
                .get(&path)
                .is_some_and(|version| event.observed.observes(&version.dot))
                || previous.conflicts.get(&path).is_some_and(|conflict| {
                    conflict
                        .values
                        .iter()
                        .any(|value| event.observed.observes(&value.dot))
                });
            if observed {
                return Ok(AdminEventValidation::Rejected(
                    "membership request was already decided".into(),
                ));
            }
        }
        if decision.kind == aruna_core::join_request::JoinDecisionKind::Withdrawn {
            return Ok(if decision.user_id == event.actor.user_id {
                AdminEventValidation::Accepted
            } else {
                AdminEventValidation::Rejected("only the requester may withdraw".into())
            });
        }
    }
    if group.owner == event.actor.user_id {
        return Ok(AdminEventValidation::Accepted);
    }
    if matches!(
        &event.op,
        AdminDocumentOperation::GroupRoleUserAssignmentRemoved { user_id, .. }
            if *user_id == event.actor.user_id
    ) {
        return Ok(AdminEventValidation::Accepted);
    }

    let realm_auth = read_admin_realm_authorization(storage, realm_id).await?;
    let group_auth = storage_read_from(
        storage,
        DocumentSyncTarget::GroupAuthorization { group_id }
            .storage_keyspace()
            .to_string(),
        DocumentSyncTarget::GroupAuthorization { group_id }.storage_key(),
    )
    .await?
    .map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let Some(group_auth) = group_auth else {
        return Ok(AdminEventValidation::Rejected(
            "group authorization state is unavailable".to_string(),
        ));
    };
    let Some(realm_auth) = realm_auth else {
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmAuthorization(realm_id)),
            reason: "realm authorization state is unavailable".to_string(),
        });
    };
    if realm_auth.realm_id != realm_id || group_auth.group_id != group_id {
        return Ok(AdminEventValidation::Rejected(
            "stored authorization identity does not match the group".to_string(),
        ));
    }
    // Any one of these paths carries the authority; a rename is also open to a
    // realm administrator who is not a member of the group.
    let paths = match &event.op {
        AdminDocumentOperation::GroupJoinDecided { .. } => {
            vec![format!("/{realm_id}/g/{group_id}/admin/users/**")]
        }
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { user_id, .. } => {
            vec![format!("/{realm_id}/g/{group_id}/admin/users/{user_id}")]
        }
        AdminDocumentOperation::GroupRoleAdded { .. }
        | AdminDocumentOperation::GroupRoleCreated { .. }
        | AdminDocumentOperation::GroupRoleRemoved { .. } => {
            vec![format!("/{realm_id}/g/{group_id}/admin")]
        }
        AdminDocumentOperation::GroupPoliciesSet { .. } => {
            vec![format!("/{realm_id}/g/{group_id}/admin/config")]
        }
        AdminDocumentOperation::GroupDisplayNameSet { .. } => vec![
            format!("/{realm_id}/g/{group_id}/admin"),
            format!("/{realm_id}/admin/groups"),
        ],
        AdminDocumentOperation::GroupCreated { .. } => unreachable!(),
        _ => unreachable!("group authority only receives group operations"),
    };
    let allowed = paths.iter().any(|path| {
        has_current_write_permission(
            event.actor.user_id,
            path,
            realm_auth.roles.values().chain(group_auth.roles.values()),
        )
    });
    Ok(if allowed {
        AdminEventValidation::Accepted
    } else {
        AdminEventValidation::Rejected("actor lacks current group write authority".to_string())
    })
}

pub(in crate::document_sync) fn has_current_write_permission<'a>(
    user_id: UserId,
    path: &str,
    roles: impl IntoIterator<Item = &'a Role>,
) -> bool {
    !user_id.is_nil() && aruna_core::structs::holds_admin_write(user_id, path, roles.into_iter())
}

/// Validates the self-consistency of a replicated node-usage snapshot against
/// its sync target: the payload must decode, its embedded `node_id` must match
/// the target's node, and the target's derived storage key must attribute back
/// to that same node. Returns a human-readable reason on rejection. Does not
/// check the publisher's identity (the caller enforces that against the signed
/// actor). A zero-counter snapshot is valid: stale-group cleanup publishes them.
pub(in crate::document_sync) fn validate_node_usage_upsert(
    target: &DocumentSyncTarget,
    bytes: &[u8],
) -> std::result::Result<(), String> {
    let DocumentSyncTarget::NodeUsage { node_id, .. } = target else {
        return Err("target is not a node usage snapshot".to_string());
    };
    let snapshot = NodeUsageSnapshot::from_bytes(bytes)
        .map_err(|error| format!("undecodable node usage snapshot: {error}"))?;
    if snapshot.node_id != *node_id {
        return Err(format!(
            "snapshot node id {} does not match target node id {node_id}",
            snapshot.node_id
        ));
    }
    let key = target.storage_key();
    if node_usage_key_node_id(key.as_ref()) != Some(*node_id) {
        return Err(format!(
            "node usage storage key does not attribute to target node id {node_id}"
        ));
    }
    Ok(())
}

/// Validates the self-consistency of a replicated watch-interest digest against
/// its sync target. Does not check the publisher's identity (the caller enforces
/// that against the signed actor). Empty digests are valid: they clear a node's
/// interest for the realm while preserving single-writer ownership.
pub(in crate::document_sync) fn validate_watch_interest(
    target: &DocumentSyncTarget,
    bytes: &[u8],
) -> std::result::Result<(), String> {
    if bytes.len() > NOTIFICATION_WATCH_INTEREST_BYTES_CAP {
        return Err(format!(
            "watch interest digest exceeds serialized byte cap {}",
            NOTIFICATION_WATCH_INTEREST_BYTES_CAP
        ));
    }
    let DocumentSyncTarget::WatchInterest { realm_id, node_id } = target else {
        return Err("target is not a watch interest digest".to_string());
    };
    let digest = WatchInterestDigest::from_bytes(bytes)
        .map_err(|error| format!("undecodable watch interest digest: {error}"))?;
    if digest.entries.len() > NOTIFICATION_WATCH_INTEREST_ENTRY_CAP {
        return Err(format!(
            "watch interest digest exceeds entry cap {}",
            NOTIFICATION_WATCH_INTEREST_ENTRY_CAP
        ));
    }
    if digest.node_id != *node_id {
        return Err(format!(
            "digest node id {} does not match target node id {node_id}",
            digest.node_id
        ));
    }
    let key = target.storage_key();
    if watch_interest_key_node_id(key.as_ref()) != Some(*node_id) {
        return Err(format!(
            "watch interest storage key does not attribute to target node id {node_id}"
        ));
    }
    if watch_interest_key_realm_id(key.as_ref()) != Some(*realm_id) {
        return Err(format!(
            "watch interest storage key does not attribute to target realm id {realm_id}"
        ));
    }
    Ok(())
}
