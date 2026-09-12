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
            let config = read_realm_config(storage, realm_id).await?;
            let state =
                read_reducer_state(storage, &AdminDocumentTarget::RealmConfig { realm_id }).await?;
            self.entry = Some((realm_id, config, state));
        }
        match &self.entry {
            Some((_, config, state)) => Ok((config.as_ref(), state.as_ref())),
            None => Ok((None, None)),
        }
    }
}

pub(in crate::document_sync) async fn read_reducer_state(
    storage: &StorageHandle,
    target: &AdminDocumentTarget,
) -> Result<Option<AdminDocumentReducerState>> {
    storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        reducer_state_key(target),
    )
    .await?
    .map(|bytes| decode_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

pub(in crate::document_sync) async fn read_realm_config(
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

pub(in crate::document_sync) async fn read_realm_authorization(
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
    let path = config_node_path(&event.origin_node_id);
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
    topic_id: ::irokle::TopicId,
    publisher: ::irokle::ActorId,
    realm_id: RealmId,
) -> Result<bool> {
    let Some(config) = read_realm_config(storage, realm_id).await? else {
        return Ok(false);
    };
    Ok(config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .any(|node_id| publisher == ::irokle::actor_id_for(topic_id, node_to_peer(&node_id))))
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
fn report_participation(
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
            // Same-topic retry only: an absent plan after the buffered run
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
    // Placement reducer state precedes config materialization; only band and
    // handle checks consult it, so the overlay is not paid for every op.
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
            overlay_placement(&mut placement_config, state, 0);
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
            .materialized_config_nodes()
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

pub(in crate::document_sync) async fn validate_realm_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentReducerState>,
) -> Result<AdminEventValidation> {
    let AdminDocumentTarget::Realm { realm_id } = event.target else {
        return Ok(AdminEventValidation::Rejected(
            "admin event target is not a realm authorization document".to_string(),
        ));
    };
    let current_config = read_realm_config(storage, realm_id).await?;
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

    let current_auth = read_realm_authorization(storage, realm_id).await?;
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

pub(in crate::document_sync) async fn validate_user_authority(
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
    let Some(config) = read_realm_config(storage, realm_id).await? else {
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
        let Some(auth) = read_realm_authorization(storage, realm_id).await? else {
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
        has_write_permission(
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

pub(in crate::document_sync) async fn validate_group_authority(
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
    let Some(config) = read_realm_config(storage, realm_id).await? else {
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

    let realm_auth = read_realm_authorization(storage, realm_id).await?;
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
        has_write_permission(
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

pub(in crate::document_sync) fn has_write_permission<'a>(
    user_id: UserId,
    path: &str,
    roles: impl IntoIterator<Item = &'a Role>,
) -> bool {
    !user_id.is_nil() && aruna_core::structs::holds_admin_write(user_id, path, roles.into_iter())
}

/// Validates a replicated node-usage snapshot against its target: the payload
/// must decode, embed its target's node, and derive a key attributing to it.
/// The caller checks the publisher; zero-counter snapshots are valid.
pub(in crate::document_sync) fn validate_usage_upsert(
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
    if usage_node_id(key.as_ref()) != Some(*node_id) {
        return Err(format!(
            "node usage storage key does not attribute to target node id {node_id}"
        ));
    }
    Ok(())
}

/// Validates a replicated watch-interest digest against its target. The caller
/// checks the publisher; empty digests are valid and clear a node's interest
/// while preserving single-writer ownership.
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
    if interest_node_id(key.as_ref()) != Some(*node_id) {
        return Err(format!(
            "watch interest storage key does not attribute to target node id {node_id}"
        ));
    }
    if interest_realm_id(key.as_ref()) != Some(*realm_id) {
        return Err(format!(
            "watch interest storage key does not attribute to target realm id {realm_id}"
        ));
    }
    Ok(())
}

pub(in crate::document_sync) fn validate_watch_upsert(
    target: &DocumentSyncTarget,
    bytes: &[u8],
    change: &DocumentSyncChange,
) -> std::result::Result<(), String> {
    let DocumentSyncTarget::WatchSubscription { owner, watch_id } = target else {
        return Err("target is not a watch subscription".to_string());
    };
    validate_watch_target(*owner, *watch_id)?;
    if change.kind != DocumentSyncChangeKind::Upsert || change.current.generation != 1 {
        return Err(
            "watch subscription upsert must carry generation 1 upsert revision".to_string(),
        );
    }
    let subscription = WatchSubscription::from_bytes(bytes)
        .map_err(|error| format!("undecodable watch subscription: {error}"))?;
    if subscription.owner != *owner || subscription.watch_id != *watch_id {
        return Err("watch subscription payload does not match its target".to_string());
    }
    if subscription.path_prefix.is_empty()
        || subscription.path_prefix.starts_with('/')
        || subscription.path_prefix.len() > NOTIFICATION_WATCH_MAX_PREFIX_LEN
    {
        return Err("watch subscription path prefix is invalid".to_string());
    }
    let known_mask = WatchEventMask::METADATA_CREATED
        | WatchEventMask::DATA_UPLOADED
        | WatchEventMask::SYNC_COMPLETED
        | WatchEventMask::SYNC_FAILED;
    if subscription.event_mask.is_empty() || subscription.event_mask.bits() & !known_mask != 0 {
        return Err("watch subscription event mask is invalid".to_string());
    }
    Ok(())
}

pub(in crate::document_sync) fn validate_watch_delete(
    target: &DocumentSyncTarget,
    change: &DocumentSyncChange,
) -> std::result::Result<(), String> {
    let DocumentSyncTarget::WatchSubscription { owner, watch_id } = target else {
        return Err("target is not a watch subscription".to_string());
    };
    validate_watch_target(*owner, *watch_id)?;
    if change.kind != DocumentSyncChangeKind::Delete || change.current.generation != 2 {
        return Err(
            "watch subscription delete must carry generation 2 delete revision".to_string(),
        );
    }
    Ok(())
}

pub(in crate::document_sync) fn validate_watch_target(
    owner: UserId,
    watch_id: Ulid,
) -> std::result::Result<(), String> {
    if owner.is_nil() {
        return Err("watch subscription owner must not be nil".to_string());
    }
    if watch_id.is_nil() {
        return Err("watch subscription id must not be nil".to_string());
    }
    Ok(())
}

/// Validates a replicated node-info document against its target: the payload
/// must decode within bounds, advertise only its own node's execution sites,
/// and match the target's node. The caller checks the publisher.
pub(in crate::document_sync) fn validate_node_upsert(
    target: &DocumentSyncTarget,
    bytes: &[u8],
) -> std::result::Result<NodeInfoDocument, String> {
    let DocumentSyncTarget::NodeInfo { node_id, .. } = target else {
        return Err("target is not a node info document".to_string());
    };
    let document = NodeInfoDocument::from_bytes(bytes)
        .map_err(|error| format!("invalid node info document: {error}"))?;
    if document.node_id != *node_id {
        return Err(format!(
            "node info document node id {} does not match target node id {node_id}",
            document.node_id
        ));
    }
    Ok(document)
}

/// Whether an incoming advertisement replaces the stored one. Undecodable
/// stored bytes are replaced; an equal or older epoch is not applied, so a
/// delayed pre-rejoin advertisement cannot shadow the current one.
pub(in crate::document_sync) fn node_info_supersedes(
    incoming: &NodeInfoDocument,
    stored: Option<&[u8]>,
) -> bool {
    match stored.and_then(|bytes| NodeInfoDocument::from_bytes(bytes).ok()) {
        Some(current) => incoming.supersedes(&current),
        None => true,
    }
}

/// Structural gate for a replicated policy document: it must address this realm
/// and target, carry a verifiable definition, and derive its own sync change,
/// so a sender cannot restate a policy under someone else's revision.
pub(in crate::document_sync) fn validate_policy_document(
    policy_id: Ulid,
    realm_id: RealmId,
    document: &PlacementPolicyDocument,
    change: &DocumentSyncChange,
) -> std::result::Result<(), String> {
    if document.policy_id() != policy_id {
        return Err(format!(
            "policy {} does not match target policy {policy_id}",
            document.policy_id()
        ));
    }
    if document.realm_id != realm_id {
        return Err(format!(
            "policy realm {} does not match this realm",
            document.realm_id
        ));
    }
    if let Err(error) = document.verified() {
        return Err(format!("policy definition is invalid: {error}"));
    }
    // Structural gate: bytes without an authentic publication are never a rule.
    if let Err(error) = document.verify_publication() {
        return Err(format!("policy publication is invalid: {error}"));
    }
    if change.kind != DocumentSyncChangeKind::Upsert {
        return Err("policy event is not an upsert".to_string());
    }
    if *change != placement_policy_change(document, change.placement) {
        return Err("policy revision does not match its sync change".to_string());
    }
    Ok(())
}

/// Validates a replicated PID mapping against its target and change: the
/// canonical PID, a consistent kind/status/provenance triple, and the derived
/// change. The caller enforces publisher identity against the signed actor.
pub(in crate::document_sync) fn validate_pid_mapping(
    document_id: Ulid,
    mapping: &PersistentIdMapping,
    change: &DocumentSyncChange,
) -> std::result::Result<(), String> {
    if mapping.target != document_id {
        return Err(format!(
            "mapping target {} does not match target document {document_id}",
            mapping.target
        ));
    }
    if mapping.pid != MetadataRegistryRecord::graph_iri_for(document_id)
        && mapping.pid != PersistentIdMapping::profile_pid(document_id)
    {
        return Err(format!("mapping pid `{}` is not canonical", mapping.pid));
    }
    if !matches!(mapping.kind, PersistentIdKind::Conceptual) {
        return Err("mapping kind is unsupported".to_string());
    }
    if !matches!(mapping.provider, PersistentIdProvider::W3id) {
        return Err("mapping provider is unsupported".to_string());
    }
    if mapping.requested_at_ms.is_some() != mapping.requested_by.is_some() {
        return Err("mapping request provenance is incomplete".to_string());
    }
    if mapping.minted_at_ms.is_some() != mapping.minted_by.is_some() {
        return Err("mapping mint provenance is incomplete".to_string());
    }
    match mapping.status {
        PersistentIdStatus::Active => {
            if mapping.minted_at_ms.is_none() || mapping.withdrawn_at_ms.is_some() {
                return Err("active mapping has inconsistent transition fields".to_string());
            }
        }
        PersistentIdStatus::Requested | PersistentIdStatus::Processing => {
            if mapping.requested_at_ms.is_none()
                || mapping.minted_at_ms.is_some()
                || mapping.withdrawn_at_ms.is_some()
            {
                return Err("pending mapping has inconsistent transition fields".to_string());
            }
        }
        PersistentIdStatus::Failed => {
            if mapping.requested_at_ms.is_none()
                || mapping.failure.is_none()
                || mapping.withdrawn_at_ms.is_some()
            {
                return Err("failed mapping has inconsistent transition fields".to_string());
            }
        }
        PersistentIdStatus::AdminWithdrawn => {
            if mapping.withdrawn_at_ms.is_none()
                || mapping.withdrawn_by.is_none()
                || mapping
                    .withdrawal_reason
                    .as_deref()
                    .is_none_or(str::is_empty)
            {
                return Err("admin-withdrawn mapping lacks actor or reason".to_string());
            }
        }
        PersistentIdStatus::Tombstoned => {
            if mapping.withdrawn_at_ms.is_none() {
                return Err("tombstoned mapping has no deletion timestamp".to_string());
            }
        }
    }
    if change.kind != DocumentSyncChangeKind::Upsert {
        return Err("mapping event is not an upsert".to_string());
    }
    if *change != persistent_id_change(mapping, change.placement) {
        return Err("mapping revision does not match its sync change".to_string());
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::document_sync) enum AdminEventValidation {
    Accepted,
    Rejected(String),
    Deferred {
        dependency: Option<DocumentSyncDependency>,
        reason: String,
    },
}

/// Per-batch snapshot of the realm-config document and reducer state for
/// admin-event validation. Decoding both per event is quadratic, and a buffered
/// run applies nothing, so the snapshot holds until the caller invalidates it.
#[derive(Default)]
pub(in crate::document_sync) struct ConfigValidationCache {
    entry: Option<(
        RealmId,
        Option<RealmConfigDocument>,
        Option<AdminDocumentReducerState>,
    )>,
}

pub(in crate::document_sync) async fn read_group_authorization(
    storage: &StorageHandle,
    group_id: GroupId,
) -> Result<Option<GroupAuthorizationDocument>> {
    let target = DocumentSyncTarget::GroupAuthorization { group_id };
    storage_read_from(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
    )
    .await?
    .map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
}

/// Validates one replicated administrative event. Authority comes from the
/// origin's signature, never the transport publisher: a relay may carry an
/// origin's event but cannot forge, re-target, or re-actor it.
#[allow(clippy::too_many_arguments)]
pub(in crate::document_sync) async fn validate_admin_event(
    storage: &StorageHandle,
    topic_id: ::irokle::TopicId,
    authenticated_actor_id: ::irokle::ActorId,
    target: &DocumentSyncTarget,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
    placement: &PlacementRef,
    origin_signature: &iroh::Signature,
    config_cache: &mut ConfigValidationCache,
) -> Result<AdminEventValidation> {
    let reject = |reason: &str| Ok(AdminEventValidation::Rejected(reason.to_string()));

    if target.sync_topic_id(realm_id, placement) != topic_id {
        return reject("document sync target does not belong to the reconciled topic");
    }
    if event.origin_node_id != event.actor.node_id {
        return reject("event origin node does not match its actor node");
    }
    if !event.origin_signed(placement, origin_signature) {
        return reject("admin event is not signed by its origin node");
    }
    // A relay hop must itself be a realm node allowed administrative traffic;
    // before config materializes only the origin may publish.
    let self_published = authenticated_actor_id
        == ::irokle::actor_id_for(topic_id, node_to_peer(&event.origin_node_id));
    if !self_published
        && !relay_publisher_allowed(storage, topic_id, authenticated_actor_id, realm_id).await?
    {
        return reject("relayed admin event publisher is not a realm relay node");
    }
    if event.actor.user_id.realm_id != event.actor.realm_id {
        return reject("actor user and actor realm do not match");
    }
    if event.event_id.is_nil() || event.origin_seq == 0 {
        return reject("event id and origin sequence must be non-zero");
    }
    if event
        .observed
        .sequence_for(&event.origin_node_id)
        .checked_add(1)
        != Some(event.origin_seq)
    {
        return reject("event origin sequence does not follow its observed clock");
    }

    // This match is deliberately exhaustive. Adding an operation requires an
    // explicit inbound authorization decision here before it can reach storage.
    let family = match &event.op {
        AdminDocumentOperation::GroupRoleAdded { .. }
        | AdminDocumentOperation::GroupRoleUserAssignmentAdded { .. }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { .. }
        | AdminDocumentOperation::GroupRoleCreated { .. }
        | AdminDocumentOperation::GroupRoleRemoved { .. }
        | AdminDocumentOperation::GroupCreated { .. }
        | AdminDocumentOperation::GroupDisplayNameSet { .. }
        | AdminDocumentOperation::GroupPoliciesSet { .. }
        | AdminDocumentOperation::GroupJoinRequested { .. }
        | AdminDocumentOperation::GroupJoinDecided { .. } => AdminOperationFamily::Group,
        AdminDocumentOperation::RealmRoleAdded { .. }
        | AdminDocumentOperation::RealmRoleUserAssignmentAdded { .. }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { .. }
        | AdminDocumentOperation::RealmRoleCreated { .. } => {
            AdminOperationFamily::RealmAuthorization
        }
        AdminDocumentOperation::UserAttributeSet { .. }
        | AdminDocumentOperation::UserAttributeRemoved { .. }
        | AdminDocumentOperation::UserNameSet { .. }
        | AdminDocumentOperation::UserSubjectIdAdded { .. }
        | AdminDocumentOperation::UserSubjectIdRemoved { .. } => AdminOperationFamily::User,
        AdminDocumentOperation::RealmConfigNodeEnsured { .. }
        | AdminDocumentOperation::RealmConfigNodeRemoved { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderUpserted { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderRemoved { .. }
        | AdminDocumentOperation::RealmConfigSettingsSet { .. }
        | AdminDocumentOperation::RealmConfigDescriptionSet { .. }
        | AdminDocumentOperation::RealmConfigQuotaSet { .. }
        | AdminDocumentOperation::RealmConfigNodePlacementSet { .. }
        | AdminDocumentOperation::RealmConfigNodePlacementRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { .. }
        | AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { .. }
        | AdminDocumentOperation::RealmConfigDefaultStrategySet { .. }
        | AdminDocumentOperation::RealmConfigJobFamilySet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
        | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
        | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
        | AdminDocumentOperation::RealmConfigPoliciesSet { .. }
        | AdminDocumentOperation::RealmConfigCandidateMapPublished { .. }
        | AdminDocumentOperation::RealmConfigActivationsInitialized { .. }
        | AdminDocumentOperation::RealmConfigTransitionStarted { .. }
        | AdminDocumentOperation::RealmConfigTransitionBarrierReported { .. }
        | AdminDocumentOperation::RealmConfigTransitionProofSubmitted { .. }
        | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
        | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. }
        | AdminDocumentOperation::RealmConfigTransitionStallReported { .. }
        | AdminDocumentOperation::RealmConfigTransitionDrainReported { .. }
        | AdminDocumentOperation::RealmConfigComputeSet { .. }
        | AdminDocumentOperation::RealmConfigTokenRevoked { .. } => {
            AdminOperationFamily::RealmConfig
        }
    };

    let target_matches = matches!(
        (family, target, &event.target),
        (
            AdminOperationFamily::Group,
            DocumentSyncTarget::GroupAuthorization { group_id },
            AdminDocumentTarget::Group { group_id: event_group_id }
        ) if group_id == event_group_id
    ) || matches!(
        (family, target, &event.target),
        (
            AdminOperationFamily::RealmAuthorization,
            DocumentSyncTarget::RealmAuthorization { realm_id },
            AdminDocumentTarget::Realm { realm_id: event_realm_id }
        ) if realm_id == event_realm_id
    ) || matches!(
        (family, target, &event.target),
        (
            AdminOperationFamily::User,
            DocumentSyncTarget::User { user_id },
            AdminDocumentTarget::User { user_id: event_user_id }
        ) if user_id == event_user_id
    ) || matches!(
        (family, target, &event.target),
        (
            AdminOperationFamily::RealmConfig,
            DocumentSyncTarget::RealmConfig { realm_id },
            AdminDocumentTarget::RealmConfig { realm_id: event_realm_id }
        ) if realm_id == event_realm_id
    );
    if !target_matches {
        return reject("operation, sync target, and admin event target do not match");
    }

    let target_realm = match &event.target {
        AdminDocumentTarget::Realm { realm_id } | AdminDocumentTarget::RealmConfig { realm_id } => {
            Some(*realm_id)
        }
        AdminDocumentTarget::User { user_id } => Some(user_id.realm_id),
        AdminDocumentTarget::Group { .. } => None,
    };
    if target_realm.is_some_and(|realm_id| realm_id != event.actor.realm_id) {
        return reject("admin event target and actor realms do not match");
    }
    if matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding }
            if matches!(
                binding.scope,
                aruna_core::structs::PlacementScope::Realm(binding_realm_id)
                    if binding_realm_id != event.actor.realm_id
            )
    ) {
        return reject("placement binding realm does not match the admin event target");
    }

    match &event.op {
        AdminDocumentOperation::GroupJoinRequested { .. }
        | AdminDocumentOperation::GroupJoinDecided { .. } => {}
        AdminDocumentOperation::GroupCreated {
            realm_id, owner, ..
        } => {
            if *realm_id != event.actor.realm_id
                || owner.realm_id != *realm_id
                || *owner != event.actor.user_id
                || owner.is_nil()
            {
                return reject("group creation realm and owner must match the actor");
            }
        }
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { user_id, .. }
        | AdminDocumentOperation::RealmRoleUserAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { user_id, .. } => {
            if user_id.realm_id != event.actor.realm_id {
                return reject("role assignment user belongs to a different realm");
            }
        }
        AdminDocumentOperation::GroupRoleCreated { role } => {
            // Distributed events must enforce the same subtree confinement as
            // local issuance; the publisher is not trusted to have done so.
            let AdminDocumentTarget::Group { group_id } = &event.target else {
                return reject("group role event target must be a group");
            };
            let subtree_root =
                aruna_core::permission_path::role_subtree_root(event.actor.realm_id, group_id);
            if role.permissions.keys().any(|pattern| {
                !aruna_core::permission_path::role_path_confined(pattern, &subtree_root)
            }) {
                return reject("group role grants outside its group subtree");
            }
        }
        AdminDocumentOperation::GroupRoleAdded { .. }
        | AdminDocumentOperation::GroupRoleRemoved { .. }
        | AdminDocumentOperation::GroupDisplayNameSet { .. }
        | AdminDocumentOperation::RealmRoleAdded { .. }
        | AdminDocumentOperation::RealmRoleCreated { .. }
        | AdminDocumentOperation::UserAttributeSet { .. }
        | AdminDocumentOperation::UserAttributeRemoved { .. }
        | AdminDocumentOperation::UserNameSet { .. }
        | AdminDocumentOperation::UserSubjectIdAdded { .. }
        | AdminDocumentOperation::UserSubjectIdRemoved { .. }
        | AdminDocumentOperation::RealmConfigNodeEnsured { .. }
        | AdminDocumentOperation::RealmConfigNodeRemoved { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderUpserted { .. }
        | AdminDocumentOperation::RealmConfigOidcProviderRemoved { .. }
        | AdminDocumentOperation::RealmConfigSettingsSet { .. }
        | AdminDocumentOperation::RealmConfigDescriptionSet { .. }
        | AdminDocumentOperation::RealmConfigQuotaSet { .. }
        | AdminDocumentOperation::RealmConfigNodePlacementRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { .. }
        | AdminDocumentOperation::RealmConfigDefaultStrategySet { .. }
        | AdminDocumentOperation::RealmConfigJobFamilySet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
        | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
        | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
        | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. } => {}
        AdminDocumentOperation::RealmConfigComputeSet { compute } => {
            // A malformed link or quota set would make every planner estimate
            // meaningless, so it is refused before it reaches storage.
            if compute.validate().is_err() {
                return reject("realm compute configuration is malformed");
            }
        }
        AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
        | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
        | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
        | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. } => {}
        AdminDocumentOperation::RealmConfigCandidateMapPublished { map } => {
            let mut seen = std::collections::BTreeSet::new();
            if map.epoch == 0 || !map.nodes.iter().all(|node| seen.insert(node.node_id)) {
                return reject("candidate placement map is malformed");
            }
        }
        AdminDocumentOperation::RealmConfigActivationsInitialized {
            candidate_map_epoch,
            ..
        } => {
            if *candidate_map_epoch == 0 {
                return reject("activation names no candidate map epoch");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionStarted { plan } => {
            let mut seen = std::collections::BTreeSet::new();
            let well_formed = plan.limits.max_incomplete_buckets >= 1
                && plan.target_map_epoch > 0
                && !plan.buckets.is_empty()
                && plan
                    .buckets
                    .iter()
                    .all(|bucket| seen.insert(bucket.bucket) && !bucket.target_holders.is_empty());
            if !well_formed {
                return reject("placement transition plan is malformed");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionBarrierReported {
            reported_by,
            frontier,
            ..
        } => {
            if *reported_by != event.origin_node_id {
                return reject("transition report does not come from the node it names");
            }
            if frontier.len() > aruna_core::structs::MAX_BARRIER_FRONTIER_BYTES {
                return reject("transition barrier frontier exceeds its size bound");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionStallReported {
            reported_by,
            reason,
            ..
        } => {
            if *reported_by != event.origin_node_id {
                return reject("transition report does not come from the node it names");
            }
            if reason.len() > aruna_core::structs::MAX_STALL_REASON_BYTES {
                return reject("transition stall reason exceeds its size bound");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionDrainReported { reported_by, .. } => {
            if *reported_by != event.origin_node_id {
                return reject("transition report does not come from the node it names");
            }
        }
        AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
            transition_id,
            strategy_id,
            proof,
        } => {
            // Verified here as well as in the reducer: a forged proof must never
            // reach storage, and the publisher binding already fixed the origin.
            if proof.holder != event.origin_node_id
                || !proof.verify(event.actor.realm_id, *transition_id, *strategy_id)
            {
                return reject("transition completion proof does not verify");
            }
        }
        AdminDocumentOperation::RealmConfigNodePlacementSet { entry } => {
            if let Some(label) = reserved_label(&entry.labels) {
                return reject(&format!(
                    "placement entry must not set derived label {label}"
                ));
            }
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy }
            if strategy.replica_count == Some(0) =>
        {
            return reject("placement strategy replica count must be greater than zero");
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { .. } => {}
        AdminDocumentOperation::RealmConfigPoliciesSet { policies }
        | AdminDocumentOperation::GroupPoliciesSet { policies } => {
            if let Err(error) = aruna_core::request_policy::validate_policy_set(policies) {
                return reject(&format!("invalid policy set: {error}"));
            }
        }
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash,
            expires_at,
            token_owner,
            ..
        } => {
            if !aruna_core::auth::valid_token_hash(token_hash) {
                return reject("revoked bearer token hash is malformed");
            }
            if !valid_revocation_expiry(*expires_at, unix_timestamp_secs()) {
                return reject("revoked bearer token expiry exceeds the admission window");
            }
            if token_owner.is_nil() || token_owner.realm_id != event.actor.realm_id {
                return reject("revoked bearer token owner is malformed");
            }
        }
    }

    let previous_state = match family {
        AdminOperationFamily::RealmConfig => {
            let AdminDocumentTarget::RealmConfig {
                realm_id: event_realm_id,
            } = event.target
            else {
                return reject("admin event target is not a realm config");
            };
            let (current_config, cached_state) = config_cache.load(storage, event_realm_id).await?;
            let authorized = validate_config_authority(current_config, event, cached_state)?;
            if !matches!(authorized, AdminEventValidation::Accepted) {
                return Ok(authorized);
            }
            cached_state.cloned()
        }
        family => {
            let previous_state = read_reducer_state(storage, &event.target).await?;
            let authorized = match family {
                AdminOperationFamily::RealmAuthorization => {
                    validate_realm_authority(storage, event, previous_state.as_ref()).await?
                }
                AdminOperationFamily::Group => {
                    validate_group_authority(storage, event, previous_state.as_ref()).await?
                }
                _ => validate_user_authority(storage, event, previous_state.as_ref()).await?,
            };
            if !matches!(authorized, AdminEventValidation::Accepted) {
                return Ok(authorized);
            }
            previous_state
        }
    };

    if previous_state
        .as_ref()
        .is_some_and(|state| state.target != event.target)
    {
        return reject("stored admin reducer state has the wrong target");
    }
    if let AdminDocumentOperation::RealmConfigTokenRevoked { token_hash, .. } = &event.op {
        if revocation_origin_full(previous_state.as_ref(), event, token_hash) {
            return reject("revocation origin reached its live revocation cap");
        }
        return Ok(AdminEventValidation::Accepted);
    }

    let mut reducer_state =
        previous_state.unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    if let Err(error) = reducer_state.apply(event) {
        return Ok(AdminEventValidation::Rejected(format!(
            "admin operation is malformed: {error}"
        )));
    }

    Ok(AdminEventValidation::Accepted)
}

/// Who a transition report claims to be, against the named plan's roles.
enum ReportParticipation {
    NotReport,
    Participant,
    UnknownPlan,
    Foreign,
}
