//! Checks admin document events against realm config, group rights and shape rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::user::validation::{MERGED_INTO_ATTRIBUTE, MERGED_PREFIX};

impl ConfigValidationCache {
    pub(in crate::document_sync) fn invalidate(&mut self) {
        self.entry = None;
    }

    pub(in crate::document_sync) async fn load(
        &mut self,
        storage: &StorageHandle,
        realm_id: RealmId,
    ) -> Result<(Option<&RealmConfigDocument>, Option<&AdminDocumentState>)> {
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
) -> Result<Option<AdminDocumentState>> {
    storage_read_from(
        storage,
        DOCUMENT_STATE_KEYSPACE.to_string(),
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
        DocumentTarget::RealmConfig { realm_id }
            .storage_keyspace()
            .to_string(),
        DocumentTarget::RealmConfig { realm_id }.storage_key(),
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
        DocumentTarget::RealmAuthorization { realm_id }
            .storage_keyspace()
            .to_string(),
        DocumentTarget::RealmAuthorization { realm_id }.storage_key(),
    )
    .await?
    .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))
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
        .any(|node_id| published_by(topic_id, publisher, &node_id)))
}

pub(in crate::document_sync) async fn validate_realm_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentState>,
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
                config.node_kind(&event.origin_node_id),
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
                        aruna_core::structs::identity::auth::Permission::WRITE,
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
    previous_state: Option<&AdminDocumentState>,
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
    let Some(origin_kind) = config
        .node_kind(&event.origin_node_id)
        .filter(|kind| kind.is_sync_eligible())
    else {
        return Ok(AdminEventValidation::Rejected(
            "user admin event origin is not a publisher-capable realm node".to_string(),
        ));
    };

    let current_user = storage_read_from(
        storage,
        DocumentTarget::User { user_id }
            .storage_keyspace()
            .to_string(),
        DocumentTarget::User { user_id }.storage_key(),
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

    // Merge pointers move logins between accounts, so only a management node's merge sets them,
    // never a self-service or other attribute change.
    let merge_key = match &event.op {
        AdminDocumentOperation::UserAttributeSet { key, .. }
        | AdminDocumentOperation::UserAttributeRemoved { key } => {
            key == MERGED_INTO_ATTRIBUTE || key.starts_with(MERGED_PREFIX)
        }
        _ => false,
    };
    if merge_key && !matches!(origin_kind, RealmNodeKind::Management) {
        return Ok(AdminEventValidation::Rejected(
            "merge attributes come only from a management node's merge".to_string(),
        ));
    }
    let self_service = event.actor.user_id == user_id;
    // Only the account owner links a login; the owner or a realm administrator unlinks it.
    let alias = matches!(
        event.op,
        AdminDocumentOperation::UserAliasAdded { .. }
            | AdminDocumentOperation::UserAliasRemoved { .. }
    );
    let management_bootstrap = !alias
        && event.actor.user_id.is_nil_in(realm_id)
        && matches!(origin_kind, RealmNodeKind::Management)
        && event.origin_seq <= 2
        && previous_state.is_none_or(|state| {
            state
                .clock
                .origins
                .keys()
                .all(|origin| *origin == event.origin_node_id)
        });
    let realm_admin = if self_service
        || management_bootstrap
        || matches!(event.op, AdminDocumentOperation::UserAliasAdded { .. })
    {
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
    let group_admin = if self_service || management_bootstrap || realm_admin || alias {
        false
    } else if let Some(group_id) = service_owner(event, previous_state, current_user.as_ref()) {
        let Some(auth) = read_group_authorization(storage, group_id).await? else {
            return Ok(AdminEventValidation::Deferred {
                dependency: Some(DocumentSyncDependency::GroupAuthorization(group_id)),
                reason: "group authorization state is unavailable".to_string(),
            });
        };
        has_write_permission(
            event.actor.user_id,
            &format!("/{realm_id}/g/{group_id}/admin"),
            auth.roles.values(),
        )
    } else {
        false
    };
    if !self_service && !management_bootstrap && !realm_admin && !group_admin {
        return Ok(AdminEventValidation::Rejected(
            "actor lacks current user write authority".to_string(),
        ));
    }

    Ok(AdminEventValidation::Accepted)
}

pub(in crate::document_sync) async fn validate_group_authority(
    storage: &StorageHandle,
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentState>,
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

    if previous_state.is_some_and(AdminDocumentState::group_deleted) {
        return Ok(AdminEventValidation::Accepted);
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
    if let AdminDocumentOperation::GroupDeleted { certificate } = &event.op
        && certificate.plan.owner != group.owner
    {
        return Ok(AdminEventValidation::Rejected(
            "deletion owner does not match the group".into(),
        ));
    }
    if let AdminDocumentOperation::GroupDeleted { certificate } = &event.op
        && certificate.plan.requested_by == event.actor.user_id
        && certificate.verify()
    {
        use aruna_core::structs::identity::group_delete::{GroupDeletePhase, GroupDeleteRecord};
        let prepared = storage_read_from(
            storage,
            aruna_core::keyspaces::GROUP_DELETE_KEYSPACE.to_string(),
            group_id.to_bytes().into(),
        )
        .await?
        .map(|value| GroupDeleteRecord::from_bytes(&value))
        .transpose()
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        if certificate.plan.matches_config(&config)
            || prepared.is_some_and(|record| {
                record.plan == certificate.plan && record.phase == GroupDeletePhase::Preparing
            })
        {
            return Ok(AdminEventValidation::Accepted);
        }
        return Ok(AdminEventValidation::Deferred {
            dependency: Some(DocumentSyncDependency::RealmConfig(realm_id)),
            reason: "deletion participant set has not synchronized".into(),
        });
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
        AdminDocumentOperation::GroupAssignmentRemoved { user_id, .. }
            if *user_id == event.actor.user_id
    ) {
        return Ok(AdminEventValidation::Accepted);
    }

    let realm_auth = read_realm_authorization(storage, realm_id).await?;
    let group_auth = storage_read_from(
        storage,
        DocumentTarget::GroupAuthorization { group_id }
            .storage_keyspace()
            .to_string(),
        DocumentTarget::GroupAuthorization { group_id }.storage_key(),
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
    // An account merge hands each role of the merged account to the surviving account; only a
    // management node merges, after both accounts proved a fresh login.
    if let AdminDocumentOperation::GroupAssignmentAdded { role_id, .. } = &event.op
        && matches!(
            config.node_kind(&event.origin_node_id),
            Some(RealmNodeKind::Management)
        )
        && group_auth
            .roles
            .get(role_id)
            .is_some_and(|role| role.assigned_users.contains(&event.actor.user_id))
    {
        return Ok(AdminEventValidation::Accepted);
    }
    // Any one of these paths carries the authority; a rename is also open to a
    // realm administrator who is not a member of the group.
    let paths = match &event.op {
        AdminDocumentOperation::GroupJoinDecided { .. } => {
            vec![format!("/{realm_id}/g/{group_id}/admin/users/**")]
        }
        AdminDocumentOperation::GroupAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::GroupAssignmentRemoved { user_id, .. } => {
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
        AdminDocumentOperation::DisplayNameSet { .. }
        | AdminDocumentOperation::GroupLocationSet { .. }
        | AdminDocumentOperation::GroupDeleted { .. } => vec![
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

/// Per-batch snapshot of the realm-config document and reducer state for
/// admin-event validation. Decoding both per event is quadratic, and a buffered
/// run applies nothing, so the snapshot holds until the caller invalidates it.
#[derive(Default)]
pub(in crate::document_sync) struct ConfigValidationCache {
    entry: Option<(
        RealmId,
        Option<RealmConfigDocument>,
        Option<AdminDocumentState>,
    )>,
}

pub(in crate::document_sync) async fn read_group_authorization(
    storage: &StorageHandle,
    group_id: GroupId,
) -> Result<Option<GroupAuthorizationDocument>> {
    let target = DocumentTarget::GroupAuthorization { group_id };
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

/// Shared admission prerequisites for every replicated admin event: the topic
/// binding, origin signature, relay permission, actor pairing, and observed
/// clock. `Accepted` means the event may reach its family decision.
#[allow(clippy::too_many_arguments)]
async fn validate_admin_envelope(
    storage: &StorageHandle,
    topic_id: ::irokle::TopicId,
    authenticated_actor_id: ::irokle::ActorId,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
    placement: &PlacementRef,
    origin_signature: &iroh::Signature,
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
    let self_published = published_by(topic_id, authenticated_actor_id, &event.origin_node_id);
    if !self_published
        && !relay_publisher_allowed(storage, topic_id, authenticated_actor_id, realm_id).await?
    {
        return reject("relayed admin event publisher is not a realm relay node");
    }
    if event.actor.user_id.realm_id != event.actor.realm_id
        && !foreign_actor_allowed(event, realm_id)
    {
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
    Ok(AdminEventValidation::Accepted)
}

/// Whether the reducer accepts the event; a rejected reduction is malformed
/// input, never a family authority decision.
fn accept_reducer_event(
    previous_state: Option<AdminDocumentState>,
    event: &AdminDocumentEvent,
) -> Result<AdminEventValidation> {
    let mut reducer_state =
        previous_state.unwrap_or_else(|| AdminDocumentState::new(event.target.clone()));
    match reducer_state.apply(event) {
        Ok(_) => Ok(AdminEventValidation::Accepted),
        Err(error) => Ok(AdminEventValidation::Rejected(format!(
            "admin operation is malformed: {error}"
        ))),
    }
}

/// Validates one replicated administrative event. Authority comes from the
/// origin's signature, never the transport publisher: a relay may carry an
/// origin's event but cannot forge, re-target, or re-actor it.
#[allow(clippy::too_many_arguments)]
pub(in crate::document_sync) async fn validate_admin_event(
    storage: &StorageHandle,
    topic_id: ::irokle::TopicId,
    authenticated_actor_id: ::irokle::ActorId,
    target: &DocumentTarget,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
    placement: &PlacementRef,
    origin_signature: &iroh::Signature,
    config_cache: &mut ConfigValidationCache,
) -> Result<AdminEventValidation> {
    let reject = |reason: &str| Ok(AdminEventValidation::Rejected(reason.to_string()));

    let admission = validate_admin_envelope(
        storage,
        topic_id,
        authenticated_actor_id,
        target,
        event,
        realm_id,
        placement,
        origin_signature,
    )
    .await?;
    if !matches!(admission, AdminEventValidation::Accepted) {
        return Ok(admission);
    }

    let family = AdminOperationFamily::of(&event.op);

    if !family_matches_targets(family, target, &event.target) {
        return reject("operation, sync target, and admin event target do not match");
    }
    if let Err(reason) = validate_event_scope(event) {
        return reject(&reason);
    }

    if let Err(reason) = validate_shape(family, event) {
        return reject(&reason);
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
            let authorized = validate_config_authority(current_config, event, cached_state);
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
                AdminOperationFamily::Distribution => {
                    validate_distribution_authority(storage, event).await?
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
    if let AdminDocumentOperation::ConfigTokenRevoked { token_hash, .. } = &event.op {
        if revocation_origin_full(previous_state.as_ref(), event, token_hash) {
            return reject("revocation origin reached its live revocation cap");
        }
        return Ok(AdminEventValidation::Accepted);
    }

    accept_reducer_event(previous_state, event)
}
