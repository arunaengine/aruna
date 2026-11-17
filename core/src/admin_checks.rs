//! Checks the shape of replicated admin events by family, before any authority is consulted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::NodeId;
use crate::UserId;
use crate::admin_documents::{AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget};
use crate::auth::valid_revocation_expiry;
use crate::document::DocumentTarget;
use crate::reducer::{AdminDocumentState, REVOCATIONS_PER_ORIGIN, config_node_path};
use crate::structs::identity::auth::Role;
use crate::structs::identity::realm::RealmId;
use crate::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use crate::structs::identity::user::User;
use crate::structs::storage::node_info::reserved_label;
use crate::time::unix_timestamp_secs;
use crate::types::GroupId;
use crate::user::validation::{DEACTIVATED_ATTRIBUTE, INSTALL_ATTRIBUTE, SERVICE_GROUP_ATTRIBUTE};

/// The document family an admin operation changes; each family has its own sync target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdminOperationFamily {
    Group,
    RealmAuthorization,
    User,
    RealmConfig,
    Distribution,
}

impl AdminOperationFamily {
    /// Deliberately exhaustive: a new operation needs an explicit inbound authorization
    /// decision here before it can reach storage.
    pub fn of(op: &AdminDocumentOperation) -> Self {
        match op {
            AdminDocumentOperation::GroupRoleAdded { .. }
            | AdminDocumentOperation::GroupAssignmentAdded { .. }
            | AdminDocumentOperation::GroupAssignmentRemoved { .. }
            | AdminDocumentOperation::GroupRoleCreated { .. }
            | AdminDocumentOperation::GroupRoleRemoved { .. }
            | AdminDocumentOperation::GroupCreated { .. }
            | AdminDocumentOperation::GroupDeleted { .. }
            | AdminDocumentOperation::DisplayNameSet { .. }
            | AdminDocumentOperation::GroupLocationSet { .. }
            | AdminDocumentOperation::GroupPoliciesSet { .. }
            | AdminDocumentOperation::GroupJoinRequested { .. }
            | AdminDocumentOperation::GroupJoinDecided { .. } => Self::Group,
            AdminDocumentOperation::RealmRoleAdded { .. }
            | AdminDocumentOperation::RealmAssignmentAdded { .. }
            | AdminDocumentOperation::RealmAssignmentRemoved { .. }
            | AdminDocumentOperation::RealmRoleCreated { .. } => Self::RealmAuthorization,
            AdminDocumentOperation::UserAttributeSet { .. }
            | AdminDocumentOperation::UserAttributeRemoved { .. }
            | AdminDocumentOperation::UserNameSet { .. }
            | AdminDocumentOperation::SubjectIdAdded { .. }
            | AdminDocumentOperation::SubjectIdRemoved { .. }
            | AdminDocumentOperation::UserAliasAdded { .. }
            | AdminDocumentOperation::UserAliasRemoved { .. } => Self::User,
            AdminDocumentOperation::ConfigNodeEnsured { .. }
            | AdminDocumentOperation::ConfigNodeRemoved { .. }
            | AdminDocumentOperation::OidcProviderUpserted { .. }
            | AdminDocumentOperation::OidcProviderRemoved { .. }
            | AdminDocumentOperation::ConfigSettingsSet { .. }
            | AdminDocumentOperation::ConfigDescriptionSet { .. }
            | AdminDocumentOperation::ConfigQuotaSet { .. }
            | AdminDocumentOperation::NodePlacementSet { .. }
            | AdminDocumentOperation::NodePlacementRemoved { .. }
            | AdminDocumentOperation::PlacementStrategyUpserted { .. }
            | AdminDocumentOperation::PlacementStrategyRemoved { .. }
            | AdminDocumentOperation::ConfigStrategySet { .. }
            | AdminDocumentOperation::JobFamilySet { .. }
            | AdminDocumentOperation::StrategyBindingSet { .. }
            | AdminDocumentOperation::StrategyBindingRemoved { .. }
            | AdminDocumentOperation::PlacementOverrideSet { .. }
            | AdminDocumentOperation::PlacementOverrideRemoved { .. }
            | AdminDocumentOperation::PlacementBindingAppended { .. }
            | AdminDocumentOperation::HandleRangeGranted { .. }
            | AdminDocumentOperation::BandPoolAssigned { .. }
            | AdminDocumentOperation::ConfigPoliciesSet { .. }
            | AdminDocumentOperation::CandidateMapPublished { .. }
            | AdminDocumentOperation::ConfigActivationsInitialized { .. }
            | AdminDocumentOperation::ConfigTransitionStarted { .. }
            | AdminDocumentOperation::TransitionBarrierReported { .. }
            | AdminDocumentOperation::TransitionProofSubmitted { .. }
            | AdminDocumentOperation::ConfigTransitionAborted { .. }
            | AdminDocumentOperation::TransitionBucketForced { .. }
            | AdminDocumentOperation::TransitionStallReported { .. }
            | AdminDocumentOperation::TransitionDrainReported { .. }
            | AdminDocumentOperation::ConfigComputeSet { .. }
            | AdminDocumentOperation::ConfigFederationSet { .. }
            | AdminDocumentOperation::ConfigPortalSet { .. }
            | AdminDocumentOperation::ConfigReplicasSet { .. }
            | AdminDocumentOperation::NodeNameSet { .. }
            | AdminDocumentOperation::NodePlaceSet { .. }
            | AdminDocumentOperation::MergeCoordinatorSet { .. }
            | AdminDocumentOperation::ConfigVaultSet { .. }
            | AdminDocumentOperation::ConfigEmailSet { .. }
            | AdminDocumentOperation::ConfigSecretSet { .. }
            | AdminDocumentOperation::ConfigPluginSet { .. }
            | AdminDocumentOperation::ConfigInstallSet { .. }
            | AdminDocumentOperation::ConfigTokenRevoked { .. } => Self::RealmConfig,
            AdminDocumentOperation::DistributionPublished { .. } => Self::Distribution,
        }
    }
}

/// The payload-shape rules of the operation's family.
pub fn validate_shape(
    family: AdminOperationFamily,
    event: &AdminDocumentEvent,
) -> Result<(), String> {
    match family {
        AdminOperationFamily::Group => validate_group_shape(event),
        AdminOperationFamily::RealmAuthorization => validate_role_assignment(event),
        AdminOperationFamily::RealmConfig => validate_config_shape(event),
        AdminOperationFamily::User => Ok(()),
        AdminOperationFamily::Distribution => validate_distribution_shape(event),
    }
}

/// A federated user acts through the reconciled realm only on group events, which the group
/// authority check still decides, to revoke its own token, and through a set's publisher.
pub fn foreign_actor_allowed(event: &AdminDocumentEvent, realm_id: RealmId) -> bool {
    event.actor.realm_id == realm_id
        && !event.actor.user_id.is_nil()
        && match &event.op {
            AdminDocumentOperation::ConfigTokenRevoked { token_owner, .. } => {
                *token_owner == event.actor.user_id
            }
            // The publisher accepted a federated admin's request; signature, realm and set
            // are still checked by the distribution shape rules.
            AdminDocumentOperation::DistributionPublished { snapshot } => {
                matches!(event.target, AdminDocumentTarget::Distribution { .. })
                    && snapshot.snapshot.root.publisher == event.origin_node_id
            }
            _ => matches!(event.target, AdminDocumentTarget::Group { .. }),
        }
}

/// Whether the dispatched family agrees with both the sync target address and
/// the payload's declared target.
pub fn family_matches_targets(
    family: AdminOperationFamily,
    target: &DocumentTarget,
    event_target: &AdminDocumentTarget,
) -> bool {
    matches!(
        (family, target, event_target),
        (
            AdminOperationFamily::Group,
            DocumentTarget::GroupAuthorization { group_id },
            AdminDocumentTarget::Group { group_id: event_group_id }
        ) if group_id == event_group_id
    ) || matches!(
        (family, target, event_target),
        (
            AdminOperationFamily::RealmAuthorization,
            DocumentTarget::RealmAuthorization { realm_id },
            AdminDocumentTarget::Realm { realm_id: event_realm_id }
        ) if realm_id == event_realm_id
    ) || matches!(
        (family, target, event_target),
        (
            AdminOperationFamily::User,
            DocumentTarget::User { user_id },
            AdminDocumentTarget::User { user_id: event_user_id }
        ) if user_id == event_user_id
    ) || matches!(
        (family, target, event_target),
        (
            AdminOperationFamily::RealmConfig,
            DocumentTarget::RealmConfig { realm_id },
            AdminDocumentTarget::RealmConfig { realm_id: event_realm_id }
        ) if realm_id == event_realm_id
    ) || matches!(
        (family, target, event_target),
        (
            AdminOperationFamily::Distribution,
            DocumentTarget::RealmConfig { realm_id },
            AdminDocumentTarget::Distribution { realm_id: event_realm_id, .. }
        ) if realm_id == event_realm_id
    )
}

/// Whether a payload target realm and any realm-scoped placement binding agree
/// with the event actor's realm.
pub fn validate_event_scope(event: &AdminDocumentEvent) -> Result<(), String> {
    let target_realm = match &event.target {
        AdminDocumentTarget::Realm { realm_id }
        | AdminDocumentTarget::RealmConfig { realm_id }
        | AdminDocumentTarget::Distribution { realm_id, .. } => Some(*realm_id),
        AdminDocumentTarget::User { user_id } => Some(user_id.realm_id),
        AdminDocumentTarget::Group { .. } => None,
    };
    if target_realm.is_some_and(|realm_id| realm_id != event.actor.realm_id) {
        return Err("admin event target and actor realms do not match".to_string());
    }
    if matches!(
        &event.op,
        AdminDocumentOperation::PlacementBindingAppended { binding }
            if matches!(
                binding.scope,
                crate::structs::placement::record::PlacementScope::Realm(binding_realm_id)
                    if binding_realm_id != event.actor.realm_id
            )
    ) {
        return Err("placement binding realm does not match the admin event target".to_string());
    }
    Ok(())
}

/// Whether a role assignment names a user in the event actor's realm. Group roles may also
/// name a federated user of another realm; realm roles stay local.
fn validate_role_assignment(event: &AdminDocumentEvent) -> Result<(), String> {
    match &event.op {
        AdminDocumentOperation::GroupAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::GroupAssignmentRemoved { user_id, .. }
            if user_id.realm_id != event.actor.realm_id && user_id.is_nil() =>
        {
            Err("role assignment names another realm's public principal".to_string())
        }
        AdminDocumentOperation::RealmAssignmentAdded { user_id, .. }
        | AdminDocumentOperation::RealmAssignmentRemoved { user_id, .. }
            if user_id.realm_id != event.actor.realm_id =>
        {
            Err("role assignment user belongs to a different realm".to_string())
        }
        _ => Ok(()),
    }
}

/// Payload-shape rules for the group family: creation identity, role subtree
/// confinement, assignment realms, and policy well-formedness.
fn validate_group_shape(event: &AdminDocumentEvent) -> Result<(), String> {
    validate_role_assignment(event)?;
    match &event.op {
        AdminDocumentOperation::GroupDeleted { certificate } => {
            let AdminDocumentTarget::Group { group_id } = event.target else {
                return Err("deletion target must be a group".into());
            };
            if certificate.plan.group_id != group_id
                || certificate.plan.realm_id != event.actor.realm_id
                || certificate.plan.owner.realm_id != event.actor.realm_id
                || certificate.plan.coordinator != event.origin_node_id
                || certificate.plan.requested_by != event.actor.user_id
                || certificate.plan.request_id.is_nil()
                || !certificate.plan.nodes.contains(&event.origin_node_id)
                || !certificate.verify()
            {
                return Err("group deletion certificate is invalid".into());
            }
        }
        AdminDocumentOperation::GroupCreated {
            realm_id, owner, ..
        } => {
            if *realm_id != event.actor.realm_id
                || owner.realm_id != *realm_id
                || *owner != event.actor.user_id
                || owner.is_nil()
            {
                return Err("group creation realm and owner must match the actor".to_string());
            }
        }
        AdminDocumentOperation::GroupRoleCreated { role } => {
            // Distributed events must enforce the same subtree confinement as
            // local issuance; the publisher is not trusted to have done so.
            let AdminDocumentTarget::Group { group_id } = &event.target else {
                return Err("group role event target must be a group".to_string());
            };
            let subtree_root =
                crate::permission_path::role_subtree_root(event.actor.realm_id, group_id);
            if role
                .permissions
                .keys()
                .any(|pattern| !crate::permission_path::role_path_confined(pattern, &subtree_root))
            {
                return Err("group role grants outside its group subtree".to_string());
            }
        }
        AdminDocumentOperation::GroupPoliciesSet { policies } => {
            if let Err(error) = crate::request_policy::validate_policy_set(policies) {
                return Err(format!("invalid policy set: {error}"));
            }
        }
        _ => {}
    }
    Ok(())
}

/// Payload-shape rules for the realm-config family: compute, candidate maps,
/// transitions, grants, labels, strategies, policies, and revocations.
fn validate_config_shape(event: &AdminDocumentEvent) -> Result<(), String> {
    match &event.op {
        AdminDocumentOperation::ConfigComputeSet { compute } => {
            // A malformed link or quota set would make every planner estimate
            // meaningless, so it is refused before it reaches storage.
            if compute.validate().is_err() {
                return Err("realm compute configuration is malformed".to_string());
            }
        }
        AdminDocumentOperation::ConfigFederationSet { settings } => {
            // Only a holder of the realm key or its delegate can publish a descriptor.
            if let AdminDocumentTarget::RealmConfig { realm_id } = &event.target
                && let Err(error) = settings.validate(realm_id)
            {
                return Err(format!("realm federation settings rejected: {error}"));
            }
        }
        AdminDocumentOperation::ConfigPortalSet { features } => {
            if let Err(error) = crate::structs::identity::realm::validate_portal_features(features)
            {
                return Err(format!("realm portal features rejected: {error}"));
            }
        }
        AdminDocumentOperation::NodeNameSet {
            display_name: Some(name),
            ..
        } => {
            let normalized = crate::structs::identity::realm::normalize_node_name(name);
            if normalized.as_ref() != Some(name) {
                return Err("node display name is not a valid name".to_string());
            }
        }
        AdminDocumentOperation::NodePlaceSet {
            place: Some(place), ..
        } => {
            if !place.valid() {
                return Err("node place is empty or not valid".to_string());
            }
        }
        // The purpose tag is checked when the event decodes; management origin in the authority.
        AdminDocumentOperation::ConfigVaultSet {
            entry,
            record: Some(record),
        } => {
            if record.entry != *entry || record.sealed.is_empty() {
                return Err("realm vault record is malformed".to_string());
            }
        }
        AdminDocumentOperation::ConfigEmailSet {
            profile: Some(profile),
        } => {
            if !profile.valid() {
                return Err("realm email profile is malformed".to_string());
            }
        }
        AdminDocumentOperation::ConfigSecretSet {
            name,
            info: Some(info),
        } => {
            let scope = crate::secrets::SecretScope::Realm;
            if info.name != *name || info.check(scope).is_err() {
                return Err("realm secret record is malformed".to_string());
            }
        }
        // The artifact was checked on the publishing management node; origin is in the authority.
        AdminDocumentOperation::ConfigPluginSet {
            name,
            version,
            plugin,
        } => {
            let matching = plugin.as_ref().is_none_or(|plugin| {
                plugin.valid() && plugin.name == *name && plugin.version == *version
            });
            if !matching || !crate::plugins::valid_name(name) {
                return Err("published plugin entry is malformed".to_string());
            }
        }
        // The writing management node checked the admin, the grants and the published version.
        AdminDocumentOperation::ConfigInstallSet {
            install_id,
            install,
        } => {
            let matching = install.as_ref().is_none_or(|install| {
                install.valid()
                    && install.install_id == *install_id
                    && install.account.realm_id == event.actor.realm_id
            });
            if !matching || install_id.is_nil() {
                return Err("plugin install record is malformed".to_string());
            }
        }
        AdminDocumentOperation::CandidateMapPublished { map } => {
            let mut seen = std::collections::BTreeSet::new();
            if map.epoch == 0 || !map.nodes.iter().all(|node| seen.insert(node.node_id)) {
                return Err("candidate placement map is malformed".to_string());
            }
        }
        AdminDocumentOperation::ConfigActivationsInitialized {
            candidate_map_epoch,
            ..
        } => {
            if *candidate_map_epoch == 0 {
                return Err("activation names no candidate map epoch".to_string());
            }
        }
        AdminDocumentOperation::ConfigTransitionStarted { plan } => {
            let mut seen = std::collections::BTreeSet::new();
            let well_formed = plan.limits.max_incomplete_buckets >= 1
                && plan.target_map_epoch > 0
                && !plan.buckets.is_empty()
                && plan
                    .buckets
                    .iter()
                    .all(|bucket| seen.insert(bucket.bucket) && !bucket.target_holders.is_empty());
            if !well_formed {
                return Err("placement transition plan is malformed".to_string());
            }
        }
        AdminDocumentOperation::TransitionBarrierReported {
            reported_by,
            frontier,
            ..
        } => {
            if *reported_by != event.origin_node_id {
                return Err("transition report does not come from the node it names".to_string());
            }
            if frontier.len() > crate::structs::placement::transition::MAX_FRONTIER_BYTES {
                return Err("transition barrier frontier exceeds its size bound".to_string());
            }
        }
        AdminDocumentOperation::TransitionStallReported {
            reported_by,
            reason,
            ..
        } => {
            if *reported_by != event.origin_node_id {
                return Err("transition report does not come from the node it names".to_string());
            }
            if reason.len() > crate::structs::placement::transition::MAX_STALL_BYTES {
                return Err("transition stall reason exceeds its size bound".to_string());
            }
        }
        AdminDocumentOperation::TransitionDrainReported { reported_by, .. } => {
            if *reported_by != event.origin_node_id {
                return Err("transition report does not come from the node it names".to_string());
            }
        }
        AdminDocumentOperation::TransitionProofSubmitted {
            transition_id,
            strategy_id,
            proof,
        } => {
            // Verified here as well as in the reducer: a forged proof must never
            // reach storage, and the publisher binding already fixed the origin.
            if proof.holder != event.origin_node_id
                || !proof.verify(event.actor.realm_id, *transition_id, *strategy_id)
            {
                return Err("transition completion proof does not verify".to_string());
            }
        }
        AdminDocumentOperation::NodePlacementSet { entry } => {
            if let Some(label) = reserved_label(&entry.labels) {
                return Err(format!(
                    "placement entry must not set derived label {label}"
                ));
            }
        }
        AdminDocumentOperation::PlacementStrategyUpserted { strategy }
            if strategy.replica_count == Some(0) =>
        {
            return Err("placement strategy replica count must be greater than zero".to_string());
        }
        AdminDocumentOperation::ConfigPoliciesSet { policies } => {
            if let Err(error) = crate::request_policy::validate_policy_set(policies) {
                return Err(format!("invalid policy set: {error}"));
            }
        }
        AdminDocumentOperation::ConfigTokenRevoked {
            token_hash,
            expires_at,
            token_owner,
            ..
        } => {
            if !crate::auth::valid_token_hash(token_hash) {
                return Err("revoked bearer token hash is malformed".to_string());
            }
            if !valid_revocation_expiry(*expires_at, unix_timestamp_secs()) {
                return Err("revoked bearer token expiry exceeds the admission window".to_string());
            }
            // A token this realm issued may belong to a federated user of another realm.
            if token_owner.is_nil() {
                return Err("revoked bearer token owner is malformed".to_string());
            }
        }
        _ => {}
    }
    Ok(())
}

/// Content rules that need no stored state: the target's set, the publisher as origin, the
/// publisher's signature and every embedded statement. A failure is invalid input.
fn validate_distribution_shape(event: &AdminDocumentEvent) -> Result<(), String> {
    let AdminDocumentTarget::Distribution { realm_id, set_id } = &event.target else {
        return Err("distribution snapshot must target a distribution record".to_string());
    };
    let AdminDocumentOperation::DistributionPublished { snapshot } = &event.op else {
        return Err("distribution records change only through published snapshots".to_string());
    };
    if snapshot.snapshot.set_id() != *set_id {
        return Err("distribution snapshot names another set".to_string());
    }
    if event.origin_node_id != snapshot.snapshot.root.publisher {
        return Err("distribution snapshot does not come from the set's publisher".to_string());
    }
    snapshot
        .verify(*realm_id)
        .map_err(|error| format!("distribution snapshot is invalid: {error}"))
}

/// Whether this origin already holds the per-origin bound a local mint obeys.
/// Replacing its own entry stays allowed; the flooding origin is rejected rather
/// than trimmed, so a valid revocation is never discarded to make room.
pub fn revocation_origin_full(
    state: Option<&AdminDocumentState>,
    event: &AdminDocumentEvent,
    token_hash: &str,
) -> bool {
    let Some(state) = state else {
        return false;
    };
    let index = state.revocation_index(state.revocation_floor.max(unix_timestamp_secs()));
    index.origin(token_hash) != Some(event.origin_node_id)
        && index.count(&event.origin_node_id) >= REVOCATIONS_PER_ORIGIN
}

pub fn revocation_origin_known(
    config: Option<&RealmConfigDocument>,
    state: Option<&AdminDocumentState>,
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

/// Publisher capability by node kind: a User node never originates a realm
/// administrative event, whichever node relayed it.
pub fn origin_may_publish(config: &RealmConfigDocument, origin_node_id: &NodeId) -> bool {
    config
        .node_kind(origin_node_id)
        .is_some_and(RealmNodeKind::is_sync_eligible)
}

/// The group whose administrators may apply this event to a service account: the owner named by
/// the event creating a new account, else the stored owner for name and status changes.
pub fn service_owner(
    event: &AdminDocumentEvent,
    previous_state: Option<&AdminDocumentState>,
    current_user: Option<&User>,
) -> Option<GroupId> {
    let attributes = previous_state.map(AdminDocumentState::materialized_user_attributes);
    let stored = current_user.and_then(User::service_group).or_else(|| {
        attributes
            .as_ref()
            .and_then(|attributes| attributes.get(SERVICE_GROUP_ATTRIBUTE))
            .and_then(|group| group.parse().ok())
    });
    match &event.op {
        AdminDocumentOperation::UserAttributeSet { key, value }
            if key == SERVICE_GROUP_ATTRIBUTE =>
        {
            let fresh =
                current_user.is_none() && attributes.is_none_or(|attributes| attributes.is_empty());
            fresh.then(|| value.parse().ok()).flatten()
        }
        // The install marker is set once, right after the owner, and never changed.
        AdminDocumentOperation::UserAttributeSet { key, .. } if key == INSTALL_ATTRIBUTE => {
            let marked = current_user.is_some_and(|user| user.install().is_some())
                || (attributes.as_ref()).is_some_and(|map| map.contains_key(INSTALL_ATTRIBUTE));
            stored.filter(|_| !marked)
        }
        AdminDocumentOperation::UserAttributeSet { key, .. }
        | AdminDocumentOperation::UserAttributeRemoved { key }
            if key == DEACTIVATED_ATTRIBUTE =>
        {
            stored
        }
        AdminDocumentOperation::UserNameSet { .. } => stored,
        _ => None,
    }
}

pub fn has_write_permission<'a>(
    user_id: UserId,
    path: &str,
    roles: impl IntoIterator<Item = &'a Role>,
) -> bool {
    !user_id.is_nil()
        && crate::structs::placement::policy::document::holds_admin_write(
            user_id,
            path,
            roles.into_iter(),
        )
}

/// Realm-config ops the reducer stores as order-insensitive immutable values
/// and whose validation no other such op can influence: a consecutive run of
/// them may apply as one read-reduce-write cycle instead of one per event.
pub fn coalescible_config_op(op: &AdminDocumentOperation) -> bool {
    matches!(
        op,
        AdminDocumentOperation::CandidateMapPublished { .. }
            | AdminDocumentOperation::ConfigActivationsInitialized { .. }
            | AdminDocumentOperation::ConfigTransitionStarted { .. }
            | AdminDocumentOperation::TransitionBarrierReported { .. }
            | AdminDocumentOperation::TransitionProofSubmitted { .. }
            | AdminDocumentOperation::ConfigTransitionAborted { .. }
            | AdminDocumentOperation::TransitionBucketForced { .. }
            | AdminDocumentOperation::TransitionStallReported { .. }
            | AdminDocumentOperation::TransitionDrainReported { .. }
    )
}

/// Realm-config operations this document applier owns; every other family has
/// its own applier.
pub fn config_mutation_allowed(op: &AdminDocumentOperation) -> bool {
    matches!(
        op,
        AdminDocumentOperation::ConfigNodeEnsured { .. }
            | AdminDocumentOperation::ConfigNodeRemoved { .. }
            | AdminDocumentOperation::OidcProviderUpserted { .. }
            | AdminDocumentOperation::OidcProviderRemoved { .. }
            | AdminDocumentOperation::ConfigSettingsSet { .. }
            | AdminDocumentOperation::ConfigDescriptionSet { .. }
            | AdminDocumentOperation::ConfigQuotaSet { .. }
            | AdminDocumentOperation::NodePlacementSet { .. }
            | AdminDocumentOperation::NodePlacementRemoved { .. }
            | AdminDocumentOperation::PlacementStrategyUpserted { .. }
            | AdminDocumentOperation::PlacementStrategyRemoved { .. }
            | AdminDocumentOperation::ConfigStrategySet { .. }
            | AdminDocumentOperation::JobFamilySet { .. }
            | AdminDocumentOperation::StrategyBindingSet { .. }
            | AdminDocumentOperation::StrategyBindingRemoved { .. }
            | AdminDocumentOperation::PlacementOverrideSet { .. }
            | AdminDocumentOperation::PlacementOverrideRemoved { .. }
            | AdminDocumentOperation::PlacementBindingAppended { .. }
            | AdminDocumentOperation::HandleRangeGranted { .. }
            | AdminDocumentOperation::BandPoolAssigned { .. }
            | AdminDocumentOperation::ConfigPoliciesSet { .. }
            | AdminDocumentOperation::CandidateMapPublished { .. }
            | AdminDocumentOperation::ConfigActivationsInitialized { .. }
            | AdminDocumentOperation::ConfigTransitionStarted { .. }
            | AdminDocumentOperation::TransitionBarrierReported { .. }
            | AdminDocumentOperation::TransitionProofSubmitted { .. }
            | AdminDocumentOperation::ConfigTransitionAborted { .. }
            | AdminDocumentOperation::TransitionBucketForced { .. }
            | AdminDocumentOperation::TransitionStallReported { .. }
            | AdminDocumentOperation::TransitionDrainReported { .. }
            | AdminDocumentOperation::ConfigComputeSet { .. }
            | AdminDocumentOperation::ConfigFederationSet { .. }
            | AdminDocumentOperation::ConfigPortalSet { .. }
            | AdminDocumentOperation::ConfigReplicasSet { .. }
            | AdminDocumentOperation::NodeNameSet { .. }
            | AdminDocumentOperation::NodePlaceSet { .. }
            | AdminDocumentOperation::ConfigTokenRevoked { .. }
            | AdminDocumentOperation::MergeCoordinatorSet { .. }
            | AdminDocumentOperation::ConfigVaultSet { .. }
            | AdminDocumentOperation::ConfigEmailSet { .. }
            | AdminDocumentOperation::ConfigSecretSet { .. }
            | AdminDocumentOperation::ConfigPluginSet { .. }
            | AdminDocumentOperation::ConfigInstallSet { .. }
    )
}

/// The realm a realm-config operation addresses: the sync target, payload
/// target, and document must all name the same realm.
pub fn validate_config_target(
    document_target: &DocumentTarget,
    event: &AdminDocumentEvent,
) -> Result<RealmId, String> {
    let DocumentTarget::RealmConfig { realm_id } = document_target else {
        return Err(
            "realm config admin operation sync only supports realm config targets".to_string(),
        );
    };
    let AdminDocumentTarget::RealmConfig {
        realm_id: event_realm_id,
    } = &event.target
    else {
        return Err("admin document operation payload target is not a realm config".to_string());
    };
    if event_realm_id != realm_id {
        return Err(format!(
            "replicated realm config admin operation target {realm_id} does not match payload realm id {event_realm_id}"
        ));
    }
    Ok(*realm_id)
}

/// Whether a realm-config event's origin node and actor belong to the target
/// realm.
pub fn validate_config_actor(realm_id: RealmId, event: &AdminDocumentEvent) -> Result<(), String> {
    // A federated user may only revoke its own token, which this realm issued.
    let own_revocation = matches!(
        &event.op,
        AdminDocumentOperation::ConfigTokenRevoked { token_owner, .. }
            if *token_owner == event.actor.user_id && !token_owner.is_nil()
    );
    if event.origin_node_id != event.actor.node_id
        || event.actor.realm_id != realm_id
        || (event.actor.user_id.realm_id != realm_id && !own_revocation)
    {
        return Err(
            "realm config event actor and origin do not match the target realm".to_string(),
        );
    }
    Ok(())
}

/// Re-checks a revocation against the transaction snapshot: the origin must
/// already be onboarded and the payload valid under the snapshot clock.
pub fn validate_revocation_snapshot(
    previous_config: Option<&RealmConfigDocument>,
    previous_state: Option<&AdminDocumentState>,
    event: &AdminDocumentEvent,
    realm_id: RealmId,
    raw_now: u64,
) -> Result<(), String> {
    let valid = revocation_origin_known(previous_config, previous_state, event, realm_id);
    if !valid {
        return Err(
            "revocation origin is not an onboarded realm node in the transaction snapshot"
                .to_string(),
        );
    }
    if let AdminDocumentOperation::ConfigTokenRevoked {
        token_hash,
        expires_at,
        token_owner,
    } = &event.op
        && (!crate::auth::valid_token_hash(token_hash)
            || !valid_revocation_expiry(*expires_at, raw_now)
            || token_owner.is_nil())
    {
        return Err("replicated revocation has invalid hash, expiry, or owner".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::auth::Actor;
    use ulid::Ulid;

    #[test]
    fn marker_set_once() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group = Ulid::from_parts(1, 9);
        let user_id = UserId::local(Ulid::from_parts(1, 7), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            user_id: UserId::local(Ulid::from_parts(1, 3), realm_id),
            realm_id,
        };
        let set = |key: &str, value: String| AdminDocumentOperation::UserAttributeSet {
            key: key.to_string(),
            value,
        };
        let mut state = AdminDocumentState::new(AdminDocumentTarget::User { user_id });
        let owner = state.apply_operation(&actor, set(SERVICE_GROUP_ATTRIBUTE, group.to_string()));
        assert_eq!(service_owner(&owner.unwrap(), None, None), Some(group));
        let owned = state.clone();
        let marker = state.apply_operation(&actor, set(INSTALL_ATTRIBUTE, "a".into()));
        assert_eq!(
            service_owner(&marker.unwrap(), Some(&owned), None),
            Some(group)
        );
        let marked = state.clone();
        let again = state.apply_operation(&actor, set(INSTALL_ATTRIBUTE, "b".into()));
        assert_eq!(service_owner(&again.unwrap(), Some(&marked), None), None);
    }
}
