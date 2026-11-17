//! Checks the shape of replicated admin events by family, before any authority is consulted.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::admin_documents::{AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget};
use crate::auth::valid_revocation_expiry;
use crate::document::DocumentTarget;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::node_info::reserved_label;
use crate::time::unix_timestamp_secs;

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
