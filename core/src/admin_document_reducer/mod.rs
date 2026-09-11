use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Included, Unbounded};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::admin_documents::{
    AdminDocumentClock, AdminDocumentDot, AdminDocumentEvent, AdminDocumentOperation,
    AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use crate::auth::{REVOCATION_GRACE_SECS, revocation_live, revocation_retained, valid_token_hash};
use crate::structs::{
    Actor, BandPool, BindingScope, BucketBarrier, BucketCompletion, BucketForceFinalize,
    CandidatePlacementMap, CompletionProof, DocumentClass, HandleRange, MAX_PLACEMENT_SHARD_COUNT,
    MetadataRegistryRecord, MetadataReplicationConfig, NodePlacementEntry, OidcProviderConfig,
    PlacementActivation, PlacementBinding, PlacementOverride, PlacementStrategy,
    PlacementTransition, QuotaConfig, RealmComputeConfig, RealmConfigDocument,
    RealmDiscoveryConfig, RealmId, RealmNodeKind, StallReport, StrategyBinding, TransitionPlan,
    TransitionStatus, reserved_label,
};
use crate::structured_id::PlacementHandle;
use crate::types::{RoleId, UserId};
use crate::user_update_validation::{
    UserAttributeValidationError, validate_user_attribute_key, validate_user_attribute_value,
};

mod paths;
mod placement;
mod revocation;
mod targets;

pub use paths::*;
use paths::{event_observes_dot, operation_paths, role_definition_value};
pub use placement::*;
use placement::{candidate_map_value, transition_plan_value, transition_proof_value};
pub use revocation::{MAX_LIVE_REVOCATIONS_PER_ORIGIN, revoked_token_entry, revoked_token_path};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminDocumentApplyStatus {
    Applied,
    Duplicate,
    Redundant,
    StaleOriginSequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdminDocumentReducerError {
    #[error("admin document event target does not match reducer state")]
    TargetMismatch,
    #[error("admin document event operation is not supported for target")]
    UnsupportedTarget,
    #[error("invalid group join request or decision")]
    InvalidJoinRequest,
    #[error(transparent)]
    InvalidUserAttribute(#[from] UserAttributeValidationError),
    #[error("placement labels must not set the derived label `{0}`")]
    ReservedPlacementLabel(String),
    #[error("placement strategy replica count must not be zero")]
    ZeroPlacementReplicaCount,
    #[error(
        "placement strategy shard count must be a non-zero power of two no greater than {}",
        MAX_PLACEMENT_SHARD_COUNT
    )]
    InvalidPlacementShardCount,
    #[error("placement strategy shard count cannot be changed")]
    PlacementShardCountChanged,
    #[error("placement handle range is malformed")]
    InvalidHandleRange,
    #[error("revoked bearer token hash is malformed")]
    InvalidTokenHash,
    #[error("candidate placement map is malformed")]
    InvalidCandidateMap,
    #[error("placement transition plan is malformed")]
    InvalidTransitionPlan,
    #[error("placement transition proof does not verify")]
    InvalidTransitionProof,
    #[error("placement transition report does not come from the node it names")]
    TransitionOriginMismatch,
    #[error("placement transition report exceeds its size bound")]
    TransitionReportOversized,
    #[error("job family placement strategy must not be nil")]
    NilJobFamily,
    #[error("job family placement strategy cannot be changed")]
    JobFamilyChanged,
    #[error("job family placement strategy cannot be removed")]
    JobFamilyRemoved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentAttributeVersion {
    pub value: Option<String>,
    pub dot: AdminDocumentDot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentConflictValue {
    pub value: Option<String>,
    pub dot: AdminDocumentDot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentConflict {
    pub path: String,
    pub values: Vec<AdminDocumentConflictValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentReducerState {
    pub target: AdminDocumentTarget,
    pub clock: AdminDocumentClock,
    pub applied_event_ids: BTreeSet<Ulid>,
    pub user_attributes: BTreeMap<String, AdminDocumentAttributeVersion>,
    pub conflicts: BTreeMap<String, AdminDocumentConflict>,
    pub user_name: Option<AdminDocumentAttributeVersion>,
    pub user_subject_ids: BTreeMap<String, AdminDocumentAttributeVersion>,
    pub equivalent_value_dots: BTreeMap<String, BTreeSet<AdminDocumentDot>>,
    pub revocation_floor: u64,
    pub revocation_next_expiry: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RevocationPath {
    hash: String,
    expires_at: u64,
    token_owner: UserId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RevocationCandidate {
    path: String,
    expires_at: u64,
    token_owner: UserId,
    dot: AdminDocumentDot,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RevocationGroup {
    paths: BTreeSet<String>,
    candidates: Vec<RevocationCandidate>,
    event_ids: BTreeSet<Ulid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevocationIndex {
    now: u64,
    groups: BTreeMap<String, RevocationGroup>,
    retained: BTreeMap<String, RevocationCandidate>,
    live: BTreeMap<String, RevocationCandidate>,
    origin_counts: BTreeMap<NodeId, usize>,
    owner_counts: BTreeMap<(NodeId, UserId), usize>,
    next_expiry: Option<u64>,
}

pub fn decode_admin_document_reducer_state(
    bytes: &[u8],
) -> Result<AdminDocumentReducerState, postcard::Error> {
    postcard::from_bytes(bytes)
}

impl AdminDocumentReducerState {
    pub fn new(target: AdminDocumentTarget) -> Self {
        Self {
            target,
            clock: AdminDocumentClock::default(),
            applied_event_ids: BTreeSet::new(),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::new(),
            user_name: None,
            user_subject_ids: BTreeMap::new(),
            equivalent_value_dots: BTreeMap::new(),
            revocation_floor: 0,
            revocation_next_expiry: None,
        }
    }

    pub fn apply_operation(
        &mut self,
        actor: &Actor,
        op: AdminDocumentOperation,
    ) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
        let observed = self.clock.clone();
        let event = AdminDocumentEvent {
            event_id: Ulid::generate(),
            target: self.target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: observed.sequence_for(&actor.node_id) + 1,
            observed,
            actor: actor.clone(),
            op,
        };
        self.apply(&event)?;
        Ok(event)
    }

    pub fn apply_revocation_operation(
        &mut self,
        actor: &Actor,
        op: AdminDocumentOperation,
        index: &mut RevocationIndex,
    ) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
        let observed = self.clock.clone();
        let event = AdminDocumentEvent {
            event_id: Ulid::generate(),
            target: self.target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: observed.sequence_for(&actor.node_id) + 1,
            observed,
            actor: actor.clone(),
            op,
        };
        self.apply_revocation_event(&event, index)?;
        Ok(event)
    }

    pub fn apply_revocation_event(
        &mut self,
        event: &AdminDocumentEvent,
        index: &mut RevocationIndex,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        if event.target != self.target {
            return Err(AdminDocumentReducerError::TargetMismatch);
        }
        if self.applied_event_ids.contains(&event.event_id) {
            return Ok(AdminDocumentApplyStatus::Duplicate);
        }
        let AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash,
            expires_at,
            token_owner,
        } = &event.op
        else {
            return Err(AdminDocumentReducerError::UnsupportedTarget);
        };
        if !matches!(&event.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Err(AdminDocumentReducerError::UnsupportedTarget);
        }
        if !valid_token_hash(token_hash) {
            return Err(AdminDocumentReducerError::InvalidTokenHash);
        }
        Ok(index.apply(self, event, token_hash, *expires_at, *token_owner))
    }

    pub fn apply(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        if event.target != self.target {
            return Err(AdminDocumentReducerError::TargetMismatch);
        }
        // Dropped by event id alone: a second event reusing an applied id never
        // overwrites the first, so an equivocating origin gains nothing here.
        // Keeping the evidence is admission's job, not the reducer's.
        if self.applied_event_ids.contains(&event.event_id) {
            return Ok(AdminDocumentApplyStatus::Duplicate);
        }
        let stale_on_all_paths = !matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
                | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
                | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
                | AdminDocumentOperation::RealmConfigTokenRevoked { .. }
                | AdminDocumentOperation::RealmConfigCandidateMapPublished { .. }
                | AdminDocumentOperation::RealmConfigActivationsInitialized { .. }
                | AdminDocumentOperation::RealmConfigTransitionStarted { .. }
                | AdminDocumentOperation::RealmConfigTransitionBarrierReported { .. }
                | AdminDocumentOperation::RealmConfigTransitionProofSubmitted { .. }
                | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
                | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. }
                | AdminDocumentOperation::RealmConfigTransitionStallReported { .. }
                | AdminDocumentOperation::RealmConfigTransitionDrainReported { .. }
        ) && operation_paths(&event.op)
            .iter()
            .all(|path| self.event_is_stale_for_path(event, path));
        let mut apply_status = AdminDocumentApplyStatus::Applied;

        match (&event.target, &event.op) {
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name,
                    owner,
                },
            ) => {
                self.apply_group_created(event, realm_id, display_name, owner);
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleAdded { role_id },
            ) => {
                self.apply_group_role(event, role_id, role_id.to_string());
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleCreated { role },
            ) => {
                self.apply_group_role(event, &role.role_id, role_definition_value(role));
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleRemoved { role_id },
            ) => {
                self.apply_group_role_removed(event, role_id);
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id },
            ) => {
                self.apply_group_role_user_assignment(
                    event,
                    role_id,
                    user_id,
                    Some(user_id.to_string()),
                );
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id },
            ) => {
                self.apply_group_role_user_assignment(event, role_id, user_id, None);
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupDisplayNameSet { display_name },
            ) => {
                self.apply_group_field(
                    event,
                    GROUP_DISPLAY_NAME_PATH,
                    Some(display_name.to_string()),
                );
            }
            (
                AdminDocumentTarget::Group { group_id },
                AdminDocumentOperation::GroupJoinRequested { request },
            ) => {
                if request.group_id != *group_id
                    || request.request_id.is_nil()
                    || request.user_id != event.actor.user_id
                    || request.user_id.is_nil()
                    || request.user_id.realm_id != event.actor.realm_id
                    || !crate::join_request::valid_message(&request.message)
                {
                    return Err(AdminDocumentReducerError::InvalidJoinRequest);
                }
                let value = serde_json::to_string(request)
                    .map_err(|_| AdminDocumentReducerError::InvalidJoinRequest)?;
                self.apply_group_field(
                    event,
                    &crate::join_request::request_path(request.request_id),
                    Some(value),
                );
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupJoinDecided { decision },
            ) => {
                use crate::join_request::JoinDecisionKind;
                if decision.request_id.is_nil()
                    || decision.user_id.is_nil()
                    || decision.user_id.realm_id != event.actor.realm_id
                    || decision.decided_by != event.actor.user_id
                    || !crate::join_request::valid_message(&decision.reason)
                    || decision.role_ids.iter().any(Ulid::is_nil)
                    || match decision.kind {
                        JoinDecisionKind::Approved => decision.role_ids.is_empty(),
                        JoinDecisionKind::Denied | JoinDecisionKind::Withdrawn => {
                            !decision.role_ids.is_empty()
                        }
                    }
                    || (decision.kind == JoinDecisionKind::Withdrawn
                        && decision.user_id != event.actor.user_id)
                {
                    return Err(AdminDocumentReducerError::InvalidJoinRequest);
                }
                let value = serde_json::to_string(decision)
                    .map_err(|_| AdminDocumentReducerError::InvalidJoinRequest)?;
                self.apply_group_field(
                    event,
                    &crate::join_request::decision_path(decision.request_id),
                    Some(value),
                );
                for role_id in &decision.role_ids {
                    self.apply_group_role_user_assignment(
                        event,
                        role_id,
                        &decision.user_id,
                        Some(decision.user_id.to_string()),
                    );
                }
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupPoliciesSet { policies },
            ) => {
                self.apply_group_field(event, GROUP_POLICIES_PATH, Some(policies_value(policies)));
            }
            (
                AdminDocumentTarget::Realm { .. },
                AdminDocumentOperation::RealmRoleAdded { role_id },
            ) => {
                self.apply_realm_role(event, role_id, role_id.to_string());
            }
            (
                AdminDocumentTarget::Realm { .. },
                AdminDocumentOperation::RealmRoleCreated { role },
            ) => {
                self.apply_realm_role(event, &role.role_id, role_definition_value(role));
            }
            (
                AdminDocumentTarget::Realm { .. },
                AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id },
            ) => {
                self.apply_realm_role_user_assignment(
                    event,
                    role_id,
                    user_id,
                    Some(user_id.to_string()),
                );
            }
            (
                AdminDocumentTarget::Realm { .. },
                AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id },
            ) => {
                self.apply_realm_role_user_assignment(event, role_id, user_id, None);
            }
            (AdminDocumentTarget::User { .. }, AdminDocumentOperation::UserNameSet { name }) => {
                self.apply_user_name(event, name);
            }
            (
                AdminDocumentTarget::User { .. },
                AdminDocumentOperation::UserSubjectIdAdded { subject_id },
            ) => {
                self.apply_user_subject_id(event, subject_id, Some(subject_id.clone()));
            }
            (
                AdminDocumentTarget::User { .. },
                AdminDocumentOperation::UserSubjectIdRemoved { subject_id },
            ) => {
                self.apply_user_subject_id(event, subject_id, None);
            }
            (
                AdminDocumentTarget::User { .. },
                AdminDocumentOperation::UserAttributeSet { key, value },
            ) => {
                validate_user_attribute_key(key)?;
                validate_user_attribute_value(key, value)?;
                self.apply_user_attribute(event, key, Some(value.clone()));
            }
            (
                AdminDocumentTarget::User { .. },
                AdminDocumentOperation::UserAttributeRemoved { key },
            ) => {
                validate_user_attribute_key(key)?;
                self.apply_user_attribute(event, key, None);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind },
            ) => {
                self.apply_realm_config_node(event, node_id, Some(realm_node_kind_value(kind)));
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigNodeRemoved { node_id },
            ) => {
                self.apply_realm_config_node(event, node_id, None);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider },
            ) => {
                self.apply_realm_config_oidc_provider(
                    event,
                    &provider.id,
                    Some(oidc_provider_value(provider)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigOidcProviderRemoved { provider_id },
            ) => {
                self.apply_realm_config_oidc_provider(event, provider_id, None);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication,
                    discovery,
                },
            ) => {
                self.apply_realm_config_settings(event, metadata_replication, discovery);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigDescriptionSet { description },
            ) => {
                self.apply_realm_config_setting(
                    event,
                    REALM_CONFIG_DESCRIPTION_PATH,
                    description.clone(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigQuotaSet { quota },
            ) => {
                self.apply_realm_config_setting(event, REALM_CONFIG_QUOTA_PATH, quota_value(quota));
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigComputeSet { compute },
            ) => {
                self.apply_realm_config_setting(
                    event,
                    REALM_CONFIG_COMPUTE_PATH,
                    compute_value(compute),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPoliciesSet { policies },
            ) => {
                self.apply_realm_config_setting(
                    event,
                    REALM_CONFIG_POLICIES_PATH,
                    policies_value(policies),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash,
                    expires_at,
                    token_owner,
                },
            ) => {
                if !valid_token_hash(token_hash) {
                    return Err(AdminDocumentReducerError::InvalidTokenHash);
                }
                apply_status =
                    self.apply_revocation_full(event, token_hash, *expires_at, *token_owner);
                self.refresh_revocation_expiry();
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigNodePlacementSet { entry },
            ) => {
                if let Some(label) = reserved_label(&entry.labels) {
                    return Err(AdminDocumentReducerError::ReservedPlacementLabel(
                        label.to_string(),
                    ));
                }
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_node_path(&entry.node_id),
                    Some(placement_entry_value(entry)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigNodePlacementRemoved { node_id },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_node_path(node_id),
                    None,
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy },
            ) => {
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
                    .materialized_realm_config_placement_strategies()
                    .get(&strategy.strategy_id)
                    .is_some_and(|current| current.shard_count != strategy.shard_count)
                {
                    return Err(AdminDocumentReducerError::PlacementShardCountChanged);
                }
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_strategy_path(&strategy.strategy_id),
                    Some(placement_strategy_value(strategy)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { strategy_id },
            ) => {
                if self.materialized_family_strategy() == Some(*strategy_id) {
                    return Err(AdminDocumentReducerError::JobFamilyRemoved);
                }
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_strategy_path(strategy_id),
                    None,
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
            ) => {
                self.apply_realm_config_setting(
                    event,
                    REALM_CONFIG_DEFAULT_STRATEGY_PATH,
                    strategy_id.to_string(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigJobFamilySet { strategy_id },
            ) => {
                if strategy_id.is_nil() {
                    return Err(AdminDocumentReducerError::NilJobFamily);
                }
                if self
                    .materialized_family_strategy()
                    .is_some_and(|current| current != *strategy_id)
                {
                    return Err(AdminDocumentReducerError::JobFamilyChanged);
                }
                self.apply_realm_config_setting(
                    event,
                    REALM_CONFIG_JOB_FAMILY_PATH,
                    strategy_id.to_string(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigStrategyBindingSet { binding },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_strategy_binding_path(&binding.scope),
                    Some(strategy_binding_value(binding)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigStrategyBindingRemoved { scope },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_strategy_binding_path(scope),
                    None,
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementOverrideSet { record },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_override_path(&record.subject),
                    Some(placement_override_value(record)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { subject },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_override_path(subject),
                    None,
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding },
            ) => {
                self.apply_placement_binding(event, binding);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigCandidateMapPublished { map },
            ) => {
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
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigActivationsInitialized {
                    strategy_id,
                    candidate_map_epoch,
                },
            ) => {
                if *candidate_map_epoch == 0 {
                    return Err(AdminDocumentReducerError::InvalidCandidateMap);
                }
                self.apply_immutable_value(
                    event,
                    activation_path(strategy_id),
                    candidate_map_epoch.to_string(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigTransitionStarted { plan },
            ) => {
                let mut seen = BTreeSet::new();
                let well_formed = plan.limits.max_incomplete_buckets >= 1
                    && plan.target_map_epoch > 0
                    && !plan.buckets.is_empty()
                    && plan.buckets.iter().all(|bucket| {
                        seen.insert(bucket.bucket) && !bucket.target_holders.is_empty()
                    });
                if !well_formed {
                    return Err(AdminDocumentReducerError::InvalidTransitionPlan);
                }
                self.apply_immutable_value(
                    event,
                    transition_path(&plan.transition_id),
                    transition_plan_value(plan),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                    transition_id,
                    bucket,
                    reported_by,
                    frontier,
                },
            ) => {
                if *reported_by != event.origin_node_id {
                    return Err(AdminDocumentReducerError::TransitionOriginMismatch);
                }
                if frontier.len() > crate::structs::MAX_BARRIER_FRONTIER_BYTES {
                    return Err(AdminDocumentReducerError::TransitionReportOversized);
                }
                self.apply_transition_report(
                    event,
                    transition_barrier_path(transition_id, *bucket, reported_by),
                    hex::encode(frontier),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { realm_id },
                AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                    transition_id,
                    strategy_id,
                    proof,
                },
            ) => {
                if proof.holder != event.origin_node_id {
                    return Err(AdminDocumentReducerError::TransitionOriginMismatch);
                }
                // The plan wins when it has replicated; otherwise the submitted
                // strategy carries the signature and materialization rechecks it
                // against the plan.
                let strategy_id = self
                    .materialized_transition_plans()
                    .get(transition_id)
                    .map(|plan| plan.strategy_id)
                    .unwrap_or(*strategy_id);
                if !proof.verify(*realm_id, *transition_id, strategy_id) {
                    return Err(AdminDocumentReducerError::InvalidTransitionProof);
                }
                self.apply_transition_report(
                    event,
                    transition_proof_path(transition_id, proof.bucket, &proof.holder),
                    transition_proof_value(&strategy_id, proof),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigTransitionAborted { transition_id },
            ) => {
                self.apply_immutable_value(
                    event,
                    transition_abort_path(transition_id),
                    true.to_string(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigTransitionBucketForced {
                    transition_id,
                    bucket,
                    at_risk_report,
                },
            ) => {
                if at_risk_report.len() > crate::structs::MAX_STALL_REASON_BYTES {
                    return Err(AdminDocumentReducerError::TransitionReportOversized);
                }
                self.apply_transition_report(
                    event,
                    transition_force_path(transition_id, *bucket),
                    at_risk_report.clone(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigTransitionStallReported {
                    transition_id,
                    bucket,
                    reported_by,
                    reason,
                },
            ) => {
                if *reported_by != event.origin_node_id {
                    return Err(AdminDocumentReducerError::TransitionOriginMismatch);
                }
                if reason.len() > crate::structs::MAX_STALL_REASON_BYTES {
                    return Err(AdminDocumentReducerError::TransitionReportOversized);
                }
                self.apply_transition_report(
                    event,
                    transition_stall_path(transition_id, *bucket, reported_by),
                    reason.clone(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigTransitionDrainReported {
                    transition_id,
                    bucket,
                    reported_by,
                },
            ) => {
                if *reported_by != event.origin_node_id {
                    return Err(AdminDocumentReducerError::TransitionOriginMismatch);
                }
                self.apply_transition_report(
                    event,
                    transition_drain_path(transition_id, *bucket, reported_by),
                    true.to_string(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigHandleRangeGranted { range },
            ) => {
                if !range.is_well_formed() {
                    return Err(AdminDocumentReducerError::InvalidHandleRange);
                }
                self.apply_handle_range(event, range);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigBandPoolAssigned { pool },
            ) => {
                if !pool.is_well_formed() {
                    return Err(AdminDocumentReducerError::InvalidHandleRange);
                }
                self.apply_band_pool(event, pool);
            }
            _ => return Err(AdminDocumentReducerError::UnsupportedTarget),
        }

        if stale_on_all_paths {
            self.applied_event_ids.insert(event.event_id);
            self.clock.advance(event.origin_node_id, event.origin_seq);
            return Ok(AdminDocumentApplyStatus::StaleOriginSequence);
        }

        self.clock.advance(event.origin_node_id, event.origin_seq);
        if apply_status != AdminDocumentApplyStatus::Redundant {
            self.applied_event_ids.insert(event.event_id);
        }
        Ok(apply_status)
    }

    /// Append-only path: a divergent value for an existing path fails closed as
    /// a conflict instead of selecting a winner.
    fn apply_immutable_value(&mut self, event: &AdminDocumentEvent, path: String, value: String) {
        let value = Some(value);
        if self.conflicts.contains_key(&path) {
            self.record_conflict_value(&path, value, event.dot());
            return;
        }

        let Some(current) = self.user_subject_ids.get(&path).cloned() else {
            let version = self.version_with_dots(&path, value, BTreeSet::from([event.dot()]));
            self.user_subject_ids.insert(path, version);
            return;
        };
        let mut dots = self.take_version_dots(&path, &current);
        if current.value != value {
            for dot in dots {
                self.record_conflict_value(&path, current.value.clone(), dot);
            }
            self.record_conflict_value(&path, value, event.dot());
            self.user_subject_ids.remove(&path);
            return;
        }

        dots.insert(event.dot());
        let version = self.version_with_dots(&path, value, dots);
        self.user_subject_ids.insert(path, version);
    }

    fn reduce_value(
        &mut self,
        event: &AdminDocumentEvent,
        path: &str,
        current: Option<AdminDocumentAttributeVersion>,
        value: Option<String>,
    ) -> Option<AdminDocumentAttributeVersion> {
        if self.event_is_stale_for_path(event, path) {
            return current;
        }
        self.remove_conflict_values_superseded_by(event, path);

        if self.conflicts.contains_key(path) {
            self.record_conflict_value(path, value.clone(), event.dot());
            let equal_values = self.conflicts.get(path).is_some_and(|conflict| {
                conflict
                    .values
                    .iter()
                    .all(|candidate| candidate.value == value)
            });
            if equal_values {
                let dots = self
                    .conflicts
                    .remove(path)
                    .into_iter()
                    .flat_map(|conflict| conflict.values)
                    .map(|candidate| candidate.dot)
                    .collect();
                return Some(self.version_with_dots(path, value, dots));
            }
            return None;
        }

        let Some(current) = current else {
            return Some(self.version_with_dots(path, value, BTreeSet::from([event.dot()])));
        };
        let mut unobserved_dots = self.take_version_dots(path, &current);
        unobserved_dots.retain(|dot| !event_observes_dot(event, dot));
        if unobserved_dots.is_empty() {
            return Some(self.version_with_dots(path, value, BTreeSet::from([event.dot()])));
        }

        if current.value != value {
            for dot in unobserved_dots {
                self.record_conflict_value(path, current.value.clone(), dot);
            }
            self.record_conflict_value(path, value, event.dot());
            return None;
        }

        unobserved_dots.insert(event.dot());
        Some(self.version_with_dots(path, value, unobserved_dots))
    }

    fn reduce_role_value(
        &mut self,
        event: &AdminDocumentEvent,
        path: &str,
        current: Option<AdminDocumentAttributeVersion>,
        value: String,
    ) -> Option<AdminDocumentAttributeVersion> {
        self.reduce_value(event, path, current, Some(value))
    }

    fn event_is_stale_for_path(&self, event: &AdminDocumentEvent, path: &str) -> bool {
        let same_origin_at_or_after = |dot: &AdminDocumentDot| {
            dot.origin_node_id == event.origin_node_id && dot.origin_seq >= event.origin_seq
        };

        self.version_for_path(path)
            .is_some_and(|version| same_origin_at_or_after(&version.dot))
            || self
                .equivalent_value_dots
                .get(path)
                .is_some_and(|dots| dots.iter().any(same_origin_at_or_after))
            || self.conflicts.get(path).is_some_and(|conflict| {
                conflict
                    .values
                    .iter()
                    .any(|value| same_origin_at_or_after(&value.dot))
            })
    }

    fn version_for_path(&self, path: &str) -> Option<&AdminDocumentAttributeVersion> {
        if path == USER_NAME_PATH {
            return self.user_name.as_ref();
        }
        if let Some(key) = path.strip_prefix("user.attributes.") {
            return self.user_attributes.get(key);
        }
        if let Some(subject_id) = path.strip_prefix("user.subject_ids.") {
            return self.user_subject_ids.get(subject_id);
        }
        self.user_subject_ids.get(path)
    }

    fn remove_conflict_values_superseded_by(&mut self, event: &AdminDocumentEvent, path: &str) {
        let should_remove_conflict = self.conflicts.get_mut(path).is_some_and(|conflict| {
            conflict
                .values
                .retain(|value| !event_observes_dot(event, &value.dot));
            conflict.values.is_empty()
        });
        if should_remove_conflict {
            self.conflicts.remove(path);
        }
    }

    fn take_version_dots(
        &mut self,
        path: &str,
        version: &AdminDocumentAttributeVersion,
    ) -> BTreeSet<AdminDocumentDot> {
        let mut dots = self.equivalent_value_dots.remove(path).unwrap_or_default();
        dots.insert(version.dot);
        dots
    }

    fn version_with_dots(
        &mut self,
        path: &str,
        value: Option<String>,
        mut dots: BTreeSet<AdminDocumentDot>,
    ) -> AdminDocumentAttributeVersion {
        let dot = dots.pop_first().expect("admin value has a causal dot");
        if dots.is_empty() {
            self.equivalent_value_dots.remove(path);
        } else {
            self.equivalent_value_dots.insert(path.to_string(), dots);
        }
        AdminDocumentAttributeVersion { value, dot }
    }

    fn record_conflict_value(&mut self, path: &str, value: Option<String>, dot: AdminDocumentDot) {
        let conflict =
            self.conflicts
                .entry(path.to_string())
                .or_insert_with(|| AdminDocumentConflict {
                    path: path.to_string(),
                    values: Vec::new(),
                });

        if !conflict.values.iter().any(|candidate| candidate.dot == dot) {
            conflict
                .values
                .push(AdminDocumentConflictValue { value, dot });
            conflict.values.sort_by_key(|value| value.dot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdminDocumentApplyStatus, AdminDocumentAttributeVersion, AdminDocumentConflict,
        AdminDocumentConflictValue, AdminDocumentReducerError, AdminDocumentReducerState,
        GROUP_DISPLAY_NAME_PATH, GROUP_REALM_ID_PATH, REALM_CONFIG_DEFAULT_STRATEGY_PATH,
        REALM_CONFIG_DESCRIPTION_PATH, REALM_CONFIG_DISCOVERY_PATH,
        REALM_CONFIG_METADATA_REPLICATION_PATH, REALM_CONFIG_QUOTA_PATH, USER_NAME_PATH,
        binding_scope_key, group_role_id_from_path, group_role_path,
        group_role_user_assignment_from_path, group_role_user_assignment_path, handle_range_path,
        metadata_replication_value, oidc_provider_value,
        overlay_realm_config_placement_reducer_materialization, placement_binding_handle,
        placement_binding_path, realm_config_node_id_from_path, realm_config_node_path,
        realm_config_oidc_provider_id_from_path, realm_config_oidc_provider_path,
        realm_config_placement_node_id_from_path, realm_config_placement_node_path,
        realm_config_placement_strategy_id_from_path, realm_config_placement_strategy_path,
        realm_config_strategy_binding_path, realm_config_strategy_binding_scope_key_from_path,
        realm_discovery_value, realm_node_kind_from_value, realm_node_kind_value,
        realm_role_id_from_path, realm_role_path, realm_role_user_assignment_from_path,
        realm_role_user_assignment_path, revoked_token_path, role_definition_value,
        user_attribute_path, user_subject_id_path,
    };
    use crate::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use crate::auth::REVOCATION_GRACE_SECS;
    use crate::structs::{
        Actor, AffinityEffect, AffinityRule, BindingError, BindingScope, BucketPlan,
        CandidateMapNode, CandidatePlacementMap, CompletionProof, DocumentClass,
        FIRST_GRANTABLE_HANDLE, GroupQuotaOverride, HandleRange, KIND_LABEL_KEY, LabelMatch,
        MAX_PLACEMENT_SHARD_COUNT, MetadataReplicationConfig, NodePlacementEntry,
        OidcProviderConfig, Permission, PlacementBinding, PlacementOverride, PlacementScope,
        PlacementStrategy, ProofClaim, QuotaConfig, RealmConfigDocument, RealmDiscoveryConfig,
        RealmId, RealmNodeKind, STORAGE_CLASS_LABEL_PREFIX, StrategyBinding, TransitionLimits,
        TransitionPlan, TransitionStatus, UserGroupCapOverride,
    };
    use crate::structured_id::PlacementHandle;
    use crate::types::{GroupId, RoleId};
    use crate::user_update_validation::UserAttributeValidationError;
    use crate::{NodeId, UserId};
    use std::collections::{BTreeMap, BTreeSet};
    use ulid::Ulid;

    mod fixtures;
    use fixtures::*;
    mod group;
    mod placement;
    mod realm_config;
    mod revocation;
    use revocation::*;
    mod roles;
    mod user;

    #[test]
    fn divergent_expiry_keeps() {
        // A second expiry for one hash must never erase the revocation; the
        // longest expiry wins so the token stays denied.
        let mut state = realm_config_state();
        state.apply(&revoke_token(1, 1, "token")).unwrap();
        let mut longer = revoke_token(2, 2, "token");
        longer.op = AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: crate::auth::bearer_token_hash("token"),
            expires_at: 5_000,
            token_owner: user_id(),
        };
        state.apply(&longer).unwrap();

        assert!(state.conflicts.is_empty());
        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([(crate::auth::bearer_token_hash("token"), 5_000)])
        );
    }

    #[test]
    fn stale_revocation_applies() {
        // A revocation from a lagging origin sequence must still deny the token.
        let mut state = realm_config_state();
        let mut ahead = revoke_token(1, 1, "ahead");
        ahead.origin_seq = 9;
        state.apply(&ahead).unwrap();
        let behind = revoke_token(2, 1, "behind");

        state.apply(&behind).unwrap();
        assert!(
            state
                .materialized_revoked_tokens()
                .contains_key(&crate::auth::bearer_token_hash("behind"))
        );
    }

    #[test]
    fn compaction_drops_expired() {
        // Expired revocations must leave no reducer residue, so a user revoking
        // token after token cannot grow the persisted state without bound.
        let mut state = realm_config_state();
        let expired = revoke_token(1, 1, "expired");
        let echoed = revoke_token(3, 2, "expired");
        let live = realm_config_event(
            2,
            node(1),
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: crate::auth::bearer_token_hash("live"),
                expires_at: 9_000,
                token_owner: user_id(),
            },
        );
        for event in [&expired, &echoed, &live] {
            state.apply(event).unwrap();
        }

        state.compact_revocations(3_000);

        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([(crate::auth::bearer_token_hash("live"), 9_000)])
        );
        assert!(!state.applied_event_ids.contains(&expired.event_id));
        assert!(!state.applied_event_ids.contains(&echoed.event_id));
        assert!(state.applied_event_ids.contains(&live.event_id));
        assert!(state.equivalent_value_dots.is_empty());
    }

    #[test]
    fn compaction_keeps_unexpired() {
        // The expiry boundary matches the materialized set, which keeps an
        // entry while `expires_at >= now`.
        let mut state = realm_config_state();
        state.apply(&revoke_token(1, 1, "token")).unwrap();

        state.compact_revocations(2_000);

        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([(crate::auth::bearer_token_hash("token"), 2_000)])
        );
    }

    #[test]
    fn compaction_spares_paths() {
        let mut state = realm_config_state();
        state
            .apply(&set_realm_config_description(1, 1, "realm"))
            .unwrap();
        state.apply(&revoke_token(2, 1, "token")).unwrap();

        state.compact_revocations(9_000);

        assert!(state.materialized_revoked_tokens().is_empty());
        assert_eq!(
            state.materialized_realm_config_description(),
            Some("realm".to_string())
        );
    }

    #[test]
    fn rejects_malformed_hash() {
        let mut state = realm_config_state();
        let event = realm_config_event(
            4,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: "not-a-hash".to_string(),
                expires_at: 2_000,
                token_owner: user_id(),
            },
        );

        assert_eq!(
            state.apply(&event),
            Err(AdminDocumentReducerError::InvalidTokenHash)
        );
        assert!(state.materialized_revoked_tokens().is_empty());
    }

    fn secret(seed: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[seed; 32])
    }

    fn map_with(epoch: u64, seeds: &[u8]) -> CandidatePlacementMap {
        CandidatePlacementMap {
            epoch,
            nodes: seeds
                .iter()
                .map(|seed| CandidateMapNode {
                    node_id: node(*seed),
                    kind: RealmNodeKind::Server,
                    location: "eu".to_string(),
                    weight: 100,
                    full: false,
                    draining: false,
                    labels: BTreeMap::new(),
                })
                .collect(),
            selectors: vec![crate::structs::FrozenStrategySelector {
                strategy_id: transition_strategy().strategy_id,
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
            }],
            shard_overrides: Vec::new(),
        }
    }

    fn transition_strategy() -> PlacementStrategy {
        PlacementStrategy {
            strategy_id: Ulid::from_bytes([21; 16]),
            name: "moved".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 2,
        }
    }

    fn transition_plan(old: &[u8], target: &[u8]) -> TransitionPlan {
        let bucket = |bucket: u32| BucketPlan {
            bucket,
            old_holders: old.iter().map(|seed| node(*seed)).collect(),
            target_holders: target.iter().map(|seed| node(*seed)).collect(),
            predecessor_epoch: 1,
        };
        TransitionPlan {
            transition_id: Ulid::from_bytes([31; 16]),
            strategy_id: transition_strategy().strategy_id,
            buckets: vec![bucket(0), bucket(1)],
            target_map_epoch: 2,
            limits: TransitionLimits::default(),
            created_by: node(1),
            created_at_ms: 5,
        }
    }

    /// The digest of the fixture's reduced barrier set (holders 1 and 2).
    fn fixture_digest(plan: &TransitionPlan, bucket: u32) -> [u8; 32] {
        let mut transition = crate::structs::PlacementTransition::new(plan.clone());
        transition.barriers = [1u8, 2]
            .iter()
            .map(|seed| crate::structs::BucketBarrier {
                bucket,
                reported_by: node(*seed),
                frontier: vec![*seed],
            })
            .collect();
        transition.barrier_digest(bucket)
    }

    fn proof_for(plan: &TransitionPlan, bucket: u32, seed: u8) -> CompletionProof {
        ProofClaim {
            realm_id: realm_id(),
            transition_id: plan.transition_id,
            strategy_id: plan.strategy_id,
            bucket,
            old_activation_epoch: 1,
            target_map_epoch: plan.target_map_epoch,
            barrier_digest: fixture_digest(plan, bucket),
            checkpoint_root: [7; 32],
            holder: node(seed),
        }
        .sign(&secret(seed))
    }

    /// Publish two maps, activate epoch 1, and start a 1 -> 2 transition.
    fn transition_events(plan: &TransitionPlan) -> Vec<AdminDocumentEvent> {
        vec![
            realm_config_event(
                40,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: transition_strategy(),
                },
            ),
            realm_config_event(
                41,
                node(1),
                2,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigCandidateMapPublished {
                    map: map_with(1, &[1, 2]),
                },
            ),
            realm_config_event(
                42,
                node(1),
                3,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigCandidateMapPublished {
                    map: map_with(2, &[3, 4]),
                },
            ),
            realm_config_event(
                43,
                node(1),
                4,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigActivationsInitialized {
                    strategy_id: plan.strategy_id,
                    candidate_map_epoch: 1,
                },
            ),
            realm_config_event(
                44,
                node(1),
                5,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigTransitionStarted { plan: plan.clone() },
            ),
        ]
    }

    /// Every barrier and proof bucket 0 needs to cut over.
    fn completion_events(plan: &TransitionPlan) -> Vec<AdminDocumentEvent> {
        let barrier = |seed: u8, event_seed: u8| {
            realm_config_event(
                event_seed,
                node(seed),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                    transition_id: plan.transition_id,
                    bucket: 0,
                    reported_by: node(seed),
                    frontier: vec![seed],
                },
            )
        };
        let proof = |seed: u8, event_seed: u8| {
            realm_config_event(
                event_seed,
                node(seed),
                2,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                    transition_id: plan.transition_id,
                    strategy_id: plan.strategy_id,
                    proof: proof_for(plan, 0, seed),
                },
            )
        };
        vec![barrier(1, 50), barrier(2, 51), proof(3, 52), proof(4, 53)]
    }

    fn transition_config(state: &AdminDocumentReducerState) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        overlay_realm_config_placement_reducer_materialization(&mut config, state, 0);
        config
    }

    #[test]
    fn foreign_reports_dropped() {
        // A barrier from a non-old-holder and a stall from an outsider reduce
        // as values but never materialize; oversized reports fail at apply.
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let mut state = realm_config_state();
        for event in transition_events(&plan) {
            state.apply(&event).unwrap();
        }
        state
            .apply(&realm_config_event(
                80,
                node(3),
                5,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                    transition_id: plan.transition_id,
                    bucket: 0,
                    reported_by: node(3),
                    frontier: vec![3],
                },
            ))
            .unwrap();
        state
            .apply(&realm_config_event(
                81,
                node(5),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigTransitionStallReported {
                    transition_id: plan.transition_id,
                    bucket: 0,
                    reported_by: node(5),
                    reason: "spoofed".to_string(),
                },
            ))
            .unwrap();

        let transitions = state.materialized_transitions();
        let transition = transitions
            .iter()
            .find(|transition| transition.plan.transition_id == plan.transition_id)
            .expect("transition materializes");
        assert!(
            transition
                .barriers
                .iter()
                .all(|barrier| barrier.reported_by != node(3))
        );
        assert!(transition.stalls.is_empty());

        let oversized = realm_config_event(
            82,
            node(1),
            9,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                transition_id: plan.transition_id,
                bucket: 0,
                reported_by: node(1),
                frontier: vec![0; crate::structs::MAX_BARRIER_FRONTIER_BYTES + 1],
            },
        );
        assert!(matches!(
            state.apply(&oversized),
            Err(AdminDocumentReducerError::TransitionReportOversized)
        ));
    }

    #[test]
    fn concurrent_plans_gated() {
        // Two complete plans derived from one activation base: only the
        // ULID-first one advances the bucket, in either delivery order, and
        // the other can never replay as its successor.
        let plan_a = transition_plan(&[1, 2], &[3, 4]);
        let mut plan_b = transition_plan(&[1, 2], &[3, 4]);
        plan_b.transition_id = Ulid::from_bytes([32; 16]);
        plan_b.target_map_epoch = 3;

        let start_b = realm_config_event(
            45,
            node(1),
            6,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionStarted {
                plan: plan_b.clone(),
            },
        );
        let completion_b: Vec<AdminDocumentEvent> = (0..4)
            .map(|index| {
                let seed = (index + 1) as u8;
                if index < 2 {
                    realm_config_event(
                        70 + index as u8,
                        node(seed),
                        3,
                        AdminDocumentClock::default(),
                        AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                            transition_id: plan_b.transition_id,
                            bucket: 0,
                            reported_by: node(seed),
                            frontier: vec![seed],
                        },
                    )
                } else {
                    realm_config_event(
                        70 + index as u8,
                        node(seed),
                        4,
                        AdminDocumentClock::default(),
                        AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                            transition_id: plan_b.transition_id,
                            strategy_id: plan_b.strategy_id,
                            proof: proof_for(&plan_b, 0, seed),
                        },
                    )
                }
            })
            .collect();

        let mut forward: Vec<AdminDocumentEvent> = transition_events(&plan_a);
        forward.extend(completion_events(&plan_a));
        forward.push(start_b.clone());
        forward.extend(completion_b.clone());

        let mut reversed: Vec<AdminDocumentEvent> = transition_events(&plan_a);
        reversed.push(start_b);
        reversed.extend(completion_b);
        reversed.extend(completion_events(&plan_a));

        for events in [forward, reversed] {
            let mut state = realm_config_state();
            for event in events {
                state.apply(&event).unwrap();
            }
            let config = transition_config(&state);
            let activation = config
                .activation(&plan_a.strategy_id, 0)
                .expect("activation");
            assert_eq!(activation.activation_epoch, 2);
            assert_eq!(activation.candidate_map_epoch, plan_a.target_map_epoch);
        }
    }

    #[test]
    fn map_conflict_fails_closed() {
        // Two divergent maps at one epoch keep the epoch unusable, both retained.
        let mut state = realm_config_state();
        for (event_seed, origin, seeds) in [(60u8, node(1), &[1u8, 2][..]), (61, node(2), &[3][..])]
        {
            state
                .apply(&realm_config_event(
                    event_seed,
                    origin,
                    1,
                    AdminDocumentClock::default(),
                    AdminDocumentOperation::RealmConfigCandidateMapPublished {
                        map: map_with(1, seeds),
                    },
                ))
                .unwrap();
        }

        let config = transition_config(&state);
        assert_eq!(config.candidate_maps.len(), 2);
        assert!(config.candidate_map(1).is_none());
        assert!(state.materialized_candidate_maps().is_empty());
    }

    #[test]
    fn activation_init_covers_buckets() {
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let mut state = realm_config_state();
        for event in transition_events(&plan).iter().take(4) {
            state.apply(event).unwrap();
        }

        let config = transition_config(&state);
        assert_eq!(config.placement_activations.len(), 2);
        for shard in 0..2 {
            let activation = config
                .activation(&plan.strategy_id, shard)
                .expect("bucket activated");
            assert_eq!(activation.activation_epoch, 1);
            assert_eq!(activation.candidate_map_epoch, 1);
            assert_eq!(activation.transition_id, None);
        }
    }

    #[test]
    fn proof_admission_rejects_forgery() {
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let mut state = realm_config_state();
        for event in transition_events(&plan) {
            state.apply(&event).unwrap();
        }
        let submit = |proof: CompletionProof, origin: NodeId, event_seed: u8| {
            realm_config_event(
                event_seed,
                origin,
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                    transition_id: plan.transition_id,
                    strategy_id: plan.strategy_id,
                    proof,
                },
            )
        };

        // A proof relayed by anyone but its holder never enters the record.
        assert_eq!(
            state.apply(&submit(proof_for(&plan, 0, 3), node(1), 70)),
            Err(AdminDocumentReducerError::TransitionOriginMismatch)
        );
        // A tampered epoch invalidates the signature over the claim.
        let mut retargeted = proof_for(&plan, 0, 3);
        retargeted.target_map_epoch = 9;
        assert_eq!(
            state.apply(&submit(retargeted, node(3), 71)),
            Err(AdminDocumentReducerError::InvalidTransitionProof)
        );
        // So does a signature made by another node key.
        let mut forged = proof_for(&plan, 0, 4);
        forged.holder = node(3);
        assert_eq!(
            state.apply(&submit(forged, node(3), 72)),
            Err(AdminDocumentReducerError::InvalidTransitionProof)
        );

        let config = transition_config(&state);
        assert!(config.placement_transitions[0].proofs.is_empty());
    }

    #[test]
    fn duplicate_proof_is_idempotent() {
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let mut state = realm_config_state();
        for event in transition_events(&plan) {
            state.apply(&event).unwrap();
        }
        let first = realm_config_event(
            73,
            node(3),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                transition_id: plan.transition_id,
                strategy_id: plan.strategy_id,
                proof: proof_for(&plan, 0, 3),
            },
        );
        let mut resent = first.clone();
        resent.event_id = Ulid::from_bytes([74; 16]);
        resent.origin_seq = 2;

        state.apply(&first).unwrap();
        assert_eq!(state.apply(&first), Ok(AdminDocumentApplyStatus::Duplicate));
        state.apply(&resent).unwrap();

        let config = transition_config(&state);
        assert_eq!(config.placement_transitions[0].proofs.len(), 1);
    }

    #[test]
    fn activation_advances_on_completion() {
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let mut state = realm_config_state();
        for event in transition_events(&plan)
            .into_iter()
            .chain(completion_events(&plan))
        {
            state.apply(&event).unwrap();
        }

        let config = transition_config(&state);
        let cut = config.activation(&plan.strategy_id, 0).expect("bucket 0");
        assert_eq!(cut.candidate_map_epoch, 2);
        assert_eq!(cut.activation_epoch, 2);
        assert_eq!(cut.transition_id, None);

        // Bucket 1 has no barrier or proof, so it stays where it was and keeps
        // naming the transition still working on it.
        let pending = config.activation(&plan.strategy_id, 1).expect("bucket 1");
        assert_eq!(pending.candidate_map_epoch, 1);
        assert_eq!(pending.activation_epoch, 1);
        assert_eq!(pending.transition_id, Some(plan.transition_id));

        let transition = &config.placement_transitions[0];
        assert_eq!(transition.completed.len(), 1);
        assert_eq!(transition.completed[0].bucket, 0);
        assert_eq!(
            transition.completed[0].completed_at_ms,
            Ulid::from_bytes([53; 16]).timestamp_ms()
        );
        assert!(!transition.is_terminal());
    }

    #[test]
    fn advance_ignores_event_order() {
        // Every replica reduces the same set into the same activations, whatever
        // order the events arrive in.
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let events: Vec<AdminDocumentEvent> = transition_events(&plan)
            .into_iter()
            .chain(completion_events(&plan))
            .collect();
        let mut forward = realm_config_state();
        for event in &events {
            forward.apply(event).unwrap();
        }
        let mut permuted = realm_config_state();
        for index in [8, 3, 6, 1, 7, 0, 5, 4, 2] {
            permuted.apply(&events[index]).unwrap();
        }

        let expected = transition_config(&forward);
        let actual = transition_config(&permuted);
        assert_eq!(expected.placement_activations, actual.placement_activations);
        assert_eq!(expected.placement_transitions, actual.placement_transitions);
        assert_eq!(expected.candidate_maps, actual.candidate_maps);
    }

    #[test]
    fn abort_keeps_cut_buckets() {
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let mut state = realm_config_state();
        for event in transition_events(&plan)
            .into_iter()
            .chain(completion_events(&plan))
        {
            state.apply(&event).unwrap();
        }
        state
            .apply(&realm_config_event(
                80,
                node(1),
                6,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigTransitionAborted {
                    transition_id: plan.transition_id,
                },
            ))
            .unwrap();

        let config = transition_config(&state);
        let transition = &config.placement_transitions[0];
        assert!(matches!(transition.status, TransitionStatus::Aborted));
        assert!(transition.is_terminal());
        // The cut bucket stays cut; the un-cut one keeps its old activation.
        assert_eq!(
            config
                .activation(&plan.strategy_id, 0)
                .unwrap()
                .candidate_map_epoch,
            2
        );
        assert_eq!(
            config
                .activation(&plan.strategy_id, 1)
                .unwrap()
                .candidate_map_epoch,
            1
        );
        assert_eq!(
            config
                .activation(&plan.strategy_id, 1)
                .unwrap()
                .transition_id,
            None
        );
    }

    #[test]
    fn late_proof_completes() {
        // A proof that lands after the abort completes its bucket anyway:
        // reduction cannot depend on arrival order, so an abort stops the
        // executors rather than un-making a hand-off every target proved.
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let abort = realm_config_event(
            81,
            node(1),
            6,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTransitionAborted {
                transition_id: plan.transition_id,
            },
        );
        let completion = completion_events(&plan);
        let mut interleaved = realm_config_state();
        for event in transition_events(&plan)
            .iter()
            .chain(completion.iter().take(3))
            .chain(std::iter::once(&abort))
            .chain(completion.iter().skip(3))
        {
            interleaved.apply(event).unwrap();
        }

        let config = transition_config(&interleaved);
        let transition = &config.placement_transitions[0];
        assert!(matches!(transition.status, TransitionStatus::Aborted));
        assert!(transition.completion(0).is_some());
        assert_eq!(
            config
                .activation(&plan.strategy_id, 0)
                .expect("bucket 0")
                .candidate_map_epoch,
            2
        );

        // The bucket the abort caught mid-flight keeps its old activation.
        assert_eq!(
            config
                .activation(&plan.strategy_id, 1)
                .expect("bucket 1")
                .candidate_map_epoch,
            1
        );

        let mut abort_last = realm_config_state();
        for event in transition_events(&plan)
            .iter()
            .chain(completion.iter())
            .chain(std::iter::once(&abort))
        {
            abort_last.apply(event).unwrap();
        }
        assert_eq!(transition_config(&abort_last), config);
    }

    #[test]
    fn prune_keeps_advances() {
        // Dropping a released record must not drop what it moved: activations
        // are replayed from the whole reduced chain, so a fold that skipped the
        // pruned record would silently regress the bucket to its old map.
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let mut state = realm_config_state();
        for event in transition_events(&plan)
            .into_iter()
            .chain(completion_events(&plan))
        {
            state.apply(&event).unwrap();
        }
        // Only a record whose every bucket cut over is terminal, so bucket one
        // has to finish before the release can be observed at all.
        for (event_seed, seed, op) in [
            (
                60u8,
                1u8,
                AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                    transition_id: plan.transition_id,
                    bucket: 1,
                    reported_by: node(1),
                    frontier: vec![1],
                },
            ),
            (
                61,
                2,
                AdminDocumentOperation::RealmConfigTransitionBarrierReported {
                    transition_id: plan.transition_id,
                    bucket: 1,
                    reported_by: node(2),
                    frontier: vec![2],
                },
            ),
            (
                62,
                3,
                AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                    transition_id: plan.transition_id,
                    strategy_id: plan.strategy_id,
                    proof: proof_for(&plan, 1, 3),
                },
            ),
            (
                63,
                4,
                AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
                    transition_id: plan.transition_id,
                    strategy_id: plan.strategy_id,
                    proof: proof_for(&plan, 1, 4),
                },
            ),
        ] {
            state
                .apply(&realm_config_event(
                    event_seed,
                    node(seed),
                    3,
                    AdminDocumentClock::default(),
                    op,
                ))
                .unwrap();
        }
        // Release additionally needs every departing holder's drain report.
        for (event_seed, seed, bucket) in [(64u8, 1u8, 0u32), (65, 2, 0), (66, 1, 1), (67, 2, 1)] {
            state
                .apply(&realm_config_event(
                    event_seed,
                    node(seed),
                    4 + u64::from(bucket),
                    AdminDocumentClock::default(),
                    AdminDocumentOperation::RealmConfigTransitionDrainReported {
                        transition_id: plan.transition_id,
                        bucket,
                        reported_by: node(seed),
                    },
                ))
                .unwrap();
        }
        let live = transition_config(&state);
        assert_eq!(live.placement_transitions.len(), 1);
        assert!(live.placement_transitions[0].is_terminal());

        let mut pruned = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        overlay_realm_config_placement_reducer_materialization(&mut pruned, &state, u64::MAX);
        assert!(pruned.placement_transitions.is_empty());
        assert_eq!(pruned.placement_activations, live.placement_activations);

        // Re-materializing from scratch reproduces the pruned view exactly, so
        // the record's absence is stable rather than a one-time loss.
        let mut again = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        overlay_realm_config_placement_reducer_materialization(&mut again, &state, u64::MAX);
        assert_eq!(again, pruned);
        // The bucket that cut over still names its target map, and that map
        // survives the prune because an activation references it.
        assert_eq!(
            pruned
                .activation(&plan.strategy_id, 0)
                .expect("bucket 0")
                .candidate_map_epoch,
            2
        );
        assert!(pruned.candidate_map(2).is_some());
    }

    #[test]
    fn drops_unreferenced_maps() {
        // A map no activation selects from and no retained transition targets
        // is unreachable - unless it is the newest, which the next transition
        // would name.
        let plan = transition_plan(&[1, 2], &[3, 4]);
        let mut state = realm_config_state();
        for event in transition_events(&plan) {
            state.apply(&event).unwrap();
        }
        state
            .apply(&realm_config_event(
                90,
                node(1),
                6,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigCandidateMapPublished {
                    map: map_with(3, &[1, 4]),
                },
            ))
            .unwrap();

        let config = transition_config(&state);
        assert!(config.candidate_map(1).is_some(), "activated");
        assert!(config.candidate_map(2).is_some(), "targeted in flight");
        assert!(config.candidate_map(3).is_some(), "newest");

        // Aborting frees the target map: nothing selects from epoch two any more.
        state
            .apply(&realm_config_event(
                91,
                node(1),
                7,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigTransitionAborted {
                    transition_id: plan.transition_id,
                },
            ))
            .unwrap();
        let config = transition_config(&state);
        assert!(config.placement_transitions.is_empty());
        assert!(config.candidate_map(2).is_none());
        assert!(config.candidate_map(1).is_some());
        assert!(config.candidate_map(3).is_some());
    }
}
