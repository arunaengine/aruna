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
    mod roles;
    mod transitions;
    mod user;
}
