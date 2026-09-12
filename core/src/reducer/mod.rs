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
use crate::user_validation::{
    UserAttributeValidationError, validate_attribute_key, validate_attribute_value,
};

mod apply;
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

pub fn decode_reducer_state(bytes: &[u8]) -> Result<AdminDocumentReducerState, postcard::Error> {
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
        if self.event_path_stale(event, path) {
            return current;
        }
        self.remove_superseded_conflicts(event, path);

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

    fn event_path_stale(&self, event: &AdminDocumentEvent, path: &str) -> bool {
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

    fn remove_superseded_conflicts(&mut self, event: &AdminDocumentEvent, path: &str) {
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
mod tests;
