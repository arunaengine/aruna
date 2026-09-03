//! Pinned scheduling inputs. Every value here is exact and already resolved:
//! the planner performs no I/O and never invents a missing value.

use crate::NodeId;
use crate::compute::{ExecutorCapability, NetworkAccess, StagingMode};
use crate::structs::{
    ComputeConfigError, EffectiveResources, LabelMatch, MAX_SELECTOR_LABELS, PlacementPolicyError,
    PlacementPolicyRef, PlacementSubject, PolicyResolution, RealmNodeKind, SubmissionId,
    VersionedObjectArn,
};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use thiserror::Error;
use ulid::Ulid;

/// Maximum inputs one plan pins.
pub const MAX_PLAN_INPUTS: usize = 512;
/// Maximum registered holders considered per input.
pub const MAX_INPUT_HOLDERS: usize = 32;
/// Maximum advertisements one planning page screens. A scan continues in the
/// next page of the same planning operation, so the realm total it reaches is
/// bounded only by membership times `MAX_ADVERTISED_EXECUTORS`.
pub const MAX_TARGET_SCAN: usize = 1_024;
/// Maximum eligible targets one plan ranks, after screening the whole scan.
pub const MAX_PLAN_CANDIDATES: usize = 128;
/// Maximum ranked alternatives kept for audit.
pub const MAX_PLAN_ALTERNATIVES: usize = 8;
/// Maximum rejection explanations kept for audit.
pub const MAX_PLAN_REJECTIONS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PlanError {
    #[error("the job has no durable logical admission record")]
    NotAdmitted,
    #[error("a plan pins at most {MAX_PLAN_INPUTS} inputs")]
    InputCount,
    #[error("an input resolves at most {MAX_INPUT_HOLDERS} holders")]
    HolderCount,
    #[error("one planning page screens at most {MAX_TARGET_SCAN} advertised targets")]
    ScanCount,
    #[error("a request declares at most {MAX_SELECTOR_LABELS} required labels")]
    LabelCount,
    #[error("destination key {key} is claimed by two inputs")]
    DuplicateInput { key: String },
    #[error("input {key} pins a version its source does not name")]
    InputMismatch { key: String },
    #[error("target {kind} on node {node_id} is advertised twice")]
    DuplicateTarget { node_id: NodeId, kind: String },
    #[error("target {kind} on node {node_id} does not continue the scan in order")]
    PageOrder { node_id: NodeId, kind: String },
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Config(#[from] ComputeConfigError),
}

/// One registered copy of an input and the storage subject it sits on. Holder
/// discovery is locality evidence only: compliance is still evaluated here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputHolder {
    pub node_id: NodeId,
    pub subject: PlacementSubject,
}

/// One exact input of the physical execution. Snapshot values stay fixed for the
/// launch that stores them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedInput {
    pub destination_key: String,
    pub source: VersionedObjectArn,
    pub version_id: Ulid,
    pub blake3: [u8; 32],
    pub bytes: u64,
    pub policies: Vec<PlacementPolicyRef>,
    pub holders: Vec<InputHolder>,
}

/// One advertised execution target plus the membership values the scheduling
/// node authenticated for it. `node_kind` and `active` come from realm
/// membership, never from the publisher's own document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetCandidate {
    pub node_id: NodeId,
    pub node_kind: RealmNodeKind,
    /// The controller is a current realm member and is not leaving.
    pub active: bool,
    /// Administratively drained by an operator.
    pub compute_draining: bool,
    /// Realm/group authorization result for running this request here.
    pub group_allowed: bool,
    pub capability: ExecutorCapability,
    /// Advertised node load; `None` is unknown and only ranks.
    pub load_permille: Option<u32>,
}

/// Everything one planning round decides on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanRequest {
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub spec_digest: [u8; 32],
    /// A durable logical admission record exists for this request.
    pub admitted: bool,
    pub resources: EffectiveResources,
    pub executor_constraint: Option<String>,
    pub required_labels: Vec<LabelMatch>,
    pub staging: StagingMode,
    pub network: NetworkAccess,
    pub inputs: Vec<ResolvedInput>,
    /// Refs the outputs and workspace inherit.
    pub output_policies: Vec<PlacementPolicyRef>,
    /// Locally verified policy documents; a missing entry blocks, never allows.
    pub policies: BTreeMap<Ulid, PolicyResolution>,
    pub now_ms: u64,
}

impl PlanRequest {
    /// Bounded, canonically ordered copy of the request. Sorting here is what
    /// makes shuffled equivalent values produce one identical plan.
    pub fn canonical(&self) -> Result<Self, PlanError> {
        if !self.admitted {
            return Err(PlanError::NotAdmitted);
        }
        if self.inputs.len() > MAX_PLAN_INPUTS {
            return Err(PlanError::InputCount);
        }
        if self.required_labels.len() > MAX_SELECTOR_LABELS {
            return Err(PlanError::LabelCount);
        }
        let mut request = self.clone();
        request.output_policies = PlacementPolicyRef::canonical_set(&self.output_policies)?;
        request.required_labels.sort_unstable_by(|left, right| {
            (&left.key, &left.value).cmp(&(&right.key, &right.value))
        });
        request.required_labels.dedup();
        request.inputs = self
            .inputs
            .iter()
            .map(ResolvedInput::canonical)
            .collect::<Result<_, _>>()?;
        request
            .inputs
            .sort_unstable_by(|left, right| left.destination_key.cmp(&right.destination_key));
        for pair in request.inputs.windows(2) {
            if pair[0].destination_key == pair[1].destination_key {
                return Err(PlanError::DuplicateInput {
                    key: pair[0].destination_key.clone(),
                });
            }
        }
        Ok(request)
    }
}

/// One advertisement's canonical scan position: node id bytes, then executor
/// kind. Pages are screened in this order and must strictly increase.
pub(crate) fn target_order(candidate: &TargetCandidate) -> (&[u8; 32], &str) {
    (
        candidate.node_id.as_bytes(),
        candidate.capability.kind.as_str(),
    )
}

/// One holder's ascending order: node, then newest subject, then the rest of
/// the subject. Deduplication keeps the first of each node, so two nodes given
/// the same duplicate rows keep the same one and plan identically.
type HolderOrder<'a> = (
    &'a [u8; 32],
    Reverse<u64>,
    &'a str,
    &'a Option<String>,
    &'a BTreeMap<String, String>,
    bool,
);

fn holder_order(holder: &InputHolder) -> HolderOrder<'_> {
    (
        holder.node_id.as_bytes(),
        Reverse(holder.subject.generation),
        holder.subject.location.as_str(),
        &holder.subject.executor_kind,
        &holder.subject.labels,
        holder.subject.local_to_controller,
    )
}

impl ResolvedInput {
    fn canonical(&self) -> Result<Self, PlanError> {
        if self.holders.len() > MAX_INPUT_HOLDERS {
            return Err(PlanError::HolderCount);
        }
        if self.source.version != self.version_id {
            return Err(PlanError::InputMismatch {
                key: self.destination_key.clone(),
            });
        }
        let mut input = self.clone();
        input.policies = PlacementPolicyRef::canonical_set(&self.policies)?;
        input
            .holders
            .sort_unstable_by(|left, right| holder_order(left).cmp(&holder_order(right)));
        input
            .holders
            .dedup_by(|left, right| left.node_id == right.node_id);
        Ok(input)
    }
}

/// One target's ascending score. Field order is the comparison order of plan
/// section 8.4, so the derived `Ord` is the specified tuple order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TargetScore {
    pub estimated_transfer_ms: u64,
    pub transfer_bytes: u64,
    pub availability_pressure_permille: u32,
    pub node_load_permille: u32,
    pub compute_priority_inverse: u32,
    pub unknown_link_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduling::tests::{node, request, resolved_input};

    #[test]
    fn canonical_sorts_inputs() {
        // Shuffled inputs and holders must reduce to one order.
        let plan = request(vec![resolved_input("b", 1), resolved_input("a", 2)]);
        let canonical = plan.canonical().expect("request is bounded");

        assert_eq!(canonical.inputs[0].destination_key, "a");
    }

    #[test]
    fn dedups_holders_alike() {
        // Two nodes handed the same duplicate rows in any order must keep the
        // same holder, or their plan and route digests diverge.
        let holder = |generation: u64, location: &str| InputHolder {
            node_id: node(4),
            subject: PlacementSubject {
                node_id: node(4),
                generation,
                location: location.to_string(),
                labels: BTreeMap::new(),
                executor_kind: None,
                local_to_controller: true,
            },
        };
        let rows = vec![holder(1, "eu-west"), holder(3, "us-east"), holder(3, "ap")];
        let mut input = resolved_input("a", 1);
        input.holders = rows.clone();
        let first = input.canonical().expect("input is bounded");

        let mut input = resolved_input("a", 1);
        input.holders = rows.into_iter().rev().collect();
        let second = input.canonical().expect("input is bounded");

        assert_eq!(first.holders, second.holders);
        assert_eq!(first.holders.len(), 1);
        assert_eq!(first.holders[0].subject.generation, 3);
        assert_eq!(first.holders[0].subject.location, "ap");
    }

    #[test]
    fn rejects_unadmitted_request() {
        let mut plan = request(Vec::new());
        plan.admitted = false;
        assert_eq!(plan.canonical(), Err(PlanError::NotAdmitted));
    }

    #[test]
    fn rejects_duplicate_inputs() {
        let plan = request(vec![resolved_input("a", 1), resolved_input("a", 2)]);
        assert!(matches!(
            plan.canonical(),
            Err(PlanError::DuplicateInput { .. })
        ));
    }
}
