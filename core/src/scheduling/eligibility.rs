//! Hard eligibility. Every rule here can only remove a target: stale telemetry
//! and free capacity are ranking inputs, and exact admission happens at the
//! target itself.

use crate::compute::NetworkAccess;
use crate::scheduling::inputs::{PlanRequest, TargetCandidate};
use crate::structs::{PlacementDecision, PlacementPolicyRef, PlacementSubject, evaluate_placement};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Why one advertised target is not legal for this request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectionVerdict {
    /// Realm or group authorization does not allow this request here.
    NotAuthorized,
    /// User nodes never take cross-node work.
    NodeKind,
    Inactive,
    ComputeDraining,
    PolicyDraining,
    /// The advertised subject does not match its advertised digest.
    SubjectDrift,
    ExecutorKind,
    Staging,
    RequiredLabels,
    Resources,
    /// Protected data with open networking on a site that enforces none.
    OpenNetwork,
    Policy {
        verdict: PolicyVerdict,
        policy_ids: Vec<Ulid>,
    },
    /// An exact input has no compliant local copy and no known legal source.
    NoLegalSource {
        destination_key: String,
    },
}

/// Bounded, serializable projection of a [`PlacementDecision`] for audit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyVerdict {
    /// Referenced policies this node has not resolved yet.
    Required,
    Unavailable,
    DigestMismatch,
    Invalid,
    Denied,
    InvalidInput,
}

impl PolicyVerdict {
    /// Whether a later round with more policy bytes may still succeed.
    pub fn retryable(&self) -> bool {
        matches!(self, PolicyVerdict::Required | PolicyVerdict::Unavailable)
    }
}

impl RejectionVerdict {
    /// Whether the rejection may resolve itself once the missing values arrive.
    pub fn retryable(&self) -> bool {
        match self {
            RejectionVerdict::Policy { verdict, .. } => verdict.retryable(),
            RejectionVerdict::NoLegalSource { .. } => true,
            _ => false,
        }
    }
}

/// Every hard filter except data-source availability, which needs the routed
/// transfer cost. `None` means the target stays a candidate.
pub fn screen(request: &PlanRequest, candidate: &TargetCandidate) -> Option<RejectionVerdict> {
    if !candidate.group_allowed {
        return Some(RejectionVerdict::NotAuthorized);
    }
    if !candidate.node_kind.is_sync_eligible() {
        return Some(RejectionVerdict::NodeKind);
    }
    if !candidate.active {
        return Some(RejectionVerdict::Inactive);
    }
    if candidate.compute_draining {
        return Some(RejectionVerdict::ComputeDraining);
    }
    if candidate.capability.policy_draining {
        return Some(RejectionVerdict::PolicyDraining);
    }
    if candidate.capability.validate(candidate.node_id).is_err() {
        return Some(RejectionVerdict::SubjectDrift);
    }
    if let Some(kind) = request.executor_constraint.as_deref()
        && kind.trim() != candidate.capability.kind.trim()
    {
        return Some(RejectionVerdict::ExecutorKind);
    }
    if !candidate.capability.supports(request.staging) {
        return Some(RejectionVerdict::Staging);
    }
    if !labels_match(request, &candidate.capability.subject) {
        return Some(RejectionVerdict::RequiredLabels);
    }
    if !candidate.capability.limits.fits(&request.resources) {
        return Some(RejectionVerdict::Resources);
    }
    if protected(request)
        && request.network == NetworkAccess::Open
        && !candidate.capability.network_policy
    {
        return Some(RejectionVerdict::OpenNetwork);
    }
    policy_verdict(request, &candidate.capability.subject)
}

/// Whether any input or output of this request is governed at all.
fn protected(request: &PlanRequest) -> bool {
    !request.output_policies.is_empty()
        || request
            .inputs
            .iter()
            .any(|input| !input.policies.is_empty())
}

fn labels_match(request: &PlanRequest, subject: &PlacementSubject) -> bool {
    request.required_labels.iter().all(|label| {
        subject
            .labels
            .get(label.key.trim())
            .map(|value| value.trim())
            == Some(label.value.trim())
    })
}

/// Every input ref set and the output ref set must allow the subject. Sets are
/// evaluated separately so a union can never exceed the declared ref bound.
fn policy_verdict(request: &PlanRequest, subject: &PlacementSubject) -> Option<RejectionVerdict> {
    let outputs = std::iter::once(request.output_policies.as_slice());
    let inputs = request.inputs.iter().map(|input| input.policies.as_slice());
    for refs in outputs.chain(inputs) {
        if let Some(verdict) = allows(refs, request, subject) {
            return Some(verdict);
        }
    }
    None
}

/// `None` when the subject satisfies `refs` under the locally verified policies.
pub fn allows(
    refs: &[PlacementPolicyRef],
    request: &PlanRequest,
    subject: &PlacementSubject,
) -> Option<RejectionVerdict> {
    let (verdict, policy_ids) = match evaluate_placement(refs, &request.policies, subject) {
        PlacementDecision::Allowed => return None,
        PlacementDecision::Required { refs } => (
            PolicyVerdict::Required,
            refs.iter().map(|policy| policy.policy_id).collect(),
        ),
        PlacementDecision::Unavailable { policy_ids } => (PolicyVerdict::Unavailable, policy_ids),
        PlacementDecision::DigestMismatch { refs } => (
            PolicyVerdict::DigestMismatch,
            refs.iter().map(|policy| policy.policy_id).collect(),
        ),
        PlacementDecision::Invalid { policy_ids } => (PolicyVerdict::Invalid, policy_ids),
        PlacementDecision::Denied { policy_ids } => (PolicyVerdict::Denied, policy_ids),
        PlacementDecision::InvalidInput { .. } => (PolicyVerdict::InvalidInput, Vec::new()),
    };
    Some(RejectionVerdict::Policy {
        verdict,
        policy_ids,
    })
}
