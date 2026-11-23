//! Applies the hard rules that remove a target from a plan and names why it was rejected.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::compute::backend::{Suitability, WorkloadSuitability};
use crate::compute::{ExecutorKind, NetworkAccess};
use crate::scheduling::inputs::{PlanRequest, TargetCandidate};
use crate::structs::placement::policy::{
    PlacementDecision, PlacementPolicyRef, PlacementSubject, evaluate_placement,
};
use crate::types::GroupId;
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
    /// The backend cannot reach S3 for a session kept off the open network.
    Session,
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
    /// The backend refuses this workload kind.
    Unsuitable,
    /// The declared walltime exceeds the backend's maximum.
    WalltimeLimit,
    /// The backend's allow-list does not name the job's group.
    GroupNotAllowed,
    /// The request names another backend.
    BackendName,
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

    /// One short phrase a report reader understands without the audit fields.
    pub fn reason(&self) -> String {
        match self {
            RejectionVerdict::NotAuthorized => "not a realm member".to_string(),
            RejectionVerdict::NodeKind => "user nodes take no shared work".to_string(),
            RejectionVerdict::Inactive => "node is not active".to_string(),
            RejectionVerdict::ComputeDraining => "node stopped taking work".to_string(),
            RejectionVerdict::PolicyDraining => "executor stopped taking work".to_string(),
            RejectionVerdict::SubjectDrift => "advertisement does not match its digest".to_string(),
            RejectionVerdict::ExecutorKind => "no executor of that kind".to_string(),
            RejectionVerdict::Staging => "staging mode is not supported".to_string(),
            RejectionVerdict::Session => "S3-only sessions do not run here".to_string(),
            RejectionVerdict::RequiredLabels => "a required label is missing".to_string(),
            RejectionVerdict::Resources => "not enough resources".to_string(),
            RejectionVerdict::OpenNetwork => "open network is not allowed here".to_string(),
            RejectionVerdict::Policy { .. } => "placement policy refused".to_string(),
            RejectionVerdict::NoLegalSource { destination_key } => {
                format!("no legal source for input {destination_key}")
            }
            RejectionVerdict::Unsuitable => "the backend refuses this kind of work".to_string(),
            RejectionVerdict::WalltimeLimit => "walltime above the backend maximum".to_string(),
            RejectionVerdict::GroupNotAllowed => {
                "the backend does not serve this group".to_string()
            }
            RejectionVerdict::BackendName => "no backend of that name".to_string(),
        }
    }
}

/// Whether a backend of `kind` hosts a session on `network`. A session kept off the open
/// network needs the session channel; a Docker backend without one runs on a remote daemon,
/// which has no session bridge for any session.
pub fn hosts_session(kind: &str, session: bool, network: NetworkAccess) -> bool {
    session
        || (network == NetworkAccess::Open && ExecutorKind::from_wire(kind) != ExecutorKind::Docker)
}

/// Why a backend's operator settings exclude a run: an allow-list without its group, a declared
/// walltime above its maximum, or a workload kind it refuses (batch and sessions exist). Taking
/// the settings directly lets a device without a placement subject select by the same rule.
pub fn backend_verdict(
    groups: &[GroupId],
    max_walltime_ms: Option<u64>,
    suitability: &WorkloadSuitability,
    group_id: GroupId,
    session: bool,
    walltime_ms: u64,
) -> Option<RejectionVerdict> {
    if !groups.is_empty() && !groups.contains(&group_id) {
        return Some(RejectionVerdict::GroupNotAllowed);
    }
    if max_walltime_ms.is_some_and(|max| walltime_ms > max) {
        return Some(RejectionVerdict::WalltimeLimit);
    }
    let suitability = match session {
        true => suitability.sessions,
        false => suitability.batch,
    };
    (suitability == Suitability::Refused).then_some(RejectionVerdict::Unsuitable)
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
    if let Some(name) = request.backend.as_deref()
        && name.trim() != candidate.capability.backend
    {
        return Some(RejectionVerdict::BackendName);
    }
    if !candidate.capability.supports(request.staging) {
        return Some(RejectionVerdict::Staging);
    }
    let capability = &candidate.capability;
    if request.session && !hosts_session(&capability.kind, capability.session, request.network) {
        return Some(RejectionVerdict::Session);
    }
    let walltime_ms = request.resources.max_walltime_ms;
    if let Some(verdict) = backend_verdict(
        &capability.groups,
        capability.max_walltime_ms,
        &capability.suitability,
        request.group_id,
        request.session,
        walltime_ms,
    ) {
        return Some(verdict);
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
