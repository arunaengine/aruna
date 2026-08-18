//! The pure execution planner.
//!
//! It hard-filters advertised targets against authenticated membership,
//! placement policy, and static capability, then ranks whatever survives by
//! directed transfer cost and stale ranking hints. It performs no I/O, decides
//! nothing about capacity the target owns, and seals every fact it used into a
//! plan digest.

mod cost;
mod digest;
mod eligibility;
mod facts;

pub use cost::{InputRoute, UNKNOWN_PERMILLE};
pub use digest::{PLAN_DIGEST_DOMAIN, plan_digest};
pub use eligibility::{PolicyVerdict, RejectionVerdict};
pub use facts::{
    InputHolder, MAX_INPUT_HOLDERS, MAX_PLAN_ALTERNATIVES, MAX_PLAN_CANDIDATES, MAX_PLAN_INPUTS,
    MAX_PLAN_REJECTIONS, PlanError, PlanRequest, ResolvedInput, TargetCandidate, TargetScore,
};

use crate::compute::ExecutionTargetId;
use crate::structs::RealmComputeConfig;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Where one pinned input comes from for the planned target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedInput {
    pub destination_key: String,
    pub version_id: Ulid,
    pub bytes: u64,
    /// `None` when the target already holds the exact compliant copy.
    pub source_node_id: Option<crate::NodeId>,
    pub transfer_ms: u64,
    pub known_link: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankedTarget {
    pub target: ExecutionTargetId,
    pub score: TargetScore,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedTarget {
    pub target: ExecutionTargetId,
    pub verdict: RejectionVerdict,
}

/// The target this plan launches on, with the facts sealed into `plan_digest`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    pub target: ExecutionTargetId,
    pub subject_digest: [u8; 32],
    pub subject_generation: u64,
    pub score: TargetScore,
    pub inputs: Vec<PlannedInput>,
    pub plan_digest: [u8; 32],
}

/// One planning round. Explanations are bounded; `omitted` counts what the
/// bound dropped so an audit never mistakes truncation for completeness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// `None` when no advertised target is currently eligible.
    pub selected: Option<Selection>,
    /// A later round may still find a target once missing policy documents or
    /// holders are observed. Always false once a target was selected.
    pub retryable: bool,
    pub alternatives: Vec<RankedTarget>,
    pub rejected: Vec<RejectedTarget>,
    pub omitted: u32,
}

/// Plans one execution over already resolved facts. Returns an error only when
/// the request itself is unusable; an empty or fully rejected candidate set is
/// a plan without a selection, not a failure.
pub fn plan_execution(
    request: &PlanRequest,
    compute: &RealmComputeConfig,
) -> Result<ExecutionPlan, PlanError> {
    compute.validate()?;
    let request = request.canonical()?;
    let links = cost::LinkIndex::new(compute);
    let mut ranked = Vec::new();
    let mut rejected = Vec::new();

    for candidate in &request.candidates {
        let target = candidate.capability.target(candidate.node_id);
        if let Some(verdict) = eligibility::screen(&request, candidate) {
            rejected.push(RejectedTarget { target, verdict });
            continue;
        }
        match routes(&request, candidate, &links) {
            Ok(routes) => {
                let score = cost::score(&routes, candidate, &links, request.now_ms);
                ranked.push((target, candidate, routes, score));
            }
            Err(verdict) => rejected.push(RejectedTarget { target, verdict }),
        }
    }
    // Score first, then node id bytes and executor kind: the total order of
    // plan section 8.4, so equal facts always choose the same target.
    ranked.sort_by(|left, right| {
        left.3
            .cmp(&right.3)
            .then_with(|| left.0.node_id.as_bytes().cmp(right.0.node_id.as_bytes()))
            .then_with(|| left.0.executor_kind.cmp(&right.0.executor_kind))
    });

    let retryable = ranked.is_empty()
        && rejected
            .iter()
            .any(|rejection| rejection.verdict.retryable());
    let omitted = rejected.len().saturating_sub(MAX_PLAN_REJECTIONS) as u32;
    rejected.truncate(MAX_PLAN_REJECTIONS);
    let alternatives = ranked
        .iter()
        .skip(1)
        .take(MAX_PLAN_ALTERNATIVES)
        .map(|(target, _, _, score)| RankedTarget {
            target: target.clone(),
            score: *score,
        })
        .collect();
    let selected = ranked
        .first()
        .map(|(target, candidate, routes, score)| Selection {
            target: target.clone(),
            subject_digest: candidate.capability.subject_digest,
            subject_generation: candidate.capability.subject.generation,
            score: *score,
            inputs: planned_inputs(&request, routes),
            plan_digest: digest::plan_digest(&request, candidate, routes, score),
        });
    Ok(ExecutionPlan {
        selected,
        retryable,
        alternatives,
        rejected,
        omitted,
    })
}

/// Cheapest legal route of every pinned input, or the input that has none.
fn routes(
    request: &PlanRequest,
    candidate: &TargetCandidate,
    links: &cost::LinkIndex<'_>,
) -> Result<Vec<InputRoute>, RejectionVerdict> {
    request
        .inputs
        .iter()
        .map(|input| {
            cost::route(input, request, candidate, links).ok_or_else(|| {
                RejectionVerdict::NoLegalSource {
                    destination_key: input.destination_key.clone(),
                }
            })
        })
        .collect()
}

fn planned_inputs(request: &PlanRequest, routes: &[InputRoute]) -> Vec<PlannedInput> {
    request
        .inputs
        .iter()
        .zip(routes)
        .map(|(input, route)| PlannedInput {
            destination_key: input.destination_key.clone(),
            version_id: input.version_id,
            bytes: input.bytes,
            source_node_id: route.source_node_id,
            transfer_ms: route.transfer_ms,
            known_link: route.known_link,
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests;
