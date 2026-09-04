//! The pure execution planner.
//!
//! It hard-filters every scanned advertisement against authenticated
//! membership, placement policy, and static capability, then ranks whatever
//! survives by directed transfer cost and stale ranking hints and keeps the
//! best of them. It performs no I/O, decides nothing about capacity the target
//! owns, and covers every value it used with a plan digest.

mod cost;
mod digest;
mod eligibility;
mod inputs;
mod rank;
mod scan;

pub use cost::{InputRoute, UNKNOWN_PERMILLE};
pub use digest::{PLAN_DIGEST_DOMAIN, plan_digest};
pub use eligibility::{PolicyVerdict, RejectionVerdict};
pub use inputs::{
    InputHolder, MAX_INPUT_HOLDERS, MAX_PLAN_ALTERNATIVES, MAX_PLAN_CANDIDATES, MAX_PLAN_INPUTS,
    MAX_PLAN_REJECTIONS, MAX_TARGET_SCAN, PlanError, PlanRequest, ResolvedInput, TargetCandidate,
    TargetScore,
};
pub use scan::Planner;

use crate::compute::ExecutionTargetId;
use crate::structs::RealmComputeConfig;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Where one pinned input comes from for the planned target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedInput {
    pub destination_key: String,
    pub version_id: Ulid,
    pub blake3: [u8; 32],
    pub bytes: u64,
    pub policies: Vec<crate::structs::PlacementPolicyRef>,
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

/// The target this plan launches on, with the values covered by `plan_digest`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    pub target: ExecutionTargetId,
    pub subject_digest: [u8; 32],
    pub subject_generation: u64,
    pub score: TargetScore,
    pub inputs: Vec<PlannedInput>,
    pub output_policies: Vec<crate::structs::PlacementPolicyRef>,
    pub plan_digest: [u8; 32],
}

/// One planning round. Explanations are bounded; `omitted` counts what the
/// bound dropped so an audit never mistakes truncation for completeness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// `None` when no scanned target is currently eligible.
    pub selected: Option<Selection>,
    /// A later round may still find a target once missing policy documents or
    /// holders are observed, or once an advertisement this round could not read
    /// is readable. Always false once a target was selected.
    pub retryable: bool,
    pub alternatives: Vec<RankedTarget>,
    pub rejected: Vec<RejectedTarget>,
    pub omitted: u32,
}

/// How one scanned target ended up in a planning round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CandidateVerdict {
    Selected,
    Ranked,
    Rejected,
}

impl CandidateVerdict {
    /// Stable machine-readable name for API payloads.
    pub fn name(&self) -> &'static str {
        match self {
            CandidateVerdict::Selected => "selected",
            CandidateVerdict::Ranked => "ranked",
            CandidateVerdict::Rejected => "rejected",
        }
    }
}

/// One target of a planning round as an external report shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanCandidate {
    pub node_id: crate::NodeId,
    pub executor_kind: Option<String>,
    pub verdict: CandidateVerdict,
    /// Place among the alternatives, counted from one. Absent for the selected
    /// target and for a rejected one.
    pub rank: Option<u32>,
    /// Why the target was rejected, in plain words.
    pub reason: Option<String>,
}

impl ExecutionPlan {
    /// Every target of this round for a report: the selection first, then the
    /// alternatives in rank order, then the rejections with their reason.
    pub fn candidates(&self) -> Vec<PlanCandidate> {
        let selected = self.selected.iter().map(|selection| PlanCandidate {
            node_id: selection.target.node_id,
            executor_kind: Some(selection.target.executor_kind.clone()),
            verdict: CandidateVerdict::Selected,
            rank: None,
            reason: None,
        });
        let ranked = self
            .alternatives
            .iter()
            .enumerate()
            .map(|(index, ranked)| PlanCandidate {
                node_id: ranked.target.node_id,
                executor_kind: Some(ranked.target.executor_kind.clone()),
                verdict: CandidateVerdict::Ranked,
                rank: Some(index as u32 + 1),
                reason: None,
            });
        let rejected = self.rejected.iter().map(|rejected| PlanCandidate {
            node_id: rejected.target.node_id,
            executor_kind: Some(rejected.target.executor_kind.clone()),
            verdict: CandidateVerdict::Rejected,
            rank: None,
            reason: Some(rejected.verdict.reason()),
        });
        selected.chain(ranked).chain(rejected).collect()
    }
}

/// Plans one execution over already resolved inputs and one complete candidate
/// set, paging it for the caller. Returns an error only when the request itself
/// is unusable; an empty or fully rejected scan is a plan without a selection,
/// not a failure, and stays retryable while a rejection may still resolve
/// itself. A caller that discovers advertisements incrementally drives
/// [`Planner`] itself.
pub fn plan_execution(
    request: &PlanRequest,
    candidates: &[TargetCandidate],
    compute: &RealmComputeConfig,
) -> Result<ExecutionPlan, PlanError> {
    let mut planner = Planner::new(request, compute)?;
    let mut ordered = candidates.to_vec();
    ordered.sort_unstable_by(|left, right| {
        inputs::target_order(left).cmp(&inputs::target_order(right))
    });
    for page in ordered.chunks(MAX_TARGET_SCAN) {
        planner.rank_page(page)?;
    }
    Ok(planner.finish(false))
}

fn planned_inputs(request: &PlanRequest, routes: &[InputRoute]) -> Vec<PlannedInput> {
    request
        .inputs
        .iter()
        .zip(routes)
        .map(|(input, route)| PlannedInput {
            destination_key: input.destination_key.clone(),
            version_id: input.version_id,
            blake3: input.blake3,
            bytes: input.bytes,
            policies: input.policies.clone(),
            source_node_id: route.source_node_id,
            transfer_ms: route.transfer_ms,
            known_link: route.known_link,
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests;
