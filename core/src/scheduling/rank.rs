//! Screening and ranking of one advertised target.
//!
//! Every screened advertisement is routed and scored. Only the best
//! [`MAX_PLAN_CANDIDATES`] of the survivors are kept, so a drained,
//! unauthorized, or unfit prefix can never hide a legal target behind the
//! ranking bound.

use crate::compute::ExecutionTargetId;
use crate::scheduling::cost::{self, InputRoute, LinkIndex};
use crate::scheduling::eligibility::{self, RejectionVerdict};
use crate::scheduling::inputs::{MAX_PLAN_CANDIDATES, PlanRequest, TargetCandidate, TargetScore};

/// What one eligible advertisement costs: how each pinned input reaches it and
/// the score that ranks it.
pub struct Evaluation {
    pub routes: Vec<InputRoute>,
    pub score: TargetScore,
}

/// One kept target with the routes and score the plan stores. It owns its
/// candidate because the page the advertisement arrived on is dropped once the
/// page is ranked.
pub struct ScoredTarget {
    pub target: ExecutionTargetId,
    pub candidate: TargetCandidate,
    pub routes: Vec<InputRoute>,
    pub score: TargetScore,
}

impl ScoredTarget {
    /// Ascending rank: score first, then node id bytes and executor kind, the
    /// total order of plan section 8.4.
    fn order(&self) -> (&TargetScore, &[u8; 32], &str) {
        (
            &self.score,
            self.target.node_id.as_bytes(),
            self.target.executor_kind.as_str(),
        )
    }
}

/// Screens, routes, and scores one advertisement. The only place these rules
/// are applied, so a single-target recheck cannot drift from the planner.
pub fn evaluate(
    request: &PlanRequest,
    candidate: &TargetCandidate,
    links: &LinkIndex<'_>,
) -> Result<Evaluation, RejectionVerdict> {
    if let Some(verdict) = eligibility::screen(request, candidate) {
        return Err(verdict);
    }
    let routes = request
        .inputs
        .iter()
        .map(|input| {
            cost::route(input, request, candidate, links).ok_or_else(|| {
                RejectionVerdict::NoLegalSource {
                    destination_key: input.destination_key.clone(),
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let score = cost::score(&routes, candidate, links, request.now_ms);
    Ok(Evaluation { routes, score })
}

/// Inserts one evaluated target into the bounded ranking, cloning the candidate
/// only where the bound keeps it. The order is total, so the kept set never
/// depends on the order or the page the advertisements arrived in.
pub fn keep(ranked: &mut Vec<ScoredTarget>, candidate: &TargetCandidate, evaluation: Evaluation) {
    let order = (
        &evaluation.score,
        candidate.node_id.as_bytes(),
        candidate.capability.kind.as_str(),
    );
    let position = ranked.partition_point(|kept| kept.order() < order);
    if position >= MAX_PLAN_CANDIDATES {
        return;
    }
    ranked.insert(
        position,
        ScoredTarget {
            target: candidate.capability.target(candidate.node_id),
            candidate: candidate.clone(),
            routes: evaluation.routes,
            score: evaluation.score,
        },
    );
    ranked.truncate(MAX_PLAN_CANDIDATES);
}
