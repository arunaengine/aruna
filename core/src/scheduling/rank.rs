//! Screening and ranking of one advertisement scan.
//!
//! Every scanned advertisement is screened and, if it survives, routed and
//! scored. Only the best [`MAX_PLAN_CANDIDATES`] of the survivors are kept, so
//! a drained, unauthorized, or unfit prefix can never hide a legal target
//! behind the ranking bound.

use crate::compute::ExecutionTargetId;
use crate::scheduling::RejectedTarget;
use crate::scheduling::cost::{self, InputRoute, LinkIndex};
use crate::scheduling::eligibility::{self, RejectionVerdict};
use crate::scheduling::facts::{MAX_PLAN_CANDIDATES, PlanRequest, TargetCandidate, TargetScore};

/// One eligible target with the routes and score the plan seals.
pub struct ScoredTarget<'a> {
    pub target: ExecutionTargetId,
    pub candidate: &'a TargetCandidate,
    pub routes: Vec<InputRoute>,
    pub score: TargetScore,
}

/// What one scan decided: the bounded ranking and every rejection it explains.
pub struct Ranking<'a> {
    pub ranked: Vec<ScoredTarget<'a>>,
    pub rejected: Vec<RejectedTarget>,
}

/// Screens, routes, and scores one advertisement. The only place these rules
/// are applied, so a single-target recheck cannot drift from the planner.
pub fn evaluate<'a>(
    request: &PlanRequest,
    candidate: &'a TargetCandidate,
    links: &LinkIndex<'_>,
) -> Result<ScoredTarget<'a>, RejectionVerdict> {
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
    Ok(ScoredTarget {
        target: candidate.capability.target(candidate.node_id),
        candidate,
        routes,
        score,
    })
}

/// Screens the whole scan and keeps the best eligible targets of it.
pub fn rank<'a>(request: &'a PlanRequest, links: &LinkIndex<'_>) -> Ranking<'a> {
    let mut ranking = Ranking {
        ranked: Vec::new(),
        rejected: Vec::new(),
    };
    for candidate in &request.candidates {
        match evaluate(request, candidate, links) {
            Ok(scored) => keep(&mut ranking.ranked, scored),
            Err(verdict) => ranking.rejected.push(RejectedTarget {
                target: candidate.capability.target(candidate.node_id),
                verdict,
            }),
        }
    }
    ranking
}

/// Inserts one scored target into the bounded ranking, dropping whatever falls
/// past the bound. The order is total, so the kept set never depends on the
/// order the advertisements arrived in.
fn keep<'a>(ranked: &mut Vec<ScoredTarget<'a>>, scored: ScoredTarget<'a>) {
    let position = ranked.partition_point(|kept| order(kept) < order(&scored));
    if position >= MAX_PLAN_CANDIDATES {
        return;
    }
    ranked.insert(position, scored);
    ranked.truncate(MAX_PLAN_CANDIDATES);
}

/// Ascending rank: score first, then node id bytes and executor kind, the total
/// order of plan section 8.4.
fn order<'a>(scored: &'a ScoredTarget<'_>) -> (&'a TargetScore, &'a [u8; 32], &'a str) {
    (
        &scored.score,
        scored.target.node_id.as_bytes(),
        scored.target.executor_kind.as_str(),
    )
}
