//! The paged advertisement scan.
//!
//! Discovery hands the planner one page of advertisements at a time. Each page
//! is screened and ranked as it arrives and only the bounded best of everything
//! seen so far is retained, so one planning operation reaches the end of the
//! realm's advertisements and stores its plan after the last page instead of at
//! a scan bound.

use std::cmp::Ordering;

use crate::compute::ExecutionTargetId;
use crate::scheduling::cost::LinkIndex;
use crate::scheduling::eligibility::RejectionVerdict;
use crate::scheduling::inputs::{
    MAX_PLAN_ALTERNATIVES, MAX_PLAN_REJECTIONS, MAX_TARGET_SCAN, PlanError, PlanRequest,
    TargetCandidate, target_order,
};
use crate::scheduling::rank::{self, ScoredTarget};
use crate::scheduling::{ExecutionPlan, RankedTarget, RejectedTarget, Selection};
use crate::structs::RealmComputeConfig;

/// One planning operation over a scan that arrives in pages. The plan is only
/// final once every page has been ranked.
pub struct Planner<'a> {
    request: PlanRequest,
    links: LinkIndex<'a>,
    ranked: Vec<ScoredTarget>,
    rejected: Vec<RejectedTarget>,
    /// Rejections the audit bound dropped.
    omitted: u32,
    /// A rejection that later values may resolve, kept for audit or not.
    retryable_rejection: bool,
    cursor: Option<ExecutionTargetId>,
    scanned: u64,
    pages: u32,
}

impl<'a> Planner<'a> {
    /// Validates and canonicalizes the request values once, before any
    /// advertisement is screened.
    pub fn new(request: &PlanRequest, compute: &'a RealmComputeConfig) -> Result<Self, PlanError> {
        compute.validate()?;
        Ok(Self {
            request: request.canonical()?,
            links: LinkIndex::new(compute),
            ranked: Vec::new(),
            rejected: Vec::new(),
            omitted: 0,
            retryable_rejection: false,
            cursor: None,
            scanned: 0,
            pages: 0,
        })
    }

    /// Screens, routes, and ranks one page of at most [`MAX_TARGET_SCAN`]
    /// advertisements. The page must be in canonical target order and start
    /// strictly after the cursor, which is what proves the scan repeats and
    /// skips nothing. An empty page changes nothing.
    pub fn rank_page(&mut self, page: &[TargetCandidate]) -> Result<(), PlanError> {
        if page.len() > MAX_TARGET_SCAN {
            return Err(PlanError::ScanCount);
        }
        let Some(last) = page.last() else {
            return Ok(());
        };
        self.continues(page)?;
        for candidate in page {
            match rank::evaluate(&self.request, candidate, &self.links) {
                Ok(evaluation) => rank::keep(&mut self.ranked, candidate, evaluation),
                Err(verdict) => self.reject(candidate, verdict),
            }
        }
        self.cursor = Some(last.capability.target(last.node_id));
        self.scanned += page.len() as u64;
        self.pages += 1;
        Ok(())
    }

    /// The plan for every page ranked so far. `incomplete` says the round could
    /// not read every advertisement, so a plan without a selection is a
    /// continuation rather than a conclusive refusal.
    pub fn finish(self, incomplete: bool) -> ExecutionPlan {
        // Nothing eligible yet may still become eligible: an advertisement this
        // round could not read, or a rejection later values resolve.
        let retryable = self.ranked.is_empty() && (incomplete || self.retryable_rejection);
        let alternatives = self
            .ranked
            .iter()
            .skip(1)
            .take(MAX_PLAN_ALTERNATIVES)
            .map(|scored| RankedTarget {
                target: scored.target.clone(),
                score: scored.score,
            })
            .collect();
        // The kept order is total and a repeated entry is refused, so the same
        // advertisement set yields the same plan whatever the page boundaries.
        let selected = self.ranked.first().map(|scored| Selection {
            target: scored.target.clone(),
            subject_digest: scored.candidate.capability.subject_digest,
            subject_generation: scored.candidate.capability.subject.generation,
            score: scored.score,
            inputs: super::planned_inputs(&self.request, &scored.routes),
            output_policies: self.request.output_policies.clone(),
            plan_digest: super::digest::plan_digest(
                &self.request,
                &scored.candidate,
                &scored.routes,
                &scored.score,
            ),
        });
        ExecutionPlan {
            selected,
            retryable,
            alternatives,
            rejected: self.rejected,
            omitted: self.omitted,
        }
    }

    /// Advertisements screened so far.
    pub fn scanned(&self) -> u64 {
        self.scanned
    }

    /// Pages ranked so far. An empty page is not one of them.
    pub fn pages(&self) -> u32 {
        self.pages
    }

    /// Last advertisement screened; the next page must continue past it.
    pub fn cursor(&self) -> Option<&ExecutionTargetId> {
        self.cursor.as_ref()
    }

    /// Every entry of the page must be strictly greater than the cursor and
    /// than the entry before it.
    fn continues(&self, page: &[TargetCandidate]) -> Result<(), PlanError> {
        let mut previous = self
            .cursor
            .as_ref()
            .map(|cursor| (cursor.node_id.as_bytes(), cursor.executor_kind.as_str()));
        for candidate in page {
            let order = target_order(candidate);
            match previous.map(|previous| previous.cmp(&order)) {
                Some(Ordering::Equal) => {
                    return Err(PlanError::DuplicateTarget {
                        node_id: candidate.node_id,
                        kind: candidate.capability.kind.clone(),
                    });
                }
                Some(Ordering::Greater) => {
                    return Err(PlanError::PageOrder {
                        node_id: candidate.node_id,
                        kind: candidate.capability.kind.clone(),
                    });
                }
                _ => previous = Some(order),
            }
        }
        Ok(())
    }

    fn reject(&mut self, candidate: &TargetCandidate, verdict: RejectionVerdict) {
        self.retryable_rejection |= verdict.retryable();
        if self.rejected.len() >= MAX_PLAN_REJECTIONS {
            self.omitted = self.omitted.saturating_add(1);
            return;
        }
        self.rejected.push(RejectedTarget {
            target: candidate.capability.target(candidate.node_id),
            verdict,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduling::tests::{candidate, config, node, request};

    fn planner(compute: &RealmComputeConfig) -> Planner<'_> {
        Planner::new(&request(Vec::new()), compute).expect("request is well formed")
    }

    #[test]
    fn rejects_page_bound() {
        // One page is bounded even though the whole scan is not.
        let compute = config(Vec::new());
        let page = vec![candidate(node(2), "docker"); MAX_TARGET_SCAN + 1];
        assert_eq!(
            planner(&compute).rank_page(&page),
            Err(PlanError::ScanCount)
        );
    }

    #[test]
    fn rejects_page_order() {
        // A page that repeats or steps back over the cursor would skip or
        // double-count advertisements, so it is refused instead of ranked.
        let compute = config(Vec::new());
        let (low, high) = match node(2).as_bytes() < node(3).as_bytes() {
            true => (node(2), node(3)),
            false => (node(3), node(2)),
        };
        let mut planner = planner(&compute);
        planner
            .rank_page(&[candidate(low, "docker"), candidate(high, "docker")])
            .expect("the page is ordered");

        assert!(matches!(
            planner.rank_page(&[candidate(high, "docker")]),
            Err(PlanError::DuplicateTarget { .. })
        ));
        assert!(matches!(
            planner.rank_page(&[candidate(low, "docker")]),
            Err(PlanError::PageOrder { .. })
        ));
        assert!(matches!(
            planner.rank_page(&[candidate(low, "docker"), candidate(low, "docker")]),
            Err(PlanError::PageOrder { .. })
        ));
        assert_eq!(planner.scanned(), 2);
        assert_eq!(planner.pages(), 1);
    }

    #[test]
    fn empty_page_noop() {
        // A flush with nothing buffered must not count as a page or move the
        // cursor the next page is checked against.
        let compute = config(Vec::new());
        let mut planner = planner(&compute);
        planner.rank_page(&[]).expect("an empty page is allowed");
        assert_eq!(planner.pages(), 0);
        assert!(planner.cursor().is_none());

        planner
            .rank_page(&[candidate(node(2), "docker")])
            .expect("the page is ordered");
        planner.rank_page(&[]).expect("an empty page is allowed");

        assert_eq!(planner.pages(), 1);
        assert_eq!(planner.scanned(), 1);
        assert_eq!(
            planner.cursor().expect("a page was ranked").node_id,
            node(2)
        );
    }
}
