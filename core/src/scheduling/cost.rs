//! Directed network cost and ranking. An unknown fact makes a target rank
//! worse; it never becomes capacity the target did not advertise.

use crate::NodeId;
use crate::compute::ExecutorCapability;
use crate::compute::ResourceEnvelope;
use crate::scheduling::eligibility::allows;
use crate::scheduling::facts::{PlanRequest, ResolvedInput, TargetCandidate, TargetScore};
use crate::structs::{DEFAULT_LOCATION, PlacementSubject, RealmComputeConfig};
use std::collections::BTreeMap;

/// Rank value of an unknown or stale ranking fact: as bad as a fully loaded
/// site, and still only a rank.
pub const UNKNOWN_PERMILLE: u32 = 1_000;

/// How one input reaches one candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputRoute {
    /// `None` when the candidate itself holds the exact compliant copy.
    pub source_node_id: Option<NodeId>,
    pub transfer_ms: u64,
    pub transfer_bytes: u64,
    pub known_link: bool,
}

impl InputRoute {
    fn local() -> Self {
        Self {
            source_node_id: None,
            transfer_ms: 0,
            transfer_bytes: 0,
            known_link: true,
        }
    }
}

/// Directed bandwidth index built once per plan.
pub struct LinkIndex<'a> {
    links: BTreeMap<(&'a str, &'a str), u64>,
    pessimistic: u64,
    stale_after_ms: u64,
}

impl<'a> LinkIndex<'a> {
    pub fn new(config: &'a RealmComputeConfig) -> Self {
        Self {
            links: config
                .links
                .iter()
                .map(|link| {
                    (
                        (link.from.trim(), link.to.trim()),
                        link.bandwidth_bytes_per_sec,
                    )
                })
                .collect(),
            pessimistic: config.pessimistic_bandwidth_bytes_per_sec.max(1),
            stale_after_ms: config.availability_stale_after_ms,
        }
    }

    /// Milliseconds to move `bytes` from one location to another, plus whether
    /// the link was configured at all.
    fn transfer_ms(&self, from: &str, to: &str, bytes: u64) -> (u64, bool) {
        let known = self.links.get(&(from, to)).copied();
        let bandwidth = known.unwrap_or(self.pessimistic).max(1);
        let millis = (u128::from(bytes) * 1_000).div_ceil(u128::from(bandwidth));
        (millis.min(u128::from(u64::MAX)) as u64, known.is_some())
    }
}

/// Location a subject sits in, in the same normalized form policies match on.
fn location(subject: &PlacementSubject) -> &str {
    match subject.location.trim() {
        "" => DEFAULT_LOCATION,
        location => location,
    }
}

/// Cheapest legal route of one input to one candidate. `None` means the input
/// has no compliant local copy and no currently known legal source, so the
/// candidate is temporarily unavailable rather than cheap.
pub fn route(
    input: &ResolvedInput,
    request: &PlanRequest,
    candidate: &TargetCandidate,
    links: &LinkIndex<'_>,
) -> Option<InputRoute> {
    let compliant =
        |subject: &PlacementSubject| allows(&input.policies, request, subject).is_none();
    let destination = location(&candidate.capability.subject);
    let mut best: Option<InputRoute> = None;
    for holder in &input.holders {
        if !compliant(&holder.subject) {
            continue;
        }
        if holder.node_id == candidate.node_id {
            return Some(InputRoute::local());
        }
        let (transfer_ms, known_link) =
            links.transfer_ms(location(&holder.subject), destination, input.bytes);
        let route = InputRoute {
            source_node_id: Some(holder.node_id),
            transfer_ms,
            transfer_bytes: input.bytes,
            known_link,
        };
        // Holders are canonically ordered, so the first cheapest one wins and
        // an equal-cost tie never depends on discovery order.
        if best
            .as_ref()
            .is_none_or(|current| route.transfer_ms < current.transfer_ms)
        {
            best = Some(route);
        }
    }
    best
}

/// Ascending score of one candidate over its routed inputs.
pub fn score(
    routes: &[InputRoute],
    candidate: &TargetCandidate,
    links: &LinkIndex<'_>,
    now_ms: u64,
) -> TargetScore {
    let mut score = TargetScore {
        availability_pressure_permille: pressure(
            &candidate.capability,
            now_ms,
            links.stale_after_ms,
        ),
        node_load_permille: candidate.load_permille.unwrap_or(UNKNOWN_PERMILLE),
        compute_priority_inverse: u32::MAX - candidate.capability.compute_priority,
        ..TargetScore::default()
    };
    for route in routes {
        score.estimated_transfer_ms = score
            .estimated_transfer_ms
            .saturating_add(route.transfer_ms);
        score.transfer_bytes = score.transfer_bytes.saturating_add(route.transfer_bytes);
        if !route.known_link {
            score.unknown_link_count = score.unknown_link_count.saturating_add(1);
        }
    }
    score
}

/// Highest used ratio across the dimensions where both a free sample and a
/// static ceiling are known. Unknown or stale telemetry ranks worst.
fn pressure(capability: &ExecutorCapability, now_ms: u64, stale_after_ms: u64) -> u32 {
    let Some(availability) = capability.availability else {
        return UNKNOWN_PERMILLE;
    };
    if now_ms.saturating_sub(availability.observed_at_ms) > stale_after_ms {
        return UNKNOWN_PERMILLE;
    }
    let ExecutorCapability { limits, .. } = capability;
    let ResourceEnvelope {
        max_cpu_cores,
        max_ram_bytes,
        max_disk_bytes,
        max_concurrent,
    } = *limits;
    let dimensions = [
        used_permille(
            availability.free_cpu_cores.map(u64::from),
            max_cpu_cores.map(u64::from),
        ),
        used_permille(availability.free_ram_bytes, max_ram_bytes),
        used_permille(availability.free_disk_bytes, max_disk_bytes),
        max_concurrent.map(|max| {
            used_permille(
                Some(u64::from(max).saturating_sub(u64::from(availability.active_executions))),
                Some(u64::from(max)),
            )
            .unwrap_or(UNKNOWN_PERMILLE)
        }),
    ];
    dimensions
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(UNKNOWN_PERMILLE)
}

/// `None` when either half of the ratio is unmeasured, so an unknown dimension
/// neither raises nor lowers the pressure of a known one.
fn used_permille(free: Option<u64>, ceiling: Option<u64>) -> Option<u32> {
    let (free, ceiling) = (free?, ceiling?);
    if ceiling == 0 {
        return Some(UNKNOWN_PERMILLE);
    }
    let free = u128::from(free.min(ceiling)) * 1_000 / u128::from(ceiling);
    Some(UNKNOWN_PERMILLE - free as u32)
}
