//! Compute quota contracts: the bounded snapshots a node publishes, the
//! standing quota an operator configures, and the pure admission evaluator.
//!
//! Two controls stay apart on purpose. Logical admission demand is group-scoped,
//! replicated, and approximate across partitions; physical reservation is
//! node-local and exact. Neither ever cancels work: a view that converges above
//! a cap only stops new logical admissions.

use crate::compute::{ExecutorAvailability, ResourceEnvelope};
use crate::structs::{AdvertisementEpoch, EffectiveResources, JobId, SubmissionId};
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

/// Groups one demand snapshot reports.
pub const MAX_DEMAND_GROUPS: usize = 32;
/// Request families one demand snapshot reports across all its groups. The
/// bound keeps one advertisement far below the 64 KiB realm message size.
pub const MAX_DEMAND_FAMILIES: usize = 128;
/// Unresolved executions one departure report names.
pub const MAX_UNRESOLVED_EXECUTIONS: usize = 256;

/// Why a published snapshot is not trustworthy.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum SnapshotError {
    #[error("a demand snapshot reports at most {MAX_DEMAND_GROUPS} groups")]
    GroupCount,
    #[error("a demand snapshot reports at most {MAX_DEMAND_FAMILIES} families")]
    FamilyCount,
    #[error("snapshot groups must be ordered by group id and unique")]
    GroupOrder,
    #[error("snapshot families must be ordered by family identity and unique")]
    FamilyOrder,
    #[error("a snapshot may not claim an epoch newer than the advertisement carrying it")]
    EpochAhead,
}

/// A count of jobs or executions plus the resources they hold. Used for both
/// controls; the counted unit differs, the arithmetic does not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceTotals {
    pub count: u32,
    pub cpu_cores: u64,
    pub ram_bytes: u64,
    pub disk_bytes: u64,
}

impl ResourceTotals {
    /// Adds one request's ceilings. Saturating: an overflowing total stays at
    /// the maximum, which denies rather than wraps into a small number.
    pub fn add(&mut self, resources: &EffectiveResources) {
        self.count = self.count.saturating_add(1);
        self.cpu_cores = self
            .cpu_cores
            .saturating_add(u64::from(resources.cpu_cores));
        self.ram_bytes = self.ram_bytes.saturating_add(resources.ram_bytes);
        self.disk_bytes = self.disk_bytes.saturating_add(resources.disk_bytes);
    }
}

/// One nonterminal admitted request family. The identity is what lets replicas
/// count a family once, however many holders admitted or project an alias of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemandFamily {
    pub submission_id: SubmissionId,
    pub request_digest: [u8; 32],
    pub resources: EffectiveResources,
}

impl DemandFamily {
    fn identity(&self) -> (&[u8; 32], &[u8; 32]) {
        (&self.submission_id.0, &self.request_digest)
    }
}

/// The demand one group holds on one publisher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemandGroup {
    pub group_id: GroupId,
    pub families: Vec<DemandFamily>,
    /// This group holds more nonterminal families than the snapshot names, so
    /// the merged view understates it instead of guessing. It is set per group:
    /// a busy group never marks a quiet one.
    pub truncated: bool,
}

/// Bounded logical admission demand this node observes. Separate from
/// [`ComputeReservationSnapshot`]: logical demand and physical reservation are
/// different controls and are never summed together.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeDemandSnapshot {
    pub epoch: AdvertisementEpoch,
    pub groups: Vec<DemandGroup>,
    /// Whole groups this snapshot could not name. A group it does name is
    /// complete unless that group carries its own truncation.
    pub truncated: bool,
}

impl ComputeDemandSnapshot {
    /// Bounds and canonical order. Duplicate identities are rejected rather
    /// than deduplicated, so a publisher cannot inflate its own demand.
    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.groups.len() > MAX_DEMAND_GROUPS {
            return Err(SnapshotError::GroupCount);
        }
        let mut families = 0usize;
        for pair in self.groups.windows(2) {
            if pair[0].group_id >= pair[1].group_id {
                return Err(SnapshotError::GroupOrder);
            }
        }
        for group in &self.groups {
            families = families.saturating_add(group.families.len());
            for pair in group.families.windows(2) {
                if pair[0].identity() >= pair[1].identity() {
                    return Err(SnapshotError::FamilyOrder);
                }
            }
        }
        match families > MAX_DEMAND_FAMILIES {
            true => Err(SnapshotError::FamilyCount),
            false => Ok(()),
        }
    }

    pub fn group(&self, group_id: &GroupId) -> Option<&DemandGroup> {
        self.groups.iter().find(|group| &group.group_id == group_id)
    }
}

/// Exact local physical reservations this node currently holds, summed over the
/// durable per-execution reservation rows. Duplicate executions of one logical
/// job are reserved and reported independently.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeReservationSnapshot {
    pub epoch: AdvertisementEpoch,
    pub reserved: ResourceTotals,
}

/// One durable local reservation of exact capacity, keyed by ExecutionId. This
/// node writes it before starting work and releases it on a terminal fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobReservationRecord {
    pub execution_id: Ulid,
    /// Mutable row of this exact physical execution.
    pub job_id: JobId,
    /// Stable external alias of the logical request family.
    pub logical_job_id: JobId,
    pub resources: EffectiveResources,
    pub created_at_ms: u64,
    /// Execution site the receipt sealed. The local attempt refuses to start
    /// when this node no longer advertises exactly this subject.
    pub subject_generation: u64,
    pub subject_digest: [u8; 32],
}

/// What a departing node could not resolve before leaving. Recorded durably so
/// removal never has to wait for it and audit keeps the last known facts.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeDepartureReport {
    pub departed_at_ms: u64,
    pub membership_generation: u64,
    /// Executions still reserved here. They are unresolved, never terminal: a
    /// departing node may not declare a remote-observed execution finished.
    pub unresolved: Vec<Ulid>,
    pub truncated: bool,
}

/// Merged nonterminal demand of one group across every observed snapshot.
/// A family counts once however many publishers report it; a truncated snapshot
/// contributes what it reports and is reported as an understated view.
pub fn merge_demand<'a>(
    snapshots: impl IntoIterator<Item = &'a ComputeDemandSnapshot>,
    group_id: &GroupId,
) -> (ResourceTotals, bool) {
    let mut seen: BTreeSet<([u8; 32], [u8; 32])> = BTreeSet::new();
    let mut totals = ResourceTotals::default();
    let mut truncated = false;
    for snapshot in snapshots {
        let Some(group) = snapshot.group(group_id) else {
            // Only a snapshot that dropped whole groups may be hiding this one.
            truncated |= snapshot.truncated;
            continue;
        };
        truncated |= group.truncated;
        for family in &group.families {
            if seen.insert((family.submission_id.0, family.request_digest)) {
                totals.add(&family.resources);
            }
        }
    }
    (totals, truncated)
}

/// Bounds one publisher's own demand to what a snapshot may report: at most
/// [`MAX_DEMAND_GROUPS`] groups sharing [`MAX_DEMAND_FAMILIES`] families. The
/// budget is handed out one family per group per round, so a busy group cannot
/// truncate a quiet one, and only a group whose own families were cut is
/// flagged. The returned bool reports groups the snapshot names nowhere.
pub fn bound_demand(groups: &mut Vec<DemandGroup>) -> bool {
    let dropped = groups.len() > MAX_DEMAND_GROUPS;
    groups.truncate(MAX_DEMAND_GROUPS);
    let counts: Vec<usize> = groups.iter().map(|group| group.families.len()).collect();
    for (group, share) in groups
        .iter_mut()
        .zip(fair_shares(&counts, MAX_DEMAND_FAMILIES))
    {
        group.truncated |= group.families.len() > share;
        group.families.truncate(share);
    }
    dropped
}

/// Water-fills `budget` over `counts`: every round hands one unit to each entry
/// that still wants one.
fn fair_shares(counts: &[usize], budget: usize) -> Vec<usize> {
    let mut shares = vec![0usize; counts.len()];
    let mut left = budget;
    let mut filling = true;
    while left > 0 && filling {
        filling = false;
        for (share, count) in shares.iter_mut().zip(counts) {
            if left == 0 {
                break;
            }
            if *share < *count {
                *share += 1;
                left -= 1;
                filling = true;
            }
        }
    }
    shares
}

/// Ranking-only availability of one backend: its static ceilings minus what this
/// node has reserved, observed now. Never a hard capacity fact: exact admission
/// is the target-side reservation.
pub fn availability(
    limits: &ResourceEnvelope,
    reserved: &ResourceTotals,
    observed_at_ms: u64,
) -> ExecutorAvailability {
    ExecutorAvailability {
        free_cpu_cores: limits.max_cpu_cores.map(|max| {
            u32::try_from(u64::from(max).saturating_sub(reserved.cpu_cores)).unwrap_or(u32::MAX)
        }),
        free_ram_bytes: limits
            .max_ram_bytes
            .map(|max| max.saturating_sub(reserved.ram_bytes)),
        free_disk_bytes: limits
            .max_disk_bytes
            .map(|max| max.saturating_sub(reserved.disk_bytes)),
        active_executions: reserved.count,
        observed_at_ms,
    }
}

/// Standing compute quota of one group. Every dimension is optional and an
/// unconfigured dimension is unbounded, never zero.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeQuota {
    pub max_jobs: Option<u32>,
    pub max_cpu_cores: Option<u64>,
    pub max_ram_bytes: Option<u64>,
    pub max_disk_bytes: Option<u64>,
    pub max_job_cpu_cores: Option<u32>,
    pub max_job_ram_bytes: Option<u64>,
    pub max_job_disk_bytes: Option<u64>,
    /// Per-job walltime bound sealed into the request, not an assertion about
    /// any clock: nothing is cancelled because this quota later changed.
    pub max_job_walltime_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaScope {
    /// The request exceeds what one job may ever ask for here.
    Job,
    /// The group's observed nonterminal demand plus this request exceeds a cap.
    Group,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaDimension {
    Jobs,
    CpuCores,
    RamBytes,
    DiskBytes,
    WalltimeMs,
}

/// Why a new logical admission was refused. It carries the exact numbers the
/// decision used so the refusal can be explained without re-deriving it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error(
    "{scope:?} {dimension:?} quota exceeded: {requested} requested, {observed} in use, {limit} allowed"
)]
pub struct QuotaDenied {
    pub scope: QuotaScope,
    pub dimension: QuotaDimension,
    pub observed: u64,
    pub requested: u64,
    pub limit: u64,
}

/// Whether one new logical admission fits the standing quota given the locally
/// observed demand of its group.
///
/// `view` is the merged replicated demand of admitted-but-nonterminal work, so
/// it is exact for this node and approximate across partitions. Deciding
/// against a stale view may overshoot a cap; that is the accepted bound. The
/// only outcome of a converged overshoot is refusing new admissions: nothing
/// already admitted, queued, preparing, or running is ever revoked here.
pub fn admits(
    view: &ResourceTotals,
    quota: &ComputeQuota,
    request: &EffectiveResources,
) -> Result<(), QuotaDenied> {
    let cpu = u64::from(request.cpu_cores);
    per_job(
        quota.max_job_cpu_cores.map(u64::from),
        cpu,
        QuotaDimension::CpuCores,
    )?;
    per_job(
        quota.max_job_ram_bytes,
        request.ram_bytes,
        QuotaDimension::RamBytes,
    )?;
    per_job(
        quota.max_job_disk_bytes,
        request.disk_bytes,
        QuotaDimension::DiskBytes,
    )?;
    per_job(
        quota.max_job_walltime_ms,
        request.max_walltime_ms,
        QuotaDimension::WalltimeMs,
    )?;
    group(
        quota.max_jobs.map(u64::from),
        u64::from(view.count),
        1,
        QuotaDimension::Jobs,
    )?;
    group(
        quota.max_cpu_cores,
        view.cpu_cores,
        cpu,
        QuotaDimension::CpuCores,
    )?;
    group(
        quota.max_ram_bytes,
        view.ram_bytes,
        request.ram_bytes,
        QuotaDimension::RamBytes,
    )?;
    group(
        quota.max_disk_bytes,
        view.disk_bytes,
        request.disk_bytes,
        QuotaDimension::DiskBytes,
    )
}

/// Refusal of a new admission whose group demand view is understated: nothing
/// below the cap can be shown, so the group is treated as standing at its first
/// configured group cap. `None` when the quota caps nothing group-scoped, since
/// such a quota never reads the demand view at all.
pub fn understated_denial(
    quota: &ComputeQuota,
    request: &EffectiveResources,
) -> Option<QuotaDenied> {
    let (dimension, limit, requested) = quota
        .max_jobs
        .map(|limit| (QuotaDimension::Jobs, u64::from(limit), 1))
        .or_else(|| {
            quota.max_cpu_cores.map(|limit| {
                (
                    QuotaDimension::CpuCores,
                    limit,
                    u64::from(request.cpu_cores),
                )
            })
        })
        .or_else(|| {
            quota
                .max_ram_bytes
                .map(|limit| (QuotaDimension::RamBytes, limit, request.ram_bytes))
        })
        .or_else(|| {
            quota
                .max_disk_bytes
                .map(|limit| (QuotaDimension::DiskBytes, limit, request.disk_bytes))
        })?;
    // `observed` is reported at the cap: an understated view stands behind no
    // smaller number.
    Some(QuotaDenied {
        scope: QuotaScope::Group,
        dimension,
        observed: limit,
        requested,
        limit,
    })
}

fn per_job(
    limit: Option<u64>,
    requested: u64,
    dimension: QuotaDimension,
) -> Result<(), QuotaDenied> {
    match limit {
        Some(limit) if requested > limit => Err(QuotaDenied {
            scope: QuotaScope::Job,
            dimension,
            observed: 0,
            requested,
            limit,
        }),
        _ => Ok(()),
    }
}

fn group(
    limit: Option<u64>,
    observed: u64,
    requested: u64,
    dimension: QuotaDimension,
) -> Result<(), QuotaDenied> {
    match limit {
        Some(limit) if observed.saturating_add(requested) > limit => Err(QuotaDenied {
            scope: QuotaScope::Group,
            dimension,
            observed,
            requested,
            limit,
        }),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resources(cpu: u32, ram: u64) -> EffectiveResources {
        EffectiveResources {
            cpu_cores: cpu,
            ram_bytes: ram,
            disk_bytes: 0,
            max_walltime_ms: 60_000,
            preemptible: false,
        }
    }

    fn family(seed: u8, cpu: u32) -> DemandFamily {
        DemandFamily {
            submission_id: SubmissionId([seed; 32]),
            request_digest: [seed; 32],
            resources: resources(cpu, 1_024),
        }
    }

    fn snapshot(group_id: GroupId, families: Vec<DemandFamily>) -> ComputeDemandSnapshot {
        ComputeDemandSnapshot {
            epoch: AdvertisementEpoch::default(),
            truncated: false,
            groups: vec![DemandGroup {
                group_id,
                families,
                truncated: false,
            }],
        }
    }

    fn quota() -> ComputeQuota {
        ComputeQuota {
            max_jobs: Some(2),
            max_cpu_cores: Some(8),
            ..Default::default()
        }
    }

    #[test]
    fn merge_counts_family_once() {
        // Two holders admitting one request family are one logical demand.
        let group_id = Ulid::from_bytes([1; 16]);
        let left = snapshot(group_id, vec![family(1, 2), family(2, 2)]);
        let right = snapshot(group_id, vec![family(2, 2), family(3, 2)]);

        let (totals, truncated) = merge_demand([&left, &right], &group_id);

        assert_eq!(totals.count, 3);
        assert_eq!(totals.cpu_cores, 6);
        assert!(!truncated);
        assert_eq!(merge_demand([&left], &Ulid::from_bytes([9; 16])).0.count, 0);
    }

    #[test]
    fn merge_reports_truncation() {
        let group_id = Ulid::from_bytes([1; 16]);
        let mut snapshot = snapshot(group_id, vec![family(1, 2)]);
        snapshot.groups[0].truncated = true;
        assert!(merge_demand([&snapshot], &group_id).1);
        snapshot.groups.clear();
        snapshot.truncated = true;
        assert!(merge_demand([&snapshot], &group_id).1);
    }

    #[test]
    fn truncation_stays_local() {
        // One busy group may not understate an unrelated group's merged view.
        let busy = Ulid::from_bytes([1; 16]);
        let quiet = Ulid::from_bytes([2; 16]);
        let mut published = ComputeDemandSnapshot {
            epoch: AdvertisementEpoch::default(),
            truncated: false,
            groups: vec![
                DemandGroup {
                    group_id: busy,
                    families: vec![family(1, 2)],
                    truncated: true,
                },
                DemandGroup {
                    group_id: quiet,
                    families: vec![family(2, 2)],
                    truncated: false,
                },
            ],
        };

        assert!(merge_demand([&published], &busy).1);
        assert!(!merge_demand([&published], &quiet).1);

        // Dropped groups only understate a group the snapshot never names.
        published.truncated = true;
        assert!(!merge_demand([&published], &quiet).1);
        assert!(merge_demand([&published], &Ulid::from_bytes([9; 16])).1);
    }

    #[test]
    fn shares_family_budget() {
        // A group that overflows the shared budget is flagged alone, and the
        // quiet group keeps the family it reported.
        let mut groups = vec![
            DemandGroup {
                group_id: Ulid::from_bytes([1; 16]),
                families: (0..=MAX_DEMAND_FAMILIES)
                    .map(|index| DemandFamily {
                        submission_id: SubmissionId([1; 32]),
                        request_digest: [(index % 256) as u8; 32],
                        resources: resources(1, 0),
                    })
                    .collect(),
                truncated: false,
            },
            DemandGroup {
                group_id: Ulid::from_bytes([2; 16]),
                families: vec![family(3, 1)],
                truncated: false,
            },
        ];

        assert!(!bound_demand(&mut groups));

        assert!(groups[0].truncated);
        assert!(!groups[1].truncated);
        assert_eq!(groups[1].families.len(), 1);
        assert_eq!(
            groups
                .iter()
                .map(|group| group.families.len())
                .sum::<usize>(),
            MAX_DEMAND_FAMILIES
        );
    }

    #[test]
    fn drops_extra_groups() {
        let mut groups: Vec<DemandGroup> = (0..=MAX_DEMAND_GROUPS)
            .map(|index| DemandGroup {
                group_id: Ulid::from_bytes([index as u8; 16]),
                families: Vec::new(),
                truncated: false,
            })
            .collect();

        assert!(bound_demand(&mut groups));
        assert_eq!(groups.len(), MAX_DEMAND_GROUPS);
    }

    #[test]
    fn understated_denies_group() {
        // A view that cannot show the cap holds refuses at the cap itself.
        let denied = understated_denial(&quota(), &resources(2, 0)).expect("group cap denies");
        assert_eq!(denied.scope, QuotaScope::Group);
        assert_eq!(denied.dimension, QuotaDimension::Jobs);
        assert_eq!(denied.observed, 2);
        assert_eq!(denied.limit, 2);
        assert_eq!(denied.requested, 1);

        let per_job_only = ComputeQuota {
            max_job_cpu_cores: Some(4),
            ..Default::default()
        };
        assert_eq!(understated_denial(&per_job_only, &resources(2, 0)), None);
    }

    #[test]
    fn partition_overshoots_cap() {
        // Two isolated views each admit; the merged view then refuses only new
        // work, and both admitted families stay in the merged demand.
        let group_id = Ulid::from_bytes([1; 16]);
        let left = snapshot(group_id, vec![family(1, 4)]);
        let right = snapshot(group_id, vec![family(2, 4)]);
        let request = resources(4, 0);
        let cap = ComputeQuota {
            max_jobs: Some(2),
            max_cpu_cores: Some(8),
            ..Default::default()
        };

        assert_eq!(
            admits(&merge_demand([&left], &group_id).0, &cap, &request),
            Ok(())
        );
        assert_eq!(
            admits(&merge_demand([&right], &group_id).0, &cap, &request),
            Ok(())
        );

        let (merged, _) = merge_demand([&left, &right], &group_id);
        assert_eq!(merged.count, 2);
        assert_eq!(merged.cpu_cores, 8);
        assert!(admits(&merged, &cap, &request).is_err());
    }

    #[test]
    fn convergence_keeps_admitted() {
        // The evaluator's only outcome is accepting or refusing a new request:
        // an over-cap view neither drops nor reduces the demand it observed.
        let group_id = Ulid::from_bytes([1; 16]);
        let over = snapshot(group_id, vec![family(1, 8), family(2, 8)]);
        let (merged, _) = merge_demand([&over], &group_id);

        assert!(admits(&merged, &quota(), &resources(1, 0)).is_err());
        assert_eq!(merge_demand([&over], &group_id).0, merged);
        assert_eq!(over.group(&group_id).expect("group").families.len(), 2);
    }

    #[test]
    fn denies_per_job_ceiling() {
        // A request above a per-job ceiling is refused on an empty view too.
        let quota = ComputeQuota {
            max_job_cpu_cores: Some(4),
            max_job_walltime_ms: Some(1_000),
            ..Default::default()
        };
        let denied = admits(&ResourceTotals::default(), &quota, &resources(8, 0))
            .expect_err("cpu ceiling denies");
        assert_eq!(denied.scope, QuotaScope::Job);
        assert_eq!(denied.dimension, QuotaDimension::CpuCores);
        assert_eq!(denied.limit, 4);

        let walltime = admits(&ResourceTotals::default(), &quota, &resources(1, 0))
            .expect_err("walltime ceiling denies");
        assert_eq!(walltime.dimension, QuotaDimension::WalltimeMs);
    }

    #[test]
    fn unconfigured_stays_unlimited() {
        let mut view = ResourceTotals::default();
        for _ in 0..1_000 {
            view.add(&resources(64, u64::MAX));
        }
        assert_eq!(
            admits(&view, &ComputeQuota::default(), &resources(64, u64::MAX)),
            Ok(())
        );
    }

    #[test]
    fn snapshot_bounds_apply() {
        let group_id = Ulid::from_bytes([1; 16]);
        let families: Vec<DemandFamily> = (0..=MAX_DEMAND_FAMILIES)
            .map(|index| DemandFamily {
                submission_id: SubmissionId([(index / 256) as u8; 32]),
                request_digest: [(index % 256) as u8; 32],
                resources: resources(1, 0),
            })
            .collect();
        assert_eq!(
            snapshot(group_id, families).validate(),
            Err(SnapshotError::FamilyCount)
        );

        let unordered = snapshot(group_id, vec![family(2, 1), family(1, 1)]);
        assert_eq!(unordered.validate(), Err(SnapshotError::FamilyOrder));

        let duplicate = snapshot(group_id, vec![family(1, 1), family(1, 1)]);
        assert_eq!(duplicate.validate(), Err(SnapshotError::FamilyOrder));

        let mut groups = ComputeDemandSnapshot {
            epoch: AdvertisementEpoch::default(),
            truncated: false,
            groups: vec![
                DemandGroup {
                    group_id: Ulid::from_bytes([2; 16]),
                    families: Vec::new(),
                    truncated: false,
                },
                DemandGroup {
                    group_id,
                    families: Vec::new(),
                    truncated: false,
                },
            ],
        };
        assert_eq!(groups.validate(), Err(SnapshotError::GroupOrder));
        groups.groups.swap(0, 1);
        assert_eq!(groups.validate(), Ok(()));
    }

    #[test]
    fn availability_subtracts_reservations() {
        // An unmeasured ceiling stays unknown; it never becomes free capacity.
        let limits = ResourceEnvelope {
            max_cpu_cores: Some(16),
            max_ram_bytes: Some(1_024),
            max_disk_bytes: None,
            max_concurrent: Some(4),
        };
        let reserved = ResourceTotals {
            count: 3,
            cpu_cores: 20,
            ram_bytes: 512,
            disk_bytes: 0,
        };

        let observed = availability(&limits, &reserved, 42);

        assert_eq!(observed.free_cpu_cores, Some(0));
        assert_eq!(observed.free_ram_bytes, Some(512));
        assert_eq!(observed.free_disk_bytes, None);
        assert_eq!(observed.active_executions, 3);
        assert_eq!(observed.observed_at_ms, 42);
    }
}
