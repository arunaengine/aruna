//! Computes the compute demand this node observes: admitted, nonterminal job families per group,
//! its reserved capacity, and a group's demand merged with the fresh heartbeats of its peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::{
    ComputeDemandSnapshot, DemandFamily, DemandGroup, MAX_DEMAND_FAMILIES, MAX_DEMAND_GROUPS,
    ResourceTotals, bound_demand,
};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::family::{
    EffectiveResources, JobFamilyId, JobFamilyRecord, JobProjection, JobRecordEnvelope,
    JobRecordKind, LogicalJobState, SubmissionId,
};
use aruna_core::structs::execution::job::input::WORKFLOW_RUN_TAG;
use aruna_core::structs::execution::job::record_keys::kind_prefix;
use aruna_core::structs::execution::job::record_rows::{ProjectionCache, from_bytes};
use aruna_core::structs::storage::node_info::AdvertisementEpoch;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;

use crate::jobs::records::RecordStoreError;
use crate::jobs::records::project::{
    FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation, ProjectedFamily,
};

/// Rows one snapshot scan reads per page.
const SNAPSHOT_PAGE_SIZE: usize = 128;

#[derive(Debug, Error, PartialEq)]
pub enum DemandError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Projection(#[from] RecordStoreError),
    #[error("malformed family projection key")]
    MalformedKey,
}

#[derive(Debug, PartialEq)]
enum DemandState {
    Page,
    Project(JobFamilyId),
    Spec,
}

type Step = Result<Next<ComputeDemandSnapshot>, DemandError>;

/// Bounded logical admission demand this node observes: admitted, nonterminal request
/// families with their group and resources (replicas dedupe by family). Truncation is
/// per group, so one busy group never understates another group's view.
#[derive(Debug, PartialEq)]
pub struct DemandSnapshotOperation {
    epoch: AdvertisementEpoch,
    collected: BTreeMap<GroupId, (Vec<DemandFamily>, bool)>,
    unnamed: bool,
    /// Projection rows of the current page still to judge, and the page's last key.
    rows: VecDeque<(Key, Value)>,
    after: Option<Key>,
    full: bool,
    /// The family whose spec is being read.
    family: Option<JobFamilyId>,
    state: DemandState,
    now_ms: u64,
}

impl DemandSnapshotOperation {
    pub fn new(epoch: AdvertisementEpoch) -> Self {
        Self {
            epoch,
            collected: BTreeMap::new(),
            unnamed: false,
            rows: VecDeque::new(),
            after: None,
            full: false,
            family: None,
            state: DemandState::Page,
            now_ms: 0,
        }
    }

    fn page(&mut self) -> Step {
        self.state = DemandState::Page;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_PROJECTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone(),
            limit: SNAPSHOT_PAGE_SIZE,
        }))
    }

    /// Judges the next projection row; a stale cache is projected again first.
    fn judge(&mut self) -> Step {
        while let Some((key, value)) = self.rows.pop_front() {
            let family = projection_family(&key)?;
            self.after = Some(key);
            match ProjectionCache::decode(value.as_ref()) {
                Some(cache) if !cache.stale => {
                    if let Some(family) = nonterminal(cache.projection.as_ref()) {
                        return self.read_spec(family);
                    }
                }
                _ => {
                    self.state = DemandState::Project(family);
                    return Ok(Next::Call(IoCall::child(ProjectFamilyOperation::new(
                        ProjectFamilyConfig {
                            family: FamilyRef::Family(family),
                            now_ms: self.now_ms,
                            rebuild: false,
                        },
                    ))));
                }
            }
        }
        match self.full {
            true => self.page(),
            false => Ok(Next::Done(self.snapshot())),
        }
    }

    /// The group and stored ceilings of one family, read from its immutable spec.
    fn read_spec(&mut self, family: JobFamilyId) -> Step {
        self.family = Some(family);
        self.state = DemandState::Spec;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: kind_prefix(&family, JobRecordKind::Spec),
            after: None,
            limit: SNAPSHOT_PAGE_SIZE,
        }))
    }

    /// Keeps one family's demand; the child tasks of one workflow run add up to a single family,
    /// so a large scatter never fills the snapshot's family budget.
    fn keep(&mut self, group_id: GroupId, resources: EffectiveResources, run: Option<&str>) {
        let Some(family) = self.family.take() else {
            return;
        };
        let demand = match run {
            Some(run) => run_family(run, resources),
            None => DemandFamily {
                submission_id: family.submission_id,
                request_digest: family.request_digest,
                resources,
            },
        };
        let full = self.collected.len() >= MAX_DEMAND_GROUPS;
        match self.collected.get_mut(&group_id) {
            Some((families, truncated)) => {
                let same = |kept: &DemandFamily| {
                    run.is_some() && kept.submission_id == demand.submission_id
                };
                match families.iter().position(same) {
                    Some(at) => {
                        let kept = &mut families[at];
                        kept.resources = combined(&kept.resources, &resources);
                    }
                    None if families.len() >= MAX_DEMAND_FAMILIES => *truncated = true,
                    None => families.push(demand),
                }
            }
            None if full => self.unnamed = true,
            None => {
                self.collected.insert(group_id, (vec![demand], false));
            }
        }
    }

    fn snapshot(&mut self) -> ComputeDemandSnapshot {
        let mut groups: Vec<DemandGroup> = std::mem::take(&mut self.collected)
            .into_iter()
            .map(|(group_id, (mut families, truncated))| {
                families
                    .sort_unstable_by_key(|family| (family.submission_id.0, family.request_digest));
                DemandGroup {
                    group_id,
                    families,
                    truncated,
                }
            })
            .collect();
        let truncated = bound_demand(&mut groups) || self.unnamed;
        ComputeDemandSnapshot {
            epoch: self.epoch,
            groups,
            truncated,
        }
    }
}

impl Operation for DemandSnapshotOperation {
    type Output = ComputeDemandSnapshot;
    type Error = DemandError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Step {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> Step {
        match std::mem::replace(&mut self.state, DemandState::Page) {
            DemandState::Page => {
                let IoResult::Page { rows, .. } = result else {
                    return Err(unexpected("IoResult::Page", &result));
                };
                self.full = rows.len() >= SNAPSHOT_PAGE_SIZE;
                self.rows = rows.into();
                self.judge()
            }
            DemandState::Project(family) => {
                let projected = result.child::<ProjectedFamily, RecordStoreError>()??;
                // A truncated projection proves nothing about the terminal state, so
                // the family keeps counting as demand (overshoot is the safe side).
                if projected.truncated {
                    return self.read_spec(family);
                }
                match nonterminal(projected.projection.as_ref()) {
                    Some(family) => self.read_spec(family),
                    None => self.judge(),
                }
            }
            DemandState::Spec => {
                let IoResult::Page { rows, .. } = result else {
                    return Err(unexpected("IoResult::Page", &result));
                };
                let spec = rows.iter().find_map(|(_, value)| {
                    let envelope = from_bytes::<JobRecordEnvelope>(value.as_ref()).ok()?;
                    match envelope.record {
                        JobFamilyRecord::Spec(spec) => Some(spec),
                        _ => None,
                    }
                });
                match spec {
                    Some(spec) => {
                        let run = spec.payload.tags.get(WORKFLOW_RUN_TAG);
                        self.keep(spec.group_id, spec.resources, run.map(String::as_str))
                    }
                    None => self.family = None,
                }
                self.judge()
            }
        }
    }
}

fn unexpected(expected: &'static str, got: &IoResult) -> DemandError {
    DemandError::Io(IoError::Unexpected {
        expected,
        got: got.label(),
    })
}

/// The family of a projection that still holds logical demand. Succeeded and cancelled
/// families released theirs; `Indeterminate` has not.
fn nonterminal(projection: Option<&JobProjection>) -> Option<JobFamilyId> {
    let projection = projection?;
    match projection.state {
        LogicalJobState::Queued | LogicalJobState::Running | LogicalJobState::Indeterminate => {
            Some(JobFamilyId {
                submission_id: projection.submission_id,
                request_digest: projection.request_digest,
            })
        }
        LogicalJobState::Succeeded
        | LogicalJobState::Cancelled
        | LogicalJobState::Failed
        | LogicalJobState::NotRun => None,
    }
}

/// The one family all child tasks of workflow run `run` report as: an identity every publisher
/// derives alike, so replicas count the run once.
fn run_family(run: &str, resources: EffectiveResources) -> DemandFamily {
    let identity = |part: &[u8]| {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"aruna-demand-workflow-run-v1\0");
        hasher.update(part);
        hasher.update(run.as_bytes());
        *hasher.finalize().as_bytes()
    };
    DemandFamily {
        submission_id: SubmissionId(identity(b"submission\0")),
        request_digest: identity(b"request\0"),
        resources,
    }
}

/// The ceilings of two tasks that are queued or run side by side.
fn combined(first: &EffectiveResources, second: &EffectiveResources) -> EffectiveResources {
    EffectiveResources {
        cpu_cores: first.cpu_cores.saturating_add(second.cpu_cores),
        ram_bytes: first.ram_bytes.saturating_add(second.ram_bytes),
        disk_bytes: first.disk_bytes.saturating_add(second.disk_bytes),
        max_walltime_ms: first.max_walltime_ms.max(second.max_walltime_ms),
        preemptible: first.preemptible && second.preemptible,
        gpus: first.gpus.saturating_add(second.gpus),
    }
}

fn projection_family(key: &Key) -> Result<JobFamilyId, DemandError> {
    let bytes: &[u8] = key.as_ref();
    let submission_id = bytes
        .get(..32)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(DemandError::MalformedKey)?;
    let request_digest = bytes
        .get(32..64)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(DemandError::MalformedKey)?;
    Ok(JobFamilyId {
        submission_id: SubmissionId(submission_id),
        request_digest,
    })
}

pub(crate) fn merge_view(
    local: ComputeDemandSnapshot,
    members: &[NodeId],
    node_id: NodeId,
    fresh: &BTreeMap<NodeId, HeldHeartbeat>,
    group_id: &aruna_core::types::GroupId,
) -> (ResourceTotals, bool) {
    let mut missing = false;
    let mut snapshots = vec![local];
    for member in members.iter().filter(|member| **member != node_id) {
        match fresh.get(member) {
            Some(held) => snapshots.push(held.heartbeat.demand.clone()),
            None => missing = true,
        }
    }
    let (totals, truncated) = aruna_core::compute::quota::merge_demand(snapshots.iter(), group_id);
    (totals, truncated || missing)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    #[test]
    fn empty_store_snapshot() {
        let mut operation = DemandSnapshotOperation::new(AdvertisementEpoch::default());
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Page {
                limit: SNAPSHOT_PAGE_SIZE,
                ..
            }))
        ));
        let Ok(Next::Done(snapshot)) = operation.next(page(Vec::new())) else {
            panic!("an empty store has no demand");
        };
        assert!(snapshot.groups.is_empty());
        assert!(!snapshot.truncated);
    }

    #[test]
    fn stale_cache_projects() {
        let mut operation = DemandSnapshotOperation::new(AdvertisementEpoch::default());
        operation.stamp(1_700_000_000_000);
        operation.first().expect("projection page");
        let key = Key::from([7u8; 64].as_slice());
        let stale = Value::from(&[0xff][..]);
        let project = ProjectFamilyOperation::new(ProjectFamilyConfig {
            family: FamilyRef::Family(projection_family(&key).unwrap()),
            now_ms: 1_700_000_000_000,
            rebuild: false,
        });
        assert_eq!(
            operation.next(page(vec![(key, stale)])),
            Ok(Next::Call(IoCall::child(project)))
        );
    }

    #[test]
    fn rejects_short_key() {
        let mut operation = DemandSnapshotOperation::new(AdvertisementEpoch::default());
        operation.first().expect("projection page");
        let short = (Key::from([7u8; 8].as_slice()), Value::from(&[0u8][..]));
        assert_eq!(
            operation.next(page(vec![short])),
            Err(DemandError::MalformedKey)
        );
    }
}
