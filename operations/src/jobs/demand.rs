//! Computes the compute demand this node observes: admitted, nonterminal job families per group,
//! its reserved capacity, and a group's demand merged with the fresh heartbeats of its peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::quota::{
    ComputeDemandSnapshot, DemandFamily, DemandGroup, JobReservationRecord, MAX_DEMAND_FAMILIES,
    MAX_DEMAND_GROUPS, ResourceTotals, bound_demand,
};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::keyspaces::{FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::record_keys::kind_prefix;
use aruna_core::structs::execution::job::record_rows::{ProjectionCache, from_bytes};
use aruna_core::structs::execution::job::{
    EffectiveResources, JobFamilyId, JobFamilyRecord, JobProjection, JobRecordEnvelope,
    JobRecordKind, LogicalJobState, SubmissionId,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::node_info::AdvertisementEpoch;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;

use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::held::HeldReservations;
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::project::{
    FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation, ProjectedFamily,
};

/// Rows one snapshot scan reads per page.
const SNAPSHOT_PAGE_SIZE: usize = 128;

/// Every reservation this node holds; [`HeldReservations::all`] is the operation behind it.
pub(crate) async fn read_reservations(
    ctx: &DriverContext,
) -> Result<Vec<JobReservationRecord>, String> {
    drive(HeldReservations::all(), ctx)
        .await
        .map_err(|error| error.to_string())
}

/// Bounded logical admission demand this node observes; see [`DemandSnapshotOperation`].
pub(crate) async fn demand_snapshot(
    ctx: &DriverContext,
    epoch: AdvertisementEpoch,
) -> Result<ComputeDemandSnapshot, String> {
    drive(DemandSnapshotOperation::new(epoch), ctx)
        .await
        .map_err(|error| error.to_string())
}

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
                            now_ms: unix_timestamp_millis(),
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

    fn keep(&mut self, group_id: GroupId, resources: EffectiveResources) {
        let Some(family) = self.family.take() else {
            return;
        };
        let demand = DemandFamily {
            submission_id: family.submission_id,
            request_digest: family.request_digest,
            resources,
        };
        let full = self.collected.len() >= MAX_DEMAND_GROUPS;
        match self.collected.get_mut(&group_id) {
            Some((families, truncated)) => match families.len() >= MAX_DEMAND_FAMILIES {
                true => *truncated = true,
                false => families.push(demand),
            },
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
                        JobFamilyRecord::Spec(spec) => Some((spec.group_id, spec.resources)),
                        _ => None,
                    }
                });
                match spec {
                    Some((group_id, resources)) => self.keep(group_id, resources),
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
        LogicalJobState::Succeeded | LogicalJobState::Cancelled | LogicalJobState::Failed => None,
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

/// Local nonterminal demand merged with the fresh heartbeats of the other sync-eligible members.
/// The bool flags an understated view: a truncated snapshot or a member without a fresh heartbeat.
pub async fn group_demand(
    ctx: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    group_id: &aruna_core::types::GroupId,
) -> Result<(ResourceTotals, bool), String> {
    let config = drive(GetConfigOperation::new(realm_id), ctx)
        .await
        .map_err(|error| format!("failed to read realm config for node info: {error}"))?;
    let members = config
        .sync_eligible_nodes()
        .map_err(|error| error.to_string())?;
    let local = demand_snapshot(ctx, AdvertisementEpoch::default()).await?;
    let fresh = ctx.fresh_heartbeats();
    Ok(merge_view(local, &members, node_id, &fresh, group_id))
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
        operation.first().expect("projection page");
        let key = Key::from([7u8; 64].as_slice());
        let stale = Value::from(&[0xff][..]);
        assert!(matches!(
            operation.next(page(vec![(key, stale)])),
            Ok(Next::Call(IoCall::Child(_)))
        ));
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
