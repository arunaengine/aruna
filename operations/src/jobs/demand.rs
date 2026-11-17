//! Computes the compute demand this node observes: admitted, nonterminal job families per group,
//! its reserved capacity, and a group's demand merged with the fresh heartbeats of its peers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::compute::quota::{
    ComputeDemandSnapshot, DemandFamily, DemandGroup, JobReservationRecord, MAX_DEMAND_FAMILIES,
    MAX_DEMAND_GROUPS, ResourceTotals, bound_demand,
};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::keyspaces::{
    FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE, JOB_RESERVATION_KEYSPACE,
};
use aruna_core::structs::execution::job::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope, JobRecordKind, LogicalJobState, SubmissionId,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::node_info::AdvertisementEpoch;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Key, Value};

use crate::driver::{DriverContext, drive};
use crate::jobs::records::keys::kind_prefix;
use crate::jobs::records::rows::{ProjectionCache, from_bytes};
use crate::jobs::records::{FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation};
use crate::realm::get_config::GetConfigOperation;

/// Rows one snapshot scan reads per page.
const SNAPSHOT_PAGE_SIZE: usize = 128;

pub(crate) async fn read_reservations(
    ctx: &DriverContext,
) -> Result<Vec<JobReservationRecord>, String> {
    let mut records = Vec::new();
    let mut start: Option<Key> = None;
    loop {
        let (page, next) = iter_page(ctx, JOB_RESERVATION_KEYSPACE, None, start).await?;
        for (_, value) in &page {
            records.push(
                postcard::from_bytes::<JobReservationRecord>(value.as_ref())
                    .map_err(|error| error.to_string())?,
            );
        }
        match next {
            Some(cursor) => start = Some(cursor),
            None => return Ok(records),
        }
    }
}

/// Bounded logical admission demand this node observes: admitted, nonterminal request
/// families with their group and resources (replicas dedupe by family). Truncation is
/// per group, so one busy group never understates another group's view.
pub(crate) async fn demand_snapshot(
    ctx: &DriverContext,
    epoch: AdvertisementEpoch,
) -> Result<ComputeDemandSnapshot, String> {
    let mut collected: BTreeMap<aruna_core::types::GroupId, (Vec<DemandFamily>, bool)> =
        BTreeMap::new();
    let mut unnamed = false;
    let mut start: Option<Key> = None;
    loop {
        let (page, next) = iter_page(ctx, FAMILY_PROJECTION_KEYSPACE, None, start).await?;
        for (key, value) in &page {
            let Some(family) = nonterminal_family(ctx, key, value).await? else {
                continue;
            };
            let Some(spec) = read_family_spec(ctx, &family).await? else {
                continue;
            };
            let demand = DemandFamily {
                submission_id: family.submission_id,
                request_digest: family.request_digest,
                resources: spec.1,
            };
            let full = collected.len() >= MAX_DEMAND_GROUPS;
            match collected.get_mut(&spec.0) {
                Some((families, truncated)) => match families.len() >= MAX_DEMAND_FAMILIES {
                    true => *truncated = true,
                    false => families.push(demand),
                },
                None if full => unnamed = true,
                None => {
                    collected.insert(spec.0, (vec![demand], false));
                }
            }
        }
        match next {
            Some(cursor) => start = Some(cursor),
            None => break,
        }
    }
    let mut groups: Vec<DemandGroup> = collected
        .into_iter()
        .map(|(group_id, (mut families, truncated))| {
            families.sort_unstable_by_key(|family| (family.submission_id.0, family.request_digest));
            DemandGroup {
                group_id,
                families,
                truncated,
            }
        })
        .collect();
    let truncated = bound_demand(&mut groups) || unnamed;
    Ok(ComputeDemandSnapshot {
        epoch,
        groups,
        truncated,
    })
}

/// The family of one projection row that still holds logical demand. Succeeded
/// and cancelled families released theirs; `Indeterminate` has not.
async fn nonterminal_family(
    ctx: &DriverContext,
    key: &Key,
    value: &Value,
) -> Result<Option<JobFamilyId>, String> {
    let family = projection_family(key)?;
    let cache = ProjectionCache::decode(value.as_ref());
    let projection = match cache {
        Some(cache) if !cache.stale => cache.projection,
        _ => {
            let projected = drive(
                ProjectFamilyOperation::new(ProjectFamilyConfig {
                    family: FamilyRef::Family(family),
                    now_ms: unix_timestamp_millis(),
                    rebuild: false,
                }),
                ctx,
            )
            .await
            .map_err(|error| error.to_string())?;
            // A truncated projection proves nothing about the terminal state, so
            // the family keeps counting as demand (overshoot is the safe side).
            if projected.truncated {
                return Ok(Some(family));
            }
            projected.projection
        }
    };
    let Some(projection) = projection else {
        return Ok(None);
    };
    match projection.state {
        LogicalJobState::Queued | LogicalJobState::Running | LogicalJobState::Indeterminate => {
            Ok(Some(JobFamilyId {
                submission_id: projection.submission_id,
                request_digest: projection.request_digest,
            }))
        }
        LogicalJobState::Succeeded | LogicalJobState::Cancelled | LogicalJobState::Failed => {
            Ok(None)
        }
    }
}

fn projection_family(key: &Key) -> Result<JobFamilyId, String> {
    let bytes: &[u8] = key.as_ref();
    let submission_id = bytes
        .get(..32)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| "malformed family projection key".to_string())?;
    let request_digest = bytes
        .get(32..64)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| "malformed family projection key".to_string())?;
    Ok(JobFamilyId {
        submission_id: SubmissionId(submission_id),
        request_digest,
    })
}

/// The group and stored ceilings of one family, read from its immutable spec.
async fn read_family_spec(
    ctx: &DriverContext,
    family: &JobFamilyId,
) -> Result<
    Option<(
        aruna_core::types::GroupId,
        aruna_core::structs::execution::job::EffectiveResources,
    )>,
    String,
> {
    let prefix = kind_prefix(family, JobRecordKind::Spec);
    let (page, _) = iter_page(ctx, FAMILY_RECORD_KEYSPACE, Some(prefix), None).await?;
    for (_, value) in &page {
        if let Ok(envelope) = from_bytes::<JobRecordEnvelope>(value.as_ref())
            && let JobFamilyRecord::Spec(spec) = &envelope.record
        {
            return Ok(Some((spec.group_id, spec.resources)));
        }
    }
    Ok(None)
}

async fn iter_page(
    ctx: &DriverContext,
    key_space: &str,
    prefix: Option<Key>,
    start: Option<Key>,
) -> Result<(Vec<(Key, Value)>, Option<Key>), String> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix,
            start: start.map(IterStart::After),
            limit: SNAPSHOT_PAGE_SIZE,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            let next = (values.len() >= SNAPSHOT_PAGE_SIZE)
                .then(|| values.last().map(|(key, _)| key.clone()))
                .flatten();
            Ok((values, next))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("{key_space} iteration failed: {}", other.label())),
    }
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
        .map_err(|error| format!("failed to read realm config for the demand view: {error}"))?;
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
