use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::compute::ExecutorCapability;
use aruna_core::compute_quota::{
    ComputeDemandSnapshot, ComputeDepartureReport, ComputeReservationSnapshot, DemandFamily,
    DemandGroup, JobReservationRecord, MAX_DEMAND_FAMILIES, MAX_DEMAND_GROUPS,
    MAX_UNRESOLVED_EXECUTIONS, ResourceTotals, availability,
};
use aruna_core::document::{DocumentSyncChange, DocumentSyncTarget};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    COMPUTE_DEPARTURE_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE,
    JOB_FAMILY_RECORD_KEYSPACE, JOB_RESERVATION_KEYSPACE, METADATA_INDEX_KEYSPACE,
    NODE_INFO_KEYSPACE, NODE_SUBJECT_KEYSPACE,
};
use aruna_core::storage_entries::document_sync_revision_key;
use aruna_core::structs::{
    AdvertisementEpoch, BackendCatalog, JobFamilyId, JobFamilyRecord, JobRecordEnvelope,
    JobRecordKind, LogicalJobState, NODE_SUBJECT_KEY, NodeInfoDocument, NodeSubjectRecord,
    NodeUrls, NodeUtilization, PlacementRef, RealmConfigDocument, RealmId,
    STORAGE_CLASS_LABEL_PREFIX, node_info_storage_key,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::{info, warn};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::jobs::records::keys::kind_prefix;
use crate::jobs::records::rows::{ProjectionCache, from_bytes};
use crate::metadata::repository::{REGISTRY_FILL_PAGE_SIZE, parse_registry_iter};
use crate::placement::{build_view, held_buckets};
use crate::replicate_documents::{ReplicateDocumentsConfig, ReplicateDocumentsOperation};

/// Rows one snapshot scan reads per page.
const SNAPSHOT_PAGE_SIZE: usize = 128;

/// Interval between node-info heartbeat republishes. Peers treat a node's
/// `heartbeat_at_ms` staleness against this cadence when scoring liveness.
pub const NODE_INFO_PUBLISH_INTERVAL: Duration = Duration::from_secs(60);

/// Arms (or shortens toward) the periodic node-info heartbeat publish task.
pub fn schedule_node_info_publish_effect(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::PublishNodeInfo,
        after,
    })
}

/// Assembles this node's info document from its executors, current
/// placement-view labels, given urls, and local usage, then persists it under the
/// single-writer node-info key without queuing replication.
pub async fn seed_node_info_document(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    urls: NodeUrls,
) -> Result<(), String> {
    let now = unix_timestamp_millis();
    let config = load_realm_config(ctx, realm_id).await?;
    let stored = read_node_info_document(&ctx.storage_handle, node_id).await?;
    let epoch = next_epoch(ctx, realm_id, stored.as_ref(), now).await?;
    let reservation = reservation_snapshot(ctx, epoch).await?;
    let document = NodeInfoDocument {
        node_id,
        executors: advertised_executors(ctx, &reservation.reserved, now).await?,
        labels: node_labels(ctx, &config, node_id)?,
        urls,
        utilization: NodeUtilization {
            storage_bytes_used: local_storage_bytes(ctx).await?,
            documents_held: held_documents(ctx, node_id, &config).await,
            load_permille: read_load_permille(),
            heartbeat_at_ms: now,
        },
        updated_at_ms: now,
        epoch,
        compute_draining: stored.as_ref().is_some_and(|old| old.compute_draining)
            || read_operator_drain(ctx).await?,
        leaving: stored.as_ref().is_some_and(|old| old.leaving),
        demand: demand_snapshot(ctx, epoch).await?,
        reservation,
    };
    write_node_info_document(&ctx.storage_handle, &document).await
}

/// Seeds this node's current info document and replicates it over the shared
/// realm topic. Bootstrap callers must use [`seed_node_info_document`] before
/// announcing the core documents so the authorized announcement is queued
/// first.
pub async fn publish_node_info(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    urls: NodeUrls,
) -> Result<(), String> {
    seed_node_info_document(ctx, node_id, realm_id, urls).await?;
    replicate_node_info(ctx, node_id, realm_id).await
}

/// This node's advertised execution targets: every enabled backend at the
/// current placement subject. A node without a subject holds and executes
/// nothing governed, so it advertises no target at all.
///
/// Availability is derived from each backend's static ceilings minus what this
/// node has actually reserved. It ranks targets and never authorizes: exact
/// admission stays the target-side reservation.
async fn advertised_executors(
    ctx: &DriverContext,
    reserved: &ResourceTotals,
    now_ms: u64,
) -> Result<Vec<ExecutorCapability>, String> {
    let Some(registry) = ctx.compute_handle.as_ref() else {
        return Ok(Vec::new());
    };
    let Some(record) = read_subject_record(&ctx.storage_handle).await? else {
        return Ok(Vec::new());
    };
    let mut capabilities = registry
        .capabilities(&record.subject, record.policy_draining)
        .map_err(|error| format!("failed to build executor advertisements: {error}"))?;
    for capability in &mut capabilities {
        capability.availability = Some(availability(&capability.limits, reserved, now_ms));
    }
    Ok(capabilities)
}

/// Exact local capacity currently reserved, summed over the durable per-
/// execution reservation rows. Duplicate executions are counted separately.
async fn reservation_snapshot(
    ctx: &DriverContext,
    epoch: AdvertisementEpoch,
) -> Result<ComputeReservationSnapshot, String> {
    let mut reserved = ResourceTotals::default();
    for record in read_reservations(ctx).await? {
        reserved.add(&record.resources);
    }
    Ok(ComputeReservationSnapshot { epoch, reserved })
}

async fn read_reservations(ctx: &DriverContext) -> Result<Vec<JobReservationRecord>, String> {
    let mut records = Vec::new();
    let mut start: Option<Key> = None;
    loop {
        let (page, next) = iter_page(ctx, JOB_RESERVATION_KEYSPACE, None, start).await?;
        for (_, value) in &page {
            match postcard::from_bytes::<JobReservationRecord>(value.as_ref()) {
                Ok(record) => records.push(record),
                Err(error) => warn!(%error, "skipping undecodable execution reservation"),
            }
        }
        match next {
            Some(cursor) => start = Some(cursor),
            None => return Ok(records),
        }
    }
}

/// Bounded logical admission demand this node observes: every request family it
/// holds records for that is admitted and not terminal, with the group and
/// resources its immutable spec sealed. Replicas deduplicate by family, so a
/// family several holders observe still counts once.
async fn demand_snapshot(
    ctx: &DriverContext,
    epoch: AdvertisementEpoch,
) -> Result<ComputeDemandSnapshot, String> {
    let mut groups: BTreeMap<aruna_core::types::GroupId, Vec<DemandFamily>> = BTreeMap::new();
    let mut families = 0usize;
    let mut truncated = false;
    let mut start: Option<Key> = None;
    loop {
        let (page, next) = iter_page(ctx, JOB_FAMILY_PROJECTION_KEYSPACE, None, start).await?;
        for (_, value) in &page {
            let Some(family) = nonterminal_family(value) else {
                continue;
            };
            if families >= MAX_DEMAND_FAMILIES {
                truncated = true;
                break;
            }
            let Some(spec) = read_family_spec(ctx, &family).await? else {
                continue;
            };
            let entry = groups.entry(spec.0).or_default();
            entry.push(DemandFamily {
                submission_id: family.submission_id,
                request_digest: family.request_digest,
                resources: spec.1,
            });
            families += 1;
        }
        match next {
            Some(cursor) if !truncated => start = Some(cursor),
            _ => break,
        }
    }
    truncated |= groups.len() > MAX_DEMAND_GROUPS;
    let groups = groups
        .into_iter()
        .take(MAX_DEMAND_GROUPS)
        .map(|(group_id, mut families)| {
            families.sort_unstable_by_key(|family| (family.submission_id.0, family.request_digest));
            DemandGroup {
                group_id,
                families,
                truncated,
            }
        })
        .collect();
    Ok(ComputeDemandSnapshot { epoch, groups })
}

/// The family of one projection row that still holds logical demand. Succeeded
/// and cancelled families released theirs; `Indeterminate` has not.
fn nonterminal_family(value: &Value) -> Option<JobFamilyId> {
    let projection = from_bytes::<ProjectionCache>(value.as_ref())
        .ok()?
        .projection?;
    match projection.state {
        LogicalJobState::Queued | LogicalJobState::Running | LogicalJobState::Indeterminate => {
            Some(JobFamilyId {
                submission_id: projection.submission_id,
                request_digest: projection.request_digest,
            })
        }
        LogicalJobState::Succeeded | LogicalJobState::Cancelled => None,
    }
}

/// The group and sealed ceilings of one family, read from its immutable spec.
async fn read_family_spec(
    ctx: &DriverContext,
    family: &JobFamilyId,
) -> Result<
    Option<(
        aruna_core::types::GroupId,
        aruna_core::structs::EffectiveResources,
    )>,
    String,
> {
    let prefix = kind_prefix(family, JobRecordKind::Spec);
    let (page, _) = iter_page(ctx, JOB_FAMILY_RECORD_KEYSPACE, Some(prefix), None).await?;
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
        other => Err(format!("{key_space} iteration failed: {other:?}")),
    }
}

async fn read_subject_record(storage: &StorageHandle) -> Result<Option<NodeSubjectRecord>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_SUBJECT_KEYSPACE.to_string(),
            key: Key::from(NODE_SUBJECT_KEY.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                NodeSubjectRecord::from_bytes(bytes.as_ref()).map_err(|error| error.to_string())
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("node subject read failed: {other:?}")),
    }
}

/// Supersession tuple of the next advertisement: the observed realm-config
/// revision plus a local counter. A rejoin observes a newer membership
/// generation, so a restarted counter still supersedes the older epoch.
async fn next_epoch(
    ctx: &DriverContext,
    realm_id: RealmId,
    stored: Option<&NodeInfoDocument>,
    now_ms: u64,
) -> Result<AdvertisementEpoch, String> {
    Ok(AdvertisementEpoch {
        membership_generation: membership_generation(ctx, realm_id).await?,
        publisher_generation: stored
            .map(|document| document.epoch.publisher_generation.saturating_add(1))
            .unwrap_or(1),
        observed_at_ms: now_ms,
    })
}

async fn membership_generation(ctx: &DriverContext, realm_id: RealmId) -> Result<u64, String> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
            key: document_sync_revision_key(&target),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .and_then(|bytes| postcard::from_bytes::<DocumentSyncChange>(bytes.as_ref()).ok())
            .map(|change| change.current.generation)
            .unwrap_or_default()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("realm config revision read failed: {other:?}")),
    }
}

/// Heartbeat: refreshes the persisted node-info document's placement-view
/// labels, utilization, and timestamps, then republishes it. URLs remain the
/// startup-seeded values. A missing document is a no-op: the startup seed always
/// runs first.
pub async fn refresh_node_info_heartbeat(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
) -> Result<(), String> {
    let Some(mut document) = read_node_info_document(&ctx.storage_handle, node_id).await? else {
        return Ok(());
    };
    let now = unix_timestamp_millis();
    let config = load_realm_config(ctx, realm_id).await?;
    document.epoch = next_epoch(ctx, realm_id, Some(&document), now).await?;
    document.reservation = reservation_snapshot(ctx, document.epoch).await?;
    document.demand = demand_snapshot(ctx, document.epoch).await?;
    document.executors = advertised_executors(ctx, &document.reservation.reserved, now).await?;
    document.labels = node_labels(ctx, &config, node_id)?;
    document.utilization.storage_bytes_used = local_storage_bytes(ctx).await?;
    document.utilization.documents_held = held_documents(ctx, node_id, &config).await;
    document.utilization.load_permille = read_load_permille();
    document.utilization.heartbeat_at_ms = now;
    document.updated_at_ms = now;
    write_node_info_document(&ctx.storage_handle, &document).await?;
    replicate_node_info(ctx, node_id, realm_id).await
}

/// The locally observed nonterminal admitted demand of one group: this node's
/// own current families merged with every advertisement it holds for a current
/// realm member. The bool reports that some publisher understated its demand.
///
/// This is the input of [`aruna_core::compute_quota::admits`]. It is exact for
/// this node's own admissions, which is what makes a local cap race exact, and
/// approximate across partitions, which is the accepted overshoot bound. A node
/// the realm no longer places is skipped: its usage stays in audit but is no
/// longer capacity of the remaining realm.
pub async fn group_demand(
    ctx: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    group_id: &aruna_core::types::GroupId,
) -> Result<(ResourceTotals, bool), String> {
    let config = load_realm_config(ctx, realm_id).await?;
    let members: BTreeSet<NodeId> = config
        .node_ids()
        .map_err(|error| error.to_string())?
        .into_iter()
        .collect();
    let local = demand_snapshot(ctx, AdvertisementEpoch::default()).await?;
    let mut snapshots = vec![local];
    let mut start: Option<Key> = None;
    loop {
        let (page, next) = iter_page(ctx, NODE_INFO_KEYSPACE, None, start).await?;
        for (_, value) in &page {
            match postcard::from_bytes::<NodeInfoDocument>(value.as_ref()) {
                Ok(document)
                    if document.node_id != node_id && members.contains(&document.node_id) =>
                {
                    snapshots.push(document.demand)
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "skipping undecodable node info advertisement"),
            }
        }
        match next {
            Some(cursor) => start = Some(cursor),
            None => break,
        }
    }
    Ok(aruna_core::compute_quota::merge_demand(
        snapshots.iter(),
        group_id,
    ))
}

/// The single durable departure-report row of this node.
const DEPARTURE_KEY: &[u8] = b"departure";
/// The operator's own compute drain, kept apart from an observed departure so
/// returning to placement can never silently undrain a node an operator drained.
const OPERATOR_DRAIN_KEY: &[u8] = b"operator_drain";

/// Applies an observed departure, or a return, to this node's compute plane.
///
/// Departing stops new offers and admissions immediately, publishes the final
/// snapshots, and records every still-reserved execution as unresolved. It
/// waits for nothing: membership removal is never blocked, and a reserved
/// execution is never declared terminal. Returning clears the flags so the
/// rejoined node advertises again under its new membership generation.
/// `Ok(false)` means the state already matched, so nothing was republished.
pub async fn set_departure_state(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    departing: bool,
) -> Result<bool, String> {
    let Some(mut document) = read_node_info_document(&ctx.storage_handle, node_id).await? else {
        return Ok(false);
    };
    // An operator drain outlives a placement observation: returning to the
    // placement map must not undrain a node somebody drained deliberately.
    let draining = departing || read_operator_drain(ctx).await?;
    if document.leaving == departing && document.compute_draining == draining {
        return Ok(false);
    }
    let now = unix_timestamp_millis();
    document.epoch = next_epoch(ctx, realm_id, Some(&document), now).await?;
    document.leaving = departing;
    document.compute_draining = draining;
    document.reservation = reservation_snapshot(ctx, document.epoch).await?;
    document.demand = demand_snapshot(ctx, document.epoch).await?;
    document.updated_at_ms = now;
    if departing {
        write_departure_report(ctx, document.epoch.membership_generation, now).await?;
    }
    info!(
        leaving = departing,
        compute_draining = draining,
        membership_generation = document.epoch.membership_generation,
        reserved = document.reservation.reserved.count,
        "Compute departure state published"
    );
    write_node_info_document(&ctx.storage_handle, &document).await?;
    replicate_node_info(ctx, node_id, realm_id).await?;
    Ok(true)
}

/// Sets or clears the operator's own compute drain and republishes the
/// advertisement. A drained node plans no new execution here; work that already
/// holds a receipt is never cancelled by it, and departure state is untouched.
/// `Ok(false)` means the state already matched, so nothing was republished.
pub async fn set_operator_drain(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    draining: bool,
) -> Result<bool, String> {
    if read_operator_drain(ctx).await? == draining {
        return Ok(false);
    }
    write_operator_drain(ctx, draining).await?;
    let Some(mut document) = read_node_info_document(&ctx.storage_handle, node_id).await? else {
        return Ok(true);
    };
    let now = unix_timestamp_millis();
    document.epoch = next_epoch(ctx, realm_id, Some(&document), now).await?;
    // A departing node stays draining whatever the operator flag says.
    document.compute_draining = draining || document.leaving;
    document.reservation = reservation_snapshot(ctx, document.epoch).await?;
    document.demand = demand_snapshot(ctx, document.epoch).await?;
    document.updated_at_ms = now;
    info!(
        operator_draining = draining,
        leaving = document.leaving,
        reserved = document.reservation.reserved.count,
        "Operator compute drain published"
    );
    write_node_info_document(&ctx.storage_handle, &document).await?;
    replicate_node_info(ctx, node_id, realm_id).await?;
    Ok(true)
}

/// Whether an operator currently drains this node's compute plane.
pub async fn read_operator_drain(ctx: &DriverContext) -> Result<bool, String> {
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            key: Key::from(OPERATOR_DRAIN_KEY.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("operator drain read failed: {other:?}")),
    }
}

async fn write_operator_drain(ctx: &DriverContext, draining: bool) -> Result<(), String> {
    let key = Key::from(OPERATOR_DRAIN_KEY.to_vec());
    let effect = match draining {
        true => StorageEffect::Write {
            key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            key,
            value: Value::from(vec![1u8]),
            txn_id: None,
        },
        false => StorageEffect::Delete {
            key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            key,
            txn_id: None,
        },
    };
    match ctx.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. })
        | Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("operator drain write failed: {other:?}")),
    }
}

/// Records the executions this node still holds capacity for. Their
/// reservations stay in place: a departing node may neither erase them nor
/// claim they ended.
async fn write_departure_report(
    ctx: &DriverContext,
    membership_generation: u64,
    now_ms: u64,
) -> Result<(), String> {
    let mut unresolved: Vec<Ulid> = read_reservations(ctx)
        .await?
        .into_iter()
        .map(|record| record.execution_id)
        .collect();
    unresolved.sort_unstable();
    let truncated = unresolved.len() > MAX_UNRESOLVED_EXECUTIONS;
    unresolved.truncate(MAX_UNRESOLVED_EXECUTIONS);
    let report = ComputeDepartureReport {
        departed_at_ms: now_ms,
        membership_generation,
        unresolved,
        truncated,
    };
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            key: Key::from(DEPARTURE_KEY.to_vec()),
            value: Value::from(postcard::to_allocvec(&report).map_err(|error| error.to_string())?),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("departure report write failed: {other:?}")),
    }
}

/// The durable departure report, if this node ever departed.
pub async fn read_departure_report(
    storage: &StorageHandle,
) -> Result<Option<ComputeDepartureReport>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            key: Key::from(DEPARTURE_KEY.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes::<ComputeDepartureReport>(bytes.as_ref())
                    .map_err(|error| error.to_string())
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("departure report read failed: {other:?}")),
    }
}

async fn load_realm_config(
    ctx: &DriverContext,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, String> {
    drive(GetRealmConfigOperation::new(realm_id), ctx)
        .await
        .map_err(|error| format!("failed to read realm config for node info: {error}"))
}

fn placement_labels(
    config: &RealmConfigDocument,
    node_id: NodeId,
) -> Result<BTreeMap<String, String>, String> {
    build_view(config)
        .nodes
        .into_iter()
        .find(|node| node.node_id == node_id)
        .map(|node| node.labels)
        .ok_or_else(|| format!("node {node_id} is missing from the realm placement view"))
}

fn class_labels(catalog: &BackendCatalog) -> BTreeMap<String, String> {
    catalog
        .classes()
        .into_iter()
        .map(|class| {
            (
                format!("{STORAGE_CLASS_LABEL_PREFIX}{class}"),
                "true".to_string(),
            )
        })
        .collect()
}

/// Derived from the node's own registry, so a class disappears from the labels
/// as soon as the operator stops offering it. Advertisement only: the class
/// labels stay in `NodeInfo` and never enter the placement selector input.
fn node_labels(
    ctx: &DriverContext,
    config: &RealmConfigDocument,
    node_id: NodeId,
) -> Result<BTreeMap<String, String>, String> {
    let mut labels = placement_labels(config, node_id)?;
    if let Some(blob_handle) = ctx.blob_handle.as_ref() {
        labels.extend(class_labels(&blob_handle.routing().catalog));
    }
    Ok(labels)
}

async fn local_storage_bytes(ctx: &DriverContext) -> Result<u64, String> {
    Ok(crate::usage_stats::read_local_global(&ctx.storage_handle)
        .await?
        .stored_bytes)
}

/// Counts documents this node holds, degrading to `None` with a warning so a
/// storage hiccup never fails the heartbeat.
async fn held_documents(
    ctx: &DriverContext,
    node_id: NodeId,
    config: &RealmConfigDocument,
) -> Option<u64> {
    match count_held_documents(ctx, node_id, config).await {
        Ok(count) => Some(count),
        Err(error) => {
            warn!(%error, "failed to count documents held for node info");
            None
        }
    }
}

/// The `(strategy, shard)` buckets `node_id` holds across every strategy. A
/// document counts as held when its recorded placement bucket is in this set,
/// so everywhere-replicated registry rows are not each counted as local.
fn held_placement_set(config: &RealmConfigDocument, node_id: NodeId) -> HashSet<(Ulid, u32)> {
    let mut held = HashSet::new();
    for strategy in &config.strategies {
        for shard in held_buckets(config, strategy, node_id) {
            held.insert((strategy.strategy_id, shard));
        }
    }
    held
}

async fn count_held_documents(
    ctx: &DriverContext,
    node_id: NodeId,
    config: &RealmConfigDocument,
) -> Result<u64, String> {
    let held = held_placement_set(config, node_id);
    let mut count = 0u64;
    let mut start_after: Option<Key> = None;
    loop {
        let event = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_INDEX_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.map(IterStart::After),
                limit: REGISTRY_FILL_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (page, next) = parse_registry_iter(event)
            .map_err(|error| format!("metadata registry iteration failed: {error:?}"))?;
        for record in &page {
            // NIL placements predate any strategy and are held by every local
            // node, matching holds_placement; count them alongside held buckets.
            if record.placement == PlacementRef::NIL
                || held.contains(&(record.placement.strategy_id, record.placement.shard))
            {
                count += 1;
            }
        }
        match next {
            Some(cursor) => start_after = Some(cursor),
            None => break,
        }
    }
    Ok(count)
}

/// 1-minute OS load average scaled to permille of logical core capacity, or
/// `None` (with a warning) when the core count is unavailable.
fn read_load_permille() -> Option<u32> {
    match std::thread::available_parallelism() {
        Ok(cores) => Some(permille_of(current_load1(), cores.get() as u64)),
        Err(error) => {
            warn!(%error, "failed to read logical core count for node info load");
            None
        }
    }
}

fn current_load1() -> f64 {
    sysinfo::System::load_average().one
}

/// Load average per core scaled to permille and clamped to `0..=1000`. Zero
/// cores yields `0` instead of dividing by zero.
fn permille_of(load1: f64, cores: u64) -> u32 {
    if cores == 0 {
        return 0;
    }
    (load1 / cores as f64 * 1000.0).round().clamp(0.0, 1000.0) as u32
}

async fn replicate_node_info(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
) -> Result<(), String> {
    drive(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node_id,
            excluded_peers: Vec::new(),
            documents: vec![DocumentSyncTarget::NodeInfo { realm_id, node_id }],
            // Shared-topic genesis is bootstrapped by publish_core_documents;
            // explicit publishes and periodic heartbeats only publish into it.
            allow_genesis: false,
        }),
        ctx,
    )
    .await
    .map_err(|error| format!("node info replication failed: {error}"))
}

async fn write_node_info_document(
    storage: &StorageHandle,
    document: &NodeInfoDocument,
) -> Result<(), String> {
    // `to_bytes` validates, so a locally built advertisement that broke its own
    // bounds never reaches storage or the shared realm topic.
    let value = Value::from(document.to_bytes().map_err(|error| error.to_string())?);
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: NODE_INFO_KEYSPACE.to_string(),
            key: Key::from(node_info_storage_key(document.node_id)),
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("node info write failed: {other:?}")),
    }
}

/// Reads a single node's persisted info document, if present.
pub async fn read_node_info_document(
    storage: &StorageHandle,
    node_id: NodeId,
) -> Result<Option<NodeInfoDocument>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_INFO_KEYSPACE.to_string(),
            key: Key::from(node_info_storage_key(node_id)),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes::<NodeInfoDocument>(bytes.as_ref())
                    .map_err(|error| error.to_string())
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("node info read failed: {other:?}")),
    }
}

/// Reads the persisted info documents for the given nodes, skipping those with
/// no document yet. Keyed by node id for the realm-nodes read surface. Takes the
/// driver context so API routes drive this through the operations layer rather
/// than touching the storage handle directly.
pub async fn read_node_info_documents(
    ctx: &DriverContext,
    node_ids: &[NodeId],
) -> Result<BTreeMap<NodeId, NodeInfoDocument>, String> {
    let mut documents = BTreeMap::new();
    for node_id in node_ids {
        if let Some(document) = read_node_info_document(&ctx.storage_handle, *node_id).await? {
            documents.insert(*node_id, document);
        }
    }
    Ok(documents)
}

/// Arms the periodic node-info heartbeat at startup. `ShortenTimer` (never
/// `ResetTimer`) so the durable-queue re-arm loop cannot push the deadline
/// forward past the handler's own post-run re-arm.
pub async fn restore_node_info_publish_timer(_storage: &StorageHandle, task_handle: &TaskHandle) {
    if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PublishNodeInfo,
            after: NODE_INFO_PUBLISH_INTERVAL,
        }))
        .await
    {
        warn!(message = %message, "Failed to arm node info heartbeat timer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord};
    use aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE;
    use aruna_core::storage_entries::metadata_registry_key;
    use aruna_core::structs::{
        KIND_LABEL_KEY, MetadataRegistryRecord, NodePlacementEntry, PlacementRef,
        PlacementStrategy, RealmConfigDocument, RealmNodeKind,
    };
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn realm_config(realm_id: RealmId, nodes: &[NodeId]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        config
    }

    async fn write_realm_config(ctx: &DriverContext, config: &RealmConfigDocument) {
        // A removal fixture writes a config that no longer names any node.
        let node_id = config
            .node_ids()
            .ok()
            .and_then(|ids| ids.first().copied())
            .unwrap_or_else(|| node(1));
        let actor = aruna_core::structs::Actor {
            node_id,
            user_id: aruna_core::types::UserId::nil(config.realm_id),
            realm_id: config.realm_id,
        };
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: config.realm_id,
        };
        let event = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn read_outbox(ctx: &DriverContext) -> Vec<DocumentSyncOutboxRecord> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 256,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| postcard::from_bytes(&value).unwrap())
                .collect(),
            other => panic!("unexpected outbox iter result: {other:?}"),
        }
    }

    #[test]
    fn classes_become_labels() {
        // Operator-only classes are advertised too: placement is operator domain.
        let catalog = BackendCatalog::new("hot".to_string())
            .with_backend("hot".to_string(), Some("hot".to_string()))
            .with_reserved("tape".to_string(), Some("archive".to_string()))
            .with_backend("plain".to_string(), None);

        let labels = class_labels(&catalog);

        assert_eq!(labels.len(), 2);
        assert_eq!(
            labels.get(&format!("{STORAGE_CLASS_LABEL_PREFIX}hot")),
            Some(&"true".to_string())
        );
        assert!(labels.contains_key(&format!("{STORAGE_CLASS_LABEL_PREFIX}archive")));
    }

    #[tokio::test]
    async fn seed_writes_document_without_queuing_outbox() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let local = node(1);
        let mut config = realm_config(realm_id, &[local]);
        config.placement_map.push(NodePlacementEntry {
            node_id: local,
            location: String::new(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
        });
        write_realm_config(&ctx, &config).await;

        seed_node_info_document(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: Some("s3.example".to_string()),
            },
        )
        .await
        .unwrap();

        let stored = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .expect("seeded node info document");
        assert_eq!(stored.labels.get("tier").unwrap(), "hot");
        assert_eq!(stored.labels.get(KIND_LABEL_KEY).unwrap(), "server");
        assert!(stored.executors.is_empty());
        assert_eq!(stored.utilization.storage_bytes_used, 0);
        assert!(read_outbox(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn publish_uses_selector_labels_and_queues_outbox() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let local = node(1);
        let mut config = realm_config(realm_id, &[local, node(2)]);
        config.placement_map.push(NodePlacementEntry {
            node_id: local,
            location: String::new(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::from([("tier".to_string(), "hot".to_string())]),
        });
        let expected_labels = build_view(&config)
            .nodes
            .into_iter()
            .find(|node| node.node_id == local)
            .unwrap()
            .labels;
        write_realm_config(&ctx, &config).await;

        publish_node_info(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: Some("s3.example".to_string()),
            },
        )
        .await
        .unwrap();

        let stored = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .expect("node info document persisted");
        assert_eq!(stored.node_id, local);
        assert_eq!(stored.labels, expected_labels);
        assert_eq!(stored.labels.get(KIND_LABEL_KEY).unwrap(), "server");
        assert_eq!(stored.urls.s3.as_deref(), Some("s3.example"));
        assert_eq!(stored.utilization.documents_held, Some(0));
        assert!(stored.utilization.load_permille.is_some());

        let outbox = read_outbox(&ctx).await;
        let record = outbox
            .iter()
            .find(|record| {
                matches!(&record.event, DocumentSyncOutboxEvent::Upsert { .. })
                    && record.target
                        == DocumentSyncTarget::NodeInfo {
                            realm_id,
                            node_id: local,
                        }
            })
            .expect("node info upsert queued");
        assert!(!record.allow_genesis);
    }

    #[tokio::test]
    async fn heartbeat_reflects_placement_changes_and_drops_stale_labels() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let local = node(1);
        let mut config = realm_config(realm_id, &[local]);
        config.placement_map.push(NodePlacementEntry {
            node_id: local,
            location: String::new(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::from([
                ("stale".to_string(), "remove-me".to_string()),
                ("zone".to_string(), "a".to_string()),
            ]),
        });
        write_realm_config(&ctx, &config).await;

        publish_node_info(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();
        let first = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();

        config.placement_map[0].labels = BTreeMap::from([
            ("rack".to_string(), "r7".to_string()),
            ("zone".to_string(), "b".to_string()),
        ]);
        let expected_labels = build_view(&config).nodes[0].labels.clone();
        write_realm_config(&ctx, &config).await;

        refresh_node_info_heartbeat(&ctx, local, realm_id)
            .await
            .unwrap();
        let second = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(second.labels, expected_labels);
        assert_eq!(second.labels.get("zone").unwrap(), "b");
        assert!(!second.labels.contains_key("stale"));
        // A node without a compute plane advertises no execution target, and
        // every republish supersedes its own predecessor.
        assert!(second.executors.is_empty());
        assert!(second.epoch.publisher_generation > first.epoch.publisher_generation);
        assert!(second.supersedes(&first));
        assert!(second.updated_at_ms >= first.updated_at_ms);
        assert!(second.utilization.heartbeat_at_ms >= first.utilization.heartbeat_at_ms);

        let node_info_records: Vec<_> = read_outbox(&ctx)
            .await
            .into_iter()
            .filter(|record| {
                record.target
                    == DocumentSyncTarget::NodeInfo {
                        realm_id,
                        node_id: local,
                    }
            })
            .collect();
        assert_eq!(node_info_records.len(), 2);
        assert!(node_info_records.iter().all(|record| !record.allow_genesis));
    }

    #[tokio::test]
    async fn heartbeat_without_seed_is_noop() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let local = node(1);
        write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;

        refresh_node_info_heartbeat(&ctx, local, realm_id)
            .await
            .unwrap();
        assert!(
            read_node_info_document(&ctx.storage_handle, local)
                .await
                .unwrap()
                .is_none()
        );
    }

    fn sharded_config(realm_id: RealmId) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([5u8; 16]),
            name: "default".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy];
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        config
    }

    fn registry_record(
        realm_id: RealmId,
        seed: u8,
        placement: PlacementRef,
    ) -> MetadataRegistryRecord {
        let id = Ulid::from_bytes([seed; 16]);
        MetadataRegistryRecord {
            realm_id,
            group_id: id,
            document_id: id,
            document_path: String::new(),
            graph_iri: String::new(),
            public: false,
            permission_path: String::new(),
            placement,
            holder_node_ids: Vec::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }

    async fn write_registry(ctx: &DriverContext, record: &MetadataRegistryRecord) {
        let event = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: METADATA_INDEX_KEYSPACE.to_string(),
                key: metadata_registry_key(record.group_id, record.document_id),
                value: postcard::to_allocvec(record).unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[test]
    fn permille_exact_values() {
        assert_eq!(permille_of(1.0, 4), 250);
        assert_eq!(permille_of(3.0, 4), 750);
        assert_eq!(permille_of(0.0, 8), 0);
        assert_eq!(permille_of(4.0, 4), 1000);
    }

    #[test]
    fn permille_clamps_high() {
        assert_eq!(permille_of(9.0, 4), 1000);
        assert_eq!(permille_of(10.0, 2), 1000);
    }

    #[test]
    fn permille_zero_cores() {
        assert_eq!(permille_of(5.0, 0), 0);
    }

    #[tokio::test]
    async fn counts_held_documents() {
        // Records whose placement bucket the node holds, plus NIL placements, count.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let local = node(1);
        let config = sharded_config(realm_id);
        let strategy = &config.strategies[0];
        let held = held_buckets(&config, strategy, local);
        assert!(!held.is_empty() && held.len() < strategy.shard_count as usize);
        let unheld = (0..strategy.shard_count)
            .find(|shard| !held.contains(shard))
            .unwrap();
        let placed = |shard| PlacementRef {
            strategy_id: strategy.strategy_id,
            shard,
        };

        write_registry(&ctx, &registry_record(realm_id, 1, placed(held[0]))).await;
        write_registry(&ctx, &registry_record(realm_id, 2, placed(held[0]))).await;
        write_registry(&ctx, &registry_record(realm_id, 3, placed(unheld))).await;
        write_registry(&ctx, &registry_record(realm_id, 4, PlacementRef::NIL)).await;

        let count = count_held_documents(&ctx, local, &config).await.unwrap();
        assert_eq!(count, 3);
    }

    fn family(seed: u8) -> JobFamilyId {
        JobFamilyId {
            submission_id: aruna_core::structs::SubmissionId([seed; 32]),
            request_digest: [seed; 32],
        }
    }

    fn spec_record(realm_id: RealmId, family: JobFamilyId, group_id: Ulid) -> JobRecordEnvelope {
        let resources = aruna_core::structs::EffectiveResources {
            cpu_cores: 2,
            ram_bytes: 1_024,
            disk_bytes: 2_048,
            max_walltime_ms: 60_000,
            preemptible: false,
        };
        let job_id = aruna_core::structs::JobId::from_bytes([9u8; 16]);
        let origin = node(1);
        let spec = aruna_core::structs::LogicalJobSpec {
            submission_id: family.submission_id,
            job_id,
            origin_node_id: origin,
            realm_id,
            group_id,
            created_by: aruna_core::types::UserId::nil(realm_id),
            created_at_ms: 1_000,
            payload: aruna_core::structs::ExecutionSpec {
                group_id,
                name: None,
                description: None,
                tags: Default::default(),
                image: "alpine:3".to_string(),
                entrypoint: None,
                command: Vec::new(),
                workdir: None,
                env: Default::default(),
                resources: Default::default(),
                executor_constraint: None,
                inputs: Vec::new(),
                file_outputs: Vec::new(),
                workspace_outputs: Vec::new(),
                output_prefixes: Vec::new(),
                collision_policy: Default::default(),
            },
            request_digest: family.request_digest,
            spec_digest: [0u8; 32],
            resources,
            retry: aruna_core::structs::JobRetryPolicy {
                max_launches_per_witness: 2,
            },
            admission: aruna_core::structs::JobAdmissionRecord {
                submission_id: family.submission_id,
                request_digest: family.request_digest,
                job_id,
                group_id,
                admitting_node_id: origin,
                membership_generation: 1,
                resources,
                admitted_at_ms: 1_000,
            },
            placement: PlacementRef::NIL,
        }
        .seal()
        .expect("spec seals");
        JobRecordEnvelope::sign(
            realm_id,
            JobFamilyRecord::Spec(Box::new(spec)),
            &iroh::SecretKey::from_bytes(&[1u8; 32]),
        )
        .expect("record signs")
    }

    async fn write_family(
        ctx: &DriverContext,
        realm_id: RealmId,
        family: JobFamilyId,
        group_id: Ulid,
        state: LogicalJobState,
    ) {
        let cache = ProjectionCache {
            revision: 1,
            stale: false,
            projection: Some(aruna_core::structs::JobProjection {
                submission_id: family.submission_id,
                request_digest: family.request_digest,
                canonical_job_id: aruna_core::structs::JobId::from_bytes([9u8; 16]),
                aliases: Vec::new(),
                state,
                canonical_execution_id: None,
                executions: Vec::new(),
                outputs: aruna_core::structs::OutputSet::new(Vec::new()).expect("empty outputs"),
                cancel_requested: false,
            }),
        };
        write_row(
            ctx,
            JOB_FAMILY_PROJECTION_KEYSPACE,
            Key::from(family.to_bytes().as_slice()),
            postcard::to_allocvec(&cache).unwrap(),
        )
        .await;
        let record = spec_record(realm_id, family, group_id);
        let mut key = kind_prefix(&family, JobRecordKind::Spec).to_vec();
        key.extend_from_slice(&[0u8; 40]);
        write_row(
            ctx,
            JOB_FAMILY_RECORD_KEYSPACE,
            Key::from(key.as_slice()),
            postcard::to_allocvec(&record).unwrap(),
        )
        .await;
    }

    async fn write_reservation(ctx: &DriverContext, execution_id: Ulid, cpu: u32) {
        let record = JobReservationRecord {
            execution_id,
            job_id: aruna_core::structs::JobId::from_bytes([9u8; 16]),
            resources: aruna_core::structs::EffectiveResources {
                cpu_cores: cpu,
                ram_bytes: 512,
                disk_bytes: 0,
                max_walltime_ms: 1_000,
                preemptible: false,
            },
            created_at_ms: 5,
            subject_generation: 1,
            subject_digest: [0u8; 32],
        };
        write_row(
            ctx,
            JOB_RESERVATION_KEYSPACE,
            Key::from(execution_id.to_bytes().as_slice()),
            postcard::to_allocvec(&record).unwrap(),
        )
        .await;
    }

    async fn write_row(ctx: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
        let event = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn publishes_separate_snapshots() {
        // Logical demand and physical reservation are different controls: a
        // terminal family leaves demand while its node still reserves capacity.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let local = node(1);
        write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
        let group_id = Ulid::from_bytes([2u8; 16]);
        write_family(&ctx, realm_id, family(1), group_id, LogicalJobState::Queued).await;
        write_family(
            &ctx,
            realm_id,
            family(2),
            group_id,
            LogicalJobState::Indeterminate,
        )
        .await;
        write_family(
            &ctx,
            realm_id,
            family(3),
            group_id,
            LogicalJobState::Succeeded,
        )
        .await;
        write_reservation(&ctx, Ulid::from_bytes([7u8; 16]), 4).await;

        publish_node_info(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();

        let stored = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .expect("document");
        let group = stored.demand.group(&group_id).expect("group demand");
        assert_eq!(group.families.len(), 2);
        assert!(!group.truncated);
        assert_eq!(stored.demand.epoch, stored.epoch);
        // Reservations are exact and local; they never enter logical demand.
        assert_eq!(stored.reservation.reserved.count, 1);
        assert_eq!(stored.reservation.reserved.cpu_cores, 4);
        assert_eq!(stored.reservation.reserved.ram_bytes, 512);
        assert!(stored.validate().is_ok());
    }

    #[tokio::test]
    async fn merges_group_demand() {
        // A family two publishers observe counts once, a removed publisher's
        // demand stops counting, and the local view is current, not the last
        // heartbeat's.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([12u8; 32]);
        let local = node(1);
        let peer = node(2);
        let removed = node(3);
        write_realm_config(&ctx, &realm_config(realm_id, &[local, peer])).await;
        let group_id = Ulid::from_bytes([2u8; 16]);
        write_family(&ctx, realm_id, family(1), group_id, LogicalJobState::Queued).await;

        let epoch = AdvertisementEpoch::default();
        let shared = DemandFamily {
            submission_id: family(1).submission_id,
            request_digest: family(1).request_digest,
            resources: aruna_core::structs::EffectiveResources {
                cpu_cores: 2,
                ram_bytes: 1_024,
                disk_bytes: 2_048,
                max_walltime_ms: 60_000,
                preemptible: false,
            },
        };
        let own = DemandFamily {
            submission_id: family(4).submission_id,
            request_digest: family(4).request_digest,
            ..shared
        };
        for (node_id, families) in [(peer, vec![shared]), (removed, vec![own])] {
            let mut document = NodeInfoDocument {
                node_id,
                executors: Vec::new(),
                labels: BTreeMap::new(),
                urls: NodeUrls {
                    api: None,
                    s3: None,
                },
                utilization: NodeUtilization {
                    storage_bytes_used: 0,
                    documents_held: None,
                    load_permille: None,
                    heartbeat_at_ms: 1,
                },
                updated_at_ms: 1,
                epoch,
                compute_draining: false,
                leaving: false,
                demand: ComputeDemandSnapshot::default(),
                reservation: ComputeReservationSnapshot::default(),
            };
            document.demand.groups = vec![DemandGroup {
                group_id,
                families,
                truncated: false,
            }];
            write_node_info_document(&ctx.storage_handle, &document)
                .await
                .unwrap();
        }

        let (totals, truncated) = group_demand(&ctx, realm_id, local, &group_id)
            .await
            .unwrap();

        // The peer republishes the family this node already holds locally.
        assert_eq!(totals.count, 1);
        assert_eq!(totals.cpu_cores, 2);
        assert!(!truncated);
        assert_eq!(
            group_demand(&ctx, realm_id, local, &Ulid::from_bytes([9u8; 16]))
                .await
                .unwrap()
                .0
                .count,
            0
        );
    }

    #[tokio::test]
    async fn departure_stops_offers() {
        // Departure revokes future eligibility and reports what it could not
        // resolve, without deleting a reservation or blocking removal.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([10u8; 32]);
        let local = node(1);
        write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
        let execution_id = Ulid::from_bytes([7u8; 16]);
        write_reservation(&ctx, execution_id, 2).await;
        publish_node_info(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();
        let before = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();

        assert!(
            set_departure_state(&ctx, local, realm_id, true)
                .await
                .unwrap()
        );

        let after = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();
        assert!(after.leaving && after.compute_draining);
        assert!(!after.offers_compute(true));
        assert!(after.supersedes(&before));
        assert_eq!(after.reservation.reserved.count, 1);

        let report = read_departure_report(&ctx.storage_handle)
            .await
            .unwrap()
            .expect("departure report");
        assert_eq!(report.unresolved, vec![execution_id]);
        assert!(!report.truncated);
        // The reservation itself survives: departing proves nothing about it.
        assert_eq!(read_reservations(&ctx).await.unwrap().len(), 1);
        // A repeated observation republishes nothing.
        assert!(
            !set_departure_state(&ctx, local, realm_id, true)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn removal_revokes_eligibility() {
        // A node the realm no longer places learns of its removal through config
        // sync. It stops offering execution without ending a receipted one.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([13u8; 32]);
        let local = node(1);
        let mut config = realm_config(realm_id, &[local]);
        config.placement_map.push(NodePlacementEntry {
            node_id: local,
            location: String::new(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        });
        write_realm_config(&ctx, &config).await;
        let subject = aruna_core::structs::storage_subject(&config.placement_map[0], 1);
        let record = NodeSubjectRecord::seed(subject).expect("subject is valid");
        write_row(
            &ctx,
            NODE_SUBJECT_KEYSPACE,
            Key::from(NODE_SUBJECT_KEY.to_vec()),
            record.to_bytes().unwrap(),
        )
        .await;
        write_reservation(&ctx, Ulid::from_bytes([7u8; 16]), 2).await;
        publish_node_info(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();

        config.placement_map.clear();
        config.nodes.clear();
        write_realm_config(&ctx, &config).await;
        crate::placement_policy::observe_placement(&ctx, realm_id, local, 10)
            .await
            .expect("observation completes");

        let stored = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .expect("document");
        assert!(stored.leaving && stored.compute_draining);
        assert!(!stored.offers_compute(false));
        // The receipted execution keeps its reservation and its audit trail.
        assert_eq!(read_reservations(&ctx).await.unwrap().len(), 1);
        assert_eq!(stored.reservation.reserved.count, 1);
    }

    #[tokio::test]
    async fn rejoin_clears_departure() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([11u8; 32]);
        let local = node(1);
        write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
        publish_node_info(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();
        set_departure_state(&ctx, local, realm_id, true)
            .await
            .unwrap();
        let departed = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();

        assert!(
            set_departure_state(&ctx, local, realm_id, false)
                .await
                .unwrap()
        );

        let rejoined = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();
        assert!(!rejoined.leaving && !rejoined.compute_draining);
        assert!(rejoined.offers_compute(true));
        assert!(rejoined.supersedes(&departed));
    }

    #[tokio::test]
    async fn drain_survives_placement() {
        // An operator drain must outlive a return-to-placement observation:
        // otherwise the very next placement sync silently undrains the node.
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let local = node(1);
        write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;
        publish_node_info(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();

        assert!(
            set_operator_drain(&ctx, local, realm_id, true)
                .await
                .unwrap()
        );
        let drained = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();
        assert!(drained.compute_draining && !drained.leaving);

        // A departure and its return leave the operator drain in place.
        set_departure_state(&ctx, local, realm_id, true)
            .await
            .unwrap();
        set_departure_state(&ctx, local, realm_id, false)
            .await
            .unwrap();
        let observed = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();
        assert!(!observed.leaving);
        assert!(observed.compute_draining, "the operator drain must survive");

        assert!(
            set_operator_drain(&ctx, local, realm_id, false)
                .await
                .unwrap()
        );
        let released = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();
        assert!(!released.compute_draining);
    }

    #[tokio::test]
    async fn heartbeat_populates_utilization() {
        let dir = tempdir().unwrap();
        let ctx = test_ctx(dir.path().to_str().unwrap());
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let local = node(1);
        write_realm_config(&ctx, &realm_config(realm_id, &[local])).await;

        publish_node_info(
            &ctx,
            local,
            realm_id,
            NodeUrls {
                api: None,
                s3: None,
            },
        )
        .await
        .unwrap();
        refresh_node_info_heartbeat(&ctx, local, realm_id)
            .await
            .unwrap();

        let stored = read_node_info_document(&ctx.storage_handle, local)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.utilization.documents_held, Some(0));
        assert!(stored.utilization.load_permille.is_some());
    }
}
