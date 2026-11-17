//! Publishes this node's usage snapshots, refreshes the realm summary and keeps dirty markers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::pin::pin;

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::usage::{
    DIRTY_GLOBAL_KEY, DIRTY_PREFIX, NodeUsageSnapshot, USAGE_GROUP_PREFIX, UsageCounters, dirty_group_id, dirty_group_key, dirty_share_id,
    usage_global_key, usage_group_id, usage_group_key, usage_node_id,
    usage_snapshot_key,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{GroupId, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use futures_util::TryStreamExt;
use tracing::warn;

use crate::distribution::usage::replica_shares;
use crate::driver::{DriverContext, drive};
use crate::node::realm_usage::RealmUsageOperation;
use crate::s3::usage::{
    LoadCountersError, dirty_rows, global_shard_read, global_sum, schedule_snapshot_publish,
};
use crate::storage_read::scan_pages;
use crate::sync::replicate_documents::{ReplicateDocumentsConfig, ReplicateDocumentsOperation};

/// Groups/global scope touched by a publish run, so callers can refresh exactly
/// the affected realm summary cache entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishedUsage {
    pub global: bool,
    pub groups: Vec<GroupId>,
}

impl PublishedUsage {
    pub fn is_empty(&self) -> bool {
        !self.global && self.groups.is_empty()
    }

    fn targets(&self, realm_id: RealmId, node_id: NodeId) -> Vec<DocumentTarget> {
        let mut targets = Vec::with_capacity(self.groups.len() + usize::from(self.global));
        if self.global {
            targets.push(DocumentTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: None,
            });
        }
        for group_id in &self.groups {
            targets.push(DocumentTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: Some(*group_id),
            });
        }
        targets
    }
}

pub(crate) async fn read_local_global(
    storage: &StorageHandle,
    txn_id: Option<TxnId>,
) -> Result<UsageCounters, String> {
    match storage.send_storage_effect(global_shard_read(txn_id)).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            global_sum::<LoadCountersError>(values).map_err(|error| error.to_string())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected global read event: {other:?}")),
    }
}

async fn read_local_group(
    storage: &StorageHandle,
    group_id: GroupId,
    txn_id: Option<TxnId>,
) -> Result<UsageCounters, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: Key::from(usage_group_key(group_id)),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => UsageCounters::from_bytes(bytes.as_ref()).map_err(|e| e.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Ok(UsageCounters::default())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected group read event: {other:?}")),
    }
}

/// Rebuilds this node's snapshot documents from live counters and distributes them
/// over the sync layer: dirty mode refreshes marked groups, full mode all groups plus
/// the global. Markers clear only after durable replication, never if re-dirtied.
pub async fn publish_usage_snapshots(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<PublishedUsage, String> {
    let (published, observed_markers) =
        publish_retaining_markers(ctx, node_id, realm_id, full).await?;
    clear_consumed_markers(&ctx.storage_handle, observed_markers).await?;
    Ok(published)
}

async fn publish_retaining_markers(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<(PublishedUsage, Vec<(Key, Value)>), String> {
    let storage = &ctx.storage_handle;

    let dirty_prefix = Some(Key::from(DIRTY_PREFIX.to_vec()));
    let mut pages = pin!(scan_pages(storage, NODE_STATS_KEYSPACE, dirty_prefix));
    let mut observed_markers = Vec::new();
    while let Some(page) = pages.try_next().await? {
        observed_markers.extend(page);
    }

    let mut groups: BTreeSet<GroupId> = BTreeSet::new();
    let mut global = false;
    for (key, _) in &observed_markers {
        if key.as_ref() == DIRTY_GLOBAL_KEY {
            global = true;
        } else if let Some(group_id) = dirty_group_id(key.as_ref()) {
            groups.insert(group_id);
        } else if let Some(group_id) = dirty_share_id(key.as_ref()) {
            // A share marker the scan saw without its group's marker still needs a publish.
            groups.insert(group_id);
        }
    }

    if full {
        global = true;
        let group_prefix = Some(Key::from(b"group/".to_vec()));
        let mut pages = pin!(scan_pages(storage, USAGE_STATS_KEYSPACE, group_prefix));
        while let Some(page) = pages.try_next().await? {
            for (key, _) in page {
                if let Some(rest) = key.as_ref().strip_prefix(b"group/")
                    && let Ok(bytes) = <[u8; 16]>::try_from(rest)
                {
                    groups.insert(GroupId::from_bytes(bytes));
                }
            }
        }
        // Republish snapshots missing a live counter as zero so peers stop summing stale data.
        let usage_prefix = Some(Key::from(USAGE_GROUP_PREFIX.to_vec()));
        let mut pages = pin!(scan_pages(storage, NODE_STATS_KEYSPACE, usage_prefix));
        while let Some(page) = pages.try_next().await? {
            for (key, _) in page {
                if usage_node_id(key.as_ref()) == Some(node_id)
                    && let Some(group_id) = usage_group_id(key.as_ref())
                {
                    groups.insert(group_id);
                }
            }
        }
    }

    if !global && groups.is_empty() {
        return Ok((PublishedUsage::default(), Vec::new()));
    }

    // Counters and replica shares come from one snapshot, as a purge debits both together.
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: true })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(format!("unexpected transaction answer {other:?}")),
    };
    let group_list: Vec<GroupId> = groups.into_iter().collect();
    let read = async {
        let shares = replica_shares(ctx, txn_id)
            .await
            .map_err(|e| e.to_string())?;
        let counters = if global {
            read_local_global(storage, Some(txn_id)).await?
        } else {
            UsageCounters::default()
        };
        let mut group_counters = Vec::with_capacity(group_list.len());
        for group_id in &group_list {
            group_counters.push(read_local_group(storage, *group_id, Some(txn_id)).await?);
        }
        Ok::<_, String>((shares, counters, group_counters))
    }
    .await;
    storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
    let (shares, counters, group_counters) = read?;
    let mut writes: Vec<(String, Key, Value)> = Vec::with_capacity(group_list.len() + 1);
    if global {
        let (mut replicas, mut revisions) = (UsageCounters::default(), BTreeMap::new());
        for share in shares.values() {
            replicas.add(&share.replicas).map_err(|e| e.to_string())?;
            revisions.extend(&share.revisions);
        }
        let snapshot = NodeUsageSnapshot {
            node_id,
            counters,
            replicas,
            revisions,
        };
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(usage_global_key(node_id)),
            Value::from(snapshot.to_bytes().map_err(|e| e.to_string())?),
        ));
    }
    for (group_id, counters) in group_list.iter().zip(group_counters) {
        let share = shares.get(group_id).cloned().unwrap_or_default();
        let snapshot = NodeUsageSnapshot {
            node_id,
            counters,
            replicas: share.replicas,
            revisions: share.revisions,
        };
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(usage_snapshot_key(*group_id, node_id)),
            Value::from(snapshot.to_bytes().map_err(|e| e.to_string())?),
        ));
    }

    // Persist the refreshed snapshots but keep the dirty markers until the
    // documents have been durably handed to replication.
    write_snapshot_documents(storage, writes).await?;

    let published = PublishedUsage {
        global,
        groups: group_list,
    };
    let targets = published.targets(realm_id, node_id);
    if let Err(error) = drive(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node_id,
            excluded_peers: Vec::new(),
            documents: targets,
            // Steady-state publishers must not mint the shared node-usage
            // genesis; bootstrap/placement-origin paths do that authoritatively.
            allow_genesis: false,
        }),
        ctx,
    )
    .await
    .map_err(|error| format!("node usage snapshot replication failed: {error}"))
    {
        if let Err(retry_error) = signal_snapshot_retry(ctx, &published, &observed_markers).await {
            return Err(format!(
                "{error}; additionally failed to mark usage snapshot retry: {retry_error}"
            ));
        }
        return Err(error);
    }

    Ok((published, observed_markers))
}

pub async fn publish_refresh_snapshots(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<PublishedUsage, String> {
    let (published, observed_markers) =
        publish_retaining_markers(ctx, node_id, realm_id, full).await?;
    if !published.is_empty()
        && let Err(error) = recompute_summary(ctx, node_id, published.global, published.groups.clone())
            .await
    {
        if let Err(retry_error) = signal_snapshot_retry(ctx, &published, &observed_markers).await {
            return Err(format!(
                "{error}; additionally failed to mark usage snapshot retry: {retry_error}"
            ));
        }
        return Err(error);
    }
    // Replication and summary refresh both succeeded; only now consume the
    // markers, and only those a concurrent write did not re-dirty meanwhile.
    clear_consumed_markers(&ctx.storage_handle, observed_markers).await?;
    Ok(published)
}

async fn signal_snapshot_retry(
    ctx: &DriverContext,
    published: &PublishedUsage,
    observed_markers: &[(Key, Value)],
) -> Result<(), String> {
    write_retry_markers(&ctx.storage_handle, published, observed_markers).await?;

    let Some(task_handle) = ctx.task_handle.as_ref() else {
        return Ok(());
    };
    match task_handle.send_effect(schedule_snapshot_publish()).await {
        Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(()),
        Event::Task(TaskEvent::Error { message, .. }) => {
            warn!(message = %message, "Failed to schedule usage snapshot retry");
            Ok(())
        }
        other => {
            warn!(event = ?other, "Unexpected usage snapshot retry schedule result");
            Ok(())
        }
    }
}

async fn write_retry_markers(
    storage: &StorageHandle,
    published: &PublishedUsage,
    observed_markers: &[(Key, Value)],
) -> Result<(), String> {
    if published.is_empty() {
        return Ok(());
    }

    let observed_keys: HashSet<Vec<u8>> = observed_markers
        .iter()
        .map(|(key, _)| key.as_ref().to_vec())
        .collect();
    let generation = Value::from(ulid::Ulid::generate().to_bytes().to_vec());
    let mut writes: Vec<(String, Key, Value)> =
        Vec::with_capacity(published.groups.len() + usize::from(published.global));
    if published.global && !observed_keys.contains(DIRTY_GLOBAL_KEY) {
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(DIRTY_GLOBAL_KEY.to_vec()),
            generation.clone(),
        ));
    }
    for group_id in &published.groups {
        let marker_key = dirty_group_key(*group_id);
        if observed_keys.contains(&marker_key) {
            continue;
        }
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(marker_key),
            generation.clone(),
        ));
    }
    if writes.is_empty() {
        return Ok(());
    }

    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("retry marker write failed: {other:?}")),
    }
}

/// Persists the refreshed snapshot documents. Each snapshot key has a single
/// writer (this node), so a plain batch write is enough.
async fn write_snapshot_documents(
    storage: &StorageHandle,
    writes: Vec<(String, Key, Value)>,
) -> Result<(), String> {
    if writes.is_empty() {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("snapshot write failed: {other:?}")),
    }
}

/// Deletes each observed dirty marker only if its stored generation still matches
/// the one seen at publish start. Re-reading inside the transaction makes a racing
/// counter update abort the commit, so its retry signal is never lost.
pub(crate) async fn clear_consumed_markers(
    storage: &StorageHandle,
    observed: Vec<(Key, Value)>,
) -> Result<(), String> {
    if observed.is_empty() {
        return Ok(());
    }

    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        return Err("failed to start marker cleanup transaction".to_string());
    };

    let reads = observed
        .iter()
        .map(|(key, _)| (NODE_STATS_KEYSPACE.to_string(), key.clone()))
        .collect();
    let current = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(format!("marker re-read failed: {other:?}"));
        }
    };

    let mut deletes: Vec<(String, Key)> = Vec::with_capacity(observed.len());
    for ((key, observed_generation), (_, current_value)) in observed.iter().zip(current) {
        if current_value.as_ref() == Some(observed_generation) {
            deletes.push((NODE_STATS_KEYSPACE.to_string(), key.clone()));
        }
    }

    if deletes.is_empty() {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Ok(());
    }

    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
        other => {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(format!("marker delete failed: {other:?}"));
        }
    }

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        // A concurrent counter update re-dirtied one of the observed markers; the
        // conflicting commit aborts and every marker survives for the next run.
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => Ok(()),
        other => Err(format!("marker cleanup commit failed: {other:?}")),
    }
}

/// Schedules the publish that consumes committed dirty markers.
pub(crate) async fn wake_publish(ctx: &DriverContext) {
    if let Some(task_handle) = ctx.task_handle.as_ref() {
        task_handle.send_effect(schedule_snapshot_publish()).await;
    }
}

/// Marks `groups` and the global for the next publish and schedules it.
pub(crate) async fn mark_dirty(
    ctx: &DriverContext,
    groups: impl IntoIterator<Item = GroupId>,
) -> Result<(), String> {
    let write = StorageEffect::BatchWrite {
        writes: dirty_rows(groups, ulid::Ulid::generate()),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("dirty marker write failed: {other:?}")),
    }
    wake_publish(ctx).await;
    Ok(())
}

/// Re-arms the debounced publish task when dirty markers survived a restart.
pub(crate) async fn restore_usage_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let has_markers = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NODE_STATS_KEYSPACE.to_string(),
            prefix: Some(Key::from(DIRTY_PREFIX.to_vec())),
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => !values.is_empty(),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan node usage dirty markers");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning node usage dirty markers");
            return;
        }
    };
    if has_markers
        && let Event::Task(TaskEvent::Error { message, .. }) =
            task_handle.send_effect(schedule_snapshot_publish()).await
    {
        warn!(message = %message, "Failed to restore node usage publish timer");
    }
}

/// Recomputes the persisted realm summary cache for the global total and the given groups.
async fn recompute_summary(
    ctx: &DriverContext,
    local_node_id: NodeId,
    include_global: bool,
    groups: Vec<GroupId>,
) -> Result<(), String> {
    let realm_id = ctx.node_and_realm().map(|(_, realm_id)| realm_id);
    let summary = RealmUsageOperation::summary(local_node_id, realm_id, include_global, groups);
    drive(summary, ctx)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}
