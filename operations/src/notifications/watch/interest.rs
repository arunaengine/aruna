use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{NOTIFICATION_WATCH_INTEREST_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::{
    RealmConfigDocument, RealmId, WATCH_INTEREST_DIRTY_PREFIX, WatchInterestDigest,
    WatchInterestEntry, WatchInterestTable, watch_interest_dirty_key,
    watch_interest_dirty_realm_id, watch_interest_key_node_id, watch_interest_key_realm_id,
    watch_interest_node_key, watch_interest_node_prefix, watch_interest_pending_key,
    watch_interest_realm_prefix,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::notifications::watch::authorization::filter_authorized_watch_subscriptions;
use crate::notifications::watch::subscriptions::list_realm_watch_subscriptions;
use crate::replicate_documents::{ReplicateDocumentsConfig, ReplicateDocumentsOperation};

/// Debounce window for the coalesced watch-interest publisher. `ShortenTimer`
/// makes the timer fire this long after the *first* dirty write of a burst and
/// keeps every later write inside the same window, so a run of watch CRUD
/// collapses into one publish with bounded latency.
pub const WATCH_INTEREST_PUBLISH_DEBOUNCE: Duration = Duration::from_secs(2);

/// Ensures this node has a document to announce when joining the shared
/// realm-scoped watch-interest topic. Existing digests may contain live watches
/// and are therefore never overwritten.
pub async fn ensure_local_watch_interest_digest(
    storage: &StorageHandle,
    realm_id: RealmId,
    node_id: NodeId,
) -> Result<(), String> {
    let key = Key::from(watch_interest_node_key(realm_id, node_id));
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => Ok(()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            let digest = WatchInterestDigest {
                node_id,
                entries: Vec::new(),
            };
            match storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                    key,
                    value: Value::from(digest.to_bytes().map_err(|error| error.to_string())?),
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
                Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
                other => Err(format!(
                    "unexpected local watch interest digest write result: {other:?}"
                )),
            }
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected local watch interest digest read result: {other:?}"
        )),
    }
}

/// Schedules (or shortens toward) the debounced watch-interest publish task.
pub fn schedule_watch_interest_publish_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PublishWatchInterest,
        after: WATCH_INTEREST_PUBLISH_DEBOUNCE,
    })
}

/// Dirty marker written, in the same transaction as a subscription row
/// write/delete, so the debounced publisher knows which realm's digest to
/// rebuild. The value is a fresh generation id: the publisher only clears a
/// marker whose stored generation still matches the one it observed, so a CRUD
/// that re-dirties a realm mid-publish keeps its retry signal.
pub fn watch_interest_dirty_marker_write(realm_id: RealmId) -> (KeySpace, Key, Value) {
    let generation = ByteView::from(Ulid::r#gen().to_bytes().to_vec());
    (
        NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
        ByteView::from(watch_interest_dirty_key(realm_id)),
        generation,
    )
}

/// Sends the debounced publish schedule from a CRUD call site. Best-effort: when
/// no task handle is wired the durable marker survives and the periodic re-arm
/// loop picks it up.
pub async fn schedule_watch_interest_publish(context: &DriverContext) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    if let Event::Task(TaskEvent::Error { message, .. }) = task_handle
        .send_effect(schedule_watch_interest_publish_effect())
        .await
    {
        warn!(message = %message, "Failed to schedule watch interest publish");
    }
}

/// Durably requests a digest rebuild after a stale subscription is observed.
pub async fn mark_watch_interest_dirty(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<(), String> {
    let (key_space, key, value) = watch_interest_dirty_marker_write(realm_id);
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {
            schedule_watch_interest_publish(context).await;
            Ok(())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected watch interest dirty marker write result: {other:?}"
        )),
    }
}

/// Rebuilds this node's watch-interest digest for every realm with a pending
/// dirty marker and distributes it over the sync layer. The digest is the union
/// of the path prefixes covered by every subscription the node holds for that
/// realm; an empty digest (the last watch was deleted) is still published so
/// peers drop the node's stale interest.
///
/// The dirty markers are only cleared after replication has durably accepted the
/// digests, and only for markers whose generation was not bumped by a concurrent
/// CRUD, so a failed publish or a racing write always leaves a retry signal
/// behind. Returns whether any digest was published.
pub async fn publish_watch_interest(ctx: &DriverContext, node_id: NodeId) -> Result<bool, String> {
    let storage = &ctx.storage_handle;

    let observed_markers = iter_all(
        storage,
        NOTIFICATION_WATCH_INTEREST_KEYSPACE,
        Some(Key::from(WATCH_INTEREST_DIRTY_PREFIX.to_vec())),
    )
    .await?;
    if observed_markers.is_empty() {
        return Ok(false);
    }

    let mut realms: BTreeSet<RealmId> = BTreeSet::new();
    for (key, _) in &observed_markers {
        if let Some(realm_id) = watch_interest_dirty_realm_id(key.as_ref()) {
            realms.insert(realm_id);
        }
    }
    if realms.is_empty() {
        // Markers are malformed; drop them so they cannot loop forever.
        clear_consumed_markers(storage, observed_markers).await?;
        return Ok(false);
    }

    let mut writes: Vec<(KeySpace, Key, Value)> = Vec::with_capacity(realms.len() * 2);
    let mut targets: Vec<(RealmId, DocumentSyncTarget)> = Vec::with_capacity(realms.len());
    let mut authorization_retries = Vec::new();
    for realm_id in &realms {
        let realm_config = drive(GetRealmConfigOperation::new(*realm_id), ctx)
            .await
            .map_err(|error| format!("failed to read realm config for watch interest: {error}"))?;
        let (digest, check_failed) =
            build_realm_digest(ctx, node_id, *realm_id, &realm_config).await?;
        if check_failed {
            authorization_retries.push(*realm_id);
        }
        let digest_key = Key::from(watch_interest_node_key(*realm_id, node_id));
        let pending_key = Key::from(watch_interest_pending_key(*realm_id));
        let digest_value = Value::from(digest.to_bytes().map_err(|e| e.to_string())?);
        let current = match storage
            .send_storage_effect(StorageEffect::BatchRead {
                reads: vec![
                    (
                        NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                        digest_key.clone(),
                    ),
                    (
                        NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                        pending_key.clone(),
                    ),
                ],
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchReadResult { values }) => values,
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("watch interest digest read failed: {other:?}")),
        };
        let [(_, current_digest), (_, pending)] = current.as_slice() else {
            return Err("watch interest digest read count mismatch".to_string());
        };
        let changed = current_digest.as_ref() != Some(&digest_value);
        if !changed && pending.is_none() {
            continue;
        }
        if changed {
            writes.push((
                NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                digest_key,
                digest_value,
            ));
            writes.push((
                NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                pending_key,
                Value::from(Vec::new()),
            ));
        }
        targets.push((
            *realm_id,
            DocumentSyncTarget::WatchInterest {
                realm_id: *realm_id,
                node_id,
            },
        ));
    }

    // Persist changed digests with pending markers, but keep the dirty markers
    // until the documents have been durably handed to replication.
    write_documents(storage, writes).await?;

    // Each realm rides its own realm-scoped topic, so replicate per realm.
    let published = !targets.is_empty();
    for (realm_id, target) in targets {
        drive(
            ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
                realm_id,
                local_node_id: node_id,
                excluded_peers: Vec::new(),
                documents: vec![target],
                // Watch-interest digests ride one shared realm topic; steady-state
                // digest publishers must not fork genesis.
                allow_genesis: false,
            }),
            ctx,
        )
        .await
        .map_err(|error| format!("watch interest replication failed: {error}"))?;
        match storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(watch_interest_pending_key(realm_id)),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => {
                return Err(format!(
                    "watch interest pending marker delete failed: {other:?}"
                ));
            }
        }
    }

    // Preserve a retry generation when a permission lookup failed. The current
    // fail-closed digest still retracts stale positive interest immediately.
    for realm_id in authorization_retries {
        mark_watch_interest_dirty(ctx, realm_id).await?;
    }

    // Replication accepted the digests; only now consume the markers, and only
    // those a concurrent CRUD did not re-dirty in the meantime.
    clear_consumed_markers(storage, observed_markers).await?;

    Ok(published)
}

/// Builds this node's digest for one realm from subscriptions whose owners are
/// still assigned to this node and retain READ on their canonical resources. A
/// realm with no valid subscriptions yields an empty digest.
async fn build_realm_digest(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    realm_config: &RealmConfigDocument,
) -> Result<(WatchInterestDigest, bool), String> {
    let subscriptions = list_realm_watch_subscriptions(&ctx.storage_handle, realm_id)
        .await
        .map_err(|error| error.to_string())?;
    let filtered =
        filter_authorized_watch_subscriptions(ctx, realm_id, realm_config, node_id, subscriptions)
            .await?;
    Ok((
        WatchInterestDigest::from_subscriptions(
            node_id,
            filtered
                .subscriptions
                .into_iter()
                .map(|subscription| (subscription.path_prefix, subscription.event_mask)),
        ),
        filtered.check_failed,
    ))
}

/// Persists changed digests and their pending markers atomically.
async fn write_documents(
    storage: &StorageHandle,
    writes: Vec<(KeySpace, Key, Value)>,
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
        other => Err(format!("watch interest digest write failed: {other:?}")),
    }
}

/// Deletes each observed dirty marker, but only if its stored generation still
/// matches the one seen when the publish run started. Re-reading the markers
/// inside the write transaction makes fjall abort the commit if a concurrent
/// CRUD re-dirtied any of them after they were observed, so a racing write never
/// loses its retry signal.
async fn clear_consumed_markers(
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
        return Err("failed to start watch interest marker cleanup transaction".to_string());
    };

    let reads = observed
        .iter()
        .map(|(key, _)| {
            (
                NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key.clone(),
            )
        })
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
            return Err(format!("watch interest marker re-read failed: {other:?}"));
        }
    };

    let mut deletes: Vec<(KeySpace, Key)> = Vec::with_capacity(observed.len());
    for ((key, observed_generation), (_, current_value)) in observed.iter().zip(current) {
        if current_value.as_ref() == Some(observed_generation) {
            deletes.push((
                NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key.clone(),
            ));
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
            return Err(format!("watch interest marker delete failed: {other:?}"));
        }
    }

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        // A concurrent CRUD re-dirtied one of the observed markers; the
        // conflicting commit aborts and every marker survives for the next run.
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => Ok(()),
        other => Err(format!(
            "watch interest marker cleanup commit failed: {other:?}"
        )),
    }
}

/// Re-arms the debounced publish task when dirty markers survived a restart.
pub async fn restore_watch_interest_publish_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
) {
    let has_markers = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Some(Key::from(WATCH_INTEREST_DIRTY_PREFIX.to_vec())),
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => !values.is_empty(),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan watch interest dirty markers");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning watch interest dirty markers");
            return;
        }
    };
    if has_markers
        && let Event::Task(TaskEvent::Error { message, .. }) = task_handle
            .send_effect(schedule_watch_interest_publish_effect())
            .await
    {
        warn!(message = %message, "Failed to restore watch interest publish timer");
    }
}

/// Rebuilds the full in-memory watch-interest table from the replicated digests
/// in local storage. Digests whose embedded node id disagrees with their key are
/// skipped defensively, and empty digests contribute nothing.
pub async fn rebuild_watch_interest_table(storage: &StorageHandle) -> WatchInterestTable {
    let mut table = WatchInterestTable::default();
    let mut eligible_by_realm: HashMap<RealmId, Option<HashSet<NodeId>>> = HashMap::new();
    let entries = match iter_all(
        storage,
        NOTIFICATION_WATCH_INTEREST_KEYSPACE,
        Some(Key::from(watch_interest_node_prefix())),
    )
    .await
    {
        Ok(entries) => entries,
        Err(error) => {
            warn!(error = %error, "Failed to scan watch interest digests");
            return table;
        }
    };
    for (key, value) in entries {
        let Some(realm_id) = watch_interest_key_realm_id(key.as_ref()) else {
            continue;
        };
        let key_node_id = watch_interest_key_node_id(key.as_ref());
        let digest = match WatchInterestDigest::from_bytes(value.as_ref()) {
            Ok(digest) => digest,
            Err(error) => {
                warn!(error = %error, "Failed to decode watch interest digest");
                continue;
            }
        };
        // Never trust a digest whose embedded node id disagrees with its key.
        if key_node_id != Some(digest.node_id) {
            continue;
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = eligible_by_realm.entry(realm_id)
        {
            let eligible = match sync_eligible_nodes(storage, realm_id).await {
                Ok(eligible) => Some(eligible),
                Err(error) => {
                    warn!(%realm_id, %error, "Ignoring watch interest without a current realm config");
                    None
                }
            };
            entry.insert(eligible);
        }
        if !eligible_by_realm
            .get(&realm_id)
            .and_then(Option::as_ref)
            .is_some_and(|eligible| eligible.contains(&digest.node_id))
        {
            continue;
        }
        table.insert(realm_id, digest.node_id, digest.entries);
    }
    table
}

/// Refreshes the in-memory watch-interest cache for realms whose interest or
/// membership changed, and schedules local digest rebuilds when replicated
/// subscriptions or placement membership changed. Mirrors
/// `refresh_realm_usage_summary_for_targets`: shared by every reconcile handler
/// (inbound apply, durable outbox drain, and the `SyncDocument` timer) so a
/// digest that lands on any of those paths updates the origin-side table.
pub async fn refresh_watch_interest_for_targets(
    ctx: &DriverContext,
    targets: &[DocumentSyncTarget],
) {
    let Some(net_handle) = ctx.net_handle.as_ref() else {
        return;
    };
    let mut realms: BTreeSet<RealmId> = BTreeSet::new();
    let mut dirty_realms: BTreeSet<RealmId> = BTreeSet::new();
    for target in targets {
        match target {
            DocumentSyncTarget::WatchInterest { realm_id, .. } => {
                realms.insert(*realm_id);
            }
            DocumentSyncTarget::WatchSubscription { owner, .. } => {
                dirty_realms.insert(owner.realm_id);
            }
            DocumentSyncTarget::GroupAuthorization { .. } => {
                dirty_realms.insert(*net_handle.realm_id());
            }
            DocumentSyncTarget::RealmAuthorization { realm_id } => {
                dirty_realms.insert(*realm_id);
            }
            DocumentSyncTarget::RealmConfig { realm_id } => {
                realms.insert(*realm_id);
                dirty_realms.insert(*realm_id);
            }
            _ => {}
        }
    }
    for realm_id in dirty_realms {
        if let Err(error) = mark_watch_interest_dirty(ctx, realm_id).await {
            warn!(%realm_id, %error, "Failed to schedule watch interest rebuild after document reconciliation");
        }
    }
    if realms.is_empty() {
        return;
    }
    for realm_id in realms {
        match build_realm_node_map(&ctx.storage_handle, realm_id).await {
            Ok(nodes) => net_handle.update_watch_interest_realm(realm_id, nodes),
            Err(error) => {
                // Membership is a security boundary. Fail closed instead of
                // retaining a previously cached removed node.
                net_handle.update_watch_interest_realm(realm_id, HashMap::new());
                warn!(%realm_id, error = %error, "Cleared watch interest after failed realm refresh")
            }
        }
    }
}

/// Builds one realm's node -> entries map from the digests in the realm's scan
/// range, dropping mismatched and empty digests.
async fn build_realm_node_map(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<HashMap<NodeId, Vec<WatchInterestEntry>>, String> {
    let eligible = sync_eligible_nodes(storage, realm_id).await?;
    let entries = iter_all(
        storage,
        NOTIFICATION_WATCH_INTEREST_KEYSPACE,
        Some(Key::from(watch_interest_realm_prefix(realm_id))),
    )
    .await?;
    let mut nodes: HashMap<NodeId, Vec<WatchInterestEntry>> = HashMap::new();
    for (key, value) in entries {
        let key_node_id = watch_interest_key_node_id(key.as_ref());
        let digest = match WatchInterestDigest::from_bytes(value.as_ref()) {
            Ok(digest) => digest,
            Err(error) => {
                warn!(%realm_id, %error, "Skipping malformed watch interest digest");
                continue;
            }
        };
        if key_node_id != Some(digest.node_id)
            || !eligible.contains(&digest.node_id)
            || digest.entries.is_empty()
        {
            continue;
        }
        nodes.insert(digest.node_id, digest.entries);
    }
    Ok(nodes)
}

async fn sync_eligible_nodes(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<HashSet<NodeId>, String> {
    let value = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: Key::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => value,
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            return Err(format!("realm config {realm_id} not found"));
        }
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected realm config read event: {other:?}")),
    };
    RealmConfigDocument::from_bytes(value.as_ref())
        .map_err(|error| error.to_string())?
        .sync_eligible_node_ids()
        .map(|nodes| nodes.into_iter().collect())
        .map_err(|error| error.to_string())
}

async fn iter_all(
    storage: &StorageHandle,
    key_space: &str,
    prefix: Option<Key>,
) -> Result<Vec<(Key, Value)>, String> {
    let mut collected = Vec::new();
    let mut start = None;
    loop {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: prefix.clone(),
                start: start.map(IterStart::After),
                limit: 1_000,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                collected.extend(values);
                match next_start_after {
                    Some(next) => start = Some(next),
                    None => break,
                }
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected iter event: {other:?}")),
        }
    }
    Ok(collected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, RealmAuthorizationDocument, RealmId, RealmNodeKind,
        WatchEventKind, WatchEventMask, WatchInterestEntry,
    };
    use aruna_core::types::UserId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use tempfile::{TempDir, tempdir};

    use crate::notifications::watch::subscriptions::{
        create_watch_subscription, delete_watch_subscription,
    };

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

    async fn ctx_with_net(
        realm_id: RealmId,
        secret: [u8; 32],
    ) -> (TempDir, DriverContext, NetHandle) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let ctx = DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (dir, ctx, net)
    }

    async fn write_digest(ctx: &DriverContext, realm_id: RealmId, digest: &WatchInterestDigest) {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(watch_interest_node_key(realm_id, digest.node_id)),
                value: Value::from(digest.to_bytes().unwrap()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected digest write event: {other:?}"),
        }
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user(realm: u8, seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId([realm; 32]))
    }

    fn mask() -> WatchEventMask {
        WatchEventMask::from_kinds([WatchEventKind::MetadataCreated])
    }

    fn metadata_prefix(group_id: Ulid, suffix: &str) -> String {
        format!("meta/{group_id}/{suffix}")
    }

    async fn install_authorization(
        ctx: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        group_id: Ulid,
        owner: UserId,
        readers: &[UserId],
    ) {
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let mut group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        group_auth
            .roles
            .values_mut()
            .find(|role| role.name == "viewer")
            .unwrap()
            .assigned_users
            .extend(readers.iter().copied());
        for (key, value) in [
            (
                realm_id.as_bytes().to_vec(),
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
        ] {
            assert!(matches!(
                ctx.storage_handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space: AUTH_KEYSPACE.to_string(),
                        key: key.into(),
                        value: value.into(),
                        txn_id: None,
                    })
                    .await,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
        }
    }

    async fn install_realm_config(
        ctx: &DriverContext,
        realm_id: RealmId,
        nodes: &[NodeId],
    ) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        let actor = Actor {
            node_id: nodes[0],
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => config,
            other => panic!("unexpected realm config write result: {other:?}"),
        }
    }

    fn user_for_holder(realm_id: RealmId, config: &RealmConfigDocument, holder: NodeId) -> UserId {
        for seed in 1..50_000u128 {
            let candidate = UserId::new(Ulid::from_bytes(seed.to_be_bytes()), realm_id);
            if crate::notifications::placement::resolve_inbox_holder(&candidate, config).unwrap()
                == Some(holder)
            {
                return candidate;
            }
        }
        panic!("no user resolved to holder {holder}");
    }

    async fn read_marker(ctx: &DriverContext, realm_id: RealmId) -> Option<Value> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(watch_interest_dirty_key(realm_id)),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected marker read event: {other:?}"),
        }
    }

    async fn read_pending(ctx: &DriverContext, realm_id: RealmId) -> Option<Value> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(watch_interest_pending_key(realm_id)),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected pending marker read event: {other:?}"),
        }
    }

    async fn read_digest(
        ctx: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
    ) -> Option<WatchInterestDigest> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(watch_interest_node_key(realm_id, node_id)),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => Some(WatchInterestDigest::from_bytes(bytes.as_ref()).unwrap()),
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => None,
            other => panic!("unexpected digest read event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_sets_dirty_marker_for_owner_realm() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let owner = user(1, 2);
        let group_id = Ulid::r#gen();

        create_watch_subscription(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create succeeds");

        assert!(read_marker(&ctx, owner.realm_id).await.is_some());
    }

    #[tokio::test]
    async fn publish_builds_digest_and_clears_markers() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(5);
        let owner = user(1, 2);
        install_realm_config(&ctx, owner.realm_id, &[node_id]).await;
        let group_id = Ulid::r#gen();
        install_authorization(&ctx, owner.realm_id, node_id, group_id, owner, &[]).await;

        create_watch_subscription(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create a");
        create_watch_subscription(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "b"),
            mask(),
            2,
        )
        .await
        .expect("create b");

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );

        let digest = read_digest(&ctx, owner.realm_id, node_id)
            .await
            .expect("digest written");
        assert_eq!(digest.node_id, node_id);
        let prefixes: Vec<&str> = digest
            .entries
            .iter()
            .map(|entry| entry.path_prefix.as_str())
            .collect();
        assert_eq!(
            prefixes,
            vec![
                metadata_prefix(group_id, "a"),
                metadata_prefix(group_id, "b")
            ]
        );

        // Markers consumed once the digest is durable.
        assert!(read_marker(&ctx, owner.realm_id).await.is_none());
        // Nothing left to publish.
        assert!(
            !publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );
    }

    // An acknowledged unchanged digest consumes a fresh dirty marker without
    // another replication handoff.
    #[tokio::test]
    async fn unchanged_skips_publish() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([7u8; 32]);
        let node_id = node(5);
        let owner = user(7, 2);
        let group_id = Ulid::r#gen();
        install_realm_config(&ctx, realm_id, &[node_id]).await;
        install_authorization(&ctx, realm_id, node_id, group_id, owner, &[]).await;
        create_watch_subscription(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create");

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );
        let before = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("digest")
            .to_bytes()
            .unwrap();
        assert!(read_pending(&ctx, realm_id).await.is_none());

        mark_watch_interest_dirty(&ctx, realm_id)
            .await
            .expect("mark dirty");
        assert!(
            !publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );

        let after = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("digest")
            .to_bytes()
            .unwrap();
        assert_eq!(after, before);
        assert!(read_marker(&ctx, realm_id).await.is_none());
        assert!(read_pending(&ctx, realm_id).await.is_none());
    }

    // Persistent authorization unavailability retains retries while identical
    // empty digests stop producing replicated events.
    #[tokio::test]
    async fn retry_stays_dirty() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([8u8; 32]);
        let node_id = node(6);
        let owner = user(8, 2);
        let group_id = Ulid::r#gen();
        install_realm_config(&ctx, realm_id, &[node_id]).await;
        create_watch_subscription(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create");

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );
        assert!(read_marker(&ctx, realm_id).await.is_some());
        let digest = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("empty digest");
        assert!(digest.entries.is_empty());
        let before = digest.to_bytes().unwrap();

        assert!(
            !publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );
        let after = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("empty digest")
            .to_bytes()
            .unwrap();
        assert_eq!(after, before);
        assert!(read_marker(&ctx, realm_id).await.is_some());
    }

    #[tokio::test]
    async fn deleting_last_watch_publishes_empty_digest() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(5);
        let owner = user(1, 2);
        install_realm_config(&ctx, owner.realm_id, &[node_id]).await;
        let group_id = Ulid::r#gen();
        install_authorization(&ctx, owner.realm_id, node_id, group_id, owner, &[]).await;

        let created = create_watch_subscription(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create");
        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );

        delete_watch_subscription(&ctx.storage_handle, owner, created.watch_id)
            .await
            .expect("delete");
        assert!(read_marker(&ctx, owner.realm_id).await.is_some());

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );
        let digest = read_digest(&ctx, owner.realm_id, node_id)
            .await
            .expect("empty digest is still written");
        assert!(digest.entries.is_empty());
    }

    #[tokio::test]
    async fn digest_omits_subscriptions_that_re_ranked_to_another_holder() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([2u8; 32]);
        let local_node_id = node(5);
        let remote_node_id = node(6);
        let config = install_realm_config(&ctx, realm_id, &[local_node_id, remote_node_id]).await;
        let local_owner = user_for_holder(realm_id, &config, local_node_id);
        let stale_owner = user_for_holder(realm_id, &config, remote_node_id);
        let group_id = Ulid::r#gen();
        install_authorization(
            &ctx,
            realm_id,
            local_node_id,
            group_id,
            local_owner,
            &[stale_owner],
        )
        .await;

        create_watch_subscription(
            &ctx.storage_handle,
            local_owner,
            metadata_prefix(group_id, "local"),
            mask(),
            1,
        )
        .await
        .unwrap();
        create_watch_subscription(
            &ctx.storage_handle,
            stale_owner,
            metadata_prefix(group_id, "stale"),
            mask(),
            2,
        )
        .await
        .unwrap();

        let digest = build_realm_digest(&ctx, local_node_id, realm_id, &config)
            .await
            .unwrap()
            .0;

        assert_eq!(digest.entries.len(), 1);
        assert_eq!(
            digest.entries[0].path_prefix,
            metadata_prefix(group_id, "local")
        );
    }

    #[tokio::test]
    async fn marker_clear_respects_generation_guard() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([1u8; 32]);

        // Two distinct generations for the same realm marker: only the observed
        // generation should be cleared.
        let (space, key, first) = watch_interest_dirty_marker_write(realm_id);
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: space,
                key: key.clone(),
                value: first.clone(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write: {other:?}"),
        }

        // A concurrent CRUD bumped the generation after we observed `first`.
        let observed = vec![(key.clone(), first)];
        let (space2, key2, second) = watch_interest_dirty_marker_write(realm_id);
        assert_eq!(key2, key);
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: space2,
                key: key2,
                value: second,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write: {other:?}"),
        }

        clear_consumed_markers(&ctx.storage_handle, observed)
            .await
            .expect("clear runs");

        // The re-dirtied marker survived because its generation moved on.
        assert!(read_marker(&ctx, realm_id).await.is_some());
    }

    #[tokio::test]
    async fn rebuild_reads_digests_from_storage() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([3u8; 32]);
        let holder = node(11);
        install_realm_config(&ctx, realm_id, &[holder]).await;

        write_digest(
            &ctx,
            realm_id,
            &WatchInterestDigest {
                node_id: holder,
                entries: vec![WatchInterestEntry {
                    path_prefix: "bucket/".to_string(),
                    event_mask: mask(),
                }],
            },
        )
        .await;
        // A positive digest from a node outside current membership must not add
        // a matchable holder.
        write_digest(
            &ctx,
            realm_id,
            &WatchInterestDigest {
                node_id: node(12),
                entries: vec![WatchInterestEntry {
                    path_prefix: "bucket/".to_string(),
                    event_mask: mask(),
                }],
            },
        )
        .await;

        let table = rebuild_watch_interest_table(&ctx.storage_handle).await;
        assert_eq!(
            table.matching_nodes(realm_id, "bucket/object", WatchEventKind::MetadataCreated),
            vec![holder]
        );
        assert_eq!(table.nodes(realm_id).map(|nodes| nodes.len()), Some(1));
    }

    // A consumed empty digest must be re-dirtied by group authorization
    // reconciliation so a later READ grant restores remote interest.
    #[tokio::test]
    async fn regrant_restores_digest() {
        let realm_id = RealmId([6u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm_id, [72u8; 32]).await;
        let node_id = net.node_id();
        install_realm_config(&ctx, realm_id, &[node_id]).await;
        let group_id = Ulid::r#gen();
        let auth_owner = user(6, 3);
        let watch_owner = user(6, 2);
        let prefix = metadata_prefix(group_id, "a");
        install_authorization(&ctx, realm_id, node_id, group_id, auth_owner, &[]).await;
        create_watch_subscription(&ctx.storage_handle, watch_owner, prefix.clone(), mask(), 1)
            .await
            .expect("create");

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );
        assert!(
            read_digest(&ctx, realm_id, node_id)
                .await
                .expect("empty digest")
                .entries
                .is_empty()
        );
        assert!(read_marker(&ctx, realm_id).await.is_none());

        install_authorization(
            &ctx,
            realm_id,
            node_id,
            group_id,
            auth_owner,
            &[watch_owner],
        )
        .await;
        refresh_watch_interest_for_targets(
            &ctx,
            &[DocumentSyncTarget::GroupAuthorization { group_id }],
        )
        .await;

        assert!(read_marker(&ctx, realm_id).await.is_some());
        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );
        let digest = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("restored digest");
        assert_eq!(digest.entries.len(), 1);
        assert_eq!(digest.entries[0].path_prefix, prefix);
    }

    #[tokio::test]
    async fn refresh_updates_the_net_handle_table() {
        let realm_id = RealmId([4u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm_id, [70u8; 32]).await;
        let holder = node(13);
        install_realm_config(&ctx, realm_id, &[holder]).await;

        assert!(net.watch_interest_snapshot().is_empty());

        write_digest(
            &ctx,
            realm_id,
            &WatchInterestDigest {
                node_id: holder,
                entries: vec![WatchInterestEntry {
                    path_prefix: "data/".to_string(),
                    event_mask: mask(),
                }],
            },
        )
        .await;

        let target = DocumentSyncTarget::WatchInterest {
            realm_id,
            node_id: holder,
        };
        refresh_watch_interest_for_targets(&ctx, &[target]).await;

        let snapshot = net.watch_interest_snapshot();
        assert_eq!(
            snapshot.matching_nodes(realm_id, "data/x", WatchEventKind::MetadataCreated),
            vec![holder]
        );
    }

    #[tokio::test]
    async fn realm_config_refresh_retracts_removed_digest_holder() {
        let realm_id = RealmId([5u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm_id, [71u8; 32]).await;
        let retained = node(14);
        let removed = node(15);
        install_realm_config(&ctx, realm_id, &[retained, removed]).await;
        for holder in [retained, removed] {
            write_digest(
                &ctx,
                realm_id,
                &WatchInterestDigest {
                    node_id: holder,
                    entries: vec![WatchInterestEntry {
                        path_prefix: "data/".to_string(),
                        event_mask: mask(),
                    }],
                },
            )
            .await;
        }
        refresh_watch_interest_for_targets(
            &ctx,
            &[DocumentSyncTarget::WatchInterest {
                realm_id,
                node_id: retained,
            }],
        )
        .await;
        assert_eq!(
            net.watch_interest_snapshot().matching_nodes(
                realm_id,
                "data/x",
                WatchEventKind::MetadataCreated,
            ),
            vec![retained, removed]
        );

        install_realm_config(&ctx, realm_id, &[retained]).await;
        refresh_watch_interest_for_targets(&ctx, &[DocumentSyncTarget::RealmConfig { realm_id }])
            .await;

        assert_eq!(
            net.watch_interest_snapshot().matching_nodes(
                realm_id,
                "data/x",
                WatchEventKind::MetadataCreated,
            ),
            vec![retained]
        );
        assert!(read_digest(&ctx, realm_id, removed).await.is_some());
    }
}
