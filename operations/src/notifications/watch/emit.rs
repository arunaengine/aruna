use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::storage_entries::watch_forward_outbox_write_entry;
use aruna_core::structs::WatchEvent;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace, UserId, Value};
use tracing::warn;

use crate::driver::DriverContext;
use crate::notifications::watch::outbox::{
    new_watch_forward_outbox_record, schedule_watch_forward_outbox_drain_effect,
};
use crate::notifications::watch::subscriptions::list_realm_watch_subscriptions;

/// Post-commit, best-effort emission of an origin watch event. Matches the event
/// against the in-memory realm interest table plus local durable subscriptions
/// that may not have published their digest yet, writes one durable
/// forward-outbox row per interested holder node, and schedules the drain. Every
/// failure warns; nothing propagates and nothing panics, so a lost watch event
/// never affects the host operation. An unmatched event writes nothing.
pub async fn emit_resource_watch_event(context: &DriverContext, event: WatchEvent) {
    // Watches are a user-plane feature; system/anonymous writes carry a nil actor
    // and are deliberately not emitted.
    if event.actor == UserId::nil(event.realm_id) {
        return;
    }

    let Some(net_handle) = context.net_handle.as_ref() else {
        return;
    };
    let mut holders = net_handle.watch_interest_snapshot().matching_nodes(
        event.realm_id,
        &event.path,
        event.kind,
    );
    if let Err(error) =
        include_local_holder_from_subscriptions(context, &event, net_handle.node_id(), &mut holders)
            .await
    {
        warn!(%error, "Failed to scan local watch subscriptions while emitting event");
    }
    if holders.is_empty() {
        return;
    }
    holders.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    holders.dedup();

    let mut writes: Vec<(KeySpace, Key, Value)> = Vec::with_capacity(holders.len());
    for holder in holders {
        let record = new_watch_forward_outbox_record(holder, event.clone());
        match watch_forward_outbox_write_entry(&record) {
            Ok(entry) => writes.push(entry),
            Err(error) => warn!(%error, "Skipping unserializable watch forward outbox row"),
        }
    }
    if writes.is_empty() {
        return;
    }

    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(%error, "Failed to write watch forward outbox rows");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while writing watch forward outbox rows");
            return;
        }
    }

    schedule_watch_forward_outbox_drain(context).await;
}

async fn include_local_holder_from_subscriptions(
    context: &DriverContext,
    event: &WatchEvent,
    local_node_id: aruna_core::NodeId,
    holders: &mut Vec<aruna_core::NodeId>,
) -> Result<(), String> {
    if holders.contains(&local_node_id) {
        return Ok(());
    }

    let subscriptions = list_realm_watch_subscriptions(&context.storage_handle, event.realm_id)
        .await
        .map_err(|error| error.to_string())?;
    if subscriptions.iter().any(|subscription| {
        subscription.event_mask.contains(event.kind)
            && event.path.starts_with(subscription.path_prefix.as_str())
    }) {
        holders.push(local_node_id);
    }
    Ok(())
}

async fn schedule_watch_forward_outbox_drain(context: &DriverContext) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    if let Event::Task(TaskEvent::Error { message, .. }) = task_handle
        .send_effect(schedule_watch_forward_outbox_drain_effect())
        .await
    {
        warn!(message = %message, key = ?TaskKey::DrainNotificationWatchOutbox, "Failed to schedule watch forward outbox drain");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::keyspaces::NOTIFICATION_WATCH_OUTBOX_KEYSPACE;
    use aruna_core::structs::{
        RealmId, WatchEventDetail, WatchEventKind, WatchEventMask, WatchForwardOutboxRecord,
        WatchInterestEntry, WatchInterestTable,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::notifications::watch::subscriptions::create_watch_subscription;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn upload_event(realm: RealmId, actor: UserId, path: &str) -> WatchEvent {
        WatchEvent {
            event_id: Ulid::new(),
            realm_id: realm,
            kind: WatchEventKind::DataUploaded,
            path: path.to_string(),
            actor,
            occurred_at_ms: 1_000,
            detail: WatchEventDetail::DataUploaded {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                size_bytes: 4,
            },
        }
    }

    async fn ctx_with_net(realm: RealmId, secret: [u8; 32]) -> (TempDir, DriverContext, NetHandle) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
                realm_id: realm,
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
        };
        (dir, ctx, net)
    }

    async fn read_outbox_rows(ctx: &DriverContext) -> Vec<WatchForwardOutboxRecord> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_WATCH_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| WatchForwardOutboxRecord::from_bytes(&value).unwrap())
                .collect(),
            other => panic!("unexpected iter event: {other:?}"),
        }
    }

    fn interest_table(realm: RealmId, holder: NodeId, prefix: &str) -> WatchInterestTable {
        let mut table = WatchInterestTable::default();
        table.insert(
            realm,
            holder,
            vec![WatchInterestEntry {
                path_prefix: prefix.to_string(),
                event_mask: WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            }],
        );
        table
    }

    #[tokio::test]
    async fn empty_table_writes_nothing() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, _net) = ctx_with_net(realm, [80u8; 32]).await;
        let actor = UserId::new(Ulid::new(), realm);
        emit_resource_watch_event(&ctx, upload_event(realm, actor, "bucket/object")).await;
        assert!(read_outbox_rows(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn local_subscription_matches_before_interest_publish() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm, [84u8; 32]).await;
        let owner = UserId::new(Ulid::new(), realm);
        create_watch_subscription(
            &ctx.storage_handle,
            owner,
            "bucket/".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("subscription creates");

        let actor = UserId::new(Ulid::new(), realm);
        emit_resource_watch_event(&ctx, upload_event(realm, actor, "bucket/object")).await;

        let rows = read_outbox_rows(&ctx).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].holder_node, net.node_id());
    }

    #[tokio::test]
    async fn matching_interest_writes_one_row_per_holder() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm, [81u8; 32]).await;
        let holder = node(9);
        net.replace_watch_interest(interest_table(realm, holder, "bucket/"));

        let actor = UserId::new(Ulid::new(), realm);
        emit_resource_watch_event(&ctx, upload_event(realm, actor, "bucket/object")).await;

        let rows = read_outbox_rows(&ctx).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].holder_node, holder);
    }

    #[tokio::test]
    async fn nil_actor_is_skipped() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm, [82u8; 32]).await;
        net.replace_watch_interest(interest_table(realm, node(9), "bucket/"));

        emit_resource_watch_event(
            &ctx,
            upload_event(realm, UserId::nil(realm), "bucket/object"),
        )
        .await;
        assert!(read_outbox_rows(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn storage_error_is_swallowed() {
        // A closed storage handle makes the batch write fail; emission must not
        // propagate the error or panic.
        let realm = RealmId([1u8; 32]);
        let (dir, ctx, net) = ctx_with_net(realm, [83u8; 32]).await;
        net.replace_watch_interest(interest_table(realm, node(9), "bucket/"));
        drop(dir);

        let actor = UserId::new(Ulid::new(), realm);
        emit_resource_watch_event(&ctx, upload_event(realm, actor, "bucket/object")).await;
    }
}
