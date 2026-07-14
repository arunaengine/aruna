use aruna_core::structs::WatchEvent;
use aruna_core::types::UserId;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::GetRealmConfigOperation;
use crate::notifications::client::deliver_watch_events_remote;
use crate::notifications::placement::filter_locally_held_watch_subscriptions;
use crate::notifications::watch::expand::expand_watch_events;
use crate::notifications::watch::interest::mark_watch_interest_dirty;
use crate::notifications::watch::subscriptions::list_realm_watch_subscriptions;

/// Post-commit, best-effort emission of an origin watch event. Matches the event
/// against the in-memory realm interest table plus local durable subscriptions
/// that may not have published their digest yet, then immediately expands it for
/// the local holder or forwards it once to each remote holder. Every failure
/// warns; nothing propagates and nothing panics, so a lost watch event never
/// affects the host operation. An unmatched event writes nothing.
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
    let local_realm_config = match include_local_holder_from_subscriptions(
        context,
        &event,
        net_handle.node_id(),
        &mut holders,
    )
    .await
    {
        Ok(realm_config) => Some(realm_config),
        Err(error) => {
            warn!(%error, "Failed to scan local watch subscriptions while emitting event");
            None
        }
    };
    if holders.is_empty() {
        return;
    }
    holders.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    holders.dedup();

    let local_node_id = net_handle.node_id();
    for holder in holders {
        if holder == local_node_id {
            let Some(realm_config) = local_realm_config.as_ref() else {
                continue;
            };
            match expand_watch_events(
                context,
                event.realm_id,
                realm_config,
                local_node_id,
                std::slice::from_ref(&event),
            )
            .await
            {
                Ok((outcome, dropped)) => {
                    for recipient in outcome.recipients {
                        net_handle.notify_inbox_activity(recipient);
                    }
                    if dropped
                        && let Err(error) = mark_watch_interest_dirty(context, event.realm_id).await
                    {
                        warn!(%error, "Failed to retract dropped local watch interest");
                    }
                }
                Err(error) => {
                    warn!(%error, "Failed to expand watch event for local holder");
                }
            }
        } else if let Err(error) =
            deliver_watch_events_remote(net_handle, holder, vec![event.clone()]).await
        {
            warn!(holder = %holder, %error, "Failed to forward watch event to remote holder");
        }
    }
}

async fn include_local_holder_from_subscriptions(
    context: &DriverContext,
    event: &WatchEvent,
    local_node_id: aruna_core::NodeId,
    holders: &mut Vec<aruna_core::NodeId>,
) -> Result<aruna_core::structs::RealmConfigDocument, String> {
    let subscriptions = list_realm_watch_subscriptions(&context.storage_handle, event.realm_id)
        .await
        .map_err(|error| error.to_string())?;
    let realm_config = drive(GetRealmConfigOperation::new(event.realm_id), context)
        .await
        .map_err(|error| error.to_string())?;
    let (subscriptions, found_stale) =
        filter_locally_held_watch_subscriptions(subscriptions, &realm_config, local_node_id)
            .map_err(|error| error.to_string())?;
    holders.retain(|holder| *holder != local_node_id);
    if subscriptions.iter().any(|subscription| {
        subscription.created_at_ms <= event.occurred_at_ms
            && subscription.event_mask.contains(event.kind)
            && event.path.starts_with(subscription.path_prefix.as_str())
    }) {
        holders.push(local_node_id);
    }
    if found_stale && let Err(error) = mark_watch_interest_dirty(context, event.realm_id).await {
        warn!(%error, "Failed to mark stale watch interest dirty while emitting event");
    }
    Ok(realm_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE};
    use aruna_core::metrics::NodeMetrics;
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, NotificationRecord, RealmAuthorizationDocument,
        RealmConfigDocument, RealmId, RealmNodeKind, WatchEventDetail, WatchEventKind,
        WatchEventMask, WatchInterestEntry, WatchInterestTable, data_watch_resource_path,
        parse_data_watch_resource_path,
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
        let resource = parse_data_watch_resource_path(path).expect("canonical data watch path");
        WatchEvent {
            event_id: Ulid::r#gen(),
            realm_id: realm,
            kind: WatchEventKind::DataUploaded,
            path: path.to_string(),
            actor,
            occurred_at_ms: 1_000,
            detail: WatchEventDetail::DataUploaded {
                group_id: resource.group_id,
                node_id: resource.node_id,
                bucket: resource.bucket.to_string(),
                key: resource.key_prefix.to_string(),
                size_bytes: 4,
            },
        }
    }

    async fn install_authorization(
        context: &DriverContext,
        realm: RealmId,
        group_id: Ulid,
        owner: UserId,
    ) {
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id: realm,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm);
        let group_auth = GroupAuthorizationDocument::new_default_group_doc(owner, realm, group_id);
        for (key, value) in [
            (
                realm.as_bytes().to_vec(),
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
        ] {
            assert!(matches!(
                context
                    .storage_handle
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
        let mut realm_config = RealmConfigDocument::default_for_realm(realm, Vec::new());
        realm_config.ensure_node(net.node_id(), RealmNodeKind::Server);
        let actor = Actor {
            node_id: net.node_id(),
            user_id: UserId::nil(realm),
            realm_id: realm,
        };
        storage
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: realm.as_bytes().to_vec().into(),
                value: realm_config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        let ctx = DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        (dir, ctx, net)
    }

    async fn read_inbox_rows(ctx: &DriverContext) -> Vec<NotificationRecord> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| NotificationRecord::from_bytes(&value).unwrap())
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
        let (_dir, ctx, net) = ctx_with_net(realm, [80u8; 32]).await;
        let actor = UserId::new(Ulid::r#gen(), realm);
        let path = data_watch_resource_path(Ulid::r#gen(), net.node_id(), "bucket", "object");
        emit_resource_watch_event(&ctx, upload_event(realm, actor, &path)).await;
        assert!(read_inbox_rows(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn local_subscription_matches_before_interest_publish() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm, [84u8; 32]).await;
        let metrics = NodeMetrics::new();
        net.notification_watch_metrics().register(&metrics).await;
        let owner = UserId::new(Ulid::r#gen(), realm);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let prefix = data_watch_resource_path(group_id, net.node_id(), "bucket", "");
        install_authorization(&ctx, realm, group_id, owner).await;
        create_watch_subscription(
            &ctx.storage_handle,
            owner,
            prefix.clone(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("subscription creates");

        let actor = UserId::new(Ulid::r#gen(), realm);
        let event_path = data_watch_resource_path(group_id, net.node_id(), "bucket", "object");
        emit_resource_watch_event(&ctx, upload_event(realm, actor, &event_path)).await;

        let rows = read_inbox_rows(&ctx).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].recipient, owner);
        assert_eq!(rows[0].created_at_ms, 1_000);
        assert_eq!(
            net.watch_interest_snapshot().matching_nodes(
                realm,
                &event_path,
                WatchEventKind::DataUploaded
            ),
            Vec::new(),
            "the local subscription has not needed a published digest"
        );

        install_authorization(&ctx, realm, group_id, UserId::new(Ulid::r#gen(), realm)).await;
        emit_resource_watch_event(&ctx, upload_event(realm, actor, &event_path)).await;
        assert_eq!(read_inbox_rows(&ctx).await.len(), 1);
        assert!(metrics.render().await.contains(
            "aruna_notification_watch_delivery_suppressions_total{reason=\"permission_denied\"} 1"
        ));
    }

    #[tokio::test]
    async fn nil_actor_is_skipped() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm, [82u8; 32]).await;
        let path = data_watch_resource_path(Ulid::r#gen(), net.node_id(), "bucket", "object");
        net.replace_watch_interest(interest_table(realm, node(9), &path));

        emit_resource_watch_event(&ctx, upload_event(realm, UserId::nil(realm), &path)).await;
        assert!(read_inbox_rows(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn local_expansion_error_is_swallowed() {
        let realm = RealmId([1u8; 32]);
        let (dir, ctx, net) = ctx_with_net(realm, [83u8; 32]).await;
        let path = data_watch_resource_path(Ulid::r#gen(), net.node_id(), "bucket", "object");
        net.replace_watch_interest(interest_table(realm, net.node_id(), &path));
        drop(dir);

        let actor = UserId::new(Ulid::r#gen(), realm);
        emit_resource_watch_event(&ctx, upload_event(realm, actor, &path)).await;
    }
}
