//! Tests the watch event emission end to end against a local store and net handle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(test)]
mod tests {
    use crate::driver::{DriverContext, drive};
    use crate::notifications::watch::origin::EmitEventOperation;
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE};
    use aruna_core::metrics::NodeMetrics;
    use aruna_core::structs::execution::notification::NotificationRecord;
    use aruna_core::structs::execution::notification_watch::{
        WatchEvent, WatchEventDetail, WatchEventKind, WatchEventMask, WatchInterestEntry,
        WatchInterestTable, parse_watch_path, watch_resource_path,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::tests::notifications::create_local_watch;

    /// Failures only warn inside the operation, as they do for every caller.
    async fn emit_watch_event(context: &DriverContext, event: WatchEvent) {
        let _ = drive(EmitEventOperation::new(event), context).await;
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn upload_event(realm: RealmId, actor: UserId, path: &str) -> WatchEvent {
        let resource = parse_watch_path(path).expect("canonical data watch path");
        WatchEvent {
            event_id: Ulid::generate(),
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
        let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm);
        let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm, group_id);
        // Policy loading resolves the group record before group policies apply.
        let group = Group {
            display_name: "watch".to_string(),
            group_id,
            realm_id: realm,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        for (key_space, key, value) in [
            (
                AUTH_KEYSPACE,
                realm.as_bytes().to_vec(),
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            assert!(matches!(
                context
                    .storage_handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space: key_space.to_string(),
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
    async fn empty_table_noop() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm, [80u8; 32]).await;
        let actor = UserId::new(Ulid::generate(), realm);
        let path = watch_resource_path(Ulid::generate(), net.node_id(), "bucket", "object");
        emit_watch_event(&ctx, upload_event(realm, actor, &path)).await;
        assert!(read_inbox_rows(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn local_matches_early() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm, [84u8; 32]).await;
        let metrics = NodeMetrics::new();
        net.notification_watch_metrics().register(&metrics);
        let owner = UserId::new(Ulid::generate(), realm);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let prefix = watch_resource_path(group_id, net.node_id(), "bucket", "");
        install_authorization(&ctx, realm, group_id, owner).await;
        create_local_watch(
            &ctx.storage_handle,
            owner,
            prefix.clone(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("subscription creates");

        let actor = UserId::new(Ulid::generate(), realm);
        let event_path = watch_resource_path(group_id, net.node_id(), "bucket", "object");
        emit_watch_event(&ctx, upload_event(realm, actor, &event_path)).await;

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

        install_authorization(&ctx, realm, group_id, UserId::new(Ulid::generate(), realm)).await;
        emit_watch_event(&ctx, upload_event(realm, actor, &event_path)).await;
        assert_eq!(read_inbox_rows(&ctx).await.len(), 1);
        assert!(metrics.render().contains(
            "aruna_notification_watch_delivery_suppressions_total{reason=\"permission_denied\"} 1"
        ));
    }

    #[tokio::test]
    async fn group_prefix_delivers() {
        // A `meta/{group}/` watch fires for a dataset created anywhere in it,
        // and a retried create carrying the same event id delivers once.
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, _net) = ctx_with_net(realm, [86u8; 32]).await;
        let owner = UserId::new(Ulid::generate(), realm);
        let group_id = Ulid::from_bytes([5u8; 16]);
        install_authorization(&ctx, realm, group_id, owner).await;
        create_local_watch(
            &ctx.storage_handle,
            owner,
            format!("meta/{group_id}/"),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
            1,
        )
        .await
        .expect("subscription creates");

        let actor = UserId::new(Ulid::generate(), realm);
        let document_id = Ulid::generate();
        let event_id = Ulid::generate();
        for _ in 0..2 {
            let emit = EmitEventOperation::metadata_created(
                realm,
                actor,
                group_id,
                document_id,
                "datasets/project",
                event_id,
            );
            drive(emit, &ctx).await.expect("the emission runs");
        }

        let rows = read_inbox_rows(&ctx).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].recipient, owner);
    }

    #[tokio::test]
    async fn nil_actor_skipped() {
        let realm = RealmId([1u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm, [82u8; 32]).await;
        let path = watch_resource_path(Ulid::generate(), net.node_id(), "bucket", "object");
        net.replace_watch_interest(interest_table(realm, node(9), &path));

        emit_watch_event(&ctx, upload_event(realm, UserId::nil(realm), &path)).await;
        assert!(read_inbox_rows(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn expansion_error_swallowed() {
        let realm = RealmId([1u8; 32]);
        let (dir, ctx, net) = ctx_with_net(realm, [83u8; 32]).await;
        let path = watch_resource_path(Ulid::generate(), net.node_id(), "bucket", "object");
        net.replace_watch_interest(interest_table(realm, net.node_id(), &path));
        drop(dir);

        let actor = UserId::new(Ulid::generate(), realm);
        emit_watch_event(&ctx, upload_event(realm, actor, &path)).await;
    }
}
