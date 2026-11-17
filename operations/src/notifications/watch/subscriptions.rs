//! Tests the durable watch subscriptions: the store, the per-owner cap and the listings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod tests {
    use crate::notifications::protocol::SUBSCRIPTION_SCAN_CAP;
    use crate::notifications::watch::listing::{
        ListWatchesOperation, SUBSCRIPTION_PAGE_LIMIT, WatchScope,
    };
    use crate::notifications::watch::records::WatchSubscriptionError;
    use crate::notifications::watch::records::decode_stored_subscription;
    use crate::tests::notifications::create_holder_watch;
    use crate::tests::notifications::{create_local_watch, delete_local_watch};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::storage_entries::watch_write_entry;
    use aruna_core::structs::execution::notification_watch::WATCH_USER_CAP;
    use aruna_core::structs::execution::notification_watch::{
        WatchAuthorizationBinding, WatchEventMask, WatchSubscription,
    };
    use aruna_storage::StorageHandle;
    use ulid::Ulid;

    async fn list_scope(
        storage: &StorageHandle,
        scope: WatchScope,
    ) -> Result<(Vec<WatchSubscription>, Option<Vec<u8>>), WatchSubscriptionError> {
        let list = ListWatchesOperation::new(scope);
        let page = crate::driver::drive(list, &context(storage)).await?;
        Ok((page.subscriptions, page.next))
    }

    async fn list_watch_subscriptions(
        storage: &StorageHandle,
        owner: UserId,
    ) -> Result<Vec<WatchSubscription>, WatchSubscriptionError> {
        Ok(list_scope(storage, WatchScope::Owner(owner)).await?.0)
    }

    async fn list_realm_subscriptions(
        storage: &StorageHandle,
        realm_id: RealmId,
    ) -> Result<Vec<WatchSubscription>, WatchSubscriptionError> {
        Ok(list_scope(storage, WatchScope::Realm(realm_id)).await?.0)
    }

    async fn list_watch_page(
        storage: &StorageHandle,
        realm_id: RealmId,
        start: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<(Vec<WatchSubscription>, Option<Vec<u8>>), WatchSubscriptionError> {
        let scope = WatchScope::Page {
            realm_id,
            start,
            limit,
        };
        list_scope(storage, scope).await
    }

    use crate::notifications::watch::records::WATCH_CAP_REACHED;
    use crate::tests::notifications::{context, temp_storage, user};
    use aruna_core::NodeId;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::execution::notification_watch::MAX_PREFIX_LEN;
    use aruna_core::structs::execution::notification_watch::{WatchEventKind, watch_resource_path};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    };

    fn mask() -> WatchEventMask {
        WatchEventMask::from_kinds([
            WatchEventKind::MetadataCreated,
            WatchEventKind::DataUploaded,
        ])
    }

    fn data_mask() -> WatchEventMask {
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded])
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    async fn install_auth(
        storage: &StorageHandle,
        realm_id: RealmId,
        owner: UserId,
        group_id: Ulid,
        node_id: NodeId,
    ) {
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
        // Policy loading resolves the group record before group policies apply.
        let group = Group {
            display_name: "watch".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        let realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        for (key_space, key, value) in [
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
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
            (
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_config.to_bytes(&actor).unwrap(),
            ),
        ] {
            match storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => panic!("unexpected auth write event: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn create_list_roundtrip() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 1);
        let created = create_local_watch(&storage, owner, "bucket".to_string(), mask(), 1_000)
            .await
            .expect("create succeeds");

        let listed = list_watch_subscriptions(&storage, owner)
            .await
            .expect("list succeeds");
        assert_eq!(listed, vec![created]);
    }

    #[tokio::test]
    async fn invalid_create_rejected() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 1);

        assert_eq!(
            create_local_watch(&storage, owner, String::new(), mask(), 1).await,
            Err(WatchSubscriptionError::EmptyPrefix)
        );
        assert_eq!(
            create_local_watch(&storage, owner, "/bucket".to_string(), mask(), 1).await,
            Err(WatchSubscriptionError::LeadingSlash)
        );
        assert_eq!(
            create_local_watch(&storage, owner, "x".repeat(MAX_PREFIX_LEN + 1), mask(), 1).await,
            Err(WatchSubscriptionError::PrefixTooLong)
        );
        assert_eq!(
            create_local_watch(
                &storage,
                owner,
                "bucket".to_string(),
                WatchEventMask::empty(),
                1
            )
            .await,
            Err(WatchSubscriptionError::EmptyMask)
        );
        assert!(
            list_watch_subscriptions(&storage, owner)
                .await
                .expect("list succeeds")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn create_enforces_cap() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 1);
        for index in 0..WATCH_USER_CAP {
            create_local_watch(&storage, owner, format!("p/{index}"), mask(), index as u64)
                .await
                .expect("create under cap succeeds");
        }
        assert_eq!(
            create_local_watch(&storage, owner, "overflow".to_string(), mask(), 9_999).await,
            Err(WatchSubscriptionError::CapExceeded)
        );
        assert_eq!(
            list_watch_subscriptions(&storage, owner)
                .await
                .expect("list succeeds")
                .len(),
            WATCH_USER_CAP
        );
    }

    // Revocation hides a watch but does not delete its replicated row. Those
    // durable rows keep occupying the owner's cap until explicit deletion.
    #[tokio::test]
    async fn revoked_rows_count() {
        let (_dir, storage) = temp_storage();
        let realm_id = RealmId([7u8; 32]);
        let owner = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let node_id = node(3);
        install_auth(&storage, realm_id, owner, group_id, node_id).await;

        let dead_prefix = watch_resource_path(Ulid::nil(), node_id, "bucket", "");
        for index in 0..WATCH_USER_CAP {
            create_local_watch(
                &storage,
                owner,
                dead_prefix.clone(),
                data_mask(),
                index as u64,
            )
            .await
            .expect("dead row create");
        }

        let driver_ctx = context(&storage);
        let authorized_prefix = watch_resource_path(group_id, node_id, "bucket", "reports/");
        assert_eq!(
            create_holder_watch(
                &driver_ctx,
                node_id,
                owner,
                authorized_prefix,
                data_mask(),
                WatchAuthorizationBinding::default(),
                9_999,
            )
            .await,
            Err(WatchSubscriptionError::CapExceeded)
        );
        assert_eq!(
            list_watch_subscriptions(&driver_ctx.storage_handle, owner)
                .await
                .expect("durable rows remain listable")
                .len(),
            WATCH_USER_CAP
        );
    }

    #[test]
    fn stored_identity_required() {
        let owner = user(1, 1);
        let watch_id = Ulid::from_bytes([9u8; 16]);
        let mut subscription = WatchSubscription::new(owner, "prefix".to_string(), mask(), 1);
        subscription.watch_id = watch_id;
        let bytes = subscription.to_bytes().expect("subscription encodes");
        let key = aruna_core::structs::execution::notification_watch::watch_subscription_key(
            owner, watch_id,
        );
        assert_eq!(
            decode_stored_subscription(&key, &bytes).expect("valid row"),
            subscription
        );

        let wrong_key = aruna_core::structs::execution::notification_watch::watch_subscription_key(
            owner,
            Ulid::generate(),
        );
        assert!(matches!(
            decode_stored_subscription(&wrong_key, &bytes),
            Err(WatchSubscriptionError::Storage(_))
        ));

        subscription.watch_id = Ulid::nil();
        let nil_key = aruna_core::structs::execution::notification_watch::watch_subscription_key(
            owner,
            Ulid::nil(),
        );
        assert!(matches!(
            decode_stored_subscription(
                &nil_key,
                &subscription.to_bytes().expect("nil row encodes")
            ),
            Err(WatchSubscriptionError::Storage(_))
        ));

        subscription.watch_id = watch_id;
        subscription.path_prefix.clear();
        assert!(matches!(
            decode_stored_subscription(
                &key,
                &subscription.to_bytes().expect("invalid row encodes")
            ),
            Err(WatchSubscriptionError::Storage(_))
        ));
    }

    #[tokio::test]
    async fn cap_owner_scoped() {
        let (_dir, storage) = temp_storage();
        let alice = user(1, 1);
        let bob = user(1, 2);
        for index in 0..WATCH_USER_CAP {
            create_local_watch(&storage, alice, format!("a/{index}"), mask(), index as u64)
                .await
                .expect("alice fills her cap");
        }
        create_local_watch(&storage, bob, "b".to_string(), mask(), 1)
            .await
            .expect("bob is unaffected by alice's cap");

        assert_eq!(
            list_watch_subscriptions(&storage, bob)
                .await
                .expect("list succeeds")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_cap() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 7);
        for index in 0..WATCH_USER_CAP - 1 {
            create_local_watch(&storage, owner, format!("p/{index}"), mask(), index as u64)
                .await
                .expect("prefill succeeds");
        }
        let (left, right) = tokio::join!(
            create_local_watch(&storage, owner, "p/left".to_string(), mask(), 99),
            create_local_watch(&storage, owner, "p/right".to_string(), mask(), 100),
        );
        assert_eq!(left.is_ok() as usize + right.is_ok() as usize, 1);
        assert_eq!(
            list_watch_subscriptions(&storage, owner)
                .await
                .expect("list succeeds")
                .len(),
            WATCH_USER_CAP
        );
    }

    #[tokio::test]
    async fn delete_owner_scoped() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 1);
        let created = create_local_watch(&storage, owner, "x".to_string(), mask(), 1)
            .await
            .expect("create succeeds");

        delete_local_watch(&storage, owner, created.watch_id)
            .await
            .expect("delete succeeds");
        delete_local_watch(&storage, owner, created.watch_id)
            .await
            .expect("deleting a missing row is ok");
        delete_local_watch(&storage, owner, Ulid::generate())
            .await
            .expect("deleting an unknown id is ok");

        assert!(
            list_watch_subscriptions(&storage, owner)
                .await
                .expect("list succeeds")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn realm_scan_complete() {
        let (_dir, storage) = temp_storage();
        let realm = RealmId([1u8; 32]);
        let other_realm = RealmId([2u8; 32]);
        let alice = user(1, 1);
        let bob = user(1, 2);
        let outsider = UserId::new(Ulid::from_bytes([3u8; 16]), other_realm);

        create_local_watch(&storage, alice, "a".to_string(), mask(), 1)
            .await
            .expect("alice create");
        create_local_watch(&storage, bob, "b".to_string(), mask(), 2)
            .await
            .expect("bob create");
        create_local_watch(&storage, outsider, "c".to_string(), mask(), 3)
            .await
            .expect("outsider create");

        let realm_subs = list_realm_subscriptions(&storage, realm)
            .await
            .expect("realm scan succeeds");
        assert_eq!(realm_subs.len(), 2);
        assert!(realm_subs.iter().all(|sub| sub.owner.realm_id == realm));

        let other_subs = list_realm_subscriptions(&storage, other_realm)
            .await
            .expect("other realm scan succeeds");
        assert_eq!(other_subs.len(), 1);
        assert_eq!(other_subs[0].owner, outsider);
    }

    #[tokio::test]
    async fn realm_scan_caps() {
        let (_dir, storage) = temp_storage();
        let realm = RealmId([9u8; 32]);
        // Owner and watch ids are offset by one: stored rows with nil identity
        // are rejected at decode time.
        let writes = (0..=SUBSCRIPTION_SCAN_CAP)
            .map(|index| {
                let owner = UserId::new(
                    Ulid::from_bytes([(index / WATCH_USER_CAP + 1) as u8; 16]),
                    realm,
                );
                let mut subscription =
                    WatchSubscription::new(owner, format!("p/{index}"), mask(), index as u64);
                subscription.watch_id = Ulid::from_bytes(((index + 1) as u128).to_be_bytes());
                watch_write_entry(&subscription).expect("subscription encodes")
            })
            .collect();
        assert!(matches!(
            storage
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes,
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));

        let (page, next) = list_watch_page(&storage, realm, None, SUBSCRIPTION_PAGE_LIMIT)
            .await
            .expect("first page succeeds");
        assert_eq!(page.len(), SUBSCRIPTION_PAGE_LIMIT);
        let (tail, _) = list_watch_page(&storage, realm, next, SUBSCRIPTION_PAGE_LIMIT)
            .await
            .expect("continuation succeeds");
        assert_eq!(tail.len(), SUBSCRIPTION_PAGE_LIMIT);

        assert!(matches!(
            list_realm_subscriptions(&storage, realm).await,
                Err(WatchSubscriptionError::Storage(reason))
                if reason.contains("subscription scan cap")
        ));
    }

    #[test]
    fn cap_reason_matches() {
        assert_eq!(
            WatchSubscriptionError::CapExceeded.to_string(),
            WATCH_CAP_REACHED
        );
    }
}
