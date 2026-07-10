use aruna_core::structs::{RealmId, WatchEvent};

use crate::driver::DriverContext;
use crate::notifications::inbox::{InboxWriteOutcome, upsert_inbox_records_reporting};
use crate::notifications::routing::route_watch_event;
use crate::notifications::watch::authorization::filter_authorized_watch_subscriptions;
use crate::notifications::watch::subscriptions::list_realm_watch_subscriptions;

/// Holder-side expansion of origin watch events into inbox records. Scans every
/// stored subscription for `realm_id`, retains only owners still assigned to the
/// local holder and still authorized for their canonical resource, routes each
/// event through [`route_watch_event`], and
/// idempotently upserts the resulting records.
/// Returns the write outcome (count plus the distinct recipients actually
/// written) plus whether stale or unauthorized rows were skipped so the caller
/// can wake live streams and retract stale interest. All events must be scoped to
/// `realm_id` (the transport gate enforces this).
pub async fn expand_watch_events(
    context: &DriverContext,
    realm_id: RealmId,
    realm_config: &aruna_core::structs::RealmConfigDocument,
    local_node_id: aruna_core::NodeId,
    events: &[WatchEvent],
) -> Result<(InboxWriteOutcome, bool), String> {
    if events.is_empty() {
        return Ok((InboxWriteOutcome::default(), false));
    }
    let subscriptions = list_realm_watch_subscriptions(&context.storage_handle, realm_id)
        .await
        .map_err(|error| error.to_string())?;
    let filtered = filter_authorized_watch_subscriptions(
        context,
        realm_id,
        realm_config,
        local_node_id,
        subscriptions,
    )
    .await?;
    if filtered.subscriptions.is_empty() {
        return Ok((InboxWriteOutcome::default(), filtered.dropped));
    }
    let mut records = Vec::new();
    for event in events {
        records.extend(route_watch_event(event, &filtered.subscriptions));
    }
    upsert_inbox_records_reporting(&context.storage_handle, &records)
        .await
        .map(|outcome| (outcome, filtered.dropped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE};
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, RealmAuthorizationDocument, RealmConfigDocument,
        RealmNodeKind, WatchEventDetail, WatchEventKind, WatchEventMask, data_watch_resource_path,
    };
    use aruna_core::types::UserId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::notifications::watch::subscriptions::create_watch_subscription;

    fn temp_context() -> (tempfile::TempDir, DriverContext) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (
            dir,
            DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
            },
        )
    }

    fn user(realm: RealmId, seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), realm)
    }

    fn local_config(realm: RealmId) -> (aruna_core::NodeId, RealmConfigDocument) {
        let local_node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let mut config = RealmConfigDocument::default_for_realm(realm, Vec::new());
        config.ensure_node(local_node_id, RealmNodeKind::Server);
        (local_node_id, config)
    }

    fn upload_event(
        realm: RealmId,
        actor: UserId,
        group_id: Ulid,
        node_id: aruna_core::NodeId,
    ) -> WatchEvent {
        let path = data_watch_resource_path(group_id, node_id, "bucket", "object");
        WatchEvent {
            event_id: Ulid::from_bytes([7u8; 16]),
            realm_id: realm,
            kind: WatchEventKind::DataUploaded,
            path,
            actor,
            occurred_at_ms: 1_000,
            detail: WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                size_bytes: 8,
            },
        }
    }

    async fn install_authorization(
        context: &DriverContext,
        realm: RealmId,
        node_id: aruna_core::NodeId,
        group_id: Ulid,
        owner: UserId,
    ) {
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

    async fn count_inbox(storage: &StorageHandle) -> usize {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
            other => panic!("unexpected iter event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn expansion_is_idempotent_across_redelivery() {
        let (_dir, context) = temp_context();
        let realm = RealmId([1u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let owner = user(realm, 1);
        let actor = user(realm, 2);
        let group_id = Ulid::from_bytes([3u8; 16]);
        install_authorization(&context, realm, local_node_id, group_id, owner).await;
        create_watch_subscription(
            &context.storage_handle,
            owner,
            data_watch_resource_path(group_id, local_node_id, "bucket", ""),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("create");

        let events = vec![upload_event(realm, actor, group_id, local_node_id)];
        let first = expand_watch_events(&context, realm, &config, local_node_id, &events)
            .await
            .expect("first delivery");
        assert_eq!(first.0.written, 1, "first delivery writes one record");
        assert_eq!(first.0.recipients, vec![owner]);
        assert!(!first.1);
        let second = expand_watch_events(&context, realm, &config, local_node_id, &events)
            .await
            .expect("redelivery");
        assert_eq!(second.0.written, 0, "redelivery writes nothing");
        assert!(second.0.recipients.is_empty());
        assert_eq!(count_inbox(&context.storage_handle).await, 1);
    }

    #[tokio::test]
    async fn expansion_without_subscriptions_writes_nothing() {
        let (_dir, context) = temp_context();
        let realm = RealmId([1u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let actor = user(realm, 2);
        let events = vec![upload_event(realm, actor, Ulid::new(), local_node_id)];
        assert_eq!(
            expand_watch_events(&context, realm, &config, local_node_id, &events)
                .await
                .expect("no subscriptions"),
            (InboxWriteOutcome::default(), false)
        );
        assert_eq!(count_inbox(&context.storage_handle).await, 0);
    }

    #[tokio::test]
    async fn expansion_stops_after_bucket_read_is_revoked() {
        let (_dir, context) = temp_context();
        let realm = RealmId([2u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let owner = user(realm, 1);
        let actor = user(realm, 2);
        let replacement_owner = user(realm, 3);
        let group_id = Ulid::from_bytes([4u8; 16]);
        install_authorization(&context, realm, local_node_id, group_id, owner).await;
        create_watch_subscription(
            &context.storage_handle,
            owner,
            data_watch_resource_path(group_id, local_node_id, "bucket", ""),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("create");

        let first_event = upload_event(realm, actor, group_id, local_node_id);
        let first = expand_watch_events(
            &context,
            realm,
            &config,
            local_node_id,
            std::slice::from_ref(&first_event),
        )
        .await
        .expect("first delivery");
        assert_eq!(first.0.written, 1);

        let replacement =
            GroupAuthorizationDocument::new_default_group_doc(replacement_owner, realm, group_id);
        let actor_record = Actor {
            node_id: local_node_id,
            user_id: replacement_owner,
            realm_id: realm,
        };
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: group_id.to_bytes().to_vec().into(),
                    value: replacement.to_bytes(&actor_record).unwrap().into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let mut second_event = first_event;
        second_event.event_id = Ulid::from_bytes([8u8; 16]);
        let second = expand_watch_events(&context, realm, &config, local_node_id, &[second_event])
            .await
            .expect("revoked delivery fails closed");
        assert_eq!(second.0.written, 0);
        assert!(second.1, "revoked subscription requests digest retraction");
        assert_eq!(count_inbox(&context.storage_handle).await, 1);
    }

    #[tokio::test]
    async fn unauthorized_subscription_does_not_block_authorized_owner() {
        let (_dir, context) = temp_context();
        let realm = RealmId([3u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let authorized_owner = user(realm, 1);
        let unauthorized_owner = user(realm, 2);
        let actor = user(realm, 3);
        let group_id = Ulid::from_bytes([5u8; 16]);
        let prefix = data_watch_resource_path(group_id, local_node_id, "bucket", "");
        install_authorization(&context, realm, local_node_id, group_id, authorized_owner).await;
        for owner in [authorized_owner, unauthorized_owner] {
            create_watch_subscription(
                &context.storage_handle,
                owner,
                prefix.clone(),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
                1,
            )
            .await
            .expect("create");
        }

        let outcome = expand_watch_events(
            &context,
            realm,
            &config,
            local_node_id,
            &[upload_event(realm, actor, group_id, local_node_id)],
        )
        .await
        .expect("authorized subscriptions still expand");

        assert_eq!(outcome.0.written, 1);
        assert_eq!(outcome.0.recipients, vec![authorized_owner]);
        assert!(outcome.1);
    }
}
