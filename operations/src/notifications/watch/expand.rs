//! Expands watch events into inbox records and retries the deliveries that failed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::WATCH_INTEREST_KEYSPACE;
use aruna_core::metrics::WatchMetricReason;
use aruna_core::structs::execution::notification::InboxWriteOutcome;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventRetry, watch_retry_prefix,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use tracing::warn;

use crate::driver::{DriverContext, drive, drive_retry};
use crate::notifications::protocol::{RETRY_BATCH_CAP, RETRY_BYTES_CAP, valid_retry};
use crate::notifications::watch::digest::InterestOperation;
use crate::notifications::watch::expansion::{ExpandError, ExpandOutcome, WatchExpandOperation};

/// Runs one expansion, once more after a conflict, and records its delivery suppressions.
async fn expand_once(
    context: &DriverContext,
    build: impl FnMut() -> WatchExpandOperation,
    seed: &[u8],
) -> Result<ExpandOutcome, ExpandError> {
    let expanded = drive_retry(build, context, seed).await;
    match &expanded {
        Ok(outcome) => {
            for reason in &outcome.suppressed {
                record_delivery_suppression(context, *reason);
            }
        }
        Err(ExpandError::Unavailable(_)) => {
            record_delivery_suppression(context, WatchMetricReason::AuthorizationUnavailable);
        }
        Err(_) => {}
    }
    expanded
}

/// Expands one bounded subscription page into idempotent inbox records.
/// Later pages are durably retried; placement and authorization are rechecked.
/// Returns the write outcome and whether stale or unauthorized rows were skipped.
pub(crate) async fn expand_watch_events(
    context: &DriverContext,
    realm_id: RealmId,
    realm_config: &RealmConfigDocument,
    local_node_id: NodeId,
    events: &[WatchEvent],
) -> Result<(InboxWriteOutcome, bool), String> {
    let build = || {
        let events = events.to_vec();
        WatchExpandOperation::new(realm_id, realm_config.clone(), local_node_id, events, None)
    };
    let expanded = expand_once(context, build, realm_id.as_bytes()).await;
    expanded
        .map(|outcome| (outcome.inbox, outcome.dropped))
        .map_err(|error| error.to_string())
}

pub(crate) async fn drain_watch_events(
    context: &DriverContext,
    realm_id: RealmId,
    realm_config: &RealmConfigDocument,
    local_node_id: NodeId,
) -> Result<(), String> {
    let values = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Some(watch_retry_prefix(realm_id).into()),
            start: None,
            limit: RETRY_BATCH_CAP.saturating_add(1),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(defer_retry(context, realm_id, error.to_string()).await);
        }
        other => {
            let error = format!("unexpected watch retry scan event: {}", other.label());
            return Err(defer_retry(context, realm_id, error).await);
        }
    };
    let over_batch = values.len() > RETRY_BATCH_CAP;
    if over_batch {
        warn!(%realm_id, "Watch retry queue row cap reached");
    }
    let mut failed = over_batch;
    for (key, value) in values.into_iter().take(RETRY_BATCH_CAP) {
        let key = key.to_vec();
        if value.len() > RETRY_BYTES_CAP {
            warn!(%realm_id, "Retaining watch retry row over byte cap");
            failed = true;
            continue;
        }
        let retry: WatchEventRetry = match postcard::from_bytes(&value) {
            Ok(retry) if valid_retry(&retry, realm_id, &key) => retry,
            Ok(_) | Err(_) => {
                warn!(%realm_id, "Retaining malformed watch retry row");
                failed = true;
                continue;
            }
        };
        let build = || {
            let resume = Some((key.clone(), retry.cursor.clone()));
            let events = retry.events.clone();
            WatchExpandOperation::new(
                realm_id,
                realm_config.clone(),
                local_node_id,
                events,
                resume,
            )
        };
        if let Err(error) = expand_once(context, build, &key).await {
            warn!(%realm_id, %error, "Watch retry row deferred");
            failed = true;
        }
    }
    if failed {
        let error = "watch retry drain deferred work".to_string();
        return Err(defer_retry(context, realm_id, error).await);
    }
    Ok(())
}

async fn defer_retry(context: &DriverContext, realm_id: RealmId, error: String) -> String {
    if let Err(marker) = drive(InterestOperation::mark(realm_id), context).await {
        warn!(%realm_id, %marker, "Failed to retain watch retry marker");
        return format!("{error}; failed to retain retry marker: {marker}");
    }
    error
}

fn record_delivery_suppression(context: &DriverContext, reason: WatchMetricReason) {
    if let Some(net_handle) = context.net_handle.as_ref() {
        net_handle
            .notification_watch_metrics()
            .record_delivery_suppression(reason);
    }
    warn!(
        parent: None,
        reason = reason.as_str(),
        "Notification watch delivery suppressed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notifications::protocol::WATCH_PAGE_LIMIT;
    use aruna_core::UserId;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::structs::execution::notification_watch::{
        WatchAuthorizationBinding, WatchEventDetail, WatchEventKind, WatchEventMask,
        watch_resource_path,
    };
    use aruna_core::structs::identity::auth::{Actor, Permission};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
    };
    use aruna_core::structs::storage::blob::object_permission_path;
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::notifications::watch::subscriptions::create_holder_watch;
    use crate::tests::notifications::create_local_watch;

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
                graph_handle: None,
                task_handle: None,
                compute_handle: None,
                outbound_handle: None,
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
        let path = watch_resource_path(group_id, node_id, "bucket", "object");
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

    #[test]
    fn expansion_caps_work() {
        assert!(expansion_budget(65, 64).is_err());
    }

    #[test]
    fn expansion_checks_overflow() {
        assert!(expansion_budget(usize::MAX, 2).is_err());
    }

    #[test]
    fn expansion_caps_candidates() {
        assert!(
            add_limit(
                EXPANSION_CANDIDATE_CAP,
                1,
                EXPANSION_CANDIDATE_CAP,
                "candidate"
            )
            .is_err()
        );
    }

    #[test]
    fn expansion_caps_records() {
        assert!(add_limit(EXPANSION_RECORD_CAP, 1, EXPANSION_RECORD_CAP, "record").is_err());
    }

    #[test]
    fn page_caps_work() {
        // A full batch pages at the tightest cap so the in-loop candidate and
        // record budgets can never abort a dense expansion mid-stream.
        assert_eq!(page_limit(1).expect("one event fits"), WATCH_PAGE_LIMIT);
        assert_eq!(
            page_limit(EVENT_BATCH_SIZE).unwrap(),
            EXPANSION_CANDIDATE_CAP / EVENT_BATCH_SIZE
        );
        assert!(page_limit(usize::MAX).is_err());
    }

    #[test]
    fn retry_key_scope() {
        let realm = RealmId([3u8; 32]);
        let event = upload_event(realm, user(realm, 1), Ulid::nil(), local_config(realm).0);
        let retry = WatchEventRetry {
            events: vec![event.clone()],
            cursor: Some(
                watch_subscription_key(user(realm, 2), Ulid::from_bytes([9u8; 16])).to_vec(),
            ),
        };
        let key = watch_retry_key(realm, event.event_id);
        assert!(valid_retry(&retry, realm, &key));
        assert!(!valid_retry(&retry, RealmId([4u8; 32]), &key));
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
        let mut realm_config = RealmConfigDocument::default_for_realm(realm, Vec::new());
        realm_config.ensure_node(node_id, RealmNodeKind::Server);
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
            (
                REALM_CONFIG_KEYSPACE,
                realm.as_bytes().to_vec(),
                realm_config.to_bytes(&actor).unwrap(),
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

    async fn retry_rows(storage: &StorageHandle, realm: RealmId) -> Vec<(Vec<u8>, Vec<u8>)> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                prefix: Some(watch_retry_prefix(realm).into()),
                start: None,
                limit: 64,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
            other => panic!("unexpected iter event: {other:?}"),
        }
    }

    async fn write_retry(context: &DriverContext, key: &[u8], retry: &WatchEventRetry) {
        let value = postcard::to_allocvec(retry).expect("encode retry");
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                    key: key.to_vec().into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
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
    async fn expansion_idempotent() {
        let (_dir, context) = temp_context();
        let realm = RealmId([1u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let owner = user(realm, 1);
        let actor = user(realm, 2);
        let group_id = Ulid::from_bytes([3u8; 16]);
        install_authorization(&context, realm, local_node_id, group_id, owner).await;
        let expired_binding = WatchAuthorizationBinding {
            expires_at_secs: 1,
            ..Default::default()
        };
        create_holder_watch(
            &context,
            local_node_id,
            owner,
            watch_resource_path(group_id, local_node_id, "bucket", ""),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            expired_binding,
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
    async fn no_subscriptions_noop() {
        let (_dir, context) = temp_context();
        let realm = RealmId([1u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let actor = user(realm, 2);
        let events = vec![upload_event(realm, actor, Ulid::generate(), local_node_id)];
        assert_eq!(
            expand_watch_events(&context, realm, &config, local_node_id, &events)
                .await
                .expect("no subscriptions"),
            (InboxWriteOutcome::default(), false)
        );
        assert_eq!(count_inbox(&context.storage_handle).await, 0);
    }

    #[tokio::test]
    async fn exact_deny_suppresses() {
        let (_dir, context) = temp_context();
        let realm = RealmId([4u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let owner = user(realm, 1);
        let actor = user(realm, 2);
        let group_id = Ulid::from_bytes([6u8; 16]);
        install_authorization(&context, realm, local_node_id, group_id, owner).await;
        create_local_watch(
            &context.storage_handle,
            owner,
            watch_resource_path(group_id, local_node_id, "bucket", ""),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("create");

        let mut group = GroupAuthorizationDocument::default_group_doc(owner, realm, group_id);
        group
            .roles
            .values_mut()
            .find(|role| role.name == "admin")
            .expect("admin role")
            .permissions
            .insert(
                object_permission_path(realm, group_id, local_node_id, "bucket", "object"),
                Permission::DENY,
            );
        let actor_record = Actor {
            node_id: local_node_id,
            user_id: owner,
            realm_id: realm,
        };
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: group_id.to_bytes().to_vec().into(),
                    value: group.to_bytes(&actor_record).unwrap().into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let outcome = expand_watch_events(
            &context,
            realm,
            &config,
            local_node_id,
            &[upload_event(realm, actor, group_id, local_node_id)],
        )
        .await
        .expect("denied event is suppressed");
        assert_eq!(outcome.0.written, 0);
        assert_eq!(count_inbox(&context.storage_handle).await, 0);
    }

    #[tokio::test]
    async fn revocation_prevents_delivery() {
        let (_dir, context) = temp_context();
        let realm = RealmId([2u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let owner = user(realm, 1);
        let actor = user(realm, 2);
        let replacement_owner = user(realm, 3);
        let group_id = Ulid::from_bytes([4u8; 16]);
        install_authorization(&context, realm, local_node_id, group_id, owner).await;
        create_local_watch(
            &context.storage_handle,
            owner,
            watch_resource_path(group_id, local_node_id, "bucket", ""),
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

        let mut second_event = first_event;
        second_event.event_id = Ulid::from_bytes([8u8; 16]);
        let replacement =
            GroupAuthorizationDocument::default_group_doc(replacement_owner, realm, group_id);
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
        let second = expand_watch_events(&context, realm, &config, local_node_id, &[second_event])
            .await
            .expect("revoked delivery fails closed");
        assert_eq!(second.0.written, 0);
        assert!(second.1, "revoked subscription requests digest retraction");
        assert_eq!(count_inbox(&context.storage_handle).await, 1);
    }

    #[tokio::test]
    async fn unauthorized_subscription_skipped() {
        let (_dir, context) = temp_context();
        let realm = RealmId([3u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let authorized_owner = user(realm, 1);
        let unauthorized_owner = user(realm, 2);
        let actor = user(realm, 3);
        let group_id = Ulid::from_bytes([5u8; 16]);
        let prefix = watch_resource_path(group_id, local_node_id, "bucket", "");
        install_authorization(&context, realm, local_node_id, group_id, authorized_owner).await;
        for owner in [authorized_owner, unauthorized_owner] {
            create_local_watch(
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

    #[tokio::test]
    async fn paging_defers_remainder() {
        // Owners past the first subscription page must survive as a durable retry
        // row and still be delivered by the drain, exactly once.
        let (_dir, context) = temp_context();
        let realm = RealmId([5u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let actor = user(realm, 2);
        let group_id = Ulid::from_bytes([6u8; 16]);
        let late_owner = user(realm, 200);
        install_authorization(&context, realm, local_node_id, group_id, late_owner).await;

        let mut events = vec![upload_event(realm, actor, group_id, local_node_id)];
        while events.len() < EVENT_BATCH_SIZE {
            let mut quiet = upload_event(realm, actor, group_id, local_node_id);
            let mut bytes = [9u8; 16];
            bytes[..8].copy_from_slice(&(events.len() as u64).to_be_bytes());
            quiet.event_id = Ulid::from_bytes(bytes);
            quiet.path = watch_resource_path(group_id, local_node_id, "quiet", "object");
            events.push(quiet);
        }
        let page = page_limit(events.len()).expect("page limit");
        for seed in 1..=page {
            create_local_watch(
                &context.storage_handle,
                user(realm, seed as u8),
                watch_resource_path(group_id, local_node_id, "idle", ""),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
                1,
            )
            .await
            .expect("create");
        }
        create_local_watch(
            &context.storage_handle,
            late_owner,
            watch_resource_path(group_id, local_node_id, "bucket", ""),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("create");

        let deferred = expand_watch_events(&context, realm, &config, local_node_id, &events)
            .await
            .expect("first page commits");
        assert_eq!(deferred.0.written, 0, "the match sits past the first page");
        assert_eq!(count_inbox(&context.storage_handle).await, 0);
        let rows = retry_rows(&context.storage_handle, realm).await;
        assert_eq!(rows.len(), 1, "the unscanned remainder is queued");
        assert_eq!(rows[0].0, watch_retry_key(realm, events[0].event_id));

        drain_watch_events(&context, realm, &config, local_node_id)
            .await
            .expect("drain resumes at the cursor");
        assert_eq!(count_inbox(&context.storage_handle).await, 1);
        assert!(
            retry_rows(&context.storage_handle, realm).await.is_empty(),
            "a completed retry row is removed"
        );

        drain_watch_events(&context, realm, &config, local_node_id)
            .await
            .expect("empty drain");
        assert_eq!(count_inbox(&context.storage_handle).await, 1);
    }

    #[tokio::test]
    async fn drain_rejects_foreign() {
        // Only the carried realm changes between the rejected and applied row, so
        // the refusal can only come from the realm scope check.
        let (_dir, context) = temp_context();
        let realm = RealmId([6u8; 32]);
        let foreign_realm = RealmId([7u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let owner = user(realm, 1);
        let group_id = Ulid::from_bytes([8u8; 16]);
        install_authorization(&context, realm, local_node_id, group_id, owner).await;
        create_local_watch(
            &context.storage_handle,
            owner,
            watch_resource_path(group_id, local_node_id, "bucket", ""),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("create");

        let local = upload_event(realm, user(realm, 2), group_id, local_node_id);
        let mut foreign = local.clone();
        foreign.realm_id = foreign_realm;
        let key = watch_retry_key(realm, foreign.event_id);
        let cursor = Some(watch_subscription_key(user(realm, 0), Ulid::nil()).to_vec());
        write_retry(
            &context,
            &key,
            &WatchEventRetry {
                events: vec![foreign],
                cursor: cursor.clone(),
            },
        )
        .await;
        assert!(
            drain_watch_events(&context, realm, &config, local_node_id)
                .await
                .is_err()
        );
        assert_eq!(retry_rows(&context.storage_handle, realm).await.len(), 1);
        assert_eq!(count_inbox(&context.storage_handle).await, 0);

        write_retry(
            &context,
            &key,
            &WatchEventRetry {
                events: vec![local],
                cursor,
            },
        )
        .await;
        drain_watch_events(&context, realm, &config, local_node_id)
            .await
            .expect("in-realm row applies");
        assert!(retry_rows(&context.storage_handle, realm).await.is_empty());
        assert_eq!(count_inbox(&context.storage_handle).await, 1);
    }

    #[tokio::test]
    async fn batch_cap_rejected() {
        let (_dir, context) = temp_context();
        let realm = RealmId([11u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let event = upload_event(
            realm,
            user(realm, 2),
            Ulid::from_bytes([3u8; 16]),
            local_node_id,
        );
        let events = vec![event; EVENT_BATCH_SIZE + 1];

        let error = expand_watch_events(&context, realm, &config, local_node_id, &events)
            .await
            .expect_err("over-cap batch");
        assert!(error.contains("exceeds cap"), "{error}");
        assert_eq!(count_inbox(&context.storage_handle).await, 0);
    }
}
