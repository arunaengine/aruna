use aruna_core::structs::{RealmId, WatchEvent};
use aruna_storage::StorageHandle;

use crate::notifications::inbox::upsert_inbox_records;
use crate::notifications::routing::route_watch_event;
use crate::notifications::watch::subscriptions::list_realm_watch_subscriptions;

/// Holder-side expansion of origin watch events into inbox records. Scans every
/// local watch subscription for `realm_id`, routes each event through
/// [`route_watch_event`], and idempotently upserts the resulting records.
/// Returns the number of newly written records. All events must already be
/// scoped to `realm_id` (the transport gate enforces this).
pub async fn expand_watch_events(
    storage: &StorageHandle,
    realm_id: RealmId,
    events: &[WatchEvent],
) -> Result<usize, String> {
    if events.is_empty() {
        return Ok(0);
    }
    let subscriptions = list_realm_watch_subscriptions(storage, realm_id)
        .await
        .map_err(|error| error.to_string())?;
    if subscriptions.is_empty() {
        return Ok(0);
    }
    let mut records = Vec::new();
    for event in events {
        records.extend(route_watch_event(event, &subscriptions));
    }
    upsert_inbox_records(storage, &records).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
    use aruna_core::structs::{WatchEventDetail, WatchEventKind, WatchEventMask};
    use aruna_core::types::UserId;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::notifications::watch::subscriptions::create_watch_subscription;

    fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (dir, storage)
    }

    fn user(realm: RealmId, seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), realm)
    }

    fn upload_event(realm: RealmId, actor: UserId, path: &str) -> WatchEvent {
        WatchEvent {
            event_id: Ulid::from_bytes([7u8; 16]),
            realm_id: realm,
            kind: WatchEventKind::DataUploaded,
            path: path.to_string(),
            actor,
            occurred_at_ms: 1_000,
            detail: WatchEventDetail::DataUploaded {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                size_bytes: 8,
            },
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
        let (_dir, storage) = temp_storage();
        let realm = RealmId([1u8; 32]);
        let owner = user(realm, 1);
        let actor = user(realm, 2);
        create_watch_subscription(
            &storage,
            owner,
            "bucket/".to_string(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            1,
        )
        .await
        .expect("create");

        let events = vec![upload_event(realm, actor, "bucket/object")];
        assert_eq!(
            expand_watch_events(&storage, realm, &events).await,
            Ok(1),
            "first delivery writes one record"
        );
        assert_eq!(
            expand_watch_events(&storage, realm, &events).await,
            Ok(0),
            "redelivery writes nothing"
        );
        assert_eq!(count_inbox(&storage).await, 1);
    }

    #[tokio::test]
    async fn expansion_without_subscriptions_writes_nothing() {
        let (_dir, storage) = temp_storage();
        let realm = RealmId([1u8; 32]);
        let actor = user(realm, 2);
        let events = vec![upload_event(realm, actor, "bucket/object")];
        assert_eq!(expand_watch_events(&storage, realm, &events).await, Ok(0));
        assert_eq!(count_inbox(&storage).await, 0);
    }
}
