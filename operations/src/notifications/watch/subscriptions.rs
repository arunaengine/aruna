use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE;
use aruna_core::storage_entries::{
    watch_subscription_delete_entry, watch_subscription_write_entry,
};
use aruna_core::structs::{
    NOTIFICATION_WATCH_MAX_PREFIX_LEN, NOTIFICATION_WATCH_PER_USER_CAP, RealmId, WatchEventMask,
    WatchSubscription, watch_subscription_prefix,
};
use aruna_core::types::{TxnId, UserId};
use aruna_storage::StorageHandle;
use thiserror::Error;
use ulid::Ulid;

use crate::notifications::watch::interest::watch_interest_dirty_marker_write;

/// Single owner-prefix scan bound. Watches are hard-capped per user, so one page
/// always covers a subscription set with a wide safety margin.
const WATCH_SUBSCRIPTION_LIST_LIMIT: usize = 256;

/// Stable reject reason for a cap-exceeded create; matched verbatim by the
/// holder proxy to surface a 409 to the API layer.
pub const WATCH_SUBSCRIPTION_CAP_REACHED: &str = "notification watch subscription cap reached";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WatchSubscriptionError {
    #[error("watch path prefix must not be empty")]
    EmptyPrefix,
    #[error("watch path prefix must not start with a slash")]
    LeadingSlash,
    #[error("watch path prefix exceeds maximum length")]
    PrefixTooLong,
    #[error("watch event mask must select at least one event")]
    EmptyMask,
    #[error("notification watch subscription cap reached")]
    CapExceeded,
    #[error("{0}")]
    Storage(String),
}

enum CreateFailure {
    Cap,
    Conflict,
    Fatal(String),
}

pub async fn create_watch_subscription(
    storage: &StorageHandle,
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
    now_ms: u64,
) -> Result<WatchSubscription, WatchSubscriptionError> {
    if path_prefix.is_empty() {
        return Err(WatchSubscriptionError::EmptyPrefix);
    }
    // Emitted event paths carry no leading slash (`{bucket}/{key}`,
    // `meta/{group_id}/{document_id}`), so a leading-slash prefix could never
    // match; reject it as a backstop behind the API-layer validation.
    if path_prefix.starts_with('/') {
        return Err(WatchSubscriptionError::LeadingSlash);
    }
    if path_prefix.len() > NOTIFICATION_WATCH_MAX_PREFIX_LEN {
        return Err(WatchSubscriptionError::PrefixTooLong);
    }
    if event_mask.is_empty() {
        return Err(WatchSubscriptionError::EmptyMask);
    }

    let subscription = WatchSubscription::new(owner, path_prefix, event_mask, now_ms);
    match create_once(storage, &subscription).await {
        Ok(()) => Ok(subscription),
        Err(CreateFailure::Cap) => Err(WatchSubscriptionError::CapExceeded),
        Err(CreateFailure::Fatal(error)) => Err(WatchSubscriptionError::Storage(error)),
        Err(CreateFailure::Conflict) => match create_once(storage, &subscription).await {
            Ok(()) => Ok(subscription),
            Err(CreateFailure::Cap) => Err(WatchSubscriptionError::CapExceeded),
            Err(CreateFailure::Fatal(error)) => Err(WatchSubscriptionError::Storage(error)),
            Err(CreateFailure::Conflict) => Err(WatchSubscriptionError::Storage(
                "watch subscription create conflicted twice".to_string(),
            )),
        },
    }
}

async fn create_once(
    storage: &StorageHandle,
    subscription: &WatchSubscription,
) -> Result<(), CreateFailure> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
        other => {
            return Err(CreateFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };

    let existing = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
            prefix: Some(watch_subscription_prefix(subscription.owner)),
            start: None,
            limit: NOTIFICATION_WATCH_PER_USER_CAP.saturating_add(1),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(abort_and_classify(storage, txn_id, error).await);
        }
        other => {
            abort_txn(storage, txn_id).await;
            return Err(CreateFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };
    if existing.len() >= NOTIFICATION_WATCH_PER_USER_CAP {
        abort_txn(storage, txn_id).await;
        return Err(CreateFailure::Cap);
    }

    let (key_space, key, value) = match watch_subscription_write_entry(subscription) {
        Ok(entry) => entry,
        Err(error) => {
            abort_txn(storage, txn_id).await;
            return Err(CreateFailure::Fatal(error.to_string()));
        }
    };
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(abort_and_classify(storage, txn_id, error).await);
        }
        other => {
            abort_txn(storage, txn_id).await;
            return Err(CreateFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    }

    write_dirty_marker(storage, subscription.owner.realm_id, txn_id).await?;

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(classify(error)),
        other => Err(CreateFailure::Fatal(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

pub async fn delete_watch_subscription(
    storage: &StorageHandle,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), WatchSubscriptionError> {
    match delete_once(storage, owner, watch_id).await {
        Ok(()) => Ok(()),
        Err(CreateFailure::Conflict) => match delete_once(storage, owner, watch_id).await {
            Ok(()) => Ok(()),
            Err(CreateFailure::Conflict) => Err(WatchSubscriptionError::Storage(
                "watch subscription delete conflicted twice".to_string(),
            )),
            Err(other) => Err(delete_failure(other)),
        },
        Err(other) => Err(delete_failure(other)),
    }
}

fn delete_failure(failure: CreateFailure) -> WatchSubscriptionError {
    match failure {
        CreateFailure::Fatal(error) => WatchSubscriptionError::Storage(error),
        // A delete never checks the per-user cap, so this is unreachable; map it
        // defensively rather than panicking.
        CreateFailure::Cap => {
            WatchSubscriptionError::Storage("unexpected cap failure on delete".to_string())
        }
        CreateFailure::Conflict => WatchSubscriptionError::Storage(
            "watch subscription delete conflicted twice".to_string(),
        ),
    }
}

async fn delete_once(
    storage: &StorageHandle,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), CreateFailure> {
    // Row delete and the interest dirty marker share one transaction so the
    // publisher can never miss the change; both are blind writes, so no read set
    // means the commit does not spuriously conflict with concurrent CRUD.
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
        other => {
            return Err(CreateFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };

    let (key_space, key) = watch_subscription_delete_entry(owner, watch_id);
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space,
            key,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(abort_and_classify(storage, txn_id, error).await);
        }
        other => {
            abort_txn(storage, txn_id).await;
            return Err(CreateFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    }

    write_dirty_marker(storage, owner.realm_id, txn_id).await?;

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(classify(error)),
        other => Err(CreateFailure::Fatal(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

/// Writes the realm's interest dirty marker inside `txn_id` and aborts the
/// transaction on failure. A blind write (no prior read of the marker key) so it
/// never adds a conflict of its own.
async fn write_dirty_marker(
    storage: &StorageHandle,
    realm_id: RealmId,
    txn_id: TxnId,
) -> Result<(), CreateFailure> {
    let (key_space, key, value) = watch_interest_dirty_marker_write(realm_id);
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(abort_and_classify(storage, txn_id, error).await)
        }
        other => {
            abort_txn(storage, txn_id).await;
            Err(CreateFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )))
        }
    }
}

pub async fn list_watch_subscriptions(
    storage: &StorageHandle,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, WatchSubscriptionError> {
    let values = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
            prefix: Some(watch_subscription_prefix(owner)),
            start: None,
            limit: WATCH_SUBSCRIPTION_LIST_LIMIT,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(WatchSubscriptionError::Storage(error.to_string()));
        }
        other => {
            return Err(WatchSubscriptionError::Storage(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };

    let mut subscriptions = Vec::with_capacity(values.len());
    for (_, value) in values {
        subscriptions.push(
            WatchSubscription::from_bytes(&value)
                .map_err(|error| WatchSubscriptionError::Storage(error.to_string()))?,
        );
    }
    Ok(subscriptions)
}

/// Pages every watch subscription this node holds for `realm_id` (all owners),
/// used by holder-side watch-event expansion. Subscription keys are realm-first,
/// so one realm's rows form a single contiguous scan range.
pub async fn list_realm_watch_subscriptions(
    storage: &StorageHandle,
    realm_id: RealmId,
) -> Result<Vec<WatchSubscription>, WatchSubscriptionError> {
    let prefix = UserId::storage_prefix(realm_id);
    let mut subscriptions = Vec::new();
    let mut start = None;
    loop {
        let (values, next) = match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start.map(IterStart::After),
                limit: 1_000,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(WatchSubscriptionError::Storage(error.to_string()));
            }
            other => {
                return Err(WatchSubscriptionError::Storage(format!(
                    "unexpected storage event: {other:?}"
                )));
            }
        };
        for (_, value) in values {
            subscriptions.push(
                WatchSubscription::from_bytes(&value)
                    .map_err(|error| WatchSubscriptionError::Storage(error.to_string()))?,
            );
        }
        match next {
            Some(next) => start = Some(next),
            None => break,
        }
    }
    Ok(subscriptions)
}

fn classify(error: StorageError) -> CreateFailure {
    if matches!(error, StorageError::TransactionConflict) {
        CreateFailure::Conflict
    } else {
        CreateFailure::Fatal(error.to_string())
    }
}

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn abort_and_classify(
    storage: &StorageHandle,
    txn_id: TxnId,
    error: StorageError,
) -> CreateFailure {
    abort_txn(storage, txn_id).await;
    classify(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmId, WatchEventKind};
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (dir, storage)
    }

    fn user(realm: u8, seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId([realm; 32]))
    }

    fn mask() -> WatchEventMask {
        WatchEventMask::from_kinds([
            WatchEventKind::MetadataCreated,
            WatchEventKind::DataUploaded,
        ])
    }

    #[tokio::test]
    async fn create_then_list_roundtrips() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 1);
        let created =
            create_watch_subscription(&storage, owner, "bucket".to_string(), mask(), 1_000)
                .await
                .expect("create succeeds");

        let listed = list_watch_subscriptions(&storage, owner)
            .await
            .expect("list succeeds");
        assert_eq!(listed, vec![created]);
    }

    #[tokio::test]
    async fn create_rejects_invalid_input() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 1);

        assert_eq!(
            create_watch_subscription(&storage, owner, String::new(), mask(), 1).await,
            Err(WatchSubscriptionError::EmptyPrefix)
        );
        assert_eq!(
            create_watch_subscription(&storage, owner, "/bucket".to_string(), mask(), 1).await,
            Err(WatchSubscriptionError::LeadingSlash)
        );
        assert_eq!(
            create_watch_subscription(
                &storage,
                owner,
                "x".repeat(NOTIFICATION_WATCH_MAX_PREFIX_LEN + 1),
                mask(),
                1
            )
            .await,
            Err(WatchSubscriptionError::PrefixTooLong)
        );
        assert_eq!(
            create_watch_subscription(
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
    async fn create_enforces_per_user_cap() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 1);
        for index in 0..NOTIFICATION_WATCH_PER_USER_CAP {
            create_watch_subscription(&storage, owner, format!("p/{index}"), mask(), index as u64)
                .await
                .expect("create under cap succeeds");
        }
        assert_eq!(
            create_watch_subscription(&storage, owner, "overflow".to_string(), mask(), 9_999).await,
            Err(WatchSubscriptionError::CapExceeded)
        );
        assert_eq!(
            list_watch_subscriptions(&storage, owner)
                .await
                .expect("list succeeds")
                .len(),
            NOTIFICATION_WATCH_PER_USER_CAP
        );
    }

    #[tokio::test]
    async fn cap_is_scoped_per_owner() {
        let (_dir, storage) = temp_storage();
        let alice = user(1, 1);
        let bob = user(1, 2);
        for index in 0..NOTIFICATION_WATCH_PER_USER_CAP {
            create_watch_subscription(&storage, alice, format!("a/{index}"), mask(), index as u64)
                .await
                .expect("alice fills her cap");
        }
        create_watch_subscription(&storage, bob, "b".to_string(), mask(), 1)
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
    async fn delete_is_idempotent_and_owner_scoped() {
        let (_dir, storage) = temp_storage();
        let owner = user(1, 1);
        let created = create_watch_subscription(&storage, owner, "x".to_string(), mask(), 1)
            .await
            .expect("create succeeds");

        delete_watch_subscription(&storage, owner, created.watch_id)
            .await
            .expect("delete succeeds");
        delete_watch_subscription(&storage, owner, created.watch_id)
            .await
            .expect("deleting a missing row is ok");
        delete_watch_subscription(&storage, owner, Ulid::new())
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
    async fn list_realm_scan_covers_every_owner_in_realm() {
        let (_dir, storage) = temp_storage();
        let realm = RealmId([1u8; 32]);
        let other_realm = RealmId([2u8; 32]);
        let alice = user(1, 1);
        let bob = user(1, 2);
        let outsider = UserId::new(Ulid::from_bytes([3u8; 16]), other_realm);

        create_watch_subscription(&storage, alice, "a".to_string(), mask(), 1)
            .await
            .expect("alice create");
        create_watch_subscription(&storage, bob, "b".to_string(), mask(), 2)
            .await
            .expect("bob create");
        create_watch_subscription(&storage, outsider, "c".to_string(), mask(), 3)
            .await
            .expect("outsider create");

        let realm_subs = list_realm_watch_subscriptions(&storage, realm)
            .await
            .expect("realm scan succeeds");
        assert_eq!(realm_subs.len(), 2);
        assert!(realm_subs.iter().all(|sub| sub.owner.realm_id == realm));

        let other_subs = list_realm_watch_subscriptions(&storage, other_realm)
            .await
            .expect("other realm scan succeeds");
        assert_eq!(other_subs.len(), 1);
        assert_eq!(other_subs[0].owner, outsider);
    }

    #[test]
    fn cap_reason_matches_sentinel() {
        assert_eq!(
            WatchSubscriptionError::CapExceeded.to_string(),
            WATCH_SUBSCRIPTION_CAP_REACHED
        );
    }
}
