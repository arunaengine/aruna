use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncOutboxRecord,
    DocumentSyncRevision, DocumentSyncTarget,
};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE;
use aruna_core::storage_entries::{
    document_sync_revision_write_entry, watch_subscription_delete_entry,
    watch_subscription_write_entry,
};
use aruna_core::structs::{
    NOTIFICATION_WATCH_MAX_PREFIX_LEN, NOTIFICATION_WATCH_PER_USER_CAP, PlacementRef, RealmId,
    WatchAuthorizationBinding, WatchEventMask, WatchSubscription, parse_watch_subscription_key,
    watch_subscription_prefix,
};
use aruna_core::types::{TxnId, UserId};
use aruna_storage::StorageHandle;
use thiserror::Error;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};
use crate::driver::DriverContext;
use crate::notifications::watch::authorization::is_watch_authorized;
use crate::notifications::watch::interest::watch_interest_dirty_marker_write;

/// Single owner-prefix scan bound. Watches are hard-capped per user, so one page
/// always covers a subscription set with a wide safety margin.
const WATCH_SUBSCRIPTION_LIST_LIMIT: usize = 256;

/// Stable reject reason for a cap-exceeded create; matched verbatim by the
/// holder proxy to surface a 409 to the API layer.
pub const WATCH_SUBSCRIPTION_CAP_REACHED: &str = "notification watch subscription cap reached";

/// Stable reject reason for an unauthorized create; matched verbatim by the
/// holder proxy to surface a 403 to the API layer.
pub const WATCH_SUBSCRIPTION_UNAUTHORIZED: &str =
    "watch subscription owner lacks READ on the watched path";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WatchSubscriptionError {
    #[error("{WATCH_SUBSCRIPTION_UNAUTHORIZED}")]
    Unauthorized,
    #[error("watch path prefix must not be empty")]
    EmptyPrefix,
    #[error("watch path prefix must not start with a slash")]
    LeadingSlash,
    #[error("watch path prefix exceeds maximum length")]
    PrefixTooLong,
    #[error("watch event mask must select at least one event")]
    EmptyMask,
    #[error("watch event mask contains an unknown event")]
    InvalidMask,
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

#[derive(Clone)]
struct WatchReplication {
    revision: DocumentSyncChange,
    outbox: DocumentSyncOutboxRecord,
}

pub async fn create_watch_subscription(
    storage: &StorageHandle,
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
    now_ms: u64,
) -> Result<WatchSubscription, WatchSubscriptionError> {
    let subscription = validated_subscription(
        owner,
        path_prefix,
        event_mask,
        now_ms,
        WatchAuthorizationBinding::default(),
    )?;
    create_subscription(storage, subscription, None).await
}

/// The one holder-side create path, used by both the local API arm and the
/// holder RPC. A proxying peer's assertion is not authority to watch, so the
/// holder that persists and replicates the subscription re-derives the canonical
/// permission path and checks the owner's READ itself before any durable write.
pub async fn create_replicated_watch_subscription(
    context: &DriverContext,
    local_node_id: aruna_core::NodeId,
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
    authorization: WatchAuthorizationBinding,
    now_ms: u64,
) -> Result<WatchSubscription, WatchSubscriptionError> {
    let subscription =
        validated_subscription(owner, path_prefix, event_mask, now_ms, authorization)?;
    if !is_watch_authorized(
        context,
        owner.realm_id,
        owner,
        &subscription.path_prefix,
        subscription.event_mask,
        &subscription.authorization,
    )
    .await
    .map_err(WatchSubscriptionError::Storage)?
    {
        return Err(WatchSubscriptionError::Unauthorized);
    }
    let replication = watch_upsert_replication(local_node_id, &subscription)?;
    let subscription =
        create_subscription(&context.storage_handle, subscription, Some(replication)).await?;
    schedule_replication(context).await;
    Ok(subscription)
}

fn validated_subscription(
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
    now_ms: u64,
    authorization: WatchAuthorizationBinding,
) -> Result<WatchSubscription, WatchSubscriptionError> {
    validate_subscription_fields(&path_prefix, event_mask)?;
    if !authorization.is_valid() {
        return Err(WatchSubscriptionError::Unauthorized);
    }

    Ok(WatchSubscription::new_with_authorization(
        owner,
        path_prefix,
        event_mask,
        now_ms,
        authorization,
    ))
}

fn validate_subscription_fields(
    path_prefix: &str,
    event_mask: WatchEventMask,
) -> Result<(), WatchSubscriptionError> {
    if path_prefix.is_empty() {
        return Err(WatchSubscriptionError::EmptyPrefix);
    }
    // Emitted event paths carry no leading slash (`s3/{group}/{node}/{bucket}/{key}`,
    // `meta/{group_id}/{document_path}`), so a leading-slash prefix could never
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
    if event_mask.bits() & !(WatchEventMask::METADATA_CREATED | WatchEventMask::DATA_UPLOADED) != 0
    {
        return Err(WatchSubscriptionError::InvalidMask);
    }

    Ok(())
}

async fn create_subscription(
    storage: &StorageHandle,
    subscription: WatchSubscription,
    replication: Option<WatchReplication>,
) -> Result<WatchSubscription, WatchSubscriptionError> {
    match create_once(storage, &subscription, replication.as_ref()).await {
        Ok(()) => Ok(subscription),
        Err(CreateFailure::Cap) => Err(WatchSubscriptionError::CapExceeded),
        Err(CreateFailure::Fatal(error)) => Err(WatchSubscriptionError::Storage(error)),
        Err(CreateFailure::Conflict) => {
            match create_once(storage, &subscription, replication.as_ref()).await {
                Ok(()) => Ok(subscription),
                Err(CreateFailure::Cap) => Err(WatchSubscriptionError::CapExceeded),
                Err(CreateFailure::Fatal(error)) => Err(WatchSubscriptionError::Storage(error)),
                Err(CreateFailure::Conflict) => Err(WatchSubscriptionError::Storage(
                    "watch subscription create conflicted twice".to_string(),
                )),
            }
        }
    }
}

async fn create_once(
    storage: &StorageHandle,
    subscription: &WatchSubscription,
    replication: Option<&WatchReplication>,
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

    let subscription_write = match watch_subscription_write_entry(subscription) {
        Ok(entry) => entry,
        Err(error) => {
            abort_txn(storage, txn_id).await;
            return Err(CreateFailure::Fatal(error.to_string()));
        }
    };
    let mut writes = vec![subscription_write];
    writes.push(watch_interest_dirty_marker_write(
        subscription.owner.realm_id,
    ));
    if let Some(replication) = replication {
        let target = watch_subscription_target(subscription.owner, subscription.watch_id);
        let revision_entry =
            match document_sync_revision_write_entry(&target, &replication.revision) {
                Ok(entry) => entry,
                Err(error) => {
                    abort_txn(storage, txn_id).await;
                    return Err(CreateFailure::Fatal(error.to_string()));
                }
            };
        let outbox_entry = match outbox_write_entry(&replication.outbox) {
            Ok(entry) => entry,
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(CreateFailure::Fatal(error.to_string()));
            }
        };
        writes.push(revision_entry);
        writes.push(outbox_entry);
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
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
    delete_subscription(storage, owner, watch_id, None)
        .await
        .map(|_| ())
}

pub async fn delete_replicated_watch_subscription(
    context: &DriverContext,
    local_node_id: aruna_core::NodeId,
    owner: UserId,
    watch_id: Ulid,
    now_ms: u64,
) -> Result<(), WatchSubscriptionError> {
    let replication = watch_delete_replication(local_node_id, owner, watch_id, now_ms);
    let deleted =
        delete_subscription(&context.storage_handle, owner, watch_id, Some(replication)).await?;
    if deleted {
        schedule_replication(context).await;
    }
    Ok(())
}

async fn delete_subscription(
    storage: &StorageHandle,
    owner: UserId,
    watch_id: Ulid,
    replication: Option<WatchReplication>,
) -> Result<bool, WatchSubscriptionError> {
    match delete_once(storage, owner, watch_id, replication.as_ref()).await {
        Ok(deleted) => Ok(deleted),
        Err(CreateFailure::Conflict) => {
            match delete_once(storage, owner, watch_id, replication.as_ref()).await {
                Ok(deleted) => Ok(deleted),
                Err(CreateFailure::Conflict) => Err(WatchSubscriptionError::Storage(
                    "watch subscription delete conflicted twice".to_string(),
                )),
                Err(other) => Err(delete_failure(other)),
            }
        }
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
    replication: Option<&WatchReplication>,
) -> Result<bool, CreateFailure> {
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

    let (_, subscription_key) = watch_subscription_delete_entry(owner, watch_id);
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
            key: subscription_key,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {}
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            abort_txn(storage, txn_id).await;
            return Ok(false);
        }
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

    let mut writes = vec![watch_interest_dirty_marker_write(owner.realm_id)];
    if let Some(replication) = replication {
        let target = watch_subscription_target(owner, watch_id);
        let revision_entry =
            match document_sync_revision_write_entry(&target, &replication.revision) {
                Ok(entry) => entry,
                Err(error) => {
                    abort_txn(storage, txn_id).await;
                    return Err(CreateFailure::Fatal(error.to_string()));
                }
            };
        let outbox_entry = match outbox_write_entry(&replication.outbox) {
            Ok(entry) => entry,
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(CreateFailure::Fatal(error.to_string()));
            }
        };
        writes.push(revision_entry);
        writes.push(outbox_entry);
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
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

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(true),
        Event::Storage(StorageEvent::Error { error }) => Err(classify(error)),
        other => Err(CreateFailure::Fatal(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

fn watch_subscription_target(owner: UserId, watch_id: Ulid) -> DocumentSyncTarget {
    DocumentSyncTarget::WatchSubscription { owner, watch_id }
}

fn watch_upsert_replication(
    local_node_id: aruna_core::NodeId,
    subscription: &WatchSubscription,
) -> Result<WatchReplication, WatchSubscriptionError> {
    let outbox_id = Ulid::r#gen();
    let revision = DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: outbox_id,
            actor: local_node_id,
            updated_at_ms: subscription.created_at_ms,
        },
        kind: DocumentSyncChangeKind::Upsert,
        placement: PlacementRef::NIL,
    };
    let target = watch_subscription_target(subscription.owner, subscription.watch_id);
    let outbox = new_outbox_record_with_id(
        outbox_id,
        local_node_id,
        target,
        Vec::new(),
        DocumentSyncOutboxEvent::Upsert {
            bytes: subscription
                .to_bytes()
                .map_err(|error| WatchSubscriptionError::Storage(error.to_string()))?,
            change: revision,
        },
        PlacementRef::NIL,
        false,
    );
    Ok(WatchReplication { revision, outbox })
}

fn watch_delete_replication(
    local_node_id: aruna_core::NodeId,
    owner: UserId,
    watch_id: Ulid,
    now_ms: u64,
) -> WatchReplication {
    let outbox_id = Ulid::r#gen();
    let revision = DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 2,
            event_id: outbox_id,
            actor: local_node_id,
            updated_at_ms: now_ms,
        },
        kind: DocumentSyncChangeKind::Delete,
        placement: PlacementRef::NIL,
    };
    let outbox = new_outbox_record_with_id(
        outbox_id,
        local_node_id,
        watch_subscription_target(owner, watch_id),
        Vec::new(),
        DocumentSyncOutboxEvent::Delete { change: revision },
        PlacementRef::NIL,
        false,
    );
    WatchReplication { revision, outbox }
}

async fn schedule_replication(context: &DriverContext) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        return;
    };
    let _ = task_handle
        .send_effect(schedule_outbox_drain_effect())
        .await;
}

fn decode_stored_subscription(
    key: &[u8],
    value: &[u8],
) -> Result<WatchSubscription, WatchSubscriptionError> {
    let (key_owner, key_watch_id) = parse_watch_subscription_key(key)
        .map_err(|error| WatchSubscriptionError::Storage(error.to_string()))?;
    let subscription = WatchSubscription::from_bytes(value)
        .map_err(|error| WatchSubscriptionError::Storage(error.to_string()))?;
    if key_owner.is_nil()
        || key_watch_id.is_nil()
        || subscription.owner != key_owner
        || subscription.watch_id != key_watch_id
    {
        return Err(WatchSubscriptionError::Storage(
            "stored watch subscription key does not match payload identity".to_string(),
        ));
    }
    validate_subscription_fields(&subscription.path_prefix, subscription.event_mask).map_err(
        |error| {
            WatchSubscriptionError::Storage(format!(
                "stored watch subscription violates invariants: {error}"
            ))
        },
    )?;
    if !subscription.authorization.is_valid() {
        return Err(WatchSubscriptionError::Storage(
            "stored watch subscription has invalid authorization binding".to_string(),
        ));
    }
    Ok(subscription)
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
    for (key, value) in values {
        let subscription = decode_stored_subscription(&key, &value)?;
        if subscription.owner != owner {
            return Err(WatchSubscriptionError::Storage(
                "stored watch subscription belongs to a different owner".to_string(),
            ));
        }
        subscriptions.push(subscription);
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
        for (key, value) in values {
            let subscription = decode_stored_subscription(&key, &value)?;
            if subscription.owner.realm_id != realm_id {
                return Err(WatchSubscriptionError::Storage(
                    "stored watch subscription belongs to a different realm".to_string(),
                ));
            }
            subscriptions.push(subscription);
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
    use aruna_core::NodeId;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, RealmAuthorizationDocument, RealmId, WatchEventKind,
        data_watch_resource_path,
    };
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

    fn data_mask() -> WatchEventMask {
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded])
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn test_context(storage: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
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
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
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
            match storage
                .send_storage_effect(StorageEffect::Write {
                    key_space: AUTH_KEYSPACE.to_string(),
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

    // Revocation hides a watch but does not delete its replicated row. Those
    // durable rows must keep occupying cap slots until explicit deletion, or a
    // user can grow storage and fan-out without bound by churning permissions.
    #[tokio::test]
    async fn revoked_rows_still_count_toward_cap() {
        let (_dir, storage) = temp_storage();
        let realm_id = RealmId([7u8; 32]);
        let owner = UserId::new(Ulid::from_bytes([1u8; 16]), realm_id);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let node_id = node(3);
        install_auth(&storage, realm_id, owner, group_id, node_id).await;

        let dead_prefix = data_watch_resource_path(Ulid::nil(), node_id, "bucket", "");
        for index in 0..NOTIFICATION_WATCH_PER_USER_CAP {
            create_watch_subscription(
                &storage,
                owner,
                dead_prefix.clone(),
                data_mask(),
                index as u64,
            )
            .await
            .expect("dead row create");
        }

        let context = test_context(storage);
        let authorized_prefix = data_watch_resource_path(group_id, node_id, "bucket", "reports/");
        assert_eq!(
            create_replicated_watch_subscription(
                &context,
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
            list_watch_subscriptions(&context.storage_handle, owner)
                .await
                .expect("durable rows remain listable")
                .len(),
            NOTIFICATION_WATCH_PER_USER_CAP
        );
    }

    #[test]
    fn stored_rows_require_valid_key_payload_identity() {
        let owner = user(1, 1);
        let watch_id = Ulid::from_bytes([9u8; 16]);
        let mut subscription = WatchSubscription::new(owner, "prefix".to_string(), mask(), 1);
        subscription.watch_id = watch_id;
        let bytes = subscription.to_bytes().expect("subscription encodes");
        let key = aruna_core::structs::watch_subscription_key(owner, watch_id);
        assert_eq!(
            decode_stored_subscription(&key, &bytes).expect("valid row"),
            subscription
        );

        let wrong_key = aruna_core::structs::watch_subscription_key(owner, Ulid::r#gen());
        assert!(matches!(
            decode_stored_subscription(&wrong_key, &bytes),
            Err(WatchSubscriptionError::Storage(_))
        ));

        subscription.watch_id = Ulid::nil();
        let nil_key = aruna_core::structs::watch_subscription_key(owner, Ulid::nil());
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
        delete_watch_subscription(&storage, owner, Ulid::r#gen())
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
