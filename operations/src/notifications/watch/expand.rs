use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, NOTIFICATION_WATCH_INTEREST_KEYSPACE, NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE,
};
use aruna_core::metrics::WatchAuthorizationMetricReason;
use aruna_core::structs::{
    NotificationRecord, RealmId, WatchEvent, WatchEventDetail, WatchEventRetry, WatchSubscription,
    watch_retry_key, watch_retry_prefix, watch_subscription_key,
};
use aruna_core::types::{Key, KeySpace, TxnId, UserId};
use tracing::warn;

use crate::driver::DriverContext;
use crate::notifications::inbox::{
    InboxWriteOutcome, UpsertFailure, upsert_inbox_records_in_transaction,
};
use crate::notifications::placement::filter_locally_held_watch_subscriptions;
use crate::notifications::protocol::{
    NOTIFICATION_WATCH_EVENT_BATCH_SIZE, NOTIFICATION_WATCH_EXPANSION_CANDIDATE_CAP,
    NOTIFICATION_WATCH_EXPANSION_RECORD_CAP, NOTIFICATION_WATCH_EXPANSION_WORK_CAP,
    NOTIFICATION_WATCH_RETRY_BATCH_CAP, NOTIFICATION_WATCH_RETRY_BYTES_CAP,
};
use crate::notifications::routing::route_watch_event;
use crate::notifications::watch::authorization::{
    WatchAuthorization, evaluate_watch_delivery, evaluate_watch_event_authorization,
};
use crate::notifications::watch::interest::{
    mark_watch_interest_dirty, schedule_watch_interest_publish, watch_interest_dirty_marker_write,
};
use crate::notifications::watch::subscriptions::list_watch_page;

/// Expands one bounded subscription page into idempotent inbox records.
/// Later pages are durably retried; placement and authorization are rechecked.
/// Returns the write outcome and whether stale or unauthorized rows were skipped.
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
    if events.len() > NOTIFICATION_WATCH_EVENT_BATCH_SIZE {
        return Err(format!(
            "watch event batch count {} exceeds cap {}",
            events.len(),
            NOTIFICATION_WATCH_EVENT_BATCH_SIZE
        ));
    }
    for attempt in 0..2 {
        match expand_watch_events_once(
            context,
            realm_id,
            realm_config,
            local_node_id,
            events,
            None,
            None,
        )
        .await
        {
            Ok(outcome) => return Ok(outcome),
            Err(UpsertFailure::Conflict) if attempt == 0 => {}
            Err(UpsertFailure::Conflict) => {
                return Err("watch event expansion conflicted twice".to_string());
            }
            Err(UpsertFailure::Fatal(error)) => return Err(error),
        }
    }
    unreachable!()
}

type WatchCandidate<'a> = (
    &'a WatchSubscription,
    &'a WatchEvent,
    Vec<NotificationRecord>,
);

const WATCH_PAGE_LIMIT: usize = 256;

async fn expand_watch_events_once(
    context: &DriverContext,
    realm_id: RealmId,
    realm_config: &aruna_core::structs::RealmConfigDocument,
    local_node_id: aruna_core::NodeId,
    events: &[WatchEvent],
    start: Option<Vec<u8>>,
    retry_key: Option<Vec<u8>>,
) -> Result<(InboxWriteOutcome, bool), UpsertFailure> {
    let page_size = page_limit(events.len()).map_err(UpsertFailure::Fatal)?;
    let (subscriptions, next) =
        list_watch_page(&context.storage_handle, realm_id, start, page_size)
            .await
            .map_err(|error| UpsertFailure::Fatal(error.to_string()))?;
    let (subscriptions, found_stale) =
        filter_locally_held_watch_subscriptions(subscriptions, realm_config, local_node_id)
            .map_err(|error| UpsertFailure::Fatal(error.to_string()))?;
    let work = expansion_budget(events.len(), subscriptions.len()).map_err(UpsertFailure::Fatal)?;
    let mut candidates = Vec::with_capacity(work.min(NOTIFICATION_WATCH_EXPANSION_CANDIDATE_CAP));
    let mut record_count = 0;
    for event in events {
        for subscription in &subscriptions {
            let routed = route_watch_event(event, std::slice::from_ref(subscription));
            if !routed.is_empty() {
                add_limit(
                    candidates.len(),
                    1,
                    NOTIFICATION_WATCH_EXPANSION_CANDIDATE_CAP,
                    "candidate",
                )
                .map_err(UpsertFailure::Fatal)?;
                record_count = add_limit(
                    record_count,
                    routed.len(),
                    NOTIFICATION_WATCH_EXPANSION_RECORD_CAP,
                    "record",
                )
                .map_err(UpsertFailure::Fatal)?;
                candidates.push((subscription, event, routed));
            }
        }
    }
    let retry_key = retry_key.or_else(|| {
        next.as_ref()
            .map(|_| watch_retry_key(realm_id, events[0].event_id))
    });
    let retry_value = next
        .as_ref()
        .map(|cursor| {
            postcard::to_allocvec(&WatchEventRetry {
                events: events.to_vec(),
                cursor: Some(cursor.clone()),
            })
            .map_err(|error| UpsertFailure::Fatal(error.to_string()))
        })
        .transpose()?;
    if retry_value
        .as_ref()
        .is_some_and(|value| value.len() > NOTIFICATION_WATCH_RETRY_BYTES_CAP)
    {
        return Err(UpsertFailure::Fatal(
            "watch retry event exceeds byte cap".to_string(),
        ));
    }
    if candidates.is_empty() && retry_key.is_none() {
        return Ok((InboxWriteOutcome::default(), found_stale));
    }

    let txn_id = start_write_transaction(context).await?;
    let (outcome, denied) = match if candidates.is_empty() {
        Ok((InboxWriteOutcome::default(), false))
    } else {
        stage_watch_expansion(context, realm_id, txn_id, candidates).await
    } {
        Ok(outcome) => outcome,
        Err(error) => {
            abort_transaction(context, txn_id).await;
            return Err(error);
        }
    };
    let queue_changed = match (retry_key, retry_value) {
        (Some(key), Some(value)) => {
            if let Err(error) = stage_retry_write(context, realm_id, txn_id, key, value).await {
                abort_transaction(context, txn_id).await;
                return Err(error);
            }
            true
        }
        (Some(key), None) => {
            match context
                .storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                    key: key.clone().into(),
                    txn_id: Some(txn_id),
                })
                .await
            {
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {}
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    abort_transaction(context, txn_id).await;
                    return Err(UpsertFailure::Conflict);
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    abort_transaction(context, txn_id).await;
                    return Err(classify_storage_error(error));
                }
                other => {
                    abort_transaction(context, txn_id).await;
                    return Err(UpsertFailure::Fatal(format!(
                        "unexpected watch retry guard event: {other:?}"
                    )));
                }
            }
            match context
                .storage_handle
                .send_storage_effect(StorageEffect::Delete {
                    key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: Some(txn_id),
                })
                .await
            {
                Event::Storage(StorageEvent::DeleteResult { .. }) => true,
                Event::Storage(StorageEvent::Error { error }) => {
                    abort_transaction(context, txn_id).await;
                    return Err(classify_storage_error(error));
                }
                other => {
                    abort_transaction(context, txn_id).await;
                    return Err(UpsertFailure::Fatal(format!(
                        "unexpected watch retry delete event: {other:?}"
                    )));
                }
            }
        }
        (None, None) => false,
        (None, Some(_)) => unreachable!(),
    };
    let dropped = found_stale || denied;
    if (next.is_some() || dropped)
        && let Err(error) = stage_dirty_marker(context, realm_id, txn_id).await
    {
        abort_transaction(context, txn_id).await;
        return Err(error);
    }
    if outcome.written == 0 && !queue_changed && !dropped {
        abort_transaction(context, txn_id).await;
        return Ok((outcome, dropped));
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
            if queue_changed && next.is_some() {
                schedule_watch_interest_publish(context).await;
            }
            Ok((outcome, dropped))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(classify_storage_error(error)),
        other => Err(UpsertFailure::Fatal(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

pub async fn drain_watch_events(
    context: &DriverContext,
    realm_id: RealmId,
    realm_config: &aruna_core::structs::RealmConfigDocument,
    local_node_id: aruna_core::NodeId,
) -> Result<(), String> {
    let values = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Some(watch_retry_prefix(realm_id).into()),
            start: None,
            limit: NOTIFICATION_WATCH_RETRY_BATCH_CAP.saturating_add(1),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(defer_retry(context, realm_id, error.to_string()).await);
        }
        other => {
            return Err(defer_retry(
                context,
                realm_id,
                format!("unexpected watch retry scan event: {other:?}"),
            )
            .await);
        }
    };
    let over_batch = values.len() > NOTIFICATION_WATCH_RETRY_BATCH_CAP;
    if over_batch {
        warn!(%realm_id, "Watch retry queue row cap reached");
    }
    let mut failed = over_batch;
    for (key, value) in values.into_iter().take(NOTIFICATION_WATCH_RETRY_BATCH_CAP) {
        let key = key.to_vec();
        if value.len() > NOTIFICATION_WATCH_RETRY_BYTES_CAP {
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
        let mut applied = false;
        for attempt in 0..2 {
            match expand_watch_events_once(
                context,
                realm_id,
                realm_config,
                local_node_id,
                &retry.events,
                retry.cursor.clone(),
                Some(key.clone()),
            )
            .await
            {
                Ok(_) => {
                    applied = true;
                    break;
                }
                Err(UpsertFailure::Conflict) if attempt == 0 => {}
                Err(error) => {
                    warn!(%realm_id, error = %retry_error(&error), "Watch retry row deferred");
                    break;
                }
            }
        }
        if !applied {
            failed = true;
        }
    }
    if failed {
        return Err(defer_retry(
            context,
            realm_id,
            "watch retry drain deferred work".to_string(),
        )
        .await);
    }
    Ok(())
}

async fn defer_retry(context: &DriverContext, realm_id: RealmId, error: String) -> String {
    if let Err(marker) = mark_watch_interest_dirty(context, realm_id).await {
        warn!(%realm_id, %marker, "Failed to retain watch retry marker");
        return format!("{error}; failed to retain retry marker: {marker}");
    }
    error
}

fn valid_retry(retry: &WatchEventRetry, realm_id: RealmId, key: &[u8]) -> bool {
    let Some(first) = retry.events.first() else {
        return false;
    };
    let Some(cursor) = retry.cursor.as_ref() else {
        return false;
    };
    retry.events.len() <= NOTIFICATION_WATCH_EVENT_BATCH_SIZE
        && retry
            .events
            .iter()
            .all(|event| event.realm_id == realm_id && !event.event_id.is_nil())
        && cursor.len() == 64
        && cursor.starts_with(UserId::storage_prefix(realm_id).as_ref())
        && key == watch_retry_key(realm_id, first.event_id).as_slice()
}

fn retry_error(error: &UpsertFailure) -> String {
    match error {
        UpsertFailure::Conflict => "storage conflict".to_string(),
        UpsertFailure::Fatal(error) => error.clone(),
    }
}

fn page_limit(events: usize) -> Result<usize, String> {
    if events == 0 {
        return Err("watch event batch is empty".to_string());
    }
    let mut limit = WATCH_PAGE_LIMIT;
    for cap in [
        NOTIFICATION_WATCH_EXPANSION_WORK_CAP,
        NOTIFICATION_WATCH_EXPANSION_CANDIDATE_CAP,
        NOTIFICATION_WATCH_EXPANSION_RECORD_CAP,
    ] {
        limit = limit.min(cap / events);
    }
    (limit > 0)
        .then_some(limit)
        .ok_or_else(|| "watch event batch exceeds expansion caps".to_string())
}

async fn stage_retry_write(
    context: &DriverContext,
    realm_id: RealmId,
    txn_id: TxnId,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), UpsertFailure> {
    let values = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Some(watch_retry_prefix(realm_id).into()),
            start: None,
            limit: NOTIFICATION_WATCH_RETRY_BATCH_CAP.saturating_add(1),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(classify_storage_error(error));
        }
        other => {
            return Err(UpsertFailure::Fatal(format!(
                "unexpected watch retry budget event: {other:?}"
            )));
        }
    };
    if values.len() > NOTIFICATION_WATCH_RETRY_BATCH_CAP {
        warn!(%realm_id, "Watch retry queue row cap reached");
        return Err(UpsertFailure::Fatal(
            "watch retry row cap reached".to_string(),
        ));
    }
    let current_bytes = values.iter().try_fold(0usize, |total, (_, value)| {
        total
            .checked_add(value.len())
            .ok_or_else(|| "watch retry byte budget overflow".to_string())
    });
    let current_bytes = current_bytes.map_err(UpsertFailure::Fatal)?;
    let existing = values
        .iter()
        .find(|(current, _)| current.as_ref() == key.as_slice());
    let replaced = existing.map_or(0, |(_, value)| value.len());
    if replaced == 0 && values.len() >= NOTIFICATION_WATCH_RETRY_BATCH_CAP {
        warn!(%realm_id, "Watch retry queue row cap reached");
        return Err(UpsertFailure::Fatal(
            "watch retry row cap reached".to_string(),
        ));
    }
    let mut write_value = value;
    if let Some((_, current)) = existing
        && let Ok(current_retry) = postcard::from_bytes::<WatchEventRetry>(current.as_ref())
        && let Ok(next_retry) = postcard::from_bytes::<WatchEventRetry>(&write_value)
        && let (Some(current_cursor), Some(next_cursor)) =
            (current_retry.cursor.as_ref(), next_retry.cursor.as_ref())
        && current_cursor >= next_cursor
    {
        write_value = current.to_vec();
    }
    let next_bytes = current_bytes
        .checked_sub(replaced)
        .and_then(|bytes| bytes.checked_add(write_value.len()))
        .ok_or_else(|| UpsertFailure::Fatal("watch retry byte budget overflow".to_string()))?;
    if next_bytes > NOTIFICATION_WATCH_RETRY_BYTES_CAP {
        warn!(%realm_id, "Watch retry queue byte cap reached");
        return Err(UpsertFailure::Fatal(
            "watch retry byte cap reached".to_string(),
        ));
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            key: key.into(),
            value: write_value.into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(classify_storage_error(error));
        }
        other => {
            return Err(UpsertFailure::Fatal(format!(
                "unexpected watch retry write event: {other:?}"
            )));
        }
    }
    Ok(())
}

async fn stage_dirty_marker(
    context: &DriverContext,
    realm_id: RealmId,
    txn_id: TxnId,
) -> Result<(), UpsertFailure> {
    let (key_space, key, value) = watch_interest_dirty_marker_write(realm_id);
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(classify_storage_error(error)),
        other => Err(UpsertFailure::Fatal(format!(
            "unexpected watch interest marker event: {other:?}"
        ))),
    }
}

async fn stage_watch_expansion(
    context: &DriverContext,
    realm_id: RealmId,
    txn_id: TxnId,
    candidates: Vec<WatchCandidate<'_>>,
) -> Result<(InboxWriteOutcome, bool), UpsertFailure> {
    let mut record_budget = 0;
    for (_, _, routed) in &candidates {
        record_budget = add_limit(
            record_budget,
            routed.len(),
            NOTIFICATION_WATCH_EXPANSION_RECORD_CAP,
            "record",
        )
        .map_err(UpsertFailure::Fatal)?;
    }
    let mut subscriptions = Vec::with_capacity(candidates.len());
    for (subscription, _, _) in &candidates {
        if !subscriptions
            .iter()
            .any(|current: &&WatchSubscription| current.watch_id == subscription.watch_id)
        {
            subscriptions.push(*subscription);
        }
    }
    let mut reads: Vec<(KeySpace, Key)> = subscriptions
        .iter()
        .map(|subscription| {
            (
                NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
                watch_subscription_key(subscription.owner, subscription.watch_id),
            )
        })
        .collect();
    let subscription_count = reads.len();
    reads.push((
        AUTH_KEYSPACE.to_string(),
        realm_id.as_bytes().to_vec().into(),
    ));
    for (_, event, _) in &candidates {
        let group_id = match &event.detail {
            WatchEventDetail::MetadataCreated { group_id, .. }
            | WatchEventDetail::DataUploaded { group_id, .. }
            | WatchEventDetail::SyncCompleted { group_id, .. }
            | WatchEventDetail::SyncFailed { group_id, .. } => *group_id,
        };
        let key: Key = group_id.to_bytes().to_vec().into();
        if !reads
            .iter()
            .any(|(key_space, current)| key_space == AUTH_KEYSPACE && current == &key)
        {
            reads.push((AUTH_KEYSPACE.to_string(), key));
        }
    }
    let expected_count = reads.len();
    let guarded = match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(classify_storage_error(error));
        }
        other => {
            return Err(UpsertFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };
    if guarded.len() != expected_count {
        return Err(UpsertFailure::Fatal(
            "watch authorization guard returned the wrong result count".to_string(),
        ));
    }
    for (subscription, (_, stored)) in subscriptions
        .iter()
        .zip(guarded.into_iter().take(subscription_count))
    {
        let expected = subscription
            .to_bytes()
            .map_err(|error| UpsertFailure::Fatal(error.to_string()))?;
        if stored.as_deref() != Some(expected.as_slice()) {
            return Err(UpsertFailure::Conflict);
        }
    }

    let mut records = Vec::with_capacity(record_budget);
    let mut dropped = false;
    for (subscription, event, routed) in candidates {
        match evaluate_watch_delivery(
            context,
            realm_id,
            subscription.owner,
            &subscription.path_prefix,
            subscription.event_mask,
            &subscription.authorization,
        )
        .await
        {
            Ok(WatchAuthorization::Authorized) => {}
            Ok(WatchAuthorization::Denied(reason)) => {
                record_delivery_suppression(context, reason.metric_reason());
                dropped = true;
                continue;
            }
            Ok(WatchAuthorization::Unavailable(error)) | Err(error) => {
                record_delivery_suppression(
                    context,
                    WatchAuthorizationMetricReason::AuthorizationUnavailable,
                );
                return Err(UpsertFailure::Fatal(error));
            }
        }
        match evaluate_watch_event_authorization(
            context,
            subscription.owner,
            &subscription.authorization,
            event,
        )
        .await
        {
            Ok(WatchAuthorization::Authorized) => records.extend(routed),
            Ok(WatchAuthorization::Denied(reason)) => {
                record_delivery_suppression(context, reason.metric_reason());
            }
            Ok(WatchAuthorization::Unavailable(error)) | Err(error) => {
                record_delivery_suppression(
                    context,
                    WatchAuthorizationMetricReason::AuthorizationUnavailable,
                );
                return Err(UpsertFailure::Fatal(error));
            }
        }
    }
    if records.is_empty() {
        return Ok((InboxWriteOutcome::default(), dropped));
    }
    upsert_inbox_records_in_transaction(&context.storage_handle, &records, txn_id)
        .await
        .map(|outcome| (outcome, dropped))
}

async fn start_write_transaction(context: &DriverContext) -> Result<TxnId, UpsertFailure> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(classify_storage_error(error)),
        other => Err(UpsertFailure::Fatal(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

fn classify_storage_error(error: StorageError) -> UpsertFailure {
    if matches!(error, StorageError::TransactionConflict) {
        UpsertFailure::Conflict
    } else {
        UpsertFailure::Fatal(error.to_string())
    }
}

fn expansion_budget(events: usize, subscriptions: usize) -> Result<usize, String> {
    let work = events
        .checked_mul(subscriptions)
        .ok_or_else(|| "watch expansion work budget overflow".to_string())?;
    if work > NOTIFICATION_WATCH_EXPANSION_WORK_CAP {
        return Err(format!(
            "watch expansion work {} exceeds cap {}",
            work, NOTIFICATION_WATCH_EXPANSION_WORK_CAP
        ));
    }
    Ok(work)
}

fn add_limit(total: usize, added: usize, limit: usize, label: &str) -> Result<usize, String> {
    let next = total
        .checked_add(added)
        .ok_or_else(|| format!("watch expansion {label} budget overflow"))?;
    if next > limit {
        return Err(format!(
            "watch expansion {label} count {next} exceeds cap {limit}"
        ));
    }
    Ok(next)
}

async fn abort_transaction(context: &DriverContext, txn_id: TxnId) {
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

fn record_delivery_suppression(context: &DriverContext, reason: WatchAuthorizationMetricReason) {
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
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, Permission, RealmAuthorizationDocument,
        RealmConfigDocument, RealmNodeKind, WatchAuthorizationBinding, WatchEventDetail,
        WatchEventKind, WatchEventMask, blob_object_permission_path, data_watch_resource_path,
    };
    use aruna_core::types::UserId;
    use aruna_storage::{FjallStorage, StorageHandle};
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::notifications::watch::subscriptions::{
        create_replicated_watch_subscription, create_watch_subscription,
    };

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
                compute_handle: None,
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
                NOTIFICATION_WATCH_EXPANSION_CANDIDATE_CAP,
                1,
                NOTIFICATION_WATCH_EXPANSION_CANDIDATE_CAP,
                "candidate"
            )
            .is_err()
        );
    }

    #[test]
    fn expansion_caps_records() {
        assert!(
            add_limit(
                NOTIFICATION_WATCH_EXPANSION_RECORD_CAP,
                1,
                NOTIFICATION_WATCH_EXPANSION_RECORD_CAP,
                "record"
            )
            .is_err()
        );
    }

    #[test]
    fn page_caps_work() {
        // A full batch pages at the tightest cap so the in-loop candidate and
        // record budgets can never abort a dense expansion mid-stream.
        assert_eq!(page_limit(1).expect("one event fits"), WATCH_PAGE_LIMIT);
        assert_eq!(
            page_limit(NOTIFICATION_WATCH_EVENT_BATCH_SIZE).unwrap(),
            NOTIFICATION_WATCH_EXPANSION_CANDIDATE_CAP / NOTIFICATION_WATCH_EVENT_BATCH_SIZE
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
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm);
        let group_auth = GroupAuthorizationDocument::new_default_group_doc(owner, realm, group_id);
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
                key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
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
                    key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                    key: key.to_vec().into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn commit_retry(
        context: &DriverContext,
        realm: RealmId,
        key: &[u8],
        retry: &WatchEventRetry,
    ) -> Result<(), UpsertFailure> {
        let value = postcard::to_allocvec(retry).expect("encode retry");
        let txn_id = start_write_transaction(context).await?;
        if let Err(error) = stage_retry_write(context, realm, txn_id, key.to_vec(), value).await {
            abort_transaction(context, txn_id).await;
            return Err(error);
        }
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
            other => panic!("unexpected commit event: {other:?}"),
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
        let expired_binding = WatchAuthorizationBinding {
            expires_at_secs: 1,
            ..Default::default()
        };
        create_replicated_watch_subscription(
            &context,
            local_node_id,
            owner,
            data_watch_resource_path(group_id, local_node_id, "bucket", ""),
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
    async fn expansion_without_subscriptions_writes_nothing() {
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
    async fn exact_event_deny_suppresses_nested_object() {
        let (_dir, context) = temp_context();
        let realm = RealmId([4u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let owner = user(realm, 1);
        let actor = user(realm, 2);
        let group_id = Ulid::from_bytes([6u8; 16]);
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

        let mut group = GroupAuthorizationDocument::new_default_group_doc(owner, realm, group_id);
        group
            .roles
            .values_mut()
            .find(|role| role.name == "admin")
            .expect("admin role")
            .permissions
            .insert(
                blob_object_permission_path(realm, group_id, local_node_id, "bucket", "object"),
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
    async fn concurrent_revocation_prevents_watch_delivery_commit() {
        let (_dir, context) = temp_context();
        let realm = RealmId([2u8; 32]);
        let (local_node_id, config) = local_config(realm);
        let owner = user(realm, 1);
        let actor = user(realm, 2);
        let replacement_owner = user(realm, 3);
        let group_id = Ulid::from_bytes([4u8; 16]);
        install_authorization(&context, realm, local_node_id, group_id, owner).await;
        let subscription = create_watch_subscription(
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

        let mut second_event = first_event;
        second_event.event_id = Ulid::from_bytes([8u8; 16]);
        let txn_id = start_write_transaction(&context)
            .await
            .expect("start transaction");
        let routed = route_watch_event(&second_event, std::slice::from_ref(&subscription));
        let staged = stage_watch_expansion(
            &context,
            realm,
            txn_id,
            vec![(&subscription, &second_event, routed)],
        )
        .await
        .expect("stage authorized event");
        assert_eq!(staged.0.written, 1);

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
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict
            })
        ));
        assert_eq!(count_inbox(&context.storage_handle).await, 1);

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
        while events.len() < NOTIFICATION_WATCH_EVENT_BATCH_SIZE {
            let mut quiet = upload_event(realm, actor, group_id, local_node_id);
            let mut bytes = [9u8; 16];
            bytes[..8].copy_from_slice(&(events.len() as u64).to_be_bytes());
            quiet.event_id = Ulid::from_bytes(bytes);
            quiet.path = data_watch_resource_path(group_id, local_node_id, "quiet", "object");
            events.push(quiet);
        }
        let page = page_limit(events.len()).expect("page limit");
        for seed in 1..=page {
            create_watch_subscription(
                &context.storage_handle,
                user(realm, seed as u8),
                data_watch_resource_path(group_id, local_node_id, "idle", ""),
                WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
                1,
            )
            .await
            .expect("create");
        }
        create_watch_subscription(
            &context.storage_handle,
            late_owner,
            data_watch_resource_path(group_id, local_node_id, "bucket", ""),
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
        create_watch_subscription(
            &context.storage_handle,
            owner,
            data_watch_resource_path(group_id, local_node_id, "bucket", ""),
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
    async fn retry_cursor_monotonic() {
        // The last write rewinds the cursor; only an advancing second write can
        // leave the queued row at `ahead`.
        let (_dir, context) = temp_context();
        let realm = RealmId([9u8; 32]);
        let node_id = local_config(realm).0;
        let event = upload_event(realm, user(realm, 2), Ulid::from_bytes([3u8; 16]), node_id);
        let key = watch_retry_key(realm, event.event_id);
        let behind = watch_subscription_key(user(realm, 2), Ulid::nil()).to_vec();
        let ahead = watch_subscription_key(user(realm, 9), Ulid::nil()).to_vec();
        for cursor in [behind.clone(), ahead.clone(), behind] {
            commit_retry(
                &context,
                realm,
                &key,
                &WatchEventRetry {
                    events: vec![event.clone()],
                    cursor: Some(cursor),
                },
            )
            .await
            .expect("stage retry");
        }

        let rows = retry_rows(&context.storage_handle, realm).await;
        assert_eq!(rows.len(), 1, "a retry row is replaced, not duplicated");
        let stored: WatchEventRetry = postcard::from_bytes(&rows[0].1).expect("decode retry");
        assert_eq!(stored.cursor, Some(ahead));
    }

    #[tokio::test]
    async fn retry_row_cap() {
        // The row cap must not strand a queued page: an existing row still updates.
        let (_dir, context) = temp_context();
        let realm = RealmId([10u8; 32]);
        let node_id = local_config(realm).0;
        let base = upload_event(realm, user(realm, 2), Ulid::from_bytes([3u8; 16]), node_id);
        let cursor = watch_subscription_key(user(realm, 2), Ulid::nil()).to_vec();
        let mut queued = Vec::new();
        for seed in 0..NOTIFICATION_WATCH_RETRY_BATCH_CAP {
            let mut event = base.clone();
            event.event_id = Ulid::from_bytes([seed as u8 + 20; 16]);
            let key = watch_retry_key(realm, event.event_id);
            commit_retry(
                &context,
                realm,
                &key,
                &WatchEventRetry {
                    events: vec![event.clone()],
                    cursor: Some(cursor.clone()),
                },
            )
            .await
            .expect("seed row");
            queued.push((key, event));
        }

        let mut overflow = base;
        overflow.event_id = Ulid::from_bytes([99u8; 16]);
        let error = commit_retry(
            &context,
            realm,
            &watch_retry_key(realm, overflow.event_id),
            &WatchEventRetry {
                events: vec![overflow],
                cursor: Some(cursor),
            },
        )
        .await
        .expect_err("row cap refuses a new row");
        assert!(matches!(error, UpsertFailure::Fatal(message) if message.contains("row cap")));

        let (key, event) = queued.remove(0);
        commit_retry(
            &context,
            realm,
            &key,
            &WatchEventRetry {
                events: vec![event],
                cursor: Some(watch_subscription_key(user(realm, 9), Ulid::nil()).to_vec()),
            },
        )
        .await
        .expect("queued row still advances");
        assert_eq!(
            retry_rows(&context.storage_handle, realm).await.len(),
            NOTIFICATION_WATCH_RETRY_BATCH_CAP
        );
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
        let events = vec![event; NOTIFICATION_WATCH_EVENT_BATCH_SIZE + 1];

        let error = expand_watch_events(&context, realm, &config, local_node_id, &events)
            .await
            .expect_err("over-cap batch");
        assert!(error.contains("exceeds cap"), "{error}");
        assert_eq!(count_inbox(&context.storage_handle).await, 0);
    }
}
