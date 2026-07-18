use aruna_core::NodeId;
use aruna_core::metrics::WatchAuthorizationMetricReason;
use aruna_core::structs::{
    NotificationClass, NotificationKind, NotificationRecord, WatchAuthorizationBinding,
    WatchEventMask, WatchInterestEntry, WatchSubscription,
};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use thiserror::Error;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use crate::notifications::client::{
    create_watch_remote, delete_watch_remote, list_remote, list_watches_remote, mark_read_remote,
    unread_count_remote,
};
use crate::notifications::list::{
    LIST_NOTIFICATIONS_MAX_LIMIT, ListNotificationsInput, ListNotificationsOperation,
};
use crate::notifications::mark_read::{MarkReadInput, MarkReadOperation};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::unread::{UNREAD_COUNT_CAP, UNREAD_SCAN_MAX_ROWS};
use crate::notifications::watch::authorization::{
    WatchAuthorization, evaluate_watch_notification_authorization,
    list_authorized_watch_subscriptions,
};
use crate::notifications::watch::interest::schedule_watch_interest_publish;
use crate::notifications::watch::subscriptions::{
    WATCH_SUBSCRIPTION_CAP_REACHED, WATCH_SUBSCRIPTION_UNAUTHORIZED, WatchSubscriptionError,
    create_replicated_watch_subscription, delete_replicated_watch_subscription,
};

/// Outcome of serving a user's inbox read op through the resolved holder.
/// Keeps holder resolution and net orchestration out of the REST layer so the
/// api crate stays clear of the effect-boundary guard.
#[derive(Debug, Error)]
pub enum NotificationDispatchError {
    #[error("no inbox holder is currently available")]
    Unavailable,
    #[error("holder proxy failed: {0}")]
    Remote(String),
    #[error("{0}")]
    Internal(String),
}

/// Watch CRUD dispatch outcome. Distinguishes the per-user cap so the REST layer
/// can answer 409, and an unauthorized watched path so it can answer 403, instead
/// of a generic proxy failure.
#[derive(Debug, Error)]
pub enum WatchDispatchError {
    #[error("no inbox holder is currently available")]
    Unavailable,
    #[error("notification watch subscription cap reached")]
    CapExceeded,
    #[error("{WATCH_SUBSCRIPTION_UNAUTHORIZED}: {}", .0.as_str())]
    Unauthorized(WatchAuthorizationMetricReason),
    #[error("holder proxy failed: {0}")]
    Remote(String),
    #[error("{0}")]
    Internal(String),
}

impl From<NotificationDispatchError> for WatchDispatchError {
    fn from(error: NotificationDispatchError) -> Self {
        match error {
            NotificationDispatchError::Unavailable => WatchDispatchError::Unavailable,
            NotificationDispatchError::Remote(reason) => WatchDispatchError::Remote(reason),
            NotificationDispatchError::Internal(reason) => WatchDispatchError::Internal(reason),
        }
    }
}

/// Resolves the node currently holding `recipient`'s inbox, using the same
/// placement the read/write dispatch paths use. Exposed so the live-stream
/// endpoint can pick between the wake-driven local arm and the holder-polling
/// remote arm.
pub async fn resolve_inbox_holder_for_user(
    context: &DriverContext,
    recipient: UserId,
) -> Result<NodeId, NotificationDispatchError> {
    resolve_holder(context, recipient).await
}

/// Receiver half of the inbox wake channel; re-exported so the REST layer never
/// names the net handle's channel type directly.
pub type InboxWakeReceiver = broadcast::Receiver<UserId>;

pub fn record_watch_creation_denial_metric(
    context: &DriverContext,
    reason: WatchAuthorizationMetricReason,
) {
    if let Some(net_handle) = context.net_handle.as_ref() {
        net_handle
            .notification_watch_metrics()
            .record_creation_denial(reason);
    }
}

/// Subscribes to local inbox-delivery wakes. `Unavailable` when the node runs
/// without a net handle, matching the other dispatch paths.
pub fn subscribe_inbox_wakes(
    context: &DriverContext,
) -> Result<InboxWakeReceiver, NotificationDispatchError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(NotificationDispatchError::Unavailable)?;
    Ok(net_handle.subscribe_notification_wakes())
}

async fn resolve_holder(
    context: &DriverContext,
    recipient: UserId,
) -> Result<NodeId, NotificationDispatchError> {
    let config = drive(GetRealmConfigOperation::new(recipient.realm_id), context)
        .await
        .map_err(|error| match error {
            GetRealmConfigError::DocumentNotFound => NotificationDispatchError::Unavailable,
            other => NotificationDispatchError::Internal(other.to_string()),
        })?;
    match resolve_inbox_holder(&recipient, &config) {
        Ok(Some(holder)) => Ok(holder),
        Ok(None) => Err(NotificationDispatchError::Unavailable),
        Err(error) => Err(NotificationDispatchError::Internal(error.to_string())),
    }
}

pub async fn list_notifications_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), NotificationDispatchError> {
    let holder = resolve_holder(context, recipient).await?;
    if holder == local_node_id {
        list_notifications_on_holder(context, recipient, cursor, limit)
            .await
            .map_err(NotificationDispatchError::Internal)
    } else {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(NotificationDispatchError::Unavailable)?;
        list_remote(net_handle, holder, recipient, cursor, limit as u32)
            .await
            .map_err(NotificationDispatchError::Remote)
    }
}

async fn notification_is_visible(
    context: &DriverContext,
    recipient: UserId,
    record: &NotificationRecord,
) -> Result<bool, String> {
    if !matches!(
        record.kind,
        NotificationKind::MetadataCreated { .. } | NotificationKind::DataUploaded { .. }
    ) {
        return Ok(record.watch_authorization.is_none());
    }
    let Some(authorization) = record.watch_authorization.as_ref() else {
        return Ok(false);
    };
    if record.class != NotificationClass::Transient || record.recipient != recipient {
        return Ok(false);
    }
    match evaluate_watch_notification_authorization(context, recipient, &record.kind, authorization)
        .await?
    {
        WatchAuthorization::Authorized => Ok(true),
        WatchAuthorization::Denied(_) => Ok(false),
        WatchAuthorization::Unavailable(error) => Err(error),
    }
}

/// Reauthorizes persisted resource-watch records at the inbox-holder boundary.
/// Suppressed rows do not consume the caller's page, so revocation cannot hide
/// older ordinary notifications behind a page of stale watch records.
pub(crate) async fn list_notifications_on_holder(
    context: &DriverContext,
    recipient: UserId,
    mut cursor: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), String> {
    let limit = limit.clamp(1, LIST_NOTIFICATIONS_MAX_LIMIT);
    let mut records = Vec::with_capacity(limit);

    loop {
        let output = drive(
            ListNotificationsOperation::new(ListNotificationsInput {
                recipient,
                cursor,
                limit: limit - records.len(),
            }),
            context,
        )
        .await
        .map_err(|error| error.to_string())?;

        for record in output.records {
            if notification_is_visible(context, recipient, &record).await? {
                records.push(record);
            }
        }

        if records.len() == limit || output.next_cursor.is_none() {
            return Ok((records, output.next_cursor));
        }
        cursor = output.next_cursor;
    }
}

pub(crate) async fn unread_count_on_holder(
    context: &DriverContext,
    recipient: UserId,
) -> Result<(u32, bool), String> {
    let mut cursor = None;
    let mut count = 0usize;
    let mut examined = 0usize;

    loop {
        let output = drive(
            ListNotificationsOperation::new(ListNotificationsInput {
                recipient,
                cursor,
                limit: UNREAD_COUNT_CAP.min(UNREAD_SCAN_MAX_ROWS - examined),
            }),
            context,
        )
        .await
        .map_err(|error| error.to_string())?;
        examined += output.records.len();

        for record in output.records {
            if record.read_at_ms.is_none()
                && notification_is_visible(context, recipient, &record).await?
            {
                if count == UNREAD_COUNT_CAP {
                    return Ok((count as u32, true));
                }
                count += 1;
            }
        }

        if examined >= UNREAD_SCAN_MAX_ROWS && output.next_cursor.is_some() {
            return Ok((count as u32, true));
        }
        match output.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok((count as u32, false)),
        }
    }
}

pub async fn unread_count_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
) -> Result<(u32, bool), NotificationDispatchError> {
    let holder = resolve_holder(context, recipient).await?;
    if holder == local_node_id {
        unread_count_on_holder(context, recipient)
            .await
            .map_err(NotificationDispatchError::Internal)
    } else {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(NotificationDispatchError::Unavailable)?;
        unread_count_remote(net_handle, holder, recipient)
            .await
            .map_err(NotificationDispatchError::Remote)
    }
}

pub async fn mark_read_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
    ids: Vec<Ulid>,
    up_to_ms: Option<u64>,
) -> Result<u32, NotificationDispatchError> {
    let holder = resolve_holder(context, recipient).await?;
    if holder == local_node_id {
        let marked = mark_read_on_holder(context, recipient, ids, up_to_ms)
            .await
            .map_err(NotificationDispatchError::Internal)?;
        if marked > 0
            && let Some(net_handle) = context.net_handle.as_ref()
        {
            net_handle.notify_inbox_activity(recipient);
        }
        Ok(marked)
    } else {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(NotificationDispatchError::Unavailable)?;
        mark_read_remote(net_handle, holder, recipient, ids, up_to_ms)
            .await
            .map_err(NotificationDispatchError::Remote)
    }
}

pub(crate) async fn mark_read_on_holder(
    context: &DriverContext,
    recipient: UserId,
    mut ids: Vec<Ulid>,
    up_to_ms: Option<u64>,
) -> Result<u32, String> {
    if ids.len() > crate::notifications::mark_read::MARK_READ_MAX_IDS {
        return Err("mark read id count exceeds cap".to_string());
    }
    if ids.is_empty() && up_to_ms.is_none() {
        return Ok(0);
    }
    ids.sort_unstable();
    ids.dedup();
    let mut cursor = None;
    let mut marked = 0u32;
    let now_ms = unix_timestamp_millis();
    loop {
        let (records, next_cursor) =
            list_notifications_on_holder(context, recipient, cursor, LIST_NOTIFICATIONS_MAX_LIMIT)
                .await?;
        let visible_ids: Vec<_> = records
            .into_iter()
            .filter(|record| {
                record.read_at_ms.is_none()
                    && (ids.binary_search(&record.notification_id).is_ok()
                        || up_to_ms.is_some_and(|limit| record.created_at_ms <= limit))
            })
            .map(|record| record.notification_id)
            .collect();
        for chunk in visible_ids.chunks(crate::notifications::mark_read::MARK_READ_MAX_IDS) {
            marked += drive(
                MarkReadOperation::new(MarkReadInput {
                    recipient,
                    ids: chunk.to_vec(),
                    up_to_ms: None,
                    now_ms,
                }),
                context,
            )
            .await
            .map_err(|error| error.to_string())?
            .marked as u32;
        }
        match next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(marked),
        }
    }
}

pub async fn create_watch_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
    authorization: WatchAuthorizationBinding,
) -> Result<WatchSubscription, WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    let subscription = if holder == local_node_id {
        let subscription = create_replicated_watch_subscription(
            context,
            local_node_id,
            owner,
            path_prefix,
            event_mask,
            authorization.clone(),
            unix_timestamp_millis(),
        )
        .await
        .map_err(|error| match error {
            WatchSubscriptionError::CapExceeded => WatchDispatchError::CapExceeded,
            WatchSubscriptionError::Unauthorized(reason) => {
                WatchDispatchError::Unauthorized(reason)
            }
            other => WatchDispatchError::Internal(other.to_string()),
        })?;
        schedule_watch_interest_publish(context).await;
        subscription
    } else {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        create_watch_remote(
            net_handle,
            holder,
            owner,
            path_prefix,
            event_mask,
            authorization,
        )
        .await
        .map_err(|reason| {
            if reason == WATCH_SUBSCRIPTION_CAP_REACHED {
                WatchDispatchError::CapExceeded
            } else if let Some(reason) = reason
                .strip_prefix(WATCH_SUBSCRIPTION_UNAUTHORIZED)
                .and_then(|value| value.strip_prefix(": "))
                .and_then(WatchAuthorizationMetricReason::parse)
            {
                WatchDispatchError::Unauthorized(reason)
            } else {
                WatchDispatchError::Remote(reason)
            }
        })?
    };
    // Bridge the interest-digest propagation window: the node that handled the
    // create knows the holder now, so an event it emits before the holder's
    // digest replicates back still routes to the holder.
    if let Some(net_handle) = context.net_handle.as_ref() {
        net_handle.register_local_watch_interest(
            subscription.watch_id,
            owner.realm_id,
            holder,
            WatchInterestEntry {
                path_prefix: subscription.path_prefix.clone(),
                event_mask: subscription.event_mask,
            },
        );
    }
    Ok(subscription)
}

pub async fn delete_watch_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    if holder == local_node_id {
        delete_replicated_watch_subscription(
            context,
            local_node_id,
            owner,
            watch_id,
            unix_timestamp_millis(),
        )
        .await
        .map_err(|error| WatchDispatchError::Internal(error.to_string()))?;
        schedule_watch_interest_publish(context).await;
    } else {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        delete_watch_remote(net_handle, holder, owner, watch_id)
            .await
            .map_err(WatchDispatchError::Remote)?;
    }
    if let Some(net_handle) = context.net_handle.as_ref() {
        net_handle.retract_local_watch_interest(watch_id);
    }
    Ok(())
}

pub async fn list_watches_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    if holder == local_node_id {
        list_authorized_watch_subscriptions(context, owner)
            .await
            .map_err(WatchDispatchError::Internal)
    } else {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        list_watches_remote(net_handle, holder, owner)
            .await
            .map_err(WatchDispatchError::Remote)
    }
}
