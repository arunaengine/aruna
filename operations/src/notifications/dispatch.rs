use aruna_core::NodeId;
use aruna_core::structs::{
    NotificationKind, NotificationRecord, WatchEventKind, WatchEventMask, WatchSubscription,
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
    is_watch_authorized, list_authorized_watch_subscriptions,
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
    #[error("{WATCH_SUBSCRIPTION_UNAUTHORIZED}")]
    Unauthorized,
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
    let watch = match &record.kind {
        NotificationKind::MetadataCreated { path, .. } => Some((
            path.as_str(),
            WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]),
        )),
        NotificationKind::DataUploaded { path, .. } => Some((
            path.as_str(),
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
        )),
        _ => None,
    };
    match watch {
        None => Ok(true),
        Some((path, event_mask)) => {
            is_watch_authorized(context, recipient.realm_id, recipient, path, event_mask).await
        }
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
        let output = drive(
            MarkReadOperation::new(MarkReadInput {
                recipient,
                ids,
                up_to_ms,
                now_ms: unix_timestamp_millis(),
            }),
            context,
        )
        .await
        .map_err(|error| NotificationDispatchError::Internal(error.to_string()))?;
        if output.marked > 0
            && let Some(net_handle) = context.net_handle.as_ref()
        {
            net_handle.notify_inbox_activity(recipient);
        }
        Ok(output.marked as u32)
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

pub async fn create_watch_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
) -> Result<WatchSubscription, WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    if holder == local_node_id {
        let subscription = create_replicated_watch_subscription(
            context,
            local_node_id,
            owner,
            path_prefix,
            event_mask,
            unix_timestamp_millis(),
        )
        .await
        .map_err(|error| match error {
            WatchSubscriptionError::CapExceeded => WatchDispatchError::CapExceeded,
            WatchSubscriptionError::Unauthorized => WatchDispatchError::Unauthorized,
            other => WatchDispatchError::Internal(other.to_string()),
        })?;
        schedule_watch_interest_publish(context).await;
        Ok(subscription)
    } else {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        create_watch_remote(net_handle, holder, owner, path_prefix, event_mask)
            .await
            .map_err(|reason| {
                if reason == WATCH_SUBSCRIPTION_CAP_REACHED {
                    WatchDispatchError::CapExceeded
                } else if reason == WATCH_SUBSCRIPTION_UNAUTHORIZED {
                    WatchDispatchError::Unauthorized
                } else {
                    WatchDispatchError::Remote(reason)
                }
            })
    }
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
        Ok(())
    } else {
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        delete_watch_remote(net_handle, holder, owner, watch_id)
            .await
            .map_err(WatchDispatchError::Remote)
    }
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
