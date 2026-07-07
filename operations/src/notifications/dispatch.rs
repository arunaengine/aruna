use aruna_core::NodeId;
use aruna_core::structs::{NotificationRecord, WatchEventMask, WatchSubscription};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use thiserror::Error;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use crate::notifications::list::{ListNotificationsInput, ListNotificationsOperation};
use crate::notifications::mark_read::{MarkReadInput, MarkReadOperation};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::unread::{UnreadCountInput, UnreadCountOperation};
use crate::notifications::watch::interest::schedule_watch_interest_publish;
use crate::notifications::watch::subscriptions::{
    WatchSubscriptionError, create_watch_subscription, delete_watch_subscription,
    list_watch_subscriptions,
};
use crate::routing::dispatch::{HolderRoutingError, dispatch_holder_call};
use crate::routing::protocol::{NotificationCall, NotificationReply, ProxiedCall, ProxiedReply};

/// Outcome of serving a user's inbox read op through the resolved holder.
/// Keeps holder resolution and net orchestration out of the REST layer so the
/// api crate stays clear of the effect-boundary guard.
#[derive(Debug, Error)]
pub enum NotificationDispatchError {
    #[error("no inbox holder is currently available")]
    Unavailable,
    #[error("holder rejected the request: {0}")]
    Forbidden(String),
    #[error("holder proxy failed: {0}")]
    Remote(String),
    #[error("{0}")]
    Internal(String),
}

/// Watch CRUD dispatch outcome. Distinguishes the per-user cap so the REST layer
/// can answer 409 instead of a generic proxy failure.
#[derive(Debug, Error)]
pub enum WatchDispatchError {
    #[error("no inbox holder is currently available")]
    Unavailable,
    #[error("notification watch subscription cap reached")]
    CapExceeded,
    #[error("holder rejected the request: {0}")]
    Forbidden(String),
    #[error("holder proxy failed: {0}")]
    Remote(String),
    #[error("{0}")]
    Internal(String),
}

impl From<NotificationDispatchError> for WatchDispatchError {
    fn from(error: NotificationDispatchError) -> Self {
        match error {
            NotificationDispatchError::Unavailable => WatchDispatchError::Unavailable,
            NotificationDispatchError::Forbidden(reason) => WatchDispatchError::Forbidden(reason),
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

/// Routes a user-facing inbox read to the recipient's inbox holder. Serving
/// locally stays a direct drive; a remote holder is reached over the holder
/// proxy with the caller's bearer forwarded, so the holder re-derives the
/// recipient from the validated identity instead of trusting a wire claim.
async fn dispatch_notification(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
    call: NotificationCall,
) -> Result<NotificationReply, NotificationDispatchError> {
    let reply = dispatch_holder_call(
        context,
        local_node_id,
        bearer,
        ProxiedCall::Notification(call),
    )
    .await
    .map_err(|error| match error {
        HolderRoutingError::Unavailable => NotificationDispatchError::Unavailable,
        HolderRoutingError::Unauthorized(reason) => NotificationDispatchError::Forbidden(reason),
        HolderRoutingError::NotFound => {
            NotificationDispatchError::Internal("inbox holder reported not found".to_string())
        }
        HolderRoutingError::Conflict(reason)
        | HolderRoutingError::BadRequest(reason)
        | HolderRoutingError::Internal(reason) => NotificationDispatchError::Internal(reason),
    })?;
    match reply {
        ProxiedReply::Notification(notification_reply) => Ok(*notification_reply),
        // A holder that answers with the wrong domain is a misbehaving upstream.
        _ => Err(NotificationDispatchError::Remote(
            "holder returned a non-notification reply".to_string(),
        )),
    }
}

/// Watch variant of [`dispatch_notification`] that recovers the per-user cap
/// signal the holder returns as a rejection reason.
async fn dispatch_watch(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
    call: NotificationCall,
) -> Result<NotificationReply, WatchDispatchError> {
    let reply = dispatch_holder_call(
        context,
        local_node_id,
        bearer,
        ProxiedCall::Notification(call),
    )
    .await
    .map_err(|error| match error {
        HolderRoutingError::Unavailable => WatchDispatchError::Unavailable,
        // The per-user watch cap is the only conflict a watch holder returns.
        HolderRoutingError::Conflict(_) => WatchDispatchError::CapExceeded,
        HolderRoutingError::Unauthorized(reason) => WatchDispatchError::Forbidden(reason),
        HolderRoutingError::NotFound => {
            WatchDispatchError::Internal("inbox holder reported not found".to_string())
        }
        HolderRoutingError::BadRequest(reason) | HolderRoutingError::Internal(reason) => {
            WatchDispatchError::Internal(reason)
        }
    })?;
    match reply {
        ProxiedReply::Notification(notification_reply) => Ok(*notification_reply),
        // A holder that answers with the wrong domain is a misbehaving upstream.
        _ => Err(WatchDispatchError::Remote(
            "holder returned a non-notification reply".to_string(),
        )),
    }
}

pub async fn list_notifications_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
    recipient: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), NotificationDispatchError> {
    let holder = resolve_holder(context, recipient).await?;
    if holder == local_node_id {
        let output = drive(
            ListNotificationsOperation::new(ListNotificationsInput {
                recipient,
                cursor,
                limit,
            }),
            context,
        )
        .await
        .map_err(|error| NotificationDispatchError::Internal(error.to_string()))?;
        Ok((output.records, output.next_cursor))
    } else {
        match dispatch_notification(
            context,
            local_node_id,
            bearer,
            NotificationCall::List {
                recipient,
                cursor,
                limit: limit as u32,
            },
        )
        .await?
        {
            NotificationReply::List {
                records,
                next_cursor,
            } => Ok((records, next_cursor)),
            _ => Err(unexpected_notification_reply()),
        }
    }
}

pub async fn unread_count_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
    recipient: UserId,
) -> Result<(u32, bool), NotificationDispatchError> {
    let holder = resolve_holder(context, recipient).await?;
    if holder == local_node_id {
        let output = drive(
            UnreadCountOperation::new(UnreadCountInput { recipient }),
            context,
        )
        .await
        .map_err(|error| NotificationDispatchError::Internal(error.to_string()))?;
        Ok((output.count as u32, output.capped))
    } else {
        match dispatch_notification(
            context,
            local_node_id,
            bearer,
            NotificationCall::UnreadCount { recipient },
        )
        .await?
        {
            NotificationReply::UnreadCount { count, capped } => Ok((count, capped)),
            _ => Err(unexpected_notification_reply()),
        }
    }
}

pub async fn mark_read_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
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
        match dispatch_notification(
            context,
            local_node_id,
            bearer,
            NotificationCall::MarkRead {
                recipient,
                ids,
                up_to_ms,
            },
        )
        .await?
        {
            NotificationReply::MarkRead { marked } => Ok(marked),
            _ => Err(unexpected_notification_reply()),
        }
    }
}

pub async fn create_watch_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
) -> Result<WatchSubscription, WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    if holder == local_node_id {
        let subscription = create_watch_subscription(
            &context.storage_handle,
            owner,
            path_prefix,
            event_mask,
            unix_timestamp_millis(),
        )
        .await
        .map_err(|error| match error {
            WatchSubscriptionError::CapExceeded => WatchDispatchError::CapExceeded,
            other => WatchDispatchError::Internal(other.to_string()),
        })?;
        schedule_watch_interest_publish(context).await;
        Ok(subscription)
    } else {
        match dispatch_watch(
            context,
            local_node_id,
            bearer,
            NotificationCall::CreateWatch {
                recipient: owner,
                path_prefix,
                event_mask,
            },
        )
        .await?
        {
            NotificationReply::Watch(subscription) => Ok(subscription),
            _ => Err(unexpected_watch_reply()),
        }
    }
}

pub async fn delete_watch_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    if holder == local_node_id {
        delete_watch_subscription(&context.storage_handle, owner, watch_id)
            .await
            .map_err(|error| WatchDispatchError::Internal(error.to_string()))?;
        schedule_watch_interest_publish(context).await;
        Ok(())
    } else {
        match dispatch_watch(
            context,
            local_node_id,
            bearer,
            NotificationCall::DeleteWatch {
                recipient: owner,
                watch_id,
            },
        )
        .await?
        {
            NotificationReply::Ack => Ok(()),
            _ => Err(unexpected_watch_reply()),
        }
    }
}

pub async fn list_watches_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    bearer: Option<&str>,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    if holder == local_node_id {
        list_watch_subscriptions(&context.storage_handle, owner)
            .await
            .map_err(|error| WatchDispatchError::Internal(error.to_string()))
    } else {
        match dispatch_watch(
            context,
            local_node_id,
            bearer,
            NotificationCall::ListWatches { recipient: owner },
        )
        .await?
        {
            NotificationReply::Watches(subscriptions) => Ok(subscriptions),
            _ => Err(unexpected_watch_reply()),
        }
    }
}

fn unexpected_notification_reply() -> NotificationDispatchError {
    NotificationDispatchError::Internal(
        "holder returned an unexpected notification reply".to_string(),
    )
}

fn unexpected_watch_reply() -> WatchDispatchError {
    WatchDispatchError::Internal("holder returned an unexpected notification reply".to_string())
}
