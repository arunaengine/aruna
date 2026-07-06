use aruna_core::NodeId;
use aruna_core::structs::{NotificationRecord, WatchEventMask, WatchSubscription};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use crate::notifications::client::{
    create_watch_remote, delete_watch_remote, list_remote, list_watches_remote, mark_read_remote,
    unread_count_remote,
};
use crate::notifications::list::{ListNotificationsInput, ListNotificationsOperation};
use crate::notifications::mark_read::{MarkReadInput, MarkReadOperation};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::unread::{UnreadCountInput, UnreadCountOperation};
use crate::notifications::watch::interest::schedule_watch_interest_publish;
use crate::notifications::watch::subscriptions::{
    WATCH_SUBSCRIPTION_CAP_REACHED, WatchSubscriptionError, create_watch_subscription,
    delete_watch_subscription, list_watch_subscriptions,
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
/// can answer 409 instead of a generic proxy failure.
#[derive(Debug, Error)]
pub enum WatchDispatchError {
    #[error("no inbox holder is currently available")]
    Unavailable,
    #[error("notification watch subscription cap reached")]
    CapExceeded,
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
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(NotificationDispatchError::Unavailable)?;
        list_remote(net_handle, holder, recipient, cursor, limit as u32)
            .await
            .map_err(NotificationDispatchError::Remote)
    }
}

pub async fn unread_count_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
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
        let net_handle = context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        create_watch_remote(net_handle, holder, owner, path_prefix, event_mask)
            .await
            .map_err(|reason| {
                if reason == WATCH_SUBSCRIPTION_CAP_REACHED {
                    WatchDispatchError::CapExceeded
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
        delete_watch_subscription(&context.storage_handle, owner, watch_id)
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
        list_watch_subscriptions(&context.storage_handle, owner)
            .await
            .map_err(|error| WatchDispatchError::Internal(error.to_string()))
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
