//! Routes notification and watch calls to the local inbox holder or to a remote node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::metrics::WatchMetricReason;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEventMask, WatchInterestEntry, WatchSubscription,
};
use aruna_core::time::unix_timestamp_millis;
use thiserror::Error;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::driver::{DriverContext, drive};
use crate::notifications::client::{
    HolderRequestOperation, listed, marked_read, unread_counted, watch_created, watch_deleted,
    watches_listed,
};
use crate::notifications::holder::{HolderAnswer, HolderRead, HolderReadOperation};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::watch::authorization::list_authorized_subscriptions;
use crate::notifications::watch::digest::InterestOperation;
use crate::notifications::watch::records::{
    WATCH_CAP_REACHED, WATCH_SUBSCRIPTION_UNAUTHORIZED, WATCH_SUBSCRIPTION_UNAVAILABLE,
    WatchSubscriptionError,
};
use crate::notifications::watch::subscriptions::{create_holder_watch, delete_holder_watch};

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
    Unauthorized(WatchMetricReason),
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

/// Resolves the node currently holding `recipient`'s inbox with the same
/// placement as read/write dispatch, so the live-stream endpoint can pick the
/// wake-driven local or holder-polling remote arm.
pub async fn resolve_user_holder(
    context: &DriverContext,
    recipient: UserId,
) -> Result<NodeId, NotificationDispatchError> {
    resolve_holder(context, recipient).await
}

/// Receiver half of the inbox wake channel; re-exported so the REST layer never
/// names the net handle's channel type directly.
pub type InboxWakeReceiver = broadcast::Receiver<UserId>;

pub fn record_watch_denial(context: &DriverContext, reason: WatchMetricReason) {
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
    let config = drive(GetConfigOperation::new(recipient.realm_id), context)
        .await
        .map_err(|error| match error {
            GetConfigError::DocumentNotFound => NotificationDispatchError::Unavailable,
            other => NotificationDispatchError::Internal(other.to_string()),
        })?;
    match resolve_inbox_holder(&recipient, &config) {
        Ok(Some(holder)) => Ok(holder),
        Ok(None) => Err(NotificationDispatchError::Unavailable),
        Err(error) => Err(NotificationDispatchError::Internal(error.to_string())),
    }
}

pub async fn list_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), NotificationDispatchError> {
    let holder = resolve_holder(context, recipient).await?;
    if holder == local_node_id {
        match read_on_holder(context, recipient, HolderRead::List { cursor, limit }).await? {
            HolderAnswer::Listed {
                records,
                next_cursor,
            } => Ok((records, next_cursor)),
            other => Err(other_answer(other)),
        }
    } else {
        context
            .net_handle
            .as_ref()
            .ok_or(NotificationDispatchError::Unavailable)?;
        let limit = limit as u32;
        let list = NotificationTransportMessage::List {
            recipient,
            cursor,
            limit,
        };
        let answer = drive(HolderRequestOperation::new(holder, list), context).await;
        listed(answer).map_err(NotificationDispatchError::Remote)
    }
}

/// Reads `recipient`'s inbox on this node, its holder, as the recipient sees it.
async fn read_on_holder(
    context: &DriverContext,
    recipient: UserId,
    read: HolderRead,
) -> Result<HolderAnswer, NotificationDispatchError> {
    let answer = drive(HolderReadOperation::new(recipient, read), context).await;
    answer.map_err(|error| NotificationDispatchError::Internal(error.to_string()))
}

fn other_answer(answer: HolderAnswer) -> NotificationDispatchError {
    NotificationDispatchError::Internal(format!("unexpected holder answer {answer:?}"))
}

pub async fn unread_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
) -> Result<(u32, bool), NotificationDispatchError> {
    let holder = resolve_holder(context, recipient).await?;
    if holder == local_node_id {
        match read_on_holder(context, recipient, HolderRead::Unread).await? {
            HolderAnswer::Unread { count, capped } => Ok((count, capped)),
            other => Err(other_answer(other)),
        }
    } else {
        context
            .net_handle
            .as_ref()
            .ok_or(NotificationDispatchError::Unavailable)?;
        let count = NotificationTransportMessage::UnreadCount { recipient };
        let answer = drive(HolderRequestOperation::new(holder, count), context).await;
        unread_counted(answer).map_err(NotificationDispatchError::Remote)
    }
}

pub async fn mark_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
    ids: Vec<Ulid>,
    up_to_ms: Option<u64>,
) -> Result<u32, NotificationDispatchError> {
    let holder = resolve_holder(context, recipient).await?;
    if holder == local_node_id {
        let now_ms = unix_timestamp_millis();
        let mark = HolderRead::Mark {
            ids,
            up_to_ms,
            now_ms,
        };
        let marked = match read_on_holder(context, recipient, mark).await? {
            HolderAnswer::Marked(marked) => marked,
            other => return Err(other_answer(other)),
        };
        if marked > 0
            && let Some(net_handle) = context.net_handle.as_ref()
        {
            net_handle.notify_inbox_activity(recipient);
        }
        Ok(marked)
    } else {
        context
            .net_handle
            .as_ref()
            .ok_or(NotificationDispatchError::Unavailable)?;
        let mark = NotificationTransportMessage::MarkRead {
            recipient,
            ids,
            up_to_ms,
        };
        let answer = drive(HolderRequestOperation::new(holder, mark), context).await;
        marked_read(answer).map_err(NotificationDispatchError::Remote)
    }
}

pub async fn create_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
    authorization: WatchAuthorizationBinding,
) -> Result<WatchSubscription, WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    let subscription = if holder == local_node_id {
        let subscription = create_holder_watch(
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
            WatchSubscriptionError::AuthorizationUnavailable(_) => WatchDispatchError::Unavailable,
            other => WatchDispatchError::Internal(other.to_string()),
        })?;
        let _ = drive(InterestOperation::publish(), context).await;
        subscription
    } else {
        context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        let create = NotificationTransportMessage::CreateWatch {
            owner,
            path_prefix,
            event_mask,
            authorization,
        };
        watch_created(drive(HolderRequestOperation::new(holder, create), context).await).map_err(
            |reason| {
                if reason == WATCH_CAP_REACHED {
                    WatchDispatchError::CapExceeded
                } else if let Some(reason) = reason
                    .strip_prefix(WATCH_SUBSCRIPTION_UNAUTHORIZED)
                    .and_then(|value| value.strip_prefix(": "))
                    .and_then(WatchMetricReason::parse)
                {
                    WatchDispatchError::Unauthorized(reason)
                } else if reason.starts_with(WATCH_SUBSCRIPTION_UNAVAILABLE) {
                    WatchDispatchError::Unavailable
                } else {
                    WatchDispatchError::Remote(reason)
                }
            },
        )?
    };
    // The creator knows the holder before its interest digest propagates.
    // Register locally so events in that window still reach the holder.
    if let Some(net_handle) = context.net_handle.as_ref() {
        net_handle.register_local_interest(
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

pub async fn delete_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    if holder == local_node_id {
        delete_holder_watch(
            context,
            local_node_id,
            owner,
            watch_id,
            unix_timestamp_millis(),
        )
        .await
        .map_err(|error| WatchDispatchError::Internal(error.to_string()))?;
        let _ = drive(InterestOperation::publish(), context).await;
    } else {
        context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        let delete = NotificationTransportMessage::DeleteWatch { owner, watch_id };
        let answer = drive(HolderRequestOperation::new(holder, delete), context).await;
        watch_deleted(answer).map_err(WatchDispatchError::Remote)?;
    }
    if let Some(net_handle) = context.net_handle.as_ref() {
        net_handle.retract_local_interest(watch_id);
    }
    Ok(())
}

pub async fn list_watches(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, WatchDispatchError> {
    let holder = resolve_holder(context, owner).await?;
    if holder == local_node_id {
        list_authorized_subscriptions(context, owner)
            .await
            .map_err(WatchDispatchError::Internal)
    } else {
        context
            .net_handle
            .as_ref()
            .ok_or(WatchDispatchError::Unavailable)?;
        let list = NotificationTransportMessage::ListWatches { owner };
        let answer = drive(HolderRequestOperation::new(holder, list), context).await;
        watches_listed(answer).map_err(WatchDispatchError::Remote)
    }
}
