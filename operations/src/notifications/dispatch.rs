//! Runs a user's notification and watch calls on the node that holds the user's inbox, for the
//! REST layer, so it stays clear of the effect boundary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::metrics::WatchMetricReason;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEventMask, WatchSubscription,
};
use aruna_core::time::unix_timestamp_millis;
use thiserror::Error;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::driver::{DriverContext, drive};
use crate::notifications::holder::{HolderAnswer, HolderRead};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::user_call::{
    UserAnswer, UserCall, UserCallOperation, WatchDispatchError,
};

/// Outcome of serving a user's inbox call through the resolved holder.
#[derive(Debug, Error)]
pub enum NotificationDispatchError {
    #[error("no inbox holder is currently available")]
    Unavailable,
    #[error("holder proxy failed: {0}")]
    Remote(String),
    #[error("{0}")]
    Internal(String),
}

impl From<WatchDispatchError> for NotificationDispatchError {
    fn from(error: WatchDispatchError) -> Self {
        match error {
            WatchDispatchError::Unavailable => Self::Unavailable,
            WatchDispatchError::Remote(reason) => Self::Remote(reason),
            WatchDispatchError::Internal(reason) => Self::Internal(reason),
            other @ (WatchDispatchError::CapExceeded | WatchDispatchError::Unauthorized(_)) => {
                Self::Internal(other.to_string())
            }
        }
    }
}

/// Runs `call` for `user` on the holder of the user's inbox.
async fn run(
    context: &DriverContext,
    local: NodeId,
    user: UserId,
    call: UserCall,
) -> Result<UserAnswer, WatchDispatchError> {
    let transport = context.net_handle.is_some();
    drive(
        UserCallOperation::new(user, local, transport, call),
        context,
    )
    .await
}

/// An answer of another kind than the call asks for.
fn other_answer(answer: UserAnswer) -> WatchDispatchError {
    WatchDispatchError::Internal(format!("unexpected dispatch answer {answer:?}"))
}

/// Resolves the node currently holding `recipient`'s inbox, so the live-stream endpoint can pick
/// the wake-driven local or holder-polling remote arm.
pub async fn resolve_user_holder(
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

/// Receiver half of the inbox wake channel, so the REST layer never names the net handle's
/// channel type directly.
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

async fn read(
    context: &DriverContext,
    local: NodeId,
    recipient: UserId,
    read: HolderRead,
) -> Result<HolderAnswer, NotificationDispatchError> {
    match run(context, local, recipient, UserCall::Read(read)).await? {
        UserAnswer::Read(answer) => Ok(answer),
        other => Err(other_answer(other).into()),
    }
}

fn other_read(answer: HolderAnswer) -> NotificationDispatchError {
    NotificationDispatchError::Internal(format!("unexpected holder answer {answer:?}"))
}

pub async fn list_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), NotificationDispatchError> {
    let list = HolderRead::List { cursor, limit };
    match read(context, local_node_id, recipient, list).await? {
        HolderAnswer::Listed {
            records,
            next_cursor,
        } => Ok((records, next_cursor)),
        other => Err(other_read(other)),
    }
}

pub async fn unread_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
) -> Result<(u32, bool), NotificationDispatchError> {
    match read(context, local_node_id, recipient, HolderRead::Unread).await? {
        HolderAnswer::Unread { count, capped } => Ok((count, capped)),
        other => Err(other_read(other)),
    }
}

pub async fn mark_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
    ids: Vec<Ulid>,
    up_to_ms: Option<u64>,
) -> Result<u32, NotificationDispatchError> {
    let mark = HolderRead::Mark {
        ids,
        up_to_ms,
        now_ms: unix_timestamp_millis(),
    };
    match read(context, local_node_id, recipient, mark).await? {
        HolderAnswer::Marked(marked) => Ok(marked),
        other => Err(other_read(other)),
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
    let create = UserCall::CreateWatch {
        path_prefix,
        event_mask,
        authorization,
        now_ms: unix_timestamp_millis(),
    };
    match run(context, local_node_id, owner, create).await? {
        UserAnswer::Created(subscription) => Ok(subscription),
        other => Err(other_answer(other)),
    }
}

pub async fn delete_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), WatchDispatchError> {
    let delete = UserCall::DeleteWatch {
        watch_id,
        now_ms: unix_timestamp_millis(),
    };
    match run(context, local_node_id, owner, delete).await? {
        UserAnswer::Deleted => Ok(()),
        other => Err(other_answer(other)),
    }
}

pub async fn list_watches(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, WatchDispatchError> {
    match run(context, local_node_id, owner, UserCall::ListWatches).await? {
        UserAnswer::Watches(subscriptions) => Ok(subscriptions),
        other => Err(other_answer(other)),
    }
}
