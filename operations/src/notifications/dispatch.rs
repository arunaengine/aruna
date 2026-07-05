use aruna_core::NodeId;
use aruna_core::structs::NotificationRecord;
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use crate::notifications::client::{list_remote, mark_read_remote, unread_count_remote};
use crate::notifications::list::{ListNotificationsInput, ListNotificationsOperation};
use crate::notifications::mark_read::{MarkReadInput, MarkReadOperation};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::unread::{UnreadCountInput, UnreadCountOperation};

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
