//! Runs a user's notification and watch calls on the node that holds the user's inbox.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEventMask, WatchSubscription,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::notifications::dispatch::{InboxHolderOperation, NotificationDispatchError};
use aruna_operations::notifications::holder::HolderRead;
use aruna_operations::notifications::user_call::{
    UserAnswer, UserCall, UserCallOperation, WatchDispatchError,
};
use ulid::Ulid;

async fn run(
    context: &DriverContext,
    local: NodeId,
    user: UserId,
    call: UserCall,
) -> Result<UserAnswer, WatchDispatchError> {
    let transport = context.net_handle.is_some();
    let operation = UserCallOperation::new(user, local, transport, call);
    drive(operation, context).await
}

pub(crate) async fn resolve_user_holder(
    context: &DriverContext,
    recipient: UserId,
) -> Result<NodeId, NotificationDispatchError> {
    drive(InboxHolderOperation::new(recipient), context).await
}

pub(crate) async fn list_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), NotificationDispatchError> {
    let list = UserCall::Read(HolderRead::List { cursor, limit });
    run(context, local_node_id, recipient, list).await?.listed()
}

pub(crate) async fn unread_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
) -> Result<(u32, bool), NotificationDispatchError> {
    let unread = UserCall::Read(HolderRead::Unread);
    run(context, local_node_id, recipient, unread)
        .await?
        .unread()
}

pub(crate) async fn mark_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
    ids: Vec<Ulid>,
    up_to_ms: Option<u64>,
) -> Result<u32, NotificationDispatchError> {
    let mark = UserCall::Read(HolderRead::Mark {
        ids,
        up_to_ms,
        now_ms: unix_timestamp_millis(),
    });
    run(context, local_node_id, recipient, mark).await?.marked()
}

pub(crate) async fn create_for_user(
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
    run(context, local_node_id, owner, create).await?.created()
}

pub(crate) async fn delete_for_user(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), WatchDispatchError> {
    let delete = UserCall::DeleteWatch {
        watch_id,
        now_ms: unix_timestamp_millis(),
    };
    run(context, local_node_id, owner, delete).await?.deleted()
}

pub(crate) async fn list_watches(
    context: &DriverContext,
    local_node_id: NodeId,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, WatchDispatchError> {
    run(context, local_node_id, owner, UserCall::ListWatches)
        .await?
        .watches()
}
