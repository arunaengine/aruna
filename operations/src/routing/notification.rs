use aruna_core::structs::AuthContext;
use aruna_core::util::unix_timestamp_millis;

use crate::driver::{DriverContext, drive};
use crate::notifications::list::{ListNotificationsInput, ListNotificationsOperation};
use crate::notifications::mark_read::{MarkReadInput, MarkReadOperation};
use crate::notifications::unread::{UnreadCountInput, UnreadCountOperation};
use crate::notifications::watch::interest::schedule_watch_interest_publish;
use crate::notifications::watch::subscriptions::{
    WATCH_SUBSCRIPTION_CAP_REACHED, WatchSubscriptionError, create_watch_subscription,
    delete_watch_subscription, list_watch_subscriptions,
};
use crate::routing::protocol::{
    HolderProxyResponse, NotificationCall, NotificationReply, ProxiedReply,
};

fn ok(reply: NotificationReply) -> HolderProxyResponse {
    HolderProxyResponse::Ok(ProxiedReply::Notification(Box::new(reply)))
}

fn internal(reason: impl ToString) -> HolderProxyResponse {
    HolderProxyResponse::internal(reason.to_string())
}

/// Serves a user-facing inbox read or watch mutation on this holder. The served
/// recipient is derived from the validated bearer, never the wire claim — this
/// is the flaw the forwarded-bearer path closes.
pub(crate) async fn serve_notification_call(
    context: &DriverContext,
    call: NotificationCall,
    auth: Option<AuthContext>,
) -> HolderProxyResponse {
    let Some(auth) = auth else {
        return HolderProxyResponse::forbidden("notification access requires a bearer token");
    };
    let recipient = auth.user_id;

    match call {
        NotificationCall::List { cursor, limit, .. } => {
            match drive(
                ListNotificationsOperation::new(ListNotificationsInput {
                    recipient,
                    cursor,
                    limit: limit as usize,
                }),
                context,
            )
            .await
            {
                Ok(output) => ok(NotificationReply::List {
                    records: output.records,
                    next_cursor: output.next_cursor,
                }),
                Err(error) => internal(error),
            }
        }
        NotificationCall::UnreadCount { .. } => {
            match drive(
                UnreadCountOperation::new(UnreadCountInput { recipient }),
                context,
            )
            .await
            {
                Ok(output) => ok(NotificationReply::UnreadCount {
                    count: output.count as u32,
                    capped: output.capped,
                }),
                Err(error) => internal(error),
            }
        }
        NotificationCall::MarkRead { ids, up_to_ms, .. } => {
            match drive(
                MarkReadOperation::new(MarkReadInput {
                    recipient,
                    ids,
                    up_to_ms,
                    now_ms: unix_timestamp_millis(),
                }),
                context,
            )
            .await
            {
                Ok(output) => {
                    if output.marked > 0
                        && let Some(net_handle) = context.net_handle.as_ref()
                    {
                        net_handle.notify_inbox_activity(recipient);
                    }
                    ok(NotificationReply::MarkRead {
                        marked: output.marked as u32,
                    })
                }
                Err(error) => internal(error),
            }
        }
        NotificationCall::CreateWatch {
            path_prefix,
            event_mask,
            ..
        } => {
            match create_watch_subscription(
                &context.storage_handle,
                recipient,
                path_prefix,
                event_mask,
                unix_timestamp_millis(),
            )
            .await
            {
                Ok(subscription) => {
                    schedule_watch_interest_publish(context).await;
                    ok(NotificationReply::Watch(subscription))
                }
                Err(WatchSubscriptionError::CapExceeded) => {
                    HolderProxyResponse::conflict(WATCH_SUBSCRIPTION_CAP_REACHED)
                }
                Err(error) => internal(error),
            }
        }
        NotificationCall::ListWatches { .. } => {
            match list_watch_subscriptions(&context.storage_handle, recipient).await {
                Ok(subscriptions) => ok(NotificationReply::Watches(subscriptions)),
                Err(error) => internal(error),
            }
        }
        NotificationCall::DeleteWatch { watch_id, .. } => {
            match delete_watch_subscription(&context.storage_handle, recipient, watch_id).await {
                Ok(()) => {
                    schedule_watch_interest_publish(context).await;
                    ok(NotificationReply::Ack)
                }
                Err(error) => internal(error),
            }
        }
    }
}
