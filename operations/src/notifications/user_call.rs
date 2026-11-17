//! Runs a user's notification or watch call on the node that holds the user's inbox: here
//! through the holder operations, or on the holder through one request frame.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::metrics::WatchMetricReason;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEventMask, WatchInterestEntry, WatchSubscription,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use thiserror::Error;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::notifications::client::{
    HolderRequestOperation, RemoteError, listed, marked_read, unread_counted, watch_created,
    watch_deleted, watches_listed,
};
use crate::notifications::holder::{
    HolderAnswer, HolderRead, HolderReadError, HolderReadOperation,
};
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::watch::check::{
    AuthorizedWatchSubscriptions, WatchCheckError, WatchFilterOperation,
};
use crate::notifications::watch::digest::InterestOperation;
use crate::notifications::watch::records::{
    WATCH_CAP_REACHED, WATCH_SUBSCRIPTION_UNAUTHORIZED, WATCH_SUBSCRIPTION_UNAVAILABLE,
    WatchSubscriptionError, watch_delete_replication,
};
use crate::notifications::watch::store::{CreateWatchOperation, StoreWatchOperation};

/// Watch CRUD dispatch outcome. Distinguishes the per-user cap so the REST layer
/// can answer 409, and an unauthorized watched path so it can answer 403, instead
/// of a generic proxy failure.
#[derive(Debug, Error, PartialEq)]
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

impl From<IoError> for WatchDispatchError {
    fn from(error: IoError) -> Self {
        Self::Internal(error.to_string())
    }
}

/// What a user asks of its inbox or its watches.
#[derive(Clone, Debug, PartialEq)]
pub enum UserCall {
    Read(HolderRead),
    CreateWatch {
        path_prefix: String,
        event_mask: WatchEventMask,
        authorization: WatchAuthorizationBinding,
        now_ms: u64,
    },
    DeleteWatch {
        watch_id: Ulid,
        now_ms: u64,
    },
    ListWatches,
}

#[derive(Debug, PartialEq)]
pub enum UserAnswer {
    Read(HolderAnswer),
    Created(WatchSubscription),
    Deleted,
    Watches(Vec<WatchSubscription>),
}

#[derive(Debug, Default, PartialEq)]
enum CallState {
    #[default]
    Config,
    Local,
    Remote(NodeId),
}

type Turn = Result<Next<UserAnswer>, WatchDispatchError>;

/// Resolves the holder of the user's inbox from the realm config, then runs the call there.
/// A new watch is noted in this node's interest at once, so events reach the holder before its
/// interest digest arrives; a deleted one is removed from it.
#[derive(Debug, PartialEq)]
pub struct UserCallOperation {
    user: UserId,
    local: NodeId,
    /// Whether this node can reach other nodes; without it only local calls run.
    transport: bool,
    call: UserCall,
    state: CallState,
}

fn watch_error(error: WatchSubscriptionError) -> WatchDispatchError {
    match error {
        WatchSubscriptionError::CapExceeded => WatchDispatchError::CapExceeded,
        WatchSubscriptionError::Unauthorized(reason) => WatchDispatchError::Unauthorized(reason),
        WatchSubscriptionError::AuthorizationUnavailable(_) => WatchDispatchError::Unavailable,
        other => WatchDispatchError::Internal(other.to_string()),
    }
}

/// A holder's refusal of a new watch, as the REST layer tells it apart.
fn remote_create_error(reason: String) -> WatchDispatchError {
    if reason == WATCH_CAP_REACHED {
        return WatchDispatchError::CapExceeded;
    }
    let unauthorized = reason
        .strip_prefix(WATCH_SUBSCRIPTION_UNAUTHORIZED)
        .and_then(|value| value.strip_prefix(": "))
        .and_then(WatchMetricReason::parse);
    match unauthorized {
        Some(reason) => WatchDispatchError::Unauthorized(reason),
        None if reason.starts_with(WATCH_SUBSCRIPTION_UNAVAILABLE) => {
            WatchDispatchError::Unavailable
        }
        None => WatchDispatchError::Remote(reason),
    }
}

impl UserCallOperation {
    pub fn new(user: UserId, local: NodeId, transport: bool, call: UserCall) -> Self {
        Self {
            user,
            local,
            transport,
            call,
            state: CallState::Config,
        }
    }

    fn resolved(&mut self, config: Result<RealmConfigDocument, GetConfigError>) -> Turn {
        let config = config.map_err(|error| match error {
            GetConfigError::DocumentNotFound => WatchDispatchError::Unavailable,
            other => WatchDispatchError::Internal(other.to_string()),
        })?;
        let holder = match resolve_inbox_holder(&self.user, &config) {
            Ok(Some(holder)) => holder,
            Ok(None) => return Err(WatchDispatchError::Unavailable),
            Err(error) => return Err(WatchDispatchError::Internal(error.to_string())),
        };
        if holder == self.local {
            self.state = CallState::Local;
            return self.local_call();
        }
        if !self.transport {
            return Err(WatchDispatchError::Unavailable);
        }
        self.state = CallState::Remote(holder);
        let request = HolderRequestOperation::new(holder, self.message());
        Ok(Next::Call(IoCall::child(request)))
    }

    fn local_call(&mut self) -> Turn {
        let (user, local) = (self.user, self.local);
        let call = match &self.call {
            UserCall::Read(read) => IoCall::child(HolderReadOperation::new(user, read.clone())),
            UserCall::CreateWatch {
                path_prefix,
                event_mask,
                authorization,
                now_ms,
            } => {
                let watch = (path_prefix.clone(), *event_mask);
                let create =
                    CreateWatchOperation::new(local, user, watch, authorization.clone(), *now_ms)
                        .map_err(watch_error)?;
                IoCall::child(create)
            }
            UserCall::DeleteWatch { watch_id, now_ms } => {
                let replication = watch_delete_replication(local, user, *watch_id, *now_ms);
                IoCall::child(StoreWatchOperation::delete(
                    user,
                    *watch_id,
                    Some(replication),
                ))
            }
            UserCall::ListWatches => IoCall::child(WatchFilterOperation::owner(user)),
        };
        Ok(Next::Call(call))
    }

    /// The request frame of the call for a remote holder.
    fn message(&self) -> NotificationTransportMessage {
        let user = self.user;
        match self.call.clone() {
            UserCall::Read(HolderRead::List { cursor, limit }) => {
                NotificationTransportMessage::List {
                    recipient: user,
                    cursor,
                    limit: limit as u32,
                }
            }
            UserCall::Read(HolderRead::Unread) => {
                NotificationTransportMessage::UnreadCount { recipient: user }
            }
            UserCall::Read(HolderRead::Mark { ids, up_to_ms, .. }) => {
                NotificationTransportMessage::MarkRead {
                    recipient: user,
                    ids,
                    up_to_ms,
                }
            }
            UserCall::CreateWatch {
                path_prefix,
                event_mask,
                authorization,
                ..
            } => NotificationTransportMessage::CreateWatch {
                owner: user,
                path_prefix,
                event_mask,
                authorization,
            },
            UserCall::DeleteWatch { watch_id, .. } => NotificationTransportMessage::DeleteWatch {
                owner: user,
                watch_id,
            },
            UserCall::ListWatches => NotificationTransportMessage::ListWatches { owner: user },
        }
    }

    /// The answer of a local call, with the wake, publish or interest change it needs.
    fn local_answer(&mut self, result: IoResult) -> Turn {
        let internal = |error: &dyn ToString| WatchDispatchError::Internal(error.to_string());
        match &self.call {
            UserCall::Read(_) => {
                let answer = result.child::<HolderAnswer, HolderReadError>()?;
                let answer = answer.map_err(|error| internal(&error))?;
                if let HolderAnswer::Marked(marked) = answer
                    && marked > 0
                {
                    let wake = Effect::Net(NetEffect::WakeInbox(vec![self.user]));
                    return Ok(Next::Then {
                        output: UserAnswer::Read(answer),
                        calls: vec![IoCall::from(wake)],
                    });
                }
                Ok(Next::Done(UserAnswer::Read(answer)))
            }
            UserCall::CreateWatch { .. } => {
                let created = result.child::<WatchSubscription, WatchSubscriptionError>()?;
                let subscription = created.map_err(watch_error)?;
                Ok(self.created(subscription, self.local, true))
            }
            UserCall::DeleteWatch { watch_id, .. } => {
                let deleted = result.child::<bool, WatchSubscriptionError>()?;
                deleted.map_err(|error| internal(&error))?;
                Ok(self.deleted(*watch_id, true))
            }
            UserCall::ListWatches => {
                let listed = result.child::<AuthorizedWatchSubscriptions, WatchCheckError>()?;
                let listed = listed.map_err(|error| internal(&error))?;
                Ok(Next::Done(UserAnswer::Watches(listed.subscriptions)))
            }
        }
    }

    fn remote_answer(&mut self, holder: NodeId, result: IoResult) -> Turn {
        let answer = result.child::<NotificationTransportMessage, RemoteError>()?;
        let remote = WatchDispatchError::Remote;
        let read = |answer| Ok(Next::Done(UserAnswer::Read(answer)));
        match &self.call {
            UserCall::Read(HolderRead::List { .. }) => {
                let (records, next_cursor) = listed(answer).map_err(remote)?;
                read(HolderAnswer::Listed {
                    records,
                    next_cursor,
                })
            }
            UserCall::Read(HolderRead::Unread) => {
                let (count, capped) = unread_counted(answer).map_err(remote)?;
                read(HolderAnswer::Unread { count, capped })
            }
            UserCall::Read(HolderRead::Mark { .. }) => {
                read(HolderAnswer::Marked(marked_read(answer).map_err(remote)?))
            }
            UserCall::CreateWatch { .. } => {
                let subscription = watch_created(answer).map_err(remote_create_error)?;
                Ok(self.created(subscription, holder, false))
            }
            UserCall::DeleteWatch { watch_id, .. } => {
                watch_deleted(answer).map_err(remote)?;
                Ok(self.deleted(*watch_id, false))
            }
            UserCall::ListWatches => {
                let watches = watches_listed(answer).map_err(remote)?;
                Ok(Next::Done(UserAnswer::Watches(watches)))
            }
        }
    }

    /// A new watch on `holder`: noted in this node's interest, and published when held here.
    fn created(
        &self,
        subscription: WatchSubscription,
        holder: NodeId,
        publish: bool,
    ) -> Next<UserAnswer> {
        let entry = WatchInterestEntry {
            path_prefix: subscription.path_prefix.clone(),
            event_mask: subscription.event_mask,
        };
        let register = NetEffect::RegisterInterest {
            watch_id: subscription.watch_id,
            realm_id: self.user.realm_id,
            holder,
            entry,
        };
        let mut calls = Vec::new();
        if publish {
            calls.push(IoCall::child(InterestOperation::publish()));
        }
        calls.push(IoCall::from(Effect::Net(register)));
        Next::Then {
            output: UserAnswer::Created(subscription),
            calls,
        }
    }

    /// A deleted watch: removed from this node's interest, and published when held here.
    fn deleted(&self, watch_id: Ulid, publish: bool) -> Next<UserAnswer> {
        let mut calls = Vec::new();
        if publish {
            calls.push(IoCall::child(InterestOperation::publish()));
        }
        calls.push(IoCall::from(Effect::Net(NetEffect::RetractInterest(
            watch_id,
        ))));
        Next::Then {
            output: UserAnswer::Deleted,
            calls,
        }
    }
}

impl Operation for UserCallOperation {
    type Output = UserAnswer;
    type Error = WatchDispatchError;

    fn first(&mut self) -> Turn {
        let config = GetConfigOperation::new(self.user.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::take(&mut self.state) {
            CallState::Config => {
                self.resolved(result.child::<RealmConfigDocument, GetConfigError>()?)
            }
            CallState::Local => self.local_answer(result),
            CallState::Remote(holder) => self.remote_answer(holder, result),
        }
    }
}

#[cfg(test)]
#[path = "user_call_tests.rs"]
mod tests;
