//! Gives the REST layer the holder of a user's inbox and the typed results of its user calls, so
//! it stays clear of the effect boundary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metrics::WatchMetricReason;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::notification_watch::WatchSubscription;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use thiserror::Error;
use tokio::sync::broadcast;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::driver::DriverContext;
use crate::notifications::holder::HolderAnswer;
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::user_call::{UserAnswer, WatchDispatchError};

/// Outcome of serving a user's inbox call through the resolved holder.
#[derive(Debug, Error, PartialEq)]
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

impl From<IoError> for NotificationDispatchError {
    fn from(error: IoError) -> Self {
        Self::Internal(error.to_string())
    }
}

/// An answer of another kind than the call asks for.
fn other_answer(answer: UserAnswer) -> WatchDispatchError {
    WatchDispatchError::Internal(format!("unexpected dispatch answer {answer:?}"))
}

fn other_read(answer: HolderAnswer) -> NotificationDispatchError {
    NotificationDispatchError::Internal(format!("unexpected holder answer {answer:?}"))
}

/// The results the callers of each user call expect.
impl UserAnswer {
    fn read(self) -> Result<HolderAnswer, NotificationDispatchError> {
        match self {
            UserAnswer::Read(answer) => Ok(answer),
            other => Err(other_answer(other).into()),
        }
    }

    pub fn listed(
        self,
    ) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), NotificationDispatchError> {
        match self.read()? {
            HolderAnswer::Listed {
                records,
                next_cursor,
            } => Ok((records, next_cursor)),
            other => Err(other_read(other)),
        }
    }

    pub fn unread(self) -> Result<(u32, bool), NotificationDispatchError> {
        match self.read()? {
            HolderAnswer::Unread { count, capped } => Ok((count, capped)),
            other => Err(other_read(other)),
        }
    }

    pub fn marked(self) -> Result<u32, NotificationDispatchError> {
        match self.read()? {
            HolderAnswer::Marked(marked) => Ok(marked),
            other => Err(other_read(other)),
        }
    }

    pub fn created(self) -> Result<WatchSubscription, WatchDispatchError> {
        match self {
            UserAnswer::Created(subscription) => Ok(subscription),
            other => Err(other_answer(other)),
        }
    }

    pub fn deleted(self) -> Result<(), WatchDispatchError> {
        match self {
            UserAnswer::Deleted => Ok(()),
            other => Err(other_answer(other)),
        }
    }

    pub fn watches(self) -> Result<Vec<WatchSubscription>, WatchDispatchError> {
        match self {
            UserAnswer::Watches(subscriptions) => Ok(subscriptions),
            other => Err(other_answer(other)),
        }
    }
}

/// Resolves the node currently holding `recipient`'s inbox, so the live-stream endpoint can pick
/// the wake-driven local or holder-polling remote arm.
#[derive(Debug, PartialEq)]
pub struct InboxHolderOperation {
    recipient: UserId,
}

impl InboxHolderOperation {
    pub fn new(recipient: UserId) -> Self {
        Self { recipient }
    }
}

impl Operation for InboxHolderOperation {
    type Output = NodeId;
    type Error = NotificationDispatchError;

    fn first(&mut self) -> Result<Next<NodeId>, NotificationDispatchError> {
        let config = GetConfigOperation::new(self.recipient.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeId>, NotificationDispatchError> {
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|error| match error {
                GetConfigError::DocumentNotFound => NotificationDispatchError::Unavailable,
                other => NotificationDispatchError::Internal(other.to_string()),
            })?;
        match resolve_inbox_holder(&self.recipient, &config) {
            Ok(Some(holder)) => Ok(Next::Done(holder)),
            Ok(None) => Err(NotificationDispatchError::Unavailable),
            Err(error) => Err(NotificationDispatchError::Internal(error.to_string())),
        }
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

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
