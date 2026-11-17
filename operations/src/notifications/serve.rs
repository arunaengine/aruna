//! Answers a peer's inbox and watch requests for a user whose inbox this node holds: list,
//! unread count, mark read, list watches and create a watch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, values};
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEventMask, WatchSubscription,
};

use crate::notifications::checks::{peer_config, verify_recipient_holder};
use crate::notifications::deliver::{AnswerTurn, DeliverError, DeliverInput, reject};
use crate::notifications::holder::{
    HolderAnswer, HolderRead, HolderReadError, HolderReadOperation,
};
use crate::notifications::mark_read::MARK_MAX_IDS;
use crate::notifications::unexpected_event;
use crate::notifications::watch::check::{
    AuthorizedWatchSubscriptions, WatchCheckError, WatchCheckOperation, WatchFilterOperation,
};
use crate::notifications::watch::digest::{InterestError, InterestOperation};
use crate::notifications::watch::paths::WatchAuthorization;
use crate::notifications::watch::records::{
    WATCH_SUBSCRIPTION_UNAUTHORIZED, WATCH_SUBSCRIPTION_UNAVAILABLE, WatchSubscriptionError,
};
use crate::notifications::watch::store::CreateWatchOperation;

/// What a peer asks of the inbox or the watches of one user.
#[derive(Clone, Debug, PartialEq)]
pub enum InboxRequest {
    Read(HolderRead),
    ListWatches,
    CreateWatch {
        path_prefix: String,
        event_mask: WatchEventMask,
        authorization: WatchAuthorizationBinding,
    },
}

#[derive(Clone, Debug, Default, PartialEq)]
enum ServeState {
    #[default]
    Config,
    Read,
    Watches,
    Forwarded,
    Create,
    /// Waits for the publish request or the inbox wake, then answers.
    Answer(NotificationTransportMessage),
}

/// Answers one inbox or watch request of a sync-eligible peer for a user whose inbox this node
/// holds. A refusal or a failure is a rejection; the output is the answer frame.
#[derive(Clone, Debug, PartialEq)]
pub struct ServeInboxOperation {
    input: DeliverInput,
    user: UserId,
    request: InboxRequest,
    state: ServeState,
}

impl ServeInboxOperation {
    pub fn new(input: DeliverInput, user: UserId, request: InboxRequest) -> Self {
        Self {
            input,
            user,
            request,
            state: ServeState::Config,
        }
    }

    /// Checks the peer and the holder, then runs the request.
    fn checked(&mut self, value: Option<&[u8]>) -> AnswerTurn {
        let input = self.input;
        let checked = peer_config(input.peer, self.user.realm_id, value)
            .and_then(|config| verify_recipient_holder(&self.user, &config, input.local));
        if let Err(reason) = checked {
            return reject(reason);
        }
        match &self.request {
            InboxRequest::Read(read) => {
                self.state = ServeState::Read;
                let read = HolderReadOperation::new(self.user, read.clone());
                Ok(Next::Call(IoCall::child(read)))
            }
            InboxRequest::ListWatches => {
                self.state = ServeState::Watches;
                Ok(Next::Call(IoCall::child(WatchFilterOperation::owner(
                    self.user,
                ))))
            }
            InboxRequest::CreateWatch {
                path_prefix,
                event_mask,
                authorization,
            } => {
                self.state = ServeState::Forwarded;
                let check = WatchCheckOperation::forwarded(
                    self.user.realm_id,
                    self.user,
                    path_prefix,
                    *event_mask,
                    authorization,
                );
                Ok(Next::Call(IoCall::child(check)))
            }
        }
    }

    fn read(&mut self, answer: Result<HolderAnswer, HolderReadError>) -> AnswerTurn {
        let answer = match answer {
            Ok(answer) => answer,
            Err(error) => return reject(error.to_string()),
        };
        let message = match answer {
            HolderAnswer::Listed {
                records,
                next_cursor,
            } => NotificationTransportMessage::ListResult {
                records,
                next_cursor,
            },
            HolderAnswer::Unread { count, capped } => {
                NotificationTransportMessage::UnreadCountResult { count, capped }
            }
            HolderAnswer::Marked(marked) if marked > 0 => {
                // The user's live streams refetch the unread count.
                let answer = NotificationTransportMessage::MarkReadResult { marked };
                self.state = ServeState::Answer(answer);
                let wake = Effect::Net(NetEffect::WakeInbox(vec![self.user]));
                return Ok(Next::Call(IoCall::from(wake)));
            }
            HolderAnswer::Marked(marked) => NotificationTransportMessage::MarkReadResult { marked },
        };
        Ok(Next::Done(message))
    }

    /// Creates the watch the forwarded check allowed.
    fn forwarded(&mut self, check: Result<WatchAuthorization, WatchCheckError>) -> AnswerTurn {
        match check {
            Ok(WatchAuthorization::Authorized) => {}
            Ok(WatchAuthorization::Denied(reason)) => {
                let reason = reason.metric_reason();
                return reject(format!(
                    "{WATCH_SUBSCRIPTION_UNAUTHORIZED}: {}",
                    reason.as_str()
                ));
            }
            Ok(WatchAuthorization::Unavailable(_)) => {
                return reject(WATCH_SUBSCRIPTION_UNAVAILABLE.to_string());
            }
            Err(error) => return reject(error.to_string()),
        }
        let InboxRequest::CreateWatch {
            path_prefix,
            event_mask,
            authorization,
        } = self.request.clone()
        else {
            return reject("the request is not a watch creation".to_string());
        };
        let input = self.input;
        let watch = (path_prefix, event_mask);
        match CreateWatchOperation::new(input.local, self.user, watch, authorization, input.now_ms)
        {
            Ok(create) => {
                self.state = ServeState::Create;
                Ok(Next::Call(IoCall::child(create)))
            }
            Err(error) => reject(error.to_string()),
        }
    }

    fn created(
        &mut self,
        created: Result<WatchSubscription, WatchSubscriptionError>,
    ) -> AnswerTurn {
        let subscription = match created {
            Ok(subscription) => subscription,
            Err(error) => return reject(error.to_string()),
        };
        self.state =
            ServeState::Answer(NotificationTransportMessage::WatchCreated { subscription });
        Ok(Next::Call(IoCall::child(InterestOperation::publish())))
    }
}

impl Operation for ServeInboxOperation {
    type Output = NotificationTransportMessage;
    type Error = DeliverError;

    fn first(&mut self) -> AnswerTurn {
        let realm_id = self.user.realm_id;
        if realm_id != self.input.local_realm {
            let peer = self.input.peer;
            return reject(format!(
                "notification peer `{peer}` addressed foreign realm `{realm_id}`"
            ));
        }
        if let InboxRequest::Read(HolderRead::Mark { ids, .. }) = &self.request
            && ids.len() > MARK_MAX_IDS
        {
            let count = ids.len();
            return reject(format!(
                "mark read id count {count} exceeds cap {MARK_MAX_IDS}"
            ));
        }
        Ok(Next::Call(read_row(
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
        )))
    }

    fn next(&mut self, result: IoResult) -> AnswerTurn {
        match std::mem::take(&mut self.state) {
            ServeState::Config => {
                let [value] = values::<1>(result)?;
                self.checked(value.as_deref())
            }
            ServeState::Read => self.read(result.child::<HolderAnswer, HolderReadError>()?),
            ServeState::Watches => {
                match result.child::<AuthorizedWatchSubscriptions, WatchCheckError>()? {
                    Ok(listed) => Ok(Next::Done(NotificationTransportMessage::WatchList {
                        subscriptions: listed.subscriptions,
                    })),
                    Err(error) => reject(error.to_string()),
                }
            }
            ServeState::Forwarded => {
                self.forwarded(result.child::<WatchAuthorization, WatchCheckError>()?)
            }
            ServeState::Create => {
                self.created(result.child::<WatchSubscription, WatchSubscriptionError>()?)
            }
            // The watch is created and the inbox marked whatever the publish or wake answers.
            ServeState::Answer(answer) => match result {
                IoResult::Child(_) => {
                    let _ = result.child::<bool, InterestError>()?;
                    Ok(Next::Done(answer))
                }
                result => match result.event()? {
                    Event::Net(NetEvent::InboxWoken) => Ok(Next::Done(answer)),
                    other => Err(unexpected_event("NetEvent::InboxWoken", &other).into()),
                },
            },
        }
    }
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;
