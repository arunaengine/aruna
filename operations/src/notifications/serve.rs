//! Answers a peer's inbox and watch requests for a user whose inbox this node holds (list,
//! unread count, mark read, list and create watches) and expands its watch events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEvent, WatchEventMask, WatchSubscription,
};
use aruna_core::structs::identity::realm::RealmId;
use tracing::warn;

use crate::notifications::checks::{
    events_realm, peer_config, validate_watch_events, verify_recipient_holder,
};
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
use crate::notifications::watch::expansion::{ExpandError, ExpandOutcome, WatchExpandOperation};
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

#[derive(Clone, Debug, Default, PartialEq)]
enum EventsState {
    #[default]
    Config,
    Expand,
    /// Asks for an interest publish after dropped watches, then wakes the recipients.
    Mark {
        recipients: Vec<UserId>,
        written: u32,
    },
    Wake {
        written: u32,
    },
}

/// Expands a peer's watch events into the inboxes this node holds, once more after a conflict.
/// Dropped watches ask for a new interest publish; recipients of new records are woken.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchEventsOperation {
    input: DeliverInput,
    events: Vec<WatchEvent>,
    state: EventsState,
}

impl WatchEventsOperation {
    pub fn new(input: DeliverInput, events: Vec<WatchEvent>) -> Self {
        Self {
            input,
            events,
            state: EventsState::Config,
        }
    }

    fn realm(&self) -> Option<RealmId> {
        self.events.first().map(|event| event.realm_id)
    }

    fn checked(&mut self, value: Option<&[u8]>) -> AnswerTurn {
        let input = self.input;
        let realm_id = match events_realm(&self.events) {
            Ok(realm_id) => realm_id,
            Err(reason) => return reject(reason),
        };
        let config = match peer_config(input.peer, realm_id, value) {
            Ok(config) => config,
            Err(reason) => return reject(reason),
        };
        if let Err(reason) = validate_watch_events(&self.events, realm_id, input.now_ms) {
            return reject(reason);
        }
        let events = self.events.clone();
        let build = move || {
            let (config, events) = (config.clone(), events.clone());
            WatchExpandOperation::new(realm_id, config, input.local, events, None)
        };
        let expand = boxed_retry(build, realm_id.as_bytes(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        self.state = EventsState::Expand;
        Ok(Next::Call(IoCall::Child(expand)))
    }

    fn expanded(&mut self, outcome: Result<ExpandOutcome, ExpandError>) -> AnswerTurn {
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => return reject(error.to_string()),
        };
        let recipients = outcome.inbox.recipients;
        let written = outcome.inbox.written as u32;
        let Some(realm_id) = self.realm().filter(|_| outcome.dropped) else {
            return self.wake(recipients, written);
        };
        self.state = EventsState::Mark {
            recipients,
            written,
        };
        Ok(Next::Call(IoCall::child(InterestOperation::mark(realm_id))))
    }

    fn wake(&mut self, recipients: Vec<UserId>, written: u32) -> AnswerTurn {
        if recipients.is_empty() {
            return Ok(Next::Done(NotificationTransportMessage::WatchEventsAck {
                written,
            }));
        }
        self.state = EventsState::Wake { written };
        let wake = Effect::Net(NetEffect::WakeInbox(recipients));
        Ok(Next::Call(IoCall::from(wake)))
    }
}

impl Operation for WatchEventsOperation {
    type Output = NotificationTransportMessage;
    type Error = DeliverError;

    fn first(&mut self) -> AnswerTurn {
        let realm_id = match events_realm(&self.events) {
            Ok(realm_id) => realm_id,
            Err(reason) => return reject(reason),
        };
        if realm_id != self.input.local_realm {
            let peer = self.input.peer;
            return reject(format!(
                "notification peer `{peer}` addressed foreign realm `{realm_id}`"
            ));
        }
        Ok(Next::Call(read_row(
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
        )))
    }

    fn next(&mut self, result: IoResult) -> AnswerTurn {
        match std::mem::take(&mut self.state) {
            EventsState::Config => {
                let [value] = values::<1>(result)?;
                self.checked(value.as_deref())
            }
            EventsState::Expand => self.expanded(result.child::<ExpandOutcome, ExpandError>()?),
            EventsState::Mark {
                recipients,
                written,
            } => {
                if let Err(error) = result.child::<bool, InterestError>()? {
                    warn!(%error, "Failed to retract dropped watch interest after delivery");
                }
                self.wake(recipients, written)
            }
            EventsState::Wake { written } => match result.event()? {
                Event::Net(NetEvent::InboxWoken) => {
                    Ok(Next::Done(NotificationTransportMessage::WatchEventsAck {
                        written,
                    }))
                }
                other => Err(unexpected_event("NetEvent::InboxWoken", &other).into()),
            },
        }
    }
}

#[cfg(test)]
#[path = "serve_tests.rs"]
mod tests;
