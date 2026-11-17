//! Sends one notification request to a remote holder node and maps its answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::{
    NotificationRecord, NotificationRequest, NotificationTransportMessage,
};
use aruna_core::structs::execution::notification_watch::WatchSubscription;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

/// Why a holder gave no answer; the text is the transport's.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct RemoteError(pub String);

#[derive(Debug, PartialEq)]
enum RequestState {
    Ready(Box<NotificationRequest>),
    Sent,
    Done(Result<NotificationTransportMessage, RemoteError>),
}

/// One request frame to the notification holder `node` and its answer frame.
#[derive(Debug, PartialEq)]
pub struct HolderRequestOperation {
    state: RequestState,
}

impl HolderRequestOperation {
    pub fn new(node: NodeId, message: NotificationTransportMessage) -> Self {
        Self {
            state: RequestState::Ready(Box::new(NotificationRequest { node, message })),
        }
    }
}

impl Operation for HolderRequestOperation {
    type Output = NotificationTransportMessage;
    type Error = RemoteError;

    fn start(&mut self) -> Effects {
        match std::mem::replace(&mut self.state, RequestState::Sent) {
            RequestState::Ready(request) => {
                smallvec![Effect::Net(NetEffect::Notification(request))]
            }
            other => {
                self.state = other;
                smallvec![]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let answer = match (&self.state, event) {
            (RequestState::Sent, Event::Net(NetEvent::Notification(answer))) => {
                answer.map(|answer| *answer).map_err(RemoteError)
            }
            (_, other) => Err(RemoteError(format!(
                "unexpected notification event: {other:?}"
            ))),
        };
        self.state = RequestState::Done(answer);
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RequestState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            RequestState::Done(answer) => answer,
            _ => Err(RemoteError(
                "the notification request did not finish".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

type Answer = Result<NotificationTransportMessage, RemoteError>;

/// Maps an answer: the expected kind through `take`, a rejection and a transport failure to
/// their reason, any other kind to an unexpected response.
fn expect<T>(
    answer: Answer,
    take: impl FnOnce(NotificationTransportMessage) -> Result<T, NotificationTransportMessage>,
) -> Result<T, String> {
    match answer.map_err(|error| error.0)? {
        NotificationTransportMessage::Reject(reason) => Err(reason),
        other => take(other).map_err(|_| "unexpected notification response".to_string()),
    }
}

pub fn delivered(answer: Answer) -> Result<u32, String> {
    expect(answer, |answer| match answer {
        NotificationTransportMessage::DeliverAck { written } => Ok(written),
        other => Err(other),
    })
}

pub fn listed(answer: Answer) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), String> {
    expect(answer, |answer| match answer {
        NotificationTransportMessage::ListResult {
            records,
            next_cursor,
        } => Ok((records, next_cursor)),
        other => Err(other),
    })
}

pub fn unread_counted(answer: Answer) -> Result<(u32, bool), String> {
    expect(answer, |answer| match answer {
        NotificationTransportMessage::UnreadCountResult { count, capped } => Ok((count, capped)),
        other => Err(other),
    })
}

pub fn marked_read(answer: Answer) -> Result<u32, String> {
    expect(answer, |answer| match answer {
        NotificationTransportMessage::MarkReadResult { marked } => Ok(marked),
        other => Err(other),
    })
}

pub(crate) fn watch_created(answer: Answer) -> Result<WatchSubscription, String> {
    expect(answer, |answer| match answer {
        NotificationTransportMessage::WatchCreated { subscription } => Ok(subscription),
        other => Err(other),
    })
}

pub(crate) fn watch_deleted(answer: Answer) -> Result<(), String> {
    expect(answer, |answer| match answer {
        NotificationTransportMessage::WatchDeleted => Ok(()),
        other => Err(other),
    })
}

pub(crate) fn watches_listed(answer: Answer) -> Result<Vec<WatchSubscription>, String> {
    expect(answer, |answer| match answer {
        NotificationTransportMessage::WatchList { subscriptions } => Ok(subscriptions),
        other => Err(other),
    })
}

pub(crate) fn events_delivered(answer: Answer) -> Result<u32, String> {
    expect(answer, |answer| match answer {
        NotificationTransportMessage::WatchEventsAck { written } => Ok(written),
        other => Err(other),
    })
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::user;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    fn list() -> NotificationTransportMessage {
        NotificationTransportMessage::UnreadCount {
            recipient: user(1, 2),
        }
    }

    #[test]
    fn request_trace() {
        let mut request = HolderRequestOperation::new(node_id(2), list());
        let sent = NotificationRequest {
            node: node_id(2),
            message: list(),
        };
        let answer = NotificationTransportMessage::UnreadCountResult {
            count: 3,
            capped: false,
        };
        check_trace(
            &mut request,
            &[Effect::Net(NetEffect::Notification(Box::new(sent)))],
            vec![(
                Event::Net(NetEvent::Notification(Ok(Box::new(answer.clone())))),
                vec![],
            )],
        );
        assert_eq!(unread_counted(request.finalize()), Ok((3, false)));
    }

    #[test]
    fn answers_mapped() {
        let rejected = Ok(NotificationTransportMessage::Reject("no".to_string()));
        assert_eq!(listed(rejected), Err("no".to_string()));
        let lost = Err(RemoteError("closed".to_string()));
        assert_eq!(delivered(lost), Err("closed".to_string()));
        let wrong = Ok(NotificationTransportMessage::WatchDeleted);
        assert_eq!(
            marked_read(wrong),
            Err("unexpected notification response".to_string())
        );
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut request = HolderRequestOperation::new(node_id(2), list());
            request.start();
            request
        };
        assert_eq!(
            rejects_events(make, sample_events, |_| false).len(),
            sample_events().len()
        );
    }

    #[test]
    fn abort_and_finalize() {
        let mut request = HolderRequestOperation::new(node_id(2), list());
        request.start();
        check_abort(&mut request, &[]);
        rejects_finalize(request);
    }
}
