//! Sends one notification request to a remote holder node and maps its answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::{
    NotificationRecord, NotificationRequest, NotificationTransportMessage,
};
use aruna_core::structs::execution::notification_watch::WatchSubscription;
use thiserror::Error;

use crate::notifications::unexpected;

/// Why a holder gave no answer; the text is the transport's.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct RemoteError(pub String);

impl From<IoError> for RemoteError {
    fn from(error: IoError) -> Self {
        RemoteError(error.to_string())
    }
}

/// One request frame to the notification holder `node` and its answer frame.
#[derive(Debug, PartialEq)]
pub struct HolderRequestOperation {
    request: Option<Box<NotificationRequest>>,
}

impl HolderRequestOperation {
    pub fn new(node: NodeId, message: NotificationTransportMessage) -> Self {
        Self {
            request: Some(Box::new(NotificationRequest { node, message })),
        }
    }
}

impl Operation for HolderRequestOperation {
    type Output = NotificationTransportMessage;
    type Error = RemoteError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let request = self.request.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::Effect(Effect::Net(
            NetEffect::Notification(request),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        match result {
            IoResult::Event(Event::Net(NetEvent::Notification(answer))) => answer
                .map(|answer| Next::Done(*answer))
                .map_err(RemoteError),
            other => Err(unexpected("NetEvent::Notification", &other).into()),
        }
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
mod call_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::user;

    fn list() -> NotificationTransportMessage {
        NotificationTransportMessage::UnreadCount {
            recipient: user(1, 2),
        }
    }

    #[test]
    fn request_calls() {
        let mut request = HolderRequestOperation::new(node_id(2), list());
        let sent = NotificationRequest {
            node: node_id(2),
            message: list(),
        };
        assert_eq!(
            request.first(),
            Ok(Next::Call(IoCall::Effect(Effect::Net(
                NetEffect::Notification(Box::new(sent))
            ))))
        );
        let answer = NotificationTransportMessage::UnreadCountResult {
            count: 3,
            capped: false,
        };
        let event = Event::Net(NetEvent::Notification(Ok(Box::new(answer.clone()))));
        assert_eq!(request.next(IoResult::Event(event)), Ok(Next::Done(answer)));
        assert!(request.first().is_err(), "the request is sent once");
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
        let counted = Ok(NotificationTransportMessage::UnreadCountResult {
            count: 3,
            capped: false,
        });
        assert_eq!(unread_counted(counted), Ok((3, false)));
    }

    #[test]
    fn other_results_fail() {
        let mut request = HolderRequestOperation::new(node_id(2), list());
        request.first().unwrap();
        let failed = Event::Net(NetEvent::Notification(Err("closed".to_string())));
        assert_eq!(
            request.next(IoResult::Event(failed)),
            Err(RemoteError("closed".to_string()))
        );
        assert!(request.next(IoResult::Done).is_err());
    }
}
