//! Sends one signed webhook delivery or one notification mail through the outbound backend.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::outbound::{
    HttpPost, MailRequest, OutboundEffect, OutboundError, OutboundEvent, OutgoingMail, SmtpSettings,
};
use aruna_core::structs::execution::webhook::{
    DELIVERY_HEADER, SIGNATURE_HEADER, WebhookDeliverySpec, sign_delivery,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// Total time one delivery attempt may take; the job runtime retries a timed out attempt.
const DELIVERY_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SendError {
    /// Sending again cannot succeed.
    #[error("{0}")]
    Refused(String),
    /// Sending again later may succeed.
    #[error("{0}")]
    Failed(String),
    /// No mail sender is cached for the profile; send again with its settings.
    #[error("no mail sender is cached for this profile")]
    NoSender,
    #[error("unexpected event while sending: {0}")]
    UnexpectedEvent(String),
    #[error("the send did not finish")]
    NotFinished,
}

impl From<OutboundError> for SendError {
    fn from(error: OutboundError) -> Self {
        match error {
            OutboundError::Permanent(message) => SendError::Refused(message),
            OutboundError::Retryable(message) => SendError::Failed(message),
            OutboundError::NoSender => SendError::NoSender,
        }
    }
}

#[derive(Debug, PartialEq)]
enum SendState {
    Ready(OutboundEffect),
    Sending,
    Done(Result<Option<u16>, SendError>),
}

/// One outbound call: a webhook POST answers with its status, a mail with `None`.
#[derive(Debug, PartialEq)]
pub struct SendOperation {
    state: SendState,
}

impl SendOperation {
    /// A signed POST of the notification body; `now_s` is the signature time in seconds.
    pub fn webhook(spec: &WebhookDeliverySpec, url: &str, secret: &[u8], now_s: u64) -> Self {
        let Some(body) = spec.body() else {
            let refused =
                SendError::Refused("only share upload notifications are delivered".into());
            return Self::done(Err(refused));
        };
        let body = match serde_json::to_vec(&body) {
            Ok(body) => body,
            Err(error) => return Self::done(Err(SendError::Refused(error.to_string()))),
        };
        let post = HttpPost {
            url: url.to_string(),
            headers: vec![
                ("content-type", "application/json".to_string()),
                (
                    DELIVERY_HEADER,
                    spec.notification.notification_id.to_string(),
                ),
                (SIGNATURE_HEADER, sign_delivery(secret, now_s, &body)),
            ],
            body: body.into(),
            deadline: DELIVERY_DEADLINE,
        };
        Self {
            state: SendState::Ready(OutboundEffect::Post(Box::new(post))),
        }
    }

    /// One mail with the cached sender of `revision`, or one built from `settings`.
    pub fn mail(revision: Ulid, settings: Option<SmtpSettings>, mail: OutgoingMail) -> Self {
        let request = MailRequest {
            revision,
            settings,
            mail,
        };
        Self {
            state: SendState::Ready(OutboundEffect::Mail(Box::new(request))),
        }
    }

    fn done(result: Result<Option<u16>, SendError>) -> Self {
        Self {
            state: SendState::Done(result),
        }
    }
}

/// A webhook receiver's status: a 4xx other than 408 and 429 is permanent.
fn posted(status: u16) -> Result<Option<u16>, SendError> {
    if (200..300).contains(&status) {
        return Ok(Some(status));
    }
    let message = format!("the webhook receiver answered {status}");
    match (400..500).contains(&status) && !matches!(status, 408 | 429) {
        true => Err(SendError::Refused(message)),
        false => Err(SendError::Failed(message)),
    }
}

impl Operation for SendOperation {
    type Output = Option<u16>;
    type Error = SendError;

    fn start(&mut self) -> Effects {
        match std::mem::replace(&mut self.state, SendState::Sending) {
            SendState::Ready(effect) => smallvec![Effect::Outbound(effect)],
            other => {
                self.state = other;
                smallvec![]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match (&self.state, event) {
            (SendState::Sending, Event::Outbound(OutboundEvent::Posted { result })) => {
                result.map_err(SendError::from).and_then(posted)
            }
            (SendState::Sending, Event::Outbound(OutboundEvent::MailSent { result })) => {
                result.map(|()| None).map_err(SendError::from)
            }
            (_, other) => Err(SendError::UnexpectedEvent(format!("{other:?}"))),
        };
        self.state = SendState::Done(result);
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, SendState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            SendState::Done(result) => result,
            _ => Err(SendError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::notifications::user;
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::execution::notification::{
        NotificationClass, NotificationKind, NotificationRecord,
    };

    fn spec() -> WebhookDeliverySpec {
        let kind = NotificationKind::ShareUpload {
            share_id: Ulid::from_parts(4, 4),
            node_id: crate::tests::locations::node_id(3),
            group_id: Ulid::from_parts(5, 5),
            bucket: "inbox".to_string(),
            key: "a.csv".to_string(),
            size_bytes: 9,
        };
        let notification = NotificationRecord::new(user(1, 2), NotificationClass::Direct, kind, 42);
        WebhookDeliverySpec {
            subscription_id: Ulid::nil(),
            notification,
        }
    }

    fn posted(result: Result<u16, OutboundError>) -> Event {
        Event::Outbound(OutboundEvent::Posted { result })
    }

    fn run(event: Event) -> Result<Option<u16>, SendError> {
        let mut send = SendOperation::webhook(&spec(), "https://hooks.example.org/a", &[9; 32], 7);
        send.start();
        send.step(event);
        send.finalize()
    }

    #[test]
    fn webhook_post_signed() {
        let mut send = SendOperation::webhook(&spec(), "https://hooks.example.org/a", &[9; 32], 7);
        let effects = send.start();
        let [Effect::Outbound(OutboundEffect::Post(post))] = effects.as_slice() else {
            panic!("one post: {effects:?}");
        };
        let body = serde_json::to_vec(&spec().body().unwrap()).unwrap();
        assert_eq!(post.body.as_ref(), body.as_slice());
        assert_eq!(post.url, "https://hooks.example.org/a");
        assert_eq!(post.deadline, DELIVERY_DEADLINE);
        assert!(
            post.headers
                .contains(&(SIGNATURE_HEADER, sign_delivery(&[9; 32], 7, &body)))
        );
        assert!(send.step(posted(Ok(204))).is_empty());
        assert_eq!(send.finalize(), Ok(Some(204)));
    }

    #[test]
    fn webhook_status_kinds() {
        assert!(matches!(run(posted(Ok(500))), Err(SendError::Failed(_))));
        assert!(matches!(run(posted(Ok(429))), Err(SendError::Failed(_))));
        assert!(matches!(run(posted(Ok(408))), Err(SendError::Failed(_))));
        assert!(matches!(run(posted(Ok(400))), Err(SendError::Refused(_))));
        let refused = posted(Err(OutboundError::Permanent("egress screen".into())));
        assert!(matches!(run(refused), Err(SendError::Refused(_))));
        let timeout = posted(Err(OutboundError::Retryable("late".into())));
        assert!(matches!(run(timeout), Err(SendError::Failed(_))));
    }

    #[test]
    fn other_notice_refused() {
        let mut other = spec();
        other.notification.kind = NotificationKind::AddedToGroup {
            group_id: Ulid::from_parts(5, 5),
            actor_user_id: user(1, 3),
        };
        let mut send = SendOperation::webhook(&other, "https://hooks.example.org/a", &[9; 32], 7);
        assert!(send.start().is_empty());
        assert!(matches!(send.finalize(), Err(SendError::Refused(_))));
    }

    #[test]
    fn mail_needs_sender() {
        let mail = OutgoingMail {
            to: "ada@example.org".to_string(),
            subject: String::new(),
            body: String::new(),
            id: Ulid::from_parts(1, 1),
        };
        let mut send = SendOperation::mail(Ulid::from_parts(2, 2), None, mail);
        assert!(matches!(
            send.start().as_slice(),
            [Effect::Outbound(OutboundEffect::Mail(_))]
        ));
        send.step(Event::Outbound(OutboundEvent::MailSent {
            result: Err(OutboundError::NoSender),
        }));
        assert_eq!(send.finalize(), Err(SendError::NoSender));
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut send =
                SendOperation::webhook(&spec(), "https://hooks.example.org/a", &[9; 32], 7);
            send.start();
            send
        };
        let refusals = rejects_events(make, sample_events, |_| false);
        assert_eq!(refusals.len(), sample_events().len());
    }

    #[test]
    fn abort_and_finalize() {
        let mut send = SendOperation::webhook(&spec(), "https://hooks.example.org/a", &[9; 32], 7);
        send.start();
        check_abort(&mut send, &[]);
        rejects_finalize(send);
    }
}
