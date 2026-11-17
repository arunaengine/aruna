//! Sends one signed webhook delivery or one notification mail through the outbound backend.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::outbound::{
    HttpPost, MailRequest, OutboundEffect, OutboundError, OutboundEvent, OutgoingMail, SmtpSettings,
};
use aruna_core::structs::execution::webhook::{
    DELIVERY_HEADER, SIGNATURE_HEADER, WebhookDeliverySpec, sign_delivery,
};
use thiserror::Error;
use ulid::Ulid;

use crate::notifications::unexpected_event;

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
    #[error(transparent)]
    Io(#[from] IoError),
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

/// One outbound call: a webhook POST answers with its status, a mail with `None`.
#[derive(Debug, PartialEq)]
pub struct SendOperation {
    /// The effect to send, or the refusal found while building it.
    ready: Option<Result<OutboundEffect, SendError>>,
}

impl SendOperation {
    /// A signed POST of the notification body; `now_s` is the signature time in seconds.
    pub fn webhook(spec: &WebhookDeliverySpec, url: &str, secret: &[u8], now_s: u64) -> Self {
        let Some(body) = spec.body() else {
            let refused =
                SendError::Refused("only share upload notifications are delivered".into());
            return Self::refused(refused);
        };
        let body = match serde_json::to_vec(&body) {
            Ok(body) => body,
            Err(error) => return Self::refused(SendError::Refused(error.to_string())),
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
            ready: Some(Ok(OutboundEffect::Post(Box::new(post)))),
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
            ready: Some(Ok(OutboundEffect::Mail(Box::new(request)))),
        }
    }

    fn refused(error: SendError) -> Self {
        Self {
            ready: Some(Err(error)),
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

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let effect = self.ready.take().ok_or(IoError::Unfinished)??;
        Ok(Next::Call(IoCall::from(Effect::Outbound(effect))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let sent = match result.event()? {
            Event::Outbound(OutboundEvent::Posted { result }) => {
                result.map_err(SendError::from).and_then(posted)
            }
            Event::Outbound(OutboundEvent::MailSent { result }) => {
                result.map(|()| None).map_err(SendError::from)
            }
            other => Err(unexpected_event("an outbound event", &other).into()),
        };
        sent.map(Next::Done)
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::notifications::user;
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

    fn posted(result: Result<u16, OutboundError>) -> IoResult {
        IoResult::Event(Box::new(Event::Outbound(OutboundEvent::Posted { result })))
    }

    fn webhook() -> SendOperation {
        SendOperation::webhook(&spec(), "https://hooks.example.org/a", &[9; 32], 7)
    }

    fn run(result: IoResult) -> Result<Next<Option<u16>>, SendError> {
        let mut send = webhook();
        send.first().unwrap();
        send.next(result)
    }

    #[test]
    fn webhook_post_signed() {
        let spec = spec();
        let mut send = SendOperation::webhook(&spec, "https://hooks.example.org/a", &[9; 32], 7);
        let Ok(Next::Call(IoCall::Effect(effect))) = send.first() else {
            panic!("one post");
        };
        let Effect::Outbound(OutboundEffect::Post(post)) = *effect else {
            panic!("one post");
        };
        let body = serde_json::to_vec(&spec.body().unwrap()).unwrap();
        assert_eq!(post.body.as_ref(), body.as_slice());
        assert_eq!(post.url, "https://hooks.example.org/a");
        assert_eq!(post.deadline, DELIVERY_DEADLINE);
        assert!(
            post.headers
                .contains(&(SIGNATURE_HEADER, sign_delivery(&[9; 32], 7, &body)))
        );
        assert_eq!(send.next(posted(Ok(204))), Ok(Next::Done(Some(204))));
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
        assert!(matches!(send.first(), Err(SendError::Refused(_))));
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
            send.first(),
            Ok(Next::Call(IoCall::from(Effect::Outbound(
                OutboundEffect::Mail(_)
            ))))
        ));
        let sent = send.next(IoResult::Event(Box::new(Event::Outbound(
            OutboundEvent::MailSent {
                result: Err(OutboundError::NoSender),
            },
        ))));
        assert_eq!(sent, Err(SendError::NoSender));
    }

    #[test]
    fn other_results_fail() {
        assert!(matches!(run(IoResult::Done), Err(SendError::Io(_))));
        let mut send = webhook();
        send.first().unwrap();
        assert!(send.first().is_err(), "the effect is sent once");
    }
}
