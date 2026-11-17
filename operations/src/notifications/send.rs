//! Sends one signed webhook delivery or one notification mail through the outbound backend.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::outbound::{
    HttpPost, MailRequest, OutboundEffect, OutboundError, OutboundEvent, OutgoingMail,
    SmtpSettings,
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
            let refused = SendError::Refused("only share upload notifications are delivered".into());
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
                (DELIVERY_HEADER, spec.notification.notification_id.to_string()),
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
