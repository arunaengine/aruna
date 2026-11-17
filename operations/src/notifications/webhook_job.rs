//! Runs one webhook delivery job: decides the delivery, then posts the signed notice to the
//! subscribed URL.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobResultPayload;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::execution::webhook::{WebhookDeliverySpec, WebhookSubscription};
use aruna_core::structs::identity::realm::RealmConfigDocument;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::notifications::send::{SendError, SendOperation, job_error};
use crate::notifications::settings::{Delivery, DeliveryOperation, DeliveryOutcome, managers};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum WebhookState {
    #[default]
    Config,
    Delivery,
    Send,
}

/// Sends one signed delivery when this node's current setting has the webhook on. A 4xx answer
/// other than 408 and 429 is permanent; while no management node opens the secret, the job
/// waits without an attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct WebhookJobOperation {
    spec: WebhookDeliverySpec,
    local: NodeId,
    now_ms: u64,
    /// The runner's clock at the latest step; receivers check how fresh the signed time is.
    clock_ms: u64,
    state: WebhookState,
}

type Turn = Result<Next<DeliveryOutcome>, IoError>;

fn status(status: Option<u16>) -> Turn {
    let payload = JobResultPayload::Webhook { status };
    Ok(Next::Done(DeliveryOutcome::Succeeded(payload)))
}

impl WebhookJobOperation {
    pub fn new(spec: WebhookDeliverySpec, local: NodeId, now_ms: u64) -> Self {
        Self {
            spec,
            local,
            now_ms,
            clock_ms: now_ms,
            state: WebhookState::Config,
        }
    }

    fn decided(&mut self, delivery: Delivery) -> Turn {
        let winner = match delivery {
            Delivery::Skip => return status(None),
            Delivery::Wait(error, delay) => {
                return Ok(Next::Done(DeliveryOutcome::DeferredFor(error, delay)));
            }
            Delivery::Send(winner) => winner,
        };
        let subscription = WebhookSubscription::from_bytes(&winner.bytes).ok();
        let (Some(url), Some(secret)) = (subscription.and_then(|found| found.url), winner.secret)
        else {
            return status(None);
        };
        self.state = WebhookState::Send;
        Ok(Next::Call(IoCall::child(self.post(&url, secret.expose()))))
    }

    /// The signed post, timed at the step that sends it.
    fn post(&self, url: &str, secret: &[u8]) -> SendOperation {
        SendOperation::webhook(&self.spec, url, secret, self.clock_ms / 1000)
    }
}

impl Operation for WebhookJobOperation {
    type Output = DeliveryOutcome;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.clock_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        let realm_id = self.spec.notification.recipient.realm_id;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            WebhookState::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let record = self.spec.notification.clone();
                let realm_id = record.recipient.realm_id;
                let delivery = DeliveryOperation::new(
                    record,
                    SettingChannel::Webhook,
                    config.ok().map(|config| managers(&config)),
                    (self.local, realm_id),
                    self.now_ms,
                );
                self.state = WebhookState::Delivery;
                Ok(Next::Call(IoCall::child(delivery)))
            }
            WebhookState::Delivery => {
                let delivery = result.child::<Delivery, IoError>()??;
                self.decided(delivery)
            }
            WebhookState::Send => match result.child::<Option<u16>, SendError>()? {
                Ok(sent) => status(sent),
                Err(error) => Ok(Next::Done(DeliveryOutcome::Failed(job_error(error)))),
            },
        }
    }
}

#[cfg(test)]
#[path = "webhook_job_tests.rs"]
mod tests;
