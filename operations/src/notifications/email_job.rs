//! Runs one email delivery job: decides the delivery, then mails the notice through the realm's
//! SMTP server, building the sender from the profile when none is cached.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::operation::Operation;
use aruna_core::outbound::{OutgoingMail, SmtpSettings};
use aruna_core::structs::execution::email::{EmailDeliverySpec, EmailPreference};
use aruna_core::structs::execution::job::{JobError, JobResultPayload};
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::realm::RealmConfigDocument;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::notifications::send::{SendError, SendOperation, job_error};
use crate::notifications::settings::{
    Delivery, DeliveryOperation, DeliveryOutcome, SettingWinner, managers,
};
use crate::realm_vault::{RealmSecretError, ReadSecretOperation};

#[derive(Clone, Debug, Default, PartialEq)]
enum EmailState {
    #[default]
    Config,
    Delivery,
    /// Sends with the cached sender of the profile revision.
    Cached,
    /// Opens the SMTP password for a sender built from the profile.
    Password,
    /// Sends with a sender built from the profile.
    Built,
}

/// Sends one notification email when this node's current setting has email on for its category.
/// A permanent SMTP refusal ends the job; while no management node opens the address, the job
/// waits without an attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct EmailJobOperation {
    spec: EmailDeliverySpec,
    local: NodeId,
    now_ms: u64,
    state: EmailState,
    config: Option<RealmConfigDocument>,
    mail: Option<OutgoingMail>,
}

type Turn = Result<Next<DeliveryOutcome>, IoError>;

fn unsent() -> Turn {
    let payload = JobResultPayload::Email { sent: false };
    Ok(Next::Done(DeliveryOutcome::Succeeded(payload)))
}

fn failed(error: JobError) -> Turn {
    Ok(Next::Done(DeliveryOutcome::Failed(error)))
}

impl EmailJobOperation {
    pub fn new(spec: EmailDeliverySpec, local: NodeId, now_ms: u64) -> Self {
        Self {
            spec,
            local,
            now_ms,
            state: EmailState::Config,
            config: None,
            mail: None,
        }
    }

    fn configured(&mut self, config: Result<RealmConfigDocument, GetConfigError>) -> Turn {
        let config = match config {
            Ok(config) if config.email.is_none() => {
                return failed(JobError::permanent("email is not configured in this realm"));
            }
            Ok(config) => Some(config),
            Err(_) => None,
        };
        let record = self.spec.notification.clone();
        let realm_id = record.recipient.realm_id;
        let managers = config.as_ref().map(managers);
        let delivery = DeliveryOperation::new(
            record,
            SettingChannel::Email,
            managers,
            (self.local, realm_id),
            self.now_ms,
        );
        self.config = config;
        self.state = EmailState::Delivery;
        Ok(Next::Call(IoCall::child(delivery)))
    }

    fn decided(&mut self, delivery: Delivery) -> Turn {
        let winner = match delivery {
            Delivery::Skip => return unsent(),
            Delivery::Wait(error, delay) => {
                return Ok(Next::Done(DeliveryOutcome::DeferredFor(error, delay)));
            }
            Delivery::Send(winner) => winner,
        };
        let (Some(config), Ok(preference)) = (
            self.config.as_ref(),
            EmailPreference::from_bytes(&winner.bytes),
        ) else {
            return unsent();
        };
        if !preference.wants(&self.spec.notification) {
            return unsent();
        }
        let Some(profile) = config.email.as_ref() else {
            return failed(JobError::permanent("email is not configured in this realm"));
        };
        let mail = match self.mail_to(winner) {
            Ok(mail) => mail,
            Err(error) => return failed(error),
        };
        let send = SendOperation::mail(profile.revision, None, mail.clone());
        self.mail = Some(mail);
        self.state = EmailState::Cached;
        Ok(Next::Call(IoCall::child(send)))
    }

    fn mail_to(&self, winner: SettingWinner) -> Result<OutgoingMail, JobError> {
        let secret = winner
            .secret
            .ok_or_else(|| JobError::permanent("the opt-in address is missing"))?;
        let address = String::from_utf8(secret.expose().to_vec())
            .map_err(|_| JobError::permanent("the opt-in address is not UTF-8 text"))?;
        let (subject, body) = self.spec.text();
        Ok(OutgoingMail {
            to: address,
            subject,
            body,
            id: self.spec.notification.notification_id,
        })
    }

    /// Opens the profile password, or builds the sender at once when the profile has none.
    fn build_sender(&mut self) -> Turn {
        let Some(config) = self.config.as_ref() else {
            return failed(JobError::permanent("email is not configured in this realm"));
        };
        match config.email.as_ref().and_then(|profile| profile.password) {
            Some(reference) => {
                self.state = EmailState::Password;
                let read = ReadSecretOperation::new(config, reference);
                Ok(Next::Call(IoCall::child(read)))
            }
            None => self.send_built(None),
        }
    }

    fn send_built(&mut self, password: Option<String>) -> Turn {
        let (Some(config), Some(mail)) = (self.config.as_ref(), self.mail.take()) else {
            return Err(IoError::Unfinished);
        };
        let Some(profile) = config.email.as_ref() else {
            return failed(JobError::permanent("email is not configured in this realm"));
        };
        let settings = SmtpSettings {
            host: profile.host.clone(),
            port: profile.port,
            tls: profile.tls,
            username: profile.username.clone(),
            password,
            from: profile.from.clone(),
        };
        self.state = EmailState::Built;
        let send = SendOperation::mail(profile.revision, Some(settings), mail);
        Ok(Next::Call(IoCall::child(send)))
    }
}

/// The outcome of a send.
fn sent(result: Result<Option<u16>, SendError>) -> Turn {
    match result {
        Ok(_) => {
            let payload = JobResultPayload::Email { sent: true };
            Ok(Next::Done(DeliveryOutcome::Succeeded(payload)))
        }
        Err(error) => failed(job_error(error)),
    }
}

impl Operation for EmailJobOperation {
    type Output = DeliveryOutcome;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        if self.now_ms >= self.spec.notification.expires_at_ms() {
            return unsent();
        }
        let realm_id = self.spec.notification.recipient.realm_id;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            EmailState::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.configured(config)
            }
            EmailState::Delivery => {
                let delivery = result.child::<Delivery, IoError>()??;
                self.decided(delivery)
            }
            EmailState::Cached => match result.child::<Option<u16>, SendError>()? {
                Err(SendError::NoSender) => self.build_sender(),
                result => sent(result),
            },
            EmailState::Password => {
                let secret = result.child::<SecretBytes, RealmSecretError>()?;
                let secret = match secret {
                    Ok(secret) => secret,
                    Err(error) => return failed(JobError::retryable(error.to_string())),
                };
                match String::from_utf8(secret.expose().to_vec()) {
                    Ok(password) => self.send_built(Some(password)),
                    Err(_) => failed(JobError::permanent("the SMTP password is not UTF-8 text")),
                }
            }
            EmailState::Built => sent(result.child::<Option<u16>, SendError>()?),
        }
    }
}

#[cfg(test)]
#[path = "email_job_tests.rs"]
mod tests;
