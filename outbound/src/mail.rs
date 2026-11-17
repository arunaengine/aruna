//! Sends notification mail through the SMTP server of a realm email profile.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aruna_core::outbound::{OutboundError, OutgoingMail, SmtpSettings};
use aruna_core::structs::execution::email::SmtpTls;
use lettre::message::Mailbox;
use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use ulid::Ulid;

/// Time one SMTP exchange may take; the job runtime retries a timed out attempt.
const SMTP_TIMEOUT: Duration = Duration::from_secs(30);

#[async_trait::async_trait]
pub trait MailSender: Send + Sync {
    async fn send(&self, mail: OutgoingMail) -> Result<(), OutboundError>;
}

pub struct SmtpMailer {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
}

impl SmtpMailer {
    pub fn new(settings: &SmtpSettings) -> Result<Self, String> {
        let from: Mailbox = settings
            .from
            .parse()
            .map_err(|_| "the SMTP sender address is invalid".to_string())?;
        let host = settings.host.as_str();
        let mut builder = match settings.tls {
            SmtpTls::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host),
            SmtpTls::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(host),
            SmtpTls::Plain => Ok(AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(
                host,
            )),
        }
        .map_err(|_| "the SMTP host is invalid".to_string())?
        .timeout(Some(SMTP_TIMEOUT));
        if let Some(port) = settings.port {
            builder = builder.port(port);
        }
        if let (Some(username), Some(password)) = (&settings.username, &settings.password) {
            builder = builder.credentials(Credentials::new(username.clone(), password.clone()));
        }
        Ok(Self {
            transport: builder.build(),
            from,
        })
    }
}

#[async_trait::async_trait]
impl MailSender for SmtpMailer {
    async fn send(&self, mail: OutgoingMail) -> Result<(), OutboundError> {
        let to: Mailbox = mail
            .to
            .parse()
            .map_err(|_| OutboundError::Permanent("the stored address is invalid".to_string()))?;
        let domain = self.from.email.domain();
        let message = Message::builder()
            .from(self.from.clone())
            .to(to)
            .subject(mail.subject)
            .message_id(Some(format!("<{}.notification@{domain}>", mail.id)))
            .header(ContentType::TEXT_PLAIN)
            .body(mail.body)
            .map_err(|_| OutboundError::Permanent("the mail could not be built".to_string()))?;
        match self.transport.send(message).await {
            Ok(_) => Ok(()),
            // Server answers may echo the recipient, so only the status class is kept.
            Err(error) if error.is_permanent() => Err(OutboundError::Permanent(status(&error))),
            Err(error) => Err(OutboundError::Retryable(status(&error))),
        }
    }
}

fn status(error: &lettre::transport::smtp::Error) -> String {
    match error.status() {
        Some(code) => format!("SMTP status {code}"),
        None if error.is_timeout() => "SMTP timeout".to_string(),
        None => "SMTP connection failed".to_string(),
    }
}

/// Whether `address` is one mail address a sender can send to.
pub fn valid_address(address: &str) -> bool {
    address.parse::<lettre::Address>().is_ok()
}

/// Builds the sender of one profile; tests swap in a fake one.
pub type MailBuilder =
    Arc<dyn Fn(&SmtpSettings) -> Result<Arc<dyn MailSender>, String> + Send + Sync>;

/// The sender of the current realm email profile, built once per profile revision.
pub(crate) struct MailProfiles {
    cache: Mutex<Option<(Ulid, Arc<dyn MailSender>)>>,
    builder: MailBuilder,
}

impl MailProfiles {
    pub(crate) fn new(builder: MailBuilder) -> Self {
        Self {
            cache: Mutex::new(None),
            builder,
        }
    }

    /// The sender built for `revision`, if it is still cached.
    pub(crate) fn cached(&self, revision: Ulid) -> Option<Arc<dyn MailSender>> {
        let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        (cache.as_ref())
            .filter(|(cached, _)| *cached == revision)
            .map(|(_, sender)| sender.clone())
    }

    /// Builds and caches the sender of `revision`, replacing any other one.
    pub(crate) fn build(
        &self,
        revision: Ulid,
        settings: &SmtpSettings,
    ) -> Result<Arc<dyn MailSender>, String> {
        let sender = (self.builder)(settings)?;
        *self.cache.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((revision, sender.clone()));
        Ok(sender)
    }

    /// Drops the cached sender unless it belongs to the `current` revision.
    pub(crate) fn refresh(&self, current: Option<Ulid>) {
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if cache.as_ref().map(|(revision, _)| *revision) != current {
            *cache = None;
        }
    }
}

pub(crate) fn smtp_builder() -> MailBuilder {
    Arc::new(|settings| Ok(Arc::new(SmtpMailer::new(settings)?) as Arc<dyn MailSender>))
}
