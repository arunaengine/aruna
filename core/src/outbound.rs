//! Declares the HTTP, mail and delivery backoff effects that the outbound backend carries out and
//! their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use bytes::Bytes;
use thiserror::Error;
use ulid::Ulid;

use crate::UserId;
use crate::structs::execution::email::SmtpTls;
use crate::structs::execution::setting::SettingChannel;

#[derive(Debug, Clone, PartialEq)]
pub enum OutboundEffect {
    /// Sends one POST through the egress screen and answers with its status.
    Post(Box<HttpPost>),
    /// Sends one mail with the cached sender of the profile revision, or with a new one built
    /// from the settings when they are given.
    Mail(Box<MailRequest>),
    /// Reads or changes the in-memory delay of the deliveries to one owner and channel.
    Backoff(BackoffRequest),
    /// Drops the cached mail sender unless it belongs to this profile revision.
    RefreshMail(Option<Ulid>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackoffRequest {
    pub owner: UserId,
    pub channel: SettingChannel,
    pub change: BackoffChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffChange {
    Read,
    /// Stores a delay of `delay_ms` that ends at `until_ms`; the operation decides both.
    Set {
        until_ms: u64,
        delay_ms: u64,
    },
    Clear,
}

/// A stored delivery delay: when it ends and how long it was, so the next one can grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredBackoff {
    pub until_ms: u64,
    pub delay_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundEvent {
    /// The status the receiver answered with.
    Posted {
        result: Result<u16, OutboundError>,
    },
    MailSent {
        result: Result<(), OutboundError>,
    },
    /// The delay stored after the change; none on a node without the outbound backend.
    Backoff {
        stored: Option<StoredBackoff>,
    },
    MailRefreshed,
}

/// A failure of one outbound call; its text never holds an address, a body or a credential.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OutboundError {
    #[error("the request was refused: {0}")]
    Permanent(String),
    #[error("the request could not be sent now: {0}")]
    Retryable(String),
    /// No sender is cached for the profile revision and no settings were given.
    #[error("no mail sender is cached for this profile")]
    NoSender,
}

#[derive(Clone, PartialEq, Eq)]
pub struct HttpPost {
    pub url: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: Bytes,
    /// Total time the call may take.
    pub deadline: Duration,
}

/// Shows neither the target path nor the body.
impl std::fmt::Debug for HttpPost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpPost")
            .field("body_len", &self.body.len())
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailRequest {
    pub revision: Ulid,
    pub settings: Option<SmtpSettings>,
    pub mail: OutgoingMail,
}

/// A realm email profile with its opened password.
#[derive(Clone, PartialEq, Eq)]
pub struct SmtpSettings {
    pub host: String,
    pub port: Option<u16>,
    pub tls: SmtpTls,
    pub username: Option<String>,
    pub password: Option<String>,
    pub from: String,
}

/// Shows neither the password nor the sender account.
impl std::fmt::Debug for SmtpSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmtpSettings")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("has_credentials", &self.password.is_some())
            .finish_non_exhaustive()
    }
}

/// One mail; the recipient is never shown by `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct OutgoingMail {
    pub to: String,
    pub subject: String,
    pub body: String,
    /// Stays the same for every retry and becomes the `Message-ID`.
    pub id: Ulid,
}

impl std::fmt::Debug for OutgoingMail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingMail")
            .field("subject", &self.subject)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
