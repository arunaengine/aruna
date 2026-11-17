//! Carries out outbound HTTP and mail effects and keeps the in-memory delivery backoffs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod backoff;
mod mail;

use std::sync::Arc;

use aruna_core::egress::EgressPolicy;
use aruna_core::outbound::{HttpPost, MailRequest, OutboundEffect, OutboundError, OutboundEvent};

use backoff::Backoffs;
pub use mail::{MailBuilder, MailSender, SmtpMailer, valid_address};
use mail::{MailProfiles, smtp_builder};

/// The outbound backend: one screened HTTP client, the mail sender of the current realm email
/// profile and the delivery backoffs. Clones share all three.
#[derive(Clone)]
pub struct OutboundHandle {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    policy: EgressPolicy,
    mail: MailProfiles,
    backoffs: Backoffs,
}

impl OutboundHandle {
    /// `http` must screen every address it connects to and follow no redirect; `policy`
    /// screens a literal host before the request starts.
    pub fn new(http: reqwest::Client, policy: EgressPolicy) -> Self {
        Self::with_mail(http, policy, smtp_builder())
    }

    /// Like [`OutboundHandle::new`] with another mail sender builder, as tests use.
    pub fn with_mail(http: reqwest::Client, policy: EgressPolicy, builder: MailBuilder) -> Self {
        Self {
            inner: Arc::new(Inner {
                http,
                policy,
                mail: MailProfiles::new(builder),
                backoffs: Backoffs::default(),
            }),
        }
    }

    pub async fn run(&self, effect: OutboundEffect) -> OutboundEvent {
        match effect {
            OutboundEffect::Post(post) => OutboundEvent::Posted {
                result: self.post(*post).await,
            },
            OutboundEffect::Mail(request) => OutboundEvent::MailSent {
                result: self.mail(*request).await,
            },
            OutboundEffect::Backoff(request) => OutboundEvent::Backoff {
                stored: self.inner.backoffs.apply(request),
            },
        }
    }

    /// Drops the cached mail sender unless it belongs to the `current` profile revision.
    pub fn refresh_mail(&self, current: Option<ulid::Ulid>) {
        self.inner.mail.refresh(current);
    }

    async fn post(&self, post: HttpPost) -> Result<u16, OutboundError> {
        let url = reqwest::Url::parse(&post.url)
            .map_err(|error| OutboundError::Permanent(error.to_string()))?;
        let host = url
            .host_str()
            .ok_or_else(|| OutboundError::Permanent("the URL has no host".to_string()))?;
        (self.inner.policy.check_host(host))
            .map_err(|error| OutboundError::Permanent(format!("egress screen: {error}")))?;
        let mut request = self.inner.http.post(url).body(post.body);
        for (name, value) in post.headers {
            request = request.header(name, value);
        }
        let response = tokio::time::timeout(post.deadline, request.send())
            .await
            .map_err(|_| OutboundError::Retryable("the receiver did not answer in time".into()))?
            .map_err(|error| OutboundError::Retryable(format!("request: {error}")))?;
        Ok(response.status().as_u16())
    }

    async fn mail(&self, request: MailRequest) -> Result<(), OutboundError> {
        let sender = match (self.inner.mail.cached(request.revision), request.settings) {
            (Some(sender), _) => sender,
            (None, Some(settings)) => (self.inner.mail.build(request.revision, &settings))
                .map_err(OutboundError::Permanent)?,
            (None, None) => return Err(OutboundError::NoSender),
        };
        sender.send(request.mail).await
    }
}
