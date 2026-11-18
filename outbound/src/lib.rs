//! Carries out outbound HTTP and mail effects and keeps the in-memory delivery backoffs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod backoff;
mod connection;
mod mail;
mod transfers;

use std::sync::Arc;

use aruna_core::egress::EgressPolicy;
use aruna_core::outbound::{
    HttpMethod, HttpPost, HttpRequest, MailRequest, OutboundEffect, OutboundError, OutboundEvent,
};

use backoff::Backoffs;
pub use connection::ConnectionSender;
use connection::ScreenedSender;
pub use mail::{MailBuilder, MailSender, SmtpMailer, valid_address};
use mail::{MailProfiles, smtp_builder};
pub use transfers::{
    OpenedDownload, TransferBody, TransferError, TransferMeter, UploadAnswer, UploadBody,
};

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
    connections: Arc<dyn ConnectionSender>,
}

impl OutboundHandle {
    /// `http` must screen every address it connects to and follow no redirect; `policy`
    /// screens a literal host before the request starts.
    pub fn new(http: reqwest::Client, policy: EgressPolicy) -> Self {
        Self::with_mail(http, policy, smtp_builder())
    }

    /// Like [`OutboundHandle::new`] with another mail sender builder, as tests use.
    pub fn with_mail(http: reqwest::Client, policy: EgressPolicy, builder: MailBuilder) -> Self {
        let connections = Arc::new(ScreenedSender {
            http: http.clone(),
            policy: policy.clone(),
        });
        Self::assembled((http, policy), builder, connections)
    }

    /// Like [`OutboundHandle::new`] with another connection sender, as tests use.
    pub fn with_connections(
        http: reqwest::Client,
        policy: EgressPolicy,
        connections: Arc<dyn ConnectionSender>,
    ) -> Self {
        Self::assembled((http, policy), smtp_builder(), connections)
    }

    fn assembled(
        (http, policy): (reqwest::Client, EgressPolicy),
        builder: MailBuilder,
        connections: Arc<dyn ConnectionSender>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                http,
                policy,
                mail: MailProfiles::new(builder),
                backoffs: Backoffs::default(),
                connections,
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
            OutboundEffect::Request(request) => OutboundEvent::Answered {
                result: self.request(*request).await,
            },
            OutboundEffect::RefreshMail(current) => {
                self.inner.mail.refresh(current);
                OutboundEvent::MailRefreshed
            }
            OutboundEffect::Connection(call) => OutboundEvent::ConnectionAnswered {
                result: self.inner.connections.send(*call).await,
            },
        }
    }

    async fn post(&self, post: HttpPost) -> Result<u16, OutboundError> {
        self.request(HttpRequest {
            method: HttpMethod::Post,
            url: post.url,
            headers: post.headers,
            body: post.body,
            deadline: post.deadline,
        })
        .await
    }

    async fn request(&self, call: HttpRequest) -> Result<u16, OutboundError> {
        let url = reqwest::Url::parse(&call.url)
            .map_err(|error| OutboundError::Permanent(error.to_string()))?;
        let host = url
            .host_str()
            .ok_or_else(|| OutboundError::Permanent("the URL has no host".to_string()))?;
        (self.inner.policy.check_host(host))
            .map_err(|error| OutboundError::Permanent(format!("egress screen: {error}")))?;
        let method = match call.method {
            HttpMethod::Head => reqwest::Method::HEAD,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Delete => reqwest::Method::DELETE,
        };
        let mut request = self.inner.http.request(method, url).body(call.body);
        for (name, value) in call.headers {
            request = request.header(name, value);
        }
        let response = tokio::time::timeout(call.deadline, request.send())
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
