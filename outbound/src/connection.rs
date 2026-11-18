//! Sends one request of a plugin connection through the egress screen and reads its answer
//! within a bound. No redirect is followed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::egress::{EgressError, EgressPolicy};
use aruna_core::outbound::{
    ConnectionAnswer, ConnectionCall, ConnectionError, MAX_CONNECTION_ANSWER,
};
use async_trait::async_trait;

/// Carries out connection calls; tests put a fake in place of the screened client.
#[async_trait]
pub trait ConnectionSender: Send + Sync {
    async fn send(&self, call: ConnectionCall) -> Result<ConnectionAnswer, ConnectionError>;
}

/// The request of `method` to `url`, once the egress screen admits its host.
pub(crate) fn screened(
    (http, policy): (&reqwest::Client, &EgressPolicy),
    method: reqwest::Method,
    url: reqwest::Url,
) -> Result<reqwest::RequestBuilder, EgressError> {
    let host = url
        .host_str()
        .ok_or_else(|| EgressError::MissingHost(url.to_string()))?;
    policy.check_host(host)?;
    Ok(http.request(method, url))
}

/// The node's screened client.
pub(crate) struct ScreenedSender {
    pub(crate) http: reqwest::Client,
    pub(crate) policy: EgressPolicy,
}

#[async_trait]
impl ConnectionSender for ScreenedSender {
    async fn send(&self, call: ConnectionCall) -> Result<ConnectionAnswer, ConnectionError> {
        let not_sent = |why: String| ConnectionError::NotSent(why);
        let ConnectionCall { request, body } = call;
        let method = reqwest::Method::from_bytes(request.method.as_bytes());
        let method = method.map_err(|error| not_sent(error.to_string()))?;
        let client = (&self.http, &self.policy);
        let builder = screened(client, method, request.url);
        let builder = builder.map_err(|error| not_sent(error.to_string()))?;
        let builder = (request.headers.iter()).fold(builder, |builder, (name, value)| {
            builder.header(name, value)
        });
        let mut response = match builder.body(body).send().await {
            Ok(response) => response,
            Err(error) if error.is_connect() || error.is_builder() => {
                return Err(not_sent(error.without_url().to_string()));
            }
            Err(error) => {
                return Err(ConnectionError::Unknown(error.without_url().to_string()));
            }
        };
        let status = response.status().as_u16();
        let headers: Vec<(String, String)> = (response.headers().iter())
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect();
        // The bound covers the whole answer the guest may get: headers and body together.
        let head: usize = (headers.iter())
            .map(|(name, value)| name.len() + value.len())
            .sum();
        if head > MAX_CONNECTION_ANSWER {
            return Err(ConnectionError::TooLarge { status });
        }
        let mut read = Vec::new();
        loop {
            let chunk = response.chunk().await;
            let chunk = chunk.map_err(|error| ConnectionError::Unknown(error.to_string()))?;
            let Some(chunk) = chunk else {
                break;
            };
            if head + read.len() + chunk.len() > MAX_CONNECTION_ANSWER {
                return Err(ConnectionError::TooLarge { status });
            }
            read.extend_from_slice(&chunk);
        }
        Ok(ConnectionAnswer {
            status,
            headers,
            body: read,
        })
    }
}
