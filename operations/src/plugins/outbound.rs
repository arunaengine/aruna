//! Sends a plugin's connection requests through the blob runtime's screened egress client, which
//! checks addresses at connect time and follows no redirect, and reads answers within a bound.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::blob::BlobHandle;
use aruna_core::injection::OutgoingRequest;
use async_trait::async_trait;

/// Bytes of an answer a connection request reads at most.
pub const MAX_RESPONSE: usize = 1024 * 1024;

/// An answer as the destination sent it, before any exposure applies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Why a request has no answer, and whether it may have reached the destination.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    #[error("the request was not sent: {0}")]
    NotSent(String),
    #[error("the request may have been sent: {0}")]
    Unknown(String),
}

#[async_trait]
pub trait Outbound: Send + Sync {
    async fn send(&self, request: OutgoingRequest, body: Vec<u8>) -> Result<Answer, SendError>;
}

/// The node's screened egress client; a node without a blob runtime sends nothing.
pub struct Egress(pub Option<BlobHandle>);

#[async_trait]
impl Outbound for Egress {
    async fn send(&self, request: OutgoingRequest, body: Vec<u8>) -> Result<Answer, SendError> {
        let not_sent = |why: String| SendError::NotSent(why);
        let blob = self
            .0
            .as_ref()
            .ok_or_else(|| not_sent("no egress client here".into()))?;
        let method = reqwest::Method::from_bytes(request.method.as_bytes());
        let method = method.map_err(|error| not_sent(error.to_string()))?;
        let builder = blob.repository_request(method, request.url);
        let builder = builder.map_err(|error| not_sent(error.to_string()))?;
        let builder = (request.headers.iter()).fold(builder, |builder, (name, value)| {
            builder.header(name, value)
        });
        let mut response = match builder.body(body).send().await {
            Ok(response) => response,
            Err(error) if error.is_connect() || error.is_builder() => {
                return Err(not_sent(error.without_url().to_string()));
            }
            Err(error) => return Err(SendError::Unknown(error.without_url().to_string())),
        };
        let status = response.status().as_u16();
        let headers = (response.headers().iter())
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect();
        let mut read = Vec::new();
        loop {
            let chunk = response.chunk().await;
            let chunk = chunk.map_err(|error| SendError::Unknown(error.to_string()))?;
            let Some(chunk) = chunk else {
                break;
            };
            if read.len() + chunk.len() > MAX_RESPONSE {
                return Err(SendError::Unknown("the answer is larger than 1 MiB".into()));
            }
            read.extend_from_slice(&chunk);
        }
        Ok(Answer {
            status,
            headers,
            body: read,
        })
    }
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
pub(crate) mod tests;
