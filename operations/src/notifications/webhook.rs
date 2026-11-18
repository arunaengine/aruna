//! Reads and writes a user's webhook subscription as a register write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::errors::ConversionError;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::execution::webhook::{URL_BYTES, WebhookSubscription, valid_url};
use thiserror::Error;

use crate::notifications::setting_rows::{
    ReadSettingOperation, SettingError, SettingValue, SettingWrite, WriteSettingOperation,
};

#[derive(Debug, Error, PartialEq)]
pub enum WebhookError {
    #[error(transparent)]
    Setting(#[from] SettingError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(
        "the webhook URL must be an http or https URL without credentials, at most {URL_BYTES} bytes"
    )]
    InvalidUrl,
}

impl From<IoError> for WebhookError {
    fn from(error: IoError) -> Self {
        Self::Setting(SettingError::Io(error))
    }
}

/// A new subscription with its signing secret, hex encoded and shown only once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebhookOutcome {
    pub subscription: WebhookSubscription,
    pub secret: String,
}

/// The caller's current subscription as this node received it; `None` when it is off.
#[derive(Debug, PartialEq)]
pub struct ReadWebhookOperation {
    owner: UserId,
}

impl ReadWebhookOperation {
    pub fn new(owner: UserId) -> Self {
        Self { owner }
    }
}

impl Operation for ReadWebhookOperation {
    type Output = Option<WebhookSubscription>;
    type Error = WebhookError;

    fn first(&mut self) -> Result<Next<Self::Output>, WebhookError> {
        let read = ReadSettingOperation::new(SettingChannel::Webhook, self.owner);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, WebhookError> {
        let bytes = result.child::<Option<Vec<u8>>, SettingError>()??;
        let subscription = bytes.as_deref().map(WebhookSubscription::from_bytes);
        Ok(Next::Done(subscription.transpose()?))
    }
}

/// Turns the webhook on for a valid URL with a new signing secret, sealed to the realm key.
#[derive(Debug, PartialEq)]
pub struct SetWebhookOperation {
    write: SettingWrite,
    url_valid: bool,
    secret: SecretBytes,
}

impl SetWebhookOperation {
    pub fn new(owner: UserId, local: NodeId, url: String, now_ms: u64) -> Self {
        let secret: [u8; 32] = rand::random();
        Self::with_secret(owner, local, url, SecretBytes::new(secret.to_vec()), now_ms)
    }

    fn with_secret(
        owner: UserId,
        local: NodeId,
        url: String,
        secret: SecretBytes,
        now_ms: u64,
    ) -> Self {
        let url_valid = valid_url(&url);
        let value = SettingValue::Webhook {
            target: Some((url, secret.clone())),
        };
        Self {
            write: SettingWrite::new(owner, local, now_ms, value),
            url_valid,
            secret,
        }
    }
}

impl Operation for SetWebhookOperation {
    type Output = WebhookOutcome;
    type Error = WebhookError;

    fn first(&mut self) -> Result<Next<WebhookOutcome>, WebhookError> {
        if !self.url_valid {
            return Err(WebhookError::InvalidUrl);
        }
        let write = WriteSettingOperation::new(self.write.clone());
        Ok(Next::Call(IoCall::child(write)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WebhookOutcome>, WebhookError> {
        let bytes = result.child::<Vec<u8>, SettingError>()??;
        Ok(Next::Done(WebhookOutcome {
            subscription: WebhookSubscription::from_bytes(&bytes)?,
            secret: hex::encode(self.secret.expose()),
        }))
    }
}

/// Turns the webhook off with a newer register write; queued deliveries then send nothing.
#[derive(Debug, PartialEq)]
pub struct RemoveWebhookOperation {
    write: SettingWrite,
}

impl RemoveWebhookOperation {
    pub fn new(owner: UserId, local: NodeId, now_ms: u64) -> Self {
        let off = SettingValue::Webhook { target: None };
        Self {
            write: SettingWrite::new(owner, local, now_ms, off),
        }
    }
}

impl Operation for RemoveWebhookOperation {
    type Output = ();
    type Error = WebhookError;

    fn first(&mut self) -> Result<Next<()>, WebhookError> {
        let write = WriteSettingOperation::new(self.write.clone());
        Ok(Next::Call(IoCall::child(write)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, WebhookError> {
        let bytes = result.child::<Vec<u8>, SettingError>()??;
        WebhookSubscription::from_bytes(&bytes)?;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
#[path = "webhook_tests.rs"]
mod tests;
