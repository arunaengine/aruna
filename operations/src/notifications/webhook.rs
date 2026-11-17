//! Writes a user's webhook subscription as a register write and delivers share
//! upload notifications to it as jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::errors::ConversionError;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::execution::webhook::{
    URL_BYTES, WebhookDeliverySpec, WebhookSubscription, valid_url,
};
use aruna_core::time::unix_timestamp_millis;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::notifications::email::{job_outcome, no_transport};
use crate::notifications::setting_rows::{
    ReadSettingOperation, SettingError, SettingValue, SettingWrite, WriteSettingOperation,
};
use crate::notifications::webhook_job::WebhookJobOperation;

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

/// A new subscription with its signing secret, hex encoded and shown only once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebhookOutcome {
    pub subscription: WebhookSubscription,
    pub secret: String,
}

/// The caller's current subscription as this node received it; `None` when it is off.
pub async fn read_webhook(
    ctx: &DriverContext,
    owner: UserId,
) -> Result<Option<WebhookSubscription>, WebhookError> {
    let read = ReadSettingOperation::new(SettingChannel::Webhook, owner);
    let bytes = drive(read, ctx).await?;
    Ok(bytes
        .as_deref()
        .map(WebhookSubscription::from_bytes)
        .transpose()?)
}

/// Turns the webhook on for `url` with a new signing secret, sealed to the realm key.
pub async fn set_webhook(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
    url: String,
) -> Result<WebhookOutcome, WebhookError> {
    if !valid_url(&url) {
        return Err(WebhookError::InvalidUrl);
    }
    let secret: [u8; 32] = rand::random();
    let target = Some((url, SecretBytes::new(secret.to_vec())));
    let subscription = write_webhook(ctx, local, owner, target).await?;
    Ok(WebhookOutcome {
        subscription,
        secret: hex::encode(secret),
    })
}

/// Turns the webhook off with a newer register write; queued deliveries then send nothing.
pub async fn remove_webhook(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
) -> Result<(), WebhookError> {
    write_webhook(ctx, local, owner, None).await.map(|_| ())
}

async fn write_webhook(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
    target: Option<(String, SecretBytes)>,
) -> Result<WebhookSubscription, WebhookError> {
    let write = SettingWrite {
        owner,
        local,
        write_id: Ulid::generate(),
        event_id: Ulid::generate(),
        now_ms: unix_timestamp_millis(),
        value: SettingValue::Webhook { target },
    };
    let bytes = drive(WriteSettingOperation::new(write), ctx).await?;
    Ok(WebhookSubscription::from_bytes(&bytes)?)
}

/// Runs the webhook job of `spec` until the job is cancelled or the node shuts down.
pub(crate) async fn run_webhook_job(ctx: &JobContext, spec: &WebhookDeliverySpec) -> JobRunOutcome {
    if ctx.driver.metadata_handle.is_none() || ctx.driver.outbound_handle.is_none() {
        return no_transport();
    }
    let local = (ctx.driver.net_handle.as_ref()).map_or(ctx.owner_node_id, |net| net.node_id());
    let job = WebhookJobOperation::new(spec.clone(), local, unix_timestamp_millis());
    tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => JobRunOutcome::Cancelled,
        () = ctx.shutdown.cancelled() => JobRunOutcome::Interrupted,
        outcome = drive(job, &ctx.driver) => job_outcome(outcome),
    }
}

#[cfg(test)]
#[path = "webhook_tests.rs"]
mod tests;
