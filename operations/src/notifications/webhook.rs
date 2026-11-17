//! Writes a user's webhook subscription as a register write and delivers share
//! upload notifications to it as jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::errors::ConversionError;
use aruna_core::structs::execution::job::JobResultPayload;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::execution::webhook::{
    URL_BYTES, WebhookDeliverySpec, WebhookSubscription, valid_url,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_outbound::Backoffs;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::notifications::email::{job_error, no_transport};
use crate::notifications::send::SendOperation;
use crate::notifications::setting_rows::{
    ReadSettingOperation, SettingError, SettingValue, SettingWrite, WriteSettingOperation,
};
use crate::notifications::settings::{
    Delivery, MetadataPeers, SettingPeers, delivery_setting, managers,
};
use crate::realm::get_config::GetConfigOperation;

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

/// Sends one signed delivery when this node's current setting has the webhook on. A 4xx answer
/// other than 408 and 429 is permanent; while no management node opens the secret, the job
/// waits without an attempt.
pub async fn run_webhook_job(ctx: &JobContext, spec: &WebhookDeliverySpec) -> JobRunOutcome {
    let (Some(_), Some(outbound)) = (&ctx.driver.metadata_handle, &ctx.driver.outbound_handle)
    else {
        return no_transport();
    };
    let local = (ctx.driver.net_handle.as_ref()).map_or(ctx.owner_node_id, |net| net.node_id());
    let peers = MetadataPeers {
        ctx: &ctx.driver,
        local,
    };
    let now_ms = unix_timestamp_millis();
    deliver_webhook(ctx, spec, &peers, outbound.backoffs(), now_ms).await
}

pub(crate) async fn deliver_webhook(
    ctx: &JobContext,
    spec: &WebhookDeliverySpec,
    peers: &dyn SettingPeers,
    backoffs: &Backoffs,
    now_ms: u64,
) -> JobRunOutcome {
    let recipient = spec.notification.recipient;
    let config = drive(GetConfigOperation::new(recipient.realm_id), &ctx.driver).await;
    let managers = config.ok().map(|config| managers(&config));
    let record = &spec.notification;
    let channel = SettingChannel::Webhook;
    let delivery = delivery_setting(
        &ctx.driver,
        peers,
        backoffs,
        record,
        channel,
        managers.as_deref(),
        now_ms,
    );
    let delivery = tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
        () = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
        delivery = delivery => delivery,
    };
    let winner = match delivery {
        Delivery::Skip => {
            return JobRunOutcome::Succeeded(JobResultPayload::Webhook { status: None });
        }
        Delivery::Wait(error, delay) => return JobRunOutcome::DeferredFor(error, delay),
        Delivery::Send(winner) => winner,
    };
    let subscription = WebhookSubscription::from_bytes(&winner.bytes).ok();
    let (Some(url), Some(secret)) = (subscription.and_then(|found| found.url), winner.secret)
    else {
        return JobRunOutcome::Succeeded(JobResultPayload::Webhook { status: None });
    };
    let now_s = unix_timestamp_millis() / 1000;
    let send = SendOperation::webhook(spec, &url, secret.expose(), now_s);
    let sent = tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
        () = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
        sent = drive(send, &ctx.driver) => sent,
    };
    match sent {
        Ok(status) => JobRunOutcome::Succeeded(JobResultPayload::Webhook { status }),
        Err(error) => JobRunOutcome::Failed(job_error(error)),
    }
}

#[cfg(test)]
#[path = "webhook_tests.rs"]
mod tests;
