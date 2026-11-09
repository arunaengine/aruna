//! Writes a user's webhook subscription through the management quorum and delivers share
//! upload notifications to it as jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::errors::ConversionError;
use aruna_core::realm_vault::seal_setting;
use aruna_core::structs::execution::job::{
    JobError, JobPayload, JobResultPayload, WorkspaceMode, user_dedup_key,
};
use aruna_core::structs::execution::notification::{NotificationKind, NotificationRecord};
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::execution::webhook::{
    DELIVERY_HEADER, SIGNATURE_HEADER, URL_BYTES, WebhookDeliverySpec, WebhookSubscription,
    sign_delivery, valid_url,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::time::unix_timestamp_millis;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::service::{find_dedup_job, mint_local_job, submit_local_job};
use crate::jobs::submit::SubmitJobSpec;
use crate::notifications::email::no_transport;
use crate::notifications::settings::{
    Backoffs, Delivery, MetadataPeers, SettingError, SettingPeers, delivery_setting, local_setting,
    managers, write_setting,
};
use crate::realm::get_config::GetConfigOperation;

/// Total time one delivery attempt may take; the job runtime retries a timed out attempt.
const DELIVERY_DEADLINE: Duration = Duration::from_secs(30);

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
    let bytes = local_setting(ctx, SettingChannel::Webhook, owner).await?;
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
    let config = realm_config(ctx, owner).await?;
    let secret: [u8; 32] = rand::random();
    let sealed_secret = SecretBytes::new(secret.to_vec());
    let build = |version: u64, id: Ulid| {
        let sealed = seal_setting(
            &owner.realm_id,
            SettingChannel::Webhook,
            &owner,
            id,
            &sealed_secret,
        )
        .map_err(|error| SettingError::Build(error.to_string()))?;
        webhook_bytes(owner, id, version, Some(url.clone()), Some(sealed))
    };
    let subscription = write_webhook(ctx, local, &config, owner, &build).await?;
    Ok(WebhookOutcome {
        subscription,
        secret: hex::encode(secret),
    })
}

/// Turns the webhook off with a newer setting; queued deliveries then send nothing.
pub async fn remove_webhook(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
) -> Result<(), WebhookError> {
    let config = realm_config(ctx, owner).await?;
    let build = |version: u64, id: Ulid| webhook_bytes(owner, id, version, None, None);
    write_webhook(ctx, local, &config, owner, &build)
        .await
        .map(|_| ())
}

async fn write_webhook(
    ctx: &DriverContext,
    local: NodeId,
    config: &RealmConfigDocument,
    owner: UserId,
    build: &(dyn Fn(u64, Ulid) -> Result<Vec<u8>, SettingError> + Sync),
) -> Result<WebhookSubscription, WebhookError> {
    let peers = MetadataPeers { ctx, local };
    let managers = managers(config);
    let channel = SettingChannel::Webhook;
    let bytes = write_setting(ctx, &peers, local, &managers, channel, owner, build).await?;
    Ok(WebhookSubscription::from_bytes(&bytes)?)
}

fn webhook_bytes(
    owner: UserId,
    id: Ulid,
    version: u64,
    url: Option<String>,
    sealed: Option<Vec<u8>>,
) -> Result<Vec<u8>, SettingError> {
    let subscription = WebhookSubscription {
        subscription_id: id,
        user_id: owner,
        version,
        url,
        sealed,
        created_at_ms: unix_timestamp_millis(),
    };
    subscription
        .to_bytes()
        .map_err(|error| SettingError::Build(error.to_string()))
}

async fn realm_config(
    ctx: &DriverContext,
    owner: UserId,
) -> Result<RealmConfigDocument, SettingError> {
    drive(GetConfigOperation::new(owner.realm_id), ctx)
        .await
        .map_err(|error| SettingError::Config(error.to_string()))
}

/// Queues one delivery job per held share upload notification, whatever the settings say now;
/// the job decides from a quorum read. The notification id deduplicates; a failure is returned
/// so the sender delivers again.
pub async fn enqueue_webhooks(
    context: &DriverContext,
    owner_node_id: NodeId,
    records: &[NotificationRecord],
) -> Result<(), String> {
    for record in records {
        if !matches!(record.kind, NotificationKind::ShareUpload { .. }) {
            continue;
        }
        let dedup_key = user_dedup_key(
            record.recipient,
            &format!("webhook/{}", record.notification_id),
        );
        let queued = find_dedup_job(context, record.recipient, &dedup_key).await;
        if queued
            .map_err(|error| format!("webhook delivery lookup: {error}"))?
            .is_some()
        {
            continue;
        }
        let realm_id = record.recipient.realm_id;
        let submitted = async {
            let job_id = mint_local_job(context, realm_id, owner_node_id, Some(&dedup_key)).await?;
            let spec = SubmitJobSpec {
                payload: JobPayload::WebhookDelivery(WebhookDeliverySpec {
                    subscription_id: Ulid::nil(),
                    notification: record.clone(),
                }),
                created_by: record.recipient,
                owner_node_id,
                dedup_key: Some(dedup_key.clone()),
                now_ms: unix_timestamp_millis(),
                retention_ms: crate::jobs::JOB_RETENTION_MS,
                workspace_mode: WorkspaceMode::default(),
                workspace_bucket: None,
                active_cap: None,
            };
            submit_local_job(context, spec, job_id).await
        };
        submitted
            .await
            .map_err(|error| format!("webhook delivery queue: {error}"))?;
    }
    Ok(())
}

/// Sends one signed delivery when a quorum of management nodes says the webhook is on. A 4xx
/// answer other than 408 and 429 is permanent; a short quorum waits without an attempt.
pub async fn run_webhook_job(ctx: &JobContext, spec: &WebhookDeliverySpec) -> JobRunOutcome {
    let Some(metadata) = ctx.driver.metadata_handle.as_ref() else {
        return no_transport();
    };
    let local = (ctx.driver.net_handle.as_ref()).map_or(ctx.owner_node_id, |net| net.node_id());
    let peers = MetadataPeers {
        ctx: &ctx.driver,
        local,
    };
    let now_ms = unix_timestamp_millis();
    deliver_webhook(ctx, spec, &peers, metadata.setting_backoffs(), now_ms).await
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
    match deliver(ctx, spec, &url, secret.expose()).await {
        Ok(status) => JobRunOutcome::Succeeded(JobResultPayload::Webhook { status }),
        Err(Failure::Cancelled) => JobRunOutcome::Cancelled,
        Err(Failure::Interrupted) => JobRunOutcome::Interrupted,
        Err(Failure::Job(error)) => JobRunOutcome::Failed(error),
    }
}

enum Failure {
    Cancelled,
    Interrupted,
    Job(JobError),
}

async fn deliver(
    ctx: &JobContext,
    spec: &WebhookDeliverySpec,
    url: &str,
    secret: &[u8],
) -> Result<Option<u16>, Failure> {
    let body = spec
        .body()
        .ok_or_else(|| permanent("only share upload notifications are delivered"))?;
    let body = serde_json::to_vec(&body).map_err(|error| permanent(error.to_string()))?;
    let url = url::Url::parse(url).map_err(|error| permanent(error.to_string()))?;
    let blob = ctx
        .driver
        .blob_handle
        .as_ref()
        .ok_or_else(|| retryable("blob handle unavailable for webhook egress"))?;
    let timestamp = unix_timestamp_millis() / 1000;
    let request = blob
        .repository_request(reqwest::Method::POST, url)
        .map_err(|error| permanent(format!("egress screen: {error}")))?
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(
            DELIVERY_HEADER,
            spec.notification.notification_id.to_string(),
        )
        .header(SIGNATURE_HEADER, sign_delivery(secret, timestamp, &body))
        .body(body);
    let response = tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => return Err(Failure::Cancelled),
        () = ctx.shutdown.cancelled() => return Err(Failure::Interrupted),
        result = tokio::time::timeout(DELIVERY_DEADLINE, request.send()) => result,
    };
    let response = response
        .map_err(|_| retryable("the webhook receiver did not answer in time"))?
        .map_err(|error| retryable(format!("webhook request: {error}")))?;
    let status = response.status();
    if status.is_success() {
        return Ok(Some(status.as_u16()));
    }
    let message = format!("the webhook receiver answered {status}");
    match status.is_client_error() && !matches!(status.as_u16(), 408 | 429) {
        true => Err(permanent(message)),
        false => Err(retryable(message)),
    }
}

fn retryable(message: impl Into<String>) -> Failure {
    Failure::Job(JobError::retryable(message))
}

fn permanent(message: impl Into<String>) -> Failure {
    Failure::Job(JobError::permanent(message))
}

#[cfg(test)]
#[path = "webhook_tests.rs"]
mod tests;
