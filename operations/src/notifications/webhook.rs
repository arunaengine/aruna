//! Keeps a user's webhook subscription and delivers share upload notifications to it as jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::WEBHOOK_KEYSPACE;
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobError, JobPayload, JobResultPayload, WorkspaceMode, user_dedup_key,
};
use aruna_core::structs::execution::notification::{NotificationKind, NotificationRecord};
use aruna_core::structs::execution::webhook::{
    DELIVERY_HEADER, SIGNATURE_HEADER, WebhookDeliverySpec, WebhookSubscription, sign_delivery,
    webhook_key,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::service::{find_dedup_job, mint_local_job, submit_local_job};
use crate::jobs::submit::SubmitJobSpec;
use crate::node_vault::{delete_secret, parse_secret, read_secret, write_secret};
use crate::storage_read::{StorageReadError, parse_storage_read};

const URL_BYTES: usize = 2048;
/// Total time one delivery attempt may take; the job runtime retries a timed out attempt.
const DELIVERY_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WebhookAction {
    Read,
    /// Replaces any subscription and its secret with a new one for `url`.
    Set {
        url: String,
    },
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct WebhookOutcome {
    pub subscription: Option<WebhookSubscription>,
    /// The new signing secret, hex encoded; present only right after `Set`.
    pub secret: Option<String>,
}

#[derive(Debug, Error, PartialEq)]
pub enum WebhookError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(
        "the webhook URL must be an http or https URL without credentials, at most {URL_BYTES} bytes"
    )]
    InvalidUrl,
    #[error("webhook operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: {event}")]
    UnexpectedEvent { state: String, event: String },
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Read,
    WriteSecret { previous: Option<Ulid> },
    WriteRecord { previous: Option<Ulid> },
    DeleteRecord { previous: Ulid },
    DeleteSecret,
    Done,
}

/// Reads, sets or removes the caller's own subscription on this node.
#[derive(Debug, PartialEq)]
pub struct WebhookOperation {
    user_id: UserId,
    action: WebhookAction,
    state: State,
    outcome: WebhookOutcome,
    secret: Option<SecretBytes>,
    output: Option<Result<WebhookOutcome, WebhookError>>,
}

impl WebhookOperation {
    pub fn new(user_id: UserId, action: WebhookAction) -> Self {
        Self {
            user_id,
            action,
            state: State::Init,
            outcome: WebhookOutcome::default(),
            secret: None,
            output: None,
        }
    }

    fn fail(&mut self, error: WebhookError) -> Effects {
        self.state = State::Done;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn done(&mut self) -> Effects {
        self.state = State::Done;
        self.output = Some(Ok(std::mem::take(&mut self.outcome)));
        smallvec![]
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(WebhookError::UnexpectedEvent {
            state,
            event: format!("{event:?}"),
        })
    }

    fn record_effect(&self, record: &WebhookSubscription) -> Result<Effect, ConversionError> {
        Ok(Effect::Storage(StorageEffect::Write {
            key_space: WEBHOOK_KEYSPACE.to_string(),
            key: webhook_key(&self.user_id).into(),
            value: record.to_bytes()?.into(),
            txn_id: None,
        }))
    }

    fn handle_read(&mut self, event: Event) -> Effects {
        let existing = match parse_storage_read(event, WebhookSubscription::from_bytes) {
            Ok(existing) => existing,
            Err(StorageReadError::Storage(error)) => return self.fail(error.into()),
            Err(StorageReadError::Conversion(error)) => return self.fail(error.into()),
        };
        match self.action.clone() {
            WebhookAction::Read => {
                self.outcome.subscription = existing;
                self.done()
            }
            WebhookAction::Remove => match existing {
                None => self.done(),
                Some(existing) => {
                    self.state = State::DeleteRecord {
                        previous: existing.subscription_id,
                    };
                    smallvec![Effect::Storage(StorageEffect::Delete {
                        key_space: WEBHOOK_KEYSPACE.to_string(),
                        key: webhook_key(&self.user_id).into(),
                        txn_id: None,
                    })]
                }
            },
            WebhookAction::Set { url } => {
                if !valid_url(&url) {
                    return self.fail(WebhookError::InvalidUrl);
                }
                let subscription = WebhookSubscription {
                    subscription_id: Ulid::generate(),
                    user_id: self.user_id,
                    url,
                    created_at_ms: unix_timestamp_millis(),
                };
                let secret: [u8; 32] = rand::random();
                self.outcome.secret = Some(hex::encode(secret));
                let entry = secret_entry(subscription.subscription_id);
                self.outcome.subscription = Some(subscription);
                self.state = State::WriteSecret {
                    previous: existing.map(|existing| existing.subscription_id),
                };
                smallvec![write_secret(entry, SecretBytes::new(secret.to_vec()), None)]
            }
        }
    }

    fn after_secret(&mut self, previous: Option<Ulid>) -> Effects {
        let Some(subscription) = self.outcome.subscription.clone() else {
            return self.fail(WebhookError::NotFinished);
        };
        match self.record_effect(&subscription) {
            Ok(effect) => {
                self.state = State::WriteRecord { previous };
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn drop_secret(&mut self, previous: Option<Ulid>) -> Effects {
        match previous {
            None => self.done(),
            Some(previous) => {
                self.state = State::DeleteSecret;
                smallvec![delete_secret(secret_entry(previous), None)]
            }
        }
    }
}

impl Operation for WebhookOperation {
    type Output = WebhookOutcome;
    type Error = WebhookError;

    fn start(&mut self) -> Effects {
        self.state = State::Read;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: WEBHOOK_KEYSPACE.to_string(),
            key: webhook_key(&self.user_id).into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match std::mem::replace(&mut self.state, State::Done) {
            State::Read => self.handle_read(event),
            State::WriteSecret { previous } => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.after_secret(previous),
                other => self.unexpected(other),
            },
            State::WriteRecord { previous } => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.drop_secret(previous),
                other => self.unexpected(other),
            },
            State::DeleteRecord { previous } => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.drop_secret(Some(previous))
                }
                other => self.unexpected(other),
            },
            State::DeleteSecret => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => self.done(),
                other => self.unexpected(other),
            },
            State::Init | State::Done => self.unexpected(event),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(WebhookError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn secret_entry(subscription_id: Ulid) -> VaultEntry {
    VaultEntry::new(VaultPurpose::Webhook, subscription_id)
}

fn valid_url(url: &str) -> bool {
    url.len() <= URL_BYTES
        && url::Url::parse(url).is_ok_and(|parsed| {
            matches!(parsed.scheme(), "http" | "https")
                && parsed.username().is_empty()
                && parsed.password().is_none()
                && parsed.host().is_some()
        })
}

/// Queues one delivery job per held share upload notification whose recipient subscribed here.
/// The notification id deduplicates; a failure is returned so the sender delivers again.
pub async fn enqueue_webhooks(
    context: &DriverContext,
    owner_node_id: NodeId,
    records: &[NotificationRecord],
) -> Result<(), String> {
    for record in records {
        if !matches!(record.kind, NotificationKind::ShareUpload { .. }) {
            continue;
        }
        let read = drive(
            WebhookOperation::new(record.recipient, WebhookAction::Read),
            context,
        )
        .await;
        let subscription = match read {
            Ok(WebhookOutcome {
                subscription: Some(subscription),
                ..
            }) => subscription,
            Ok(_) => continue,
            Err(error) => return Err(format!("webhook subscription read: {error}")),
        };
        let dedup_key = user_dedup_key(
            record.recipient,
            &format!("webhook/{}", record.notification_id),
        );
        // A queued job keeps the settings it was queued with, so changed settings never conflict.
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
                    subscription_id: subscription.subscription_id,
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

/// Sends one signed delivery. A missing or replaced subscription sends nothing; a 4xx answer
/// other than 408 and 429 is permanent, and everything else is retried by the job runtime.
pub async fn run_webhook_job(ctx: &JobContext, spec: &WebhookDeliverySpec) -> JobRunOutcome {
    match deliver(ctx, spec).await {
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

async fn deliver(ctx: &JobContext, spec: &WebhookDeliverySpec) -> Result<Option<u16>, Failure> {
    let recipient = spec.notification.recipient;
    let read = drive(
        WebhookOperation::new(recipient, WebhookAction::Read),
        &ctx.driver,
    )
    .await;
    let subscription = match read {
        Ok(outcome) => outcome.subscription,
        Err(error) => return Err(retryable(error.to_string())),
    };
    let Some(subscription) =
        subscription.filter(|found| found.subscription_id == spec.subscription_id)
    else {
        return Ok(None);
    };
    let body = spec
        .body()
        .ok_or_else(|| permanent("only share upload notifications are delivered"))?;
    let body = serde_json::to_vec(&body).map_err(|error| permanent(error.to_string()))?;
    let entry = secret_entry(subscription.subscription_id);
    let event = ctx
        .driver
        .storage_handle
        .send_effect(read_secret(entry, None))
        .await;
    let secret = parse_secret(event, entry, |bytes| {
        Ok::<_, ConversionError>(bytes.to_vec())
    })
    .map_err(|error| retryable(error.to_string()))?
    .ok_or_else(|| permanent("the webhook signing secret is missing"))?;
    let url = url::Url::parse(&subscription.url).map_err(|error| permanent(error.to_string()))?;
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
        .header(SIGNATURE_HEADER, sign_delivery(&secret, timestamp, &body))
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
