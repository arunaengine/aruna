//! Writes a user's email opt-in as a register write and mails the chosen notices
//! through one SMTP server.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::realm_vault::seal_setting;
use aruna_core::register::RegisterStamp;
use aruna_core::structs::execution::email::{
    EMAIL_CATEGORIES, EmailDeliverySpec, EmailPreference, RealmEmailProfile,
};
use aruna_core::structs::execution::job::{
    JobError, JobPayload, JobResultPayload, WorkspaceMode, user_dedup_key,
};
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::user::validation::{EMAIL_HASH_ATTRIBUTE, email_hash};
use aruna_core::outbound::{OutgoingMail, SmtpSettings};
use aruna_outbound::Backoffs;
use lettre::Address;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::service::{find_dedup_job, mint_local_job, submit_local_job};
use crate::jobs::submit::SubmitJobSpec;
use crate::notifications::send::{SendError, SendOperation};
use crate::notifications::settings::{
    BACKOFF_START_MS, Delivery, MetadataPeers, SettingError, SettingPeers, SettingWinner,
    delivery_setting, local_setting, managers, write_setting,
};
use crate::realm::get_config::GetConfigOperation;
use crate::realm_vault::{prune_secret_cache, read_realm_secret};
use crate::storage_read::{StorageReadError, parse_storage_read};

/// Whether a mail sender can be built from `settings`: a valid sender address and host.
pub fn check_smtp(settings: &SmtpSettings) -> Result<(), String> {
    aruna_outbound::SmtpMailer::new(settings).map(|_| ())
}

/// The hook every sync result handler runs after applying `targets`: inbound, outbound and
/// restored. A realm config change refreshes the cached mail sender of this node.
pub async fn config_applied(context: &DriverContext, targets: &[DocumentTarget]) {
    let Some(realm_id) = targets.iter().find_map(|target| match target {
        DocumentTarget::RealmConfig { realm_id } => Some(*realm_id),
        _ => None,
    }) else {
        return;
    };
    refresh_mail(context, realm_id).await;
}

/// Runs when the realm config changes: drops the cached sender and passwords it no longer
/// names. A config read error keeps both.
pub async fn refresh_mail(context: &DriverContext, realm_id: RealmId) {
    let config = match drive(GetConfigOperation::new(realm_id), context).await {
        Ok(config) => config,
        Err(error) => {
            warn!(error = %error, "Realm config read failed; cached mail settings stay");
            return;
        }
    };
    if let Some(outbound) = context.outbound_handle.as_ref() {
        outbound.refresh_mail(config.email.as_ref().map(|profile| profile.revision));
    }
    if let Err(error) = prune_secret_cache(context, &config).await {
        warn!(error = %error, "Pruning cached realm secrets failed");
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum EmailError {
    #[error(transparent)]
    Setting(#[from] SettingError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("choose at least one of these categories: {}", EMAIL_CATEGORIES.join(", "))]
    InvalidCategories,
    #[error("the address is not the verified email address of this account")]
    NotVerified,
    #[error("email is not configured in this realm")]
    NotConfigured,
}

/// The caller's current opt-in as this node received it; `None` when email is off.
pub async fn read_email(
    ctx: &DriverContext,
    owner: UserId,
) -> Result<Option<EmailPreference>, EmailError> {
    let bytes = local_setting(ctx, SettingChannel::Email, owner).await?;
    Ok(bytes
        .as_deref()
        .map(EmailPreference::from_bytes)
        .transpose()?)
}

/// Turns on email for `categories` to the account's verified `address`. The address is
/// sealed to the realm key, so only management nodes open it.
pub async fn set_email(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
    address: String,
    categories: BTreeSet<String>,
) -> Result<EmailPreference, EmailError> {
    let config = realm_config(ctx, owner).await?;
    if config.email.is_none() {
        return Err(EmailError::NotConfigured);
    }
    let known = (categories.iter()).all(|category| EMAIL_CATEGORIES.contains(&category.as_str()));
    if categories.is_empty() || !known {
        return Err(EmailError::InvalidCategories);
    }
    let address = address.trim().to_string();
    if !verified(ctx, owner, &address).await? || address.parse::<Address>().is_err() {
        return Err(EmailError::NotVerified);
    }
    let secret = SecretBytes::new(address.into_bytes());
    let build = |stamp: RegisterStamp| {
        let id = stamp.write_id;
        let sealed = seal_setting(&owner.realm_id, SettingChannel::Email, &owner, id, &secret)
            .map_err(|error| SettingError::Build(error.to_string()))?;
        email_bytes(owner, stamp, categories.clone(), Some(sealed))
    };
    write_email(ctx, local, owner, &build).await
}

/// Turns email off with a newer register write; queued emails then send nothing.
pub async fn remove_email(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
) -> Result<(), EmailError> {
    let build = |stamp: RegisterStamp| email_bytes(owner, stamp, BTreeSet::new(), None);
    write_email(ctx, local, owner, &build).await.map(|_| ())
}

async fn write_email(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
    build: &(dyn Fn(RegisterStamp) -> Result<Vec<u8>, SettingError> + Sync),
) -> Result<EmailPreference, EmailError> {
    let bytes = write_setting(ctx, local, SettingChannel::Email, owner, build).await?;
    Ok(EmailPreference::from_bytes(&bytes)?)
}

fn email_bytes(
    owner: UserId,
    stamp: RegisterStamp,
    categories: BTreeSet<String>,
    sealed: Option<Vec<u8>>,
) -> Result<Vec<u8>, SettingError> {
    let preference = EmailPreference {
        preference_id: stamp.write_id,
        user_id: owner,
        stamp,
        categories,
        sealed,
        created_at_ms: unix_timestamp_millis(),
    };
    preference
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

/// Whether `address` is the address the account's sign-in provider verified.
async fn verified(ctx: &DriverContext, owner: UserId, address: &str) -> Result<bool, EmailError> {
    let read = StorageEffect::Read {
        key_space: USER_KEYSPACE.to_string(),
        key: owner.to_bytes().into(),
        txn_id: None,
    };
    let event = ctx.storage_handle.send_effect(Effect::Storage(read)).await;
    let user = match parse_storage_read(event, User::from_bytes) {
        Ok(user) => user,
        Err(StorageReadError::Storage(error)) => return Err(error.into()),
        Err(StorageReadError::Conversion(error)) => return Err(error.into()),
    };
    let hash = user.and_then(|user| user.attributes.get(EMAIL_HASH_ATTRIBUTE).cloned());
    Ok(hash.is_some_and(|hash| hash == email_hash(address)))
}

/// Queues one email job per held notice of an email category, whatever the settings say now;
/// the job decides from the setting when it runs. The notification id deduplicates; a failure
/// is returned so the sender delivers again.
pub async fn enqueue_emails(
    context: &DriverContext,
    owner_node_id: NodeId,
    records: &[NotificationRecord],
) -> Result<(), String> {
    // A realm without a profile queues nothing; a read error queues, and the job retries.
    if let Some(record) = records.first() {
        let config = drive(GetConfigOperation::new(record.recipient.realm_id), context).await;
        if config.is_ok_and(|config| config.email.is_none()) {
            return Ok(());
        }
    }
    for record in records {
        if !EMAIL_CATEGORIES.contains(&record.kind.category()) {
            continue;
        }
        let dedup_key = user_dedup_key(
            record.recipient,
            &format!("email/{}", record.notification_id),
        );
        let queued = find_dedup_job(context, record.recipient, &dedup_key).await;
        if queued
            .map_err(|error| format!("email delivery lookup: {error}"))?
            .is_some()
        {
            continue;
        }
        let realm_id = record.recipient.realm_id;
        let submitted = async {
            let job_id = mint_local_job(context, realm_id, owner_node_id, Some(&dedup_key)).await?;
            let spec = SubmitJobSpec {
                payload: JobPayload::EmailDelivery(EmailDeliverySpec {
                    preference_id: Ulid::nil(),
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
            .map_err(|error| format!("email delivery queue: {error}"))?;
    }
    Ok(())
}

/// Sends one notification email when this node's current setting has email on for its
/// category. A permanent SMTP refusal ends the job; while no management node opens the address,
/// the job waits without an attempt.
pub async fn run_email_job(ctx: &JobContext, spec: &EmailDeliverySpec) -> JobRunOutcome {
    let (Some(_), Some(outbound)) = (&ctx.driver.metadata_handle, &ctx.driver.outbound_handle)
    else {
        return no_transport();
    };
    let local = (ctx.driver.net_handle.as_ref()).map_or(ctx.owner_node_id, |net| net.node_id());
    let peers = MetadataPeers {
        ctx: &ctx.driver,
        local,
    };
    let backoffs = outbound.backoffs();
    tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => JobRunOutcome::Cancelled,
        () = ctx.shutdown.cancelled() => JobRunOutcome::Interrupted,
        outcome = deliver_email(ctx, spec, &peers, backoffs, unix_timestamp_millis()) => outcome,
    }
}

/// The waiting time of a job on a node without a metadata transport, which cannot open secrets.
pub(crate) fn no_transport() -> JobRunOutcome {
    JobRunOutcome::DeferredFor(
        JobError::retryable("the metadata transport is unavailable"),
        Duration::from_millis(BACKOFF_START_MS),
    )
}

pub(crate) async fn deliver_email(
    ctx: &JobContext,
    spec: &EmailDeliverySpec,
    peers: &dyn SettingPeers,
    backoffs: &Backoffs,
    now_ms: u64,
) -> JobRunOutcome {
    let unsent = JobRunOutcome::Succeeded(JobResultPayload::Email { sent: false });
    if now_ms >= spec.notification.expires_at_ms() {
        return unsent;
    }
    let recipient = spec.notification.recipient;
    let config = drive(GetConfigOperation::new(recipient.realm_id), &ctx.driver).await;
    let config = match config {
        Ok(config) if config.email.is_none() => {
            return JobRunOutcome::Failed(JobError::permanent(
                "email is not configured in this realm",
            ));
        }
        Ok(config) => Some(config),
        Err(_) => None,
    };
    let managers = config.as_ref().map(managers);
    let record = &spec.notification;
    let channel = SettingChannel::Email;
    let delivery = delivery_setting(
        &ctx.driver,
        peers,
        backoffs,
        record,
        channel,
        managers.as_deref(),
        now_ms,
    );
    let winner = match delivery.await {
        Delivery::Skip => return unsent,
        Delivery::Wait(error, delay) => return JobRunOutcome::DeferredFor(error, delay),
        Delivery::Send(winner) => winner,
    };
    let (Some(config), Ok(preference)) = (config, EmailPreference::from_bytes(&winner.bytes))
    else {
        return unsent;
    };
    if !preference.wants(record) {
        return unsent;
    }
    match send_email(ctx, spec, &config, winner).await {
        Ok(()) => JobRunOutcome::Succeeded(JobResultPayload::Email { sent: true }),
        Err(error) => JobRunOutcome::Failed(error),
    }
}

async fn send_email(
    ctx: &JobContext,
    spec: &EmailDeliverySpec,
    config: &RealmConfigDocument,
    winner: SettingWinner,
) -> Result<(), JobError> {
    let profile = (config.email.as_ref())
        .ok_or_else(|| JobError::permanent("email is not configured in this realm"))?;
    let secret = winner
        .secret
        .ok_or_else(|| JobError::permanent("the opt-in address is missing"))?;
    let address = String::from_utf8(secret.expose().to_vec())
        .map_err(|_| JobError::permanent("the opt-in address is not UTF-8 text"))?;
    let (subject, body) = spec.text();
    let mail = OutgoingMail {
        to: address,
        subject,
        body,
        id: spec.notification.notification_id,
    };
    let send = SendOperation::mail(profile.revision, None, mail.clone());
    match drive(send, &ctx.driver).await {
        Err(SendError::NoSender) => {}
        result => return result.map(|_| ()).map_err(job_error),
    }
    let settings = smtp_settings(ctx, config, profile).await?;
    let send = SendOperation::mail(profile.revision, Some(settings), mail);
    drive(send, &ctx.driver).await.map(|_| ()).map_err(job_error)
}

/// A send failure as the job runtime takes it.
pub(crate) fn job_error(error: SendError) -> JobError {
    match error {
        SendError::Failed(message) => JobError::retryable(message),
        error => JobError::permanent(error.to_string()),
    }
}

/// The profile with its password opened or fetched, for a sender that is not cached.
async fn smtp_settings(
    ctx: &JobContext,
    config: &RealmConfigDocument,
    profile: &RealmEmailProfile,
) -> Result<SmtpSettings, JobError> {
    let password = match profile.password {
        None => None,
        Some(reference) => {
            let secret = read_realm_secret(&ctx.driver, config, reference)
                .await
                .map_err(|error| JobError::retryable(error.to_string()))?;
            let text = String::from_utf8(secret.expose().to_vec())
                .map_err(|_| JobError::permanent("the SMTP password is not UTF-8 text"))?;
            Some(text)
        }
    };
    Ok(SmtpSettings {
        host: profile.host.clone(),
        port: profile.port,
        tls: profile.tls,
        username: profile.username.clone(),
        password,
        from: profile.from.clone(),
    })
}

#[cfg(test)]
#[path = "email_tests.rs"]
mod tests;
