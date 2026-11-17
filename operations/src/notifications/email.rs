//! Writes a user's email opt-in as a register write and mails the chosen notices
//! through one SMTP server.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::IoError;
use aruna_core::compute::SecretBytes;
use aruna_core::document::DocumentTarget;
use aruna_core::outbound::SmtpSettings;
use aruna_core::structs::execution::email::{EmailDeliverySpec, EmailPreference};
use aruna_core::structs::execution::job::JobError;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_millis;
use tracing::warn;
use ulid::Ulid;

use crate::document_repository::GetConfigOperation;
use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::notifications::email_job::EmailJobOperation;
use crate::notifications::opt_in::{EmailError, EmailOptIn, SetEmailOperation};
use crate::notifications::setting_rows::{
    ReadSettingOperation, SettingValue, SettingWrite, WriteSettingOperation,
};
use crate::notifications::settings::{BACKOFF_START_MS, DeliveryOutcome};
use crate::realm_vault::PruneCacheOperation;

/// Whether a mail sender can be built from `settings`: a valid sender address and host.
pub fn check_smtp(settings: &SmtpSettings) -> Result<(), String> {
    aruna_outbound::SmtpMailer::new(settings).map(|_| ())
}

/// The hook every sync result handler runs after applying `targets`: inbound, outbound and
/// restored. A realm config change refreshes the cached mail sender of this node.
pub(crate) async fn config_applied(context: &DriverContext, targets: &[DocumentTarget]) {
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
    if let Err(error) = drive(PruneCacheOperation::new(&config), context).await {
        warn!(error = %error, "Pruning cached realm secrets failed");
    }
}

/// The caller's current opt-in as this node received it; `None` when email is off.
pub async fn read_email(
    ctx: &DriverContext,
    owner: UserId,
) -> Result<Option<EmailPreference>, EmailError> {
    let bytes = drive(ReadSettingOperation::new(SettingChannel::Email, owner), ctx).await?;
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
    let address = address.trim().to_string();
    let opt_in = EmailOptIn {
        owner,
        local,
        address_valid: aruna_outbound::valid_address(&address),
        address,
        categories,
        write_id: Ulid::generate(),
        event_id: Ulid::generate(),
        now_ms: unix_timestamp_millis(),
    };
    drive(SetEmailOperation::new(opt_in), ctx).await
}

/// Turns email off with a newer register write; queued emails then send nothing.
pub async fn remove_email(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
) -> Result<(), EmailError> {
    let off = write_email(ctx, local, owner, BTreeSet::new(), None);
    off.await.map(|_| ())
}

async fn write_email(
    ctx: &DriverContext,
    local: NodeId,
    owner: UserId,
    categories: BTreeSet<String>,
    address: Option<SecretBytes>,
) -> Result<EmailPreference, EmailError> {
    let write = SettingWrite {
        owner,
        local,
        write_id: Ulid::generate(),
        event_id: Ulid::generate(),
        now_ms: unix_timestamp_millis(),
        value: SettingValue::Email {
            categories,
            address,
        },
    };
    let bytes = drive(WriteSettingOperation::new(write), ctx).await?;
    Ok(EmailPreference::from_bytes(&bytes)?)
}

/// Runs the email job of `spec` until the job is cancelled or the node shuts down.
pub(crate) async fn run_email_job(ctx: &JobContext, spec: &EmailDeliverySpec) -> JobRunOutcome {
    if ctx.driver.metadata_handle.is_none() || ctx.driver.outbound_handle.is_none() {
        return no_transport();
    }
    let local = (ctx.driver.net_handle.as_ref()).map_or(ctx.owner_node_id, |net| net.node_id());
    let job = EmailJobOperation::new(spec.clone(), local, unix_timestamp_millis());
    tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => JobRunOutcome::Cancelled,
        () = ctx.shutdown.cancelled() => JobRunOutcome::Interrupted,
        outcome = drive(job, &ctx.driver) => job_outcome(outcome),
    }
}

/// The waiting time of a job on a node without a metadata transport, which cannot open secrets.
pub(crate) fn no_transport() -> JobRunOutcome {
    JobRunOutcome::DeferredFor(
        JobError::retryable("the metadata transport is unavailable"),
        Duration::from_millis(BACKOFF_START_MS),
    )
}

/// A delivery job's outcome as the job runtime takes it; a failed run may pass on a retry.
pub(crate) fn job_outcome(outcome: Result<DeliveryOutcome, IoError>) -> JobRunOutcome {
    match outcome {
        Ok(DeliveryOutcome::Succeeded(payload)) => JobRunOutcome::Succeeded(payload),
        Ok(DeliveryOutcome::Failed(error)) => JobRunOutcome::Failed(error),
        Ok(DeliveryOutcome::DeferredFor(error, delay)) => JobRunOutcome::DeferredFor(error, delay),
        Err(error) => JobRunOutcome::Failed(JobError::retryable(error.to_string())),
    }
}

#[cfg(test)]
#[path = "email_tests.rs"]
mod tests;
