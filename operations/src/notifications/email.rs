//! Writes a user's email opt-in as a register write and mails the chosen notices
//! through one SMTP server.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
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
pub use aruna_core::structs::execution::email::SmtpTls;
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
use lettre::message::Mailbox;
use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Address, AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::service::{find_dedup_job, mint_local_job, submit_local_job};
use crate::jobs::submit::SubmitJobSpec;
use crate::notifications::settings::{
    BACKOFF_START_MS, Backoffs, Delivery, MetadataPeers, SettingError, SettingPeers, SettingWinner,
    delivery_setting, local_setting, managers, write_setting,
};
use crate::realm::get_config::GetConfigOperation;
use crate::realm_vault::{prune_secret_cache, read_realm_secret};
use crate::storage_read::{StorageReadError, parse_storage_read};

/// Time one SMTP exchange may take; the job runtime retries a timed out attempt.
const SMTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The realm email profile with its opened password, as `SmtpMailer::new` takes it.
#[derive(Clone, PartialEq, Eq)]
pub struct SmtpSettings {
    pub host: String,
    pub port: Option<u16>,
    pub tls: SmtpTls,
    pub username: Option<String>,
    pub password: Option<String>,
    pub from: String,
}

/// Shows neither the password nor the sender account.
impl std::fmt::Debug for SmtpSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SmtpSettings")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("has_credentials", &self.password.is_some())
            .finish_non_exhaustive()
    }
}

/// One mail; the recipient is never shown by `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct OutgoingMail {
    pub to: String,
    pub subject: String,
    pub body: String,
    /// Stays the same for every retry and becomes the `Message-ID`.
    pub id: Ulid,
}

impl std::fmt::Debug for OutgoingMail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutgoingMail")
            .field("subject", &self.subject)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// A send failure; its text never holds an address or a credential.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum MailError {
    #[error("the mail server refused the mail: {0}")]
    Permanent(String),
    #[error("the mail could not be sent now: {0}")]
    Retryable(String),
}

#[async_trait::async_trait]
pub trait MailSender: Send + Sync {
    async fn send(&self, mail: OutgoingMail) -> Result<(), MailError>;
}

pub struct SmtpMailer {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
}

impl SmtpMailer {
    pub fn new(settings: &SmtpSettings) -> Result<Self, String> {
        let from: Mailbox = settings
            .from
            .parse()
            .map_err(|_| "the SMTP sender address is invalid".to_string())?;
        let host = settings.host.as_str();
        let mut builder = match settings.tls {
            SmtpTls::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host),
            SmtpTls::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(host),
            SmtpTls::Plain => Ok(AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(
                host,
            )),
        }
        .map_err(|_| "the SMTP host is invalid".to_string())?
        .timeout(Some(SMTP_TIMEOUT));
        if let Some(port) = settings.port {
            builder = builder.port(port);
        }
        if let (Some(username), Some(password)) = (&settings.username, &settings.password) {
            builder = builder.credentials(Credentials::new(username.clone(), password.clone()));
        }
        Ok(Self {
            transport: builder.build(),
            from,
        })
    }
}

#[async_trait::async_trait]
impl MailSender for SmtpMailer {
    async fn send(&self, mail: OutgoingMail) -> Result<(), MailError> {
        let to: Mailbox = mail
            .to
            .parse()
            .map_err(|_| MailError::Permanent("the stored address is invalid".to_string()))?;
        let domain = self.from.email.domain();
        let message = Message::builder()
            .from(self.from.clone())
            .to(to)
            .subject(mail.subject)
            .message_id(Some(format!("<{}.notification@{domain}>", mail.id)))
            .header(ContentType::TEXT_PLAIN)
            .body(mail.body)
            .map_err(|_| MailError::Permanent("the mail could not be built".to_string()))?;
        match self.transport.send(message).await {
            Ok(_) => Ok(()),
            // Server answers may echo the recipient, so only the status class is kept.
            Err(error) if error.is_permanent() => Err(MailError::Permanent(status(&error))),
            Err(error) => Err(MailError::Retryable(status(&error))),
        }
    }
}

/// Builds the sender of one profile; tests swap in a fake one.
pub type MailBuilder =
    Arc<dyn Fn(&SmtpSettings) -> Result<Arc<dyn MailSender>, String> + Send + Sync>;

/// The sender of the current realm email profile, built once per profile revision.
pub struct MailProfiles {
    cache: Mutex<Option<(Ulid, Arc<dyn MailSender>)>>,
    builder: MailBuilder,
}

impl MailProfiles {
    pub fn new(builder: MailBuilder) -> Self {
        Self {
            cache: Mutex::new(None),
            builder,
        }
    }

    /// The sender built for `revision`, if it is still cached.
    pub fn cached(&self, revision: Ulid) -> Option<Arc<dyn MailSender>> {
        let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        (cache.as_ref())
            .filter(|(cached, _)| *cached == revision)
            .map(|(_, sender)| sender.clone())
    }

    /// Builds and caches the sender of `revision`, replacing any other one.
    pub fn build(
        &self,
        revision: Ulid,
        settings: &SmtpSettings,
    ) -> Result<Arc<dyn MailSender>, String> {
        let sender = (self.builder)(settings)?;
        *self.cache.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((revision, sender.clone()));
        Ok(sender)
    }

    /// Drops the cached sender unless it belongs to the `current` profile.
    pub fn refresh(&self, current: Option<&RealmEmailProfile>) {
        let mut cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if cache.as_ref().map(|(revision, _)| *revision) != current.map(|profile| profile.revision)
        {
            *cache = None;
        }
    }
}

impl std::fmt::Debug for MailProfiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        let revision = cache.as_ref().map(|(revision, _)| *revision);
        f.debug_struct("MailProfiles")
            .field("revision", &revision)
            .finish_non_exhaustive()
    }
}

impl Default for MailProfiles {
    fn default() -> Self {
        Self::new(Arc::new(|settings| {
            Ok(Arc::new(SmtpMailer::new(settings)?) as Arc<dyn MailSender>)
        }))
    }
}

/// The hook every sync result handler runs after applying `targets`: inbound, outbound and
/// restored. A realm config change refreshes the senders this node's metadata handle shares.
pub async fn config_applied(context: &DriverContext, targets: &[DocumentTarget]) {
    let Some(realm_id) = targets.iter().find_map(|target| match target {
        DocumentTarget::RealmConfig { realm_id } => Some(*realm_id),
        _ => None,
    }) else {
        return;
    };
    let mail = (context.metadata_handle.as_ref()).and_then(|metadata| metadata.mail());
    refresh_mail(context, realm_id, mail.as_deref()).await;
}

/// Runs when the realm config changes: drops cached senders and passwords it no longer names.
/// A config read error keeps both.
pub async fn refresh_mail(context: &DriverContext, realm_id: RealmId, mail: Option<&MailProfiles>) {
    let config = match drive(GetConfigOperation::new(realm_id), context).await {
        Ok(config) => config,
        Err(error) => {
            warn!(error = %error, "Realm config read failed; cached mail settings stay");
            return;
        }
    };
    if let Some(mail) = mail {
        mail.refresh(config.email.as_ref());
    }
    if let Err(error) = prune_secret_cache(context, &config).await {
        warn!(error = %error, "Pruning cached realm secrets failed");
    }
}

fn status(error: &lettre::transport::smtp::Error) -> String {
    match error.status() {
        Some(code) => format!("SMTP status {code}"),
        None if error.is_timeout() => "SMTP timeout".to_string(),
        None => "SMTP connection failed".to_string(),
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
    let Some(metadata) = ctx.driver.metadata_handle.as_ref() else {
        return no_transport();
    };
    let local = (ctx.driver.net_handle.as_ref()).map_or(ctx.owner_node_id, |net| net.node_id());
    let peers = MetadataPeers {
        ctx: &ctx.driver,
        local,
    };
    let backoffs = metadata.setting_backoffs();
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
    let profile = (config.email.clone())
        .ok_or_else(|| JobError::permanent("email is not configured in this realm"))?;
    let secret = winner
        .secret
        .ok_or_else(|| JobError::permanent("the opt-in address is missing"))?;
    let address = String::from_utf8(secret.expose().to_vec())
        .map_err(|_| JobError::permanent("the opt-in address is not UTF-8 text"))?;
    let mailer = mail_sender(ctx, config, &profile).await?;
    let (subject, body) = spec.text();
    let mail = OutgoingMail {
        to: address,
        subject,
        body,
        id: spec.notification.notification_id,
    };
    match mailer.send(mail).await {
        Ok(()) => Ok(()),
        Err(MailError::Permanent(message)) => Err(JobError::permanent(message)),
        Err(MailError::Retryable(message)) => Err(JobError::retryable(message)),
    }
}

/// The cached sender of this profile revision, or a new one with the password opened or fetched.
async fn mail_sender(
    ctx: &JobContext,
    config: &RealmConfigDocument,
    profile: &RealmEmailProfile,
) -> Result<Arc<dyn MailSender>, JobError> {
    if let Some(sender) = ctx.mail.cached(profile.revision) {
        return Ok(sender);
    }
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
    let settings = SmtpSettings {
        host: profile.host.clone(),
        port: profile.port,
        tls: profile.tls,
        username: profile.username.clone(),
        password,
        from: profile.from.clone(),
    };
    (ctx.mail)
        .build(profile.revision, &settings)
        .map_err(JobError::permanent)
}

#[cfg(test)]
#[path = "email_tests.rs"]
mod tests;
