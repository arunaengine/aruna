//! Keeps a user's email notification opt-in and mails the chosen notices through one SMTP server.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{EMAIL_KEYSPACE, USER_KEYSPACE};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::Operation;
pub use aruna_core::structs::execution::email::SmtpTls;
use aruna_core::structs::execution::email::{
    EMAIL_CATEGORIES, EmailDeliverySpec, EmailPreference, RealmEmailProfile, email_key,
};
use aruna_core::structs::execution::job::{
    JobError, JobPayload, JobResultPayload, WorkspaceMode, user_dedup_key,
};
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::Effects;
use aruna_core::user::validation::{EMAIL_HASH_ATTRIBUTE, email_hash};
use lettre::message::Mailbox;
use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Address, AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::service::{find_dedup_job, mint_local_job, submit_local_job};
use crate::jobs::submit::SubmitJobSpec;
use crate::node_vault::{delete_secret, parse_secret, read_secret, write_secret};
use crate::realm::get_config::GetConfigOperation;
use crate::realm_vault::{prune_secret_cache, read_realm_secret};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EmailAction {
    Read,
    /// Replaces any opt-in with one for `address` and `categories`.
    OptIn {
        address: String,
        categories: BTreeSet<String>,
    },
    OptOut,
}

#[derive(Debug, Error, PartialEq)]
pub enum EmailError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("choose at least one of these categories: {}", EMAIL_CATEGORIES.join(", "))]
    InvalidCategories,
    #[error("the address is not the verified email address of this account")]
    NotVerified,
    #[error("email operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: {event}")]
    UnexpectedEvent { state: String, event: String },
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Read,
    WriteAddress { previous: Option<Ulid> },
    WriteRecord { previous: Option<Ulid> },
    DeleteRecord { previous: Ulid },
    DeleteAddress,
    Done,
}

/// Reads, sets or removes the caller's own opt-in on this node. An opt-in names only the
/// account's verified address, which is sealed in the node vault.
#[derive(Debug, PartialEq)]
pub struct EmailOperation {
    user_id: UserId,
    action: EmailAction,
    state: State,
    preference: Option<EmailPreference>,
    output: Option<Result<Option<EmailPreference>, EmailError>>,
}

impl EmailOperation {
    pub fn new(user_id: UserId, action: EmailAction) -> Self {
        Self {
            user_id,
            action,
            state: State::Init,
            preference: None,
            output: None,
        }
    }

    fn fail(&mut self, error: EmailError) -> Effects {
        self.state = State::Done;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn done(&mut self) -> Effects {
        self.state = State::Done;
        self.output = Some(Ok(self.preference.take()));
        smallvec![]
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(EmailError::UnexpectedEvent {
            state,
            event: format!("{event:?}"),
        })
    }

    fn handle_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event);
        };
        let [(_, stored), (_, user)] = values.as_slice() else {
            return self.fail(EmailError::NotFinished);
        };
        let existing = match stored
            .as_deref()
            .map(EmailPreference::from_bytes)
            .transpose()
        {
            Ok(existing) => existing,
            Err(error) => return self.fail(error.into()),
        };
        match self.action.clone() {
            EmailAction::Read => {
                self.preference = existing;
                self.done()
            }
            EmailAction::OptOut => match existing {
                None => self.done(),
                Some(existing) => {
                    self.state = State::DeleteRecord {
                        previous: existing.preference_id,
                    };
                    smallvec![Effect::Storage(StorageEffect::Delete {
                        key_space: EMAIL_KEYSPACE.to_string(),
                        key: email_key(&self.user_id).into(),
                        txn_id: None,
                    })]
                }
            },
            EmailAction::OptIn {
                address,
                categories,
            } => {
                let known = categories
                    .iter()
                    .all(|category| EMAIL_CATEGORIES.contains(&category.as_str()));
                if categories.is_empty() || !known {
                    return self.fail(EmailError::InvalidCategories);
                }
                let verified = match user.as_deref().map(User::from_bytes).transpose() {
                    Ok(user) => {
                        user.and_then(|user| user.attributes.get(EMAIL_HASH_ATTRIBUTE).cloned())
                    }
                    Err(error) => return self.fail(error.into()),
                };
                let matches = verified.is_some_and(|hash| hash == email_hash(&address));
                if !matches || address.trim().parse::<Address>().is_err() {
                    return self.fail(EmailError::NotVerified);
                }
                let preference = EmailPreference {
                    preference_id: Ulid::generate(),
                    user_id: self.user_id,
                    categories,
                    created_at_ms: unix_timestamp_millis(),
                };
                let entry = address_entry(preference.preference_id);
                self.preference = Some(preference);
                self.state = State::WriteAddress {
                    previous: existing.map(|existing| existing.preference_id),
                };
                let address = SecretBytes::new(address.trim().as_bytes().to_vec());
                smallvec![write_secret(entry, address, None)]
            }
        }
    }

    fn write_record(&mut self, previous: Option<Ulid>) -> Effects {
        let value = match self.preference.as_ref().map(EmailPreference::to_bytes) {
            Some(Ok(value)) => value,
            Some(Err(error)) => return self.fail(error.into()),
            None => return self.fail(EmailError::NotFinished),
        };
        self.state = State::WriteRecord { previous };
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: EMAIL_KEYSPACE.to_string(),
            key: email_key(&self.user_id).into(),
            value: value.into(),
            txn_id: None,
        })]
    }

    fn drop_address(&mut self, previous: Option<Ulid>) -> Effects {
        match previous {
            None => self.done(),
            Some(previous) => {
                self.state = State::DeleteAddress;
                smallvec![delete_secret(address_entry(previous), None)]
            }
        }
    }
}

impl Operation for EmailOperation {
    type Output = Option<EmailPreference>;
    type Error = EmailError;

    fn start(&mut self) -> Effects {
        self.state = State::Read;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (EMAIL_KEYSPACE.to_string(), email_key(&self.user_id).into()),
                (USER_KEYSPACE.to_string(), self.user_id.to_bytes().into()),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match std::mem::replace(&mut self.state, State::Done) {
            State::Read => self.handle_read(event),
            State::WriteAddress { previous } => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.write_record(previous),
                other => self.unexpected(other),
            },
            State::WriteRecord { previous } => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.drop_address(previous),
                other => self.unexpected(other),
            },
            State::DeleteRecord { previous } => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.drop_address(Some(previous))
                }
                other => self.unexpected(other),
            },
            State::DeleteAddress => match event {
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
        self.output.unwrap_or(Err(EmailError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn address_entry(preference_id: Ulid) -> VaultEntry {
    VaultEntry::new(VaultPurpose::EmailAddress, preference_id)
}

/// Queues one email job per held notice of an opted-in category. The notification id
/// deduplicates; a failure is returned so the sender delivers again.
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
    let mut preferences: HashMap<UserId, Option<EmailPreference>> = HashMap::new();
    for record in records {
        if !EMAIL_CATEGORIES.contains(&record.kind.category()) {
            continue;
        }
        if let Entry::Vacant(slot) = preferences.entry(record.recipient) {
            let preference = drive(
                EmailOperation::new(record.recipient, EmailAction::Read),
                context,
            )
            .await
            .map_err(|error| format!("email opt-in read: {error}"))?;
            slot.insert(preference);
        }
        let Some(preference) = preferences[&record.recipient]
            .as_ref()
            .filter(|preference| preference.wants(record))
        else {
            continue;
        };
        let dedup_key = user_dedup_key(
            record.recipient,
            &format!("email/{}", record.notification_id),
        );
        // A queued job keeps the settings it was queued with, so changed settings never conflict.
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
                    preference_id: preference.preference_id,
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

/// Sends one notification email. A removed or replaced opt-in sends nothing; a permanent
/// SMTP refusal ends the job, and every other failure is retried by the job runtime.
pub async fn run_email_job(ctx: &JobContext, spec: &EmailDeliverySpec) -> JobRunOutcome {
    let result = tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
        () = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
        result = send_email(ctx, spec) => result,
    };
    match result {
        Ok(sent) => JobRunOutcome::Succeeded(JobResultPayload::Email { sent }),
        Err(error) => JobRunOutcome::Failed(error),
    }
}

async fn send_email(ctx: &JobContext, spec: &EmailDeliverySpec) -> Result<bool, JobError> {
    let recipient = spec.notification.recipient;
    let config = drive(GetConfigOperation::new(recipient.realm_id), &ctx.driver)
        .await
        .map_err(|error| JobError::retryable(format!("realm config read: {error}")))?;
    let profile = (config.email.clone())
        .ok_or_else(|| JobError::permanent("email is not configured in this realm"))?;
    let preference = drive(
        EmailOperation::new(recipient, EmailAction::Read),
        &ctx.driver,
    )
    .await
    .map_err(|error| JobError::retryable(error.to_string()))?;
    let Some(preference) = preference.filter(|found| {
        found.preference_id == spec.preference_id && found.wants(&spec.notification)
    }) else {
        return Ok(false);
    };
    let entry = address_entry(preference.preference_id);
    let event = ctx
        .driver
        .storage_handle
        .send_effect(read_secret(entry, None))
        .await;
    let address = parse_secret(event, entry, |bytes| {
        String::from_utf8(bytes.to_vec()).map_err(ConversionError::from)
    })
    .map_err(|error| JobError::retryable(error.to_string()))?
    .ok_or_else(|| JobError::permanent("the opt-in address is missing"))?;
    let mailer = mail_sender(ctx, &config, &profile).await?;
    let (subject, body) = spec.text();
    let mail = OutgoingMail {
        to: address,
        subject,
        body,
        id: spec.notification.notification_id,
    };
    match mailer.send(mail).await {
        Ok(()) => Ok(true),
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
