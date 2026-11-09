//! Tests the verified-address opt-in as a register write, email jobs that decide from this
//! node's setting, the realm profile and the deduplication of queued emails.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::executor::ProgressReporter;
use crate::jobs::store::find_dedup_plan;
use crate::jobs::submit::mint_job_id;
use crate::node_vault::{parse_secret, read_secret, write_secret};
use crate::tests::notifications::{CannedPeers, managers, opening, store_setting};
use crate::tests::staging::setup_driver_context;
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::structs::execution::job::{JobErrorKind, JobProgress};
use aruna_core::structs::execution::notification::{NotificationClass, NotificationKind};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use ed25519_dalek::SigningKey;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;

const ADDRESS: &str = "ada@example.org";

/// A user of a realm whose id is a realm key, so addresses can be sealed to it.
fn user() -> UserId {
    let realm_key = SigningKey::from_bytes(&[5; 32]).verifying_key();
    UserId::local(
        Ulid::from_parts(1, 1),
        RealmId::from_bytes(realm_key.to_bytes()),
    )
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[8; 32]).public()
}

async fn seed_user(context: &DriverContext) {
    let user = User {
        user_id: user(),
        name: "Ada".to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: [(EMAIL_HASH_ATTRIBUTE.to_string(), email_hash(ADDRESS))].into(),
    };
    let actor = Actor {
        node_id: node(),
        user_id: user.user_id,
        realm_id: user.user_id.realm_id,
    };
    let write = StorageEffect::Write {
        key_space: USER_KEYSPACE.to_string(),
        key: user.user_id.to_bytes().into(),
        value: user.to_bytes(&actor).unwrap().into(),
        txn_id: None,
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(write))
        .await;
}

fn notice(kind: NotificationKind) -> NotificationRecord {
    NotificationRecord {
        notification_id: Ulid::from_parts(7, 7),
        recipient: user(),
        class: NotificationClass::Direct,
        kind,
        created_at_ms: 42,
        read_at_ms: None,
        watch_authorization: None,
    }
}

fn share_upload() -> NotificationRecord {
    notice(NotificationKind::ShareUpload {
        share_id: Ulid::from_parts(4, 4),
        node_id: node(),
        group_id: Ulid::from_parts(5, 5),
        bucket: "inbox".to_string(),
        key: "a.csv".to_string(),
        size_bytes: 9,
    })
}

#[derive(Default)]
struct FakeSender {
    sent: Mutex<Vec<OutgoingMail>>,
    answer: Mutex<Option<MailError>>,
}

#[async_trait::async_trait]
impl MailSender for FakeSender {
    async fn send(&self, mail: OutgoingMail) -> Result<(), MailError> {
        if let Some(error) = self.answer.lock().unwrap().clone() {
            return Err(error);
        }
        self.sent.lock().unwrap().push(mail);
        Ok(())
    }
}

/// Mail profiles that hand out `sender` and count how often a sender was built.
fn fake_mail(sender: Arc<FakeSender>) -> (Arc<MailProfiles>, Arc<Mutex<Vec<SmtpSettings>>>) {
    let built = Arc::new(Mutex::new(Vec::new()));
    let seen = built.clone();
    let builder: MailBuilder = Arc::new(move |settings: &SmtpSettings| {
        seen.lock().unwrap().push(settings.clone());
        Ok(sender.clone() as Arc<dyn MailSender>)
    });
    (Arc::new(MailProfiles::new(builder)), built)
}

const PASSWORD: &str = "canary-smtp-31";

/// Stores a realm config with three managers and `profile`; a password is sealed and already
/// cached on this node.
async fn seed_profile(context: &DriverContext, profile: Option<RealmEmailProfile>) {
    use aruna_core::document::DocumentTarget;
    use aruna_core::realm_vault::{RealmVaultKey, RealmVaultRecord};
    let realm_id = user().realm_id;
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    for manager in managers() {
        config.ensure_node(manager, RealmNodeKind::Management);
    }
    if let Some(reference) = profile.as_ref().and_then(|profile| profile.password) {
        let key = RealmVaultKey::derive(&ed25519_dalek::SigningKey::from_bytes(&[5; 32]));
        let secret = SecretBytes::new(PASSWORD.as_bytes().to_vec());
        let sealed = key
            .seal(&realm_id, reference.entry, reference.revision, &secret)
            .unwrap();
        config.vault.push(RealmVaultRecord {
            entry: reference.entry,
            revision: reference.revision,
            sealed,
            changed_by: user(),
            changed_at_ms: 1,
        });
        let cached = [
            &[reference.entry.purpose.tag()][..],
            &reference.revision.to_bytes(),
            PASSWORD.as_bytes(),
        ];
        let row = VaultEntry::new(VaultPurpose::RealmSecret, reference.entry.id);
        let write = write_secret(row, SecretBytes::new(cached.concat()), None);
        context.storage_handle.send_effect(write).await;
    }
    config.email = profile;
    let actor = Actor {
        node_id: node(),
        user_id: user(),
        realm_id,
    };
    let target = DocumentTarget::RealmConfig { realm_id };
    let write = StorageEffect::Write {
        key_space: target.storage_keyspace().to_string(),
        key: target.storage_key(),
        value: config.to_bytes(&actor).unwrap().into(),
        txn_id: None,
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(write))
        .await;
}

fn profile(revision: u128, password: bool) -> RealmEmailProfile {
    use aruna_core::realm_vault::{RealmSecretRef, RealmVaultEntry, RealmVaultPurpose};
    let entry = RealmVaultEntry::new(RealmVaultPurpose::SmtpPassword, Ulid::from_parts(3, 3));
    RealmEmailProfile {
        revision: Ulid::from_parts(9, revision),
        host: "smtp.example.org".to_string(),
        port: Some(587),
        tls: SmtpTls::Starttls,
        username: password.then(|| "aruna".to_string()),
        from: "aruna@example.org".to_string(),
        password: password.then(|| RealmSecretRef {
            entry,
            revision: Ulid::from_parts(4, revision),
        }),
        changed_by: user(),
        changed_at_ms: 1,
    }
}

fn job_context(context: DriverContext, mail: Arc<MailProfiles>) -> JobContext {
    JobContext {
        driver: Arc::new(context),
        job_id: mint_job_id(
            PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            BucketId::new(0).unwrap(),
        )
        .unwrap(),
        owner_node_id: node(),
        claim_token: Ulid::generate(),
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&JobProgress::new("steps")),
        range_turns: Default::default(),
        mail,
    }
}

/// Stores an email setting of `user()` with `counter`, as a local write or sync would; no
/// category turns email off.
async fn seed_setting(context: &DriverContext, counter: u64, categories: &[&str]) {
    let id = Ulid::generate();
    let on = !categories.is_empty();
    let preference = EmailPreference {
        preference_id: id,
        user_id: user(),
        stamp: RegisterStamp {
            counter,
            node_id: node(),
            write_id: id,
        },
        categories: categories
            .iter()
            .map(|category| category.to_string())
            .collect(),
        sealed: on.then(|| vec![1]),
        created_at_ms: 1,
    };
    let target = SettingChannel::Email.target(user(), id);
    store_setting(
        &context.storage_handle,
        &target,
        preference.to_bytes().unwrap(),
    )
    .await;
}

fn spec() -> EmailDeliverySpec {
    EmailDeliverySpec {
        preference_id: Ulid::nil(),
        notification: share_upload(),
    }
}

/// A time before the notice expires.
const NOW_MS: u64 = 1_000;

#[tokio::test]
async fn requires_verified_address() {
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    let set = |categories: &[&str], address: &str| {
        let categories = categories.iter().map(|category| category.to_string());
        set_email(
            context,
            node(),
            user(),
            address.to_string(),
            categories.collect(),
        )
    };
    let unreadable = set(&["share.upload"], ADDRESS).await;
    assert!(matches!(
        unreadable,
        Err(EmailError::Setting(SettingError::Config(_)))
    ));
    seed_profile(context, None).await;
    let unconfigured = set(&["share.upload"], ADDRESS).await;
    assert_eq!(unconfigured, Err(EmailError::NotConfigured));
    seed_profile(context, Some(profile(1, false))).await;
    seed_user(context).await;
    let other = set(&["share.upload"], "eve@example.org").await;
    assert_eq!(other, Err(EmailError::NotVerified));
    let unknown = set(&["resource.watch"], ADDRESS).await;
    assert_eq!(unknown, Err(EmailError::InvalidCategories));
    let empty = set(&[], ADDRESS).await;
    assert_eq!(empty, Err(EmailError::InvalidCategories));

    // A valid opt-in is one local register write, without any management node.
    let stored = set(&["share.upload"], ADDRESS).await.unwrap();
    assert_eq!((stored.stamp.counter, stored.stamp.node_id), (1, node()));
    let sealed = stored.sealed.clone().unwrap();
    assert!(
        !sealed
            .windows(ADDRESS.len())
            .any(|part| part == ADDRESS.as_bytes())
    );
    assert_eq!(read_email(context, user()).await.unwrap(), Some(stored));
    remove_email(context, node(), user()).await.unwrap();
    assert_eq!(read_email(context, user()).await.unwrap(), None);
}

#[tokio::test]
async fn on_setting_sends() {
    let staging = setup_driver_context().await;
    let context = staging.driver_context.clone();
    let sender = Arc::new(FakeSender::default());
    let (mail, built) = fake_mail(sender.clone());
    let ctx = job_context(context.clone(), mail);
    seed_profile(&context, Some(profile(1, true))).await;
    seed_setting(&context, 1, &["share.upload"]).await;
    let peers = opening(ADDRESS.as_bytes());
    let backoffs = Backoffs::default();
    let spec = spec();
    let run = || deliver_email(&ctx, &spec, &peers, &backoffs, NOW_MS);

    let result = run().await;
    assert!(matches!(
        result,
        JobRunOutcome::Succeeded(JobResultPayload::Email { sent: true })
    ));
    assert_eq!(
        peers.opens.load(Ordering::SeqCst),
        1,
        "one manager opens the address"
    );
    let mail = sender.sent.lock().unwrap()[0].clone();
    assert_eq!(
        (mail.to.as_str(), mail.id),
        (ADDRESS, Ulid::from_parts(7, 7))
    );
    assert!(
        !format!("{mail:?}").contains(ADDRESS),
        "Debug hides the recipient"
    );
    let settings = built.lock().unwrap()[0].clone();
    assert_eq!(settings.password.as_deref(), Some(PASSWORD));
    assert!(!format!("{settings:?}").contains(PASSWORD));

    *sender.answer.lock().unwrap() = Some(MailError::Retryable("SMTP status 451".to_string()));
    let JobRunOutcome::Failed(error) = run().await else {
        panic!("a transient refusal fails the attempt");
    };
    assert_eq!(error.kind, JobErrorKind::Retryable);
    *sender.answer.lock().unwrap() = Some(MailError::Permanent("SMTP status 550".to_string()));
    let JobRunOutcome::Failed(error) = run().await else {
        panic!("a permanent refusal ends the job");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
    assert_eq!(
        built.lock().unwrap().len(),
        1,
        "one revision builds one sender"
    );

    seed_profile(&context, None).await;
    let JobRunOutcome::Failed(error) = run().await else {
        panic!("a realm without a profile sends nothing");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

#[tokio::test]
async fn off_setting_skips() {
    let staging = setup_driver_context().await;
    let context = staging.driver_context.clone();
    let sender = Arc::new(FakeSender::default());
    let (mail, _) = fake_mail(sender.clone());
    let ctx = job_context(context.clone(), mail);
    seed_profile(&context, Some(profile(1, false))).await;
    let peers = opening(ADDRESS.as_bytes());
    let unsent = |outcome| {
        matches!(
            outcome,
            JobRunOutcome::Succeeded(JobResultPayload::Email { sent: false })
        )
    };
    let none = deliver_email(&ctx, &spec(), &peers, &Backoffs::default(), NOW_MS).await;
    assert!(unsent(none), "no setting means off");
    seed_setting(&context, 1, &["share.upload"]).await;
    seed_setting(&context, 2, &[]).await;
    let off = deliver_email(&ctx, &spec(), &peers, &Backoffs::default(), NOW_MS).await;
    assert!(unsent(off), "the newer opt-out decides");
    assert_eq!(peers.opens.load(Ordering::SeqCst), 0);
    assert!(sender.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn category_excluded_skips() {
    let staging = setup_driver_context().await;
    let context = staging.driver_context.clone();
    let sender = Arc::new(FakeSender::default());
    let (mail, _) = fake_mail(sender.clone());
    let ctx = job_context(context.clone(), mail);
    seed_profile(&context, Some(profile(1, false))).await;
    seed_setting(&context, 1, &["group.membership"]).await;
    let peers = opening(ADDRESS.as_bytes());
    let result = deliver_email(&ctx, &spec(), &peers, &Backoffs::default(), NOW_MS).await;
    assert!(matches!(
        result,
        JobRunOutcome::Succeeded(JobResultPayload::Email { sent: false })
    ));
    assert!(sender.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn expired_notice_finishes() {
    let staging = setup_driver_context().await;
    let context = staging.driver_context.clone();
    let (mail, _) = fake_mail(Arc::new(FakeSender::default()));
    let ctx = job_context(context.clone(), mail);
    seed_profile(&context, Some(profile(1, false))).await;
    seed_setting(&context, 1, &["share.upload"]).await;
    let peers = opening(ADDRESS.as_bytes());
    let expired = spec().notification.expires_at_ms();
    let result = deliver_email(&ctx, &spec(), &peers, &Backoffs::default(), expired).await;
    assert!(matches!(
        result,
        JobRunOutcome::Succeeded(JobResultPayload::Email { sent: false })
    ));
    assert_eq!(
        peers.opens.load(Ordering::SeqCst),
        0,
        "an expired notice opens nothing"
    );
}

#[tokio::test]
async fn no_manager_defers() {
    // No readable realm config, or no management node that opens the address, waits without
    // spending an attempt.
    let staging = setup_driver_context().await;
    let context = staging.driver_context.clone();
    let (mail, built) = fake_mail(Arc::new(FakeSender::default()));
    let ctx = job_context(context.clone(), mail);
    seed_setting(&context, 1, &["share.upload"]).await;
    let backoffs = Backoffs::default();
    let peers = CannedPeers::default();
    let unreadable = deliver_email(&ctx, &spec(), &peers, &backoffs, NOW_MS).await;
    let JobRunOutcome::DeferredFor(error, delay) = unreadable else {
        panic!("an unreadable config defers");
    };
    assert_eq!(
        (error.kind, delay),
        (JobErrorKind::Retryable, Duration::from_secs(30))
    );

    seed_profile(&context, Some(profile(1, false))).await;
    let waiting = deliver_email(&ctx, &spec(), &peers, &backoffs, NOW_MS + 10_000).await;
    let JobRunOutcome::DeferredFor(_, delay) = waiting else {
        panic!("a running backoff defers");
    };
    assert_eq!(
        delay,
        Duration::from_secs(20),
        "the backoff runs to its end"
    );
    assert_eq!(
        peers.opens.load(Ordering::SeqCst),
        0,
        "a running backoff asks nobody"
    );
    let short = deliver_email(&ctx, &spec(), &peers, &backoffs, NOW_MS + 30_000).await;
    let JobRunOutcome::DeferredFor(_, delay) = short else {
        panic!("no manager opens the address");
    };
    assert_eq!(delay, Duration::from_secs(60), "the next delay doubles");
    assert_eq!(
        peers.opens.load(Ordering::SeqCst),
        3,
        "every manager was asked once"
    );
    assert!(built.lock().unwrap().is_empty());
}

#[tokio::test]
async fn refresh_drops_stale() {
    // A replicated profile removal clears the cached password and sender without any email job.
    let staging = setup_driver_context().await;
    let context = staging.driver_context.clone();
    let current = profile(1, true);
    seed_profile(&context, Some(current.clone())).await;
    let (mail, _) = fake_mail(Arc::new(FakeSender::default()));
    let settings = SmtpSettings {
        host: current.host.clone(),
        port: None,
        tls: SmtpTls::Plain,
        username: None,
        password: None,
        from: current.from.clone(),
    };
    mail.build(current.revision, &settings).unwrap();
    refresh_mail(&context, user().realm_id, Some(&mail)).await;
    assert!(
        mail.cached(current.revision).is_some(),
        "the current profile stays"
    );

    seed_profile(&context, None).await;
    refresh_mail(&context, user().realm_id, Some(&mail)).await;
    assert!(mail.cached(current.revision).is_none());
    let row = VaultEntry::new(VaultPurpose::RealmSecret, Ulid::from_parts(3, 3));
    let event = context
        .storage_handle
        .send_effect(read_secret(row, None))
        .await;
    let cached = parse_secret(event, row, |bytes| Ok(bytes.to_vec())).unwrap();
    assert_eq!(cached, None);
}

#[tokio::test]
async fn skips_without_profile() {
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    seed_profile(context, None).await;
    let records = [share_upload()];
    enqueue_emails(context, node(), &records).await.unwrap();
    let key = user_dedup_key(user(), &format!("email/{}", records[0].notification_id));
    let queued = find_dedup_plan(&context.storage_handle, user(), &key, None).await;
    assert_eq!(
        queued.unwrap(),
        None,
        "a realm without a profile queues nothing"
    );
}

#[tokio::test]
async fn enqueue_always_queues() {
    // No opt-in is read at enqueue: each email-category notice gets one job, once.
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    seed_profile(context, Some(profile(1, false))).await;
    let mut watched = notice(NotificationKind::BucketDeleted {
        bucket: "inbox".to_string(),
        node_id: node(),
        group_id: Ulid::from_parts(5, 5),
    });
    watched.notification_id = Ulid::from_parts(8, 8);
    let records = [share_upload(), watched];
    enqueue_emails(context, node(), &records).await.unwrap();
    let key = |id: Ulid| user_dedup_key(user(), &format!("email/{id}"));
    let storage = &context.storage_handle;
    let first = find_dedup_plan(storage, user(), &key(records[0].notification_id), None)
        .await
        .unwrap()
        .expect("the notice is queued without an opt-in");
    enqueue_emails(context, node(), &records).await.unwrap();
    let again = find_dedup_plan(storage, user(), &key(records[0].notification_id), None)
        .await
        .unwrap();
    assert_eq!(again.map(|(job_id, _)| job_id), Some(first.0));
    let skipped = find_dedup_plan(storage, user(), &key(records[1].notification_id), None)
        .await
        .unwrap();
    assert_eq!(
        skipped, None,
        "a notice outside every email category queues nothing"
    );
}

#[tokio::test]
async fn restore_prunes_password() {
    // A deletion that arrives through restart recovery clears the cached password too.
    use aruna_core::document::{DocumentNetEvent, DocumentTarget};
    let staging = setup_driver_context().await;
    let context = Arc::new(staging.driver_context.clone());
    seed_profile(&context, Some(profile(1, true))).await;
    seed_profile(&context, None).await;
    let applied = DocumentNetEvent::DocumentsReconciled {
        applied: 1,
        targets: vec![DocumentTarget::RealmConfig {
            realm_id: user().realm_id,
        }],
        metadata_create_events: Vec::new(),
        metadata_graph_tombstones: Vec::new(),
    };
    assert!(crate::node::startup::apply_restored_reconcile(&context, node(), applied).await);
    let row = VaultEntry::new(VaultPurpose::RealmSecret, Ulid::from_parts(3, 3));
    let event = context
        .storage_handle
        .send_effect(read_secret(row, None))
        .await;
    let cached = parse_secret(event, row, |bytes| Ok(bytes.to_vec())).unwrap();
    assert!(cached.is_none(), "the deleted password is no longer cached");
}
