//! Tests the verified-address opt-in as a register write, email jobs that decide from this
//! node's setting, the realm profile and the deduplication of queued emails.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::jobs::store::query::find_dedup_plan;
use crate::node_vault::{parse_secret, read_secret, write_secret};
use crate::notifications::enqueue::EnqueueDeliveriesOperation;
use crate::notifications::opt_in::{EmailOptIn, SetEmailOperation};
use crate::notifications::setting_rows::SettingError;
use crate::tests::notifications::managers;
use crate::tests::staging::setup_driver_context;
use aruna_core::calls::ChildOutcome;
use aruna_core::compute::SecretBytes;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::egress::EgressPolicy;
use aruna_core::handle::Handle;
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::outbound::{
    MailRequest, OutboundEffect, OutboundError, OutboundEvent, OutgoingMail,
};
use aruna_core::register::RegisterStamp;
use aruna_core::structs::execution::email::{RealmEmailProfile, SmtpTls};
use aruna_core::structs::execution::job::user_dedup_key;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::notification::{NotificationClass, NotificationKind};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::user::validation::{EMAIL_HASH_ATTRIBUTE, email_hash};
use aruna_outbound::{MailBuilder, MailSender, OutboundHandle};
use ed25519_dalek::SigningKey;
use std::sync::{Arc, Mutex};
use ulid::Ulid;

const ADDRESS: &str = "ada@example.org";

async fn read_email(
    context: &DriverContext,
    owner: UserId,
) -> Result<Option<EmailPreference>, EmailError> {
    drive(ReadEmailOperation::new(owner), context).await
}

async fn set_email(
    context: &DriverContext,
    local: NodeId,
    owner: UserId,
    address: String,
    categories: BTreeSet<String>,
) -> Result<EmailPreference, EmailError> {
    let opt_in = EmailOptIn::new(owner, local, address, categories, unix_timestamp_millis());
    drive(SetEmailOperation::new(opt_in), context).await
}

async fn remove_email(
    context: &DriverContext,
    local: NodeId,
    owner: UserId,
) -> Result<(), EmailError> {
    let remove = RemoveEmailOperation::new(owner, local, unix_timestamp_millis());
    drive(remove, context).await
}

async fn refresh_mail(context: &DriverContext, realm_id: RealmId) {
    drive(RefreshMailOperation::new(realm_id), context)
        .await
        .expect("the refresh runs");
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn mail_refreshed() -> IoResult {
    IoResult::Event(Box::new(Event::Outbound(OutboundEvent::MailRefreshed)))
}

/// The refresh names the profile revision to the sender, then prunes the cached secrets.
#[test]
fn refresh_follows_config() {
    let realm_id = user().realm_id;
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.email = Some(profile(7, false));
    let mut refresh = RefreshMailOperation::new(realm_id);
    assert!(matches!(refresh.first(), Ok(Next::Call(IoCall::Child(_)))));
    let revision = config.email.as_ref().map(|profile| profile.revision);
    assert_eq!(
        refresh.next(child::<_, GetConfigError>(Ok(config))),
        Ok(Next::Call(IoCall::from(Effect::Outbound(
            OutboundEffect::RefreshMail(revision)
        ))))
    );
    assert!(matches!(
        refresh.next(mail_refreshed()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let failed = child::<usize, _>(Err(RealmSecretError::NotReplicated));
    assert_eq!(refresh.next(failed), Ok(Next::Done(())));
}

/// A config read error keeps the cache, and only a realm config change starts a refresh.
#[test]
fn refresh_keeps_cache() {
    let mut refresh = RefreshMailOperation::new(user().realm_id);
    refresh.first().unwrap();
    let missing = child::<RealmConfigDocument, _>(Err(GetConfigError::DocumentNotFound));
    assert_eq!(refresh.next(missing), Ok(Next::Done(())));

    let mut other = RefreshMailOperation::applied(&[]);
    assert_eq!(other.first(), Ok(Next::Done(())));
    let target = DocumentTarget::RealmConfig {
        realm_id: user().realm_id,
    };
    assert_eq!(
        RefreshMailOperation::applied(&[target]),
        RefreshMailOperation::new(user().realm_id)
    );
}

fn stored_email(categories: &[&str]) -> EmailPreference {
    let id = Ulid::from_parts(6, 6);
    EmailPreference {
        preference_id: id,
        user_id: user(),
        stamp: RegisterStamp {
            counter: 1,
            node_id: node(),
            write_id: id,
        },
        categories: categories.iter().map(|name| name.to_string()).collect(),
        sealed: (!categories.is_empty()).then(|| vec![1]),
        created_at_ms: 1,
    }
}

#[test]
fn read_decodes_preference() {
    let mut read = ReadEmailOperation::new(user());
    assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
    let stored = stored_email(&["share.upload"]);
    let bytes = stored.to_bytes().unwrap();
    let found = child::<_, SettingError>(Ok(Some(bytes)));
    assert_eq!(read.next(found), Ok(Next::Done(Some(stored))));
    let mut off = ReadEmailOperation::new(user());
    off.first().unwrap();
    let none = child::<Option<Vec<u8>>, SettingError>(Ok(None));
    assert_eq!(off.next(none), Ok(Next::Done(None)));
}

#[test]
fn remove_writes_off() {
    let mut remove = RemoveEmailOperation::new(user(), node(), 5);
    let off = SettingValue::Email {
        categories: BTreeSet::new(),
        address: None,
    };
    assert_eq!(remove.write.value, off);
    assert!(matches!(remove.first(), Ok(Next::Call(IoCall::Child(_)))));
    let bytes = stored_email(&[]).to_bytes().unwrap();
    let written = child::<_, SettingError>(Ok(bytes));
    assert_eq!(remove.next(written), Ok(Next::Done(())));
    let mut refused = RemoveEmailOperation::new(user(), node(), 5);
    refused.first().unwrap();
    let error = || SettingError::Build("sealing failed".to_string());
    let failed = child::<Vec<u8>, _>(Err(error()));
    assert_eq!(refused.next(failed), Err(EmailError::Setting(error())));
}

/// The address is trimmed and checked once, and each opt-in gets its own ids.
#[test]
fn opt_in_trimmed() {
    let categories = BTreeSet::from(["share.upload".to_string()]);
    let opt_in =
        |address: &str| EmailOptIn::new(user(), node(), address.to_string(), categories.clone(), 5);
    let valid = opt_in(" ada@example.org ");
    assert_eq!(valid.address, ADDRESS);
    assert!(valid.address_valid);
    assert_ne!(valid.write_id, opt_in(ADDRESS).write_id);
    assert!(!opt_in("not an address").address_valid);
}

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
    answer: Mutex<Option<OutboundError>>,
}

#[async_trait::async_trait]
impl MailSender for FakeSender {
    async fn send(&self, mail: OutgoingMail) -> Result<(), OutboundError> {
        if let Some(error) = self.answer.lock().unwrap().clone() {
            return Err(error);
        }
        self.sent.lock().unwrap().push(mail);
        Ok(())
    }
}

/// An outbound backend that hands out `sender` and counts how often a sender was built.
fn fake_mail(sender: Arc<FakeSender>) -> (OutboundHandle, Arc<Mutex<Vec<SmtpSettings>>>) {
    let built = Arc::new(Mutex::new(Vec::new()));
    let seen = built.clone();
    let builder: MailBuilder = Arc::new(move |settings: &SmtpSettings| {
        seen.lock().unwrap().push(settings.clone());
        Ok(sender.clone() as Arc<dyn MailSender>)
    });
    let http = reqwest::Client::new();
    let outbound = OutboundHandle::with_mail(http, EgressPolicy::loopback(), builder);
    (outbound, built)
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

/// The stored realm config with a job control binding for this node, so jobs can be placed.
async fn placed_config(context: &DriverContext) -> RealmConfigDocument {
    let mut config = drive(GetConfigOperation::new(user().realm_id), context)
        .await
        .unwrap();
    config.seed_default_placement();
    config.seed_job_control(node(), 0);
    config
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
async fn refresh_drops_stale() {
    // A replicated profile removal clears the cached password and sender without any email job.
    let staging = setup_driver_context().await;
    let context = staging.driver_context.clone();
    let current = profile(1, true);
    seed_profile(&context, Some(current.clone())).await;
    let (outbound, _) = fake_mail(Arc::new(FakeSender::default()));
    let mut context = context;
    context.outbound_handle = Some(outbound.clone());
    let settings = SmtpSettings {
        host: current.host.clone(),
        port: None,
        tls: SmtpTls::Plain,
        username: None,
        password: None,
        from: current.from.clone(),
    };
    let send = |settings: Option<SmtpSettings>| {
        let mail = OutgoingMail {
            to: ADDRESS.to_string(),
            subject: String::new(),
            body: String::new(),
            id: Ulid::from_parts(9, 9),
        };
        let request = MailRequest {
            revision: current.revision,
            settings,
            mail,
        };
        outbound.run(OutboundEffect::Mail(Box::new(request)))
    };
    send(Some(settings)).await;
    refresh_mail(&context, user().realm_id).await;
    let cached = OutboundEvent::MailSent { result: Ok(()) };
    assert_eq!(send(None).await, cached, "the current profile stays");

    seed_profile(&context, None).await;
    refresh_mail(&context, user().realm_id).await;
    let dropped = OutboundEvent::MailSent {
        result: Err(OutboundError::NoSender),
    };
    assert_eq!(send(None).await, dropped);
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
    let config = placed_config(context).await;
    let enqueue = EnqueueDeliveriesOperation::new(node(), Some(&config), &records, NOW_MS);
    drive(enqueue, context).await.unwrap();
    let key = user_dedup_key(user(), &format!("email/{}", records[0].notification_id));
    let queued = find_dedup_plan(&context, user(), &key, None).await;
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
    let config = placed_config(context).await;
    let enqueue = || EnqueueDeliveriesOperation::new(node(), Some(&config), &records, NOW_MS);
    drive(enqueue(), context).await.unwrap();
    let key = |id: Ulid| user_dedup_key(user(), &format!("email/{id}"));
    let first = find_dedup_plan(&context, user(), &key(records[0].notification_id), None)
        .await
        .unwrap()
        .expect("the notice is queued without an opt-in");
    drive(enqueue(), context).await.unwrap();
    let again = find_dedup_plan(&context, user(), &key(records[0].notification_id), None)
        .await
        .unwrap();
    assert_eq!(again.map(|(job_id, _)| job_id), Some(first.0));
    let skipped = find_dedup_plan(&context, user(), &key(records[1].notification_id), None)
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
