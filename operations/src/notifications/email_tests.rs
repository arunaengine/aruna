//! Tests the verified-address opt-in, sealed addresses, email jobs and their deduplication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::executor::ProgressReporter;
use crate::jobs::store::find_dedup_plan;
use crate::jobs::submit::mint_job_id;
use crate::tests::staging::setup_driver_context;
use aruna_core::structs::execution::job::{JobErrorKind, JobProgress};
use aruna_core::structs::execution::notification::{NotificationClass, NotificationKind};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

const ADDRESS: &str = "ada@example.org";

fn user() -> UserId {
    UserId::local(Ulid::from_parts(1, 1), RealmId::from_bytes([1; 32]))
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

fn opt_in(address: &str, categories: &[&str]) -> EmailAction {
    EmailAction::OptIn {
        address: address.to_string(),
        categories: categories
            .iter()
            .map(|category| category.to_string())
            .collect(),
    }
}

async fn act(
    context: &DriverContext,
    action: EmailAction,
) -> Result<Option<EmailPreference>, EmailError> {
    drive(EmailOperation::new(user(), action), context).await
}

async fn sealed(context: &DriverContext, preference_id: Ulid) -> Option<String> {
    let entry = address_entry(preference_id);
    let event = context
        .storage_handle
        .send_effect(read_secret(entry, None))
        .await;
    parse_secret(event, entry, |bytes| {
        String::from_utf8(bytes.to_vec()).map_err(ConversionError::from)
    })
    .unwrap()
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

fn job_context(context: DriverContext, mailer: Option<Arc<dyn MailSender>>) -> JobContext {
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
        mailer,
    }
}

#[tokio::test]
async fn requires_verified_address() {
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    seed_user(context).await;
    let other = act(context, opt_in("eve@example.org", &["share.upload"])).await;
    assert_eq!(other, Err(EmailError::NotVerified));
    let unknown = act(context, opt_in(ADDRESS, &["resource.watch"])).await;
    assert_eq!(unknown, Err(EmailError::InvalidCategories));
    let empty = act(context, opt_in(ADDRESS, &[])).await;
    assert_eq!(empty, Err(EmailError::InvalidCategories));

    let first = act(context, opt_in(ADDRESS, &["share.upload"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sealed(context, first.preference_id).await.as_deref(),
        Some(ADDRESS)
    );
    let second = act(context, opt_in(ADDRESS, &["group.membership"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sealed(context, first.preference_id).await,
        None,
        "the old address is gone"
    );
    let read = act(context, EmailAction::Read).await.unwrap();
    assert_eq!(read, Some(second.clone()));

    act(context, EmailAction::OptOut).await.unwrap();
    assert_eq!(act(context, EmailAction::Read).await.unwrap(), None);
    assert_eq!(sealed(context, second.preference_id).await, None);
}

#[tokio::test]
async fn sends_opted_notices() {
    let staging = setup_driver_context().await;
    let context = staging.driver_context.clone();
    seed_user(&context).await;
    let preference = act(&context, opt_in(ADDRESS, &["share.upload"]))
        .await
        .unwrap()
        .unwrap();
    let sender = Arc::new(FakeSender::default());
    let ctx = job_context(context.clone(), Some(sender.clone()));
    let spec = EmailDeliverySpec {
        preference_id: preference.preference_id,
        notification: share_upload(),
    };

    let result = run_email_job(&ctx, &spec).await;
    assert!(matches!(
        result,
        JobRunOutcome::Succeeded(JobResultPayload::Email { sent: true })
    ));
    let mail = sender.sent.lock().unwrap()[0].clone();
    assert_eq!(
        (mail.to.as_str(), mail.id),
        (ADDRESS, Ulid::from_parts(7, 7))
    );
    assert!(
        !format!("{mail:?}").contains(ADDRESS),
        "Debug hides the recipient"
    );

    *sender.answer.lock().unwrap() = Some(MailError::Retryable("SMTP status 451".to_string()));
    let JobRunOutcome::Failed(error) = run_email_job(&ctx, &spec).await else {
        panic!("a transient refusal fails the attempt");
    };
    assert_eq!(error.kind, JobErrorKind::Retryable);
    *sender.answer.lock().unwrap() = Some(MailError::Permanent("SMTP status 550".to_string()));
    let JobRunOutcome::Failed(error) = run_email_job(&ctx, &spec).await else {
        panic!("a permanent refusal ends the job");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);

    let unconfigured = job_context(context.clone(), None);
    let JobRunOutcome::Failed(error) = run_email_job(&unconfigured, &spec).await else {
        panic!("a node without SMTP sends nothing");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);

    act(&context, EmailAction::OptOut).await.unwrap();
    *sender.answer.lock().unwrap() = None;
    let result = run_email_job(&ctx, &spec).await;
    assert!(matches!(
        result,
        JobRunOutcome::Succeeded(JobResultPayload::Email { sent: false })
    ));
    assert_eq!(sender.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn queues_each_once() {
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    seed_user(context).await;
    act(context, opt_in(ADDRESS, &["share.upload"]))
        .await
        .unwrap();
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
        .expect("the opted notice is queued");
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
        "a category outside the opt-in queues nothing"
    );
}

#[tokio::test]
async fn replacement_keeps_job() {
    // A retry after the opt-in was replaced finds the queued job instead of a conflict.
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    seed_user(context).await;
    act(context, opt_in(ADDRESS, &["share.upload"]))
        .await
        .unwrap();
    let records = [share_upload()];
    enqueue_emails(context, node(), &records).await.unwrap();
    let key = user_dedup_key(user(), &format!("email/{}", records[0].notification_id));
    let storage = &context.storage_handle;
    let first = find_dedup_plan(storage, user(), &key, None).await.unwrap();
    assert!(first.is_some(), "the opted notice is queued");

    act(context, opt_in(ADDRESS, &["share.upload"]))
        .await
        .unwrap();
    enqueue_emails(context, node(), &records)
        .await
        .expect("a replaced opt-in does not conflict");
    let again = find_dedup_plan(storage, user(), &key, None).await.unwrap();
    assert_eq!(again, first);
}
