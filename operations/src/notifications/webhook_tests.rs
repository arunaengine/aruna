//! Tests webhook subscriptions as register writes, signed delivery, status handling and
//! deliveries that decide from this node's setting.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::jobs::store::test_calls::find_dedup_plan;
use crate::notifications::enqueue::EnqueueDeliveriesOperation;
use crate::tests::staging::setup_driver_context;
use aruna_core::calls::ChildOutcome;
use aruna_core::register::RegisterStamp;
use aruna_core::structs::execution::job::user_dedup_key;
use aruna_core::structs::execution::notification::{NotificationKind, NotificationRecord};
use aruna_core::time::unix_timestamp_millis;
use ulid::Ulid;

use aruna_core::structs::execution::notification::NotificationClass;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use ed25519_dalek::SigningKey;

/// A time before the notice expires.
const NOW_MS: u64 = 1_000;

/// A user of a realm whose id is a realm key, so secrets can be sealed to it.
fn user() -> UserId {
    let realm_key = SigningKey::from_bytes(&[5; 32]).verifying_key();
    UserId::local(
        Ulid::from_parts(1, 1),
        RealmId::from_bytes(realm_key.to_bytes()),
    )
}

fn upload(recipient: UserId) -> NotificationRecord {
    NotificationRecord {
        notification_id: Ulid::from_parts(7, 7),
        recipient,
        class: NotificationClass::Direct,
        kind: NotificationKind::ShareUpload {
            share_id: Ulid::from_parts(4, 4),
            node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            group_id: Ulid::from_parts(5, 5),
            bucket: "inbox".to_string(),
            key: "a.csv".to_string(),
            size_bytes: 9,
        },
        created_at_ms: 42,
        read_at_ms: None,
        watch_authorization: None,
    }
}

const HOOK: &str = "https://hooks.example.org/aruna";

async fn read_webhook(
    context: &DriverContext,
    owner: UserId,
) -> Result<Option<WebhookSubscription>, WebhookError> {
    drive(ReadWebhookOperation::new(owner), context).await
}

async fn set_webhook(
    context: &DriverContext,
    local: NodeId,
    owner: UserId,
    url: String,
) -> Result<WebhookOutcome, WebhookError> {
    let now_ms = unix_timestamp_millis();
    drive(SetWebhookOperation::new(owner, local, url, now_ms), context).await
}

async fn remove_webhook(
    context: &DriverContext,
    local: NodeId,
    owner: UserId,
) -> Result<(), WebhookError> {
    let now_ms = unix_timestamp_millis();
    drive(RemoveWebhookOperation::new(owner, local, now_ms), context).await
}

fn stored(url: Option<&str>) -> WebhookSubscription {
    let id = Ulid::from_parts(6, 6);
    WebhookSubscription {
        subscription_id: id,
        user_id: user(),
        stamp: RegisterStamp {
            counter: 1,
            node_id: iroh::SecretKey::from_bytes(&[8; 32]).public(),
            write_id: id,
        },
        url: url.map(str::to_string),
        sealed: url.map(|_| vec![1]),
        created_at_ms: 1,
    }
}

fn answer<T>(result: Result<T, SettingError>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

#[test]
fn read_decodes_setting() {
    let mut read = ReadWebhookOperation::new(user());
    assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
    let bytes = stored(Some(HOOK)).to_bytes().unwrap();
    assert_eq!(
        read.next(answer(Ok(Some(bytes)))),
        Ok(Next::Done(Some(stored(Some(HOOK)))))
    );
    let mut off = ReadWebhookOperation::new(user());
    off.first().unwrap();
    assert_eq!(
        off.next(answer::<Option<Vec<u8>>>(Ok(None))),
        Ok(Next::Done(None))
    );
    let mut broken = ReadWebhookOperation::new(user());
    broken.first().unwrap();
    assert!(matches!(
        broken.next(answer(Ok(Some(vec![0xff])))),
        Err(WebhookError::Conversion(_))
    ));
}

#[test]
fn set_returns_secret() {
    let node = iroh::SecretKey::from_bytes(&[8; 32]).public();
    let secret = SecretBytes::new(vec![0xab; 32]);
    let mut set = SetWebhookOperation::with_secret(user(), node, HOOK.to_string(), secret, 5);
    assert!(matches!(set.first(), Ok(Next::Call(IoCall::Child(_)))));
    let bytes = stored(Some(HOOK)).to_bytes().unwrap();
    assert_eq!(
        set.next(answer(Ok(bytes))),
        Ok(Next::Done(WebhookOutcome {
            subscription: stored(Some(HOOK)),
            secret: "ab".repeat(32),
        }))
    );
    let mut invalid = SetWebhookOperation::new(user(), node, "not a url".to_string(), 5);
    assert_eq!(invalid.first(), Err(WebhookError::InvalidUrl));
}

#[test]
fn remove_writes_off() {
    let node = iroh::SecretKey::from_bytes(&[8; 32]).public();
    let mut remove = RemoveWebhookOperation::new(user(), node, 5);
    assert_eq!(remove.write.value, SettingValue::Webhook { target: None });
    assert!(matches!(remove.first(), Ok(Next::Call(IoCall::Child(_)))));
    let bytes = stored(None).to_bytes().unwrap();
    assert_eq!(remove.next(answer(Ok(bytes))), Ok(Next::Done(())));
    let mut refused = RemoveWebhookOperation::new(user(), node, 5);
    refused.first().unwrap();
    let error = || SettingError::Build("sealing failed".to_string());
    assert_eq!(
        refused.next(answer::<Vec<u8>>(Err(error()))),
        Err(WebhookError::Setting(error()))
    );
}

#[tokio::test]
async fn refuses_unusable_urls() {
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    let node = iroh::SecretKey::from_bytes(&[8; 32]).public();
    for url in [
        "ftp://example.org/x",
        "https://user:pw@example.org/x",
        "not a url",
    ] {
        let result = set_webhook(context, node, user(), url.to_string()).await;
        assert_eq!(result, Err(WebhookError::InvalidUrl), "{url}");
    }
    // A valid subscription is one local register write, without any management node.
    let outcome = set_webhook(context, node, user(), HOOK.to_string())
        .await
        .unwrap();
    assert_eq!(outcome.secret.len(), 64);
    assert_eq!(outcome.subscription.stamp.counter, 1);
    let read = read_webhook(context, user()).await.unwrap();
    assert_eq!(read, Some(outcome.subscription.clone()));
    let replaced = set_webhook(context, node, user(), HOOK.to_string())
        .await
        .unwrap();
    assert_eq!(replaced.subscription.stamp.counter, 2);
    assert_ne!(
        replaced.secret, outcome.secret,
        "a new subscription gets a new secret"
    );
    remove_webhook(context, node, user()).await.unwrap();
    assert_eq!(read_webhook(context, user()).await.unwrap(), None);
}

#[tokio::test]
async fn enqueue_always_queues() {
    // No subscription is read at enqueue: each upload gets one job, once.
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    let records = [upload(user())];
    let owner = iroh::SecretKey::from_bytes(&[8; 32]).public();
    let mut config = RealmConfigDocument::new(user().realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.seed_job_control(owner, 0);
    let enqueue = || EnqueueDeliveriesOperation::new(owner, Some(&config), &records, NOW_MS);
    drive(enqueue(), context).await.unwrap();
    let key = format!("webhook/{}", records[0].notification_id);
    let key = user_dedup_key(user(), &key);
    let first = find_dedup_plan(&context, user(), &key, None).await.unwrap();
    assert!(
        first.is_some(),
        "the upload is queued without a subscription"
    );
    drive(enqueue(), context).await.unwrap();
    let again = find_dedup_plan(&context, user(), &key, None).await.unwrap();
    assert_eq!(again, first);
}
