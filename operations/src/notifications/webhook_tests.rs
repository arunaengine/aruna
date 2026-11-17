//! Tests webhook subscriptions as register writes, signed delivery, status handling and
//! deliveries that decide from this node's setting.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::executor::ProgressReporter;
use crate::jobs::store::query::find_dedup_plan;
use crate::jobs::submit::mint_job_id;
use crate::notifications::enqueue::EnqueueDeliveriesOperation;
use crate::tests::notifications::{managers, store_setting};
use crate::tests::staging::setup_driver_context;
use aruna_core::structs::execution::job::user_dedup_key;
use aruna_core::structs::execution::notification::{NotificationKind, NotificationRecord};
use ulid::Ulid;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::handle::Handle;
use aruna_core::structs::execution::job::{JobErrorKind, JobProgress};
use aruna_core::structs::execution::notification::NotificationClass;
use aruna_core::structs::execution::webhook::{DELIVERY_HEADER, SIGNATURE_HEADER, sign_delivery};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use axum::http::{HeaderMap, StatusCode};
use ed25519_dalek::SigningKey;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
