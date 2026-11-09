//! Tests webhook subscriptions, signed delivery, status handling and deliveries that decide
//! from a quorum read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::executor::ProgressReporter;
use crate::jobs::store::find_dedup_plan;
use crate::jobs::submit::mint_job_id;
use crate::notifications::settings::{SettingAnswer, SettingWinner};
use crate::tests::notifications::{CannedPeers, managers};
use crate::tests::staging::setup_driver_context;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::handle::Handle;
use aruna_core::structs::execution::job::{JobErrorKind, JobProgress};
use aruna_core::structs::execution::notification::NotificationClass;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use axum::http::{HeaderMap, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// A time before the notice expires.
const NOW_MS: u64 = 1_000;
const SECRET: [u8; 32] = [9; 32];

fn user() -> UserId {
    UserId::local(Ulid::from_parts(1, 1), RealmId::from_bytes([1; 32]))
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

fn job_context(context: DriverContext) -> JobContext {
    JobContext {
        driver: Arc::new(context),
        job_id: mint_job_id(
            PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            BucketId::new(0).unwrap(),
        )
        .unwrap(),
        owner_node_id: iroh::SecretKey::from_bytes(&[8; 32]).public(),
        claim_token: Ulid::generate(),
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&JobProgress::new("steps")),
        range_turns: Default::default(),
        mail: Default::default(),
    }
}

type Received = mpsc::UnboundedReceiver<(HeaderMap, Vec<u8>)>;

/// A receiver answering with the status in `answer`, reporting every request it gets.
async fn receiver(answer: Arc<AtomicU16>) -> (String, Received) {
    let (sender, received) = mpsc::unbounded_channel();
    let router = axum::Router::new().route(
        "/hook",
        axum::routing::post(move |headers: HeaderMap, body: axum::body::Bytes| {
            let (sender, answer) = (sender.clone(), answer.clone());
            async move {
                let _ = sender.send((headers, body.to_vec()));
                StatusCode::from_u16(answer.load(Ordering::SeqCst)).unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}/hook"), received)
}

/// Stores a realm config whose three managers are the canned peers.
async fn seed_config(context: &DriverContext) {
    let realm_id = user().realm_id;
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    for manager in managers() {
        config.ensure_node(manager, RealmNodeKind::Management);
    }
    let actor = Actor {
        node_id: managers()[0],
        user_id: user(),
        realm_id,
    };
    let target = aruna_core::document::DocumentTarget::RealmConfig { realm_id };
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

/// Answers from every manager with a subscription for `url`; `None` turns the webhook off.
fn answering(url: Option<String>) -> CannedPeers {
    let on = url.is_some();
    let subscription = WebhookSubscription {
        subscription_id: Ulid::from_parts(2, 2),
        user_id: user(),
        version: 1,
        url,
        sealed: on.then(|| vec![1]),
        created_at_ms: 1,
    };
    let answer = SettingAnswer {
        max_version: 1,
        winner: Some(SettingWinner {
            bytes: subscription.to_bytes().unwrap(),
            secret: on.then(|| SecretBytes::new(SECRET.to_vec())),
        }),
    };
    CannedPeers {
        answers: managers().map(|manager| (manager, answer.clone())).into(),
        ..Default::default()
    }
}

fn spec() -> WebhookDeliverySpec {
    WebhookDeliverySpec {
        subscription_id: Ulid::nil(),
        notification: upload(user()),
    }
}

#[tokio::test]
async fn refuses_unusable_urls() {
    let staging = setup_driver_context().await;
    let node = iroh::SecretKey::from_bytes(&[8; 32]).public();
    for url in [
        "ftp://example.org/x",
        "https://user:pw@example.org/x",
        "not a url",
    ] {
        let result = set_webhook(&staging.driver_context, node, user(), url.to_string()).await;
        assert_eq!(result, Err(WebhookError::InvalidUrl), "{url}");
    }
    let url = "https://hooks.example.org/aruna".to_string();
    let unreadable = set_webhook(&staging.driver_context, node, user(), url.clone()).await;
    assert!(matches!(
        unreadable,
        Err(WebhookError::Setting(SettingError::Config(_)))
    ));
    seed_config(&staging.driver_context).await;
    let unreachable = set_webhook(&staging.driver_context, node, user(), url).await;
    assert_eq!(
        unreachable,
        Err(WebhookError::Setting(SettingError::NoQuorum))
    );
}

#[tokio::test]
async fn on_answer_sends() {
    let staging = setup_driver_context().await;
    seed_config(&staging.driver_context).await;
    let answer = Arc::new(AtomicU16::new(204));
    let (url, mut received) = receiver(answer.clone()).await;
    let peers = answering(Some(url));
    let backoffs = Backoffs::default();
    let ctx = job_context(staging.driver_context.clone());
    let spec = spec();
    let run = || deliver_webhook(&ctx, &spec, &peers, &backoffs, NOW_MS);

    let result = run().await;
    assert!(matches!(
        result,
        JobRunOutcome::Succeeded(JobResultPayload::Webhook { status: Some(204) })
    ));
    let (headers, body) = received.recv().await.unwrap();
    assert_eq!(
        headers[DELIVERY_HEADER],
        spec.notification.notification_id.to_string()
    );
    let signature = headers[SIGNATURE_HEADER].to_str().unwrap();
    let timestamp: u64 = signature
        .strip_prefix("t=")
        .and_then(|rest| rest.split(',').next())
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(signature, sign_delivery(&SECRET, timestamp, &body));
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body, spec.body().unwrap());

    answer.store(500, Ordering::SeqCst);
    let JobRunOutcome::Failed(error) = run().await else {
        panic!("a server error fails the attempt");
    };
    assert_eq!(error.kind, JobErrorKind::Retryable);
    answer.store(400, Ordering::SeqCst);
    let JobRunOutcome::Failed(error) = run().await else {
        panic!("a client error fails the delivery");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

#[tokio::test]
async fn off_answer_skips() {
    let staging = setup_driver_context().await;
    seed_config(&staging.driver_context).await;
    let ctx = job_context(staging.driver_context.clone());
    let (spec, peers, backoffs) = (spec(), answering(None), Backoffs::default());
    let result = deliver_webhook(&ctx, &spec, &peers, &backoffs, NOW_MS);
    assert!(matches!(
        result.await,
        JobRunOutcome::Succeeded(JobResultPayload::Webhook { status: None })
    ));
}

#[tokio::test]
async fn expired_notice_finishes() {
    let staging = setup_driver_context().await;
    seed_config(&staging.driver_context).await;
    let ctx = job_context(staging.driver_context.clone());
    let peers = answering(Some("https://hooks.example.org/aruna".to_string()));
    let expired = spec().notification.expires_at_ms();
    let result = deliver_webhook(&ctx, &spec(), &peers, &Backoffs::default(), expired).await;
    assert!(matches!(
        result,
        JobRunOutcome::Succeeded(JobResultPayload::Webhook { status: None })
    ));
    assert_eq!(
        peers.reads.load(Ordering::SeqCst),
        0,
        "an expired notice reads nothing"
    );
}

#[tokio::test]
async fn short_quorum_defers() {
    let staging = setup_driver_context().await;
    seed_config(&staging.driver_context).await;
    let ctx = job_context(staging.driver_context.clone());
    let mut peers = answering(Some("https://hooks.example.org/aruna".to_string()));
    peers.answers.retain(|node, _| *node == managers()[2]);
    let backoffs = Backoffs::default();
    let JobRunOutcome::DeferredFor(error, delay) =
        deliver_webhook(&ctx, &spec(), &peers, &backoffs, NOW_MS).await
    else {
        panic!("one answer of three defers");
    };
    assert_eq!(
        (error.kind, delay),
        (JobErrorKind::Retryable, Duration::from_secs(30))
    );
    // The notice expires during the backoff, so the wait ends at the expiry.
    let late = spec().notification.expires_at_ms() - 5_000;
    let JobRunOutcome::DeferredFor(_, delay) =
        deliver_webhook(&ctx, &spec(), &peers, &backoffs, late).await
    else {
        panic!("a short read near the expiry defers");
    };
    assert_eq!(delay, Duration::from_secs(5));
}

#[tokio::test]
async fn enqueue_always_queues() {
    // No subscription is read at enqueue: each upload gets one job, once.
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    let records = [upload(user())];
    let owner = iroh::SecretKey::from_bytes(&[8; 32]).public();
    enqueue_webhooks(context, owner, &records).await.unwrap();
    let key = format!("webhook/{}", records[0].notification_id);
    let key = user_dedup_key(user(), &key);
    let storage = &context.storage_handle;
    let first = find_dedup_plan(storage, user(), &key, None).await.unwrap();
    assert!(
        first.is_some(),
        "the upload is queued without a subscription"
    );
    enqueue_webhooks(context, owner, &records).await.unwrap();
    let again = find_dedup_plan(storage, user(), &key, None).await.unwrap();
    assert_eq!(again, first);
}
