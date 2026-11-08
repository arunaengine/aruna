//! Tests webhook subscriptions, signed delivery, status handling and replaced subscriptions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::executor::ProgressReporter;
use crate::jobs::store::find_dedup_plan;
use crate::jobs::submit::mint_job_id;
use crate::tests::staging::setup_driver_context;

use aruna_core::structs::execution::job::{JobErrorKind, JobProgress};
use aruna_core::structs::execution::notification::NotificationClass;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use axum::http::{HeaderMap, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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

async fn act(
    context: &DriverContext,
    action: WebhookAction,
) -> Result<WebhookOutcome, WebhookError> {
    drive(WebhookOperation::new(user(), action), context).await
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
        mailer: None,
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

#[tokio::test]
async fn subscription_replaces_secret() {
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    let url = "https://hooks.example.org/aruna".to_string();
    let first = act(context, WebhookAction::Set { url: url.clone() })
        .await
        .unwrap();
    assert_eq!(first.secret.as_ref().map(String::len), Some(64));
    let stored = act(context, WebhookAction::Read).await.unwrap();
    assert_eq!(stored.subscription, first.subscription);
    assert_eq!(stored.secret, None, "a read never shows the secret");

    let second = act(context, WebhookAction::Set { url }).await.unwrap();
    let old = first.subscription.unwrap().subscription_id;
    assert_ne!(second.subscription.as_ref().unwrap().subscription_id, old);
    let event = context
        .storage_handle
        .send_effect(read_secret(secret_entry(old), None))
        .await;
    let gone = parse_secret(event, secret_entry(old), |bytes| {
        Ok::<_, ConversionError>(bytes.to_vec())
    });
    assert_eq!(gone.unwrap(), None, "the replaced secret is deleted");

    act(context, WebhookAction::Remove).await.unwrap();
    let removed = act(context, WebhookAction::Read).await.unwrap();
    assert_eq!(removed.subscription, None);
}

#[tokio::test]
async fn refuses_unusable_urls() {
    let staging = setup_driver_context().await;
    for url in [
        "ftp://example.org/x",
        "https://user:pw@example.org/x",
        "not a url",
    ] {
        let result = act(
            &staging.driver_context,
            WebhookAction::Set {
                url: url.to_string(),
            },
        )
        .await;
        assert_eq!(result, Err(WebhookError::InvalidUrl), "{url}");
    }
}

#[tokio::test]
async fn delivers_signed_upload() {
    let staging = setup_driver_context().await;
    let answer = Arc::new(AtomicU16::new(204));
    let (url, mut received) = receiver(answer.clone()).await;
    let outcome = act(&staging.driver_context, WebhookAction::Set { url })
        .await
        .unwrap();
    let secret = hex::decode(outcome.secret.unwrap()).unwrap();
    let spec = WebhookDeliverySpec {
        subscription_id: outcome.subscription.unwrap().subscription_id,
        notification: upload(user()),
    };
    let ctx = job_context(staging.driver_context.clone());

    let result = run_webhook_job(&ctx, &spec).await;
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
    assert_eq!(signature, sign_delivery(&secret, timestamp, &body));
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body, spec.body().unwrap());

    answer.store(500, Ordering::SeqCst);
    let JobRunOutcome::Failed(error) = run_webhook_job(&ctx, &spec).await else {
        panic!("a server error fails the attempt");
    };
    assert_eq!(error.kind, JobErrorKind::Retryable);
    answer.store(400, Ordering::SeqCst);
    let JobRunOutcome::Failed(error) = run_webhook_job(&ctx, &spec).await else {
        panic!("a client error fails the delivery");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

#[tokio::test]
async fn replaced_subscription_skips() {
    // A delivery queued for a replaced or removed subscription sends nothing.
    let staging = setup_driver_context().await;
    let (url, mut received) = receiver(Arc::new(AtomicU16::new(200))).await;
    act(&staging.driver_context, WebhookAction::Set { url })
        .await
        .unwrap();
    let spec = WebhookDeliverySpec {
        subscription_id: Ulid::generate(),
        notification: upload(user()),
    };
    let ctx = job_context(staging.driver_context.clone());
    let result = run_webhook_job(&ctx, &spec).await;
    assert!(matches!(
        result,
        JobRunOutcome::Succeeded(JobResultPayload::Webhook { status: None })
    ));
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn replacement_keeps_job() {
    // A retry after the subscription was replaced finds the queued job instead of a conflict.
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    let url = "https://hooks.example.org/aruna".to_string();
    act(context, WebhookAction::Set { url: url.clone() })
        .await
        .unwrap();
    let records = [upload(user())];
    let owner = iroh::SecretKey::from_bytes(&[8; 32]).public();
    enqueue_webhooks(context, owner, &records).await.unwrap();
    let key = format!("webhook/{}", records[0].notification_id);
    let key = user_dedup_key(user(), &key);
    let storage = &context.storage_handle;
    let first = find_dedup_plan(storage, user(), &key, None).await.unwrap();
    assert!(first.is_some(), "the upload is queued");

    act(context, WebhookAction::Set { url }).await.unwrap();
    enqueue_webhooks(context, owner, &records)
        .await
        .expect("a replaced subscription does not conflict");
    let again = find_dedup_plan(storage, user(), &key, None).await.unwrap();
    assert_eq!(again, first);
}
