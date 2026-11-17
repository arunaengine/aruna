//! Tests the webhook job as calls: the delivery decision and the signed post of the notice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::document_repository::GetConfigError;
use crate::notifications::settings::SettingWinner;
use crate::tests::notifications::user;
use aruna_core::calls::ChildOutcome;
use aruna_core::compute::SecretBytes;
use aruna_core::register::RegisterStamp;
use aruna_core::structs::execution::job::{JobError, JobErrorKind};
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use std::time::Duration;
use ulid::Ulid;

const NOW_MS: u64 = 1_000;

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[8; 32]).public()
}

fn spec() -> WebhookDeliverySpec {
    let kind = NotificationKind::ShareUpload {
        share_id: Ulid::from_parts(4, 4),
        node_id: node(),
        group_id: Ulid::from_parts(5, 5),
        bucket: "inbox".to_string(),
        key: "a.csv".to_string(),
        size_bytes: 9,
    };
    WebhookDeliverySpec {
        subscription_id: Ulid::nil(),
        notification: NotificationRecord::new(user(1, 2), NotificationClass::Direct, kind, 42),
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn winner(url: Option<&str>) -> Delivery {
    let id = Ulid::from_parts(6, 6);
    let subscription = WebhookSubscription {
        subscription_id: id,
        user_id: user(1, 2),
        stamp: RegisterStamp {
            counter: 1,
            node_id: node(),
            write_id: id,
        },
        url: url.map(str::to_string),
        sealed: Some(vec![1]),
        created_at_ms: 1,
    };
    Delivery::Send(SettingWinner {
        bytes: subscription.to_bytes().unwrap(),
        secret: Some(SecretBytes::new(vec![9; 32])),
    })
}

/// A job that read an unreadable config, which still decides, and then `delivery`.
fn decided(delivery: Delivery) -> (WebhookJobOperation, Turn) {
    let mut job = WebhookJobOperation::new(spec(), node(), NOW_MS);
    assert!(matches!(job.first(), Ok(Next::Call(IoCall::Child(_)))));
    let config = child::<RealmConfigDocument, _>(Err(GetConfigError::DocumentNotFound));
    assert!(matches!(job.next(config), Ok(Next::Call(IoCall::Child(_)))));
    let turn = job.next(child::<_, IoError>(Ok(delivery)));
    (job, turn)
}

fn status_turn(status: Option<u16>) -> Turn {
    Ok(Next::Done(DeliveryOutcome::Succeeded(
        JobResultPayload::Webhook { status },
    )))
}

#[test]
fn off_setting_skips() {
    assert_eq!(decided(Delivery::Skip).1, status_turn(None));
    assert_eq!(decided(winner(None)).1, status_turn(None));
}

#[test]
fn waiting_defers() {
    let error = JobError::retryable("waiting for a management node");
    let wait = Delivery::Wait(error.clone(), Duration::from_secs(20));
    assert_eq!(
        decided(wait).1,
        Ok(Next::Done(DeliveryOutcome::DeferredFor(
            error,
            Duration::from_secs(20)
        )))
    );
}

/// An on setting posts the notice; a permanent refusal ends the job.
#[test]
fn on_setting_posts() {
    let (mut job, turn) = decided(winner(Some("https://hooks.example.org/aruna")));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(
        job.next(child::<_, SendError>(Ok(Some(204)))),
        status_turn(Some(204))
    );

    let (mut refused, _) = decided(winner(Some("https://hooks.example.org/aruna")));
    let refusal = SendError::Refused("status 404".into());
    let Ok(Next::Done(DeliveryOutcome::Failed(error))) =
        refused.next(child::<Option<u16>, _>(Err(refusal)))
    else {
        panic!("a refusal fails the job");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

/// The post is signed with the runner's time of the step that sends it, not the job start.
#[test]
fn signed_when_sent() {
    let mut job = WebhookJobOperation::new(spec(), node(), NOW_MS);
    let later_ms = NOW_MS + 90_000;
    job.stamp(later_ms);
    let url = "https://hooks.example.org/aruna";
    let fresh = SendOperation::webhook(&spec(), url, &[9; 32], later_ms / 1000);
    assert_eq!(job.post(url, &[9; 32]), fresh);
    let stale = SendOperation::webhook(&spec(), url, &[9; 32], NOW_MS / 1000);
    assert_ne!(job.post(url, &[9; 32]), stale);
}

#[test]
fn unexpected_results_fail() {
    let mut job = WebhookJobOperation::new(spec(), node(), NOW_MS);
    job.first().unwrap();
    assert!(matches!(
        job.next(IoResult::Done),
        Err(IoError::Unexpected { .. })
    ));
}
