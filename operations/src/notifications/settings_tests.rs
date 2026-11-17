//! Tests local register writes of settings, deliveries decided from this node's view, opening
//! the secret on one management node, and the shared delivery backoff.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::notifications::setting_rows::{
    ReadSettingOperation, SettingError, SettingValue, SettingWrite, WriteSettingOperation,
};
use crate::tests::notifications::{context, managers, store_setting, temp_storage};
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{PeerRequestError, StorageEvent};
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::register::RegisterError;
use aruna_core::register::RegisterStamp;
use aruna_core::structs::execution::notification::{NotificationClass, NotificationKind};
use aruna_core::structs::execution::setting::SettingHead;
use aruna_core::structs::execution::webhook::WebhookSubscription;
use aruna_core::structs::identity::realm::RealmId;
use ulid::Ulid;

const SECRET: &[u8] = b"canary-hook-17";
/// A time before the notice expires.
const NOW_MS: u64 = 1_000;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn owner() -> UserId {
    UserId::local(Ulid::from_bytes([6u8; 16]), RealmId::from_bytes([1u8; 32]))
}

/// Builds webhook settings of `owner()`; an on setting carries a placeholder sealed secret.
fn webhook(on: bool) -> impl Fn(RegisterStamp) -> Result<Vec<u8>, SettingError> + Sync {
    move |stamp| {
        let subscription = WebhookSubscription {
            subscription_id: stamp.write_id,
            user_id: owner(),
            stamp,
            url: on.then(|| "https://hooks.example.org/aruna".to_string()),
            sealed: on.then(|| vec![1; 48]),
            created_at_ms: 1,
        };
        Ok(subscription.to_bytes().unwrap())
    }
}

/// Turns the webhook of `owner()` off with a new local register write.
fn webhook_off(writer: NodeId) -> WriteSettingOperation {
    WriteSettingOperation::new(SettingWrite {
        owner: owner(),
        local: writer,
        write_id: Ulid::generate(),
        event_id: Ulid::generate(),
        now_ms: NOW_MS,
        value: SettingValue::Webhook { target: None },
    })
}

async fn write(ctx: &DriverContext, writer: NodeId) -> SettingHead {
    let bytes = drive(webhook_off(writer), ctx).await.unwrap();
    SettingChannel::Webhook.head(&bytes).unwrap()
}

/// Stores a webhook setting written elsewhere, as sync would.
async fn received(ctx: &DriverContext, counter: u64, writer: NodeId, on: bool) -> SettingHead {
    let stamp = RegisterStamp {
        counter,
        node_id: writer,
        write_id: Ulid::generate(),
    };
    let bytes = webhook(on)(stamp).unwrap();
    let target = SettingChannel::Webhook.target(owner(), stamp.write_id);
    store_setting(&ctx.storage_handle, &target, bytes.clone()).await;
    SettingChannel::Webhook.head(&bytes).unwrap()
}

fn notice() -> NotificationRecord {
    let kind = NotificationKind::AddedToGroup {
        group_id: Ulid::from_bytes([2u8; 16]),
        actor_user_id: owner(),
    };
    NotificationRecord::new(owner(), NotificationClass::Direct, kind, 1)
}

/// The bytes of an on webhook setting written on another node.
fn on_setting() -> Vec<u8> {
    let stamp = RegisterStamp {
        counter: 1,
        node_id: node(2),
        write_id: Ulid::from_bytes([4u8; 16]),
    };
    webhook(true)(stamp).unwrap()
}

/// A decision on `node(9)`, which is no management node.
fn deciding(managers: Option<Vec<NodeId>>, now_ms: u64) -> DeliveryOperation {
    let local = (node(9), owner().realm_id);
    DeliveryOperation::new(notice(), SettingChannel::Webhook, managers, local, now_ms)
}

fn all_managers() -> Option<Vec<NodeId>> {
    Some(managers().to_vec())
}

fn setting(result: Result<Option<Vec<u8>>, SettingError>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

fn backoff(change: BackoffChange) -> Result<Next<Delivery>, IoError> {
    let request = BackoffRequest {
        owner: owner(),
        channel: SettingChannel::Webhook,
        change,
    };
    Ok(Next::Call(IoCall::from(Effect::Outbound(
        OutboundEffect::Backoff(request),
    ))))
}

fn stored(stored: Option<StoredBackoff>) -> IoResult {
    IoResult::Event(Box::new(Event::Outbound(OutboundEvent::Backoff { stored })))
}

fn opened(peer: NodeId, result: Result<SecretBytes, MetadataReadError>) -> IoResult {
    let message = MetadataTransportMessage::OpenSettingResult { result };
    let answer = MetadataAnswer {
        peer,
        elapsed: Duration::ZERO,
        result: Ok(encode_message(&message).unwrap()),
    };
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

/// The request that asks `peer` to open the on setting.
fn open_call(peer: NodeId) -> Result<Next<Delivery>, IoError> {
    let target = SettingChannel::Webhook.target(owner(), Ulid::from_bytes([4u8; 16]));
    let message = MetadataTransportMessage::OpenSetting {
        target,
        bytes: on_setting(),
    };
    let request = MetadataRequestEffect {
        peer,
        frame: encode_message(&message).unwrap(),
        timeout: OPEN_TIMEOUT,
    };
    Ok(Next::Call(IoCall::from(Effect::Net(
        NetEffect::MetadataRequest(Box::new(request)),
    ))))
}

/// Reads an on setting and no running delay, up to the first manager's request.
fn asking() -> DeliveryOperation {
    let mut decide = deciding(all_managers(), NOW_MS);
    assert!(matches!(decide.first(), Ok(Next::Call(IoCall::Child(_)))));
    let read = decide.next(setting(Ok(Some(on_setting()))));
    assert_eq!(read, backoff(BackoffChange::Read));
    assert_eq!(decide.next(stored(None)), open_call(managers()[0]));
    decide
}

#[test]
fn off_setting_skips() {
    let mut decide = deciding(all_managers(), NOW_MS);
    decide.first().unwrap();
    assert_eq!(
        decide.next(setting(Ok(None))),
        Ok(Next::Done(Delivery::Skip))
    );
    let expired = notice().expires_at_ms();
    assert_eq!(
        deciding(all_managers(), expired).first(),
        Ok(Next::Done(Delivery::Skip))
    );
}

/// A manager that does not open the secret is skipped; the next one in config order opens it
/// and the delay is cleared.
#[test]
fn first_manager_opens() {
    let mut decide = asking();
    let refused = opened(managers()[0], Err(MetadataReadError::Unavailable));
    assert_eq!(decide.next(refused), open_call(managers()[1]));
    let secret = SecretBytes::new(SECRET.to_vec());
    let open = decide.next(opened(managers()[1], Ok(secret.clone())));
    assert_eq!(open, backoff(BackoffChange::Clear));
    let winner = SettingWinner {
        bytes: on_setting(),
        secret: Some(secret),
    };
    assert_eq!(
        decide.next(stored(None)),
        Ok(Next::Done(Delivery::Send(winner)))
    );
}

/// No manager answers: the delay grows from the stored one and the job waits for it.
#[test]
fn no_manager_waits() {
    let mut decide = asking();
    for (at, peer) in managers().into_iter().enumerate().skip(1) {
        let failed = IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
            MetadataAnswer {
                peer: managers()[at - 1],
                elapsed: Duration::ZERO,
                result: Err(PeerRequestError::TimedOut),
            },
        )))));
        assert_eq!(decide.next(failed), open_call(peer));
    }
    let last = opened(managers()[2], Err(MetadataReadError::Unavailable));
    let until_ms = NOW_MS + 30_000;
    let set = BackoffChange::Set {
        until_ms,
        delay_ms: 30_000,
    };
    assert_eq!(decide.next(last), backoff(set));
    let wait = decide.next(stored(Some(StoredBackoff {
        until_ms,
        delay_ms: 30_000,
    })));
    assert!(matches!(
        wait,
        Ok(Next::Done(Delivery::Wait(_, delay))) if delay == Duration::from_secs(30)
    ));
}

/// A running delay asks nobody; an unreadable config or setting counts as no answer.
#[test]
fn running_backoff_waits() {
    let mut decide = deciding(all_managers(), NOW_MS);
    decide.first().unwrap();
    decide.next(setting(Ok(Some(on_setting())))).unwrap();
    let running = Some(StoredBackoff {
        until_ms: NOW_MS + 20_000,
        delay_ms: 30_000,
    });
    assert!(matches!(
        decide.next(stored(running)),
        Ok(Next::Done(Delivery::Wait(_, delay))) if delay == Duration::from_secs(20)
    ));

    let mut unconfigured = deciding(None, NOW_MS);
    unconfigured.first().unwrap();
    unconfigured.next(setting(Ok(Some(on_setting())))).unwrap();
    let grown = BackoffChange::Set {
        until_ms: NOW_MS + 60_000,
        delay_ms: 60_000,
    };
    let earlier = Some(StoredBackoff {
        until_ms: NOW_MS,
        delay_ms: 30_000,
    });
    assert_eq!(unconfigured.next(stored(earlier)), backoff(grown));

    let mut unread = deciding(all_managers(), NOW_MS);
    unread.first().unwrap();
    let failed = setting(Err(SettingError::Storage("down".to_string())));
    assert_eq!(unread.next(failed), backoff(BackoffChange::Read));
    let first = BackoffChange::Set {
        until_ms: NOW_MS + 30_000,
        delay_ms: 30_000,
    };
    assert_eq!(unread.next(stored(None)), backoff(first));
}

/// This node opens the secret itself when it is a manager.
#[test]
fn local_manager_serves() {
    let local = (managers()[0], owner().realm_id);
    let channel = SettingChannel::Webhook;
    let mut decide = DeliveryOperation::new(notice(), channel, all_managers(), local, NOW_MS);
    decide.first().unwrap();
    decide.next(setting(Ok(Some(on_setting())))).unwrap();
    assert!(matches!(
        decide.next(stored(None)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let served: SecretAnswer = Ok(SecretBytes::new(SECRET.to_vec()));
    let served = IoResult::Child(ChildOutcome::new(Ok::<_, MetadataReadError>(served)));
    assert_eq!(decide.next(served), backoff(BackoffChange::Clear));
    assert!(matches!(
        decide.next(IoResult::Done),
        Err(IoError::Unexpected { .. })
    ));
}

#[test]
fn delays_double_capped() {
    let mut previous = None;
    let delays: Vec<_> = (0..8)
        .map(|_| {
            let delay = next_delay(previous);
            previous = Some(delay);
            delay / 1_000
        })
        .collect();
    assert_eq!(delays, [30, 60, 120, 240, 480, 960, 1_800, 1_800]);
}

async fn outbox_rows(ctx: &DriverContext) -> usize {
    let page = StorageEffect::Iter {
        key_space: SYNC_OUTBOX_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: 64,
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(page).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
        other => panic!("unexpected outbox page: {other:?}"),
    }
}

#[tokio::test]
async fn write_counts_up() {
    // Each write is one above the highest counter this node holds, also from other nodes.
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    let first = write(&ctx, node(1)).await;
    assert_eq!((first.stamp.counter, first.stamp.node_id), (1, node(1)));
    assert_eq!(first.stamp.write_id, first.id);
    received(&ctx, 7, node(2), true).await;
    let next = write(&ctx, node(1)).await;
    assert_eq!(next.stamp.counter, 8);
    assert_eq!(outbox_rows(&ctx).await, 2, "every local write is published");
    let read = ReadSettingOperation::new(SettingChannel::Webhook, owner());
    let shown = drive(read, &ctx).await;
    assert_eq!(
        shown.unwrap(),
        None,
        "the newest write turned the webhook off"
    );
}

#[tokio::test]
async fn counter_refused() {
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    received(&ctx, u64::MAX, node(2), true).await;
    let refused = drive(webhook_off(node(1)), &ctx).await;
    assert_eq!(
        refused,
        Err(SettingError::Register(RegisterError::CounterLimit))
    );
    assert_eq!(outbox_rows(&ctx).await, 0);
}
