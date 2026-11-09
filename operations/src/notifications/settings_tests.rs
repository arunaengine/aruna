//! Tests local register writes of settings, deliveries decided from this node's view, opening
//! the secret on one management node, and the shared delivery backoff.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::notifications::{
    CannedPeers, context, managers, opening, store_setting, temp_storage,
};
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::structs::execution::notification::{NotificationClass, NotificationKind};
use aruna_core::structs::execution::webhook::WebhookSubscription;
use aruna_core::structs::identity::realm::RealmId;
use std::sync::atomic::Ordering;

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

async fn write(ctx: &DriverContext, writer: NodeId, on: bool) -> SettingHead {
    let build = webhook(on);
    let channel = SettingChannel::Webhook;
    let bytes = write_setting(ctx, writer, channel, owner(), &build)
        .await
        .unwrap();
    channel.head(&bytes).unwrap()
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

async fn decide(
    ctx: &DriverContext,
    peers: &CannedPeers,
    backoffs: &Backoffs,
    now_ms: u64,
) -> Delivery {
    let managers = managers();
    let channel = SettingChannel::Webhook;
    delivery_setting(
        ctx,
        peers,
        backoffs,
        &notice(),
        channel,
        Some(managers.as_slice()),
        now_ms,
    )
    .await
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
    let first = write(&ctx, node(1), true).await;
    assert_eq!((first.stamp.counter, first.stamp.node_id), (1, node(1)));
    assert_eq!(first.stamp.write_id, first.id);
    received(&ctx, 7, node(2), true).await;
    let next = write(&ctx, node(1), false).await;
    assert_eq!(next.stamp.counter, 8);
    assert_eq!(outbox_rows(&ctx).await, 2, "every local write is published");
    let shown = local_setting(&ctx, SettingChannel::Webhook, owner()).await;
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
    let build = webhook(false);
    let channel = SettingChannel::Webhook;
    let refused = write_setting(&ctx, node(1), channel, owner(), &build).await;
    assert_eq!(
        refused,
        Err(SettingError::Register(RegisterError::CounterLimit))
    );
    assert_eq!(outbox_rows(&ctx).await, 0);
}

#[tokio::test]
async fn local_view_decides() {
    // The highest stamp on this node decides; an opt-out wins only with a higher stamp.
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    let peers = opening(SECRET);
    let backoffs = Backoffs::default();
    assert!(matches!(
        decide(&ctx, &peers, &backoffs, NOW_MS).await,
        Delivery::Skip
    ));
    received(&ctx, 2, node(2), true).await;
    received(&ctx, 1, node(3), false).await;
    let Delivery::Send(winner) = decide(&ctx, &peers, &backoffs, NOW_MS).await else {
        panic!("the newer opt-in decides");
    };
    assert_eq!(winner.secret.unwrap().expose(), SECRET);
    assert_eq!(
        peers.opens.load(Ordering::SeqCst),
        1,
        "one manager opens the secret"
    );
    received(&ctx, 3, node(3), false).await;
    assert!(matches!(
        decide(&ctx, &peers, &backoffs, NOW_MS).await,
        Delivery::Skip
    ));
    assert_eq!(
        peers.opens.load(Ordering::SeqCst),
        1,
        "an off setting opens nothing"
    );
}

#[tokio::test]
async fn first_manager_opens() {
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    received(&ctx, 1, node(2), true).await;
    let mut peers = opening(SECRET);
    peers.secrets.remove(&managers()[0]);
    let delivery = decide(&ctx, &peers, &Backoffs::default(), NOW_MS).await;
    assert!(matches!(delivery, Delivery::Send(_)));
    assert_eq!(
        peers.opens.load(Ordering::SeqCst),
        2,
        "the next manager in config order opens"
    );
}

#[tokio::test]
async fn no_manager_waits() {
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    received(&ctx, 1, node(2), true).await;
    let peers = CannedPeers::default();
    let backoffs = Backoffs::default();
    let Delivery::Wait(_, delay) = decide(&ctx, &peers, &backoffs, NOW_MS).await else {
        panic!("no manager answers");
    };
    assert_eq!(delay, Duration::from_secs(30));
    assert_eq!(peers.opens.load(Ordering::SeqCst), 3);
    let Delivery::Wait(_, delay) = decide(&ctx, &peers, &backoffs, NOW_MS + 10_000).await else {
        panic!("the backoff still runs");
    };
    assert_eq!(delay, Duration::from_secs(20));
    assert_eq!(
        peers.opens.load(Ordering::SeqCst),
        3,
        "a running backoff asks nobody"
    );
    let (peers, backoffs, record) = (opening(SECRET), Backoffs::default(), notice());
    let unreadable = delivery_setting(
        &ctx,
        &peers,
        &backoffs,
        &record,
        SettingChannel::Webhook,
        None,
        NOW_MS,
    );
    assert!(
        matches!(unreadable.await, Delivery::Wait(..)),
        "no config means no manager"
    );
}

#[test]
fn backoff_shared_owner() {
    let backoffs = Backoffs::default();
    let other = UserId::local(Ulid::from_bytes([7u8; 16]), owner().realm_id);
    assert_eq!(backoffs.fail(owner(), SettingChannel::Email, 1_000), 31_000);
    // Every notice of the owner and channel waits for the same end.
    assert_eq!(
        backoffs.until(owner(), SettingChannel::Email, 2_000),
        Some(31_000)
    );
    assert_eq!(backoffs.until(other, SettingChannel::Email, 2_000), None);
    assert_eq!(
        backoffs.until(owner(), SettingChannel::Webhook, 2_000),
        None
    );
    assert_eq!(backoffs.until(owner(), SettingChannel::Email, 31_000), None);
}

#[test]
fn backoff_doubles_capped() {
    let backoffs = Backoffs::default();
    let channel = SettingChannel::Webhook;
    let delays: Vec<_> = (0..8)
        .map(|_| backoffs.fail(owner(), channel, 0) / 1_000)
        .collect();
    assert_eq!(delays, [30, 60, 120, 240, 480, 960, 1_800, 1_800]);
    backoffs.clear(owner(), channel);
    assert_eq!(
        backoffs.fail(owner(), channel, 0),
        30_000,
        "an opened secret resets the delay"
    );
}
