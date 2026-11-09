//! Tests quorum reads, the two-phase setting write against three in-process management nodes,
//! and the shared delivery backoff.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::realm_vault::{confirm_setting, read_setting, store_setting};
use crate::tests::notifications::{context, temp_storage};
use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
use aruna_core::realm_vault::{RealmSealKey, seal_setting};
use aruna_core::structs::execution::email::EmailPreference;
use aruna_core::structs::identity::realm::RealmId;
use ed25519_dalek::SigningKey;
use std::collections::{BTreeSet, HashSet};
use tempfile::TempDir;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm_key() -> SigningKey {
    SigningKey::from_bytes(&[5u8; 32])
}

fn owner() -> UserId {
    let realm_id = RealmId::from_bytes(realm_key().verifying_key().to_bytes());
    UserId::local(Ulid::from_bytes([6u8; 16]), realm_id)
}

/// The server node that writes; managers are nodes 1, 2 and 3 in config order.
fn writer() -> NodeId {
    node(4)
}

/// The managers a racing writer stores on, and its document.
type Race = (Vec<NodeId>, DocumentTarget, Vec<u8>);

/// Three management nodes with their own storage. A node in `down` answers nothing, one in
/// `refuse_store` or `refuse_confirm` fails only that phase.
struct Cluster {
    config: RealmConfigDocument,
    key: RealmSealKey,
    nodes: HashMap<NodeId, (TempDir, StorageHandle)>,
    down: Mutex<HashSet<NodeId>>,
    refuse_store: Mutex<HashSet<NodeId>>,
    refuse_confirm: Mutex<HashSet<NodeId>>,
    /// Stored on these nodes after the next quorum read, as a racing writer would.
    race: Mutex<Option<Race>>,
    calls: Mutex<Vec<&'static str>>,
}

impl Cluster {
    fn new() -> Self {
        let realm_id = owner().realm_id;
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        for seed in 1..=3 {
            config.ensure_node(node(seed), RealmNodeKind::Management);
        }
        config.ensure_node(writer(), RealmNodeKind::Server);
        let nodes = (1..=3).map(|seed| (node(seed), temp_storage())).collect();
        Self {
            config,
            key: RealmSealKey::derive(&realm_key()),
            nodes,
            down: Mutex::default(),
            refuse_store: Mutex::default(),
            refuse_confirm: Mutex::default(),
            race: Mutex::default(),
            calls: Mutex::default(),
        }
    }

    fn managers(&self) -> Vec<NodeId> {
        managers(&self.config)
    }

    fn storage(&self, node: NodeId) -> Result<&StorageHandle, MetadataReadError> {
        if self.down.lock().unwrap().contains(&node) {
            return Err(MetadataReadError::Unavailable);
        }
        Ok(&self.nodes[&node].1)
    }

    fn set(set: &Mutex<HashSet<NodeId>>, nodes: &[NodeId]) {
        *set.lock().unwrap() = nodes.iter().copied().collect();
    }
}

#[async_trait::async_trait]
impl SettingPeers for Cluster {
    async fn read(
        &self,
        node: NodeId,
        owner: UserId,
        channel: SettingChannel,
    ) -> Result<SettingAnswer, MetadataReadError> {
        self.calls.lock().unwrap().push("read");
        let answer = read_setting(self.storage(node)?, &self.key, &self.config, owner, channel);
        let answer = answer.await;
        // The racing writer stores after the last read, so this read is already stale.
        if node == self.managers()[1] {
            let race = self.race.lock().unwrap().take();
            if let Some((nodes, target, bytes)) = race {
                for racer in nodes {
                    let change = new_change(writer());
                    let (target, bytes) = (target.clone(), bytes.clone());
                    let storage = &self.nodes[&racer].1;
                    let stored =
                        store_setting(storage, &self.config, writer(), target, bytes, change);
                    assert_eq!(stored.await, Ok(StoreAnswer::Stored));
                }
            }
        }
        answer
    }

    async fn store(
        &self,
        node: NodeId,
        target: DocumentTarget,
        bytes: Vec<u8>,
        change: DocumentChange,
    ) -> Result<StoreAnswer, MetadataReadError> {
        self.calls.lock().unwrap().push("store");
        if self.refuse_store.lock().unwrap().contains(&node) {
            return Err(MetadataReadError::Unavailable);
        }
        let peer = change.current.actor;
        store_setting(
            self.storage(node)?,
            &self.config,
            peer,
            target,
            bytes,
            change,
        )
        .await
    }

    async fn confirm(&self, node: NodeId, target: DocumentTarget) -> Result<(), MetadataReadError> {
        self.calls.lock().unwrap().push("confirm");
        if self.refuse_confirm.lock().unwrap().contains(&node) {
            return Err(MetadataReadError::Unavailable);
        }
        confirm_setting(self.storage(node)?, &self.config, writer(), target).await
    }
}

/// Builds email settings of `owner()`: on with a sealed address, or off.
fn email(on: bool) -> impl Fn(u64, Ulid) -> Result<Vec<u8>, SettingError> + Sync {
    move |version, id| {
        let secret = SecretBytes::new(b"ada@example.org".to_vec());
        let sealed = on.then(|| {
            seal_setting(
                &owner().realm_id,
                SettingChannel::Email,
                &owner(),
                id,
                &secret,
            )
            .unwrap()
        });
        let categories = match on {
            true => BTreeSet::from(["share.upload".to_string()]),
            false => BTreeSet::new(),
        };
        let preference = EmailPreference {
            preference_id: id,
            user_id: owner(),
            version,
            categories,
            sealed,
            created_at_ms: 1,
        };
        Ok(preference.to_bytes().unwrap())
    }
}

async fn write(
    cluster: &Cluster,
    ctx: &DriverContext,
    on: bool,
) -> Result<SettingHead, SettingError> {
    let managers = cluster.managers();
    let channel = SettingChannel::Email;
    let build = email(on);
    let bytes = write_setting(ctx, cluster, writer(), &managers, channel, owner(), &build).await?;
    Ok(channel.head(&bytes).unwrap())
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
async fn write_reads_first() {
    let cluster = Cluster::new();
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    assert_eq!(write(&cluster, &ctx, true).await.unwrap().version, 1);
    let second = write(&cluster, &ctx, false).await.unwrap();
    assert_eq!(
        second.version, 2,
        "the next version is above every version read"
    );
    let calls = cluster.calls.lock().unwrap().clone();
    assert_eq!(&calls[..2], ["read", "read"], "a quorum of three is two");
    assert_eq!(&calls[2..5], ["store", "store", "store"]);
    let local = local_setting(&ctx, SettingChannel::Email, owner())
        .await
        .unwrap();
    assert_eq!(local, None, "the writer shows the published off setting");
}

#[tokio::test]
async fn conflict_bumps_version() {
    let cluster = Cluster::new();
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    let racer = Ulid::from_bytes([9u8; 16]);
    let racing = email(false)(1, racer).unwrap();
    let target = SettingChannel::Email.target(owner(), racer);
    *cluster.race.lock().unwrap() = Some((vec![node(1), node(2)], target, racing));
    let written = write(&cluster, &ctx, true).await.unwrap();
    assert_eq!(
        written.version, 2,
        "a conflict retries above the racing version"
    );
}

#[tokio::test]
async fn publish_after_store() {
    let cluster = Cluster::new();
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    Cluster::set(&cluster.refuse_store, &[node(1), node(2)]);
    assert_eq!(
        write(&cluster, &ctx, true).await,
        Err(SettingError::NoQuorum)
    );
    assert_eq!(
        outbox_rows(&ctx).await,
        0,
        "nothing is published without a store quorum"
    );
    let local = confirmed_settings(&storage, SettingChannel::Email, &owner(), None).await;
    assert!(local.unwrap().is_empty());
    let owner = owner();
    let held = tentative_settings(
        &cluster.nodes[&node(3)].1,
        SettingChannel::Email,
        &owner,
        None,
    );
    assert_eq!(
        held.await.unwrap().len(),
        1,
        "the minority store stays tentative"
    );
}

#[tokio::test]
async fn ack_after_confirm() {
    let cluster = Cluster::new();
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    Cluster::set(&cluster.refuse_confirm, &[node(1), node(2)]);
    assert_eq!(
        write(&cluster, &ctx, true).await,
        Err(SettingError::NoQuorum)
    );
    assert_eq!(
        outbox_rows(&ctx).await,
        1,
        "a store quorum publishes before the confirms"
    );
    let local = local_setting(&ctx, SettingChannel::Email, owner())
        .await
        .unwrap();
    assert!(
        local.is_some(),
        "the published setting is confirmed where it is written"
    );
}

#[tokio::test]
async fn puts_then_delete() {
    // Two failed opt-ins leave on settings v1 and v2 on manager 3 only; an opt-out without it
    // wins every later read.
    let cluster = Cluster::new();
    let (_dir, storage) = temp_storage();
    let ctx = context(&storage);
    Cluster::set(&cluster.refuse_store, &[node(1), node(2)]);
    for _ in 0..2 {
        assert_eq!(
            write(&cluster, &ctx, true).await,
            Err(SettingError::NoQuorum)
        );
    }
    let owner = owner();
    let held = tentative_settings(
        &cluster.nodes[&node(3)].1,
        SettingChannel::Email,
        &owner,
        None,
    );
    let versions: BTreeSet<_> = held
        .await
        .unwrap()
        .iter()
        .map(|(head, _)| head.version)
        .collect();
    assert_eq!(versions, BTreeSet::from([1, 2]));

    Cluster::set(&cluster.refuse_store, &[]);
    Cluster::set(&cluster.down, &[node(3)]);
    let off = write(&cluster, &ctx, false).await.unwrap();
    assert_eq!((off.version, off.on), (1, false));

    Cluster::set(&cluster.down, &[node(2)]);
    let managers = cluster.managers();
    let read = read_quorum(&cluster, &managers, owner, SettingChannel::Email).await;
    let read = read.expect("managers 1 and 3 answer");
    let winner = read.winner.expect("the confirmed off setting decides");
    assert_eq!(
        SettingChannel::Email.head(&winner.bytes).unwrap().id,
        off.id
    );
    assert!(winner.secret.is_none(), "nothing is sent");
    let manager = context(&cluster.nodes[&node(3)].1);
    let shown = local_setting(&manager, SettingChannel::Email, owner);
    assert_eq!(shown.await.unwrap(), None, "manager 3 shows email off");
    let next = write(&cluster, &ctx, true).await.unwrap();
    assert_eq!(
        next.version, 3,
        "the tentative versions raise the next write"
    );
}

#[tokio::test]
async fn read_needs_quorum() {
    let cluster = Cluster::new();
    let managers = cluster.managers();
    Cluster::set(&cluster.down, &[node(1), node(2)]);
    let read = read_quorum(&cluster, &managers, owner(), SettingChannel::Email).await;
    assert_eq!(read, None);
    assert_eq!(
        quorum_size(0),
        1,
        "a realm without managers never has a quorum"
    );
    assert_eq!(
        read_quorum(&cluster, &[], owner(), SettingChannel::Email).await,
        None
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
        "a full quorum resets the delay"
    );
}
