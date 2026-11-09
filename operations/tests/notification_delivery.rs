//! Tests that notifications cross nodes, survive an outage and stay idempotent on retry, and
//! that notification settings decide from a quorum of management nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::document::DocumentEffect;
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{NOTIFICATION_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::realm_vault::RealmSealKey;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationOutboxRecord, NotificationRecord,
};
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::execution::webhook::WebhookSubscription;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions};
use aruna_operations::notifications::client::{mark_read_remote, unread_count_remote};
use aruna_operations::notifications::dispatch::{
    NotificationDispatchError, list_for_user, mark_for_user, unread_for_user,
};
use aruna_operations::notifications::emit::{EmitNotificationsInput, EmitNotificationsOperation};
use aruna_operations::notifications::list::LIST_MAX_LIMIT;
use aruna_operations::notifications::placement::resolve_inbox_holder;
use aruna_operations::notifications::settings::{Delivery, MetadataPeers, delivery_setting};
use aruna_operations::notifications::webhook::{
    WebhookOutcome, read_webhook, remove_webhook, set_webhook,
};
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::sync::incoming::initialize_net_holder;
use aruna_operations::tasks::incoming::{drain_notification_outbox, start_task_queues};
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use tempfile::TempDir;
use ulid::Ulid;

use crate::convergence;
use convergence::wait_for_convergence;

const LIST_LIMIT: u32 = LIST_MAX_LIMIT as u32;

struct TestNode {
    _temp_dir: Option<TempDir>,
    /// Holds the metadata store of a node that serves setting requests.
    _metadata_dir: Option<TempDir>,
    net: NetHandle,
    context: Arc<DriverContext>,
    /// Keeps the inbound handler's scheduled tasks tied to a live owner for
    /// the node's whole lifetime.
    _shutdown: aruna_core::shutdown::Shutdown,
}

#[tokio::test]
async fn notification_crosses_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([12u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);
    let record = added_group_record(recipient, realm_id, unix_timestamp_millis());
    let target = record.notification_id;

    emit_on(&nodes[0], vec![record]).await?;

    wait_for(|| {
        let nodes = &nodes;
        async move {
            list_via(&nodes[1], recipient)
                .await
                .iter()
                .any(|record| record.notification_id == target)
        }
    })
    .await?;

    let (count, capped) = unread_count_remote(&nodes[1].net, holder, recipient).await?;
    assert_eq!((count, capped), (1, false));

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn delivery_retries_outage() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([22u8; 32]);
    let secrets: [[u8; 32]; 3] = [[11u8; 32], [22u8; 32], [33u8; 32]];
    // The holder's storage must outlive its node struct so the post-outage
    // restart reopens the same persisted dir instead of an empty one.
    let holder_dir = tempfile::tempdir()?;
    let holder_storage =
        FjallStorage::open(holder_dir.path().to_str().ok_or("invalid temp path")?)?;
    let mut nodes = Vec::with_capacity(3);
    nodes.push(spawn_with_secret(realm_id, secrets[0]).await?);
    nodes.push(spawn_with_secret(realm_id, secrets[1]).await?);
    nodes.push(respawn_with_storage(realm_id, secrets[2], holder_storage.clone()).await?);
    mesh_nodes(&nodes).await;
    install_realm_config(&nodes, realm_id).await?;

    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);
    let record = added_group_record(recipient, realm_id, unix_timestamp_millis());
    let target = record.notification_id;

    nodes[0]
        .context
        .task_handle
        .as_ref()
        .expect("task handle")
        .clear_inbound_handler()
        .await;
    nodes[2].net.shutdown().await;
    emit_on(&nodes[0], vec![record]).await?;
    drain_notification_outbox(nodes[0].context.clone()).await;
    let retained = outbox_records(&nodes[0]).await;
    assert!(
        retained
            .iter()
            .any(|row| row.record.notification_id == target),
        "outbox must retain an undeliverable record while its holder is offline"
    );

    nodes[2] = respawn_with_storage(realm_id, secrets[2], holder_storage.clone()).await?;
    mesh_nodes(&nodes).await;
    install_realm_config(&nodes, realm_id).await?;
    drain_notification_outbox(nodes[0].context.clone()).await;

    wait_for(|| {
        let nodes = &nodes;
        async move {
            let delivered = list_via(&nodes[2], recipient)
                .await
                .iter()
                .any(|record| record.notification_id == target);
            delivered && outbox_records(&nodes[0]).await.is_empty()
        }
    })
    .await?;

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn duplicate_delivery_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([33u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);
    let record = added_group_record(recipient, realm_id, unix_timestamp_millis());
    let target = record.notification_id;

    emit_on(&nodes[0], vec![record.clone()]).await?;
    wait_for(|| {
        let nodes = &nodes;
        async move {
            list_via(&nodes[2], recipient)
                .await
                .iter()
                .any(|record| record.notification_id == target)
        }
    })
    .await?;

    let marked = mark_read_remote(&nodes[1].net, holder, recipient, vec![target], None).await?;
    assert_eq!(marked, 1);

    emit_on(&nodes[0], vec![record]).await?;
    wait_for(|| {
        let nodes = &nodes;
        async move { outbox_records(&nodes[0]).await.is_empty() }
    })
    .await?;

    let inbox = list_via(&nodes[2], recipient).await;
    let matching: Vec<&NotificationRecord> = inbox
        .iter()
        .filter(|record| record.notification_id == target)
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "duplicate delivery must not fan out rows"
    );
    assert!(
        matching[0].read_at_ms.is_some(),
        "read state must survive a redelivery of the same notification"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn unread_crosses_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([44u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);

    let now = unix_timestamp_millis();
    let records: Vec<NotificationRecord> = (0..3)
        .map(|offset| added_group_record(recipient, realm_id, now + offset))
        .collect();
    let ids: Vec<Ulid> = vec![records[0].notification_id, records[1].notification_id];

    emit_on(&nodes[0], records).await?;
    wait_for(|| {
        let nodes = &nodes;
        async move { list_via(&nodes[1], recipient).await.len() >= 3 }
    })
    .await?;

    let (count, _) = unread_count_remote(&nodes[1].net, holder, recipient).await?;
    assert_eq!(count, 3);

    let marked = mark_read_remote(&nodes[1].net, holder, recipient, ids.clone(), None).await?;
    assert_eq!(marked, 2);

    let (count, _) = unread_count_remote(&nodes[1].net, holder, recipient).await?;
    assert_eq!(count, 1);

    let (count_from_a, _) = unread_count_remote(&nodes[0].net, holder, recipient).await?;
    assert_eq!(count_from_a, 1);

    let marked_again = mark_read_remote(&nodes[1].net, holder, recipient, ids, None).await?;
    assert_eq!(marked_again, 0);

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn placement_converges() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([55u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;

    for _ in 0..20 {
        let recipient = UserId::local(Ulid::generate(), realm_id);
        let mut holders = Vec::with_capacity(nodes.len());
        for node in &nodes {
            holders.push(resolve_holder_on(node, recipient).await?);
        }
        assert!(
            holders.windows(2).all(|pair| pair[0] == pair[1]),
            "nodes disagree on holder for {recipient}: {holders:?}"
        );
    }

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn dispatch_proxies_nonholder() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([66u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);
    // Neither the emitter nor the holder: every op it serves must proxy one hop
    // to the holder through the dispatch remote arms.
    let reader_ctx = nodes[1].context.clone();
    let reader_id = nodes[1].net.node_id();

    let now = unix_timestamp_millis();
    let records: Vec<NotificationRecord> = (0..3)
        .map(|offset| added_group_record(recipient, realm_id, now + offset))
        .collect();
    let ids: Vec<Ulid> = vec![records[0].notification_id, records[1].notification_id];

    emit_on(&nodes[0], records).await?;
    wait_for(|| {
        let reader_ctx = reader_ctx.clone();
        async move {
            list_for_user(
                reader_ctx.as_ref(),
                reader_id,
                recipient,
                None,
                LIST_LIMIT as usize,
            )
            .await
            .map(|(records, _)| records.len() >= 3)
            .unwrap_or(false)
        }
    })
    .await?;

    let (count, capped) = unread_for_user(reader_ctx.as_ref(), reader_id, recipient).await?;
    assert_eq!((count, capped), (3, false));

    let marked =
        mark_for_user(reader_ctx.as_ref(), reader_id, recipient, ids.clone(), None).await?;
    assert_eq!(marked, 2);

    let (count, _) = unread_for_user(reader_ctx.as_ref(), reader_id, recipient).await?;
    assert_eq!(count, 1);

    // A resolvable remote holder with no net handle is Unavailable, not a 500.
    let no_net = Arc::new(DriverContext {
        storage_handle: reader_ctx.storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let unavailable = list_for_user(
        no_net.as_ref(),
        reader_id,
        recipient,
        None,
        LIST_LIMIT as usize,
    )
    .await
    .expect_err("missing net handle must be Unavailable");
    assert!(matches!(
        unavailable,
        NotificationDispatchError::Unavailable
    ));

    // An unreachable holder surfaces as a Remote proxy error (mapped to 502).
    nodes[2].net.shutdown().await;
    let remote = wait_dispatch_error(reader_ctx.as_ref(), reader_id, recipient).await;
    assert!(matches!(remote, NotificationDispatchError::Remote(_)));

    nodes[0].net.shutdown().await;
    nodes[1].net.shutdown().await;
    Ok(())
}

async fn wait_dispatch_error(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
) -> NotificationDispatchError {
    wait_for_convergence("holder never became unreachable", || async {
        Ok::<usize, Box<dyn std::error::Error>>(usize::from(
            list_for_user(context, local_node_id, recipient, None, LIST_LIMIT as usize)
                .await
                .is_ok(),
        ))
    })
    .await
    .expect("holder never became unreachable");
    match list_for_user(context, local_node_id, recipient, None, LIST_LIMIT as usize).await {
        Err(error) => error,
        Ok(_) => panic!("holder never became unreachable"),
    }
}

fn added_group_record(
    recipient: UserId,
    realm_id: RealmId,
    created_at_ms: u64,
) -> NotificationRecord {
    NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::AddedToGroup {
            group_id: Ulid::generate(),
            actor_user_id: UserId::local(Ulid::generate(), realm_id),
        },
        created_at_ms,
    )
}

fn realm_config_for(nodes: &[TestNode], realm_id: RealmId) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }
    config
}

fn user_with_holder(
    realm_config: &RealmConfigDocument,
    holder: NodeId,
    realm_id: RealmId,
) -> UserId {
    for _ in 0..10_000 {
        let candidate = UserId::local(Ulid::generate(), realm_id);
        if matches!(resolve_inbox_holder(&candidate, realm_config), Ok(Some(node)) if node == holder)
        {
            return candidate;
        }
    }
    panic!("no user hashed to holder {holder} within the sampling bound");
}

async fn emit_on(
    node: &TestNode,
    records: Vec<NotificationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    drive(
        EmitNotificationsOperation::new(EmitNotificationsInput { records }),
        node.context.as_ref(),
    )
    .await?;
    Ok(())
}

async fn list_via(node: &TestNode, recipient: UserId) -> Vec<NotificationRecord> {
    list_for_user(
        node.context.as_ref(),
        node.net.node_id(),
        recipient,
        None,
        LIST_LIMIT as usize,
    )
    .await
    .expect("dispatch list notifications")
    .0
}

async fn resolve_holder_on(
    node: &TestNode,
    recipient: UserId,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    let config = drive(
        GetConfigOperation::new(recipient.realm_id),
        node.context.as_ref(),
    )
    .await?;
    Ok(resolve_inbox_holder(&recipient, &config)?.ok_or("no eligible inbox holder")?)
}

async fn outbox_records(node: &TestNode) -> Vec<NotificationOutboxRecord> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1024,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| postcard::from_bytes(&value).expect("valid outbox row"))
            .collect(),
        other => panic!("unexpected outbox iter event: {other:?}"),
    }
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(*realm_id).await?);
    }
    mesh_nodes(&nodes).await;
    install_realm_config(&nodes, *realm_id).await?;
    Ok(nodes)
}

async fn mesh_nodes(nodes: &[TestNode]) {
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            nodes[i]
                .net
                .add_peer_addr(nodes[j].net.endpoint_addr())
                .await;
            nodes[j]
                .net
                .add_peer_addr(nodes[i].net.endpoint_addr())
                .await;
        }
    }
}

async fn spawn_node(realm_id: RealmId) -> Result<TestNode, Box<dyn std::error::Error>> {
    build_node(realm_id, None, None, None).await
}

async fn spawn_with_secret(
    realm_id: RealmId,
    secret: [u8; 32],
) -> Result<TestNode, Box<dyn std::error::Error>> {
    build_node(realm_id, Some(secret), None, None).await
}

async fn respawn_with_storage(
    realm_id: RealmId,
    secret: [u8; 32],
    storage: StorageHandle,
) -> Result<TestNode, Box<dyn std::error::Error>> {
    build_node(realm_id, Some(secret), Some(storage), None).await
}

/// `metadata` adds a metadata handle, which serves and sends notification setting requests.
async fn build_node(
    realm_id: RealmId,
    secret: Option<[u8; 32]>,
    reused_storage: Option<StorageHandle>,
    metadata: Option<MetadataHandleOptions>,
) -> Result<TestNode, Box<dyn std::error::Error>> {
    let (temp_dir, storage) = match reused_storage {
        Some(storage) => (None, storage),
        None => {
            let temp_dir = tempfile::tempdir()?;
            let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
            (Some(temp_dir), storage)
        }
    };
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            secret_key: secret.map(|bytes| iroh::SecretKey::from_bytes(&bytes)),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let (metadata_dir, metadata_handle) = match metadata {
        Some(options) => {
            let dir = tempfile::tempdir()?;
            let handle = MetadataHandle::new_with_options(
                dir.path().join("metadata"),
                net.node_id(),
                storage.clone(),
                Some(net.clone()),
                Some(net.document_sync_node()),
                Some(net.document_sync_database()),
                options,
            )?;
            (Some(dir), Some(handle))
        }
        None => (None, None),
    };

    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });

    let jobs_runtime = aruna_operations::jobs::runtime::JobsRuntime::new();
    let shutdown = aruna_core::shutdown::Shutdown::new();
    initialize_net_holder(
        context.clone(),
        aruna_core::structs::execution::job::RoCrateLimits::default(),
        jobs_runtime.clone(),
        &shutdown,
    );
    start_task_queues(context.clone(), task_handle, jobs_runtime, &shutdown).await;

    Ok(TestNode {
        _temp_dir: temp_dir,
        _metadata_dir: metadata_dir,
        net,
        context,
        _shutdown: shutdown,
    })
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    install_config(nodes, &realm_config_for(nodes, realm_id)).await
}

async fn install_config(
    nodes: &[TestNode],
    config: &RealmConfigDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = config.realm_id;
    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let bytes = config.to_bytes(&actor)?;
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: (*realm_id.as_bytes()).into(),
                value: bytes.into(),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => return Err(format!("unexpected realm config write event: {other:?}").into()),
        }
        node.net.refresh_document_peers(config).await?;
    }
    // Config apply hook: the shard's rank-0 holder eagerly creates each
    // shard topic genesis (mirrors the production realm-config apply path).
    for node in nodes {
        aruna_operations::placement::process_placements::process_shard_placements(
            &node.context,
            realm_id,
            node.net.node_id(),
        )
        .await;
    }
    Ok(())
}

async fn wait_for<F, Fut>(condition: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    wait_for_convergence("condition not met before deadline", || async {
        Ok(usize::from(!condition().await))
    })
    .await
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}

const HOOK_URL: &str = "https://hooks.example.org/aruna";

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Management nodes A, B and C and the server S of a realm whose id is the realm key.
struct SettingRealm {
    seed: u8,
    realm_id: RealmId,
    realm_key: SigningKey,
    nodes: Vec<TestNode>,
}

impl SettingRealm {
    async fn spawn(seed: u8) -> TestResult<Self> {
        let realm_key = SigningKey::from_bytes(&[seed; 32]);
        let realm_id = RealmId::from_bytes(realm_key.verifying_key().to_bytes());
        let mut realm = Self {
            seed,
            realm_id,
            realm_key,
            nodes: Vec::new(),
        };
        for index in 0..4 {
            let node = realm.start(index).await?;
            realm.nodes.push(node);
        }
        mesh_nodes(&realm.nodes).await;
        install_config(&realm.nodes, &realm.config()).await?;
        // Onboarding admits every infrastructure node to the shared realm topics; here S mints
        // the topic and the managers pull it from S.
        let peers = (0..4).map(|index| realm.id(index)).collect::<Vec<_>>();
        let writer = &realm.nodes[3].net;
        writer
            .ensure_sync_topics(&[realm.topic()], peers.clone())
            .await?;
        writer.allow_topic_peers(&[realm.topic()], peers).await?;
        for index in 0..3 {
            realm.pull(index, 3).await;
        }
        Ok(realm)
    }

    async fn start(&self, index: usize) -> TestResult<TestNode> {
        let secret = [self.seed.wrapping_mul(4).wrapping_add(index as u8 + 1); 32];
        let mut options = MetadataHandleOptions::default();
        if index < 3 {
            options = options.with_seal_key(RealmSealKey::derive(&self.realm_key));
        }
        build_node(self.realm_id, Some(secret), None, Some(options)).await
    }

    fn id(&self, index: usize) -> NodeId {
        self.nodes[index].net.node_id()
    }

    fn managers(&self) -> Vec<NodeId> {
        (0..3).map(|index| self.id(index)).collect()
    }

    fn config(&self) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(self.realm_id, Vec::new());
        config.seed_default_placement();
        for index in 0..3 {
            config.ensure_node(self.id(index), RealmNodeKind::Management);
        }
        config.ensure_node(self.id(3), RealmNodeKind::Server);
        config
    }

    fn context(&self, index: usize) -> &DriverContext {
        self.nodes[index].context.as_ref()
    }

    fn topic(&self) -> ::irokle::TopicId {
        let owner = UserId::nil(self.realm_id);
        SettingChannel::Webhook
            .target(owner, Ulid::nil())
            .sync_topic_id(self.realm_id, &PlacementRef::NIL)
    }

    /// Pulls the realm settings topic on node `index` from node `from`.
    async fn pull(&self, index: usize, from: usize) {
        let effect = DocumentEffect::SyncDocument {
            topic: self.topic(),
            peers: vec![self.id(from)],
        };
        let net = &self.nodes[index].net;
        net.send_effect(Effect::Net(NetEffect::DocumentSync(effect)))
            .await;
    }

    /// Pulls on node `index` from every node in `from` until its setting satisfies `done`.
    async fn sync_until<F>(
        &self,
        index: usize,
        from: &[usize],
        owner: UserId,
        done: F,
    ) -> TestResult<()>
    where
        F: Fn(Option<WebhookSubscription>) -> bool,
    {
        wait_for(|| async {
            for source in from {
                self.pull(index, *source).await;
            }
            read_webhook(self.context(index), owner)
                .await
                .is_ok_and(&done)
        })
        .await
    }

    /// The delivery decision of node `index` for a notice of `owner`.
    async fn decide(&self, index: usize, owner: UserId) -> Delivery {
        let record = NotificationRecord::new(
            owner,
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::generate(),
                actor_user_id: owner,
            },
            unix_timestamp_millis(),
        );
        let context = self.context(index);
        let metadata = context.metadata_handle.as_ref().expect("metadata handle");
        let peers = MetadataPeers {
            ctx: context,
            local: self.id(index),
        };
        let managers = self.managers();
        let channel = SettingChannel::Webhook;
        let backoffs = metadata.setting_backoffs();
        let now = unix_timestamp_millis();
        let managers = Some(managers.as_slice());
        delivery_setting(context, &peers, backoffs, &record, channel, managers, now).await
    }

    async fn set(&self, index: usize, owner: UserId, url: &str) -> TestResult<WebhookOutcome> {
        let set = set_webhook(self.context(index), self.id(index), owner, url.to_string());
        Ok(set.await?)
    }

    async fn shutdown(self) {
        shutdown_nodes(self.nodes).await;
    }
}

#[tokio::test]
async fn settings_follow_inbox() -> TestResult<()> {
    // A setting written on S reaches every node, and each node decides from its own view and
    // opens the secret on a management node, so a moved inbox delivers the same.
    let realm = SettingRealm::spawn(82).await?;
    let owner = UserId::local(Ulid::generate(), realm.realm_id);
    let outcome = realm.set(3, owner, HOOK_URL).await?;
    let written = Some(outcome.subscription.clone());
    for index in 0..3 {
        realm
            .sync_until(index, &[3], owner, |current| current == written)
            .await?;
    }
    for index in 0..4 {
        let Delivery::Send(winner) = realm.decide(index, owner).await else {
            panic!("node {index} decides to send");
        };
        let secret = hex::encode(winner.secret.ok_or("secret")?.expose());
        assert_eq!(secret, outcome.secret, "node {index} opens the same secret");
    }
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn racing_writes_converge() -> TestResult<()> {
    // A and S write without seeing each other; once both writes arrive, every node keeps the one
    // with the higher stamp.
    let realm = SettingRealm::spawn(83).await?;
    let owner = UserId::local(Ulid::generate(), realm.realm_id);
    let first = realm.set(0, owner, HOOK_URL).await?.subscription;
    let second = realm
        .set(3, owner, "https://hooks.example.org/other")
        .await?
        .subscription;
    assert_eq!((first.stamp.counter, second.stamp.counter), (1, 1));
    let kept = [first, second].into_iter().max_by_key(|write| write.stamp);
    for index in 0..4 {
        realm
            .sync_until(index, &[0, 3], owner, |current| current == kept)
            .await?;
    }
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn opt_out_wins() -> TestResult<()> {
    // B turns the webhook off after it saw S turn it on, so the opt-out has the higher stamp.
    let realm = SettingRealm::spawn(84).await?;
    let owner = UserId::local(Ulid::generate(), realm.realm_id);
    let on = Some(realm.set(3, owner, HOOK_URL).await?.subscription);
    realm
        .sync_until(1, &[3], owner, |current| current == on)
        .await?;
    remove_webhook(realm.context(1), realm.id(1), owner).await?;
    for index in [0, 2, 3] {
        realm
            .sync_until(index, &[1, 3], owner, |current| current.is_none())
            .await?;
    }
    for index in 0..4 {
        let decision = realm.decide(index, owner).await;
        assert!(
            matches!(decision, Delivery::Skip),
            "node {index} sends nothing"
        );
    }
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn one_manager_opens() -> TestResult<()> {
    // One reachable management node is enough to open the secret.
    let realm = SettingRealm::spawn(85).await?;
    let owner = UserId::local(Ulid::generate(), realm.realm_id);
    let outcome = realm.set(3, owner, HOOK_URL).await?;
    realm.nodes[0].net.shutdown().await;
    realm.nodes[1].net.shutdown().await;
    let Delivery::Send(winner) = realm.decide(3, owner).await else {
        panic!("C opens the secret");
    };
    let secret = hex::encode(winner.secret.ok_or("secret")?.expose());
    assert_eq!(secret, outcome.secret);
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn management_outage_waits() -> TestResult<()> {
    // Writes need no management node; a delivery waits while none opens the secret.
    let realm = SettingRealm::spawn(86).await?;
    let owner = UserId::local(Ulid::generate(), realm.realm_id);
    for node in &realm.nodes[..3] {
        node.net.shutdown().await;
    }
    let outcome = realm.set(3, owner, HOOK_URL).await?;
    assert_eq!(outcome.subscription.stamp.counter, 1);
    let Delivery::Wait(_, delay) = realm.decide(3, owner).await else {
        panic!("a delivery without a management node waits");
    };
    assert_eq!(delay, std::time::Duration::from_secs(30));
    realm.shutdown().await;
    Ok(())
}
