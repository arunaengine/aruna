use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{NOTIFICATION_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::{
    Actor, NotificationClass, NotificationKind, NotificationOutboxRecord, NotificationRecord,
    RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::notifications::client::{mark_read_remote, unread_count_remote};
use aruna_operations::notifications::dispatch::{
    NotificationDispatchError, list_notifications_for_user, mark_read_for_user,
    unread_count_for_user,
};
use aruna_operations::notifications::emit::{EmitNotificationsInput, EmitNotificationsOperation};
use aruna_operations::notifications::list::LIST_NOTIFICATIONS_MAX_LIMIT;
use aruna_operations::notifications::outbox::NOTIFICATION_DELIVERY_RETRY_AFTER;
use aruna_operations::notifications::placement::resolve_inbox_holder;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

const POLL_TIMEOUT: Duration = Duration::from_secs(60);
const RESPAWN_POLL_TIMEOUT: Duration = Duration::from_secs(120);
const LIST_LIMIT: u32 = LIST_NOTIFICATIONS_MAX_LIMIT as u32;

struct TestNode {
    _temp_dir: Option<TempDir>,
    net: NetHandle,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn notification_emitted_on_a_is_visible_via_b() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([12u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);
    let record = added_to_group_record(recipient, realm_id, unix_timestamp_millis());
    let target = record.notification_id;

    emit_on(&nodes[0], vec![record]).await?;

    wait_for(POLL_TIMEOUT, || {
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
async fn delivery_retries_through_holder_outage() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([22u8; 32]);
    let secrets: [[u8; 32]; 3] = [[11u8; 32], [22u8; 32], [33u8; 32]];
    // The holder's storage must outlive its node struct so the post-outage
    // restart reopens the same persisted dir instead of an empty one.
    let holder_dir = tempfile::tempdir()?;
    let holder_storage =
        FjallStorage::open(holder_dir.path().to_str().ok_or("invalid temp path")?)?;
    let mut nodes = Vec::with_capacity(3);
    nodes.push(spawn_node_with_secret(realm_id, secrets[0]).await?);
    nodes.push(spawn_node_with_secret(realm_id, secrets[1]).await?);
    nodes.push(spawn_node_reusing_storage(realm_id, secrets[2], holder_storage.clone()).await?);
    mesh_nodes(&nodes).await;
    install_realm_config(&nodes, realm_id).await?;

    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);
    let record = added_to_group_record(recipient, realm_id, unix_timestamp_millis());
    let target = record.notification_id;

    nodes[2].net.shutdown().await;
    emit_on(&nodes[0], vec![record]).await?;

    sleep(NOTIFICATION_DELIVERY_RETRY_AFTER + Duration::from_secs(5)).await;
    let retained = outbox_records(&nodes[0]).await;
    assert!(
        retained
            .iter()
            .any(|row| row.record.notification_id == target),
        "outbox must retain an undeliverable record while its holder is offline"
    );

    nodes[2] = spawn_node_reusing_storage(realm_id, secrets[2], holder_storage.clone()).await?;
    mesh_nodes(&nodes).await;
    install_realm_config(&nodes, realm_id).await?;

    wait_for(RESPAWN_POLL_TIMEOUT, || {
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
async fn duplicate_delivery_is_idempotent_across_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([33u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);
    let record = added_to_group_record(recipient, realm_id, unix_timestamp_millis());
    let target = record.notification_id;

    emit_on(&nodes[0], vec![record.clone()]).await?;
    wait_for(POLL_TIMEOUT, || {
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
    wait_for(POLL_TIMEOUT, || {
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
async fn unread_and_mark_read_across_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([44u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let recipient = user_with_holder(&config, holder, realm_id);

    let now = unix_timestamp_millis();
    let records: Vec<NotificationRecord> = (0..3)
        .map(|offset| added_to_group_record(recipient, realm_id, now + offset))
        .collect();
    let ids: Vec<Ulid> = vec![records[0].notification_id, records[1].notification_id];

    emit_on(&nodes[0], records).await?;
    wait_for(POLL_TIMEOUT, || {
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
async fn placement_is_uniform_across_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([55u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;

    for _ in 0..20 {
        let recipient = UserId::local(Ulid::r#gen(), realm_id);
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
async fn dispatch_proxies_inbox_ops_from_non_holder() -> Result<(), Box<dyn std::error::Error>> {
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
        .map(|offset| added_to_group_record(recipient, realm_id, now + offset))
        .collect();
    let ids: Vec<Ulid> = vec![records[0].notification_id, records[1].notification_id];

    emit_on(&nodes[0], records).await?;
    wait_for(POLL_TIMEOUT, || {
        let reader_ctx = reader_ctx.clone();
        async move {
            list_notifications_for_user(
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

    let (count, capped) = unread_count_for_user(reader_ctx.as_ref(), reader_id, recipient).await?;
    assert_eq!((count, capped), (3, false));

    let marked =
        mark_read_for_user(reader_ctx.as_ref(), reader_id, recipient, ids.clone(), None).await?;
    assert_eq!(marked, 2);

    let (count, _) = unread_count_for_user(reader_ctx.as_ref(), reader_id, recipient).await?;
    assert_eq!(count, 1);

    // A resolvable remote holder with no net handle is Unavailable, not a 500.
    let no_net = Arc::new(DriverContext {
        storage_handle: reader_ctx.storage_handle.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
    });
    let unavailable = list_notifications_for_user(
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
    let remote = wait_for_dispatch_error(reader_ctx.as_ref(), reader_id, recipient).await;
    assert!(matches!(remote, NotificationDispatchError::Remote(_)));

    nodes[0].net.shutdown().await;
    nodes[1].net.shutdown().await;
    Ok(())
}

async fn wait_for_dispatch_error(
    context: &DriverContext,
    local_node_id: NodeId,
    recipient: UserId,
) -> NotificationDispatchError {
    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        match list_notifications_for_user(
            context,
            local_node_id,
            recipient,
            None,
            LIST_LIMIT as usize,
        )
        .await
        {
            Err(error) => return error,
            Ok(_) => {
                assert!(Instant::now() < deadline, "holder never became unreachable");
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

fn added_to_group_record(
    recipient: UserId,
    realm_id: RealmId,
    created_at_ms: u64,
) -> NotificationRecord {
    NotificationRecord::new(
        recipient,
        NotificationClass::Direct,
        NotificationKind::AddedToGroup {
            group_id: Ulid::r#gen(),
            actor_user_id: UserId::local(Ulid::r#gen(), realm_id),
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
        let candidate = UserId::local(Ulid::r#gen(), realm_id);
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
    list_notifications_for_user(
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
        GetRealmConfigOperation::new(recipient.realm_id),
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
    build_node(realm_id, None, None).await
}

async fn spawn_node_with_secret(
    realm_id: RealmId,
    secret: [u8; 32],
) -> Result<TestNode, Box<dyn std::error::Error>> {
    build_node(realm_id, Some(secret), None).await
}

async fn spawn_node_reusing_storage(
    realm_id: RealmId,
    secret: [u8; 32],
    storage: StorageHandle,
) -> Result<TestNode, Box<dyn std::error::Error>> {
    build_node(realm_id, Some(secret), Some(storage)).await
}

async fn build_node(
    realm_id: RealmId,
    secret: Option<[u8; 32]>,
    reused_storage: Option<StorageHandle>,
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

    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
    });

    initialize_net_incoming(context.clone());
    initialize_task_incoming(context.clone(), task_handle).await;

    Ok(TestNode {
        _temp_dir: temp_dir,
        net,
        context,
    })
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = realm_config_for(nodes, realm_id);
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
        node.net.refresh_realm_peers_from_document(&config).await?;
    }
    Ok(())
}

async fn wait_for<F, Fut>(
    timeout: Duration,
    mut condition: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition().await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("condition not met before deadline".into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
