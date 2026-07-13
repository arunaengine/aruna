use std::sync::Arc;
use std::time::Duration;

use aruna_core::document::{DocumentSyncNetEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, DOCUMENT_SYNC_OUTBOX_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE,
    NOTIFICATION_WATCH_INTEREST_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::structs::{
    Actor, GroupAuthorizationDocument, NOTIFICATION_WATCH_PER_USER_CAP, NotificationClass,
    NotificationKind, NotificationRecord, PlacementRef, RealmAuthorizationDocument,
    RealmConfigDocument, RealmId, RealmNodeKind, WatchEvent, WatchEventDetail, WatchEventKind,
    WatchEventMask, WatchInterestDigest, WatchSubscription, data_watch_resource_path,
    watch_interest_node_key, watch_notification_id,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_core::{DocumentSyncEffect, NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::notifications::dispatch::{
    WatchDispatchError, create_watch_for_user, delete_watch_for_user, list_notifications_for_user,
    list_watches_for_user,
};
use aruna_operations::notifications::list::LIST_NOTIFICATIONS_MAX_LIMIT;
use aruna_operations::notifications::placement::resolve_inbox_holder;
use aruna_operations::notifications::watch::emit::emit_resource_watch_event;
use aruna_operations::notifications::watch::interest::{
    ensure_local_watch_interest_digest, mark_watch_interest_dirty,
    refresh_watch_interest_for_targets,
};
use aruna_operations::notifications::watch::subscriptions::list_watch_subscriptions;
use aruna_operations::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsOperation,
};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use tokio::time::Instant;
use tokio::time::sleep;
use ulid::Ulid;

// Positive delivery waits poll to a condition; the ceiling only bounds a
// genuine hang. With the outbox drain and placement pulls retrying on the
// queue scale delivery measures single-digit seconds under contention, so 60s
// is a generous backstop for a loaded CI runner, not a latency budget.
const POLL_TIMEOUT: Duration = Duration::from_secs(60);
const LIST_LIMIT: usize = LIST_NOTIFICATIONS_MAX_LIMIT;
// Interest publication is debounced by 2s, so a few seconds comfortably bounds
// the window an erroneous delivery would need to land in for negative assertions.
const NEGATIVE_WAIT: Duration = Duration::from_secs(5);

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

// The Definition of Done: a watch created via node A fires for an upload handled
// by node B and is visible within seconds through node C. The watch owner's inbox
// holder is node A, so node B only learns the interest through the replicated
// digest, forwards the origin event over the wire, and node C reads it back by
// proxying to the holder.
#[tokio::test]
async fn watch_on_node_a_fires_for_upload_on_node_b_visible_via_node_c()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([90u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[0].net.node_id();
    let watcher = user_with_holder(&config, holder, realm_id);
    let uploader = UserId::local(Ulid::r#gen(), realm_id);
    let group_id = Ulid::r#gen();
    let data_node_id = nodes[1].net.node_id();
    install_group_authorization(&nodes, realm_id, group_id, watcher).await?;
    let prefix = data_watch_resource_path(group_id, data_node_id, "bucket", "reports/");
    let probe = data_watch_resource_path(group_id, data_node_id, "bucket", "reports/probe");

    let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
    let subscription = create_watch_via(&nodes[0], watcher, &prefix, mask).await?;

    // Node B must observe the holder's interest before the upload matches.
    wait_for_holder(
        &nodes[1],
        realm_id,
        &probe,
        WatchEventKind::DataUploaded,
        holder,
        true,
    )
    .await?;

    let event_id = Ulid::r#gen();
    let occurred_at_ms = unix_timestamp_millis();
    let event = upload_event(
        event_id,
        realm_id,
        uploader,
        UploadLocation {
            group_id,
            node_id: data_node_id,
            bucket: "bucket",
            key: "reports/q3/summary.csv",
        },
        occurred_at_ms,
    );
    emit_resource_watch_event(nodes[1].context.as_ref(), event).await;

    let expected_id = watch_notification_id(event_id, subscription.watch_id);
    wait_for(POLL_TIMEOUT, || {
        let node_c = &nodes[2];
        async move {
            list_via(node_c, watcher)
                .await
                .iter()
                .any(|record| record.notification_id == expected_id)
        }
    })
    .await?;

    let listed = list_via(&nodes[2], watcher).await;
    let record = listed
        .iter()
        .find(|record| record.notification_id == expected_id)
        .expect("watch record present via node C");
    assert_eq!(record.recipient, watcher);
    // Transient class routes the record through the same prune cap/TTL path as
    // every other transient notification (prune.rs); a class assertion suffices.
    assert_eq!(record.class, NotificationClass::Transient);
    assert_eq!(record.created_at_ms, occurred_at_ms);
    assert!(
        record.read_at_ms.is_none(),
        "a fresh watch record is unread"
    );
    assert_eq!(record.kind.category(), "resource.watch");
    assert_eq!(record.kind.name(), "data_uploaded");
    match &record.kind {
        NotificationKind::DataUploaded {
            path,
            group_id: event_group_id,
            node_id: event_node_id,
            bucket,
            key,
            size_bytes,
            actor_user_id,
        } => {
            assert_eq!(
                path,
                &data_watch_resource_path(
                    group_id,
                    data_node_id,
                    "bucket",
                    "reports/q3/summary.csv"
                )
            );
            assert_eq!(*event_group_id, group_id);
            assert_eq!(*event_node_id, data_node_id);
            assert_eq!(bucket, "bucket");
            assert_eq!(key, "reports/q3/summary.csv");
            assert_eq!(*size_bytes, 2048);
            assert_eq!(*actor_user_id, uploader);
        }
        other => panic!("unexpected notification kind: {other:?}"),
    }

    // Exactly one record: no fan-out to a second row.
    assert_eq!(
        listed
            .iter()
            .filter(|record| record.notification_id == expected_id)
            .count(),
        1
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

// An event whose path matches no watched prefix is not forwarded, and no inbox
// record materializes anywhere.
#[tokio::test]
async fn unmatched_event_writes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([91u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[0].net.node_id();
    let watcher = user_with_holder(&config, holder, realm_id);
    let uploader = UserId::local(Ulid::r#gen(), realm_id);
    let group_id = Ulid::r#gen();
    let data_node_id = nodes[1].net.node_id();
    install_group_authorization(&nodes, realm_id, group_id, watcher).await?;
    let prefix = data_watch_resource_path(group_id, data_node_id, "bucket", "reports/");
    let probe = data_watch_resource_path(group_id, data_node_id, "bucket", "reports/probe");

    let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
    create_watch_via(&nodes[0], watcher, &prefix, mask).await?;
    wait_for_holder(
        &nodes[1],
        realm_id,
        &probe,
        WatchEventKind::DataUploaded,
        holder,
        true,
    )
    .await?;

    // Path is outside every watched prefix even though real interest exists.
    let event = upload_event(
        Ulid::r#gen(),
        realm_id,
        uploader,
        UploadLocation {
            group_id,
            node_id: data_node_id,
            bucket: "logs",
            key: "app/2026.log",
        },
        unix_timestamp_millis(),
    );
    emit_resource_watch_event(nodes[1].context.as_ref(), event).await;

    sleep(NEGATIVE_WAIT).await;
    for node in &nodes {
        assert_eq!(
            inbox_len(node).await,
            0,
            "an unmatched event must not write any inbox record"
        );
    }
    assert!(list_via(&nodes[2], watcher).await.is_empty());

    shutdown_nodes(nodes).await;
    Ok(())
}

// A matching event whose actor is the watcher itself is forwarded directly, but
// the holder's expansion suppresses self-notify, so the watcher's inbox stays empty.
#[tokio::test]
async fn self_authored_event_notifies_no_one() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([92u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[0].net.node_id();
    let watcher = user_with_holder(&config, holder, realm_id);
    let group_id = Ulid::r#gen();
    let data_node_id = nodes[1].net.node_id();
    install_group_authorization(&nodes, realm_id, group_id, watcher).await?;
    let prefix = data_watch_resource_path(group_id, data_node_id, "bucket", "reports/");
    let probe = data_watch_resource_path(group_id, data_node_id, "bucket", "reports/probe");

    let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
    create_watch_via(&nodes[0], watcher, &prefix, mask).await?;
    wait_for_holder(
        &nodes[1],
        realm_id,
        &probe,
        WatchEventKind::DataUploaded,
        holder,
        true,
    )
    .await?;

    // Actor == watcher and the path matches, so the origin forwards the event,
    // proving the empty inbox is self-notify suppression rather than a missed match.
    let event = upload_event(
        Ulid::r#gen(),
        realm_id,
        watcher,
        UploadLocation {
            group_id,
            node_id: data_node_id,
            bucket: "bucket",
            key: "reports/self.csv",
        },
        unix_timestamp_millis(),
    );
    emit_resource_watch_event(nodes[1].context.as_ref(), event).await;

    sleep(NEGATIVE_WAIT).await;
    for node in &nodes {
        assert_eq!(
            inbox_len(node).await,
            0,
            "a self-authored event must not notify the watcher"
        );
    }
    assert!(list_via(&nodes[2], watcher).await.is_empty());

    shutdown_nodes(nodes).await;
    Ok(())
}

// Digest convergence in both directions: a remote create makes the holder's entry
// appear in a peer's interest table, and deleting the last watch publishes an
// empty digest that retracts the holder from that table.
#[tokio::test]
async fn digest_converges_and_retracts_across_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([93u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[2].net.node_id();
    let watcher = user_with_holder(&config, holder, realm_id);
    let group_id = Ulid::r#gen();
    let prefix = format!("meta/{group_id}/datasets/team");
    let probe = format!("{prefix}/runs/run-42");
    install_group_authorization(&nodes, realm_id, group_id, watcher).await?;

    // Create through node A while the holder is node C: exercises the remote
    // create arm on the way to publishing the digest.
    let mask = WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]);
    let subscription = create_watch_via(&nodes[0], watcher, &prefix, mask).await?;

    wait_for_holder(
        &nodes[1],
        realm_id,
        &probe,
        WatchEventKind::MetadataCreated,
        holder,
        true,
    )
    .await?;

    // Deleting the last watch retracts the interest via an empty digest.
    delete_watch_via(&nodes[0], watcher, subscription.watch_id).await?;

    wait_for_holder(
        &nodes[1],
        realm_id,
        &probe,
        WatchEventKind::MetadataCreated,
        holder,
        false,
    )
    .await?;
    assert!(
        nodes[1]
            .net
            .watch_interest_snapshot()
            .nodes(realm_id)
            .is_none(),
        "the realm's interest entry drops once its last node retracts"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

// The per-user watch cap is enforced on the holder. Filling it through a
// non-holder node and then overflowing proves the holder's cap sentinel string
// round-trips over the wire and maps back to the typed `CapExceeded`, not a
// generic remote-proxy failure.
#[tokio::test]
async fn remote_create_surfaces_cap_conflict_over_the_wire()
-> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([94u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let config = realm_config_for(&nodes, realm_id);
    let holder = nodes[0].net.node_id();
    let owner = user_with_holder(&config, holder, realm_id);
    let mask = WatchEventMask::from_kinds([WatchEventKind::DataUploaded]);
    let group_id = Ulid::r#gen();
    let data_node_id = nodes[1].net.node_id();

    // Node B is not the holder, so every create proxies to node A over the wire.
    for index in 0..NOTIFICATION_WATCH_PER_USER_CAP {
        create_watch_for_user(
            nodes[1].context.as_ref(),
            nodes[1].net.node_id(),
            owner,
            data_watch_resource_path(group_id, data_node_id, "bucket", &format!("{index}/")),
            mask,
        )
        .await
        .expect("remote create under cap succeeds");
    }

    let error = create_watch_for_user(
        nodes[1].context.as_ref(),
        nodes[1].net.node_id(),
        owner,
        data_watch_resource_path(group_id, data_node_id, "bucket", "overflow/"),
        mask,
    )
    .await
    .expect_err("cap must be enforced over the wire");
    assert!(
        matches!(error, WatchDispatchError::CapExceeded),
        "expected CapExceeded, got {error:?}"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn subscription_survives_inbox_holder_rerank() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([95u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 3).await?;
    let full_config = realm_config_for(&nodes, realm_id);
    let mut reduced_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    for node in &nodes[..2] {
        reduced_config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }
    install_config_document(&nodes, realm_id, &reduced_config).await?;

    let (watcher, old_holder, new_holder) =
        user_with_changed_holder(&reduced_config, &full_config, realm_id);
    let group_id = Ulid::r#gen();
    let data_node_id = nodes[0].net.node_id();
    let prefix = data_watch_resource_path(group_id, data_node_id, "bucket", "reranked/");
    let probe = format!("{prefix}probe");
    install_group_authorization(&nodes, realm_id, group_id, watcher).await?;

    let subscription = create_watch_via(
        &nodes[0],
        watcher,
        &prefix,
        WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
    )
    .await?;
    let old_holder_node = node_by_id(&nodes, old_holder);
    wait_for(POLL_TIMEOUT, || async {
        list_watch_subscriptions(&old_holder_node.context.storage_handle, watcher)
            .await
            .is_ok_and(|rows| rows.contains(&subscription))
    })
    .await?;
    // The row and outbox record commit atomically; wait until the record has
    // published into topic history before changing which node holds the inbox.
    wait_for(POLL_TIMEOUT, || async {
        iter_len(old_holder_node, DOCUMENT_SYNC_OUTBOX_KEYSPACE).await == 0
    })
    .await?;

    install_config_document(&nodes, realm_id, &full_config).await?;
    let new_holder_node = node_by_id(&nodes, new_holder);
    let sync_event = new_holder_node
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::SyncDocument {
                topic: DocumentSyncTarget::WatchInterest {
                    realm_id,
                    node_id: old_holder,
                }
                .sync_topic_id(realm_id, &PlacementRef::NIL),
                peers: vec![old_holder],
            },
        )))
        .await;
    let reconciled = match sync_event {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
            targets,
            ..
        })) => targets,
        other => return Err(format!("unexpected subscription history sync: {other:?}").into()),
    };
    refresh_watch_interest_for_targets(new_holder_node.context.as_ref(), &reconciled).await;
    for node in &nodes {
        mark_watch_interest_dirty(node.context.as_ref(), realm_id).await?;
    }

    wait_for(POLL_TIMEOUT, || async {
        list_watch_subscriptions(&new_holder_node.context.storage_handle, watcher)
            .await
            .is_ok_and(|rows| rows.contains(&subscription))
    })
    .await?;
    wait_for(POLL_TIMEOUT, || async {
        nodes[1].net.watch_interest_snapshot().matching_nodes(
            realm_id,
            &probe,
            WatchEventKind::DataUploaded,
        ) == vec![new_holder]
    })
    .await?;
    assert_eq!(
        list_watches_for_user(nodes[1].context.as_ref(), nodes[1].net.node_id(), watcher).await?,
        vec![subscription.clone()]
    );

    let event_id = Ulid::r#gen();
    emit_resource_watch_event(
        nodes[1].context.as_ref(),
        upload_event(
            event_id,
            realm_id,
            UserId::local(Ulid::r#gen(), realm_id),
            UploadLocation {
                group_id,
                node_id: data_node_id,
                bucket: "bucket",
                key: "reranked/object",
            },
            unix_timestamp_millis(),
        ),
    )
    .await;
    let expected_id = watch_notification_id(event_id, subscription.watch_id);
    wait_for(POLL_TIMEOUT, || async {
        list_via(&nodes[0], watcher)
            .await
            .iter()
            .any(|record| record.notification_id == expected_id)
    })
    .await?;

    delete_watch_via(&nodes[0], watcher, subscription.watch_id).await?;
    wait_for(POLL_TIMEOUT, || async {
        list_watches_for_user(nodes[1].context.as_ref(), nodes[1].net.node_id(), watcher)
            .await
            .is_ok_and(|rows| rows.is_empty())
    })
    .await?;

    shutdown_nodes(nodes).await;
    Ok(())
}

struct UploadLocation<'a> {
    group_id: Ulid,
    node_id: NodeId,
    bucket: &'a str,
    key: &'a str,
}

fn upload_event(
    event_id: Ulid,
    realm_id: RealmId,
    actor: UserId,
    location: UploadLocation<'_>,
    occurred_at_ms: u64,
) -> WatchEvent {
    let UploadLocation {
        group_id,
        node_id,
        bucket,
        key,
    } = location;
    WatchEvent {
        event_id,
        realm_id,
        kind: WatchEventKind::DataUploaded,
        path: data_watch_resource_path(group_id, node_id, bucket, key),
        actor,
        occurred_at_ms,
        detail: WatchEventDetail::DataUploaded {
            group_id,
            node_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            size_bytes: 2048,
        },
    }
}

async fn create_watch_via(
    node: &TestNode,
    owner: UserId,
    path_prefix: &str,
    event_mask: WatchEventMask,
) -> Result<WatchSubscription, Box<dyn std::error::Error>> {
    Ok(create_watch_for_user(
        node.context.as_ref(),
        node.net.node_id(),
        owner,
        path_prefix.to_string(),
        event_mask,
    )
    .await?)
}

async fn delete_watch_via(
    node: &TestNode,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), Box<dyn std::error::Error>> {
    delete_watch_for_user(node.context.as_ref(), node.net.node_id(), owner, watch_id).await?;
    Ok(())
}

async fn wait_for_holder(
    node: &TestNode,
    realm_id: RealmId,
    probe_path: &str,
    kind: WatchEventKind,
    holder: NodeId,
    present: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for(POLL_TIMEOUT, || async {
        node.net
            .watch_interest_snapshot()
            .matching_nodes(realm_id, probe_path, kind)
            .contains(&holder)
            == present
    })
    .await
}

async fn list_via(node: &TestNode, recipient: UserId) -> Vec<NotificationRecord> {
    list_notifications_for_user(
        node.context.as_ref(),
        node.net.node_id(),
        recipient,
        None,
        LIST_LIMIT,
    )
    .await
    .expect("dispatch list notifications")
    .0
}

async fn inbox_len(node: &TestNode) -> usize {
    iter_len(node, NOTIFICATION_INBOX_KEYSPACE).await
}

async fn iter_len(node: &TestNode, key_space: &str) -> usize {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 1024,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
        other => panic!("unexpected iter event: {other:?}"),
    }
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

fn user_with_changed_holder(
    before: &RealmConfigDocument,
    after: &RealmConfigDocument,
    realm_id: RealmId,
) -> (UserId, NodeId, NodeId) {
    for _ in 0..10_000 {
        let candidate = UserId::local(Ulid::r#gen(), realm_id);
        let old_holder = resolve_inbox_holder(&candidate, before).unwrap().unwrap();
        let new_holder = resolve_inbox_holder(&candidate, after).unwrap().unwrap();
        if old_holder != new_holder {
            return (candidate, old_holder, new_holder);
        }
    }
    panic!("no user changed holder within the sampling bound");
}

fn node_by_id(nodes: &[TestNode], node_id: NodeId) -> &TestNode {
    nodes
        .iter()
        .find(|node| node.net.node_id() == node_id)
        .expect("holder belongs to test mesh")
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
    // Mirror production core-document announcement for every node.
    bootstrap_watch_interest_topic(&nodes, *realm_id).await?;
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
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
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
    install_config_document(nodes, realm_id, &config).await
}

async fn install_config_document(
    nodes: &[TestNode],
    realm_id: RealmId,
    config: &RealmConfigDocument,
) -> Result<(), Box<dyn std::error::Error>> {
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
        node.net.refresh_realm_peers_from_document(config).await?;
    }
    // Config apply hook: the shard's rank-0 holder eagerly creates each
    // shard topic genesis (mirrors the production realm-config apply path).
    for node in nodes {
        aruna_operations::process_placements::process_shard_placements(
            &node.context,
            realm_id,
            node.net.node_id(),
        )
        .await;
    }
    Ok(())
}

async fn install_group_authorization(
    nodes: &[TestNode],
    realm_id: RealmId,
    group_id: Ulid,
    owner: UserId,
) -> Result<(), Box<dyn std::error::Error>> {
    let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
    let group_auth = GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: owner,
            realm_id,
        };
        for (key, value) in [
            (realm_id.as_bytes().to_vec(), realm_auth.to_bytes(&actor)?),
            (group_id.to_bytes().to_vec(), group_auth.to_bytes(&actor)?),
        ] {
            match node
                .context
                .storage_handle
                .send_effect(Effect::Storage(StorageEffect::Write {
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                }))
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => {
                    return Err(format!("unexpected authorization write event: {other:?}").into());
                }
            }
        }
    }
    Ok(())
}

async fn bootstrap_watch_interest_topic(
    nodes: &[TestNode],
    realm_id: RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    for (index, node) in nodes.iter().enumerate() {
        let node_id = node.net.node_id();
        ensure_local_watch_interest_digest(&node.context.storage_handle, realm_id, node_id).await?;
        drive(
            ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
                realm_id,
                local_node_id: node_id,
                excluded_peers: Vec::new(),
                documents: vec![DocumentSyncTarget::WatchInterest { realm_id, node_id }],
                allow_genesis: index == 0,
            }),
            node.context.as_ref(),
        )
        .await?;

        // All digests share one realm topic. Reconcile each append before the
        // next node writes so concurrent outbox drains cannot race topic setup.
        let key = watch_interest_node_key(realm_id, node_id);
        for peer in nodes {
            wait_for(POLL_TIMEOUT, || async {
                read_watch_interest_digest(peer, key.clone())
                    .await
                    .is_some()
            })
            .await?;
        }
    }
    Ok(())
}

async fn read_watch_interest_digest(node: &TestNode, key: Vec<u8>) -> Option<WatchInterestDigest> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => WatchInterestDigest::from_bytes(bytes.as_ref()).ok(),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => None,
        other => panic!("unexpected watch-interest digest read: {other:?}"),
    }
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
