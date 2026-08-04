// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{Actor, PathRestriction, RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::s3::create_user_access::{CreateUserAccessConfig, CreateUserAccessOperation};
use aruna_operations::s3::get_user_access::GetUserAccessOperation;
use aruna_operations::s3::revoke_user_access::RevokeUserAccessOperation;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::TempDir;
use ulid::Ulid;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn credential_stays_local() -> Result<(), Box<dyn std::error::Error>> {
    // Issuer-local by design: a credential minted on node 0 must never appear
    // on node 1, even after a full explicit sync of the shared realm topic.
    let realm_id = RealmId([41u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;

    let restrictions = vec![PathRestriction {
        pattern: "datasets/**".to_string(),
        permission: aruna_core::structs::Permission::READ,
    }];
    let config = CreateUserAccessConfig {
        user_identity: UserId::local(Ulid::generate(), realm_id),
        group_id: Ulid::generate(),
        expiry: SystemTime::now() + Duration::from_secs(3600),
        path_restrictions: Some(restrictions.clone()),
        issued_by: *nodes[0].net.node_id().as_bytes(),
    };
    let seal_key = aruna_core::credential_seal::CredentialSealKey::random();
    let (access_key, _, _) = drive(
        CreateUserAccessOperation::new(config, seal_key),
        nodes[0].context.as_ref(),
    )
    .await??;

    let local = drive(
        GetUserAccessOperation::new(access_key.clone()),
        nodes[0].context.as_ref(),
    )
    .await?;
    assert!(matches!(local, Some(Ok(_))));

    // Positive control: pull the whole shared realm topic from the issuer, so
    // absence afterwards proves non-replication rather than sync lag.
    let topic = aruna_core::document::DocumentSyncTarget::RealmAuthorization { realm_id }
        .sync_topic_id(realm_id, &aruna_core::structs::PlacementRef::NIL);
    nodes[1]
        .net
        .sync_document_topics(vec![topic], vec![nodes[0].net.node_id()])
        .await;

    let remote = drive(
        GetUserAccessOperation::new(access_key.clone()),
        nodes[1].context.as_ref(),
    )
    .await?;
    assert!(remote.is_none());

    drive(
        RevokeUserAccessOperation::new(access_key.clone()),
        nodes[0].context.as_ref(),
    )
    .await?
    .expect("credential present")
    .expect("revoke succeeds");

    let revoked = drive(
        GetUserAccessOperation::new(access_key),
        nodes[0].context.as_ref(),
    )
    .await?;
    assert!(matches!(revoked, Some(Ok(access)) if access.revoked_at.is_some()));

    shutdown_nodes(nodes).await;
    Ok(())
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(*realm_id).await?);
    }
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
    install_realm_config(&nodes, realm_id).await?;
    Ok(nodes)
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
        compute_handle: None,
    });

    initialize_net_incoming(context.clone());
    initialize_task_incoming(
        context.clone(),
        task_handle,
        aruna_operations::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    Ok(TestNode {
        _temp_dir: temp_dir,
        net,
        context,
    })
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::default_for_realm(*realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }

    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(*realm_id),
            realm_id: *realm_id,
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

    for _ in 0..5 {
        for node in nodes {
            aruna_operations::startup::restore_shard_subscriptions(
                &node.context,
                node.net.node_id(),
                *realm_id,
            )
            .await;
        }
        let mut retry = false;
        for node in nodes {
            retry |= aruna_operations::process_placements::process_shard_placements(
                &node.context,
                *realm_id,
                node.net.node_id(),
            )
            .await
            .retry_scheduled;
        }
        if !retry {
            break;
        }
    }

    Ok(config)
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
