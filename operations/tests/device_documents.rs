//! A device holds no realm data, but it is judged by the realm's own documents:
//! a revoked token must stop working there too. A device runs no document sync,
//! so it fetches them as a routed read whenever it comes back.

mod topology;

use std::collections::{HashMap, HashSet};

use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::structs::{Actor, RealmNodeKind, User};
use aruna_core::util::unix_timestamp_secs;
use aruna_operations::auth::realm_token_revoked;
use aruna_operations::device::realm_documents::{fetch_realm_documents, installed_memberships};
use aruna_operations::driver::drive;
use aruna_operations::read_user_document::ReadUserDocumentOperation;
use aruna_operations::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenOperation,
};

use topology::{
    TestNode, TestResult, Topology, read_group_auth, read_group_record, read_realm_config,
    replicate_config, spawn_node, write,
};

const MANAGEMENT_NODES: usize = 2;
const REPLICATION_FACTOR: u32 = 1;
/// Far above an in-process exchange, and never a performance assertion: it is
/// only here so a hung peer ends the test instead of the suite.
const FETCH_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

#[tokio::test]
async fn device_fetches_revocation() -> TestResult<()> {
    // The realm revokes a token while the device is away. Nothing is pushed to a
    // device, so its own fetch is the only way it learns, and until it runs the
    // token still passes there.
    let realm = Topology::spawn(MANAGEMENT_NODES, 0, REPLICATION_FACTOR).await?;
    let realm_id = realm.realm_id;
    let token_hash = "a".repeat(64);

    drive(
        RevokeTokenOperation::new(RevokeTokenConfig {
            actor: realm.actor(realm.node(0)),
            token_hash: token_hash.clone(),
            expires_at: unix_timestamp_secs() + 3_600,
            token_owner: realm.user_id,
            admission: RevokeTokenAdmission::Privileged,
            now: unix_timestamp_secs(),
        }),
        realm.node(0).context.as_ref(),
    )
    .await?;
    replicate_config(&realm.nodes, realm_id).await;
    assert!(
        read_realm_config(realm.node(0), realm_id)
            .await?
            .token_revoked(&token_hash, unix_timestamp_secs()),
        "the realm must hold its own revocation"
    );

    // The device joins afterwards, with the configuration as it was before the
    // revocation: exactly what a device that was closed comes back to.
    let device = join_device(&realm).await?;
    assert!(
        !realm_token_revoked(&device.context.storage_handle, realm_id, &token_hash).await?,
        "the device must not know the revocation before it fetches anything"
    );

    assert!(
        fetch_realm_documents(&device.context, FETCH_BUDGET).await,
        "a realm node must serve the device the realm documents"
    );
    assert!(
        realm_token_revoked(&device.context.storage_handle, realm_id, &token_hash).await?,
        "the device must deny a token the realm revoked while it was away"
    );
    Ok(())
}

#[tokio::test]
async fn device_fetches_owner() -> TestResult<()> {
    // The owner's profile lives on the realm nodes; a device holds no copy
    // until it fetches, and the owner's own token would find nothing there.
    let realm = Topology::spawn(MANAGEMENT_NODES, 0, REPLICATION_FACTOR).await?;
    let owner = User {
        user_id: realm.user_id,
        name: "Device Owner".to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: HashSet::new(),
        attributes: HashMap::from([("ui.theme".to_string(), "dark".to_string())]),
    };
    for node in &realm.nodes {
        write(
            node,
            USER_KEYSPACE,
            realm.user_id.to_bytes(),
            owner.to_bytes(&realm.actor(node))?,
        )
        .await?;
    }
    let device = join_device(&realm).await?;
    assert!(
        drive(
            ReadUserDocumentOperation::new(realm.user_id),
            device.context.as_ref()
        )
        .await
        .is_err(),
        "the device must not hold the owner before it fetches anything"
    );

    assert!(
        fetch_realm_documents(&device.context, FETCH_BUDGET).await,
        "a realm node must serve the device the realm documents"
    );
    let fetched = drive(
        ReadUserDocumentOperation::new(realm.user_id),
        device.context.as_ref(),
    )
    .await?;
    assert_eq!(fetched, owner);
    Ok(())
}

#[tokio::test]
async fn device_fetches_groups() -> TestResult<()> {
    // Group documents replicate on a plane devices are excluded from, so the
    // fetched projection is the only way the owner's groups reach a device.
    let realm = Topology::spawn(MANAGEMENT_NODES, 0, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let device = join_device(&realm).await?;
    assert!(
        installed_memberships(&device.context, realm.realm_id)
            .await
            .is_empty(),
        "the device must hold no group before it fetches anything"
    );

    assert!(
        fetch_realm_documents(&device.context, FETCH_BUDGET).await,
        "a realm node must serve the device the realm documents"
    );

    let memberships = installed_memberships(&device.context, realm.realm_id).await;
    let projected: Vec<_> = memberships
        .iter()
        .map(|membership| membership.group_id)
        .collect();
    assert_eq!(projected, vec![group_id]);
    let roles: Vec<&str> = memberships[0]
        .roles
        .iter()
        .map(|role| role.name.as_str())
        .collect();
    assert_eq!(roles, vec!["admin"], "only the owner's own roles reach it");

    // A display projection must never give a device group authorization.
    assert!(read_group_auth(&device, group_id).await?.is_none());
    assert!(read_group_record(&device, group_id).await?.is_none());
    Ok(())
}

/// Spawns a device of the realm's user, meshes it with every realm node and
/// registers it in their configurations. The device itself starts from the
/// fixture's configuration: what a device that was closed comes back with.
async fn join_device(realm: &Topology) -> TestResult<TestNode> {
    let realm_id = realm.realm_id;
    let kind = RealmNodeKind::User {
        owner: realm.user_id,
    };
    let device = spawn_node(realm_id, kind.clone()).await?;
    for node in &realm.nodes {
        device.net.add_peer_addr(node.net.endpoint_addr()).await;
        node.net.add_peer_addr(device.net.endpoint_addr()).await;
        let mut known = read_realm_config(node, realm_id).await?;
        known.ensure_node(device.node_id(), kind.clone());
        write(
            node,
            aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            known.to_bytes(&realm.actor(node))?,
        )
        .await?;
        node.net.refresh_realm_peers_from_document(&known).await?;
    }
    let mut config = realm.config.clone();
    config.ensure_node(device.node_id(), kind);
    let actor = Actor {
        node_id: device.node_id(),
        user_id: realm.user_id,
        realm_id,
    };
    write(
        &device,
        aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        config.to_bytes(&actor)?,
    )
    .await?;
    device
        .net
        .refresh_realm_peers_from_document(&config)
        .await?;
    Ok(device)
}
