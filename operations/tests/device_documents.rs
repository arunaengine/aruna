//! A device holds no realm data, but it is judged by the realm's own documents:
//! a revoked token must stop working there too. The realm pushes them when it
//! can, and a device that was closed at the time fetches them itself.

mod topology;

use aruna_core::structs::{Actor, RealmNodeKind};
use aruna_core::util::unix_timestamp_secs;
use aruna_operations::auth::realm_token_revoked;
use aruna_operations::driver::drive;
use aruna_operations::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenOperation,
};
use aruna_operations::startup::pull_realm_documents;

use topology::{TestResult, Topology, read_realm_config, replicate_config, spawn_node, write};

const MANAGEMENT_NODES: usize = 2;
const REPLICATION_FACTOR: u32 = 1;

#[tokio::test]
async fn device_fetches_revocation() -> TestResult<()> {
    // The realm revokes a token while the device is not in its configuration at
    // all, so no push can reach it. The device's own fetch is the only way it
    // learns, and until it runs the token still passes there.
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
    let device = spawn_node(
        realm_id,
        RealmNodeKind::User {
            owner: realm.user_id,
        },
    )
    .await?;
    let mut config = realm.config.clone();
    config.ensure_node(
        device.node_id(),
        RealmNodeKind::User {
            owner: realm.user_id,
        },
    );
    for node in &realm.nodes {
        device.net.add_peer_addr(node.net.endpoint_addr()).await;
        node.net.add_peer_addr(device.net.endpoint_addr()).await;
        // The realm nodes learn the device as a peer they may serve, and never
        // write it into their own configuration: nothing pushes to it.
        node.net.refresh_realm_peers_from_document(&config).await?;
    }
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
    assert!(
        !realm_token_revoked(&device.context.storage_handle, realm_id, &token_hash).await?,
        "the device must not know the revocation before it fetches anything"
    );

    assert!(
        pull_realm_documents(&device.context).await,
        "the device must reconcile the realm documents it fetched"
    );
    assert!(
        realm_token_revoked(&device.context.storage_handle, realm_id, &token_hash).await?,
        "the device must deny a token the realm revoked while it was away"
    );
    Ok(())
}
