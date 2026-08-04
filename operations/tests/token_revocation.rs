// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::admin_document_reducer::AdminDocumentReducerState;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::auth::bearer_token_hash;
use aruna_core::document::{DocumentSyncPublish, DocumentSyncTarget};
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{
    Actor, NodePlacementEntry, RealmConfigDocument, RealmId, RealmNodeKind, TokenClaims,
};
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::auth::{
    ArunaBearerTokenError, ArunaBearerTokenValidationState, decode_aruna_bearer_token,
    realm_token_revoked,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::revoke_token::{RevokeTokenConfig, RevokeTokenOperation};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::{FjallStorage, StorageHandle};
use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use tempfile::TempDir;
use ulid::Ulid;

mod convergence;
use convergence::wait_for_convergence;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

/// Mirrors how production wires revocation: enforcement reads the replicated
/// realm config of the node that is validating, never a node-local list.
struct PeerAuthState {
    storage: StorageHandle,
    realm_id: RealmId,
}

#[async_trait]
impl ArunaBearerTokenValidationState for PeerAuthState {
    async fn is_bearer_token_revoked(&self, token_hash: &str) -> bool {
        realm_token_revoked(&self.storage, self.realm_id, token_hash).await
    }

    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        *realm_id == self.realm_id
    }
}

#[tokio::test]
async fn revocation_denies_token_on_peer() -> TestResult<()> {
    // A token revoked on node A must be rejected on node B once the realm
    // config converges, without B ever seeing the raw token.
    let signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let nodes = build_realm_nodes(realm_id, 2).await?;
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let token = mint_token(&signing_key, realm_id, user_id);
    let token_hash = bearer_token_hash(&token);

    let peer = peer_auth(&nodes[1], realm_id);
    decode_aruna_bearer_token(&peer, &token)
        .await
        .expect("peer accepts the token before it is revoked");

    drive(
        RevokeTokenOperation::new(RevokeTokenConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id,
                realm_id,
            },
            token_hash: token_hash.clone(),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "the revocation never reached the peer node",
        || async {
            Ok(usize::from(
                !realm_token_revoked(&nodes[1].context.storage_handle, realm_id, &token_hash).await,
            ))
        },
    )
    .await?;

    let error = decode_aruna_bearer_token(&peer, &token)
        .await
        .expect_err("peer rejects the revoked token");
    assert!(matches!(error, ArunaBearerTokenError::TokenRevoked));

    // The revocation is durable realm state, not process state: a validation
    // state built fresh over the same storage still denies the token.
    let restarted = peer_auth(&nodes[1], realm_id);
    assert!(matches!(
        decode_aruna_bearer_token(&restarted, &token).await,
        Err(ArunaBearerTokenError::TokenRevoked)
    ));

    // Only the revoked token is denied; the realm stays usable.
    let other = mint_token(&signing_key, realm_id, user_id);
    decode_aruna_bearer_token(&peer, &other)
        .await
        .expect("peer still accepts a token that was never revoked");

    shutdown_nodes(nodes).await;
    Ok(())
}

fn peer_auth(node: &TestNode, realm_id: RealmId) -> PeerAuthState {
    PeerAuthState {
        storage: node.context.storage_handle.clone(),
        realm_id,
    }
}

fn mint_token(signing_key: &SigningKey, realm_id: RealmId, user_id: UserId) -> String {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let claims = TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::generate().to_string(),
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
    };
    let key_pem = signing_key
        .to_pkcs8_pem(LineEnding::LF)
        .expect("realm key encodes");
    encode(
        &Header::new(Algorithm::EdDSA),
        &claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).expect("realm key is an ed25519 key"),
    )
    .expect("token signs")
}

async fn build_realm_nodes(realm_id: RealmId, count: usize) -> TestResult<Vec<TestNode>> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(realm_id).await?);
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

async fn spawn_node(realm_id: RealmId) -> TestResult<TestNode> {
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
    let task_handle = aruna_tasks::TaskHandle::new();
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

fn base_config(nodes: &[TestNode], realm_id: RealmId) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        let node_id = node.net.node_id();
        config.ensure_node(node_id, RealmNodeKind::Management);
        config.placement_map.push(NodePlacementEntry {
            node_id,
            location: "eu".to_string(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        });
    }
    config
}

async fn install_realm_config(nodes: &[TestNode], realm_id: RealmId) -> TestResult<()> {
    let config = base_config(nodes, realm_id);
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
    seed_config_sync_topic(nodes, realm_id, &config).await?;
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

/// Creates the realm config sync topic on both nodes, as the production realm
/// config apply path does, so a later admin event has a topic to ride.
async fn seed_config_sync_topic(
    nodes: &[TestNode],
    realm_id: RealmId,
    config: &RealmConfigDocument,
) -> TestResult<()> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let placement =
        aruna_operations::placement::placement_ref_for_target(config, &target, Default::default());
    let topic = target.sync_topic_id(realm_id, &placement);
    let actor = Actor {
        node_id: nodes[1].net.node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    let mut reducer_state =
        AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
    let event = reducer_state.apply_operation(
        &actor,
        AdminDocumentOperation::RealmConfigNodePlacementSet {
            entry: config
                .placement_map
                .first()
                .ok_or("realm config has no placement entry")?
                .clone(),
        },
    )?;

    match nodes[1]
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::PublishDocuments {
                documents: vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(event),
                    placement,
                    allow_genesis: true,
                }],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsPublished { .. })) => {}
        other => return Err(format!("unexpected config seed publish event: {other:?}").into()),
    }

    match nodes[0]
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::SyncDocuments {
                topics: vec![topic],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
            ..
        })) => Ok(()),
        other => Err(format!("unexpected config seed sync event: {other:?}").into()),
    }
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
