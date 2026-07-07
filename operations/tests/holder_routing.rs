use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::structs::{
    Actor, Group, GroupAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
    TokenClaims,
};
use aruna_core::types::UserId;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::placement::{placement_ref_for_target, resolve_shard_holders};
use aruna_operations::routing::dispatch::{HolderRoutingError, dispatch_holder_call};
use aruna_operations::routing::protocol::{GroupCall, GroupReply, ProxiedCall, ProxiedReply};
use aruna_storage::{FjallStorage, StorageHandle};
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use tempfile::TempDir;
use ulid::Ulid;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

// A non-holder origin proxies a group read to the shard's holder, round-tripping
// the document with a valid bearer and being refused with an invalid one.
#[tokio::test]
async fn non_holder_dispatch_round_trips_group_and_refuses_invalid_bearer() {
    let (realm_key, realm_id, user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, holder) = (&nodes[0], &nodes[1]);

    let config = config_with_replica(realm_id, &node_ids(&nodes), 1);
    install_config(&nodes, &config).await;
    persist_trusted_realm(holder, realm_id).await;

    // A group whose single holder is the other node, so the origin must forward.
    let (group_id, holders) =
        sample_group(&config, |h| h.len() == 1 && h[0] == holder.net.node_id());
    assert!(!holders.contains(&origin.net.node_id()));

    let (group, authorization) = make_group(group_id, realm_id, user_id);
    write_group(holder, &group, &authorization).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, user_id));
    let reply = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect("valid bearer round-trips the group");
    let ProxiedReply::Group(reply) = reply;
    let GroupReply::Document {
        group: fetched,
        authorization: fetched_auth,
    } = *reply;
    assert_eq!(fetched, group);
    assert_eq!(fetched_auth, authorization);

    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some("not-a-valid-jwt"),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect_err("invalid bearer is refused by the holder");
    assert!(
        matches!(error, HolderRoutingError::Unauthorized(_)),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// Every holder of the shard is unreachable: dispatch reports Unavailable.
#[tokio::test]
async fn all_holders_unreachable_is_unavailable() {
    let (_realm_key, realm_id, _user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 1).await;
    let origin = &nodes[0];

    // The sole holder of some shards is a phantom node the origin cannot dial.
    let phantom = iroh::SecretKey::from_bytes(&[201u8; 32]).public();
    let config = config_with_replica(realm_id, &[origin.net.node_id(), phantom], 1);
    install_config(&nodes, &config).await;

    let (group_id, holders) = sample_group(&config, |h| h.len() == 1 && h[0] == phantom);
    assert!(!holders.contains(&origin.net.node_id()));

    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some("not-a-valid-jwt"),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect_err("no reachable holder");
    assert!(
        matches!(error, HolderRoutingError::Unavailable),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// The origin contacts holders in resolver rank order: the document lives only on
// the rank-1 holder, yet the rank-0 holder is reached first and its authoritative
// NotFound stops the walk.
#[tokio::test]
async fn dispatch_respects_rank_order() {
    let (realm_key, realm_id, user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 3).await;
    let origin = &nodes[0];

    let config = config_with_replica(realm_id, &node_ids(&nodes), 2);
    install_config(&nodes, &config).await;
    for node in &nodes[1..] {
        persist_trusted_realm(node, realm_id).await;
    }

    // Two holders, neither of them the origin.
    let (group_id, holders) = sample_group(&config, |h| {
        h.len() == 2 && !h.contains(&origin.net.node_id())
    });
    let placement =
        placement_ref_for_target(&config, &DocumentSyncTarget::Group { group_id }, None);
    assert_eq!(holders, resolve_shard_holders(&config, &placement));

    // Seed the document only on the rank-1 holder.
    let rank_one = node_by_id(&nodes, holders[1]);
    let (group, authorization) = make_group(group_id, realm_id, user_id);
    write_group(rank_one, &group, &authorization).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, user_id));
    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect_err("the rank-0 holder is reached first and lacks the document");
    assert!(
        matches!(error, HolderRoutingError::NotFound),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

fn realm_fixture() -> (SigningKey, RealmId, UserId) {
    let mut rng = jsonwebtoken::signature::rand_core::OsRng;
    let signing_key = SigningKey::generate(&mut rng);
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let user_id = UserId::local(Ulid::new(), realm_id);
    (signing_key, realm_id, user_id)
}

fn token_claims(realm_id: RealmId, user_id: UserId) -> TokenClaims {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::new().to_string(),
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
    }
}

fn sign_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
    let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

fn config_with_replica(
    realm_id: RealmId,
    node_ids: &[NodeId],
    replica: u32,
) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    if let Some(strategy_id) = config.default_strategy_id
        && let Some(strategy) = config
            .strategies
            .iter_mut()
            .find(|strategy| strategy.strategy_id == strategy_id)
    {
        strategy.replica_count = Some(replica);
    }
    for node_id in node_ids {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

fn make_group(
    group_id: Ulid,
    realm_id: RealmId,
    owner: UserId,
) -> (Group, GroupAuthorizationDocument) {
    let group = Group {
        display_name: "team".to_string(),
        group_id,
        realm_id,
        roles: HashSet::new(),
        owner,
    };
    let authorization = GroupAuthorizationDocument {
        group_id,
        roles: HashMap::new(),
    };
    (group, authorization)
}

fn sample_group<F>(config: &RealmConfigDocument, predicate: F) -> (Ulid, Vec<NodeId>)
where
    F: Fn(&[NodeId]) -> bool,
{
    for _ in 0..100_000 {
        let group_id = Ulid::new();
        let placement =
            placement_ref_for_target(config, &DocumentSyncTarget::Group { group_id }, None);
        let holders = resolve_shard_holders(config, &placement);
        if predicate(&holders) {
            return (group_id, holders);
        }
    }
    panic!("no group matched the holder predicate within the sampling bound");
}

fn node_ids(nodes: &[TestNode]) -> Vec<NodeId> {
    nodes.iter().map(|node| node.net.node_id()).collect()
}

fn node_by_id(nodes: &[TestNode], node_id: NodeId) -> &TestNode {
    nodes
        .iter()
        .find(|node| node.net.node_id() == node_id)
        .expect("holder is one of the spawned nodes")
}

async fn meshed_nodes(realm_id: RealmId, count: usize) -> Vec<TestNode> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(realm_id).await);
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
    nodes
}

async fn spawn_node(realm_id: RealmId) -> TestNode {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
    });
    initialize_net_incoming(context.clone());
    TestNode {
        _temp_dir: temp_dir,
        net,
        context,
    }
}

async fn install_config(nodes: &[TestNode], config: &RealmConfigDocument) {
    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(config.realm_id),
            realm_id: config.realm_id,
        };
        let bytes = config.to_bytes(&actor).expect("config serializes");
        write_storage(
            &node.context.storage_handle,
            REALM_CONFIG_KEYSPACE,
            config.realm_id.as_bytes().to_vec(),
            bytes,
        )
        .await;
        node.net
            .refresh_realm_peers_from_document(config)
            .await
            .expect("refresh realm peers");
    }
}

async fn persist_trusted_realm(node: &TestNode, realm_id: RealmId) {
    let trusted: HashSet<RealmId> = HashSet::from([realm_id]);
    let bytes = postcard::to_allocvec(&trusted).expect("trusted realms serialize");
    write_storage(
        &node.context.storage_handle,
        API_STATE_KEYSPACE,
        TRUSTED_REALMS_LIST_KEY.to_vec(),
        bytes,
    )
    .await;
}

async fn write_group(node: &TestNode, group: &Group, authorization: &GroupAuthorizationDocument) {
    let key = group.group_id.to_bytes().to_vec();
    write_storage(
        &node.context.storage_handle,
        GROUP_KEYSPACE,
        key.clone(),
        postcard::to_allocvec(group).expect("group serializes"),
    )
    .await;
    write_storage(
        &node.context.storage_handle,
        AUTH_KEYSPACE,
        key,
        postcard::to_allocvec(authorization).expect("auth doc serializes"),
    )
    .await;
}

async fn write_storage(storage: &StorageHandle, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event for {key_space}: {other:?}"),
    }
}

async fn shutdown(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
