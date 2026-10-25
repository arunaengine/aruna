//! Builds a distributed bucket over this node and a peer, and a mock server for the peer's api.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::server::state::ServerState;
use crate::tests::assistant::spawn_mock;
use crate::tests::routes::{seed_group_docs, seed_realm_auth, test_state, write_doc};
use aruna_blob::blob::BlobHandler;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DISTRIBUTION_KEYSPACE, NODE_INFO_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::{
    RealmConfigDocument, RealmId, RealmNode, RealmNodeKind,
};
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo};
use aruna_core::structs::storage::distribution::presets::shard_layout;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionConfig, DistributionRequest, DistributionSettings,
    DistributionSnapshot, DistributionView, SetRoot, SignedSnapshot, Statement, next_snapshot,
};
use aruna_core::structs::storage::node_info::{
    AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization, node_info_key,
};
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::distribution::inventory::open_ranges;
use aruna_operations::driver::DriverContext;
use aruna_storage::FjallStorage;
use axum::Router;
use axum::http::{StatusCode, Uri};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use ulid::Ulid;

/// The secret of the fixture node, which publishes the set and is member 1.
pub(crate) const LOCAL: [u8; 32] = [11; 32];
/// The peer that is member 2 and coordinates every key from `m` on.
pub(crate) const PEER: [u8; 32] = [12; 32];
/// The incarnation both members are admitted with.
const INCARNATION: u128 = 5;

pub(crate) fn node_id(seed: [u8; 32]) -> NodeId {
    iroh::SecretKey::from_bytes(&seed).public()
}

fn member(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

/// The ordinary local bucket of the fixture's group.
pub(crate) const BUCKET: &str = "routed";

/// A node with blob and metadata planes and the `LOCAL` key, a group owned by `owner` and its
/// bucket.
pub(crate) struct Node {
    _dir: TempDir,
    pub(crate) state: Arc<ServerState>,
    pub(crate) owner: AuthContext,
    /// A realm user with no grant in the group.
    pub(crate) stranger: AuthContext,
    pub(crate) group_id: Ulid,
}

pub(crate) async fn node_state() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let storage = FjallStorage::open(root).unwrap();
    storage.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let realm_id = RealmId([3; 32]);
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        secret_key: Some(iroh::SecretKey::from_bytes(&LOCAL)),
        realm_id,
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };
    let net = NetHandle::new(config, storage.clone()).await.unwrap();
    let backend = BackendConfig {
        backend_type: Backend::FileSystem,
        root: format!("{root}/blobs"),
        service_config: HashMap::new(),
        bucket_prefix: Some("aruna-".to_string()),
        max_bucket_size: Some(1_000_000),
        multipart_bucket: Some("multipart".to_string()),
        timeouts: Default::default(),
    };
    let blob = BlobHandler::new(backend, storage.clone(), net.clone())
        .await
        .unwrap();
    let metadata = aruna_operations::metadata::MetadataHandle::new(
        dir.path().join("metadata"),
        node_id(LOCAL),
        storage.clone(),
        Some(net.clone()),
        None,
        None,
    )
    .unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net),
        blob_handle: Some(blob),
        metadata_handle: Some(metadata),
        task_handle: None,
        compute_handle: None,
    });
    let auth = |seed: u8| AuthContext {
        user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let (owner, stranger) = (auth(1), auth(2));
    let actor = Actor {
        node_id: node_id(LOCAL),
        user_id: owner.user_id,
        realm_id,
    };
    let group_id = Ulid::from_bytes([20; 16]);
    // Both nodes are realm servers, so this node may publish and the peer may hold a member.
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.nodes = [LOCAL, PEER]
        .map(|seed| RealmNode {
            node_id: node_id(seed).to_string(),
            kind: RealmNodeKind::Server,
        })
        .to_vec();
    let config = config.to_bytes(&actor).unwrap();
    write_doc(
        &context,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec().into(),
        config.into(),
    )
    .await;
    seed_realm_auth(&context, realm_id, &actor).await;
    seed_group_docs(
        &context,
        realm_id,
        &actor,
        group_id,
        "routed",
        owner.user_id,
    )
    .await;
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = Arc::new(test_state(context, realm_id, node_id(LOCAL), capabilities).await);
    let info = bucket_info(group_id, None);
    let key = BUCKET.as_bytes().to_vec();
    write(&state, S3_BUCKET_KEYSPACE, key, info.to_bytes().unwrap()).await;
    Node {
        _dir: dir,
        state,
        owner,
        stranger,
        group_id,
    }
}

fn bucket_info(group_id: Ulid, distribution: Option<DistributionBinding>) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution,
    }
}

pub(crate) async fn write(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let write = StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    };
    let event = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(write)
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

/// How the fixture splits a set between this node (member 1) and the peer (member 2).
#[derive(Clone, Copy)]
enum Split {
    /// Two shards split at `m`: this node coordinates the lower one, the peer the upper one.
    Shards,
}

/// The active record of `bucket`, published by this node.
fn record(state: &ServerState, owner_group: Ulid, bucket: &str, split: Split) -> SignedSnapshot {
    let realm_id = state.get_realm_id();
    let root = SetRoot {
        publisher: node_id(LOCAL),
        owner_group,
        nonce: [7; 16],
    };
    let layout = match split {
        Split::Shards => {
            let nodes = [(member(1), node_id(LOCAL)), (member(2), node_id(PEER))];
            shard_layout(root.set_id(), &nodes, &["m".to_string()], 0)
        }
    }
    .unwrap();
    let create = DistributionRequest::Create {
        root: root.clone(),
        config: DistributionConfig {
            bucket: bucket.to_string(),
            owner_group,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
    };
    let mut head = next_snapshot(realm_id, None, &create).unwrap();
    for (seed, secret) in [(1, LOCAL), (2, PEER)] {
        let intent = head.member(member(seed)).unwrap().intent;
        let statement =
            Statement::admit(realm_id, head.set_id(), member(seed), INCARNATION, intent);
        let signed = statement.sign(&iroh::SecretKey::from_bytes(&secret));
        let request = DistributionRequest::Statement(Box::new(signed));
        head = next_snapshot(realm_id, Some(&head), &request).unwrap();
    }
    head = next_snapshot(realm_id, Some(&head), &DistributionRequest::Activate).unwrap();
    let publisher = iroh::SecretKey::from_bytes(&LOCAL);
    SignedSnapshot::sign(head, realm_id, &publisher).unwrap()
}

/// Splits `bucket` into two shards: this node coordinates keys below `m`, the peer the rest.
pub(crate) async fn distribute(
    state: &ServerState,
    owner_group: Ulid,
    bucket: &str,
) -> DistributionSnapshot {
    install(state, owner_group, bucket, Split::Shards).await
}

/// Stores the record of `bucket` and binds the local bucket of that name to member 1.
async fn install(
    state: &ServerState,
    owner_group: Ulid,
    bucket: &str,
    split: Split,
) -> DistributionSnapshot {
    let head = record(state, owner_group, bucket, split);
    let snapshot = head.snapshot.clone();
    let view = DistributionView {
        head,
        conflict: None,
    };
    let set_id = snapshot.set_id();
    let bytes = view.to_bytes().unwrap();
    write(
        state,
        DISTRIBUTION_KEYSPACE,
        set_id.to_bytes().to_vec(),
        bytes,
    )
    .await;
    let binding = DistributionBinding {
        set_id,
        member_id: member(1),
        incarnation: INCARNATION,
        revision: snapshot.revision(),
    };
    let info = bucket_info(owner_group, Some(binding));
    let key = bucket.as_bytes().to_vec();
    write(state, S3_BUCKET_KEYSPACE, key, info.to_bytes().unwrap()).await;
    // The ranges member 1 holds are installed, as its admission does.
    let held: Vec<_> = snapshot
        .held_ranges(member(1))
        .into_iter()
        .cloned()
        .collect();
    let writes = open_ranges(set_id, &held).unwrap();
    let batch = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    let event = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(batch)
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    snapshot
}

/// Publishes `api` as the api url of `node_id` in its node info document.
pub(crate) async fn publish_api(state: &ServerState, node_id: NodeId, api: &str) {
    let document = NodeInfoDocument {
        node_id,
        executors: Vec::new(),
        labels: Default::default(),
        urls: NodeUrls {
            api: Some(api.to_string()),
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 0,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 1_700_000_000_000,
            download_slots: None,
            storage_capacity_bytes: None,
        },
        updated_at_ms: 1_700_000_000_500,
        epoch: AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation: 1,
            observed_at_ms: 1_700_000_000_500,
        },
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    };
    let key = node_info_key(node_id);
    write(state, NODE_INFO_KEYSPACE, key, document.to_bytes().unwrap()).await;
}

/// A peer api that answers every request with one status and body and records the paths.
pub(crate) struct MockPeer {
    pub(crate) url: String,
    pub(crate) paths: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) async fn mock_peer(status: StatusCode, body: &str) -> MockPeer {
    let paths = Arc::new(Mutex::new(Vec::new()));
    let seen = paths.clone();
    let body = body.to_string();
    let answer = move |uri: Uri| {
        seen.lock().unwrap().push(uri.to_string());
        let body = body.clone();
        async move {
            let json = [("content-type", "application/json")];
            (status, json, body)
        }
    };
    let (url, task) = spawn_mock(Router::new().fallback(answer)).await;
    MockPeer { url, paths, task }
}

/// An api url nothing can connect to: port 0 is never a listening port.
pub(crate) const CLOSED_API: &str = "http://127.0.0.1:0";
